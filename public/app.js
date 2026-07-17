// ==========================================================================
// CRDT RGA Sequence Core Functions
// ==========================================================================

function compareIds(id1, id2) {
  if (id1.clock !== id2.clock) {
    return id1.clock - id2.clock;
  }
  return id1.site < id2.site ? -1 : (id1.site > id2.site ? 1 : 0);
}

// Converts a node ID to a hash key
function nodeIdKey(id) {
  if (!id) return 'start';
  return `${id.site}:${id.clock}`;
}

// Finds the index of a node in the list
function findNodeIndex(nodes, id) {
  if (!id) return -1;
  for (let i = 0; i < nodes.length; i++) {
    if (nodes[i].id.site === id.site && nodes[i].id.clock === id.clock) {
      return i;
    }
  }
  return -1;
}

// Integrates a new node into the RGA list
function integrateNode(replica, node) {
  const key = nodeIdKey(node.id);
  if (replica.appliedIds.has(key)) {
    return;
  }

  // Find origin index
  let originIdx = -1;
  if (node.origin !== null) {
    originIdx = findNodeIndex(replica.nodes, node.origin);
    if (originIdx === -1) {
      // Out-of-order delivery: origin has not arrived yet! Buffer it.
      if (!replica.pendingInserts.some(n => nodeIdKey(n.id) === key)) {
        replica.pendingInserts.push(node);
      }
      return;
    }
  }

  // Find insertion index
  let insertIdx = originIdx + 1;
  while (insertIdx < replica.nodes.length) {
    const nextNode = replica.nodes[insertIdx];
    let nextOriginIdx = -1;
    if (nextNode.origin !== null) {
      nextOriginIdx = findNodeIndex(replica.nodes, nextNode.origin);
    }

    if (nextOriginIdx < originIdx) {
      // Next node's origin is before our origin. Since nodes are ordered, stop.
      break;
    } else if (nextOriginIdx === originIdx) {
      // Sibling node (same origin). Break tie deterministically.
      // Higher (clock, site) wins the left slot.
      if (compareIds(nextNode.id, node.id) > 0) {
        insertIdx++;
      } else {
        break;
      }
    } else { // nextOriginIdx > originIdx
      // Next node is a descendant of a sibling or node to the right. Skip it.
      insertIdx++;
    }
  }

  // Splice node in
  replica.nodes.splice(insertIdx, 0, node);
  replica.appliedIds.add(key);

  // Apply any pending deletes
  if (replica.pendingDeletes.has(key)) {
    node.deleted = true;
    replica.pendingDeletes.delete(key);
  }

  // Trigger any pending inserts that are now unblocked
  let progress = true;
  while (progress) {
    progress = false;
    for (let i = 0; i < replica.pendingInserts.length; i++) {
      const pNode = replica.pendingInserts[i];
      let pOriginIdx = -1;
      if (pNode.origin !== null) {
        pOriginIdx = findNodeIndex(replica.nodes, pNode.origin);
      }
      if (pNode.origin === null || pOriginIdx !== -1) {
        replica.pendingInserts.splice(i, 1);
        integrateNode(replica, pNode);
        progress = true;
        break;
      }
    }
  }
}

// Applies a delete tombstone
function applyDelete(replica, targetId) {
  const key = nodeIdKey(targetId);
  const idx = findNodeIndex(replica.nodes, targetId);
  if (idx !== -1) {
    replica.nodes[idx].deleted = true;
  } else {
    // Out-of-order delete: remember it
    replica.pendingDeletes.add(key);
  }
}

// Get the previous visible node ID from a given node
function getVisiblePreviousNodeId(replica, targetNode) {
  const idx = findNodeIndex(replica.nodes, targetNode.id);
  if (idx === -1) return null;
  for (let i = idx - 1; i >= 0; i--) {
    if (!replica.nodes[i].deleted) {
      return replica.nodes[i].id;
    }
  }
  return null;
}

// Get the previous visible node ID from a given node ID
function getVisiblePreviousNodeIdById(replica, targetId) {
  const idx = findNodeIndex(replica.nodes, targetId);
  if (idx === -1) return null;
  for (let i = idx - 1; i >= 0; i--) {
    if (!replica.nodes[i].deleted) {
      return replica.nodes[i].id;
    }
  }
  return null;
}

// Maps caret cursor pos to actual visible node it attaches to
function getCaretAttachment(replica, posId) {
  if (posId === null) return null;
  const idx = findNodeIndex(replica.nodes, posId);
  if (idx === -1) return null;
  for (let i = idx; i >= 0; i--) {
    if (!replica.nodes[i].deleted) {
      return replica.nodes[i].id;
    }
  }
  return null;
}

// ==========================================================================
// Client State & App Configuration
// ==========================================================================

const replicas = {
  A: createReplicaObject('A', 'Alice', '#ff6b4a'),
  B: createReplicaObject('B', 'Bob', '#00e5ff'),
  C: createReplicaObject('C', 'Charlie', '#baff00')
};
const allReplicas = [replicas.A, replicas.B, replicas.C];

const loggedSeqs = new Set();
const latencySlider = document.getElementById('latency-slider');
const latencyValEl = document.getElementById('latency-val');
let showTombstones = false;
let currentLogFilter = 'all';

function getSimulatedLatency() {
  return parseInt(latencySlider.value, 10);
}

latencySlider.addEventListener('input', () => {
  latencyValEl.textContent = `${latencySlider.value}ms`;
});

function createReplicaObject(siteId, name, color) {
  return {
    siteId,
    name,
    color,
    nodes: [],
    appliedSeqs: new Set(),
    appliedIds: new Set(),
    pendingDeletes: new Set(),
    pendingInserts: [],
    clock: 0,
    caretPosId: null,
    online: true,
    syncing: false,
    lastSeenSeq: 0,
    remoteCursors: {},
    sseSource: null,
    outboundQueue: []
  };
}

// Reset replica local states
function resetLocalReplicaState(replica) {
  replica.nodes = [];
  replica.appliedSeqs.clear();
  replica.appliedIds.clear();
  replica.pendingDeletes.clear();
  replica.pendingInserts = [];
  replica.clock = 0;
  replica.caretPosId = null;
  replica.lastSeenSeq = 0;
  replica.remoteCursors = {};
  replica.outboundQueue = [];
  
  if (replica.siteId === 'A') {
    loggedSeqs.clear();
    const logEl = document.getElementById('sync-log-entries');
    if (logEl) logEl.innerHTML = '';
  }

  updateReplicaRibbon(replica);
  renderReplica(replica);
  checkSystemConvergence();
}

// ==========================================================================
// Network & Synchronization Controller
// ==========================================================================

// Connect EventSource (SSE) for real-time updates
function connectReplicaStream(replica) {
  if (replica.sseSource) {
    replica.sseSource.close();
  }

  const sse = new EventSource(`/api/crdt/stream?siteId=${replica.siteId}`);
  replica.sseSource = sse;

  sse.onmessage = (event) => {
    if (!replica.online || replica.syncing) return;

    const message = JSON.parse(event.data);
    const latency = getSimulatedLatency();

    // Delay incoming messages by simulated latency
    setTimeout(() => {
      if (!replica.online || replica.syncing) return;
      handleStreamMessage(replica, message);
    }, latency);
  };

  sse.onerror = (err) => {
    console.error(`Site ${replica.siteId} SSE connection closed:`, err);
    sse.close();
  };
}

// Handle real-time EventSource messages
function handleStreamMessage(replica, message) {
  if (message.type === 'op') {
    const op = message.op;
    if (replica.appliedSeqs.has(op.seq)) return;

    applyOp(replica, op);
    addSyncLogEntry(op);
    renderReplica(replica);
    checkSystemConvergence();
  } else if (message.type === 'cursor') {
    replica.remoteCursors[message.siteId] = message.posId;
    renderReplica(replica);
  } else if (message.type === 'reset') {
    console.log(`Site ${replica.siteId} resetting local state.`);
    resetLocalReplicaState(replica);
    catchUpReplica(replica);
  }
}

// Apply an op to the replica
function applyOp(replica, op) {
  if (replica.appliedSeqs.has(op.seq)) return;

  if (op.type === 'insert') {
    const nodeClone = JSON.parse(JSON.stringify(op.node));
    integrateNode(replica, nodeClone);
    
    // Ensure replica local clock is ahead of anything it integrates
    if (op.node.id.site === replica.siteId) {
      replica.clock = Math.max(replica.clock, op.node.id.clock);
    }
  } else if (op.type === 'delete') {
    applyDelete(replica, op.targetId);
  }

  replica.appliedSeqs.add(op.seq);
  replica.lastSeenSeq = Math.max(replica.lastSeenSeq, op.seq);
}

// Catch-up operation polling
async function catchUpReplica(replica) {
  try {
    const response = await fetch(`/api/crdt/ops?since=${replica.lastSeenSeq}`);
    const result = await response.json();
    if (result.status === 'success') {
      const ops = result.data;
      for (const op of ops) {
        applyOp(replica, op);
        addSyncLogEntry(op);
      }
      renderReplica(replica);
      checkSystemConvergence();
    }
  } catch (err) {
    console.error(`Failed catching up Site ${replica.siteId}:`, err);
  }
}

// Sends an operation to the server
function sendOpToServer(replica, op) {
  const latency = getSimulatedLatency();

  // Simulated latency delay
  setTimeout(async () => {
    try {
      const response = await fetch('/api/crdt/ops', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(op)
      });
      const result = await response.json();
      if (result.status === 'success') {
        const returnedOp = result.data;
        replica.appliedSeqs.add(returnedOp.seq);
        replica.lastSeenSeq = Math.max(replica.lastSeenSeq, returnedOp.seq);
        addSyncLogEntry(returnedOp);
        checkSystemConvergence();
      }
    } catch (err) {
      console.error(`Send op failed for Site ${replica.siteId}:`, err);
    }
  }, latency);
}

// Sends cursor positions to the server
function sendCursorToServer(replica) {
  if (!replica.online || replica.syncing) return;

  const latency = getSimulatedLatency();
  const body = {
    siteId: replica.siteId,
    posId: replica.caretPosId
  };

  setTimeout(async () => {
    if (!replica.online) return;
    try {
      await fetch('/api/crdt/cursor', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
    } catch (err) {
      // ignore
    }
  }, latency);
}

// Global Sync Log Renderer
function addSyncLogEntry(op) {
  if (loggedSeqs.has(op.seq)) return;
  loggedSeqs.add(op.seq);

  const logEl = document.getElementById('sync-log-entries');
  if (!logEl) return;

  const entryEl = document.createElement('div');
  entryEl.className = `log-entry log-${op.sender}`;

  let details = '';
  if (op.type === 'insert') {
    const originStr = op.node.origin ? `${op.node.origin.site}:${op.node.origin.clock}` : 'START';
    const charDisplay = op.node.char === '\n' ? '↵ (newline)' : `'${op.node.char}'`;
    details = `INSERT char ${charDisplay} | Node ID: ${op.node.id.site}:${op.node.id.clock} | Origin: ${originStr}`;
  } else if (op.type === 'delete') {
    details = `DELETE Node ID: ${op.targetId.site}:${op.targetId.clock}`;
  }

  entryEl.textContent = `[Seq: ${op.seq}] [User: ${op.sender}] -> ${details}`;
  
  if (currentLogFilter !== 'all' && op.sender !== currentLogFilter) {
    entryEl.style.display = 'none';
  }

  logEl.appendChild(entryEl);
  logEl.scrollTop = logEl.scrollHeight;
}

// Refilters the sync log DOM nodes
function applyLogFiltering() {
  const logEntries = document.querySelectorAll('.log-entry');
  logEntries.forEach(entry => {
    if (currentLogFilter === 'all') {
      entry.style.display = 'block';
    } else {
      if (entry.classList.contains(`log-${currentLogFilter}`)) {
        entry.style.display = 'block';
      } else {
        entry.style.display = 'none';
      }
    }
  });
}

// Updates the connection ribbon visual
function updateReplicaRibbon(replica) {
  const ribbon = document.getElementById(`ribbon-${replica.siteId}`);
  if (!ribbon) return;

  const textEl = ribbon.querySelector('.conn-text');

  if (replica.syncing) {
    ribbon.className = 'connection-status-ribbon syncing';
    textEl.textContent = 'Synchronizing operations ledger...';
  } else if (replica.online) {
    ribbon.className = 'connection-status-ribbon online';
    textEl.textContent = 'Active connection to global log (SSE)';
  } else {
    ribbon.className = 'connection-status-ribbon offline';
    textEl.textContent = 'Disconnected from global log (Local Mode)';
  }
}

// Toggles Online/Offline states
async function toggleReplicaStatus(replica) {
  const btn = document.getElementById(`toggle-${replica.siteId}`);
  const card = document.querySelector(`.site-${replica.siteId}`);

  if (replica.online) {
    // Go offline
    replica.online = false;
    if (replica.sseSource) {
      replica.sseSource.close();
      replica.sseSource = null;
    }
    
    replica.remoteCursors = {};

    btn.textContent = 'OFFLINE';
    btn.className = 'status-pill offline';
    card.classList.add('offline-replica');
    
    updateReplicaRibbon(replica);
    renderReplica(replica);
    checkSystemConvergence();
  } else {
    // Reconnect
    replica.syncing = true;
    btn.textContent = 'SYNCING...';
    btn.className = 'status-pill syncing';
    updateReplicaRibbon(replica);
    
    setTimeout(async () => {
      await catchUpReplica(replica);
      connectReplicaStream(replica);

      replica.syncing = false;
      replica.online = true;
      btn.textContent = 'ONLINE';
      btn.className = 'status-pill online';
      card.classList.remove('offline-replica');
      
      updateReplicaRibbon(replica);

      while (replica.outboundQueue.length > 0) {
        const op = replica.outboundQueue.shift();
        sendOpToServer(replica, op);
      }

      sendCursorToServer(replica);
      renderReplica(replica);
      checkSystemConvergence();
    }, 600);
  }
  
  renderPresence();
}

// Helper to compute plain-text representation
function getReplicaText(replica) {
  return replica.nodes
    .filter(node => !node.deleted)
    .map(node => node.char)
    .join('');
}

// Compares plain-text contents of online replicas
function checkSystemConvergence() {
  const badge = document.getElementById('convergence-badge');
  const onlineReplicas = allReplicas.filter(r => r.online && !r.syncing);

  if (onlineReplicas.length <= 1) {
    badge.innerHTML = '<span class="badge-dot"></span><span class="badge-text">✓ CONVERGED</span>';
    badge.className = 'badge badge-converged';
    return;
  }

  const firstText = getReplicaText(onlineReplicas[0]);
  let converged = true;
  for (let i = 1; i < onlineReplicas.length; i++) {
    if (getReplicaText(onlineReplicas[i]) !== firstText) {
      converged = false;
      break;
    }
  }

  if (converged) {
    badge.innerHTML = '<span class="badge-dot"></span><span class="badge-text">✓ CONVERGED</span>';
    badge.className = 'badge badge-converged';
  } else {
    badge.innerHTML = '<span class="badge-dot"></span><span class="badge-text">⧗ DIVERGING</span>';
    badge.className = 'badge badge-diverging';
  }
}

// Renders the presence status rows in all replicas
function renderPresence() {
  allReplicas.forEach(r => {
    allReplicas.forEach(other => {
      if (r.siteId === other.siteId) return;
      const el = document.getElementById(`presence-${r.siteId}-${other.siteId}`);
      if (!el) return;

      const statusSpan = el.querySelector('.status-text');
      const dot = el.querySelector('.status-dot');

      if (other.syncing) {
        statusSpan.textContent = 'SYNCING...';
        dot.style.opacity = '0.6';
      } else if (other.online) {
        statusSpan.textContent = 'ONLINE';
        dot.style.opacity = '1';
      } else {
        statusSpan.textContent = 'OFFLINE';
        dot.style.opacity = '0.2';
      }
    });
  });
}

// ==========================================================================
// Keyboard Input & Text Editing Model
// ==========================================================================

function handleInsert(replica, char) {
  replica.clock++;
  const nodeId = { site: replica.siteId, clock: replica.clock };
  const origin = replica.caretPosId;
  const node = {
    id: nodeId,
    char,
    deleted: false,
    origin
  };

  integrateNode(replica, node);
  replica.caretPosId = nodeId; 

  renderReplica(replica);

  const op = {
    type: 'insert',
    sender: replica.siteId,
    node
  };

  if (replica.online) {
    sendOpToServer(replica, op);
  } else {
    replica.outboundQueue.push(op);
  }

  sendCursorToServer(replica);
}

function handleBackspace(replica) {
  if (replica.caretPosId === null) return;

  const targetId = replica.caretPosId;
  const prevVisibleId = getVisiblePreviousNodeIdById(replica, targetId);

  applyDelete(replica, targetId);
  replica.caretPosId = prevVisibleId;

  renderReplica(replica);

  const op = {
    type: 'delete',
    sender: replica.siteId,
    targetId
  };

  if (replica.online) {
    sendOpToServer(replica, op);
  } else {
    replica.outboundQueue.push(op);
  }

  sendCursorToServer(replica);
}

function moveCaretHorizontal(replica, direction) {
  if (direction === 'left') {
    if (replica.caretPosId === null) return;
    replica.caretPosId = getVisiblePreviousNodeIdById(replica, replica.caretPosId);
  } else if (direction === 'right') {
    if (replica.caretPosId === null) {
      const firstVisible = replica.nodes.find(n => !n.deleted);
      if (firstVisible) {
        replica.caretPosId = firstVisible.id;
      }
    } else {
      const idx = findNodeIndex(replica.nodes, replica.caretPosId);
      if (idx !== -1) {
        let nextVisibleId = replica.caretPosId;
        for (let i = idx + 1; i < replica.nodes.length; i++) {
          if (!replica.nodes[i].deleted) {
            nextVisibleId = replica.nodes[i].id;
            break;
          }
        }
        replica.caretPosId = nextVisibleId;
      }
    }
  }

  sendCursorToServer(replica);
  renderReplica(replica);
}

function moveCaretVertically(replica, direction) {
  const editorEl = document.getElementById(`editor-${replica.siteId}`);
  const caretEl = editorEl.querySelector('.local-caret');
  if (!caretEl) return;

  const caretRect = caretEl.getBoundingClientRect();
  const caretX = (caretRect.left + caretRect.right) / 2;
  const caretY = (caretRect.top + caretRect.bottom) / 2;

  const spans = Array.from(editorEl.querySelectorAll('.char-span'));
  if (spans.length === 0) return;

  const charHeight = spans[0].getBoundingClientRect().height || 18;
  const targetY = direction === 'up' ? caretY - charHeight : caretY + charHeight;

  let bestSpan = null;
  let minDistance = Infinity;

  for (const span of spans) {
    const isTombstone = span.classList.contains('tombstone');
    if (isTombstone && !showTombstones) continue;

    const rect = span.getBoundingClientRect();
    const spanY = (rect.top + rect.bottom) / 2;
    const spanX = (rect.left + rect.right) / 2;

    const vDist = Math.abs(spanY - targetY);
    if (vDist < charHeight * 0.75) {
      const hDist = Math.abs(spanX - caretX);
      if (hDist < minDistance) {
        minDistance = hDist;
        bestSpan = span;
      }
    }
  }

  if (bestSpan) {
    const nodeObj = JSON.parse(bestSpan.dataset.node);
    const rect = bestSpan.getBoundingClientRect();
    if (caretX < (rect.left + rect.right) / 2) {
      replica.caretPosId = getVisiblePreviousNodeId(replica, nodeObj);
    } else {
      replica.caretPosId = nodeObj.id;
    }
  } else {
    if (direction === 'up') {
      replica.caretPosId = null;
    } else {
      const visibleNodes = replica.nodes.filter(n => !n.deleted);
      if (visibleNodes.length > 0) {
        replica.caretPosId = visibleNodes[visibleNodes.length - 1].id;
      }
    }
  }

  sendCursorToServer(replica);
  renderReplica(replica);
}

// ==========================================================================
// DOM Renderer
// ==========================================================================

function renderReplica(replica) {
  const editorEl = document.getElementById(`editor-${replica.siteId}`);
  if (!editorEl) return;

  const caretsByAttachment = {};

  const isFocused = document.activeElement === editorEl;
  if (isFocused) {
    const localAttachedId = getCaretAttachment(replica, replica.caretPosId);
    const localKey = nodeIdKey(localAttachedId);
    if (!caretsByAttachment[localKey]) caretsByAttachment[localKey] = [];
    caretsByAttachment[localKey].push({
      type: 'local',
      siteId: replica.siteId,
      name: replica.name,
      color: replica.color
    });
  }

  for (const other of allReplicas) {
    if (other.siteId !== replica.siteId && other.online && !other.syncing) {
      const rawPosId = replica.remoteCursors[other.siteId];
      const posId = rawPosId !== undefined ? rawPosId : null;
      const attachedId = getCaretAttachment(replica, posId);
      const remoteKey = nodeIdKey(attachedId);
      if (!caretsByAttachment[remoteKey]) caretsByAttachment[remoteKey] = [];
      caretsByAttachment[remoteKey].push({
        type: 'remote',
        siteId: other.siteId,
        name: other.name,
        color: other.color
      });
    }
  }

  function makeCaretElement(caret) {
    if (caret.type === 'local') {
      const caretSpan = document.createElement('span');
      caretSpan.className = 'local-caret';
      return caretSpan;
    } else {
      const caretSpan = document.createElement('span');
      caretSpan.className = `remote-caret border-${caret.siteId}`;
      caretSpan.style.borderColor = caret.color;
      
      const tag = document.createElement('span');
      tag.className = `remote-caret-tag bg-${caret.siteId}`;
      tag.style.backgroundColor = caret.color;
      tag.textContent = caret.name.toUpperCase();
      
      caretSpan.appendChild(tag);
      return caretSpan;
    }
  }

  editorEl.innerHTML = '';

  if (caretsByAttachment['start']) {
    caretsByAttachment['start'].forEach(caret => {
      editorEl.appendChild(makeCaretElement(caret));
    });
  }

  replica.nodes.forEach(node => {
    if (node.deleted && !showTombstones) return;

    const charSpan = document.createElement('span');
    charSpan.className = 'char-span';
    
    if (node.deleted) {
      charSpan.className += ' tombstone';
    }

    charSpan.dataset.meta = `ID: ${node.id.site}:${node.id.clock} | Origin: ${node.origin ? node.origin.site + ':' + node.origin.clock : 'START'}${node.deleted ? ' | (TOMBSTONE)' : ''}`;
    charSpan.dataset.node = JSON.stringify(node);

    if (node.char === '\n') {
      charSpan.className += ' char-span-newline';
      charSpan.textContent = '\n';
    } else {
      charSpan.textContent = node.char;
    }

    editorEl.appendChild(charSpan);

    const key = nodeIdKey(node.id);
    if (caretsByAttachment[key]) {
      caretsByAttachment[key].forEach(caret => {
        editorEl.appendChild(makeCaretElement(caret));
      });
    }
  });

  const charCountVal = replica.nodes.filter(n => !n.deleted).length;
  document.getElementById(`char-count-${replica.siteId}`).textContent = charCountVal;
  document.getElementById(`ops-seen-${replica.siteId}`).textContent = replica.appliedSeqs.size;
}

// ==========================================================================
// Setup Page Event Listeners
// ==========================================================================

function handleKeyDown(e, replica) {
  if (replica.syncing) {
    e.preventDefault();
    return;
  }

  const key = e.key;

  if (e.ctrlKey || e.metaKey || e.altKey) {
    return;
  }

  if (key === 'Backspace') {
    e.preventDefault();
    handleBackspace(replica);
  } else if (key === 'Enter') {
    e.preventDefault();
    handleInsert(replica, '\n');
  } else if (key === 'ArrowLeft') {
    e.preventDefault();
    moveCaretHorizontal(replica, 'left');
  } else if (key === 'ArrowRight') {
    e.preventDefault();
    moveCaretHorizontal(replica, 'right');
  } else if (key === 'ArrowUp') {
    e.preventDefault();
    moveCaretVertically(replica, 'up');
  } else if (key === 'ArrowDown') {
    e.preventDefault();
    moveCaretVertically(replica, 'down');
  } else if (key.length === 1) {
    e.preventDefault();
    handleInsert(replica, key);
  }
}

function initApp() {
  allReplicas.forEach(replica => {
    const editorEl = document.getElementById(`editor-${replica.siteId}`);
    
    editorEl.addEventListener('keydown', (e) => handleKeyDown(e, replica));
    editorEl.addEventListener('focus', () => {
      renderReplica(replica);
      sendCursorToServer(replica);
    });
    editorEl.addEventListener('blur', () => {
      setTimeout(() => {
        renderReplica(replica);
      }, 150);
    });

    editorEl.addEventListener('click', (e) => {
      const charSpan = e.target.closest('.char-span');
      if (charSpan) {
        const nodeObj = JSON.parse(charSpan.dataset.node);
        const rect = charSpan.getBoundingClientRect();
        const clickX = e.clientX - rect.left;
        
        if (clickX < rect.width / 2) {
          replica.caretPosId = getVisiblePreviousNodeId(replica, nodeObj);
        } else {
          replica.caretPosId = nodeObj.id;
        }
      } else {
        const visibleNodes = replica.nodes.filter(n => !n.deleted);
        if (visibleNodes.length > 0) {
          replica.caretPosId = visibleNodes[visibleNodes.length - 1].id;
        } else {
          replica.caretPosId = null;
        }
      }
      sendCursorToServer(replica);
      renderReplica(replica);
    });

    document.getElementById(`toggle-${replica.siteId}`).addEventListener('click', () => {
      toggleReplicaStatus(replica);
    });

    connectReplicaStream(replica);
    catchUpReplica(replica);
  });

  document.getElementById('reset-btn').addEventListener('click', async () => {
    try {
      await fetch('/api/crdt/reset', { method: 'POST' });
    } catch (err) {
      console.error('Reset database failed:', err);
    }
  });

  document.getElementById('run-tests-btn').addEventListener('click', () => {
    runDiagnostics();
  });
  
  document.getElementById('close-diag-btn').addEventListener('click', () => {
    document.getElementById('diagnostic-pane').classList.add('hidden');
  });

  const tombstoneToggle = document.getElementById('tombstone-toggle');
  tombstoneToggle.addEventListener('change', () => {
    showTombstones = tombstoneToggle.checked;
    allReplicas.forEach(renderReplica);
  });

  const filterBtns = document.querySelectorAll('.filter-btn');
  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentLogFilter = btn.dataset.filter;
      applyLogFiltering();
    });
  });

  renderPresence();
  allReplicas.forEach(updateReplicaRibbon);
}

window.addEventListener('DOMContentLoaded', initApp);

// ==========================================================================
// Systems Verification Diagnostic Test Suite
// ==========================================================================

function runDiagnostics() {
  const diagPane = document.getElementById('diagnostic-pane');
  const logEl = document.getElementById('test-results-log');
  diagPane.classList.remove('hidden');
  logEl.innerHTML = '';
  
  function log(msg, type = 'info') {
    const line = document.createElement('div');
    if (type === 'pass') {
      line.style.color = '#baff00';
      line.style.fontWeight = 'bold';
      line.textContent = `[PASS] ${msg}`;
    } else if (type === 'fail') {
      line.style.color = '#ff6b4a';
      line.style.fontWeight = 'bold';
      line.textContent = `[FAIL] ${msg}`;
    } else {
      line.style.color = '#94a3b8';
      line.textContent = `[INFO] ${msg}`;
    }
    logEl.appendChild(line);
    logEl.scrollTop = logEl.scrollHeight;
  }

  log("Starting RGA CRDT verification suite...\n");
  
  try {
    log("Test 1: Sibling concurrent inserts tie-breaking");
    const r1 = createReplicaObject('X', 'TestX', '#fff');
    const r2 = createReplicaObject('Y', 'TestY', '#fff');
    
    const nodeX = {
      id: { site: 'X', clock: 1 },
      char: 'x',
      deleted: false,
      origin: null
    };
    const nodeY = {
      id: { site: 'Y', clock: 1 },
      char: 'y',
      deleted: false,
      origin: null
    };
    
    integrateNode(r1, nodeX);
    integrateNode(r1, nodeY);
    
    integrateNode(r2, nodeY);
    integrateNode(r2, nodeX);
    
    const text1 = r1.nodes.map(n => n.char).join('');
    const text2 = r2.nodes.map(n => n.char).join('');
    
    if (text1 === text2 && text1 === 'yx') {
      log(`Replica X: "${text1}" | Replica Y: "${text2}"`, 'info');
      log("Tie-breaking converges and matches expected order 'yx'.", 'pass');
    } else {
      log(`Convergence failure. Replica X: "${text1}" | Replica Y: "${text2}"`, 'fail');
    }
  } catch (err) {
    log(`Test 1 threw error: ${err.message}`, 'fail');
  }

  try {
    log("\nTest 2: Out-of-order delete retroactive tombstoning");
    const r = createReplicaObject('Z', 'TestZ', '#fff');
    const targetId = { site: 'A', clock: 100 };
    
    applyDelete(r, targetId);
    log(`Applied delete for A:100 before insert. pendingDeletes contains A:100? ${r.pendingDeletes.has('A:100')}`, 'info');
    
    const insertNode = {
      id: targetId,
      char: 'a',
      deleted: false,
      origin: null
    };
    integrateNode(r, insertNode);
    
    if (r.nodes.length === 1 && r.nodes[0].deleted) {
      log("Insert successfully integrated as tombstone retroactively.", 'pass');
    } else {
      log(`Failed retroactive tombstone. nodes length=${r.nodes.length}, deleted=${r.nodes[0]?.deleted}`, 'fail');
    }
  } catch (err) {
    log(`Test 2 threw error: ${err.message}`, 'fail');
  }

  try {
    log("\nTest 3: Offline conflict simulation and catch-up");
    const rAlice = createReplicaObject('A', 'Alice', '#fff');
    const rBob = createReplicaObject('B', 'Bob', '#fff');
    
    const genesisNode = { id: { site: 'S', clock: 1 }, char: 'H', deleted: false, origin: null };
    integrateNode(rAlice, genesisNode);
    integrateNode(rBob, genesisNode);
    
    const nodeAlice = { id: { site: 'A', clock: 1 }, char: 'A', deleted: false, origin: { site: 'S', clock: 1 } };
    integrateNode(rAlice, nodeAlice);
    
    const nodeBob = { id: { site: 'B', clock: 1 }, char: 'B', deleted: false, origin: { site: 'S', clock: 1 } };
    integrateNode(rBob, nodeBob);
    
    integrateNode(rAlice, nodeBob);
    integrateNode(rBob, nodeAlice);
    
    const textAlice = rAlice.nodes.filter(n => !n.deleted).map(n => n.char).join('');
    const textBob = rBob.nodes.filter(n => !n.deleted).map(n => n.char).join('');
    
    if (textAlice === textBob && textAlice === 'HBA') {
      log(`Alice merged text: "${textAlice}"`, 'info');
      log(`Bob merged text: "${textBob}"`, 'info');
      log("Both replicas converged to the same text 'HBA' successfully.", 'pass');
    } else {
      log(`Convergence mismatch. Alice: "${textAlice}" | Bob: "${textBob}"`, 'fail');
    }
  } catch (err) {
    log(`Test 3 threw error: ${err.message}`, 'fail');
  }
}
