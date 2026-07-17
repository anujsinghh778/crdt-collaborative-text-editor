const express = require('express');
const path = require('path');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

let sseClients = [];

function broadcastToSse(message, excludeSiteId = null) {
  const payload = `data: ${JSON.stringify(message)}\n\n`;
  sseClients.forEach(client => {
    if (!excludeSiteId || client.siteId !== excludeSiteId) {
      try {
        client.res.write(payload);
      } catch (err) {
        // SSE connection might have closed
      }
    }
  });
}

// Keep SSE streams alive
setInterval(() => {
  sseClients.forEach(client => {
    try {
      client.res.write(': keepalive\n\n');
    } catch (err) {
      // ignore
    }
  });
}, 20000);

// Seed CRDT Genesis Document
function seedGenesisDoc() {
  db.resetCrdtDatabase();
  
  const text = "Welcome to the Collaborative CRDT Editor!\n\nThis is an observability dashboard showing how conflict-free text replication (RGA) works under the hood.\n\nQuick steps to test:\n1. Type concurrently in Alice and Bob.\n2. Toggle Charlie offline, make edits in Charlie, edit elsewhere, and reconnect.\n3. Watch the sync log below to see sequence numbers and node ties resolving.\n";
  
  let lastId = null;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    const clock = i + 1;
    const nodeId = { site: 'A', clock };
    const op = {
      type: 'insert',
      sender: 'A',
      node: {
        id: nodeId,
        char,
        deleted: false,
        origin: lastId
      }
    };
    db.insertCrdtOp(op);
    lastId = nodeId;
  }
}

// Seed on startup if database is empty
const initialOps = db.getCrdtOps(0);
if (initialOps.length === 0) {
  console.log('Seeding initial CRDT genesis document...');
  seedGenesisDoc();
}

// GET /api/crdt/stream - SSE connection endpoint
app.get('/api/crdt/stream', (req, res) => {
  const siteId = req.query.siteId;
  if (!siteId) {
    return res.status(400).json({ error: 'siteId query parameter is required' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders(); // Establish stream

  const client = { siteId, res };
  sseClients.push(client);
  
  console.log(`Site ${siteId} connected to real-time stream.`);
  res.write(`data: ${JSON.stringify({ type: 'status', message: 'connected' })}\n\n`);

  req.on('close', () => {
    sseClients = sseClients.filter(c => c !== client);
    console.log(`Site ${siteId} disconnected.`);
  });
});

// GET /api/crdt/ops - Fetch operations for catch-up syncing
app.get('/api/crdt/ops', (req, res) => {
  try {
    const since = parseInt(req.query.since || 0, 10);
    const ops = db.getCrdtOps(since);
    res.json({ status: 'success', data: ops });
  } catch (error) {
    console.error('Error fetching CRDT ops:', error);
    res.status(500).json({ status: 'error', error: 'Failed to fetch ops' });
  }
});

// POST /api/crdt/ops - Submit a new operation
app.post('/api/crdt/ops', (req, res) => {
  try {
    const op = req.body;
    if (!op || !op.type || !op.sender) {
      return res.status(400).json({ status: 'error', error: 'Invalid operation format' });
    }
    const insertedOp = db.insertCrdtOp(op);
    
    // Broadcast real-time update to all other online replicas
    broadcastToSse({ type: 'op', op: insertedOp }, op.sender);
    
    res.status(201).json({ status: 'success', data: insertedOp });
  } catch (error) {
    console.error('Error inserting CRDT op:', error);
    res.status(500).json({ status: 'error', error: 'Failed to record operation' });
  }
});

// POST /api/crdt/cursor - Ephemeral cursor broadcast
app.post('/api/crdt/cursor', (req, res) => {
  try {
    const { siteId, posId } = req.body;
    if (!siteId) {
      return res.status(400).json({ status: 'error', error: 'siteId is required' });
    }
    
    // Broadcast cursor to other online replicas
    broadcastToSse({ type: 'cursor', siteId, posId }, siteId);
    
    res.json({ status: 'success' });
  } catch (error) {
    console.error('Error broadcasting cursor:', error);
    res.status(500).json({ status: 'error', error: 'Failed to broadcast cursor' });
  }
});

// POST /api/crdt/reset - Reset the DB and re-seed the genesis document
app.post('/api/crdt/reset', (req, res) => {
  try {
    console.log('Resetting CRDT database and seeding genesis document...');
    seedGenesisDoc();
    
    // Notify all connected clients to reload/reset
    broadcastToSse({ type: 'reset' });
    
    res.json({ status: 'success', message: 'CRDT database reset and seeded successfully' });
  } catch (error) {
    console.error('Error resetting CRDT database:', error);
    res.status(500).json({ status: 'error', error: 'Failed to reset database' });
  }
});

// Fallback to frontend index.html
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start Server
app.listen(PORT, () => {
  console.log(`==================================================`);
  console.log(`  Collaborative CRDT Editor server running locally`);
  console.log(`  Access it here: http://localhost:${PORT}`);
  console.log(`==================================================`);
});
