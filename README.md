# RGA-CRDT Collaborative Text Editor Telemetry Dashboard

A high-fidelity, real-time collaborative plain-text editor utilizing a Sequence CRDT—specifically, a Replicated Growable Array (RGA)—engineered in a Model-View-Controller (MVC) local architecture with Node.js/Express, Server-Sent Events (SSE), and SQLite database persistence.

This project is built as a systems-design exercise to make the distributed-systems mechanics, convergence algorithms, and database telemetry visible and correct rather than hidden behind standard text areas.

---

## 🚀 Key Features

* **Commutative Sequence CRDT (RGA)**: Operates on character-level nodes. Resolves sibling insert conflicts deterministically by comparing `(clock, site)` coordinate tuples.
* **Retroactive Tombstones**: Handles network out-of-order execution (e.g., a delete arriving before the insert it targets) by buffering target IDs and applying deletions retroactively as soon as the matching character is integrated.
* **Real-time SSE Streaming**: Decoupled clients (Alice, Bob, Charlie) connect to the central Express server using native Server-Sent Events (SSE) to receive real-time updates and cursor movements.
* **Simulated Network Latency**: Adjustable slider in the control panel to add up to 1500ms of transit delay. Demonstrates how systems diverge during flight and merge cleanly back to `✓ CONVERGED`.
* **Offline Mode & Syncing**: Toggle any replica offline to simulate network partitioning. Local edits are buffered and resolved automatically when reconnected, prompting a `SYNCING...` catch-up state.
* **Pedagogical Debugging Tools**:
  * **Show Deleted Tombstones**: Toggles the visibility of deleted character tombstones (struck-through in red), proving visually that deleted nodes are preserved in the RGA array to maintain ordering.
  * **Ledger Filter**: Filter the scrolling, monospace transaction ledger by user (`ALL`, `ALICE`, `BOB`, `CHARLIE`) to inspect operations in isolation.
* **Geometric Vertical Cursor Navigation**: Caret navigation uses `getBoundingClientRect()` to compute cursor coordinates and geometric center points, projecting vertical movements (`ArrowUp` / `ArrowDown`) accurately to wrap-around lines.
* **In-Browser Test Diagnostics**: Execute a programmatic test suite covering sibling tie-breaking, retroactive tombstoning, and offline merging directly within the dashboard.

---

## 🛠️ MVC Directory Structure

```
├── db.js                   # [MODEL] SQLite Database configuration & schemas
├── server.js               # [CONTROLLER] Express REST API & SSE Real-time Router
├── package.json            # NPM dependencies (Express)
├── .gitignore              # Ignores local databases & node modules
└── public/                 # [VIEW & FRONTEND CLIENT]
    ├── index.html          # HTML Layout (Observability Dashboard View)
    ├── styles.css          # Design System CSS
    └── app.js              # Frontend CRDT Engine & Client State
```

* **Model (`db.js`)**: Manages the SQLite `crdt_ops` table, persisting the append-only transaction ledger on local disks.
* **Controller (`server.js`)**: Receives POST operations, inserts them, and relays them to online clients via Server-Sent Events. Handles ephemeral cursor broadcasts and database wipes.
* **View (`public/`)**: Serves the technical dark-mode dashboard interface and client-side CRDT integration loops.

---

## 📐 RGA CRDT Integration Algorithm

Every character typed is represented as a node:
```json
{
  "id": { "site": "A", "clock": 5 },
  "char": "e",
  "deleted": false,
  "origin": { "site": "A", "clock": 4 }
}
```

When integrating a node:
1. Find the index of the `origin` node (the character immediately to its left at time of creation). If `origin` is null, start index is `-1`.
2. Scan rightward from `insertIdx = originIdx + 1`.
3. If we encounter a sibling node (a node sharing the same `origin`):
   * Compare coordinate tuples: `(clock, site)`.
   * Higher ID wins the left slot (meaning we skip past it, incrementing `insertIdx`). If our node is higher, we stop scanning.
4. If we encounter a descendant of a skipped node (origin index > our origin), we skip past it.
5. If we encounter a node with an origin index < our origin, we stop.
6. Splice the node into the list.

---

## 🏁 Quick Start & Installation

### Prerequisites
* **Node.js**: Version 22.5.0 or higher (required for native `node:sqlite` database binding).

### Installation
1. Clone the repository and navigate to the directory:
   ```bash
   cd crdt-collaborative-text-editor
   ```
2. Install dependencies:
   ```bash
   npm install
   ```

### Running the Application
1. Start the server:
   ```bash
   npm start
   ```

---

## 🧪 Verification & Testing Scenarios

1. **Test Concurrent Inserts**: Type in Alice and Bob concurrently. Watch character integration, real-time logs, and cursor trackers.
2. **Test Offline Merge**:
   * Turn **Charlie** offline.
   * Type in Alice (`OnlineEdit`) and Bob.
   * Type in Charlie (`OfflineEdit`).
   * Turn Charlie online. Observe the yellow `SYNCING...` state as Charlie catch-up and merges histories correctly.
3. **Verify Tombstones**: Delete characters, then check **Show Deleted Tombstones** to see how deletes are treated as tombstones (`deleted = true`) rather than physically spliced out.
