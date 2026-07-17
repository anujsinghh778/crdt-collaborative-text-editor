const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');

// Resolve database path
const dbPath = process.env.DATABASE_PATH || path.join(__dirname, 'data.db');

// Ensure parent directory exists
const dbDir = path.dirname(dbPath);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

const db = new DatabaseSync(dbPath);

console.log(`CRDT Database connected at: ${dbPath}`);

// Initialize CRDT schema
function initDatabase() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS crdt_ops (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      sender TEXT NOT NULL,
      site TEXT,
      clock INTEGER,
      char TEXT,
      origin_site TEXT,
      origin_clock INTEGER,
      target_site TEXT,
      target_clock INTEGER
    );
  `);
}
initDatabase();

module.exports = {
  // CRDT database operations
  getCrdtOps: (sinceSeq) => {
    const stmt = db.prepare(`
      SELECT * FROM crdt_ops 
      WHERE seq > ? 
      ORDER BY seq ASC
    `);
    const rows = stmt.all(sinceSeq || 0);
    return rows.map(row => {
      const op = {
        seq: row.seq,
        type: row.type,
        sender: row.sender
      };
      if (row.type === 'insert') {
        op.node = {
          id: { site: row.site, clock: row.clock },
          char: row.char,
          deleted: false,
          origin: row.origin_site ? { site: row.origin_site, clock: row.origin_clock } : null
        };
      } else if (row.type === 'delete') {
        op.targetId = { site: row.target_site, clock: row.target_clock };
      }
      return op;
    });
  },

  insertCrdtOp: (op) => {
    let stmt;
    let runResult;
    if (op.type === 'insert') {
      stmt = db.prepare(`
        INSERT INTO crdt_ops (type, sender, site, clock, char, origin_site, origin_clock)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      runResult = stmt.run(
        op.type,
        op.sender,
        op.node.id.site,
        op.node.id.clock,
        op.node.char,
        op.node.origin ? op.node.origin.site : null,
        op.node.origin ? op.node.origin.clock : null
      );
    } else if (op.type === 'delete') {
      stmt = db.prepare(`
        INSERT INTO crdt_ops (type, sender, target_site, target_clock)
        VALUES (?, ?, ?, ?)
      `);
      runResult = stmt.run(
        op.type,
        op.sender,
        op.targetId.site,
        op.targetId.clock
      );
    }
    
    const seq = runResult.lastInsertRowid;
    return { ...op, seq };
  },

  resetCrdtDatabase: () => {
    db.exec('DELETE FROM crdt_ops;');
    db.exec('DELETE FROM sqlite_sequence WHERE name = \'crdt_ops\';');
  }
};
