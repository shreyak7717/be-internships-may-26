import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new Database(dbPath);

db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('busy_timeout = 5000');

db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       TEXT    NOT NULL,
    type          TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    idempotency_key TEXT  UNIQUE,           -- DB-level uniqueness guarantee
    created_at    INTEGER NOT NULL
  );

  -- Covering index for the GET /v1/signals?userId list query
  CREATE INDEX IF NOT EXISTS idx_user_created ON signals(user_id, created_at DESC);

  -- Rate-limit counters table (per-process for SQLite; see SCALE.md for Redis path)
  CREATE TABLE IF NOT EXISTS rate_buckets (
    user_id    TEXT    PRIMARY KEY,
    window_start INTEGER NOT NULL,
    cnt        INTEGER NOT NULL DEFAULT 0
  );
`);

function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

const RETRYABLE = new Set(['SQLITE_BUSY', 'SQLITE_LOCKED', 'simulated_db_failure']);

export async function withRetry(fn, { maxAttempts = 4, baseMs = 50, maxMs = 1000 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return fn();            
    } catch (err) {
      attempt++;
      const retryable = RETRYABLE.has(err.code) || err.message === 'simulated_db_failure';
      if (!retryable || attempt >= maxAttempts) throw err;

      const ceiling = Math.min(maxMs, baseMs * 2 ** attempt);
      const jitter   = Math.floor(Math.random() * ceiling);
      await new Promise(r => setTimeout(r, jitter));
    }
  }
}

const stmtInsertOrIgnore = db.prepare(
  `INSERT OR IGNORE INTO signals
     (user_id, type, payload, idempotency_key, created_at)
   VALUES (?, ?, ?, ?, ?)`
);

const stmtSelectByIdem = db.prepare(
  `SELECT id,
          user_id          AS userId,
          type,
          payload,
          idempotency_key  AS idempotencyKey,
          created_at       AS createdAt
   FROM signals
   WHERE idempotency_key = ?`
);

const stmtInsert = db.prepare(
  `INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
   VALUES (?, ?, ?, ?, ?)`
);

const stmtList = db.prepare(
  `SELECT id,
          user_id         AS userId,
          type,
          payload,
          idempotency_key AS idempotencyKey,
          created_at      AS createdAt
   FROM signals
   WHERE user_id = ?
   ORDER BY created_at DESC
   LIMIT ?`
);

const upsertAndFetch = db.transaction((userId, type, payload, idemKey, nowMs) => {
  stmtInsertOrIgnore.run(userId, type, String(payload), idemKey, nowMs);
  return stmtSelectByIdem.get(idemKey);
});


export async function insertSignal(userId, type, payload, idemKey, nowMs) {
  return withRetry(() => {
    maybeFail();
    if (idemKey) {
      return upsertAndFetch(userId, type, payload, idemKey, nowMs);
    }
    const info = stmtInsert.run(userId, type, String(payload), null, nowMs);
    return { lastInsertRowid: info.lastInsertRowid, created: true };
  });
}

export async function getByIdemKey(idemKey) {
  return withRetry(() => {
    maybeFail();
    return stmtSelectByIdem.get(idemKey);
  });
}

export async function listSignals(userId, limit) {
  return withRetry(() => {
    maybeFail();
    return stmtList.all(userId, limit);
  });
}

const WINDOW_MS = 60_000;

const stmtRateUpsert = db.prepare(`
  INSERT INTO rate_buckets (user_id, window_start, cnt)
    VALUES (?, ?, 1)
  ON CONFLICT(user_id) DO UPDATE SET
    window_start = CASE WHEN (? - rate_buckets.window_start) >= ${WINDOW_MS}
                        THEN ?
                        ELSE rate_buckets.window_start END,
    cnt          = CASE WHEN (? - rate_buckets.window_start) >= ${WINDOW_MS}
                        THEN 1
                        ELSE rate_buckets.cnt + 1 END
  RETURNING window_start, cnt
`);

export function checkAndConsumeDB(userId, nowMs, rateLimit) {
  try {
    maybeFail();
    const row = stmtRateUpsert.get(userId, nowMs, nowMs, nowMs, nowMs);
    const ok        = row.cnt <= rateLimit;
    const resetMs   = row.window_start + WINDOW_MS;
    const remaining = Math.max(rateLimit - row.cnt, 0);
    return { ok, remaining, resetMs };
  } catch (_) {
    return { ok: true, remaining: rateLimit, resetMs: nowMs + WINDOW_MS };
  }
}