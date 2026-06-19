import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new Database(dbPath);

// WAL mode: readers don't block writers, better concurrency for SQLite
db.pragma('journal_mode = WAL');
// Enforce foreign keys and tighten sync for crash safety without full fsync cost
db.pragma('synchronous = NORMAL');
// Increase busy timeout so concurrent writes queue rather than immediately fail
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

// ---------------------------------------------------------------------------
// Failure simulation (set DB_FAIL_RATE=0.3 for 30% failure rate)
// ---------------------------------------------------------------------------
function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Retry / back-off with full jitter
// Retryable SQLite codes: SQLITE_BUSY, SQLITE_LOCKED
// ---------------------------------------------------------------------------
const RETRYABLE = new Set(['SQLITE_BUSY', 'SQLITE_LOCKED', 'simulated_db_failure']);

export async function withRetry(fn, { maxAttempts = 4, baseMs = 50, maxMs = 1000 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return fn();            // better-sqlite3 is synchronous
    } catch (err) {
      attempt++;
      const retryable = RETRYABLE.has(err.code) || err.message === 'simulated_db_failure';
      if (!retryable || attempt >= maxAttempts) throw err;

      // Full-jitter exponential back-off: sleep in [0, min(cap, base * 2^attempt)]
      const ceiling = Math.min(maxMs, baseMs * 2 ** attempt);
      const jitter   = Math.floor(Math.random() * ceiling);
      await new Promise(r => setTimeout(r, jitter));
    }
  }
}

// ---------------------------------------------------------------------------
// Atomic upsert for idempotency
//
// Strategy: INSERT OR IGNORE on the UNIQUE(idempotency_key) constraint.
// If a row already exists the INSERT is a no-op (no error, changes = 0).
// We then SELECT to return the canonical row in both the new and duplicate case.
// This is a single round-trip and race-free: two concurrent requests with the
// same key will both hit the INSERT; exactly one wins, the other silently loses,
// and both then read back the same persisted row.
// ---------------------------------------------------------------------------
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

// Wrapped in a serializable transaction so INSERT + SELECT are atomic
const upsertAndFetch = db.transaction((userId, type, payload, idemKey, nowMs) => {
  stmtInsertOrIgnore.run(userId, type, String(payload), idemKey, nowMs);
  return stmtSelectByIdem.get(idemKey);
});

/**
 * insertSignal
 *
 * When idemKey is provided: uses atomic INSERT OR IGNORE + SELECT so concurrent
 * callers with the same key all receive the same persisted row without duplicates.
 *
 * When idemKey is null: plain INSERT, returns { lastInsertRowid }.
 */
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

// ---------------------------------------------------------------------------
// Atomic rate-limit increment using SQLite as the counter store.
//
// Single UPSERT per request:
//   • If no row yet → insert (window_start=now, cnt=1)
//   • If within the same window  → increment cnt
//   • If window expired          → reset to (window_start=now, cnt=1)
//
// Because SQLite serialises writes, this is race-free within one process.
// For multi-process / multi-instance safety swap to a Redis INCR + EXPIRE
// (see SCALE.md).
// ---------------------------------------------------------------------------
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
  // Synchronous — intentionally not wrapped in withRetry; if the rate table
  // is unavailable we fail open (allow the request) rather than DoS the user.
  try {
    maybeFail();
    const row = stmtRateUpsert.get(userId, nowMs, nowMs, nowMs, nowMs);
    const ok        = row.cnt <= rateLimit;
    const resetMs   = row.window_start + WINDOW_MS;
    const remaining = Math.max(rateLimit - row.cnt, 0);
    return { ok, remaining, resetMs };
  } catch (_) {
    // Fail open on rate-limit DB errors — don't block legitimate traffic
    return { ok: true, remaining: rateLimit, resetMs: nowMs + WINDOW_MS };
  }
}