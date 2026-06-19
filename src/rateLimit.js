import { checkAndConsumeDB } from './db.js';

const WINDOW_MS = 60_000;

const buckets = new Map();

/**
 * Sliding-window check-and-consume (in-memory, single-process safe).
 *
 * @param {string} userId
 * @param {number} [nowMs]  - injectable for deterministic unit tests
 * @returns {{ ok: boolean, remaining: number, resetMs: number }}
 */
export function checkAndConsume(userId, nowMs = Date.now()) {
  const rate    = Number(process.env.RATE_LIMIT_PER_MIN || 5);
  const wStart  = nowMs - WINDOW_MS;

  let timestamps = (buckets.get(userId) || []).filter(ts => ts > wStart);

  if (timestamps.length >= rate) {
    // resetMs = when the oldest slot will fall out of the window
    const resetMs = timestamps[0] + WINDOW_MS;
    // Keep bucket updated (expired slots already pruned above)
    buckets.set(userId, timestamps);
    return { ok: false, remaining: 0, resetMs };
  }

  timestamps.push(nowMs);
  buckets.set(userId, timestamps);

  const remaining = rate - timestamps.length;
  // resetMs = when the oldest slot (timestamps[0]) expires
  const resetMs   = timestamps[0] + WINDOW_MS;
  return { ok: true, remaining, resetMs };
}

/**
 * DB-backed atomic rate limit (multi-process safe on the same host).
 * Falls back to in-memory when USE_DB_RATE_LIMIT is not set.
 *
 * @param {string} userId
 * @param {number} [nowMs]
 * @returns {{ ok: boolean, remaining: number, resetMs: number }}
 */
export function checkAndConsumeAtomic(userId, nowMs = Date.now()) {
  const rate = Number(process.env.RATE_LIMIT_PER_MIN || 5);
  if (process.env.USE_DB_RATE_LIMIT === 'true') {
    return checkAndConsumeDB(userId, nowMs, rate);
  }
  return checkAndConsume(userId, nowMs);
}

/**
 * Clear all in-memory buckets.
 * Exposed for test isolation — do NOT call in production paths.
 */
export function _resetBuckets() {
  buckets.clear();
}