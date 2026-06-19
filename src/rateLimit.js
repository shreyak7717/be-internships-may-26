/**
 * rateLimit.js — Per-userId sliding-window rate limiter
 *
 * Algorithm: sliding-window log
 *   Each user entry is an array of request timestamps within the last WINDOW_MS.
 *   On each call, timestamps older than (now - WINDOW_MS) are pruned first.
 *   If the remaining count is below the limit, the request is allowed and the
 *   timestamp is appended.
 *
 * Concurrency safety (single Node.js process):
 *   JavaScript's event loop is single-threaded; the Map read-modify-write is
 *   never interrupted by another incoming request callback, so there is NO
 *   race condition within one process.
 *
 * Multi-instance note:
 *   Each process has its own in-memory Map — rate limits are per-process.
 *   For true cross-instance enforcement, swap to Redis (see SCALE.md):
 *     ZADD / ZREMRANGEBYSCORE / ZCARD in a Lua script for atomic sliding window.
 *
 * Optional DB-backed path:
 *   Set USE_DB_RATE_LIMIT=true to route through the SQLite atomic UPSERT
 *   in db.js — race-free across all processes on the same host.
 */

import { checkAndConsumeDB } from './db.js';

const WINDOW_MS = 60_000;

// In-memory store: userId → sorted array of request timestamps (ms)
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

  // Prune expired timestamps and retrieve active ones
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