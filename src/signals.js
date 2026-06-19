import { insertSignal, getByIdemKey, listSignals } from './db.js';
import { checkAndConsume } from './rateLimit.js';

function nowMs() { return Date.now(); }

export async function postSignal(req, reply) {
  const idem = req.headers['idempotency-key'] || null;
  const { userId, type, payload } = req.body || {};

  if (!userId || !type || typeof payload === 'undefined') {
    return reply.code(400).send({ error: 'invalid_body' });
  }

  if (idem) {
    try {
      const existing = await getByIdemKey(idem);
      if (existing) {
        return reply.code(200).send(existing);
      }
    } catch (e) {
      req.log.error({ err: e, ctx: 'getByIdemKey-pre-check' });
      return reply.code(503).send({ error: 'db_unavailable' });
    }
  }

  const { ok, remaining, resetMs } = checkAndConsume(userId, nowMs());
  if (!ok) {
    return reply.code(429).send({ error: 'rate_limited', remaining, resetMs });
  }

  const t = nowMs();
  try {
    const result = await insertSignal(userId, type, payload, idem, t);

    if (idem) {
      return reply.code(200).send(result);
    }

    return reply.code(201).send({
      id:             Number(result.lastInsertRowid),
      userId,
      type,
      payload:        String(payload),
      idempotencyKey: null,
      createdAt:      t,
    });
  } catch (e) {
    req.log.error({ err: e, ctx: 'insertSignal' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}

export async function getSignals(req, reply) {
  const { userId, limit = 20 } = req.query || {};
  if (!userId) return reply.code(400).send({ error: 'missing_userId' });
  const lim = Math.min(Number(limit) || 20, 100);
  try {
    const rows = await listSignals(userId, lim);
    return reply.code(200).send({ items: rows });
  } catch (e) {
    req.log.error({ err: e, ctx: 'listSignals' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}