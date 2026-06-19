import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';

function spawnServer(port, extra = {}) {
  return spawn('node', ['src/server.js'], {
    env: { ...process.env, API_KEY: 'testkey', PORT: String(port), ...extra },
    stdio: 'pipe',
  });
}

async function waitForServer(port, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await new Promise((resolve, reject) => {
        const req = http.get(`http://localhost:${port}/healthz`, (res) => {
          res.resume();
          res.on('end', resolve);
        });
        req.on('error', reject);
        req.setTimeout(200, () => { req.destroy(); reject(new Error('timeout')); });
      });
      return; // server is up
    } catch {
      await wait(50); // retry after 50 ms
    }
  }
  throw new Error(`Server on port ${port} did not start within ${timeoutMs}ms`);
}

async function postJson(url, { headers = {}, body = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      { method: 'POST', headers: { 'content-type': 'application/json', ...headers } },
      (res) => {
        let raw = '';
        res.on('data', (d) => (raw += d));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(raw || '{}') }));
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

test('idempotency: sequential — same key always returns same id and 200', async () => {
  const proc = spawnServer(9100);
  await waitForServer(9100);

  const hdrs = { 'x-api-key': 'testkey', 'idempotency-key': 'seq-idem-1' };
  const bdyy = { userId: 'u1', type: 'note', payload: 'hello' };

  const a = await postJson('http://localhost:9100/v1/signals', { headers: hdrs, body: bdyy });
  const b = await postJson('http://localhost:9100/v1/signals', { headers: hdrs, body: bdyy });

  assert.equal(a.status, 200, `first: expected 200, got ${a.status} — ${JSON.stringify(a.body)}`);
  assert.equal(b.status, 200, `second: expected 200, got ${b.status} — ${JSON.stringify(b.body)}`);
  assert.equal(a.body.id, b.body.id, 'ids must match');
  assert.equal(a.body.idempotencyKey, 'seq-idem-1');

  proc.kill();
});

test('idempotency: concurrent — 10 parallel requests share the same id, no 503', async () => {
  const proc = spawnServer(9101, { RATE_LIMIT_PER_MIN: '100' });
  await waitForServer(9101);

  const hdrs = { 'x-api-key': 'testkey', 'idempotency-key': 'concurrent-idem-1' };
  const bdyy = { userId: 'u2', type: 'event', payload: 'concurrent' };

  const results = await Promise.all(
    Array.from({ length: 10 }, () =>
      postJson('http://localhost:9101/v1/signals', { headers: hdrs, body: bdyy })
    )
  );

  for (const r of results) {
    assert.ok(
      r.status === 200,
      `Expected 200, got ${r.status} — ${JSON.stringify(r.body)}`
    );
  }

  const ids = new Set(results.map((r) => r.body.id));
  assert.equal(ids.size, 1, `All concurrent requests must return same id; got: ${[...ids]}`);

  proc.kill();
});

test('idempotency: retry with same key bypasses rate limit (not blocked by 429)', async () => {
  const proc = spawnServer(9102, { RATE_LIMIT_PER_MIN: '1' });
  await waitForServer(9102);

  const hdrs = { 'x-api-key': 'testkey', 'idempotency-key': 'rate-bypass-idem' };
  const bdyy = { userId: 'u3', type: 'ping', payload: 'original' };

  const first = await postJson('http://localhost:9102/v1/signals', { headers: hdrs, body: bdyy });
  assert.equal(first.status, 200, `First request failed with ${first.status} — ${JSON.stringify(first.body)}`);

  const retry = await postJson('http://localhost:9102/v1/signals', { headers: hdrs, body: bdyy });
  assert.equal(retry.status, 200, `Retry got ${retry.status} — should bypass rate limit`);
  assert.equal(retry.body.id, first.body.id, 'Retry must return the same resource');

  proc.kill();
});

test('db retry: moderate DB_FAIL_RATE=0.5 — at least 3/5 requests succeed', async () => {
  const proc = spawnServer(9103, { DB_FAIL_RATE: '0.5', RATE_LIMIT_PER_MIN: '100' });
  await waitForServer(9103);

  const results = await Promise.all(
    Array.from({ length: 5 }, (_, i) =>
      postJson('http://localhost:9103/v1/signals', {
        headers: { 'x-api-key': 'testkey' },
        body: { userId: 'retry-user', type: 'test', payload: String(i) },
      })
    )
  );

  const successes = results.filter((r) => r.status === 200 || r.status === 201).length;
  assert.ok(successes >= 3, `Expected ≥3 successes, got ${successes}`);

  proc.kill();
});

test('no idempotency key: distinct records created for each request', async () => {
  const proc = spawnServer(9104, { RATE_LIMIT_PER_MIN: '100' });
  await waitForServer(9104);

  const a = await postJson('http://localhost:9104/v1/signals', {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'u5', type: 'note', payload: 'first' },
  });
  const b = await postJson('http://localhost:9104/v1/signals', {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'u5', type: 'note', payload: 'second' },
  });

  assert.equal(a.status, 201, `Expected 201, got ${a.status} — ${JSON.stringify(a.body)}`);
  assert.equal(b.status, 201, `Expected 201, got ${b.status} — ${JSON.stringify(b.body)}`);
  assert.notEqual(a.body.id, b.body.id, 'Keyless requests must produce distinct ids');

  proc.kill();
});

test('GET /v1/signals returns items for userId', async () => {
  const proc = spawnServer(9105, { RATE_LIMIT_PER_MIN: '100' });
  await waitForServer(9105);

  await postJson('http://localhost:9105/v1/signals', {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'getuser', type: 'click', payload: 'btn' },
  });

  const res = await new Promise((resolve, reject) => {
    const req = http.request(
      'http://localhost:9105/v1/signals?userId=getuser',
      { headers: { 'x-api-key': 'testkey' } },
      (res) => {
        let raw = '';
        res.on('data', (d) => (raw += d));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(raw || '{}') }));
      }
    );
    req.on('error', reject);
    req.end();
  });

  assert.equal(res.status, 200);
  assert.ok(Array.isArray(res.body.items), 'items must be an array');
  assert.ok(res.body.items.length >= 1, 'must have at least 1 item');

  proc.kill();
});
