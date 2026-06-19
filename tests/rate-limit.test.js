import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function spawnServer(port, extra = {}) {
  return spawn('node', ['src/server.js'], {
    env: { ...process.env, API_KEY: 'testkey', PORT: String(port), ...extra },
    stdio: 'pipe',
  });
}

/**
 * Poll GET /healthz until the server responds or timeout expires.
 */
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
      await wait(50);
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

async function postStatus(url, { headers = {}, body = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      { method: 'POST', headers: { 'content-type': 'application/json', ...headers } },
      (res) => { res.resume(); res.on('end', () => resolve(res.statusCode)); }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Test 1: Basic — allow RATE_LIMIT_PER_MIN, block on next
// ---------------------------------------------------------------------------
test('rate limit: allow 5 per minute, 6th is 429', async () => {
  const proc = spawnServer(9200, { RATE_LIMIT_PER_MIN: '5' });
  await waitForServer(9200);

  const statuses = [];
  for (let i = 0; i < 6; i++) {
    const code = await postStatus('http://localhost:9200/v1/signals', {
      headers: { 'x-api-key': 'testkey' },
      body: { userId: 'rl-u1', type: 'note', payload: String(i) },
    });
    statuses.push(code);
  }

  const ok      = statuses.filter((s) => s === 201).length;
  const blocked = statuses.filter((s) => s === 429).length;
  assert.ok(ok >= 5,      `Expected ≥5 successful (201), got ${ok}`);
  assert.ok(blocked >= 1, `Expected ≥1 rate-limited (429), got ${blocked}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 2: Different userIds have independent rate limit buckets
// ---------------------------------------------------------------------------
test('rate limit: different userIds have independent quotas', async () => {
  const proc = spawnServer(9201, { RATE_LIMIT_PER_MIN: '2' });
  await waitForServer(9201);

  // Exhaust u1's quota (3 requests, limit=2, so at least one is 429)
  const u1 = await Promise.all([
    postJson('http://localhost:9201/v1/signals', { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '1' } }),
    postJson('http://localhost:9201/v1/signals', { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '2' } }),
    postJson('http://localhost:9201/v1/signals', { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '3' } }),
  ]);
  assert.ok(u1.some((r) => r.status === 429), 'u1 should be rate-limited after 2 requests');

  // u2 must still have a fresh quota
  const u2 = await postJson('http://localhost:9201/v1/signals', {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'rl-ub', type: 't', payload: 'fresh' },
  });
  assert.equal(u2.status, 201, `u2 should not be rate-limited; got ${u2.status}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 3: 429 body shape — remaining=0 and future resetMs
// ---------------------------------------------------------------------------
test('rate limit: 429 body has correct shape', async () => {
  const proc = spawnServer(9202, { RATE_LIMIT_PER_MIN: '2' });
  await waitForServer(9202);

  // Exhaust limit
  await postJson('http://localhost:9202/v1/signals', { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-slide', type: 't', payload: '1' } });
  await postJson('http://localhost:9202/v1/signals', { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-slide', type: 't', payload: '2' } });

  const blocked = await postJson('http://localhost:9202/v1/signals', {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'rl-slide', type: 't', payload: '3' },
  });

  assert.equal(blocked.status, 429, 'Should be rate limited');
  assert.equal(blocked.body.error, 'rate_limited');
  assert.equal(blocked.body.remaining, 0);
  assert.ok(typeof blocked.body.resetMs === 'number', 'resetMs must be a number');
  assert.ok(blocked.body.resetMs > Date.now(), 'resetMs must be in the future');

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 4: Burst — concurrent requests respect the limit
// ---------------------------------------------------------------------------
test('rate limit: burst of concurrent requests — no more than RATE succeed', async () => {
  const RATE = 5;
  const proc = spawnServer(9203, { RATE_LIMIT_PER_MIN: String(RATE) });
  await waitForServer(9203);

  const results = await Promise.all(
    Array.from({ length: 10 }, (_, i) =>
      postJson('http://localhost:9203/v1/signals', {
        headers: { 'x-api-key': 'testkey' },
        body: { userId: 'burst-user', type: 'burst', payload: String(i) },
      })
    )
  );

  const successes = results.filter((r) => r.status === 201).length;
  const blocked   = results.filter((r) => r.status === 429).length;

  assert.ok(successes <= RATE, `No more than ${RATE} requests should succeed; got ${successes}`);
  assert.ok(blocked >= 1,      `At least 1 request should be rate-limited; got ${blocked}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 5: 401 for missing / wrong API key
// ---------------------------------------------------------------------------
test('auth: missing or wrong API key returns 401', async () => {
  const proc = spawnServer(9204);
  await waitForServer(9204);

  const noKey = await postStatus('http://localhost:9204/v1/signals', {
    body: { userId: 'u', type: 't', payload: 'x' },
  });
  assert.equal(noKey, 401, `No key: expected 401, got ${noKey}`);

  const wrongKey = await postStatus('http://localhost:9204/v1/signals', {
    headers: { 'x-api-key': 'wrong' },
    body: { userId: 'u', type: 't', payload: 'x' },
  });
  assert.equal(wrongKey, 401, `Wrong key: expected 401, got ${wrongKey}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 6: Healthz is exempt from auth
// ---------------------------------------------------------------------------
test('healthz: accessible without API key', async () => {
  const proc = spawnServer(9205);
  await waitForServer(9205);

  const status = await new Promise((resolve, reject) => {
    const req = http.get('http://localhost:9205/healthz', (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
  });

  assert.equal(status, 200);
  proc.kill();
});
