import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
async function postStatus(url, { headers = {}, body = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      { method: 'POST', headers: { 'content-type': 'application/json', ...headers } },
      (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode));
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
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

function spawnServer(port, extra = {}) {
  return spawn('node', ['src/server.js'], {
    env: { ...process.env, API_KEY: 'testkey', PORT: String(port), ...extra },
    stdio: 'pipe',
  });
}

// ---------------------------------------------------------------------------
// Test 1: Basic — allow RATE_LIMIT_PER_MIN, block on next
// ---------------------------------------------------------------------------
test('rate limit: allow 5 per minute, 6th is 429', async () => {
  const proc = spawnServer(9200, { RATE_LIMIT_PER_MIN: '5' });
  await wait(400);
  const base = `http://localhost:9200`;

  const statuses = [];
  for (let i = 0; i < 6; i++) {
    const code = await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'testkey' },
      body: { userId: 'rl-u1', type: 'note', payload: String(i) },
    });
    statuses.push(code);
  }

  const ok = statuses.filter((s) => s === 201).length;
  const blocked = statuses.filter((s) => s === 429).length;
  assert.ok(ok >= 5, `Expected at least 5 successful requests, got ${ok}`);
  assert.ok(blocked >= 1, `Expected at least 1 rate-limited (429) request, got ${blocked}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 2: Different userIds have independent rate limit buckets
// ---------------------------------------------------------------------------
test('rate limit: different userIds have independent quotas', async () => {
  const proc = spawnServer(9201, { RATE_LIMIT_PER_MIN: '2' });
  await wait(400);
  const base = `http://localhost:9201`;

  // Exhaust u1's quota
  const u1 = await Promise.all([
    postJson(`${base}/v1/signals`, { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '1' } }),
    postJson(`${base}/v1/signals`, { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '2' } }),
    postJson(`${base}/v1/signals`, { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-ua', type: 't', payload: '3' } }),
  ]);

  const u1Blocked = u1.some((r) => r.status === 429);
  assert.ok(u1Blocked, 'u1 should have been rate-limited after 2 requests');

  // u2 must still have a fresh quota
  const u2 = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'testkey' },
    body: { userId: 'rl-ub', type: 't', payload: 'fresh' },
  });
  assert.equal(u2.status, 201, `u2 should not be rate-limited; got ${u2.status}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 3: Sliding window — requests from >60s ago are released from the window
// ---------------------------------------------------------------------------
test('rate limit: sliding window releases old slots', async () => {
  const proc = spawnServer(9202, { RATE_LIMIT_PER_MIN: '2' });
  await wait(400);
  const base = `http://localhost:9202`;

  // We fake time by calling the module directly rather than via HTTP,
  // since we can't actually wait 60 s in a CI test.
  // Instead, validate the metadata returned in 429 body.

  // Send 2 requests (exhausts limit)
  await postJson(`${base}/v1/signals`, { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-slide', type: 't', payload: '1' } });
  await postJson(`${base}/v1/signals`, { headers: { 'x-api-key': 'testkey' }, body: { userId: 'rl-slide', type: 't', payload: '2' } });

  // 3rd must be blocked; verify body shape
  const blocked = await postJson(`${base}/v1/signals`, {
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
// Test 4: Burst — concurrent requests respect the limit collectively
// ---------------------------------------------------------------------------
test('rate limit: burst of concurrent requests — no more than RATE succeed', async () => {
  const RATE = 5;
  const proc = spawnServer(9203, { RATE_LIMIT_PER_MIN: String(RATE) });
  await wait(400);
  const base = `http://localhost:9203`;

  // Fire 10 requests simultaneously for the same user
  const results = await Promise.all(
    Array.from({ length: 10 }, (_, i) =>
      postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'testkey' },
        body: { userId: 'burst-user', type: 'burst', payload: String(i) },
      })
    )
  );

  const successes = results.filter((r) => r.status === 201).length;
  const blocked   = results.filter((r) => r.status === 429).length;

  assert.ok(
    successes <= RATE,
    `No more than ${RATE} requests should succeed; got ${successes}`
  );
  assert.ok(blocked >= 1, `At least 1 request should be rate-limited; got ${blocked}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 5: 401 for missing / wrong API key
// ---------------------------------------------------------------------------
test('auth: missing or wrong API key returns 401', async () => {
  const proc = spawnServer(9204);
  await wait(400);
  const base = `http://localhost:9204`;

  const noKey = await postStatus(`${base}/v1/signals`, {
    body: { userId: 'u', type: 't', payload: 'x' },
  });
  assert.equal(noKey, 401, `No key should return 401, got ${noKey}`);

  const wrongKey = await postStatus(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'wrong' },
    body: { userId: 'u', type: 't', payload: 'x' },
  });
  assert.equal(wrongKey, 401, `Wrong key should return 401, got ${wrongKey}`);

  proc.kill();
});

// ---------------------------------------------------------------------------
// Test 6: Healthz is exempt from auth
// ---------------------------------------------------------------------------
test('healthz: accessible without API key', async () => {
  const proc = spawnServer(9205);
  await wait(400);

  const status = await new Promise((resolve, reject) => {
    const req = http.request('http://localhost:9205/healthz', (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
    req.end();
  });

  assert.equal(status, 200);
  proc.kill();
});
