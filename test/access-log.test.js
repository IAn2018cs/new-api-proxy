const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { once } = require('node:events');
const { test } = require('node:test');
const { createAccessLogger, formatTimestamp } = require('../access-log');
const { createProxyServer } = require('../index');

async function tempDirectory(t) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'api-proxy-logs-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  return directory;
}

async function readRecords(directory) {
  const files = (await fs.readdir(directory)).filter((name) => /^access-.*\.log$/.test(name)).sort();
  const records = [];
  for (const name of files) {
    const contents = await fs.readFile(path.join(directory, name), 'utf8');
    for (const line of contents.trim().split('\n')) if (line) records.push(JSON.parse(line));
  }
  return records;
}

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

function request(port, url = '/', method = 'GET') {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: url, method, agent: false,
      headers: { authorization: 'Bearer secret-header', 'x-api-key': 'secret-key' } }, (res) => {
      res.resume();
      res.on('error', reject);
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
    req.end();
  });
}

async function proxyFixture(t, directory, handler = (_req, res) => res.end('ok'), loggerOptions = {}) {
  const upstream = http.createServer(handler);
  const sockets = new Set();
  upstream.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
  });
  const upstreamPort = await listen(upstream);
  const logger = createAccessLogger({ directory, ...loggerOptions });
  const proxy = createProxyServer({ target: `http://127.0.0.1:${upstreamPort}`,
    apiKey: '', accessLogger: logger, shutdownTimeout: 50 });
  const port = await listen(proxy);
  const closed = once(proxy, 'close');
  t.after(async () => {
    proxy.shutdown();
    await closed;
    await proxy.flushAccessLogs();
    for (const socket of sockets) socket.destroy();
    if (upstream.listening) await new Promise((resolve) => upstream.close(resolve));
  });
  return { port, proxy, logger, upstream, sockets };
}

test('daily files rotate at Shanghai midnight and append on restart', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const today = formatTimestamp().slice(0, 10);
  const midnight = Date.parse(`${today}T00:00:00+08:00`);
  const yesterday = formatTimestamp(midnight - 1).slice(0, 10);
  const logger = createAccessLogger({ directory });
  logger.write({ at: midnight - 1, event: 'start', fields: { method: 'GET', path: '/one' } });
  logger.write({ at: midnight, event: 'complete', fields: { method: 'GET', path: '/one' }, status: 200 });
  await logger.close();
  assert.deepEqual((await fs.readdir(directory)).sort(), [`access-${yesterday}.log`, `access-${today}.log`].sort());
  const records = await readRecords(directory);
  assert.equal(records[0].time, `${yesterday}T23:59:59.999+08:00`);
  assert.equal(records[1].time, `${today}T00:00:00.000+08:00`);
  const restarted = createAccessLogger({ directory });
  restarted.write({ at: midnight + 1, event: 'start', fields: { path: '/two' } });
  await restarted.close();
  assert.equal((await readRecords(directory)).length, 3);
});

test('retains today and 89 preceding dates, preserving unrelated files and directories', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const today = formatTimestamp().slice(0, 10);
  const midnight = Date.parse(`${today}T00:00:00+08:00`);
  const days = Array.from({ length: 92 }, (_, i) => formatTimestamp(midnight - i * 86400000).slice(0, 10));
  await Promise.all(days.map((day) => fs.writeFile(path.join(directory, `access-${day}.log`), '')));
  await fs.writeFile(path.join(directory, 'other-2000-01-01.log'), 'keep');
  await fs.mkdir(path.join(directory, 'access-1999-01-01.log'));
  const logger = createAccessLogger({ directory });
  await logger.close();
  const names = await fs.readdir(directory);
  const owned = names.filter((name) => /^access-/.test(name) && name !== 'access-1999-01-01.log').sort();
  assert.deepEqual(owned, days.slice(0, 90).map((day) => `access-${day}.log`).sort());
  assert.equal(await fs.readFile(path.join(directory, 'other-2000-01-01.log'), 'utf8'), 'keep');
  assert.equal((await fs.stat(path.join(directory, 'access-1999-01-01.log'))).isDirectory(), true);
});

test('bounded queues discard overflow without blocking and flush accepted entries', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const logger = createAccessLogger({ directory, maxPending: 2 });
  assert.equal(logger.write({ at: Date.now(), event: 'start', fields: { path: '/one' } }), true);
  assert.equal(logger.write({ at: Date.now(), event: 'start', fields: { path: '/two' } }), true);
  assert.equal(logger.write({ at: Date.now(), event: 'start', fields: { path: '/overflow' } }), false);
  await logger.close();
  assert.deepEqual((await readRecords(directory)).map((record) => record.path), ['/one', '/two']);
});

test('records starts and completions, local OPTIONS, error status, time and duration without credentials', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const { port, logger } = await proxyFixture(t, directory, (req, res) => {
    if (req.url.startsWith('/reset')) req.socket.resetAndDestroy();
    else res.end('ok');
  });
  assert.equal(await request(port, '/v1/models?api_key=secret-query'), 200);
  assert.equal(await request(port, '/preflight', 'OPTIONS'), 204);
  // POST must not retry a reset on the already pooled socket.
  assert.equal(await request(port, '/reset?token=secret-query', 'POST'), 502);
  await logger.close();
  const records = await readRecords(directory);
  assert.equal(records.length, 6);
  const complete = records.filter((record) => record.event === 'complete');
  assert.deepEqual(complete.map((record) => record.status), [200, 204, 502]);
  assert.equal(complete[2].error, 'ECONNRESET');
  for (const record of complete) {
    assert.match(record.time, /\+08:00$/);
    assert.equal(record.client_ip, '127.0.0.1');
    assert.ok(record.duration_ms >= 0);
    assert.ok(records.some((start) => start.event === 'start' && start.request_id === record.request_id));
  }
  assert.doesNotMatch(JSON.stringify(records), /secret|api_key|authorization/);
});

test('SSE starts are visible before completion; cancellations log once', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const { port, logger } = await proxyFixture(t, directory, (_req, res) => res.write('data: first\n\n'));
  let stream;
  await new Promise((resolve, reject) => {
    http.get({ hostname: '127.0.0.1', port, agent: false }, (res) => {
      stream = res;
      res.on('error', () => {});
      res.once('data', resolve);
    }).on('error', reject);
  });
  t.after(() => stream.destroy());
  // Allow an async batch to reach disk while the response is still open.
  const deadline = Date.now() + 2000;
  let records = [];
  while (!records.length && Date.now() < deadline) {
    records = await readRecords(directory);
    if (!records.length) await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.equal(records.length, 1);
  assert.equal(records[0].event, 'start');
  assert.equal(stream.complete, false);
  stream.destroy();
  await new Promise((resolve) => setTimeout(resolve, 20));
  await logger.close();
  records = await readRecords(directory);
  assert.equal(records.length, 2);
  assert.equal(records[1].event, 'aborted');
  assert.equal(records[1].error, 'CLIENT_ABORTED');
});

test('WebSocket handshake and closure are logged alongside the initial access', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const { port, upstream, logger } = await proxyFixture(t, directory);
  let peer;
  upstream.on('upgrade', (_req, socket) => {
    peer = socket;
    socket.write('HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n');
    socket.on('end', () => socket.end());
    socket.resume();
  });
  const client = net.connect(port, '127.0.0.1');
  client.on('error', () => {});
  t.after(() => client.destroy());
  await once(client, 'connect');
  const handshake = once(client, 'data');
  client.write('GET /ws?token=secret HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n');
  await handshake;
  const closed = new Promise((resolve) => peer.once('close', resolve));
  client.resetAndDestroy();
  await closed;
  await logger.close();
  const records = await readRecords(directory);
  assert.deepEqual(records.map((record) => record.event), ['start', 'upgrade', 'close']);
  assert.equal(records[1].status, 101);
  assert.equal(records[2].status, 101);
  assert.ok(records.every((record) => record.protocol === 'websocket' && record.path === '/ws'));
});

test('an unwritable log destination cannot prevent forwarding or bounded shutdown', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const blocked = path.join(directory, 'blocked');
  await fs.writeFile(blocked, 'not a directory');
  const { port, proxy, logger } = await proxyFixture(t, blocked, undefined, { closeTimeout: 150 });
  assert.equal(await request(port), 200);
  await logger.close();
  assert.equal(await request(port), 200);
  const closed = once(proxy, 'close');
  proxy.shutdown();
  await closed;
});

test('logger retries file failures and recovers after the destination is repaired', { timeout: 5000 }, async (t) => {
  const directory = await tempDirectory(t);
  const blocked = path.join(directory, 'blocked');
  await fs.writeFile(blocked, 'not a directory');
  const logger = createAccessLogger({ directory: blocked });
  t.after(() => logger.close());
  logger.write({ at: Date.now(), event: 'start', fields: { path: '/before' } });
  // Give the first batch time to fail before fixing the destination.
  await new Promise((resolve) => setTimeout(resolve, 250));
  await fs.unlink(blocked);
  await fs.mkdir(blocked);
  logger.write({ at: Date.now(), event: 'start', fields: { path: '/after' } });
  const deadline = Date.now() + 2500;
  let records = [];
  while (!records.some((record) => record.path === '/after') && Date.now() < deadline) {
    records = await readRecords(blocked);
    if (!records.length) await new Promise((resolve) => setTimeout(resolve, 30));
  }
  assert.ok(records.some((record) => record.path === '/after'));
  await logger.close();
});
