const assert = require('node:assert/strict');
const http = require('node:http');
const net = require('node:net');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { test: nodeTest } = require('node:test');
const { createProxyServer } = require('../index');

function test(name, options, fn) {
  if (typeof options === 'function') return nodeTest(name, { timeout: 5000 }, options);
  return nodeTest(name, { timeout: 5000, ...options }, fn);
}

async function listen(server, port = 0) {
  server.listen(port, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function fixture(t, handler = (_req, res) => res.end('ok'), options = {}) {
  const upstream = http.createServer(handler);
  const sockets = new Set();
  upstream.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
  });
  const upstreamPort = await listen(upstream);
  const proxy = createProxyServer({
    target: `http://127.0.0.1:${upstreamPort}`, apiKey: '',
    upstreamProxyUrl: '',
    connectTimeout: 1000, upstreamTimeout: 1000,
    freeSocketTimeout: 500, shutdownTimeout: 50, ...options,
    accessLogger: options.accessLogger || null,
  });
  const port = await listen(proxy);
  const proxyClosed = once(proxy, 'close');
  t.after(async () => {
    proxy.shutdown();
    await proxyClosed;
    await proxy.flushAccessLogs();
    for (const socket of sockets) socket.destroy();
    if (upstream.listening) await new Promise((resolve) => upstream.close(resolve));
  });
  return { upstream, upstreamPort, proxy, port, sockets };
}

function request(port, url = '/', { body, onResponse, onData, ...options } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: url, agent: false, ...options }, (res) => {
      const chunks = [];
      onResponse?.(res);
      res.on('data', (chunk) => {
        chunks.push(chunk);
        onData?.(chunk, res);
      });
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.end(body);
  });
}

function readUntil(socket, predicate) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const timer = setTimeout(() => finish(new Error('Socket response timed out')), 2000);
    const finish = (error, buffer) => {
      clearTimeout(timer);
      socket.removeListener('data', onData);
      socket.removeListener('error', onError);
      socket.removeListener('end', onEnd);
      if (error) reject(error);
      else resolve(buffer);
    };
    const onData = (chunk) => {
      chunks.push(chunk);
      const buffer = Buffer.concat(chunks);
      if (predicate(buffer)) finish(null, buffer);
    };
    const onError = (err) => finish(err);
    const onEnd = () => {
      const buffer = Buffer.concat(chunks);
      if (predicate(buffer)) finish(null, buffer);
      else finish(new Error('Socket closed before the expected response'));
    };
    socket.on('data', onData);
    socket.on('error', onError);
    socket.on('end', onEnd);
  });
}

const upgradeHeaders = 'GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n';
const switchingProtocols = 'HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n';

async function connect(t, port) {
  const socket = net.connect(port, '127.0.0.1');
  socket.on('error', () => {});
  t.after(() => socket.destroy());
  await once(socket, 'connect');
  return socket;
}

async function healthy(port) {
  const res = await request(port, '/healthy');
  assert.equal(res.status, 200);
  assert.equal(res.body.toString(), 'ok');
}

test('forwards binary bodies, paths, status, duplicate cookies and auth overrides', { timeout: 4000 }, async (t) => {
  const body = Buffer.from([0, 1, 255, 128, 3]);
  const { port, upstreamPort } = await fixture(t, (req, res) => {
    assert.equal(req.method, 'POST');
    assert.equal(req.url, '/v1/chat/completions?stream=true');
    assert.equal(req.headers.host, `127.0.0.1:${upstreamPort}`);
    assert.equal(req.headers.authorization, 'Bearer replacement');
    assert.equal(req.headers['x-api-key'], 'replacement');
    assert.equal(req.headers['api-key'], 'replacement');
    assert.equal(req.headers['x-hop-only'], undefined);
    assert.equal(req.headers['proxy-authorization'], undefined);
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      assert.deepEqual(Buffer.concat(chunks), body);
      res.writeHead(201, { 'Set-Cookie': ['a=1', 'b=2'], 'X-Upstream': 'yes' });
      res.end(body);
    });
  }, { apiKey: 'replacement' });
  const res = await request(port, '/v1/chat/completions?stream=true', {
    method: 'POST', body,
    headers: { authorization: 'Bearer client', 'x-api-key': 'client', 'api-key': 'client',
      connection: 'close, x-hop-only', 'x-hop-only': 'remove', 'proxy-authorization': 'remove' },
  });
  assert.equal(res.status, 201);
  assert.deepEqual(res.body, body);
  assert.deepEqual(res.headers['set-cookie'], ['a=1', 'b=2']);
  assert.equal(res.headers['x-upstream'], 'yes');
  assert.equal(res.headers['access-control-allow-origin'], '*');
});

for (const apiKey of ['', 'replacement']) {
  test(`auth ${apiKey ? 'injection' : 'passthrough'} and local CORS preflight`, async (t) => {
    let calls = 0;
    const { port } = await fixture(t, (req, res) => {
      calls++;
      res.end(req.headers.authorization || 'absent');
    }, { apiKey });
    assert.equal((await request(port)).body.toString(), apiKey ? 'Bearer replacement' : 'absent');
    assert.equal((await request(port, '/', { headers: { authorization: 'Bearer client' } })).body.toString(), apiKey ? 'Bearer replacement' : 'Bearer client');
    const preflight = await request(port, '/', { method: 'OPTIONS' });
    assert.equal(preflight.status, 204);
    assert.equal(calls, 2);
  });
}

test('reuses upstream connections even when clients send Connection: close', async (t) => {
  const { port, upstream } = await fixture(t);
  let connections = 0;
  upstream.on('connection', () => connections++);
  for (let i = 0; i < 10; i++) await healthy(port);
  assert.equal(connections, 1);
});

test('unused upstream connections expire and are replaced on the next request', async (t) => {
  const { port, upstream, sockets } = await fixture(t, undefined, { freeSocketTimeout: 40 });
  let connections = 0;
  upstream.on('connection', () => connections++);
  await healthy(port);
  await new Promise((resolve) => [...sockets][0].once('close', resolve));
  await healthy(port);
  assert.equal(connections, 2);
});

test('a failed retry stops after one replay', async (t) => {
  let calls = 0;
  const { port } = await fixture(t, (req, res) => {
    if (++calls === 1) res.end('ok');
    else req.socket.resetAndDestroy();
  });
  await healthy(port);
  assert.equal((await request(port)).status, 502);
  assert.equal(calls, 3);
});

for (const method of ['GET', 'HEAD']) {
  test(`retries a reset pooled ${method} once on a new connection`, async (t) => {
    let calls = 0;
    const { port } = await fixture(t, (req, res) => {
      calls++;
      if (calls === 2) req.socket.resetAndDestroy();
      else res.end('ok');
    });
    await healthy(port);
    assert.equal((await request(port, '/', { method })).status, 200);
    assert.equal(calls, 3);
  });
}

for (const method of ['POST', 'GET']) {
  test(`never replays a ${method} with a body after a pooled reset`, async (t) => {
    let calls = 0;
    const { port } = await fixture(t, (req, res) => {
      calls++;
      if (calls === 2) req.socket.resetAndDestroy();
      else res.end('ok');
    });
    await healthy(port);
    const res = await request(port, '/', { method, body: 'payload', headers: { 'content-length': 7 } });
    assert.equal(res.status, 502);
    assert.equal(calls, 2);
    await healthy(port);
  });
}

test('a first-connection reset is not retried, and service remains available', async (t) => {
  let calls = 0;
  const { port } = await fixture(t, (req, res) => {
    calls++;
    if (calls === 1) req.socket.resetAndDestroy();
    else res.end('ok');
  });
  assert.equal((await request(port)).status, 502);
  assert.equal(calls, 1);
  await healthy(port);
});

test('connection refusal returns 502 and recovers when upstream returns', async (t) => {
  const { port, upstream, upstreamPort } = await fixture(t);
  await new Promise((resolve) => upstream.close(resolve));
  const res = await request(port);
  assert.equal(res.status, 502);
  assert.equal(res.headers['access-control-allow-origin'], '*');
  await listen(upstream, upstreamPort);
  await healthy(port);
});

test('an upstream reset after response headers terminates the stream cleanly', async (t) => {
  let payload = '';
  const { port, sockets } = await fixture(t, (req, res) => {
    if (req.url === '/healthy') return res.end('ok');
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: first\n\n');
  });
  await assert.rejects(request(port, '/stream', {
    onData: (chunk) => {
      payload += chunk;
      // Reset after the first chunk has actually reached the downstream client.
      for (const socket of sockets) socket.resetAndDestroy();
    },
  }), { code: 'ECONNRESET' });
  assert.equal(payload, 'data: first\n\n');
  await healthy(port);
});

test('SSE chunks arrive immediately and active streams can outlive the idle timeout', async (t) => {
  let finished = false;
  let firstArrivedBeforeFinish = false;
  const { port } = await fixture(t, (_req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: 0\n\n');
    let i = 1;
    const timer = setInterval(() => {
      res.write(`data: ${i++}\n\n`);
      if (i === 8) {
        finished = true;
        clearInterval(timer);
        res.end();
      }
    }, 25);
    res.on('close', () => clearInterval(timer));
  }, { upstreamTimeout: 100, freeSocketTimeout: 10 });
  const res = await request(port, '/stream', {
    onData: () => { if (!finished) firstArrivedBeforeFinish = true; },
  });
  assert.equal(res.status, 200);
  assert.equal(firstArrivedBeforeFinish, true);
  assert.equal(res.body.toString(), Array.from({ length: 8 }, (_, i) => `data: ${i}\n\n`).join(''));
});

test('disabling active idle timeout does not inherit the free-pool timeout', async (t) => {
  const { port } = await fixture(t, (_req, res) => {
    const timer = setTimeout(() => res.end('ok'), 100);
    res.on('close', () => clearTimeout(timer));
  }, { upstreamTimeout: 0, freeSocketTimeout: 10 });
  await healthy(port);
});

test('an idle upstream returns 504 without killing subsequent requests', async (t) => {
  const { port } = await fixture(t, (req, res) => {
    if (req.url === '/healthy') res.end('ok');
  }, { upstreamTimeout: 40 });
  assert.equal((await request(port, '/idle')).status, 504);
  await healthy(port);
});

test('an idle timeout after streaming starts closes the stream without appending a 504 body', async (t) => {
  let payload = '';
  const { port } = await fixture(t, (req, res) => {
    if (req.url === '/healthy') return res.end('ok');
    res.write('data: first\n\n');
  }, { upstreamTimeout: 40 });
  await assert.rejects(request(port, '/idle-stream', {
    onData: (chunk) => { payload += chunk; },
  }), { code: 'ECONNRESET' });
  assert.equal(payload, 'data: first\n\n');
  await healthy(port);
});

test('a stalled TLS handshake is bounded by the connect timeout', async (t) => {
  const sockets = new Set();
  const stalled = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('data', () => {});
  });
  const stalledPort = await listen(stalled);
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => stalled.close(resolve));
  });
  const { port } = await fixture(t, undefined, {
    target: `https://127.0.0.1:${stalledPort}`, connectTimeout: 50, upstreamTimeout: 1000,
  });
  assert.equal((await request(port)).status, 504);
});

test('canceling a downstream stream releases its upstream connection', async (t) => {
  let resolveClosed;
  const upstreamClosed = new Promise((resolve) => { resolveClosed = resolve; });
  const { port } = await fixture(t, (req, res) => {
    if (req.url === '/healthy') return res.end('ok');
    res.on('close', resolveClosed);
    res.write('first');
  });
  await assert.rejects(request(port, '/stream', {
    onData: (_chunk, res) => res.destroy(new Error('Canceled by client')),
  }), /Canceled by client/);
  await upstreamClosed;
  await healthy(port);
});

test('canceling a partial upload releases the upstream and does not replay it', async (t) => {
  let calls = 0;
  let resolveReceived;
  let resolveClosed;
  const received = new Promise((resolve) => { resolveReceived = resolve; });
  const closed = new Promise((resolve) => { resolveClosed = resolve; });
  const { port } = await fixture(t, (req, res) => {
    if (req.url === '/healthy') return res.end('ok');
    calls++;
    req.on('error', () => {});
    req.on('close', resolveClosed);
    req.once('data', resolveReceived);
  });
  const req = http.request({ hostname: '127.0.0.1', port, path: '/upload', method: 'POST',
    headers: { 'content-length': 1000000 }, agent: false });
  req.on('error', () => {});
  req.write(Buffer.alloc(1024));
  await received;
  req.destroy();
  await closed;
  assert.equal(calls, 1);
  await healthy(port);
});

test('WebSocket forwards early frames in both directions and permits idle connections', async (t) => {
  const frame = Buffer.from([0x81, 0x04, ...Buffer.from('pong')]);
  const maskedFrame = Buffer.from([0x81, 0x84, 1, 2, 3, 4, 0x71, 0x6b, 0x6d, 0x63]);
  let resolveReceived;
  const received = new Promise((resolve) => { resolveReceived = resolve; });
  const { port, upstream } = await fixture(t, undefined, { upstreamTimeout: 50 });
  upstream.on('upgrade', (_req, socket, head) => {
    socket.write(Buffer.concat([Buffer.from(switchingProtocols), frame]));
    if (head.length) resolveReceived(head);
    else socket.once('data', resolveReceived);
    const timer = setTimeout(() => socket.write(frame), 120);
    socket.on('close', () => clearTimeout(timer));
  });
  const client = await connect(t, port);
  const first = readUntil(client, (bytes) => bytes.includes(frame));
  client.write(Buffer.concat([Buffer.from(upgradeHeaders), maskedFrame]));
  assert.match((await first).toString(), /101 Switching Protocols/);
  assert.deepEqual(await received, maskedFrame);
  assert.deepEqual(await readUntil(client, (bytes) => bytes.length === frame.length), frame);
  await healthy(port);
});

for (const resetSide of ['upstream', 'client']) {
  test(`WebSocket ${resetSide} reset closes the other side and service remains available`, async (t) => {
    let peer;
    const { port, upstream } = await fixture(t);
    upstream.on('upgrade', (_req, socket) => {
      peer = socket;
      peer.on('end', () => peer.end());
      peer.resume();
      peer.write(switchingProtocols);
    });
    const client = await connect(t, port);
    const handshake = readUntil(client, (bytes) => bytes.includes(Buffer.from('\r\n\r\n')));
    client.write(upgradeHeaders);
    await handshake;
    const other = resetSide === 'client' ? peer : client;
    const otherClosed = new Promise((resolve) => other.once('close', resolve));
    (resetSide === 'client' ? client : peer).resetAndDestroy();
    await otherClosed;
    await healthy(port);
  });
}

test('WebSocket handshake rejection preserves status, cookies and a decoded chunked body', async (t) => {
  const { port } = await fixture(t, (_req, res) => {
    res.writeHead(401, { 'content-type': 'text/plain', 'set-cookie': ['a=1', 'b=2'] });
    res.write('permission ');
    res.end('denied');
  });
  const client = await connect(t, port);
  const response = readUntil(client, (bytes) => bytes.includes(Buffer.from('permission denied')));
  client.write(upgradeHeaders);
  const raw = (await response).toString();
  assert.match(raw, /HTTP\/1\.1 401 Unauthorized/);
  assert.match(raw, /set-cookie: a=1\r\nset-cookie: b=2/i);
  assert.doesNotMatch(raw, /transfer-encoding/i);
  assert.equal(raw.split('\r\n\r\n')[1], 'permission denied');
});

test('WebSocket connection refusal returns an HTTP 502', async (t) => {
  const { port, upstream } = await fixture(t);
  await new Promise((resolve) => upstream.close(resolve));
  const client = await connect(t, port);
  const response = readUntil(client, (bytes) => bytes.includes(Buffer.from('Bad Gateway\r\n')));
  client.write(upgradeHeaders);
  assert.match((await response).toString(), /HTTP\/1\.1 502 Bad Gateway/);
});

test('shutdown lets an active HTTP response finish', async (t) => {
  let resolveStarted;
  const started = new Promise((resolve) => { resolveStarted = resolve; });
  const { port, proxy } = await fixture(t, (_req, res) => {
    res.write('first');
    resolveStarted();
    const timer = setTimeout(() => res.end('last'), 50);
    res.on('close', () => clearTimeout(timer));
  }, { shutdownTimeout: 500 });
  const response = request(port);
  await started;
  const closed = once(proxy, 'close');
  proxy.shutdown();
  assert.equal((await response).body.toString(), 'firstlast');
  await closed;
});

test('shutdown closes persistent WebSockets at its deadline', async (t) => {
  const { port, proxy, upstream } = await fixture(t, undefined, { shutdownTimeout: 40 });
  upstream.on('upgrade', (_req, socket) => socket.write(switchingProtocols));
  const client = await connect(t, port);
  const handshake = readUntil(client, (bytes) => bytes.includes(Buffer.from('\r\n\r\n')));
  client.write(upgradeHeaders);
  await handshake;
  const clientClosed = new Promise((resolve) => client.once('close', resolve));
  const serverClosed = once(proxy, 'close');
  proxy.shutdown();
  await Promise.all([clientClosed, serverClosed]);
});

async function startChild(t, target) {
  const logDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'api-proxy-child-logs-'));
  const child = spawn(process.execPath, [path.join(__dirname, '..', 'index.js')], {
    env: { ...process.env, TARGET_DOMAIN: target, PORT: '0', API_KEY: '',
      CONNECT_TIMEOUT_MS: '1000', UPSTREAM_IDLE_TIMEOUT_MS: '1000',
      KEEP_ALIVE_TIMEOUT_MS: '500', SHUTDOWN_TIMEOUT_MS: '50', LOG_DIR: logDirectory, UPSTREAM_PROXY_URL: '' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let logs = '';
  child.stderr.on('data', (chunk) => { logs += chunk; });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit');
      child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 2000);
      await exited;
      clearTimeout(timer);
    }
    await fs.rm(logDirectory, { recursive: true, force: true });
  });
  const port = await new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => reject(new Error('Child startup timed out')), 2000);
    child.on('error', reject);
    child.once('exit', () => { clearTimeout(timer); reject(new Error(`Child exited: ${logs}`)); });
    child.stdout.on('data', (chunk) => {
      output += chunk;
      const match = output.match(/running on port (\d+)/);
      if (match) { clearTimeout(timer); resolve(Number(match[1])); }
    });
  });
  return { child, port, logDirectory, logs: () => logs };
}

test('the standalone process survives response/socket resets and exits cleanly on SIGTERM', { timeout: 5000 }, async (t) => {
  let peer;
  const f = await fixture(t, (req, res) => {
    if (req.url === '/healthy') return res.end('ok');
    res.write('first');
  });
  f.upstream.on('upgrade', (_req, socket) => { peer = socket; socket.write(switchingProtocols); });
  const { child, port, logDirectory, logs } = await startChild(t, `http://127.0.0.1:${f.upstreamPort}`);
  await assert.rejects(request(port, '/stream', {
    onData: () => { for (const socket of f.sockets) socket.resetAndDestroy(); },
  }), { code: 'ECONNRESET' });
  const client = await connect(t, port);
  const handshake = readUntil(client, (bytes) => bytes.includes(Buffer.from('\r\n\r\n')));
  client.write(upgradeHeaders);
  await handshake;
  const closed = new Promise((resolve) => client.once('close', resolve));
  peer.resetAndDestroy();
  await closed;
  await healthy(port);
  assert.equal(child.exitCode, null);
  assert.doesNotMatch(logs(), /Unhandled|uncaught|ERR_HTTP_HEADERS_SENT/);
  const exit = once(child, 'exit');
  child.kill('SIGTERM');
  assert.deepEqual(await exit, [0, null]);
  const files = await fs.readdir(logDirectory);
  const records = (await fs.readFile(path.join(logDirectory, files[0]), 'utf8')).trim().split('\n').map(JSON.parse);
  assert.equal(records.length, 7);
  assert.ok(records.some((record) => record.path === '/healthy' && record.event === 'complete' && record.status === 200));
});
