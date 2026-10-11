const assert = require('node:assert/strict');
const http = require('node:http');
const net = require('node:net');
const { spawn } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { once } = require('node:events');
const { test: nodeTest } = require('node:test');
const { createProxyServer } = require('../index');
const { fixture, request, upgradeHeaders } = require('../scripts/proxy-fixture.cjs');

const test = (name, fn) => nodeTest(name, { timeout: 5000 }, fn);
const schemes = ['http', 'https', 'socks5', 'socks5h'];

for (const scheme of schemes) {
  for (const secure of [false, true]) {
    const combination = `${scheme} proxy -> ${secure ? 'HTTPS' : 'HTTP'}`;
    test(`${combination}: paths, binary body, auth and connection reuse`, async (t) => {
      const originalPath = '/nested/%2F/../echo?q=a%2Bb&x=1';
      const payload = Buffer.from([0, 1, 255, 128, 4]);
      const seen = [];
      const f = await fixture(t, { scheme, secure, handler: (req, res) => {
        seen.push({ method: req.method, path: req.url, headers: req.headers });
        req.pipe(res);
      } });
      for (let i = 0; i < 2; i++) {
        const response = await request(f.port, originalPath, { method: 'POST', body: payload,
          headers: { authorization: 'Bearer client', 'x-api-key': 'client-key', 'api-key': 'other-key',
            'content-length': payload.length, 'proxy-authorization': 'must-not-leak' } });
        assert.equal(response.status, 200, `iteration ${i}; ${JSON.stringify(f.stats)}`);
        assert.deepEqual(response.body, payload);
      }
      for (const req of seen) {
        assert.equal(req.path, originalPath);
        assert.equal(req.method, 'POST');
        assert.equal(req.headers.authorization, 'Bearer client');
        assert.equal(req.headers['x-api-key'], 'client-key');
        assert.equal(req.headers['api-key'], 'other-key');
        assert.equal(req.headers['proxy-authorization'], undefined);
      }
      assert.equal(f.stats.connections, 1);
      if (scheme.startsWith('socks')) {
        assert.equal(f.stats.socks.length, 1);
        assert.equal(f.originConnections(), 1);
        if (scheme === 'socks5') assert.ok([1, 4].includes(f.stats.socks[0].type));
        else assert.deepEqual([f.stats.socks[0].type, f.stats.socks[0].host], [3, 'only-proxy.invalid']);
      } else if (secure) assert.equal(f.stats.connects, 1);
      else assert.equal(f.stats.requests, 2);
    });

    test(`${combination}: SSE remains live and upload streams before request completion`, async (t) => {
      let resolveUpload;
      const uploadReceived = new Promise((resolve) => { resolveUpload = resolve; });
      const f = await fixture(t, { scheme, secure, upstreamTimeout: 100, freeSocketTimeout: 10,
        handler: (req, res) => {
          if (req.url === '/upload') {
            req.once('data', resolveUpload);
            return req.pipe(res);
          }
          res.writeHead(200, { 'content-type': 'text/event-stream' });
          res.write('data: first\n\n');
          let count = 0;
          const timer = setInterval(() => {
            res.write('data: next\n\n');
            if (++count === 6) { clearInterval(timer); res.end(); }
          }, 30);
          res.on('close', () => clearInterval(timer));
        } });
      let early = false;
      const response = await request(f.port, '/stream', { onData: (_chunk, res) => { early ||= !res.complete; } });
      assert.equal(response.status, 200);
      assert.equal(early, true);
      assert.equal(response.body.toString(), 'data: first\n\n' + 'data: next\n\n'.repeat(6));
      const upload = http.request({ hostname: '127.0.0.1', port: f.port, path: '/upload', method: 'POST', agent: false });
      upload.on('error', () => {});
      t.after(() => upload.destroy());
      const result = new Promise((resolve) => upload.on('response', (res) => { res.resume(); res.on('end', resolve); }));
      upload.write('first');
      assert.equal((await uploadReceived).toString(), 'first');
      upload.end('last');
      await result;
    });

    test(`${combination}: WebSocket/WSS forwards early bytes and survives quiet periods`, async (t) => {
      const f = await fixture(t, { scheme, secure, upstreamTimeout: 50 });
      const earlyFrame = Buffer.from([0x81, 2, 0x6f, 0x6b]);
      let resolveReceived;
      const received = new Promise((resolve) => { resolveReceived = resolve; });
      f.origin.on('upgrade', (_req, socket, head) => {
        socket.write(Buffer.concat([Buffer.from(upgradeHeaders), earlyFrame]));
        if (head.length) resolveReceived(head);
        else socket.once('data', resolveReceived);
        const timer = setTimeout(() => socket.write(earlyFrame), 100);
        socket.on('close', () => clearTimeout(timer));
      });
      const client = net.connect(f.port, '127.0.0.1');
      client.on('error', () => {});
      t.after(() => client.destroy());
      await once(client, 'connect');
      const data = [];
      let resolveLate;
      const late = new Promise((resolve) => { resolveLate = resolve; });
      client.on('data', (chunk) => {
        data.push(chunk);
        const all = Buffer.concat(data);
        const end = all.indexOf('\r\n\r\n');
        if (end >= 0 && all.length >= end + 4 + earlyFrame.length * 2) resolveLate(all);
      });
      client.write(Buffer.concat([Buffer.from('GET /ws?q=1 HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n'), earlyFrame]));
      assert.deepEqual(await received, earlyFrame);
      const all = await late;
      assert.match(all.toString(), /101 Switching Protocols/);
      assert.deepEqual(all.subarray(all.indexOf('\r\n\r\n') + 4), Buffer.concat([earlyFrame, earlyFrame]));
    });
  }

  test(`${scheme}: API_KEY replacement still applies through the proxy`, async (t) => {
    const f = await fixture(t, { scheme, apiKey: 'replacement', handler: (req, res) => res.end(JSON.stringify(req.headers)) });
    const headers = JSON.parse((await request(f.port, '/', { headers: { authorization: 'Bearer client', 'x-api-key': 'client' } })).body);
    assert.equal(headers.authorization, 'Bearer replacement');
    assert.equal(headers['x-api-key'], 'replacement');
  });

  test(`${scheme}: stale GET retry uses a new proxy connection; POST is never replayed`, async (t) => {
    let calls = 0;
    const f = await fixture(t, { scheme, secure: true, handler: (req, res) => {
      if (++calls === 2 || calls === 4) req.socket.destroy();
      else res.end('ok');
    } });
    assert.equal((await request(f.port)).status, 200);
    assert.equal((await request(f.port)).status, 200);
    assert.equal(calls, 3);
    assert.equal(f.stats.connections, 2);
    assert.equal((await request(f.port, '/', { method: 'POST' })).status, 502);
    assert.equal(calls, 4);
  });

  test(`${scheme}: a stalled handshake times out and releases its proxy socket`, async (t) => {
    const f = await fixture(t, { scheme, secure: true, stall: true, connectTimeout: 50 });
    assert.equal((await request(f.port)).status, 504);
    const deadline = Date.now() + 500;
    while (f.sockets.size && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(f.sockets.size, 0);
    assert.equal(f.originConnections(), 0);
  });

  test(`${scheme}: cancellation during handshake releases the proxy without reaching upstream`, async (t) => {
    const f = await fixture(t, { scheme, secure: true, stall: true, connectTimeout: 1000 });
    const connection = once(f.gateway, 'connection');
    const req = http.get({ hostname: '127.0.0.1', port: f.port, agent: false });
    req.on('error', () => {});
    const [socket] = await connection;
    const closed = new Promise((resolve) => socket.once('close', resolve));
    req.destroy();
    await closed;
    assert.equal(f.originConnections(), 0);
  });

  test(`${scheme}: failed proxy authentication does not fall back to direct`, async (t) => {
    const f = await fixture(t, { scheme, secure: true, reject: true });
    const result = await request(f.port);
    assert.ok([407, 502].includes(result.status));
    assert.equal(f.originConnections(), 0);
  });
}

test('invalid proxy URLs fail at startup without exposing credentials', async () => {
  for (const url of ['not-a-proxy-secret', 'ftp://user:secret@localhost:8080', 'http://user:%ZZ@localhost:8080',
    'http://user:secret@localhost:8080/path', 'socks5h://user:secret@localhost:0']) {
    assert.throws(() => createProxyServer({ upstreamProxyUrl: url, accessLogger: null }), (err) => {
      assert.match(err.message, /UPSTREAM_PROXY_URL/);
      assert.doesNotMatch(err.message, /secret|%ZZ/);
      return true;
    });
  }
});

for (const scheme of schemes) {
  test(`${scheme}: refused proxy never reaches an otherwise reachable target`, async (t) => {
    const f = await fixture(t, { scheme, targetHost: '127.0.0.1' });
    await new Promise((resolve) => f.gateway.close(resolve));
    assert.equal((await request(f.port)).status, 502);
    assert.equal(f.originConnections(), 0);
  });
}

test('untrusted HTTPS proxy certificates fail without reaching the target', async (t) => {
  const f = await fixture(t, { scheme: 'https', tlsOptions: {} });
  assert.equal((await request(f.port)).status, 502);
  assert.equal(f.originConnections(), 0);
});

test('untrusted upstream TLS fails inside the SOCKS tunnel and releases it', async (t) => {
  const f = await fixture(t, { scheme: 'socks5h', secure: true, tlsOptions: {} });
  assert.equal((await request(f.port)).status, 502);
  const deadline = Date.now() + 500;
  while (f.sockets.size && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(f.sockets.size, 0);
  assert.equal(f.originConnections(), 1);
});

test('empty proxy URL keeps direct routing and does not use ambient HTTP_PROXY', async (t) => {
  const f = await fixture(t, { upstreamProxyUrl: '', targetHost: '127.0.0.1' });
  const previous = process.env.HTTP_PROXY;
  process.env.HTTP_PROXY = f.url;
  try { assert.equal((await request(f.port)).status, 200); }
  finally {
    if (previous === undefined) delete process.env.HTTP_PROXY;
    else process.env.HTTP_PROXY = previous;
  }
  assert.equal(f.stats.connections, 0);
  assert.equal(f.originConnections(), 1);
});

for (const scheme of schemes) {
  test(`${scheme}: proxies without URL credentials are supported`, async (t) => {
    const f = await fixture(t, { scheme, secure: true, authenticate: false });
    assert.equal((await request(f.port)).status, 200);
    assert.equal(f.stats.connections, 1);
  });
}

test('an explicit proxy remains mandatory even when NO_PROXY=*', async (t) => {
  const previous = process.env.NO_PROXY;
  process.env.NO_PROXY = '*';
  try {
    const f = await fixture(t, { scheme: 'http', targetHost: '127.0.0.1' });
    assert.equal((await request(f.port)).status, 200);
    assert.equal(f.stats.requests, 1);
  } finally {
    if (previous === undefined) delete process.env.NO_PROXY;
    else process.env.NO_PROXY = previous;
  }
});

test('standalone logs redact proxy credentials even when DEBUG=*', async (t) => {
  const f = await fixture(t);
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'proxy-redaction-'));
  const child = spawn(process.execPath, [path.join(__dirname, '../index.js')], {
    env: { ...process.env, PORT: '0', TARGET_DOMAIN: `http://only-proxy.invalid:${f.originPort}`,
      UPSTREAM_PROXY_URL: f.url, API_KEY: '', DEBUG: '*', LOG_DIR: directory, SHUTDOWN_TIMEOUT_MS: '50' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stderr.on('data', (chunk) => { output += chunk; });
  const port = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.once('exit', () => reject(new Error('Proxy exited before listening')));
    child.stdout.on('data', (chunk) => {
      output += chunk;
      const match = output.match(/running on port (\d+)/);
      if (match) resolve(Number(match[1]));
    });
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit');
      child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 1500);
      await exited;
      clearTimeout(timer);
    }
    await fs.rm(directory, { recursive: true, force: true });
  });
  assert.equal((await request(port)).status, 200);
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  assert.deepEqual(await exited, [0, null]);
  assert.doesNotMatch(output, /test-user|p:a ss@|p%3Aa%20ss%40/);
  assert.ok(!output.includes(Buffer.from('test-user:p:a ss@').toString('base64')));
  assert.match(output, /Upstream proxy: http:\/\/127\.0\.0\.1:/);
  const files = await fs.readdir(directory);
  const records = await fs.readFile(path.join(directory, files[0]), 'utf8');
  assert.doesNotMatch(records, /test-user|p:a ss@|p%3Aa%20ss%40/);
  assert.ok(!records.includes(Buffer.from('test-user:p:a ss@').toString('base64')));
});

for (const generation of ['/v1/chat/completions', '/v1/responses', '/v1/messages', '/v1beta/models/demo:streamGenerateContent?alt=sse', '/v1/%72esponses']) {
  test(`generation endpoint ${generation} is never replayed even with a bodyless GET`, async (t) => {
    let calls = 0;
    const f = await fixture(t, { scheme: 'socks5h', handler: (req, res) => {
      if (++calls === 1) res.end('ok');
      else req.socket.destroy();
    } });
    assert.equal((await request(f.port, '/v1/models')).status, 200);
    assert.equal((await request(f.port, generation)).status, 502);
    assert.equal(calls, 2);
  });
}

for (const scheme of ['http', 'socks5h']) {
  test(`${scheme}: target certificate hostname is verified inside the tunnel`, async (t) => {
    const f = await fixture(t, { scheme, secure: true, targetHost: 'wrong-name.invalid' });
    assert.equal((await request(f.port)).status, 502);
    assert.equal(f.originConnections(), 1);
  });
}
