const http = require('node:http');
const https = require('node:https');
const net = require('node:net');
const fs = require('node:fs');
const path = require('node:path');
const { once } = require('node:events');
const { createProxyServer } = require('../index');

// Public fixtures used only by local protocol tests and benchmarks.
const cert = fs.readFileSync(path.join(__dirname, '../test/fixtures/proxy-cert.pem'));
const key = fs.readFileSync(path.join(__dirname, '../test/fixtures/proxy-key.pem'));
const username = 'test-user';
const password = 'p:a ss@';
const authorization = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
const upgradeHeaders = 'HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n';

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

function reader(socket) {
  let buffered = Buffer.alloc(0);
  let waiting;
  const data = (chunk) => {
    buffered = Buffer.concat([buffered, chunk]);
    if (waiting && buffered.length >= waiting.size) {
      const item = waiting;
      waiting = undefined;
      const result = buffered.subarray(0, item.size);
      buffered = buffered.subarray(item.size);
      item.resolve(result);
    }
  };
  socket.on('data', data);
  return {
    read(size) {
      if (buffered.length >= size) {
        const result = buffered.subarray(0, size);
        buffered = buffered.subarray(size);
        return Promise.resolve(result);
      }
      return new Promise((resolve, reject) => {
        waiting = { size, resolve, reject };
        socket.once('close', () => reject(new Error('Socket closed during negotiation')));
      });
    },
    detach() { socket.removeListener('data', data); return buffered; },
  };
}

function track(server, sockets) {
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.on('end', () => socket.end());
  });
}

async function createGateway({ scheme, originPort, reject = false, stall = false, authenticate = true }) {
  const sockets = new Set();
  const stats = { requests: 0, connects: 0, socks: [], connections: 0, parseErrors: [] };
  let gateway;
  const forwardAgent = new http.Agent({ keepAlive: true });
  const tunnel = async (client, head, acknowledge) => {
    if (stall) return;
    const peer = net.connect(originPort, '127.0.0.1');
    sockets.add(peer);
    peer.on('error', () => client.destroy());
    peer.on('close', () => sockets.delete(peer));
    client.on('close', () => peer.destroy());
    await once(peer, 'connect');
    acknowledge();
    if (head.length) peer.write(head);
    client.pipe(peer);
    peer.pipe(client);
  };
  if (scheme.startsWith('socks5')) {
    gateway = net.createServer((socket) => {
      const input = reader(socket);
      (async () => {
        const greeting = await input.read(2);
        await input.read(greeting[1]);
        if (stall) return;
        socket.write(Buffer.from([5, authenticate ? 2 : 0]));
        let authenticated = !reject;
        if (authenticate) {
          const auth = await input.read(2);
          const user = (await input.read(auth[1])).toString();
          const passLength = (await input.read(1))[0];
          const pass = (await input.read(passLength)).toString();
          authenticated &&= user === username && pass === password;
          socket.write(Buffer.from([1, authenticated ? 0 : 1]));
        }
        if (!authenticated) return socket.end();
        const request = await input.read(4);
        let host;
        if (request[3] === 1) host = [...await input.read(4)].join('.');
        else if (request[3] === 4) host = (await input.read(16)).toString('hex');
        else host = (await input.read((await input.read(1))[0])).toString();
        const port = (await input.read(2)).readUInt16BE();
        stats.socks.push({ type: request[3], host, port, authenticated });
        await tunnel(socket, input.detach(), () => socket.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, 0, 0])));
      })().catch(() => socket.destroy());
    });
  } else {
    const handler = (req, res) => {
      stats.requests++;
      if (reject || authenticate && req.headers['proxy-authorization'] !== authorization) {
        res.writeHead(407, { 'proxy-authenticate': 'Basic realm="fixture"' });
        return res.end('proxy authentication required');
      }
      if (stall) return;
      const originStart = req.url.indexOf('://') + 3;
      const pathStart = req.url.indexOf('/', originStart);
      const headers = { ...req.headers };
      delete headers['proxy-authorization'];
      delete headers['proxy-connection'];
      const outgoing = http.request({ hostname: '127.0.0.1', port: originPort,
        path: pathStart === -1 ? '/' : req.url.slice(pathStart), method: req.method, headers, agent: forwardAgent }, (response) => {
        res.writeHead(response.statusCode, response.headers);
        response.pipe(res);
      });
      outgoing.on('error', () => { res.writeHead(502); res.end(); });
      req.pipe(outgoing);
    };
    gateway = scheme === 'https' ? https.createServer({ key, cert }, handler) : http.createServer(handler);
    gateway.on('connect', (req, socket, head) => {
      stats.connects++;
      if (reject || authenticate && req.headers['proxy-authorization'] !== authorization) {
        return socket.end('HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n');
      }
      tunnel(socket, head, () => socket.write('HTTP/1.1 200 Connection Established\r\n\r\n')).catch(() => socket.destroy());
    });
    gateway.on('upgrade', (req, socket, head) => {
      stats.requests++;
      if (reject || authenticate && req.headers['proxy-authorization'] !== authorization) return socket.destroy();
      const pathStart = req.url.indexOf('/', req.url.indexOf('://') + 3);
      const headers = Object.entries(req.headers).filter(([name]) => !name.startsWith('proxy-'));
      const header = `${req.method} ${req.url.slice(pathStart)} HTTP/1.1\r\n${headers.map(([name, value]) => `${name}: ${value}`).join('\r\n')}\r\n\r\n`;
      tunnel(socket, Buffer.concat([Buffer.from(header), head]), () => {}).catch(() => socket.destroy());
    });
  }
  track(gateway, sockets);
  gateway.on('close', () => forwardAgent.destroy());
  gateway.on('clientError', (err, socket) => {
    stats.parseErrors.push(err.code);
    socket.end('HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n');
  });
  gateway.on('connection', () => stats.connections++);
  const port = await listen(gateway);
  return { gateway, stats, sockets, url: `${scheme}://${authenticate ? `${username}:${encodeURIComponent(password)}@` : ''}127.0.0.1:${port}` };
}

async function fixture(t, { scheme = 'http', secure = false, handler = (_req, res) => res.end('ok'),
  reject = false, stall = false, authenticate = true, targetHost, ...options } = {}) {
  const origin = secure ? https.createServer({ key, cert }, handler) : http.createServer(handler);
  const originSockets = new Set();
  track(origin, originSockets);
  let originConnections = 0;
  origin.on('connection', () => originConnections++);
  const originPort = await listen(origin);
  const gateway = await createGateway({ scheme, originPort, reject, stall, authenticate });
  const host = targetHost || (scheme === 'socks5' ? 'localhost' : 'only-proxy.invalid');
  const target = `${secure ? 'https' : 'http'}://${host}:${originPort}`;
  const proxy = createProxyServer({ target, upstreamProxyUrl: gateway.url, apiKey: '', accessLogger: null,
    tlsOptions: { ca: cert }, connectTimeout: 1000, upstreamTimeout: 1000, freeSocketTimeout: 500,
    shutdownTimeout: 50, ...options });
  const port = await listen(proxy);
  const closed = once(proxy, 'close');
  t.after(async () => {
    proxy.shutdown();
    await closed;
    for (const socket of [...originSockets, ...gateway.sockets]) socket.destroy();
    await Promise.all([origin, gateway.gateway].filter((server) => server.listening).map((server) => new Promise((resolve) => server.close(resolve))));
  });
  return { port, proxy, origin, originPort, originSockets, originConnections: () => originConnections, ...gateway };
}

function request(port, url = '/', { body, onData, ...options } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: url, agent: false, ...options }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => { chunks.push(chunk); onData?.(chunk, res); });
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.end(body);
  });
}

module.exports = { fixture, createGateway, listen, request, cert, key, upgradeHeaders };
