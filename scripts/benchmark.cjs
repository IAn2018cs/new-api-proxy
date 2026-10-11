const http = require('node:http');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const runtime = process.execPath;
if (!process.argv[2]) {
  console.error('Usage: npm run benchmark -- /path/to/original-index.cjs');
  process.exit(1);
}
const baseline = path.resolve(process.argv[2]);
const current = path.join(__dirname, '..', 'index.js');
const payload = Buffer.alloc(1024, 'x');
const children = [];
let logDirectory;
let connections = 0;
const upstream = http.createServer((_req, res) => {
  res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': payload.length });
  res.end(payload);
});
upstream.on('connection', () => connections++);
async function boot(file, target) {
  const reserve = http.createServer();
  reserve.listen(0, '127.0.0.1');
  await once(reserve, 'listening');
  const port = reserve.address().port;
  await new Promise((resolve) => reserve.close(resolve));
  const child = spawn(runtime, [file], {
    env: { ...process.env, PORT: String(port), TARGET_DOMAIN: target, API_KEY: '',
      CONNECT_TIMEOUT_MS: '10000', UPSTREAM_IDLE_TIMEOUT_MS: '300000',
      KEEP_ALIVE_TIMEOUT_MS: '4000', SHUTDOWN_TIMEOUT_MS: '1000',
      LOG_DIR: path.join(logDirectory, String(port)), UPSTREAM_PROXY_URL: '' },
    // Benchmark the unchanged direct path regardless of the caller's proxy env.
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  children.push(child);
  await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code) => reject(new Error(`Child exited: ${code}`)));
    child.stdout.once('data', resolve);
    child.stderr.on('data', (chunk) => process.stderr.write(chunk));
  });
  return port;
}
async function run(port, keepAlive, count, concurrency) {
  const agent = keepAlive ? new http.Agent({ keepAlive: true, maxSockets: concurrency }) : false;
  let next = 0;
  const latencies = [];
  const beforeConnections = connections;
  const start = performance.now();
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (next++ < count) {
      const sent = performance.now();
      await new Promise((resolve, reject) => {
        const req = http.get({ hostname: '127.0.0.1', port, path: '/v1/test', agent }, (res) => {
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
          res.resume();
          res.once('end', resolve);
          res.once('error', reject);
        });
        req.once('error', reject);
      });
      latencies.push(performance.now() - sent);
    }
  }));
  const elapsed = performance.now() - start;
  agent && agent.destroy();
  latencies.sort((a, b) => a - b);
  return { rps: count * 1000 / elapsed, p50: latencies[Math.floor(count * .5)],
    p95: latencies[Math.floor(count * .95)], upstreamConnections: connections - beforeConnections };
}
function median(values) { return values.sort((a, b) => a - b)[Math.floor(values.length / 2)]; }
(async () => {
  logDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'api-proxy-benchmark-logs-'));
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const target = `http://127.0.0.1:${upstream.address().port}`;
  const ports = {
    before: await boot(baseline, target),
    after: await boot(current, target),
  };
  for (const keepAlive of [true, false]) {
    const samples = { before: [], after: [] };
    for (const port of Object.values(ports)) await run(port, keepAlive, 1000, 32);
    for (let round = 0; round < 5; round++) {
      for (const name of round % 2 ? ['after', 'before'] : ['before', 'after']) {
        samples[name].push(await run(ports[name], keepAlive, keepAlive ? 5000 : 3000, 32));
      }
    }
    const summary = {};
    for (const [name, results] of Object.entries(samples)) {
      summary[name] = Object.fromEntries(['rps', 'p50', 'p95', 'upstreamConnections'].map((key) =>
        [key, Number(median(results.map((result) => result[key])).toFixed(3))]));
    }
    console.log(JSON.stringify({ node: process.version, keepAlive, concurrency: 32, responseBytes: payload.length,
      rounds: 5, requestsPerRound: keepAlive ? 5000 : 3000, ...summary,
      rpsChangePercent: Number(((summary.after.rps / summary.before.rps - 1) * 100).toFixed(2)) }));
  }
})().catch((err) => { console.error(err); process.exitCode = 1; }).finally(async () => {
  for (const child of children) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    const exited = once(child, 'exit');
    child.kill('SIGTERM');
    const timer = setTimeout(() => child.kill('SIGKILL'), 2000);
    await exited;
    clearTimeout(timer);
  }
  upstream.closeAllConnections();
  await new Promise((resolve) => upstream.close(resolve));
  if (logDirectory) await fs.rm(logDirectory, { recursive: true, force: true });
});
