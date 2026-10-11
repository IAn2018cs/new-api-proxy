const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { performance } = require('node:perf_hooks');
const { createAccessLogger } = require('../access-log');
const { fixture } = require('./proxy-fixture.cjs');

const concurrency = 32;
const count = 1000;
const rounds = 3;
const payload = Buffer.alloc(1024, 'x');
function median(values) { return values.sort((a, b) => a - b)[Math.floor(values.length / 2)]; }
async function run(port, agent, requests) {
  let next = 0;
  const latencies = [];
  const started = performance.now();
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (next++ < requests) {
      const sent = performance.now();
      await new Promise((resolve, reject) => {
        const req = http.get({ hostname: '127.0.0.1', port, path: '/benchmark', agent }, (res) => {
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
          res.resume();
          res.on('end', resolve);
          res.on('error', reject);
        });
        req.on('error', reject);
      });
      latencies.push(performance.now() - sent);
    }
  }));
  const elapsed = performance.now() - started;
  latencies.sort((a, b) => a - b);
  return { rps: requests * 1000 / elapsed, p50: latencies[Math.floor(requests * .5)], p95: latencies[Math.floor(requests * .95)] };
}

(async () => {
  for (const scheme of ['http', 'https', 'socks5', 'socks5h']) {
    for (const secure of [false, true]) {
      const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'proxy-protocol-bench-'));
      const cleanup = [];
      const logger = createAccessLogger({ directory });
      const clientAgent = new http.Agent({ keepAlive: true, maxSockets: concurrency });
      let priming = true;
      const initialResponses = [];
      const send = (res) => {
        res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': payload.length });
        res.end(payload);
      };
      try {
        const f = await fixture({ after: (fn) => cleanup.push(fn) }, {
          scheme, secure, accessLogger: logger, freeSocketTimeout: 4000, upstreamTimeout: 300000,
          handler: (_req, res) => {
            // Hold the first wave until all 32 connections exist. Fast local
            // responses can otherwise finish before the full pool is warm.
            if (priming) {
              initialResponses.push(res);
              if (initialResponses.length === concurrency) {
                priming = false;
                initialResponses.forEach(send);
              }
            } else send(res);
          },
        });
        await run(f.port, clientAgent, 400);
        const warmConnections = f.stats.connections;
        const samples = [];
        for (let round = 0; round < rounds; round++) samples.push(await run(f.port, clientAgent, count));
        clientAgent.destroy();
        for (const fn of cleanup) await fn();
        cleanup.length = 0;
        await logger.close();
        const filenames = await fs.readdir(directory);
        let logRecords = 0;
        for (const name of filenames) logRecords += (await fs.readFile(path.join(directory, name), 'utf8')).trim().split('\n').length;
        const result = { node: process.version, proxy: scheme, target: secure ? 'https' : 'http', concurrency,
          responseBytes: payload.length, rounds, requestsPerRound: count, accessLogging: true,
          additionalProxyConnections: f.stats.connections - warmConnections, logRecords,
          expectedLogRecords: (400 + rounds * count) * 2 };
        for (const key of ['rps', 'p50', 'p95']) result[key] = Number(median(samples.map((sample) => sample[key])).toFixed(3));
        console.log(JSON.stringify(result));
        if (result.additionalProxyConnections !== 0 || logRecords !== result.expectedLogRecords) throw new Error('Connection reuse or log accounting failed');
      } finally {
        clientAgent.destroy();
        for (const fn of cleanup) await fn();
        await logger.close();
        await fs.rm(directory, { recursive: true, force: true });
      }
    }
  }
})().catch((err) => { console.error(err.code || err.message); process.exitCode = 1; });
