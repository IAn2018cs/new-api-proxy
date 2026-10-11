const http = require('http');
const https = require('https');
const { performance } = require('node:perf_hooks');
const { createAccessLogger, formatTimestamp } = require('./access-log');
const { createUpstreamAgents } = require('./upstream-proxy');

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
  'Access-Control-Max-Age': '86400',
};

const hopHeaders = new Set(['connection', 'keep-alive', 'proxy-authenticate',
  'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade']);

const generationPath = /(?:^|[/:])(?:completions|responses|messages|generate|generations|generateContent|streamGenerateContent|predict|rawPredict|streamRawPredict|speech|transcriptions|translations)(?:\/|$)/i;

function mayRetry(req) {
  if ((req.method !== 'GET' && req.method !== 'HEAD') || req.headers['transfer-encoding'] ||
    req.headers['content-length'] && Number(req.headers['content-length']) !== 0) return false;
  const query = req.url.indexOf('?');
  let pathname = query === -1 ? req.url : req.url.slice(0, query);
  if (pathname.includes('%')) {
    try { pathname = decodeURIComponent(pathname); }
    catch { return false; }
  }
  return !generationPath.test(pathname);
}

function envNumber(name, fallback, maximum = 2 ** 31 - 1) {
  const value = process.env[name];
  if (value === undefined || value === '') return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number < 0 || number > maximum) {
    throw new Error(`${name} must be an integer between 0 and ${maximum}`);
  }
  return number;
}

function cleanHeaders(source, upgrade = false) {
  const headers = {};
  for (const key in source) {
    if (!hopHeaders.has(key) || (upgrade && key === 'upgrade')) headers[key] = source[key];
  }
  // Connection-specific headers belong to this hop, not the upstream connection.
  const connection = source.connection;
  if (connection && connection !== 'keep-alive' && connection !== 'close') {
    for (const name of connection.split(',')) {
      const key = name.trim().toLowerCase();
      if (key && !(upgrade && key === 'upgrade')) delete headers[key];
    }
  }
  if (upgrade) headers.connection = 'Upgrade';
  return headers;
}

function timeoutError(message) {
  return Object.assign(new Error(message), { code: 'ETIMEDOUT' });
}

function watchConnection(request, timeout, onTimeout, secure) {
  if (!timeout) return;
  request.once('socket', (socket) => {
    // A pooled connection is already established; it needs no connect timer.
    if (request.destroyed || request.reusedSocket ||
      (!socket.connecting && (!secure || !socket.secureConnecting))) return;
    const event = secure ? 'secureConnect' : 'connect';
    const timer = setTimeout(() => onTimeout(timeoutError('Upstream connection timed out')), timeout);
    timer.unref();
    const clear = () => {
      clearTimeout(timer);
      socket.removeListener(event, clear);
      request.removeListener('error', clear);
      request.removeListener('close', clear);
    };
    socket.once(event, clear);
    request.once('error', clear);
    request.once('close', clear);
  });
}

function logError(kind, req, err) {
  // Do not log credentials, request bodies, or query strings.
  console.error(`[${formatTimestamp()}] ${kind}: ${req.method} ${req.url.split('?')[0]} ${err.code || 'UPSTREAM_ERROR'}`);
}

function rawResponse(response, headers) {
  const lines = [`HTTP/1.1 ${response.statusCode} ${response.statusMessage || ''}`];
  for (const [key, value] of Object.entries(headers)) {
    for (const item of Array.isArray(value) ? value : [value]) lines.push(`${key}: ${item}`);
  }
  return `${lines.join('\r\n')}\r\n\r\n`;
}

function createProxyServer({
  target = process.env.TARGET_DOMAIN || 'https://example.com',
  apiKey = process.env.API_KEY || '',
  upstreamProxyUrl = process.env.UPSTREAM_PROXY_URL || '',
  tlsOptions,
  connectTimeout = envNumber('CONNECT_TIMEOUT_MS', 10000),
  upstreamTimeout = envNumber('UPSTREAM_IDLE_TIMEOUT_MS', 300000),
  freeSocketTimeout = envNumber('KEEP_ALIVE_TIMEOUT_MS', 4000),
  shutdownTimeout = envNumber('SHUTDOWN_TIMEOUT_MS', 10000),
  logDirectory = process.env.LOG_DIR,
  accessLogger,
} = {}) {
  const targetUrl = new URL(target);
  if (!['http:', 'https:'].includes(targetUrl.protocol)) {
    throw new Error('TARGET_DOMAIN must use http:// or https://');
  }
  const protocol = targetUrl.protocol;
  const secure = protocol === 'https:';
  const transport = secure ? https : http;
  const hostname = targetUrl.hostname.replace(/^\[|\]$/g, '');
  const port = targetUrl.port || (secure ? 443 : 80);
  const host = targetUrl.host;
  const upstream = createUpstreamAgents({ target: targetUrl, proxyUrl: upstreamProxyUrl,
    freeSocketTimeout, connectTimeout, tlsOptions });
  const agent = upstream.agent;
  const sockets = new Set();
  const logger = accessLogger === undefined ? createAccessLogger({ directory: logDirectory }) : accessLogger;
  const session = `${process.pid}-${Date.now().toString(36)}`;
  let requestSequence = 0;
  let stopping = false;

  function startAccess(req, protocol) {
    if (!logger) return undefined;
    const query = req.url.indexOf('?');
    const fields = {
      request_id: `${session}-${++requestSequence}`,
      protocol,
      client_ip: req.socket.remoteAddress,
      method: req.method,
      path: req.url.slice(0, query === -1 ? 1024 : Math.min(query, 1024)),
    };
    logger.write({ at: Date.now(), event: 'start', fields });
    return { fields, started: performance.now(), status: undefined, error: undefined, retries: 0, ended: false };
  }

  function recordAccess(access, event, status, final = true) {
    if (!access || access.ended) return;
    access.ended = final;
    logger.write({
      at: Date.now(), event, fields: access.fields, status,
      duration_ms: Math.round((performance.now() - access.started) * 100) / 100,
      error: access.error, retries: access.retries,
    });
  }

  function requestOptions(req, upgrade = false) {
    const headers = cleanHeaders(req.headers, upgrade);
    headers.host = host;
    if (apiKey) {
      if (headers.authorization) headers.authorization = `Bearer ${apiKey}`;
      if (headers['x-api-key']) headers['x-api-key'] = apiKey;
      if (headers['api-key']) headers['api-key'] = apiKey;
      if (!headers.authorization && !headers['x-api-key'] && !headers['api-key']) {
        headers.authorization = `Bearer ${apiKey}`;
      }
    }
    return {
      ...tlsOptions,
      hostname,
      protocol,
      port,
      path: req.url,
      method: req.method,
      headers,
      agent,
      // Override the agent's idle-pool timeout while a request is active.
      timeout: upstreamTimeout,
    };
  }

  const server = http.createServer((req, res) => {
    const access = startAccess(req, 'http');
    if (access) {
      res.once('finish', () => recordAccess(access, 'complete', res.statusCode));
      res.once('close', () => {
        if (!res.writableFinished) {
          access.error ||= 'CLIENT_ABORTED';
          recordAccess(access, 'aborted', res.headersSent ? res.statusCode : 499);
        }
      });
    }
    if (stopping) {
      res.writeHead(503, { ...corsHeaders, Connection: 'close' });
      res.end('Service Unavailable');
      return;
    }
    if (req.method === 'OPTIONS') {
      res.writeHead(204, corsHeaders);
      res.end();
      return;
    }

    let upstreamRequest;
    let upstreamResponse;
    let failed = false;
    let clientGone = false;
    let retries = 0;
    const options = requestOptions(req);
    const canRetry = mayRetry(req);

    const cleanup = () => {
      req.unpipe(upstreamRequest);
      upstreamResponse?.destroy();
      upstream.cancel(upstreamRequest);
      upstreamRequest?.destroy();
    };
    const cancel = () => {
      clientGone = true;
      cleanup();
    };
    const fail = (err) => {
      if (failed || clientGone || res.destroyed || res.writableEnded) return;
      failed = true;
      if (access) access.error = err.code || 'UPSTREAM_ERROR';
      logError('Proxy error', req, err);
      cleanup();
      if (res.headersSent) {
        // Once streaming has started, terminate it without appending an error body.
        res.destroy();
      } else {
        const status = err.code === 'ETIMEDOUT' ? 504 : 502;
        res.writeHead(status, { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8', Connection: 'close' });
        res.end(status === 504 ? 'Gateway Timeout' : 'Bad Gateway');
      }
    };

    req.on('aborted', cancel);
    req.on('error', cancel);
    res.on('error', cancel);
    res.on('close', () => {
      if (!res.writableFinished) cancel();
    });

    const forward = () => {
      upstreamRequest = transport.request(retries ? { ...options, agent: upstream.freshAgent } : options, (response) => {
        upstreamResponse = response;
        response.on('error', fail);
        response.on('aborted', () => fail(Object.assign(new Error('Upstream response aborted'), { code: 'ECONNRESET' })));
        if (clientGone || failed || res.destroyed) {
          response.destroy();
          return;
        }
        res.writeHead(response.statusCode, { ...cleanHeaders(response.headers), ...corsHeaders });
        response.pipe(res);
      });
      const attempt = upstreamRequest;
      if (!upstream.proxied) watchConnection(attempt, connectTimeout, fail, secure);
      attempt.on('timeout', () => fail(timeoutError('Upstream became idle')));
      attempt.on('error', (err) => {
        // Only replay a bodyless safe request after a stale pooled socket fails.
        if (!failed && !clientGone && !res.destroyed && !upstreamResponse &&
          canRetry && req.complete && retries === 0 && attempt.reusedSocket && err.code === 'ECONNRESET') {
          req.unpipe(attempt);
          retries++;
          if (access) access.retries = retries;
          forward();
          return;
        }
        fail(err);
      });
      attempt.on('upgrade', (_response, socket) => {
        socket.on('error', () => socket.destroy());
        socket.destroy();
        fail(Object.assign(new Error('Unexpected upstream upgrade'), { code: 'EPROTO' }));
      });
      if (retries) attempt.end();
      else req.pipe(attempt);
    };
    forward();
  });

  server.on('upgrade', (req, client, head) => {
    const access = startAccess(req, 'websocket');
    let upstreamRequest;
    let upstreamResponse;
    let peer;
    let closed = false;
    let responseStarted = false;
    const cleanup = () => {
      upstream.cancel(upstreamRequest);
      upstreamRequest?.destroy();
      upstreamResponse?.destroy();
      peer?.destroy();
    };
    const close = (err) => {
      if (access && err?.code) access.error ||= err.code;
      recordAccess(access, 'close', access?.status || 499);
      closed = true;
      cleanup();
      client.destroy();
    };
    const fail = (err) => {
      if (closed) return;
      closed = true;
      if (access) {
        access.error = err.code || 'UPSTREAM_ERROR';
        if (!responseStarted) access.status = err.code === 'ETIMEDOUT' ? 504 : 502;
      }
      logError('WebSocket proxy error', req, err);
      cleanup();
      if (responseStarted || client.destroyed) client.destroy();
      else {
        const status = err.code === 'ETIMEDOUT' ? 504 : 502;
        const message = status === 504 ? 'Gateway Timeout' : 'Bad Gateway';
        client.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(message)}\r\n\r\n${message}`, () => client.destroy());
      }
    };
    client.on('error', close);
    client.on('close', close);
    if (stopping) {
      if (access) access.status = 503;
      client.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n', () => client.destroy());
      return;
    }

    upstreamRequest = transport.request(requestOptions(req, true));
    if (!upstream.proxied) watchConnection(upstreamRequest, connectTimeout, fail, secure);
    upstreamRequest.on('timeout', () => fail(timeoutError('WebSocket handshake became idle')));
    upstreamRequest.on('error', fail);
    upstreamRequest.on('upgrade', (response, socket, proxyHead) => {
      peer = socket;
      peer.on('error', fail);
      peer.on('close', () => {
        // Allow already-buffered bytes to drain on an orderly upstream close.
        if (!closed) client.end();
      });
      if (closed || client.destroyed) {
        peer.destroy();
        return;
      }
      responseStarted = true;
      if (access) access.status = response.statusCode;
      // Established WebSockets may remain quiet for arbitrarily long periods.
      peer.setTimeout(0);
      client.setTimeout(0);
      peer.setNoDelay(true);
      client.setNoDelay(true);
      peer.setKeepAlive(true, 60000);
      client.setKeepAlive(true, 60000);
      client.write(rawResponse(response, cleanHeaders(response.headers, true)));
      if (proxyHead.length) client.write(proxyHead);
      if (head.length) peer.write(head);
      peer.pipe(client);
      client.pipe(peer);
      recordAccess(access, 'upgrade', response.statusCode, false);
    });
    upstreamRequest.on('response', (response) => {
      // Preserve upstream handshake rejections (e.g. 401), rather than hanging.
      upstreamResponse = response;
      response.on('error', fail);
      response.on('aborted', () => fail(Object.assign(new Error('Upstream response aborted'), { code: 'ECONNRESET' })));
      if (closed || client.destroyed) {
        response.destroy();
        return;
      }
      responseStarted = true;
      if (access) access.status = response.statusCode;
      // The decoded stream has no chunk framing, so delimit it by connection close.
      const headers = { ...cleanHeaders(response.headers), ...corsHeaders, connection: 'close' };
      client.write(rawResponse(response, headers));
      response.pipe(client);
    });
    upstreamRequest.end();
  });

  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('error', () => socket.destroy());
    socket.once('close', () => sockets.delete(socket));
  });
  server.on('close', () => {
    upstream.destroy();
    logger?.close();
  });
  server.flushAccessLogs = () => logger?.close() || Promise.resolve();
  server.upstreamProxyDescription = upstream.description;
  server.shutdown = () => {
    if (stopping) return;
    stopping = true;
    const timer = setTimeout(() => {
      for (const socket of sockets) socket.destroy();
      upstream.destroy();
    }, shutdownTimeout);
    timer.unref();
    server.close(() => clearTimeout(timer));
    server.closeIdleConnections?.();
  };
  return server;
}

if (require.main === module) {
  const server = createProxyServer();
  const port = envNumber('PORT', 3000, 65535);
  server.listen(port, () => {
    console.log(`[${formatTimestamp()}] Proxy server running on port ${server.address().port}`);
    console.log(`[${formatTimestamp()}] Forwarding to: ${process.env.TARGET_DOMAIN || 'https://example.com'}`);
    console.log(`[${formatTimestamp()}] API key override: ${process.env.API_KEY ? 'enabled' : 'disabled'}`);
    console.log(`[${formatTimestamp()}] Upstream proxy: ${server.upstreamProxyDescription || 'direct'}`);
  });
  process.on('SIGTERM', () => server.shutdown());
  process.on('SIGINT', () => server.shutdown());
}

module.exports = { createProxyServer };
