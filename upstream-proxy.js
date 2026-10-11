const http = require('node:http');
const https = require('node:https');
const net = require('node:net');
const tls = require('node:tls');
const dns = require('node:dns/promises');
const { once } = require('node:events');

function proxyConfig(value) {
  if (value === undefined || value === '') return undefined;
  let url;
  try {
    url = new URL(value);
    decodeURIComponent(url.username);
    decodeURIComponent(url.password);
  } catch {
    // URL parser/decoder errors can contain the original credentials.
    throw new Error('Invalid UPSTREAM_PROXY_URL: expected a valid proxy URL');
  }
  if (!['http:', 'https:', 'socks5:', 'socks5h:'].includes(url.protocol)) {
    throw new Error('UPSTREAM_PROXY_URL supports http://, https://, socks5:// and socks5h://');
  }
  if (!url.hostname || url.port === '0' || url.pathname && url.pathname !== '/' || url.search || url.hash) {
    throw new Error('Invalid UPSTREAM_PROXY_URL: expected a proxy host and port without path, query or fragment');
  }
  if (url.protocol.startsWith('socks5') &&
    (Buffer.byteLength(decodeURIComponent(url.username)) > 255 || Buffer.byteLength(decodeURIComponent(url.password)) > 255)) {
    throw new Error('Invalid UPSTREAM_PROXY_URL: SOCKS credentials must fit within 255 bytes');
  }
  return url;
}

function codedError(code, message) {
  return Object.assign(new Error(message), { code });
}

function createUpstreamAgents({ target, proxyUrl, freeSocketTimeout, connectTimeout, tlsOptions = {} }) {
  const proxy = proxyConfig(proxyUrl);
  const poolOptions = { keepAlive: true, scheduling: 'lifo', maxFreeSockets: 256, timeout: freeSocketTimeout };
  if (!proxy) {
    const agent = new (target.protocol === 'https:' ? https : http).Agent({ ...tlsOptions, ...poolOptions });
    return { agent, freshAgent: false, proxied: false, cancel() {}, destroy() { agent.destroy(); } };
  }

  // These namespaces print credential-bearing proxy URLs or headers when DEBUG=*.
  const debug = require('debug');
  debug.enable(`${debug.disable()},-http-proxy-agent,-https-proxy-agent,-socks-proxy-agent`);
  const { HttpProxyAgent } = require('http-proxy-agent');
  const { HttpsProxyAgent } = require('https-proxy-agent');
  const { SocksProxyAgent } = require('socks-proxy-agent');
  const { SocksClient } = require('socks');
  const socks = proxy.protocol.startsWith('socks5');
  const AgentType = socks ? SocksProxyAgent : target.protocol === 'https:' ? HttpsProxyAgent : HttpProxyAgent;
  const connecting = new Map();

  class UpstreamAgent extends AgentType {
    constructor(keepAlive) {
      const options = { ...tlsOptions, ...poolOptions, keepAlive };
      super(proxy, options);
      // HttpsProxyAgent replaces options; retain idle-pool settings and custom CA.
      this.options = { ...this.options, ...options };
      this.protocol = target.protocol;
      this.defaultPort = target.protocol === 'https:' ? 443 : 80;
    }

    setRequestProps(req, opts) {
      const originalPath = req.path;
      super.setRequestProps(req, opts);
      // Preserve encoded paths/dot segments, and always use the configured target.
      req.path = `${target.origin}${originalPath.startsWith('/') ? originalPath : `/${originalPath}`}`;
    }

    async connect(req, opts) {
      const controller = new AbortController();
      const { signal } = controller;
      connecting.set(req, controller);
      const cancel = () => controller.abort(codedError('ABORT_ERR', 'Upstream connection canceled'));
      req.once('close', cancel);
      const timer = connectTimeout ? setTimeout(() => {
        controller.abort(codedError('ETIMEDOUT', 'Upstream proxy connection timed out'));
      }, connectTimeout) : undefined;
      timer?.unref();
      let rejectAbort;
      const aborted = new Promise((_resolve, reject) => { rejectAbort = () => reject(signal.reason); });
      signal.addEventListener('abort', rejectAbort, { once: true });
      const establish = async () => {
        if (req.destroyed) cancel();
        signal.throwIfAborted();
        let socket;
        if (socks) {
          // Own the transport socket so cancellation during DNS/auth/negotiation
          // closes it immediately; SocksProxyAgent alone does not expose it then.
          let host = opts.host;
          if (this.shouldLookup && !net.isIP(host)) host = (await dns.lookup(host)).address;
          signal.throwIfAborted();
          socket = net.connect({ host: proxy.hostname.replace(/^\[|\]$/g, ''), port: Number(proxy.port) || 1080, signal });
          socket.on('error', () => {});
          await once(socket, 'connect');
          const result = await SocksClient.createConnection({
            proxy: this.proxy, destination: { host, port: Number(opts.port) }, command: 'connect',
            existing_socket: socket, timeout: connectTimeout || 30000,
          });
          socket = result.socket;
          if (opts.secureEndpoint) {
            socket = tls.connect({ ...tlsOptions, host: opts.host, socket, signal,
              servername: net.isIP(opts.host) ? undefined : opts.host });
          }
        } else {
          const proxyHost = proxy.hostname.replace(/^\[|\]$/g, '');
          const connectOpts = { ...this.connectOpts, signal,
            servername: net.isIP(proxyHost) ? undefined : proxyHost };
          if (AgentType === HttpProxyAgent) {
            // addRequest() already prepares the absolute URL and proxy auth.
            // Rebuilding buffered headers in HttpProxyAgent.connect() can send
            // duplicate headers when upload chunks arrive during async connect.
            socket = proxy.protocol === 'https:' ? tls.connect(connectOpts) : net.connect(connectOpts);
            socket.on('error', () => {});
            await once(socket, 'connect');
          } else {
            // Scope connection-specific signals without mutating the pool.
            const scope = Object.create(this);
            scope.connectOpts = connectOpts;
            const verify = opts.checkServerIdentity || tls.checkServerIdentity;
            socket = await super.connect.call(scope, req, { ...opts, signal,
              checkServerIdentity: (_hostname, certificate) => verify(opts.host, certificate) });
          }
        }
        socket.on('error', () => {});
        if (socket.encrypted && socket.secureConnecting) await once(socket, 'secureConnect');
        signal.throwIfAborted();
        return socket;
      };
      try {
        return await Promise.race([establish(), aborted]);
      } catch (err) {
        err.code ||= 'EPROXY_CONNECTION';
        controller.abort(err);
        throw signal.reason || err;
      } finally {
        clearTimeout(timer);
        signal.removeEventListener('abort', rejectAbort);
        req.removeListener('close', cancel);
        connecting.delete(req);
      }
    }
  }

  const agent = new UpstreamAgent(true);
  const freshAgent = new UpstreamAgent(false);
  return {
    agent, freshAgent, proxied: true,
    description: `${proxy.protocol}//${proxy.host}`,
    cancel(req) { connecting.get(req)?.abort(codedError('ABORT_ERR', 'Upstream connection canceled')); },
    destroy() {
      for (const controller of connecting.values()) controller.abort(codedError('ABORT_ERR', 'Proxy shutting down'));
      agent.destroy();
      freshAgent.destroy();
    },
  };
}

module.exports = { createUpstreamAgents, proxyConfig };
