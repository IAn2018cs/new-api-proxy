const http = require('http');
const https = require('https');
const { URL } = require('url');

const TARGET_DOMAIN = process.env.TARGET_DOMAIN || 'https://example.com';
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || '';

const targetUrl = new URL(TARGET_DOMAIN);
const isHttps = targetUrl.protocol === 'https:';
const httpModule = isHttps ? https : http;

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
  'Access-Control-Max-Age': '86400',
};

const server = http.createServer((req, res) => {
  // Handle preflight OPTIONS request
  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders);
    res.end();
    return;
  }

  const headers = { ...req.headers, host: targetUrl.host };

  if (API_KEY) {
    if (headers['authorization']) {
      headers['authorization'] = `Bearer ${API_KEY}`;
    }
    if (headers['x-api-key']) {
      headers['x-api-key'] = API_KEY;
    }
    if (headers['api-key']) {
      headers['api-key'] = API_KEY;
    }
    if (!headers['authorization'] && !headers['x-api-key'] && !headers['api-key']) {
      headers['authorization'] = `Bearer ${API_KEY}`;
    }
  }

  const options = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || (isHttps ? 443 : 80),
    path: req.url,
    method: req.method,
    headers,
  };

  const proxyReq = httpModule.request(options, (proxyRes) => {
    // Merge CORS headers with response headers
    const headers = { ...proxyRes.headers, ...corsHeaders };
    res.writeHead(proxyRes.statusCode, headers);
    proxyRes.pipe(res);
  });

  proxyReq.on('error', (err) => {
    console.error('Proxy error:', err.message);
    res.writeHead(502);
    res.end('Bad Gateway');
  });

  req.pipe(proxyReq);
});

// WebSocket upgrade handling
server.on('upgrade', (req, socket, head) => {
  const headers = { ...req.headers, host: targetUrl.host };

  if (API_KEY) {
    if (headers['authorization']) {
      headers['authorization'] = `Bearer ${API_KEY}`;
    }
    if (headers['x-api-key']) {
      headers['x-api-key'] = API_KEY;
    }
    if (headers['api-key']) {
      headers['api-key'] = API_KEY;
    }
    if (!headers['authorization'] && !headers['x-api-key'] && !headers['api-key']) {
      headers['authorization'] = `Bearer ${API_KEY}`;
    }
  }

  const options = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || (isHttps ? 443 : 80),
    path: req.url,
    method: req.method,
    headers,
  };

  const proxyReq = httpModule.request(options);

  proxyReq.on('upgrade', (proxyRes, proxySocket, proxyHead) => {
    socket.write(
      `HTTP/1.1 101 Switching Protocols\r\n` +
      Object.entries(proxyRes.headers)
        .map(([k, v]) => `${k}: ${v}`)
        .join('\r\n') +
      '\r\n\r\n'
    );

    if (proxyHead.length > 0) {
      socket.write(proxyHead);
    }

    proxySocket.pipe(socket);
    socket.pipe(proxySocket);
  });

  proxyReq.on('error', (err) => {
    console.error('WebSocket proxy error:', err.message);
    socket.end();
  });

  proxyReq.end();
});

server.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
  console.log(`Forwarding to: ${TARGET_DOMAIN}`);
  console.log(`API key override: ${API_KEY ? 'enabled' : 'disabled'}`);
});
