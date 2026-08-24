/* global process */
import http from 'http';

const PORT = process.env.PORT || 8080;
const BASE44_APP_HOST = process.env.BASE44_APP_HOST || 'smart-trade-pulse-sophisticated.base44.app';
const SIGNAL_ENDPOINT = process.env.SIGNAL_ENDPOINT || '/functions/mt5Webhook';

const server = http.createServer((req, res) => {
  // Manejo de ping / latencia ultrarrápida
  if (req.url === '/ping' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', server_ts: Date.now() }));
    return;
  }
  
  // Reenvío de señales hacia Base44
  if (req.url === '/signal') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const options = {
        hostname: BASE44_APP_HOST,
        port: 443,
        path: SIGNAL_ENDPOINT,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      };
      const proxyReq = http.request(options, proxyRes => {
        let resData = '';
        proxyRes.on('data', chunk => { resData += chunk; });
        proxyRes.on('end', () => {
          res.writeHead(proxyRes.statusCode, { 'Content-Type': 'application/json' });
          res.end(resData);
        });
      });
      proxyReq.on('error', e => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      });
      proxyReq.write(body);
      proxyReq.end();
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`=== TradeBot Pro HTTP Bridge Active on port ${PORT} ===`);
});
