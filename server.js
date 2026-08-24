/* global process, Buffer */
import http from 'http';

const PORT = process.env.PORT || 8080;
const BASE44_APP_HOST = process.env.BASE44_APP_HOST || 'smart-trade-pulse-sophisticated.base44.app';

// Función auxiliar para hacer proxy HTTP hacia el backend serverless de Base44
function forwardToBase44(targetPath, bodyStr) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: BASE44_APP_HOST,
      port: 443,
      path: targetPath,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        'User-Agent': 'TradeBotPro-Relay/2.0'
      },
      timeout: 8000
    };

    const req = http.request(options, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        resolve({ statusCode: res.statusCode, data });
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Base44 timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

const server = http.createServer((req, res) => {
  // 1. Health check / Ping de latencia
  if (req.url === '/ping' || req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', server_ts: Date.now() }));
    return;
  }

  // 2. Endpoints operativos del EA (/signal, /confirm, /pnl, /shadow)
  const allowedEndpoints = ['/signal', '/confirm', '/pnl', '/shadow'];
  if (allowedEndpoints.includes(req.url)) {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        // Redirigimos al webhook principal de Base44
        const base44Path = `/functions/mt5Webhook`; 
        const result = await forwardToBase44(base44Path, body);
        
        res.writeHead(result.statusCode, { 'Content-Type': 'application/json' });
        res.end(result.data);
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 500, error: e.message }));
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`=== TradeBot Pro HTTP Bridge (v2) Active on port ${PORT} ===`);
});
