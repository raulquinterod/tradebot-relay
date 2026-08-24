/* global process, Buffer */
import { WebSocketServer } from 'ws';
import http from 'http';

// --- Configuración ---
const PORT = process.env.PORT || 8080;
const BASE44_APP_HOST = process.env.BASE44_APP_HOST || 'smart-trade-pulse-sophisticated.base44.app';
const SIGNAL_ENDPOINT = process.env.SIGNAL_ENDPOINT || '/functions/mt5Webhook';
const MAX_TICK_BUFFER = 5000;

const tickBuffers = {};
const clientStats = {};
let clientIdCounter = 0;

// --- HTTP helper: POST a Base44 serverless ---
function postToBase44(path, payload) {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(payload);
    const options = {
      hostname: BASE44_APP_HOST,
      port: 443,
      path: path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'TradeBotPro-Relay/1.0',
        'Accept': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
      },
      timeout: 5000,
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); } 
        catch (e) { resolve({ status: res.statusCode, data: data }); }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Base44 request timeout')); });
    req.write(bodyStr);
    req.end();
  });
}

function storeTick(pair, bid, ask, ts) {
  if (!tickBuffers[pair]) tickBuffers[pair] = [];
  const buf = tickBuffers[pair];
  buf.push({ bid, ask, ts, recvAt: Date.now() });
  if (buf.length > MAX_TICK_BUFFER) buf.shift();
}

// --- 1. Servidor HTTP Unificado (Health Check) ---
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

// --- 2. Servidor WSS montado sobre el mismo HTTP ---
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const clientId = ++clientIdCounter;
  const clientIp = req.socket.remoteAddress;
  clientStats[clientId] = { connectedAt: Date.now(), ip: clientIp, pings: 0, ticks: 0, signals: 0 };
  console.log(`[+] Client #${clientId} connected from ${clientIp}`);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } 
    catch (e) { return; }

    switch (msg.type) {
      case 'ping':
        clientStats[clientId].pings++;
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type: 'pong', server_ts: Date.now(), client_ts: msg.client_ts, seq: msg.seq }));
        break;
      case 'tick':
        clientStats[clientId].ticks++;
        storeTick(msg.pair, msg.bid, msg.ask, msg.ts);
        break;
      case 'signal_request':
        clientStats[clientId].signals++;
        try {
          const result = await postToBase44(SIGNAL_ENDPOINT, msg.body || {});
          if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type: 'signal_response', request_id: msg.request_id || Date.now(), status: result.status, data: result.data }));
        } catch (e) {
          if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type: 'signal_response', request_id: msg.request_id || Date.now(), status: 500, error: e.message }));
        }
        break;
    }
  });

  ws.on('close', () => delete clientStats[clientId]);
  ws.send(JSON.stringify({ type: 'welcome', server_ts: Date.now(), message: 'TradeBot Pro Relay — connected' }));
});

// --- 3. Encendido en un ÚNICO puerto ---
server.listen(PORT, () => {
  console.log(`=== TradeBot Pro WSS Relay ===`);
  console.log(`[i] Server listening on port ${PORT}`);
  console.log(`[i] WebSocket ready on path: /ws`);
});
