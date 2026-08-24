/* global process, Buffer */
//+------------------------------------------------------------------+
//| TradeBot Pro — Stateful WSS Relay                                 |
//|                                                                  |
//| PROPÓSITO: Puente persistente entre el EA (MQL5 WSS nativo) y     |
//|   el backend serverless de Base44 (HTTP POST).                    |
//|                                                                  |
//| ARQUITECTURA:                                                     |
//|   EA (MQL5 WSS) ──persistent──→ este relay ──HTTP POST──→ Base44  |
//|                                                                  |
//| DESPLIEGUE:                                                       |
//|   1. Sube package.json + server.js a un repo de GitHub            |
//|   2. Render.com → New Web Service → conecta el repo               |
//|   3. Build: npm install | Start: npm start | Plan: Free           |
//|   4. Obtén URL: wss://tradebotpro-relay.onrender.com/ws           |
//|   5. Configura en el EA: InpServerHost = "tradebotpro-relay.onrender.com"
//|      InpServerPort = 443 | InpWssPath = "/ws"                     |
//|                                                                  |
//| MENSAJES (EA → Relay):                                            |
//|   { type: "ping", client_ts, seq }      → latencia probe          |
//|   { type: "tick", pair, bid, ask, ts }   → telemetría tick         |
//|   { type: "signal_request", body }       → evaluar señal          |
//|                                                                  |
//| MENSAJES (Relay → EA):                                            |
//|   { type: "welcome", server_ts }         → handshake inicial      |
//|   { type: "pong", server_ts, client_ts } → respuesta latencia     |
//|   { type: "signal_response", status, data } → resultado eval     |
//+------------------------------------------------------------------+
import { WebSocketServer } from 'ws';
import http from 'http';

// --- Configuración ---
const PORT = process.env.PORT || 8080;
const BASE44_APP_HOST = process.env.BASE44_APP_HOST || 'smart-trade-pulse-sophisticated.base44.app';
const SIGNAL_ENDPOINT = process.env.SIGNAL_ENDPOINT || '/functions/mt5Webhook';
const MAX_TICK_BUFFER = 5000;

// --- Telemetría en memoria (ring buffer por par) ---
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
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data) });
        } catch (e) {
          resolve({ status: res.statusCode, data: data });
        }
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

// --- Servidor WSS ---
const wss = new WebSocketServer({ port: PORT, path: '/ws' });

wss.on('connection', (ws, req) => {
  const clientId = ++clientIdCounter;
  const clientIp = req.socket.remoteAddress;
  clientStats[clientId] = { connectedAt: Date.now(), ip: clientIp, pings: 0, ticks: 0, signals: 0 };
  console.log(`[+] Client #${clientId} connected from ${clientIp}`);

  ws.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      console.warn(`[!] Client #${clientId}: invalid JSON — ${e.message}`);
      return;
    }

    switch (msg.type) {
      case 'ping': {
        clientStats[clientId].pings++;
        const pong = { type: 'pong', server_ts: Date.now(), client_ts: msg.client_ts, seq: msg.seq };
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(pong));
        break;
      }
      case 'tick': {
        clientStats[clientId].ticks++;
        storeTick(msg.pair, msg.bid, msg.ask, msg.ts);
        break;
      }
      case 'signal_request': {
        clientStats[clientId].signals++;
        try {
          const result = await postToBase44(SIGNAL_ENDPOINT, msg.body || {});
          if (ws.readyState === ws.OPEN) {
            ws.send(JSON.stringify({ type: 'signal_response', request_id: msg.request_id || Date.now(), status: result.status, data: result.data }));
          }
        } catch (e) {
          console.error(`[!] Client #${clientId}: signal request failed — ${e.message}`);
          if (ws.readyState === ws.OPEN) {
            ws.send(JSON.stringify({ type: 'signal_response', request_id: msg.request_id || Date.now(), status: 500, error: e.message }));
          }
        }
        break;
      }
      default:
        console.warn(`[?] Client #${clientId}: unknown message type "${msg.type}"`);
    }
  });

  ws.on('close', () => {
    const stats = clientStats[clientId];
    const uptime = stats ? Math.floor((Date.now() - stats.connectedAt) / 1000) : 0;
    console.log(`[-] Client #${clientId} disconnected (uptime ${uptime}s, pings:${stats?.pings||0} ticks:${stats?.ticks||0} signals:${stats?.signals||0})`);
    delete clientStats[clientId];
  });

  ws.on('error', (err) => console.error(`[!] Client #${clientId} socket error: ${err.message}`));

  ws.send(JSON.stringify({ type: 'welcome', server_ts: Date.now(), message: 'TradeBot Pro Relay — connected' }));
});

// --- Health check HTTP (para uptime monitoring de Render/Railway) ---
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    const activeClients = wss.clients.size;
    const pairs = Object.keys(tickBuffers);
    const totalTicks = Object.values(tickBuffers).reduce((s, b) => s + b.length, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime(), activeClients, pairs, totalTicks, base44Host: BASE44_APP_HOST }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

healthServer.listen(PORT + 1, () => console.log(`[i] Health check on http://localhost:${PORT + 1}/health`));

console.log(`=== TradeBot Pro WSS Relay ===`);
console.log(`[i] WSS listening on port ${PORT} (path: /ws)`);
console.log(`[i] Forwarding signal requests to https://${BASE44_APP_HOST}${SIGNAL_ENDPOINT}`);
