// Serves index.html and runs the physics on the server. Each browser tab gets its own thread (so its own
// CPU core) and receives the particle frames over a WebSocket. Falls back to the page's own worker if
// the server is full or unreachable.
const http = require('http'), fs = require('fs'), path = require('path');
const { Worker } = require('worker_threads');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// each running simulation fills one core, so keep this at or below the vCPUs of your Railway plan
const MAX_SESSIONS = +process.env.MAX_SESSIONS || 4;
const MAX_FPS = +process.env.MAX_FPS || 30;
const OFF = -200, RANGE = 1800;
let sessions = 0;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    return fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html missing'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  }
  res.writeHead(404); res.end('not found');
});

// gauge samples must not be lost when a frame is skipped, so skipped frames fold into the next one
function mergeScope(a, b) {
  if (!a || a.epoch !== b.epoch) return b;
  b.from = a.from;
  b.t = Array.from(a.t).concat(Array.from(b.t));
  b.v = Array.from(a.v).concat(Array.from(b.v));
  return b;
}
function pack(m, scope, hud) {
  const n = m.n;
  const hb = Buffer.from(JSON.stringify({ t: 'frame', n, a: m.a, plateHalf: m.plateHalf, plen: m.plen,
    bolts: m.bolts, gauges: m.gauges, scope: scope && Object.assign({}, scope, { t: Array.from(scope.t), v: Array.from(scope.v) }), hud }));
  const hl = (hb.length + 3) & ~3, ab = new ArrayBuffer(4 + hl + 9 * n);
  new DataView(ab).setUint32(0, hl, true);
  new Uint8Array(ab, 4, hl).fill(32);
  new Uint8Array(ab, 4, hb.length).set(hb);
  const q = new Uint16Array(ab, 4 + hl, 2 * n), k = 65535 / RANGE, xy = m.b.xy;
  for (let i = 0; i < 2 * n; i++) { const v = Math.round((xy[i] - OFF) * k); q[i] = v < 0 ? 0 : v > 65535 ? 65535 : v; }
  new Uint32Array(ab, 4 + hl + 4 * n, n).set(m.b.col.subarray(0, n));
  new Uint8Array(ab, 4 + hl + 8 * n, n).set(m.b.glow.subarray(0, n));
  return new Uint8Array(ab);
}

const wss = new WebSocketServer({ server, path: '/ws', perMessageDeflate: false });
wss.on('connection', (ws) => {
  if (sessions >= MAX_SESSIONS) { ws.close(1013, 'busy'); return; }
  sessions++;
  const w = new Worker(path.join(__dirname, 'server-sim.js'));
  let pending = null, hudLatest = null, lastSend = 0, closed = false;
  w.on('message', (m) => {
    if (closed || ws.readyState !== 1) return;
    if (m.t !== 'frame') { ws.send(JSON.stringify(m)); return; }
    if (m.scope) pending = mergeScope(pending, m.scope);
    if (m.hud) hudLatest = m.hud;
    const now = Date.now();
    if (ws.bufferedAmount < 262144 && now - lastSend >= 1000 / MAX_FPS) {
      ws.send(pack(m, pending, hudLatest));
      pending = null; hudLatest = null; lastSend = now;
    }
    const B = m.b;
    w.postMessage({ t: 'ret', b: B }, [B.xy.buffer, B.col.buffer, B.glow.buffer]);
  });
  w.on('error', (e) => { console.error('sim error', e); ws.close(1011, 'sim error'); });
  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (m && typeof m.t === 'string') w.postMessage(m);
  });
  const end = () => { if (closed) return; closed = true; sessions--; w.terminate(); };
  ws.on('close', end); ws.on('error', end);
});

server.listen(PORT, '0.0.0.0', () => console.log('listening on', PORT, '· up to', MAX_SESSIONS, 'simulations'));
