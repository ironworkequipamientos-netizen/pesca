/* Datos del panel privado. Solo responde si se manda la clave correcta (variable PANEL_KEY en Vercel). */
import { createHash, timingSafeEqual } from 'node:crypto';

const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const PANEL_KEY = process.env.PANEL_KEY;

function same(a, b) {
  const x = createHash('sha256').update(String(a)).digest();
  const y = createHash('sha256').update(String(b)).digest();
  return timingSafeEqual(x, y);
}

function day(n) {
  return new Date(Date.now() - 3 * 3600e3 - n * 864e5).toISOString().slice(0, 10);
}

function json(body, status) {
  return new Response(JSON.stringify(body), {
    status: status || 200,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }
  });
}

export async function POST(request) {
  if (!URL_ || !TOKEN) return json({ ok: false, error: 'sin_base' });
  if (!PANEL_KEY) return json({ ok: false, error: 'sin_clave' });
  let b = {};
  try { b = JSON.parse(await request.text()); } catch (e) {}
  if (!b.k || !same(b.k, PANEL_KEY)) {
    await new Promise(r => setTimeout(r, 700));
    return json({ ok: false, error: 'clave' }, 401);
  }
  try {
    const n = Math.max(7, Math.min(90, parseInt(b.days, 10) || 30));
    const dates = [];
    for (let i = 2 * n - 1; i >= 0; i--) dates.push(day(i)); // del más viejo al más nuevo
    const r = await fetch(URL_ + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(dates.map(d => ['HGETALL', 'd:' + d]))
    });
    const out = await r.json();
    const days = dates.map((d, i) => {
      const raw = out[i] && out[i].result;
      const h = {};
      if (Array.isArray(raw)) { for (let j = 0; j < raw.length; j += 2) h[raw[j]] = parseInt(raw[j + 1], 10) || 0; }
      else if (raw && typeof raw === 'object') { Object.keys(raw).forEach(k => { h[k] = parseInt(raw[k], 10) || 0; }); }
      return { d, h };
    });
    return json({ ok: true, n, days });
  } catch (e) {
    return json({ ok: false, error: 'datos' });
  }
}
