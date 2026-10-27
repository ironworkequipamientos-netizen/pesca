/* Datos de Mercado Libre para el panel (solo lectura). Responde únicamente con la clave del panel. */
import { createHash, timingSafeEqual } from 'node:crypto';

const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const PANEL_KEY = process.env.PANEL_KEY;
const CID = process.env.ML_CLIENT_ID;
const CSECRET = process.env.ML_CLIENT_SECRET;
const API = 'https://api.mercadolibre.com';
const CACHE_SECONDS = 1800; // se guarda 30 minutos para no gastar llamadas

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  return r.json();
}
function same(a, b) {
  const x = createHash('sha256').update(String(a)).digest();
  const y = createHash('sha256').update(String(b)).digest();
  return timingSafeEqual(x, y);
}
function json(body, status) {
  return new Response(JSON.stringify(body), {
    status: status || 200,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }
  });
}

// Nombres de provincias (Mercado Libre a veces usa "Capital Federal" o "Bs.As. G.B.A. Norte")
const PROV = ['Buenos Aires', 'Capital Federal', 'Catamarca', 'Chaco', 'Chubut', 'Córdoba', 'Corrientes', 'Entre Ríos', 'Formosa', 'Jujuy', 'La Pampa', 'La Rioja', 'Mendoza', 'Misiones', 'Neuquén', 'Río Negro', 'Salta', 'San Juan', 'San Luis', 'Santa Cruz', 'Santa Fe', 'Santiago del Estero', 'Tierra del Fuego', 'Tucumán'];
function norm(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z ]/g, ' ').replace(/\s+/g, ' ').trim();
}
function canon(s) {
  const n = norm(s);
  if (!n || n === 'sin dato') return 'Sin dato';
  if (n.includes('capital federal') || n === 'caba' || n.includes('ciudad autonoma')) return 'Capital Federal';
  if (/^(bs as|buenos aires|gba)/.test(n)) return 'Buenos Aires';
  for (const p of PROV) { if (norm(p) === n) return p; }
  if (n.startsWith('tierra del fuego')) return 'Tierra del Fuego';
  if (n.startsWith('santiago del estero')) return 'Santiago del Estero';
  return String(s);
}
const PENDING_LIMIT = 25; // cuántas ubicaciones nuevas se leen por vez (así no se corta por tiempo)
const sleep = ms => new Promise(r => setTimeout(r, ms));
// Fecha en horario de Argentina (UTC-3), hace "n" días
function day(n) { return new Date(Date.now() - 3 * 3600e3 - n * 864e5).toISOString().slice(0, 10); }

// Acceso a Mercado Libre: dura 6 horas y se renueva solo
async function getToken() {
  const r = await redis([['GET', 'ml:tokens']]);
  if (!r[0].result) return { error: 'no_conectado' };
  let t = JSON.parse(r[0].result);
  if (t.expires_at - Date.now() > 5 * 60e3) return { t };
  const got = (await redis([['SET', 'ml:lock', '1', 'NX', 'EX', 20]]))[0].result;
  if (!got) {
    await sleep(2500);
    const r2 = await redis([['GET', 'ml:tokens']]);
    t = JSON.parse(r2[0].result);
    if (t.expires_at - Date.now() > 60e3) return { t };
  }
  const rr = await fetch(API + '/oauth/token', {
    method: 'POST',
    headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', client_id: CID, client_secret: CSECRET, refresh_token: t.refresh_token })
  });
  const nt = await rr.json().catch(() => ({}));
  if (!rr.ok || !nt.access_token) {
    await redis([['DEL', 'ml:lock']]);
    return { error: 'reconectar' };
  }
  t = {
    access_token: nt.access_token,
    refresh_token: nt.refresh_token || t.refresh_token,
    expires_at: Date.now() + (nt.expires_in || 21600) * 1000,
    user_id: nt.user_id || t.user_id
  };
  await redis([['SET', 'ml:tokens', JSON.stringify(t)], ['DEL', 'ml:lock']]);
  return { t };
}

async function api(path, t) {
  const r = await fetch(API + path, { headers: { Authorization: 'Bearer ' + t.access_token, accept: 'application/json' } });
  const body = await r.json().catch(() => ({}));
  if (r.status === 401 || r.status === 403) throw { code: 'auth', status: r.status, body };
  if (!r.ok) throw { code: 'api', status: r.status, body };
  return body;
}

async function pool(list, size, fn) {
  const out = new Array(list.length);
  let i = 0;
  await Promise.all(Array.from({ length: size }, async () => {
    while (i < list.length) { const k = i++; out[k] = await fn(list[k], k); }
  }));
  return out;
}

async function build(t, n) {
  const uid = t.user_id;
  // 1) publicaciones activas y pausadas
  const ids = [];
  for (const status of ['active', 'paused']) {
    for (let o = 0; o < 1000; o += 100) {
      const r = await api('/users/' + uid + '/items/search?status=' + status + '&limit=100&offset=' + o, t);
      (r.results || []).forEach(id => ids.push(id));
      if ((r.results || []).length < 100) break;
    }
  }
  // 2) datos de cada publicación
  const items = {};
  for (let i = 0; i < ids.length; i += 20) {
    const chunk = ids.slice(i, i + 20);
    const r = await api('/items?ids=' + chunk.join(','), t);
    (Array.isArray(r) ? r : []).forEach(x => {
      const b = x && x.body;
      if (b && b.id) {
        const st = b.start_time ? Math.floor((Date.now() - new Date(b.start_time).getTime()) / 864e5) : null;
        items[b.id] = {
          id: b.id, t: b.title, p: b.price, s: b.status, l: b.permalink, stock: b.available_quantity,
          img: String(b.secure_thumbnail || b.thumbnail || '').replace(/^http:/, 'https:'),
          pc: Array.isArray(b.pictures) ? b.pictures.length : null,
          fs: !!(b.shipping && b.shipping.free_shipping),
          lt: (b.shipping && b.shipping.logistic_type) || '',
          ty: b.listing_type_id || '',
          hl: typeof b.health === 'number' ? b.health : null,
          age: st, sold: Number(b.sold_quantity) || 0,
          visits: 0, units: 0, rev: 0, orders: 0, up: 0, rp: 0, ls: ''
        };
      }
    });
  }
  // 3) visitas por día de cada publicación
  const dailyV = {};
  let visitErrors = 0;
  await pool(Object.keys(items), 6, async id => {
    try {
      const r = await api('/items/' + id + '/visits/time_window?last=' + n + '&unit=day', t);
      let total = 0;
      (r.results || []).forEach(x => {
        const d = String(x.date || '').slice(0, 10);
        const v = Number(x.total) || 0;
        if (!d || d < day(n - 1) || d > day(0)) return;
        total += v;
        dailyV[d] = (dailyV[d] || 0) + v;
      });
      items[id].visits = total;
    } catch (e) {
      if (e && e.code === 'auth') throw e;
      visitErrors++;
    }
  });
  // 4) ventas de este período y del anterior (para comparar)
  const from = day(2 * n - 1) + 'T00:00:00.000-03:00';
  const to = day(0) + 'T23:59:59.999-03:00';
  const startCur = day(n - 1);
  const ords = [];
  for (let o = 0; o < 2000; o += 50) {
    const qs = new URLSearchParams({ seller: String(uid), 'order.date_created.from': from, 'order.date_created.to': to, sort: 'date_desc', limit: '50', offset: String(o) });
    const r = await api('/orders/search?' + qs.toString(), t);
    const res = r.results || [];
    res.forEach(ord => {
      if (ord.status === 'cancelled' || ord.status === 'invalid') return;
      const d = new Date(new Date(ord.date_created).getTime() - 3 * 3600e3).toISOString().slice(0, 10);
      ords.push({ id: String(ord.id), d, buyer: ord.buyer && ord.buyer.id ? String(ord.buyer.id) : '', ship: ord.shipping && ord.shipping.id ? String(ord.shipping.id) : '', items: ord.order_items || [] });
    });
    if (res.length < 50) break;
  }
  // 5) ubicación (provincia y ciudad) de cada venta. Se guarda solo "provincia|ciudad", sin datos personales.
  //    Clave nueva (ml:geo2): la anterior podía tener "Sin dato" guardado por error. Los errores NO se guardan.
  const known = {};
  for (let i = 0; i < ords.length; i += 100) {
    const chunk = ords.slice(i, i + 100);
    const r = await redis([['HMGET', 'ml:geo2', ...chunk.map(x => x.id)]]);
    (r[0].result || []).forEach((v, j) => { if (v != null) known[chunk[j].id] = v; });
  }
  const geoDiag = { orders: ords.length, noShip: 0, fromCache: Object.keys(known).length, resolved: 0, errors: {}, sample: '' };
  const todo = [];
  const writes = [];
  ords.forEach(x => {
    if (known[x.id] != null) return;
    if (!x.ship) { geoDiag.noShip++; known[x.id] = 'Sin envío|'; writes.push(['HSET', 'ml:geo2', x.id, 'Sin envío|']); }
    else todo.push(x);
  });
  const batch = todo.slice(0, PENDING_LIMIT);
  await pool(batch, 6, async x => {
    try {
      const r = await fetch(API + '/shipments/' + x.ship, { headers: { Authorization: 'Bearer ' + t.access_token, accept: 'application/json', 'x-format-new': 'true' } });
      if (!r.ok) { geoDiag.errors[r.status] = (geoDiag.errors[r.status] || 0) + 1; return; }
      const s = await r.json().catch(() => ({}));
      const dest = s.destination || {};
      const ad = dest.shipping_address || dest.receiver_address || s.shipping_address || s.receiver_address || {};
      const stName = ad.state && (ad.state.name || ad.state);
      const ctName = ad.city && (ad.city.name || ad.city);
      if (!stName) { geoDiag.errors.sin_direccion = (geoDiag.errors.sin_direccion || 0) + 1; if (!geoDiag.sample) geoDiag.sample = Object.keys(s).slice(0, 12).join(','); return; }
      const val = String(stName) + '|' + String(ctName || '');
      known[x.id] = val; geoDiag.resolved++;
      writes.push(['HSET', 'ml:geo2', x.id, val]);
    } catch (e) { geoDiag.errors.red = (geoDiag.errors.red || 0) + 1; }
  });
  if (writes.length) await redis(writes);
  const pending = ords.filter(x => known[x.id] == null).length;
  const stuck = pending > 0 && geoDiag.resolved === 0 && Object.keys(geoDiag.errors).length > 0;
  // 6) sumas por publicación, provincia y ciudad (período actual y anterior)
  const dailyO = {}, dailyU = {};
  const bc = {}, bp = {};
  const tot = { orders: 0, units: 0, rev: 0, ordersP: 0, unitsP: 0, revP: 0 };
  const prov = {}, city = {};
  const bump = (map, key, cur, u, r) => {
    const e = map[key] || (map[key] = { n: key, o: 0, u: 0, r: 0, op: 0, up: 0 });
    if (cur) { e.o += 1; e.u += u; e.r += r; } else { e.op += 1; e.up += u; }
  };
  ords.forEach(x => {
    const cur = x.d >= startCur;
    let ou = 0, orv = 0;
    x.items.forEach(oi => {
      const id = oi.item && oi.item.id;
      if (!id) return;
      const q = Number(oi.quantity) || 0, amount = q * (Number(oi.unit_price) || 0);
      if (!items[id]) items[id] = { id, t: (oi.item && oi.item.title) || id, p: Number(oi.unit_price) || 0, s: 'closed', l: '', stock: null, visits: 0, units: 0, rev: 0, orders: 0, up: 0, rp: 0 };
      if (cur) { items[id].units += q; items[id].rev += amount; items[id].orders += 1; } else { items[id].up += q; items[id].rp += amount; }
      ou += q; orv += amount;
    });
    if (cur) { tot.orders++; tot.units += ou; tot.rev += orv; dailyO[x.d] = (dailyO[x.d] || 0) + 1; dailyU[x.d] = (dailyU[x.d] || 0) + ou; }
    else { tot.ordersP++; tot.unitsP += ou; tot.revP += orv; }
    if (x.buyer) { const m = cur ? bc : bp; m[x.buyer] = (m[x.buyer] || 0) + 1; }
    x.items.forEach(oi => { const id = oi.item && oi.item.id; if (id && items[id] && x.d > (items[id].ls || '')) items[id].ls = x.d; });
    const loc = known[x.id];
    if (loc != null && loc !== 'Sin envío|') {
      const parts = loc.split('|');
      const pv = canon(parts[0]);
      bump(prov, pv, cur, ou, orv);
      if (parts[1]) bump(city, parts[1] + ' (' + pv + ')', cur, ou, orv);
    }
  });
  const sortGeo = map => Object.keys(map).map(k => map[k]).sort((a, b) => (b.o - a.o) || (b.op - a.op));
  const geo = { prov: sortGeo(prov), city: sortGeo(city).slice(0, 25) };
  const daily = [];
  for (let i = n - 1; i >= 0; i--) { const d = day(i); daily.push({ d, v: dailyV[d] || 0, o: dailyO[d] || 0, u: dailyU[d] || 0 }); }
  const bk = Object.keys(bc);
  const buyers = { unique: bk.length, repeat: bk.filter(k => bc[k] > 1).length, returning: bk.filter(k => bp[k]).length, uniqueP: Object.keys(bp).length };
  const list = Object.keys(items).map(k => items[k]).sort((a, b) => (b.units - a.units) || (b.visits - a.visits));
  const visits = list.reduce((s, x) => s + x.visits, 0);
  return { ok: true, n, generated: Date.now(), totals: { visits, orders: tot.orders, units: tot.units, rev: tot.rev, ordersP: tot.ordersP, unitsP: tot.unitsP, revP: tot.revP, visitErrors }, daily, items: list, geo, pending, geoDiag, stuck, buyers };
}

export async function POST(request) {
  if (!URL_ || !TOKEN) return json({ ok: false, error: 'sin_base' });
  if (!PANEL_KEY) return json({ ok: false, error: 'sin_clave' });
  if (!CID || !CSECRET) return json({ ok: false, error: 'sin_ml' });
  let b = {};
  try { b = JSON.parse(await request.text()); } catch (e) {}
  if (!b.k || !same(b.k, PANEL_KEY)) {
    await new Promise(r => setTimeout(r, 700));
    return json({ ok: false, error: 'clave' }, 401);
  }
  const n = Math.max(7, Math.min(90, parseInt(b.days, 10) || 30));
  try {
    // Importar ubicaciones desde el reporte de ventas de Mercado Libre: [[nºventa, provincia, ciudad], ...]
    if (Array.isArray(b.import)) {
      const rows = b.import.slice(0, 6000).filter(r => Array.isArray(r) && /^\d{6,20}$/.test(String(r[0])) && r[1]);
      const cmds = rows.map(r => ['HSET', 'ml:geo2', String(r[0]), String(r[1]).slice(0, 60) + '|' + String(r[2] || '').slice(0, 60)]);
      for (let i = 0; i < cmds.length; i += 500) await redis(cmds.slice(i, i + 500));
      await redis([['DEL', 'ml:cache:7', 'ml:cache:30', 'ml:cache:90']]);
      return json({ ok: true, imported: rows.length });
    }
    if (!b.refresh) {
      const c = await redis([['GET', 'ml:cache:' + n]]);
      if (c[0].result) return json(Object.assign(JSON.parse(c[0].result), { cached: true }));
    }
    const tk = await getToken();
    if (tk.error) return json({ ok: false, error: tk.error });
    const data = await build(tk.t, n);
    if (!data.pending && !data.stuck) await redis([['SET', 'ml:cache:' + n, JSON.stringify(data), 'EX', CACHE_SECONDS]]);
    return json(data);
  } catch (e) {
    if (e && e.code === 'auth') return json({ ok: false, error: 'permisos', status: e.status });
    return json({ ok: false, error: 'ml', status: e && e.status, detail: e && e.body && (e.body.message || e.body.error) });
  }
}
