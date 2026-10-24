/* Registro de visitas, clics y pedidos de la página (todo se guarda por día en Upstash Redis).
   - POST: la página manda los eventos que van pasando.
   - GET: devuelve los números chicos que se muestran en la página (visitas, clics por producto, pedidos). */
const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const KEEP_SECONDS = 400 * 86400; // se guardan ~13 meses

const IDS = [
  'haragan-pesca-aluminio-1-20-mts',
  'haragan-pesca-aluminio-1-50-mts',
  'haragan-pesca-con-bandeja-chica-1-20-mts',
  'haragan-pesca-con-bandeja-grande-1-20-mts',
  'haragan-pesca-con-bandeja-grande-1-50-mts',
  'haragan-extensible-75-cm-140-cm',
  'haragan-extensible-con-bandeja-grande-75-cm-140-cm',
  'kit-posacana-25-mm',
  'kit-posacana-32-mm',
  'kit-posacana-automatico-25-mm',
  'bandeja-organizadora-de-pesca-25-mm',
  'bandeja-organizadora-de-pesca-32-mm',
  'bandeja-chica-para-carnada-25-mm',
  'kit-basico-pescador',
  'soporte-con-linterna-posacana-25-mm',
  'asistente-pesca-con-bandeja'
];
const STORES = ['corsario', 'shark', 'pesca-ian', 'pae-pesca', 'la-triestina', 'punto-pesca', 'alma-mia', 'surucho', 'carlitos', 'fly-shop'];
const SOURCES = ['instagram', 'youtube', 'google', 'facebook', 'whatsapp', 'qr', 'directo', 'otros'];

// Lista de eventos permitidos (cualquier otro se ignora)
const ALLOWED = new Set([
  'v', 'conv',
  'click_producto', 'click_tienda', 'click_ver_productos', 'click_whatsapp', 'click_mayorista', 'click_carrusel',
  'sec_promesa', 'sec_dolores', 'sec_deseos', 'sec_linea', 'sec_accesorios', 'sec_pasos', 'sec_faq', 'sec_puntos', 'sec_final',
  'm_visita', 'm_producto', 'm_datos', 'm_pedido', 'm_unidades'
]);
IDS.forEach(id => ALLOWED.add('p_' + id));
STORES.forEach(s => ALLOWED.add('ll_' + s));
SOURCES.forEach(s => { ALLOWED.add('src_' + s); ALLOWED.add('conv_s_' + s); });
['mobile', 'desktop'].forEach(d => { ALLOWED.add('dev_' + d); ALLOWED.add('conv_d_' + d); });

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  return r.json();
}

// Fecha en horario de Argentina (UTC-3), hace "n" días
function day(n) {
  return new Date(Date.now() - 3 * 3600e3 - n * 864e5).toISOString().slice(0, 10);
}

function json(body, status, cache) {
  return new Response(JSON.stringify(body), {
    status: status || 200,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': cache || 'no-store' }
  });
}

export async function POST(request) {
  if (!URL_ || !TOKEN) return json({ ok: false });
  try {
    const ua = request.headers.get('user-agent') || '';
    if (/bot|crawl|spider|preview|facebookexternalhit|slurp|headless/i.test(ua)) {
      return json({ ok: true, skip: true });
    }
    let b = {};
    try { b = JSON.parse(await request.text()); } catch (e) {}
    const list = Array.isArray(b.e) ? b.e.slice(0, 30) : [];
    const key = 'd:' + day(0);
    const cmds = [];
    list.forEach(x => {
      if (!x || !ALLOWED.has(x.n)) return;
      let amount = 1;
      if (x.n === 'm_unidades') amount = Math.max(1, Math.min(5000, parseInt(x.v, 10) || 1));
      cmds.push(['HINCRBY', key, x.n, amount]);
    });
    if (!cmds.length) return json({ ok: false }, 400);
    cmds.push(['EXPIRE', key, KEEP_SECONDS]);
    await redis(cmds);
    return json({ ok: true });
  } catch (e) {
    return json({ ok: false });
  }
}

// Números que se muestran en la página (se cachean 15 minutos)
export async function GET() {
  if (!URL_ || !TOKEN) return json({ ok: false });
  try {
    const fields = ['v'].concat(IDS.map(id => 'p_' + id));
    const d7 = [0, 1, 2, 3, 4, 5, 6].map(day);
    const d30 = Array.from({ length: 30 }, (_, i) => day(i));
    const cmds = d7.map(d => ['HMGET', 'd:' + d, ...fields])
      .concat(d30.map(d => ['HGET', 'd:' + d, 'm_pedido']));
    const out = await redis(cmds);
    const n = x => parseInt(x, 10) || 0;
    let visitas = 0;
    const productos = {};
    IDS.forEach(id => { productos[id] = 0; });
    for (let i = 0; i < 7; i++) {
      const row = out[i].result || [];
      visitas += n(row[0]);
      IDS.forEach((id, j) => { productos[id] += n(row[j + 1]); });
    }
    let pedidos = 0;
    for (let i = 7; i < 37; i++) pedidos += n(out[i].result);
    return json({ ok: true, visitas, pedidos, productos }, 200, 's-maxage=900, stale-while-revalidate=1800');
  } catch (e) {
    return json({ ok: false });
  }
}
