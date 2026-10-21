/* Contador real de visitas, clics por producto y pedidos mayoristas.
   Guarda los números en Upstash Redis (se conecta desde Vercel > Storage). */
const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

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

// Suma 1 a la visita, al clic de un producto o al pedido
export async function POST(request) {
  if (!URL_ || !TOKEN) return json({ ok: false });
  try {
    const ua = request.headers.get('user-agent') || '';
    if (/bot|crawl|spider|preview|facebookexternalhit|slurp|headless/i.test(ua)) {
      return json({ ok: true, skip: true });
    }
    let b = {};
    try { b = await request.json(); } catch (e) {}
    let key = null;
    if (b.t === 'visita') key = 'v';
    else if (b.t === 'pedido') key = 'o';
    else if (b.t === 'producto' && IDS.includes(b.id)) key = 'p:' + b.id;
    if (!key) return json({ ok: false }, 400);
    const k = key + ':' + day(0);
    await redis([['INCR', k], ['EXPIRE', k, 3456000]]);
    return json({ ok: true });
  } catch (e) {
    return json({ ok: false });
  }
}

// Devuelve los números para mostrar en la página
export async function GET() {
  if (!URL_ || !TOKEN) return json({ ok: false });
  try {
    const d7 = [0, 1, 2, 3, 4, 5, 6].map(day);
    const d30 = Array.from({ length: 30 }, (_, i) => day(i));
    const keys = []
      .concat(d7.map(d => 'v:' + d))
      .concat(d30.map(d => 'o:' + d));
    IDS.forEach(id => d7.forEach(d => keys.push('p:' + id + ':' + d)));

    const out = await redis([['MGET', ...keys]]);
    const vals = (out[0].result || []).map(x => parseInt(x, 10) || 0);
    const sum = (a, from, n) => a.slice(from, from + n).reduce((s, x) => s + x, 0);

    const productos = {};
    IDS.forEach((id, i) => { productos[id] = sum(vals, 37 + i * 7, 7); });

    return json(
      { ok: true, visitas: sum(vals, 0, 7), pedidos: sum(vals, 7, 30), productos },
      200,
      's-maxage=60, stale-while-revalidate=300'
    );
  } catch (e) {
    return json({ ok: false });
  }
}
