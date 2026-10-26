/* Conexión con Mercado Libre.
   - POST (solo con la clave del panel): devuelve el link para autorizar la aplicación.
   - GET: es donde Mercado Libre te devuelve después de autorizar; guarda el acceso de forma privada. */
import { createHash, timingSafeEqual, randomBytes } from 'node:crypto';

const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const PANEL_KEY = process.env.PANEL_KEY;
const CID = process.env.ML_CLIENT_ID;
const CSECRET = process.env.ML_CLIENT_SECRET;
const REDIRECT = process.env.ML_REDIRECT_URI || 'https://ironworkequipamientos.vercel.app/api/ml-callback';

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

function page(msg, ok) {
  const html = '<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex"><title>Conexión con Mercado Libre</title></head>' +
    '<body style="background:#070707;color:#fff;font-family:Arial,sans-serif;display:grid;place-items:center;min-height:100vh;margin:0;padding:20px;text-align:center">' +
    '<div style="max-width:420px"><h1 style="color:' + (ok ? '#2fd27a' : '#ff6a00') + ';font-size:1.4rem">' + (ok ? 'Listo' : 'No se pudo conectar') + '</h1>' +
    '<p style="color:#b0b0b0;line-height:1.5">' + msg + '</p>' +
    '<a href="/panel.html" style="display:inline-block;margin-top:14px;background:#ff6a00;color:#000;padding:12px 24px;border-radius:99px;font-weight:700;text-decoration:none">Volver al panel</a></div></body></html>';
  return new Response(html, { status: ok ? 200 : 400, headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } });
}

// Paso 1: el panel pide el link para autorizar (solo con la clave del panel)
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
  const state = randomBytes(16).toString('hex');
  await redis([['SET', 'ml:state:' + state, '1', 'EX', 600]]);
  const url = 'https://auth.mercadolibre.com.ar/authorization?' + new URLSearchParams({
    response_type: 'code', client_id: CID, redirect_uri: REDIRECT, state
  }).toString();
  return json({ ok: true, url });
}

// Paso 2: Mercado Libre vuelve acá con un código; se cambia por el acceso y se guarda en la base
export async function GET(request) {
  try {
    if (!URL_ || !TOKEN || !CID || !CSECRET) return page('Falta configurar las variables en Vercel.', false);
    const u = new URL(request.url);
    const code = u.searchParams.get('code');
    const state = u.searchParams.get('state');
    if (!code || !state || !/^[a-f0-9]{32}$/.test(state)) return page('Faltan datos en el regreso desde Mercado Libre.', false);
    const chk = await redis([['GET', 'ml:state:' + state], ['DEL', 'ml:state:' + state]]);
    if (!chk[0].result) return page('El enlace venció o no es válido. Volvé al panel y probá de nuevo.', false);
    const tr = await fetch('https://api.mercadolibre.com/oauth/token', {
      method: 'POST',
      headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'authorization_code', client_id: CID, client_secret: CSECRET, code, redirect_uri: REDIRECT
      })
    });
    const t = await tr.json().catch(() => ({}));
    if (!tr.ok || !t.access_token) return page('Mercado Libre no aceptó la autorización (' + (t.error || t.message || tr.status) + ').', false);
    if (!t.refresh_token) return page('Mercado Libre no entregó permiso de renovación. En tu aplicación, tildá “Refresh Token” y el acceso sin conexión, y volvé a conectar.', false);
    await redis([['SET', 'ml:tokens', JSON.stringify({
      access_token: t.access_token,
      refresh_token: t.refresh_token,
      expires_at: Date.now() + (t.expires_in || 21600) * 1000,
      user_id: t.user_id
    })]]);
    return new Response(null, { status: 302, headers: { Location: '/panel.html?ml=ok', 'Cache-Control': 'no-store' } });
  } catch (e) {
    return page('Ocurrió un error inesperado. Probá de nuevo.', false);
  }
}
