// auth.js — enforce the SWA "editor" role server-side.
//
// Azure Static Web Apps injects an `x-ms-client-principal` header (base64 JSON)
// on every request that reaches the API, describing the logged-in user and
// their roles. We require the `editor` role for any mutating endpoint. This is
// defense-in-depth on top of the route-level allowedRoles in staticwebapp.config.json.
//
// EDITOR_AUTH_DISABLED=true bypasses the check (local convenience only — never set in cloud).

const EDITOR_ROLE = 'editor';

function getPrincipal(req) {
  const h = req && req.headers &&
    (req.headers['x-ms-client-principal'] || req.headers['X-MS-CLIENT-PRINCIPAL']);
  if (!h) return null;
  try {
    return JSON.parse(Buffer.from(h, 'base64').toString('utf-8'));
  } catch (_) {
    return null;
  }
}

function isEditor(req) {
  if ((process.env.EDITOR_AUTH_DISABLED || '').toLowerCase() === 'true') return true;
  const p = getPrincipal(req);
  const roles = (p && Array.isArray(p.userRoles)) ? p.userRoles : [];
  return roles.includes(EDITOR_ROLE);
}

// Returns true if authorized; otherwise sets a 401 on context.res and returns false.
function requireEditor(context, req) {
  if (isEditor(req)) return true;
  context.res = {
    status: 401,
    headers: { 'Content-Type': 'application/json' },
    body: { error: 'Not authorized — editor role required' }
  };
  return false;
}

module.exports = { EDITOR_ROLE, getPrincipal, isEditor, requireEditor };
