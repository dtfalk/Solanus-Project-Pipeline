// POST /api/stats-refresh → pull REAL live numbers into page-stats.json (the DRAFT copy).
//
// Editor-gated (SWA `editor` role) OR an `x-stats-refresh-secret` header matching
// STATS_REFRESH_SECRET (for a scheduled refresh, which can't be an editor).
//
// It walks page-stats.json for blocks/items carrying a `source` and replaces the value with
// the real figure from that provider. ONLY sources with a public API + configured keys update
// (YouTube subscribers, Instagram followers). Everything else — the Spotify metrics, the
// charts, the rankings — has NO public API, so those values are left EXACTLY as they are
// (edited by hand in the editor, or wired later via artist OAuth). Nothing is ever invented:
// no keys → nothing changes. Statistics are real or they're untouched.
//
// Writes to the dev/draft container only; the editor then Publishes to push it live.

const { requireEditor } = require('../_lib/auth');
const { readJsonBlob, writeJsonBlob } = require('../_lib/content');
const { getContentDevContainer } = require('../_lib/storage');

const PAGE = 'page-stats.json';

// 12400 → "12.4k", 480000 → "480k", 1.2e6 → "1.2M".
function fmt(n) {
  n = Number(n);
  if (!isFinite(n)) return null;
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e8 ? 0 : 1).replace(/\.0$/, '') + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e5 ? 0 : 1).replace(/\.0$/, '') + 'k';
  return String(Math.round(n));
}

// Each resolver returns a Number from a REAL source, or null to skip (leave the value as-is).
async function youtubeSubs() {
  const key = process.env.YOUTUBE_API_KEY, ch = process.env.YOUTUBE_CHANNEL_ID;
  if (!key || !ch) return null;
  const r = await fetch('https://www.googleapis.com/youtube/v3/channels?part=statistics&id=' +
    encodeURIComponent(ch) + '&key=' + encodeURIComponent(key));
  if (!r.ok) throw new Error('YouTube ' + r.status);
  const j = await r.json();
  const s = j && j.items && j.items[0] && j.items[0].statistics;
  return s && s.subscriberCount != null ? Number(s.subscriberCount) : null;
}
async function instagramFollowers() {
  const tok = process.env.IG_ACCESS_TOKEN, id = process.env.IG_ACCOUNT_ID;
  if (!tok || !id) return null;
  const r = await fetch('https://graph.facebook.com/v19.0/' + encodeURIComponent(id) +
    '?fields=followers_count&access_token=' + encodeURIComponent(tok));
  if (!r.ok) throw new Error('Instagram ' + r.status);
  const j = await r.json();
  return j && j.followers_count != null ? Number(j.followers_count) : null;
}

const RESOLVERS = {
  'youtube.subscribers': youtubeSubs,
  'instagram.followers': instagramFollowers
  // spotify.* / streams.byPlatform / *.topTracks → no public API; manual / OAuth-later.
};

module.exports = async function (context, req) {
  const secret = (process.env.STATS_REFRESH_SECRET || '').trim();
  const hdr = req && req.headers && (req.headers['x-stats-refresh-secret'] || req.headers['X-Stats-Refresh-Secret']);
  const viaSecret = !!secret && hdr === secret;
  if (!viaSecret && !requireEditor(context, req)) return;

  try {
    const dev = getContentDevContainer();
    const data = await readJsonBlob(dev, PAGE);
    if (!data || !Array.isArray(data.sections)) {
      context.res = { status: 404, headers: { 'Content-Type': 'application/json' }, body: { error: 'page-stats.json not found' } };
      return;
    }

    const cache = {};
    async function resolve(src) {
      if (!src || !(src in RESOLVERS)) return undefined;       // no public-API resolver → skip
      if (!(src in cache)) {
        try { cache[src] = await RESOLVERS[src](); }
        catch (e) { cache[src] = { __err: e.message }; }
      }
      return cache[src];
    }

    const updated = [], skipped = [], errors = [];
    async function applyTo(holder) {
      if (!holder || !holder.source) return;
      const v = await resolve(holder.source);
      if (v === undefined) { skipped.push(holder.source); return; }              // no public API → untouched
      if (v && v.__err) { errors.push(holder.source + ': ' + v.__err); return; }
      if (v == null) { skipped.push(holder.source + ' (key not set)'); return; } // key missing → untouched
      const f = fmt(v);
      if (f != null) { holder.value = f; updated.push(holder.source + ' → ' + f); }
    }

    for (const sec of data.sections) {
      await applyTo(sec);                                       // spotlight / chart / ranking level
      if (Array.isArray(sec.items)) for (const it of sec.items) await applyTo(it);  // stat-card items
    }

    if (updated.length) await writeJsonBlob(dev, PAGE, data);

    context.res = {
      status: errors.length ? 207 : 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        ok: errors.length === 0,
        updated, skipped, errors,
        message: updated.length
          ? ('Refreshed ' + updated.length + ' live stat(s) — Publish to push live.')
          : 'Nothing to refresh: no provider API keys set, so there are no real numbers to pull. (Spotify metrics have no public API — edit those by hand.)'
      }
    };
  } catch (err) {
    context.log.error('[stats-refresh] ' + err.message);
    context.res = { status: 500, headers: { 'Content-Type': 'application/json' }, body: { error: 'Stats refresh failed: ' + err.message } };
  }
};
