// media.js — media path allow-listing + responsive variant generation.
// Ported verbatim in spirit from dev-site/lib/media-routes.js.

const path = require('path');

let sharp = null;
try { sharp = require('sharp'); } catch (_) { sharp = null; }

const MEDIA_ALLOWED_EXT = new Set([
  '.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg',
  '.mp4', '.webm', '.mov', '.m4v', '.ico'
]);

// Media paths are allow-listed by SHAPE, not by a per-site folder list: a
// root-level file ("logo.png") or one folder deep ("<folder>/<file>"), safe
// charsets, no traversal. Keeping this a generic pattern (like the content
// allowlist in content.js) is what lets the same engine host any site.
const MEDIA_FOLDER_RE = /^[a-z0-9][a-z0-9-]{0,39}$/;
const MEDIA_FILE_RE = /^[a-z0-9][a-z0-9._-]{0,127}\.[a-z0-9]+$/i;

function isAllowedMediaPath(p) {
  if (typeof p !== 'string') return false;
  if (!p || p.length > 512) return false;
  if (p.includes('..') || p.startsWith('/') || p.startsWith('\\')) return false;
  const parts = p.split('/');
  if (parts.length === 1) return MEDIA_FILE_RE.test(parts[0]);
  if (parts.length === 2) return MEDIA_FOLDER_RE.test(parts[0]) && MEDIA_FILE_RE.test(parts[1]);
  return false;
}

// A listing prefix: '' (the whole container / root files) or '<folder>/'.
function isAllowedMediaPrefix(p) {
  if (typeof p !== 'string') return false;
  if (p === '') return true;
  return MEDIA_FOLDER_RE.test(p.endsWith('/') ? p.slice(0, -1) : p);
}

const MIME_BY_EXT = {
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
  '.webp': 'image/webp', '.gif': 'image/gif', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.mp4': 'video/mp4', '.webm': 'video/webm',
  '.mov': 'video/quicktime', '.m4v': 'video/x-m4v'
};

function mimeFromExt(ext) {
  return MIME_BY_EXT[(ext || '').toLowerCase()] || 'application/octet-stream';
}

// Convert a raster image buffer to WebP (auto-orients via EXIF). Returns null if
// sharp isn't available so the caller can fall back to the original.
async function toWebp(buffer, quality) {
  if (!sharp) return null;
  return sharp(buffer).rotate().webp({ quality: quality || 82 }).toBuffer();
}

// Build -mobile (max 800w) and -thumb (max 400w) WebP variants for big rasters.
async function generateResponsiveVariants(buffer, originalBlobName) {
  if (!sharp) return [];
  const out = [];
  const meta = await sharp(buffer).metadata();
  if (!meta.width || meta.width < 1200) return out;

  const dir = path.posix.dirname(originalBlobName);
  const ext = path.posix.extname(originalBlobName);
  const stem = path.posix.basename(originalBlobName, ext);

  for (const t of [{ suffix: '-mobile', width: 800 }, { suffix: '-thumb', width: 400 }]) {
    if (meta.width <= t.width) continue;
    const variantBuf = await sharp(buffer)
      .resize({ width: t.width, withoutEnlargement: true })
      .webp({ quality: 82 })
      .toBuffer();
    const variantName = `${dir === '.' ? '' : dir + '/'}${stem}${t.suffix}.webp`;
    out.push({ name: variantName, buffer: variantBuf, width: t.width, contentType: 'image/webp' });
  }
  return out;
}

module.exports = {
  MEDIA_ALLOWED_EXT,
  isAllowedMediaPath, isAllowedMediaPrefix, mimeFromExt, generateResponsiveVariants, toWebp,
  sharpAvailable: () => !!sharp
};
