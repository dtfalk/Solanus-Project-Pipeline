// POST /api/media-upload  { prefix, filename, contentBase64 }  → upload one blob (+ variants).
// Base64-JSON body (not multipart) keeps the Function simple and dependency-free.
const path = require('path');
const { requireEditor } = require('../_lib/auth');
const { getMediaContainer, mediaPublicBase } = require('../_lib/storage');
const { MEDIA_ALLOWED_EXT, isAllowedMediaPath, mimeFromExt, generateResponsiveVariants, toWebp } = require('../_lib/media');
const { transcodeToWebMp4 } = require('../_lib/video');

const WEBP_FROM = ['.jpg', '.jpeg', '.png'];           // rasters we re-encode to WebP
const VIDEO_EXT = ['.mp4', '.webm', '.mov', '.m4v'];
// Catch "huge phone video" uploads and shrink them.
const VIDEO_MIN = parseInt(process.env.MEDIA_VIDEO_TRANSCODE_MIN_BYTES || String(5 * 1024 * 1024), 10);
// Media prefixes whose uploads keep FULL QUALITY (no WebP/transcode, no size
// cap) — comma-separated app setting, e.g. "press-kit/" for a folder whose
// recipients need originals. Empty (the default) = optimize everything. This
// is site/deploy config, mirroring fullQualityMedia in the site's config.
const FULL_QUALITY_PREFIXES = (process.env.MEDIA_FULL_QUALITY_PREFIXES || '')
  .split(',').map((s) => s.trim()).filter(Boolean);
const isFullQuality = (name) => FULL_QUALITY_PREFIXES.some((p) => name.startsWith(p));

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;

  const body = req.body || {};
  const prefix = (body.prefix || '').toString().replace(/\/+$/, '');
  const filename = path.basename((body.filename || '').toString());
  const b64 = (body.contentBase64 || '').toString();

  if (!filename || !b64) {
    context.res = { status: 400, headers: json(), body: { error: 'filename and contentBase64 required' } };
    return;
  }
  const ext = path.extname(filename).toLowerCase();
  if (!MEDIA_ALLOWED_EXT.has(ext)) {
    context.res = { status: 400, headers: json(), body: { error: `Extension ${ext} not allowed` } };
    return;
  }
  const blobName = prefix ? `${prefix}/${filename}` : filename;
  if (!isAllowedMediaPath(blobName)) {
    context.res = { status: 400, headers: json(), body: { error: 'Invalid target path' } };
    return;
  }

  const comma = b64.indexOf(',');
  const raw = b64.startsWith('data:') && comma !== -1 ? b64.slice(comma + 1) : b64;
  let buffer;
  try { buffer = Buffer.from(raw, 'base64'); }
  catch (_) { context.res = { status: 400, headers: json(), body: { error: 'Invalid base64' } }; return; }
  if (!buffer.length) {
    context.res = { status: 400, headers: json(), body: { error: 'Empty file' } };
    return;
  }

  // Full-quality prefixes keep originals; everything else gets web-optimized.
  const isFull = isFullQuality(blobName);

  // Hard size cap for optimized media (defense in depth; the editor enforces it too).
  if (!isFull) {
    const capBytes = VIDEO_EXT.includes(ext)
      ? parseInt(process.env.MEDIA_MAX_VIDEO_BYTES || String(50 * 1024 * 1024), 10)
      : parseInt(process.env.MEDIA_MAX_IMAGE_BYTES || String(40 * 1024 * 1024), 10);
    if (buffer.length > capBytes) {
      context.res = { status: 413, headers: json(), body: { error: `File is ${Math.round(buffer.length / 1048576)} MB — over the ${Math.round(capBytes / 1048576)} MB limit. For big videos, use a YouTube link.` } };
      return;
    }
  }
  let outBuffer = buffer;
  let outName = blobName;
  let outExt = ext;
  let optimized = null; // 'webp' | 'video' for the response

  if (!isFull && WEBP_FROM.includes(ext)) {
    try {
      const webp = await toWebp(buffer);
      if (webp && webp.length) { outBuffer = webp; outName = blobName.replace(/\.[^.]+$/, '.webp'); outExt = '.webp'; optimized = 'webp'; }
    } catch (e) { context.log.warn('[media-upload] webp convert failed (storing original): ' + e.message); }
  } else if (!isFull && VIDEO_EXT.includes(ext) && buffer.length >= VIDEO_MIN) {
    try {
      const mp4 = await transcodeToWebMp4(buffer, { maxWidth: 1920, crf: 28 });
      if (mp4 && mp4.length) { outBuffer = mp4; outName = blobName.replace(/\.[^.]+$/, '.mp4'); outExt = '.mp4'; optimized = 'video'; }
    } catch (e) { context.log.warn('[media-upload] video transcode failed (storing original): ' + e.message); }
  }

  try {
    const container = getMediaContainer();
    await container.getBlockBlobClient(outName).uploadData(outBuffer, {
      blobHTTPHeaders: { blobContentType: mimeFromExt(outExt), blobCacheControl: 'public, max-age=300' }
    });

    const responsive = [];
    if (['.jpg', '.jpeg', '.png', '.webp'].includes(outExt)) {
      try {
        for (const v of await generateResponsiveVariants(outBuffer, outName)) {
          await container.getBlockBlobClient(v.name).uploadData(v.buffer, {
            blobHTTPHeaders: { blobContentType: v.contentType, blobCacheControl: 'public, max-age=300' }
          });
          responsive.push({ name: v.name, width: v.width });
        }
      } catch (e) {
        context.log.warn('[media-upload] variants failed: ' + e.message);
      }
    }

    const base = mediaPublicBase();
    context.res = {
      status: 200,
      headers: json(),
      body: {
        ok: true,
        name: outName,
        path: '/media-content/' + outName,
        url: base ? `${base}/${outName}` : null,
        optimized,
        originalBytes: buffer.length,
        storedBytes: outBuffer.length,
        responsive
      }
    };
  } catch (err) {
    context.log.error('[media-upload] ' + err.message);
    context.res = { status: 500, headers: json(), body: { error: 'Upload failed: ' + err.message } };
  }
};

function json() { return { 'Content-Type': 'application/json' }; }
