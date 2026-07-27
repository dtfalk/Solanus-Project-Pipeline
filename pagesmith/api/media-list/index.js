// GET /api/media?prefix=<folder>/  → list blobs for the media picker.
const { requireEditor } = require('../_lib/auth');
const { getMediaContainer, mediaPublicBase } = require('../_lib/storage');
const { isAllowedMediaPrefix } = require('../_lib/media');

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;

  const prefix = (req.query.prefix || '').toString();
  if (prefix && !isAllowedMediaPrefix(prefix)) {
    context.res = { status: 400, headers: json(), body: { error: 'Invalid prefix' } };
    return;
  }

  try {
    const container = getMediaContainer();
    const base = mediaPublicBase();
    const items = [];
    for await (const blob of container.listBlobsFlat({ prefix })) {
      // hide generated responsive variants from the picker grid
      if (/-(mobile|thumb)\.webp$/i.test(blob.name)) continue;
      items.push({
        name: blob.name,
        path: '/media-content/' + blob.name,
        url: base ? `${base}/${blob.name}` : null,
        size: blob.properties.contentLength,
        contentType: blob.properties.contentType,
        lastModified: blob.properties.lastModified
      });
    }
    context.res = { status: 200, headers: json(), body: { items, publicBase: base } };
  } catch (err) {
    context.log.error('[media-list] ' + err.message);
    context.res = { status: 500, headers: json(), body: { error: 'List failed: ' + err.message } };
  }
};

function json() { return { 'Content-Type': 'application/json' }; }
