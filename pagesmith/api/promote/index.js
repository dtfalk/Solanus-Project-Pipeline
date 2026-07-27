// POST /api/promote  → publish: copy content JSON dev→prod, then sync media dev→prod.
const { requireEditor } = require('../_lib/auth');
const { listAllContentFiles, copyAcross, readJsonBlob } = require('../_lib/content');
const {
  getContentDevContainer, getContentProdContainer,
  getMediaContainer, getMediaProdContainer
} = require('../_lib/storage');

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;

  try {
    const dev = getContentDevContainer();
    const prod = getContentProdContainer();
    const results = [];
    const names = await listAllContentFiles(dev);
    // Pages marked 'dev' in global.json stay in draft — never copied to the live site.
    const devOnly = {};
    try {
      const gj = await readJsonBlob(dev, 'global.json');
      if (gj && Array.isArray(gj.nav)) gj.nav.forEach(function (it) {
        const m = it && it.visibility === 'dev' && /^\/p\/([a-z0-9-]+)$/.exec(it.url || '');
        if (m) devOnly['page-' + m[1] + '.json'] = true;
      });
    } catch (_) { /* if global is unreadable, publish everything */ }
    for (const name of names) {
      if (devOnly[name]) { results.push({ name, ok: true, skipped: 'dev-only' }); continue; }
      results.push(await copyAcross(dev, prod, name));
    }
    const failed = results.filter((r) => !r.ok);

    // Media is split draft→live; sync only when a prod media container is
    // configured (cloud). Skipped locally where there is no media-prod.
    let media = null;
    if (process.env.AZURE_MEDIA_PROD_CONTAINER) {
      try { media = await syncMedia(getMediaContainer(), getMediaProdContainer()); }
      catch (e) { context.log.warn('[promote] media sync failed: ' + e.message); media = { error: e.message }; }
    }

    context.res = {
      status: failed.length ? 207 : 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        ok: failed.length === 0,
        message: `Promoted ${results.length - failed.length}/${results.length} files`,
        results,
        media
      }
    };
  } catch (err) {
    context.log.error('[promote] ' + err.message);
    context.res = { status: 500, headers: { 'Content-Type': 'application/json' }, body: { error: 'Promote failed: ' + err.message } };
  }
};

// Copy blobs from draft → live that are missing or size-changed in live.
// Server-side copy from the (public-read) draft URL — no data flows through the
// Function. Bounded by skipping blobs already present at the same size.
async function syncMedia(draft, live) {
  const out = { copied: 0, skipped: 0, failed: 0 };
  const liveSizes = {};
  for await (const b of live.listBlobsFlat()) liveSizes[b.name] = b.properties.contentLength;
  for await (const b of draft.listBlobsFlat()) {
    if (liveSizes[b.name] === b.properties.contentLength) { out.skipped++; continue; }
    try {
      const src = draft.getBlobClient(b.name).url;
      const poller = await live.getBlobClient(b.name).beginCopyFromURL(src);
      await poller.pollUntilDone();
      out.copied++;
    } catch (_) { out.failed++; }
  }
  return out;
}
