// POST /api/revert  → discard all UNPUBLISHED edits by copying every content
// file PROD → DEV (the inverse of promote). This restores the dev/editing site
// to exactly the last published version.
const { requireEditor } = require('../_lib/auth');
const { listAllContentFiles, copyAcross } = require('../_lib/content');
const { getContentDevContainer, getContentProdContainer } = require('../_lib/storage');

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;

  try {
    const dev = getContentDevContainer();
    const prod = getContentProdContainer();
    const results = [];
    const names = await listAllContentFiles(prod);
    for (const name of names) {
      results.push(await copyAcross(prod, dev, name));
    }
    const failed = results.filter((r) => !r.ok);
    context.res = {
      status: failed.length ? 207 : 200,
      headers: { 'Content-Type': 'application/json' },
      body: {
        ok: failed.length === 0,
        message: `Reverted ${results.length - failed.length}/${results.length} files to the published version`,
        results
      }
    };
  } catch (err) {
    context.log.error('[revert] ' + err.message);
    context.res = { status: 500, headers: { 'Content-Type': 'application/json' }, body: { error: 'Revert failed: ' + err.message } };
  }
};
