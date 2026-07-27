// POST /api/save-content/{file}  → write one content JSON file to the DEV container.
const { requireEditor } = require('../_lib/auth');
const { isAllowedContentFile, writeJsonBlob } = require('../_lib/content');
const { getContentDevContainer } = require('../_lib/storage');

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;

  const file = (context.bindingData.file || '').toString();
  const name = file.endsWith('.json') ? file : file + '.json';
  if (!isAllowedContentFile(name)) {
    context.res = { status: 400, headers: json(), body: { error: 'Unknown content file: ' + name } };
    return;
  }

  let data = req.body;
  if (typeof data === 'string') {
    try { data = JSON.parse(data); }
    catch (_) { context.res = { status: 400, headers: json(), body: { error: 'Invalid JSON body' } }; return; }
  }
  if (!data || typeof data !== 'object') {
    context.res = { status: 400, headers: json(), body: { error: 'Body must be a JSON object' } };
    return;
  }

  try {
    await writeJsonBlob(getContentDevContainer(), name, data);
    context.res = { status: 200, headers: json(), body: { ok: true, file: name } };
  } catch (err) {
    context.log.error('[save-content] ' + err.message);
    context.res = { status: 500, headers: json(), body: { error: 'Save failed: ' + err.message } };
  }
};

function json() { return { 'Content-Type': 'application/json' }; }
