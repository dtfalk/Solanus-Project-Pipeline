// GET /api/questionnaire-export?format=csv|json  → download all questionnaire
// responses. Editor-only (it's PII). Lists the questionnaire-responses container,
// reads each response blob, and returns a downloadable CSV (default) or JSON.
const { requireEditor } = require('../_lib/auth');
const { getQuestionnaireContainer } = require('../_lib/storage');

module.exports = async function (context, req) {
  if (!requireEditor(context, req)) return;
  try {
    const container = getQuestionnaireContainer();
    const rows = [];
    for await (const b of container.listBlobsFlat()) {
      if (!/\.json$/i.test(b.name)) continue;
      try {
        const buf = await container.getBlobClient(b.name).downloadToBuffer();
        rows.push(JSON.parse(buf.toString('utf-8')));
      } catch (_) { /* skip an unreadable blob rather than fail the whole export */ }
    }
    rows.sort((a, b) => String((a && a.timestamp) || '').localeCompare(String((b && b.timestamp) || '')));

    const format = ((req.query && req.query.format) || 'csv').toLowerCase();
    if (format === 'json') {
      context.res = {
        status: 200,
        headers: { 'Content-Type': 'application/json; charset=utf-8', 'Content-Disposition': 'attachment; filename="questionnaire-responses.json"' },
        body: JSON.stringify(rows, null, 2)
      };
      return;
    }

    // CSV with dynamic columns for whatever questionnaire fields exist.
    const qKeys = [];
    rows.forEach(r => Object.keys((r && r.questionnaire) || {}).forEach(k => { if (qKeys.indexOf(k) === -1) qKeys.push(k); }));
    const cols = ['timestamp', 'email', 'clientIp'].concat(qKeys);
    const esc = v => {
      v = (v == null ? '' : (typeof v === 'object' ? JSON.stringify(v) : String(v)));
      // CSV formula-injection guard: a cell starting with = + - @ (or tab/CR) is
      // executed as a formula by Excel/Sheets. Since responses come from an
      // anonymous public form, prefix a single quote to neutralise it before
      // the operator ever opens the file.
      if (/^[=+\-@\t\r]/.test(v)) v = "'" + v;
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
    };
    const lines = [cols.join(',')];
    rows.forEach(r => {
      const q = (r && r.questionnaire) || {};
      lines.push(cols.map(c => esc((r && c in r) ? r[c] : q[c])).join(','));
    });
    context.res = {
      status: 200,
      headers: { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': 'attachment; filename="questionnaire-responses.csv"' },
      body: lines.join('\n')
    };
  } catch (err) {
    context.log.error('[questionnaire-export] ' + err.message);
    context.res = { status: 500, headers: { 'Content-Type': 'application/json' }, body: { error: 'Export failed: ' + err.message } };
  }
};
