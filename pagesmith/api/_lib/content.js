// content.js — the editable JSON content files + blob read/write/promote.

// Authoritative allow-list, by SHAPE not by name: a content file is a flat
// lowercase JSON filename — the built-in pages (index.json, global.json, …) and
// any user-created page-<slug>.json. Keeping this a generic pattern rather than a
// hardcoded per-site list is what lets the same engine host any site. The slug
// is restricted to lowercase letters/digits/hyphens (no slashes/dots), so there
// is no path traversal and nothing site-specific lives here.
const CONTENT_FILE_RE = /^[a-z0-9][a-z0-9-]{0,39}\.json$/;
function isAllowedContentFile(name) { return CONTENT_FILE_RE.test(name); }

// Every content file present in a container (so promote/revert carry them all).
async function listAllContentFiles(container) {
  const names = [];
  try {
    for await (const b of container.listBlobsFlat()) {
      if (CONTENT_FILE_RE.test(b.name) && names.indexOf(b.name) === -1) names.push(b.name);
    }
  } catch (_) { /* listing unavailable → empty (nothing to promote) */ }
  return names;
}

async function readJsonBlob(container, name) {
  try {
    const buf = await container.getBlobClient(name).downloadToBuffer();
    return JSON.parse(buf.toString('utf-8'));
  } catch (err) {
    if (err.statusCode === 404 || err.code === 'BlobNotFound') return null;
    throw err;
  }
}

async function writeJsonBlob(container, name, data) {
  const body = JSON.stringify(data, null, 2);
  await container.getBlockBlobClient(name).upload(body, Buffer.byteLength(body), {
    blobHTTPHeaders: {
      blobContentType: 'application/json; charset=utf-8',
      blobCacheControl: 'public, max-age=60'
    }
  });
}

// Download-then-upload copy dev → prod (re-applies cache headers; files are tiny).
async function copyAcross(srcContainer, dstContainer, name) {
  const srcBlob = srcContainer.getBlobClient(name);
  if (!(await srcBlob.exists())) return { name, ok: false, reason: 'missing in dev' };
  const buf = await srcBlob.downloadToBuffer();
  await dstContainer.getBlockBlobClient(name).upload(buf, buf.length, {
    blobHTTPHeaders: {
      blobContentType: 'application/json; charset=utf-8',
      blobCacheControl: 'public, max-age=60'
    }
  });
  return { name, ok: true };
}

module.exports = { CONTENT_FILE_RE, isAllowedContentFile, listAllContentFiles, readJsonBlob, writeJsonBlob, copyAcross };
