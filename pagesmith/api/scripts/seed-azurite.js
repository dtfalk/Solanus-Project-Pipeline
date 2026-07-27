// seed-azurite.js — seed the local Azurite blob emulator with the repo's
// existing content JSON + media, mirroring the cloud container layout.
// Purely local; touches nothing in Azure.

const fs = require('fs');
const path = require('path');
const { BlobServiceClient } = require('@azure/storage-blob');
const { generateResponsiveVariants } = require('../_lib/media');

const CONN = process.env.AZURE_STORAGE_CONNECTION_STRING ||
  'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;';

const APP = path.resolve(__dirname, '../../app');
const DATA_DIR = path.join(APP, 'data');
const MEDIA_DIR = path.join(APP, 'media-content');

const CONTENT_DEV = process.env.AZURE_CONTENT_DEV_CONTAINER || 'website-content-dev';
const CONTENT_PROD = process.env.AZURE_CONTENT_PROD_CONTAINER || 'website-content-prod';
const MEDIA = process.env.AZURE_MEDIA_CONTAINER || 'website-media-dev';
const QUESTIONNAIRE = process.env.AZURE_QUESTIONNAIRE_CONTAINER || 'questionnaire-responses';

const MIME = {
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
  '.mp4': 'video/mp4', '.webm': 'video/webm', '.mov': 'video/quicktime', '.m4v': 'video/x-m4v',
  '.json': 'application/json'
};

function walk(dir, base = dir) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...walk(full, base));
    else out.push({ full, rel: path.relative(base, full).split(path.sep).join('/') });
  }
  return out;
}

(async () => {
  const svc = BlobServiceClient.fromConnectionString(CONN);

  // CORS so the localhost:4280 SWA page can fetch blobs from localhost:10000.
  await svc.setProperties({
    cors: [{
      allowedOrigins: '*',
      allowedMethods: 'GET,HEAD,OPTIONS,PUT,POST',
      allowedHeaders: '*',
      exposedHeaders: '*',
      maxAgeInSeconds: 3600
    }]
  });
  console.log('CORS set (*)');

  for (const c of [CONTENT_DEV, CONTENT_PROD, MEDIA]) {
    await svc.getContainerClient(c).createIfNotExists({ access: 'blob' });
    console.log('container ready:', c);
  }
  // questionnaire responses (private — no public access)
  await svc.getContainerClient(QUESTIONNAIRE).createIfNotExists();
  console.log('container ready:', QUESTIONNAIRE);

  // content JSON → both dev and prod
  let nc = 0;
  for (const f of fs.readdirSync(DATA_DIR)) {
    if (!f.endsWith('.json')) continue;
    const buf = fs.readFileSync(path.join(DATA_DIR, f));
    for (const c of [CONTENT_DEV, CONTENT_PROD]) {
      await svc.getContainerClient(c).getBlockBlobClient(f).upload(buf, buf.length, {
        blobHTTPHeaders: { blobContentType: 'application/json; charset=utf-8', blobCacheControl: 'public, max-age=60' }
      });
    }
    nc++;
  }
  console.log('seeded content files:', nc);

  // media tree → media container (blob name == path relative to media-content/)
  const mediaCc = svc.getContainerClient(MEDIA);
  let nm = 0;
  for (const { full, rel } of walk(MEDIA_DIR)) {
    const ext = path.extname(rel).toLowerCase();
    const buf = fs.readFileSync(full);
    await mediaCc.getBlockBlobClient(rel).upload(buf, buf.length, {
      blobHTTPHeaders: { blobContentType: MIME[ext] || 'application/octet-stream', blobCacheControl: 'public, max-age=300' }
    });
    nm++;
    // Generate -mobile/-thumb webp variants for big rasters (so the mobile CSS
    // backgrounds — e.g. background-mobile.webp — actually exist).
    if (['.jpg', '.jpeg', '.png', '.webp'].includes(ext)) {
      try {
        for (const v of await generateResponsiveVariants(buf, rel)) {
          await mediaCc.getBlockBlobClient(v.name).upload(v.buffer, v.buffer.length, {
            blobHTTPHeaders: { blobContentType: v.contentType, blobCacheControl: 'public, max-age=300' }
          });
          nm++;
        }
      } catch (e) { /* variants are best-effort */ }
    }
  }
  console.log('seeded media files:', nm);
  console.log('SEED DONE');
})().catch((e) => { console.error('SEED FAILED:', e); process.exit(1); });
