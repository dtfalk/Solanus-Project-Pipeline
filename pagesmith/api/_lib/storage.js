// storage.js — container clients, credential-agnostic.
//
// Selection order (so the SAME code runs in every environment):
//   1) AZURE_STORAGE_CONNECTION_STRING  → local Azurite, or any conn string
//   2) per-container SAS env var         → free-tier cloud (server-side only)
//   3) DefaultAzureCredential            → managed identity (Standard plan)
//
// Nothing here is ever sent to the browser.

const { BlobServiceClient, ContainerClient } = require('@azure/storage-blob');

let DefaultAzureCredential = null;
try { ({ DefaultAzureCredential } = require('@azure/identity')); } catch (_) { /* optional until cloud */ }

function accountName() {
  return process.env.AZURE_STORAGE_ACCOUNT_NAME || 'devstoreaccount1';
}

function connString() {
  return (process.env.AZURE_STORAGE_CONNECTION_STRING || '').trim();
}

function serviceClient() {
  const conn = connString();
  if (conn) return BlobServiceClient.fromConnectionString(conn);
  if (!DefaultAzureCredential) {
    throw new Error('No AZURE_STORAGE_CONNECTION_STRING and @azure/identity unavailable');
  }
  return new BlobServiceClient(`https://${accountName()}.blob.core.windows.net`, new DefaultAzureCredential());
}

function getContainer(name, sasEnvVar) {
  // A per-container SAS only applies when there is no connection string.
  if (!connString() && sasEnvVar) {
    const rawSas = (process.env[sasEnvVar] || '').trim();
    if (rawSas) {
      const token = rawSas.startsWith('?') ? rawSas : '?' + rawSas;
      return new ContainerClient(`https://${accountName()}.blob.core.windows.net/${name}${token}`);
    }
  }
  return serviceClient().getContainerClient(name);
}

const contentDevName = () => process.env.AZURE_CONTENT_DEV_CONTAINER || 'website-content-dev';
const contentProdName = () => process.env.AZURE_CONTENT_PROD_CONTAINER || 'website-content-prod';
const mediaName = () => process.env.AZURE_MEDIA_CONTAINER || 'website-media-dev';
const mediaProdName = () => process.env.AZURE_MEDIA_PROD_CONTAINER || 'media-prod';

function mediaPublicBase() {
  const explicit = (process.env.MEDIA_PUBLIC_BASE || '').replace(/\/+$/, '');
  if (explicit) return explicit;
  const conn = connString();
  if (conn) {
    const m = conn.match(/BlobEndpoint=([^;]+)/i);
    const base = (m ? m[1] : `https://${accountName()}.blob.core.windows.net`).replace(/\/+$/, '');
    return `${base}/${mediaName()}`;
  }
  return `https://${accountName()}.blob.core.windows.net/${mediaName()}`;
}

module.exports = {
  getContentDevContainer: () => getContainer(contentDevName(), 'AZURE_CONTENT_DEV_SAS_TOKEN'),
  getContentProdContainer: () => getContainer(contentProdName(), 'AZURE_CONTENT_PROD_SAS_TOKEN'),
  getMediaContainer: () => getContainer(mediaName(), 'AZURE_MEDIA_SAS_TOKEN'),
  getMediaProdContainer: () => getContainer(mediaProdName(), 'AZURE_MEDIA_PROD_SAS_TOKEN'),
  getQuestionnaireContainer: () => getContainer(process.env.AZURE_QUESTIONNAIRE_CONTAINER || 'questionnaire-responses', 'QUESTIONNAIRE_SAS_TOKEN'),
  mediaContainerName: mediaName,
  mediaPublicBase
};
