const { getQuestionnaireContainer } = require("../_lib/storage");
const { v4: uuidv4 } = require("uuid");
const appInsights = require("applicationinsights");

// -------- Application Insights init (cold start only) --------
if (!appInsights.defaultClient) {
  const conn =
    process.env.APPLICATIONINSIGHTS_CONNECTION_STRING ||
    process.env.APPINSIGHTS_INSTRUMENTATIONKEY;
  if (conn) {
    appInsights
      .setup(conn)
      .setAutoCollectRequests(false) // Functions host already tracks requests
      .setAutoCollectDependencies(true)
      .setAutoCollectExceptions(true)
      .setAutoCollectConsole(true, true)
      .setUseDiskRetryCaching(true)
      .setSendLiveMetrics(false)
      .start();

    const client = appInsights.defaultClient;
    const keys = client.context.keys;
    client.context.tags[keys.cloudRole] =
      process.env.APPLICATIONINSIGHTS_ROLE_NAME || "swa-api";
  }
}

const aiClient = appInsights.defaultClient;

// safe wrappers
function trackEvent(name, properties = {}) {
  try { aiClient && aiClient.trackEvent({ name, properties }); } catch {}
}
function trackException(err, properties = {}) {
  try { aiClient && aiClient.trackException({ exception: err, properties }); } catch {}
}
function trackDependency({ target, name, data, durationMs, resultCode, success, type = "Azure Blob" }) {
  try {
    aiClient && aiClient.trackDependency({
      dependencyTypeName: type,
      target,
      name,
      data,
      duration: durationMs,
      resultCode: String(resultCode),
      success
    });
  } catch {}
}

// ----------------------------- Handler -----------------------------
async function saveQuestionnaire(context, req) {
  const requestId = uuidv4();
  context.log.info("Function invoked", {
    customDimensions: {
      requestId,
      method: req.method,
      contentLength: req.headers["content-length"] || "unknown",
    }
  });

  if (req.method !== "POST") {
    context.log.warn("Rejected non-POST request", { customDimensions: { requestId } });
    trackEvent("submit-questionnaire-bad-method", { requestId, method: req.method || "unknown" });
    context.res = { status: 405, body: "Method Not Allowed" };
    return;
  }

  const body = req.body || {};
  const bodySize = Buffer.byteLength(JSON.stringify(body), "utf8");
  if (bodySize > 2000) {
    context.log.warn("Payload too large", { customDimensions: { requestId, bodySize } });
    trackEvent("submit-questionnaire-too-large", { requestId, bodySize });
    context.res = { status: 413, body: "Payload too large. Limit is 2000 characters." };
    return;
  }

  const { rawEmail, ...questionnaire } = body;
  const trimmedEmail = (rawEmail || "").trim();
  const [localPart, domainPart] = trimmedEmail.split("@");
  const email = domainPart ? `${localPart}@${domainPart.toLowerCase()}` : trimmedEmail;

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!email || !emailRegex.test(email)) {
    context.log.warn("Invalid email", { customDimensions: { requestId, email } });
    trackEvent("submit-questionnaire-invalid-email", { requestId, email });
    context.res = { status: 400, body: "Invalid email address provided." };
    return;
  }

  const clientIp =
    req.headers["x-forwarded-for"] ||
    req.headers["x-client-ip"] ||
    req.headers["x-real-ip"] ||
    "unknown";

  const depTarget = (process.env.AZURE_STORAGE_ACCOUNT_NAME || 'storage');
  const blobName = `${uuidv4()}.json`;

  context.log.info("Preparing blob upload", {
    customDimensions: { requestId, email, clientIp, blobName }
  });

  try {
    const containerClient = getQuestionnaireContainer();
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    const content = JSON.stringify({
      timestamp: new Date().toISOString(),
      version: "v0",
      email,
      clientIp,
      questionnaire
    }, null, 2);

    const maxAttempts = 5;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const depStart = Date.now();
      let depCode = 0;

      try {
        const res = await blockBlobClient.upload(content, Buffer.byteLength(content));
        depCode = res?._response?.status || 201;

        context.log.info("Blob upload succeeded", {
          customDimensions: { requestId, attempt, blobName }
        });
        trackEvent("submit-questionnaire-success", { requestId, blobName, attempt });
        trackDependency({
          target: depTarget,
          name: "BlockBlobClient.upload",
          data: blobName,
          durationMs: Date.now() - depStart,
          resultCode: depCode,
          success: true
        });

        context.res = {
          status: 200,
          body: "Survey response saved successfully.",
        };
        return;
      } catch (uploadError) {
        depCode = uploadError.statusCode || uploadError.status || "ERR";
        context.log.warn("Blob upload attempt failed", {
          customDimensions: {
            requestId,
            attempt,
            error: uploadError.message,
            blobName
          }
        });
        trackException(uploadError, { requestId, attempt, blobName });
        trackEvent("submit-questionnaire-failure", {
          requestId, attempt, blobName, reason: uploadError.message || "unknown"
        });
        trackDependency({
          target: depTarget,
          name: "BlockBlobClient.upload",
          data: blobName,
          durationMs: Date.now() - depStart,
          resultCode: depCode,
          success: false
        });

        if (attempt === maxAttempts) {
          context.res = {
            status: 500,
            body: "Failed to save survey response after multiple attempts.",
          };
          return;
        }

        await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 100));
      }
    }
  } catch (err) {
    context.log.error("Fatal error during blob upload", {
      customDimensions: {
        requestId,
        error: err.message,
        blobName,
      }
    });
    trackException(err, { requestId, blobName });

    context.res = {
      status: 500,
      body: `Upload failed: ${err.message || "Unknown error"}`,
    };
  }
}

module.exports = async function (context, req) {
  return await saveQuestionnaire(context, req);
};
