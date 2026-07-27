#!/usr/bin/env bash
# Provision the Pagesmith stack on the $0 path: SWA **Free** + built-in managed functions +
# scoped per-container SAS (no managed identity, no Function App, no App Insights).
#
# Creates a DEDICATED, isolated resource group with distinct names so the live stack
# (example.com, in a different RG) is NEVER touched. Tear down = delete this one RG.
#
# REVIEW the variables, then run. Requires: az login as Owner/Contributor on the sub.
# Not run automatically — it's a checklist you execute.
#
# vs the paid path (provision.sh): no Function App / identity / RBAC / App Insights /
# backend link. The API ships as the SWA's built-in managed functions (deploy.yml,
# api_location: "api"); storage is reached via the per-container SAS tokens set below,
# which storage.js already supports ("free-tier cloud, server-side only").
set -euo pipefail

# ------------------------------- edit these -------------------------------
SUB="f6ae9b66-a85b-4f77-80ea-ad8bbf6c6e7d"   # Azure subscription 1
RG="pagesmith-free-rg"
LOC="eastus2"
ACCT="pagesmithfree$RANDOM"        # storage account — GLOBALLY UNIQUE, 3-24 lowercase alnum
SWA="pagesmith-free-site"          # static web app name (Free)
SAS_EXPIRY="$(date -u -d '+365 days' '+%Y-%m-%dT%H:%MZ' 2>/dev/null || date -u -v+365d '+%Y-%m-%dT%H:%MZ')"
# --------------------------------------------------------------------------

az account set --subscription "$SUB"
az group create -n "$RG" -l "$LOC" -o none
echo "✓ resource group $RG"

# ---- Storage: one account, per-environment containers ----
az storage account create -n "$ACCT" -g "$RG" -l "$LOC" \
  --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access true -o none
az storage account blob-service-properties update --account-name "$ACCT" \
  --enable-versioning true --enable-delete-retention true --delete-retention-days 30 \
  --enable-container-delete-retention true --container-delete-retention-days 30 -o none

KEY=$(az storage account keys list -n "$ACCT" -g "$RG" --query "[0].value" -o tsv)
for c in content-dev content-prod media-dev media-prod; do
  az storage container create -n "$c" --account-name "$ACCT" --account-key "$KEY" --public-access blob -o none
done
az storage container create -n questionnaire --account-name "$ACCT" --account-key "$KEY" --public-access off -o none
echo "✓ storage $ACCT + containers"

# ---- Blob CORS: the browser fetches content/media JSON cross-origin (SWA host → blob host),
# which the data plane blocks until a CORS rule exists. The blobs are already public-read, so
# '*' for GET/HEAD only widens nothing — it just lets page JS read the response. Without this,
# every page renders empty (curl 200, but the browser preflight 403s). ----
az storage cors clear --services b --account-name "$ACCT" --account-key "$KEY" -o none 2>/dev/null || true
az storage cors add --services b --methods GET HEAD OPTIONS --origins '*' \
  --allowed-headers '*' --exposed-headers '*' --max-age 3600 \
  --account-name "$ACCT" --account-key "$KEY" -o none
echo "✓ blob CORS (GET/HEAD/OPTIONS) — takes a minute or two to propagate"

# ---- Scoped per-container SAS (least-privilege, server-side; rotate before expiry) ----
gensas () { az storage container generate-sas -n "$1" --account-name "$ACCT" --account-key "$KEY" \
  --permissions "$2" --expiry "$SAS_EXPIRY" -o tsv; }
SAS_CONTENT_DEV=$(gensas content-dev  racwl)   # read+write+list (editor draft)
SAS_CONTENT_PROD=$(gensas content-prod racwl)  # write on publish
SAS_MEDIA_DEV=$(gensas media-dev  racwl)
SAS_MEDIA_PROD=$(gensas media-prod racwl)
SAS_QUESTIONNAIRE=$(gensas questionnaire racwl)  # submit writes; export reads
STATS_SECRET=$(openssl rand -hex 24 2>/dev/null || head -c 24 /dev/urandom | base64 | tr -dc a-zA-Z0-9)
echo "✓ scoped SAS tokens (expire $SAS_EXPIRY)"

# ---- Static Web App (Free) ----
az staticwebapp create -n "$SWA" -g "$RG" -l "$LOC" --sku Free -o none
echo "✓ static web app $SWA (Free)"

# ---- App settings for the managed functions (storage via SAS; stats keys empty for now) ----
az staticwebapp appsettings set -n "$SWA" -g "$RG" --setting-names \
  AZURE_STORAGE_ACCOUNT_NAME="$ACCT" \
  AZURE_CONTENT_DEV_CONTAINER=content-dev \
  AZURE_CONTENT_PROD_CONTAINER=content-prod \
  AZURE_MEDIA_CONTAINER=media-dev \
  AZURE_MEDIA_PROD_CONTAINER=media-prod \
  AZURE_QUESTIONNAIRE_CONTAINER=questionnaire \
  AZURE_CONTENT_DEV_SAS_TOKEN="$SAS_CONTENT_DEV" \
  AZURE_CONTENT_PROD_SAS_TOKEN="$SAS_CONTENT_PROD" \
  AZURE_MEDIA_SAS_TOKEN="$SAS_MEDIA_DEV" \
  AZURE_MEDIA_PROD_SAS_TOKEN="$SAS_MEDIA_PROD" \
  QUESTIONNAIRE_SAS_TOKEN="$SAS_QUESTIONNAIRE" \
  MEDIA_PUBLIC_BASE="https://$ACCT.blob.core.windows.net/media-dev" \
  MEDIA_FULL_QUALITY_PREFIXES="epk-media/" \
  STATS_REFRESH_SECRET="$STATS_SECRET" \
  YOUTUBE_API_KEY="" YOUTUBE_CHANNEL_ID="" \
  IG_ACCESS_TOKEN="" IG_ACCOUNT_ID="" -o none
echo "✓ app settings (storage via SAS; add YouTube/IG/Spotify keys later)"

HOST=$(az staticwebapp show -n "$SWA" -g "$RG" --query defaultHostname -o tsv)
TOKEN=$(az staticwebapp secrets list -n "$SWA" -g "$RG" --query "properties.apiKey" -o tsv)
cat <<EOF

Done. Free stack up at: https://$HOST   (this is the dev/editable host — reads *-dev blobs)

Next:
  1. Seed blobs:   bash scripts/seed-azure.sh $ACCT     (uploads app/data + media to *-dev)
  2. GitHub repo Variable:  STORAGE_ACCOUNT=$ACCT
  3. GitHub repo Secret:    AZURE_STATIC_WEB_APPS_API_TOKEN  (value below)
       $TOKEN
  4. Deploy: push to main (deploy.yml, api_location:"api"), OR directly:
       swa deploy ./app --api-location ./api --deployment-token "$TOKEN" --env production
  5. Invite yourself as editor:
       az staticwebapp users invite -n $SWA -g $RG --authentication-provider github \\
         --user-details <your-github-username> --roles editor --domain $HOST \\
         --invitation-expiration-in-hours 168
  6. Stats keys (optional, later): az staticwebapp appsettings set -n $SWA -g $RG \\
       --setting-names YOUTUBE_API_KEY=... YOUTUBE_CHANNEL_ID=... IG_ACCESS_TOKEN=... IG_ACCOUNT_ID=...
  7. Stats refresh: REAL data only — pulls YouTube/Instagram when those keys are set, and is a
       no-op otherwise (it never fabricates). Refresh from the editor (More → Refresh stats). To
       run it hourly once keys exist, .github/workflows/stats-cron.yml POSTs the secret header
       (SWA managed funcs can't self-schedule); it just no-ops until real keys are present.
  8. Teardown anytime:  az group delete -n $RG --yes --no-wait

NOTE: SAS tokens expire $SAS_EXPIRY — regenerate + re-set the app settings before then.
The live domain ($([ -n "\${LIVE_DOMAIN:-}" ] && echo "\$LIVE_DOMAIN" || echo example.com)) is untouched by this script.
EOF
