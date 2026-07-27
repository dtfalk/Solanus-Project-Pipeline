#!/usr/bin/env bash
# Provision the NEW Pagesmith stack (lean, managed-identity, $0-idle API).
# Creates a dedicated resource group so the whole thing is one teardownable unit
# and the current live stack (different RG) is never touched.
#
# REVIEW the variables, then run.  Requires: az login as an Owner/Contributor.
# Nothing here is run automatically — it is a checklist you execute.
set -euo pipefail

# ------------------------------- edit these -------------------------------
SUB="f6ae9b66-a85b-4f77-80ea-ad8bbf6c6e7d"   # Azure subscription 1
RG="pagesmith-v2-rg"
LOC="eastus2"
ACCT="pagesmithsitedata"          # storage account — GLOBALLY UNIQUE, 3-24 lowercase alnum
SWA="pagesmith-site"              # static web app name
FN="pagesmith-site-api"           # function app — GLOBALLY UNIQUE
# --------------------------------------------------------------------------

az account set --subscription "$SUB"
az group create -n "$RG" -l "$LOC" -o none
echo "✓ resource group $RG"

# ---- Storage: one account, per-environment containers ----
az storage account create -n "$ACCT" -g "$RG" -l "$LOC" \
  --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access true -o none

# Data protection: versioning + change feed (=> point-in-time restore) + soft delete.
az storage account blob-service-properties update --account-name "$ACCT" \
  --enable-versioning true --enable-change-feed true \
  --enable-delete-retention true --delete-retention-days 30 \
  --enable-container-delete-retention true --container-delete-retention-days 30 -o none

# Public-read for website assets (content JSON + media); private for submissions.
for c in content-dev content-prod media-dev media-prod; do
  az storage container create -n "$c" --account-name "$ACCT" --auth-mode login --public-access blob -o none
done
az storage container create -n questionnaire --account-name "$ACCT" --auth-mode login --public-access off -o none
echo "✓ storage $ACCT + containers"

# ---- Function App (Flex Consumption, ~$0 idle) with system-assigned identity ----
az functionapp create -n "$FN" -g "$RG" --flexconsumption-location "$LOC" \
  --runtime node --runtime-version 20 --storage-account "$ACCT" -o none
az functionapp identity assign -n "$FN" -g "$RG" -o none
MI=$(az functionapp identity show -n "$FN" -g "$RG" --query principalId -o tsv)
SCOPE=$(az storage account show -n "$ACCT" -g "$RG" --query id -o tsv)
# Data-plane writes via identity — NO account key, NO SAS.
az role assignment create --assignee "$MI" --role "Storage Blob Data Contributor" --scope "$SCOPE" -o none
echo "✓ function app $FN (+ identity + RBAC)"

# --- Application Insights (telemetry: requests, failures, dependencies, logs) ---
az monitor log-analytics workspace create -g "$RG" -n pagesmith-logs -l "$LOC" -o none
WS_ID=$(az monitor log-analytics workspace show -g "$RG" -n pagesmith-logs --query id -o tsv)
az monitor app-insights component create --app pagesmith-insights -g "$RG" -l "$LOC" --workspace "$WS_ID" -o none
AI_CONN=$(az monitor app-insights component show --app pagesmith-insights -g "$RG" --query connectionString -o tsv)
echo "✓ application insights (pagesmith-insights)"

# App settings: account + container names + App Insights. Note the ABSENCE of any
# storage key/SAS/conn string — that is what makes storage.js use managed identity.
az functionapp config appsettings set -n "$FN" -g "$RG" --settings \
  AZURE_STORAGE_ACCOUNT_NAME="$ACCT" \
  AZURE_CONTENT_DEV_CONTAINER=content-dev \
  AZURE_CONTENT_PROD_CONTAINER=content-prod \
  AZURE_MEDIA_CONTAINER=media-dev \
  AZURE_MEDIA_PROD_CONTAINER=media-prod \
  AZURE_QUESTIONNAIRE_CONTAINER=questionnaire \
  MEDIA_PUBLIC_BASE="https://$ACCT.blob.core.windows.net/media-dev" \
  MEDIA_FULL_QUALITY_PREFIXES="epk-media/" \
  APPLICATIONINSIGHTS_CONNECTION_STRING="$AI_CONN" -o none
echo "✓ function app settings (App Insights wired; no storage secrets)"

# ---- Static Web App (Standard) + link the Function App as its /api ----
az staticwebapp create -n "$SWA" -g "$RG" -l "$LOC" --sku Standard -o none
FN_ID=$(az functionapp show -n "$FN" -g "$RG" --query id -o tsv)
az staticwebapp backends link -n "$SWA" -g "$RG" --backend-resource-id "$FN_ID" --backend-region "$LOC" -o none
echo "✓ static web app $SWA (+ linked backend)"

HOST=$(az staticwebapp show -n "$SWA" -g "$RG" --query defaultHostname -o tsv)
cat <<EOF

Done. New stack is up at: https://$HOST

Next (see LAUNCH.md):
  1. GitHub repo Variables: STORAGE_ACCOUNT=$ACCT  FUNCTION_APP_NAME=$FN
  2. GitHub repo Secret:    AZURE_STATIC_WEB_APPS_API_TOKEN
       az staticwebapp secrets list -n $SWA -g $RG --query "properties.apiKey" -o tsv
  3. OIDC for the API deploy: AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_SUBSCRIPTION_ID
  4. Seed blobs:  bash scripts/seed-azure.sh $ACCT
  5. Invite yourself as editor (email):
       az staticwebapp users invite -n $SWA -g $RG --authentication-provider github \\
         --user-details <your-github-username> --roles editor --domain $HOST \\
         --invitation-expiration-in-hours 168
  6. Push to main → deploys. Test on https://$HOST, then move the domain (LAUNCH.md §Cutover).

Hardening (optional): switch the Function host storage (AzureWebJobsStorage) to identity
too — set AzureWebJobsStorage__accountName=$ACCT, remove the conn-string setting, and grant
the identity "Storage Blob Data Owner" on $ACCT. Data writes already use identity.
EOF
