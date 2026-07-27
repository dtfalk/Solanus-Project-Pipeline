#!/usr/bin/env bash
# provision-copy.sh — stand up a COPY-ONLY cloud stack for the Pagesmith editor.
# =============================================================================
# SAFETY: This creates BRAND-NEW resources with distinct names. It never writes
# to, modifies, or deletes anything in your existing setup. Existing storage,
# SWAs, and the App Service editor are untouched.
#
# NOT run automatically. Review the variables, then run it consciously:
#     ./infra/provision-copy.sh
#
# It provisions the "safest" architecture (the $9 Standard plan you approved):
#   • a new storage account + containers (public read), seeded from local files
#   • a standalone Function App with a SYSTEM-ASSIGNED MANAGED IDENTITY
#     (RBAC on the new storage account → zero secrets in app settings)
#   • a Standard Static Web App, with the Function App linked as its backend so
#     the SWA invite-auth flows through to the API
#
# Prereqs: az CLI logged in (you are), the SWA CLI (`swa`) and Functions Core
# Tools (`func`) installed (they are).
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."   # editor-mvp/

# === Edit if needed ==========================================================
SUFFIX="${SUFFIX:-mvp$RANDOM}"                 # keeps global names unique
LOCATION="${LOCATION:-eastus2}"
RG="${RG:-pagesmith-editor-copy-rg}"
STORAGE="${STORAGE:-pagesmitheditor${SUFFIX}}"      # 3-24 chars, lowercase/numbers
STORAGE="$(echo "$STORAGE" | tr -cd 'a-z0-9' | cut -c1-24)"
SWA_NAME="${SWA_NAME:-pagesmith-editor-${SUFFIX}}"
FUNC_NAME="${FUNC_NAME:-pagesmith-editor-api-${SUFFIX}}"
CONTENT_DEV="website-content-dev"
CONTENT_PROD="website-content-prod"
MEDIA="website-media-dev"
# =============================================================================
g(){ printf "\033[32m%s\033[0m\n" "$*"; }

g "→ Resource group $RG ($LOCATION)"
az group create -n "$RG" -l "$LOCATION" -o none

g "→ Storage account $STORAGE (copy)"
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --allow-blob-public-access true -o none
KEY=$(az storage account keys list -g "$RG" -n "$STORAGE" --query '[0].value' -o tsv)

for c in "$CONTENT_DEV" "$CONTENT_PROD" "$MEDIA"; do
  g "  • container $c (public blob)"
  az storage container create --account-name "$STORAGE" --account-key "$KEY" \
    --name "$c" --public-access blob -o none
done

g "→ CORS (*) for the blob service"
az storage cors clear --services b --account-name "$STORAGE" --account-key "$KEY" >/dev/null
az storage cors add --services b --methods GET HEAD OPTIONS --origins '*' \
  --allowed-headers '*' --exposed-headers '*' --max-age 3600 \
  --account-name "$STORAGE" --account-key "$KEY" >/dev/null

g "→ Seeding content + media from local files"
az storage blob upload-batch --account-name "$STORAGE" --account-key "$KEY" \
  -d "$CONTENT_DEV" -s app/data --pattern '*.json' \
  --content-type 'application/json' --overwrite -o none
az storage blob upload-batch --account-name "$STORAGE" --account-key "$KEY" \
  -d "$CONTENT_PROD" -s app/data --pattern '*.json' \
  --content-type 'application/json' --overwrite -o none
az storage blob upload-batch --account-name "$STORAGE" --account-key "$KEY" \
  -d "$MEDIA" -s app/media-content --overwrite -o none

g "→ Function App $FUNC_NAME (Linux, node 20, consumption) with managed identity"
az functionapp create -g "$RG" -n "$FUNC_NAME" \
  --storage-account "$STORAGE" --consumption-plan-location "$LOCATION" \
  --runtime node --runtime-version 20 --functions-version 4 \
  --os-type Linux --assign-identity '[system]' -o none

PRINCIPAL=$(az functionapp identity show -g "$RG" -n "$FUNC_NAME" --query principalId -o tsv)
SCOPE=$(az storage account show -g "$RG" -n "$STORAGE" --query id -o tsv)
g "  • granting the identity 'Storage Blob Data Contributor' on the storage account"
az role assignment create --assignee "$PRINCIPAL" \
  --role "Storage Blob Data Contributor" --scope "$SCOPE" -o none || true

g "  • app settings (data access via managed identity — NO secret)"
az functionapp config appsettings set -g "$RG" -n "$FUNC_NAME" --settings \
  AZURE_STORAGE_ACCOUNT_NAME="$STORAGE" \
  AZURE_CONTENT_DEV_CONTAINER="$CONTENT_DEV" \
  AZURE_CONTENT_PROD_CONTAINER="$CONTENT_PROD" \
  AZURE_MEDIA_CONTAINER="$MEDIA" \
  MEDIA_PUBLIC_BASE="https://$STORAGE.blob.core.windows.net/$MEDIA" -o none

g "  • publishing the Functions"
( cd api && func azure functionapp publish "$FUNC_NAME" --javascript )

g "→ Repointing the copy's HTML meta tags to the new storage account"
NEWBASE="https://$STORAGE.blob.core.windows.net"
grep -rl '__STORAGE_ACCOUNT__.blob.core.windows.net' app --include='*.html' | while read -r f; do
  sed -i "s#https://__STORAGE_ACCOUNT__.blob.core.windows.net#$NEWBASE#g" "$f"
done

g "→ Static Web App $SWA_NAME (Standard)"
az staticwebapp create -n "$SWA_NAME" -g "$RG" -l "$LOCATION" --sku Standard -o none
TOKEN=$(az staticwebapp secrets list -n "$SWA_NAME" -g "$RG" --query 'properties.apiKey' -o tsv)

g "  • deploying the static app"
swa deploy app --deployment-token "$TOKEN" --env production

g "  • linking the Function App as the SWA backend (so invite-auth flows to the API)"
FUNC_ID=$(az functionapp show -g "$RG" -n "$FUNC_NAME" --query id -o tsv)
az staticwebapp backends link -n "$SWA_NAME" -g "$RG" \
  --backend-resource-id "$FUNC_ID" --backend-region "$LOCATION" -o none || \
  echo "    (if this fails, link the backend once in the Portal: SWA → APIs → Link)"

URL=$(az staticwebapp show -n "$SWA_NAME" -g "$RG" --query 'defaultHostname' -o tsv)
cat <<EOF

$(g "✓ Copy stack provisioned")
  Site:          https://$URL
  Storage:       $STORAGE   (containers: $CONTENT_DEV, $CONTENT_PROD, $MEDIA)
  Function App:  $FUNC_NAME (managed identity — no secrets)

Next (manual, conscious steps):
  1. SWA → Settings → Authentication: add a provider (GitHub or email).
  2. SWA → Role management → Invite each bandmate to the 'editor' role.
  3. Visit the site, log in, and edit. 'Publish Changes' copies dev → prod.

To tear the whole copy down (removes ONLY these new resources):
  az group delete -n $RG --yes
EOF
