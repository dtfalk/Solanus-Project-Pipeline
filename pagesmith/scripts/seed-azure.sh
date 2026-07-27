#!/usr/bin/env bash
# One-time seed of the NEW storage account from the repo:
#   app/data/*.json   → content-dev AND content-prod   (so the live site renders immediately)
#   app/media-content → media-dev AND media-prod
#
# Uses the account key for this admin-only seed (runtime still uses managed identity).
# Re-runnable; --overwrite makes it idempotent.
set -euo pipefail
ACCT="${1:?usage: seed-azure.sh <storage-account>}"
APP="$(cd "$(dirname "$0")/.." && pwd)/app"

KEY=$(az storage account keys list --account-name "$ACCT" --query "[0].value" -o tsv)

for cont in content-dev content-prod; do
  az storage blob upload-batch --account-name "$ACCT" --account-key "$KEY" \
    -d "$cont" -s "$APP/data" --pattern "*.json" --overwrite -o none
  echo "✓ seeded $cont (content JSON)"
done

for cont in media-dev media-prod; do
  az storage blob upload-batch --account-name "$ACCT" --account-key "$KEY" \
    -d "$cont" -s "$APP/media-content" --overwrite -o none
  echo "✓ seeded $cont (media)"
done

echo "seed complete → https://$ACCT.blob.core.windows.net/content-prod/index.json"
