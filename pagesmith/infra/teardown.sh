#!/usr/bin/env bash
# Tear down the ENTIRE new stack — just deletes its dedicated resource group.
# The current live stack lives in a DIFFERENT resource group and is untouched.
set -euo pipefail
RG="${1:-pagesmith-v2-rg}"
read -r -p "Delete resource group '$RG' and EVERYTHING in it? (type 'yes') " ok
[ "$ok" = "yes" ] || { echo "aborted"; exit 1; }
az group delete -n "$RG" --yes
echo "deleted $RG"
