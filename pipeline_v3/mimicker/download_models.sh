#!/usr/bin/env bash
# download_models.sh — reproducibly fetch the base models for the Solanus mimicker QLoRA run.
#
#   ./download_models.sh fast     # just Llama-3.1-8B-Instruct  (~16 GB)  -> "fast" profile
#   ./download_models.sh top      # just Llama-3.3-70B-Instruct (~140 GB) -> "top" profile
#   ./download_models.sh both     # both (default)
#
# Models referenced by mimic_config.py:TRAIN. Embeddings/neutralizer are Gemini API (no download).
# Transport: HF Xet (hf-xet, already in the venv) — parallel, deduped, near line-speed.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILES="${1:-both}"

# Cache location precedence:
#   1. explicit HF_HUB_CACHE          (you set it)
#   2. $HF_HOME/hub                    (Midway3: HF_HOME=/project/smbowdre/dtfalk/models/cache)
#   3. project-local .hf_home/hub      (this desktop; keeps the big blobs off the 563 GB / volume)
# Whatever resolves here MUST be re-exported at train time so from_pretrained() finds the cache.
if   [ -n "${HF_HUB_CACHE:-}" ]; then :
elif [ -n "${HF_HOME:-}"      ]; then export HF_HUB_CACHE="$HF_HOME/hub"
else                                  export HF_HUB_CACHE="$HERE/.hf_home/hub"
fi
mkdir -p "$HF_HUB_CACHE"

# Prefer the venv's tools, fall back to whatever is on PATH.
HF="$HERE/venv/bin/hf";     [ -x "$HF" ] || HF="$(command -v hf)"
PY="$HERE/venv/bin/python"; [ -x "$PY" ] || PY="$(command -v python3)"

# Idempotent: ensures fast transport exists on a fresh box (no-op where hf-xet is already installed).
"$PY" -m pip install -q -U "huggingface_hub[hf_xet]" >/dev/null 2>&1 || true

# --- auth: the Meta-Llama repos are GATED ---------------------------------------------------------
if [ -z "${HF_TOKEN:-}" ] && ! "$HF" auth whoami >/dev/null 2>&1; then
  cat <<EOF
!! No Hugging Face credentials found, and these are GATED Meta-Llama repos.
   1) Accept the license (one click, usually auto-approved within minutes) on each page you want:
        https://huggingface.co/meta-llama/Llama-3.1-8B-Instruct
        https://huggingface.co/meta-llama/Llama-3.3-70B-Instruct
   2) Make a READ token:  https://huggingface.co/settings/tokens
   3) Then EITHER:   export HF_TOKEN=hf_xxx   &&  ./download_models.sh $PROFILES
            OR:      $HF auth login           &&  ./download_models.sh $PROFILES
EOF
  exit 1
fi
[ -n "${HF_TOKEN:-}" ] && export HF_TOKEN

# Pull safetensors only: skip original/*.pth (a full duplicate copy) and any .gguf/.pth.
dl () {
  echo ">> downloading $1  ->  $HF_HUB_CACHE"
  "$HF" download "$1" --exclude "original/*" "*.gguf" "*.pth"
}

# Stage the general-instruct REPLAY dataset into the HF datasets cache so the (offline) Midway3 compute
# node can read it. train_qlora.py --replay tatsu-lab/alpaca pulls slices from this cache. Harmless on the
# desktop (just caches it); only matters for the "top" run, which is why it's skipped for "fast".
stage_replay () {
  echo ">> staging replay dataset tatsu-lab/alpaca into the datasets cache"
  "$PY" - <<'PY'
from datasets import load_dataset
load_dataset("tatsu-lab/alpaca", split="train")
print("   alpaca cached")
PY
}

case "$PROFILES" in
  fast) dl meta-llama/Llama-3.1-8B-Instruct ;;
  top)  dl meta-llama/Llama-3.3-70B-Instruct; stage_replay ;;
  both) dl meta-llama/Llama-3.1-8B-Instruct; dl meta-llama/Llama-3.3-70B-Instruct; stage_replay ;;
  *)    echo "usage: $0 [fast|top|both]"; exit 2 ;;
esac

cat <<EOF

>> done. Cache populated at: $HF_HUB_CACHE

   Desktop smoke test (export the same cache var so from_pretrained() finds it):
     export HF_HUB_CACHE=$HF_HUB_CACHE
     $PY train_qlora.py --profile fast --max-steps 30

   Midway3: run this on a LOGIN node with HF_HOME=/project/smbowdre/dtfalk/models/cache exported first,
   then 'sbatch submit_job.sh' (the job reads this same cache offline).
EOF
