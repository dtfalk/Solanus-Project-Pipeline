#!/usr/bin/env bash
# setup_env.sh — NUKE-AND-REBUILD a clean, self-contained conda env on a Midway3 LOGIN node.
# Fixes the ~/.local shadowing for good: parks ~/.local site-packages, recreates the env, and installs
# everything INTO the env in one consistent resolve. Run from the repo dir:  ./setup_env.sh
#
# Not `set -e`: module/conda return nonzero benignly; we check the things that matter explicitly.
set -uo pipefail

ENV_NAME="${MIMIC_ENV:-solanus-mimicker}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

module load cuda/12.6
module load python/miniforge-25.3.0
conda deactivate 2>/dev/null || true
export PYTHONNOUSERSITE=1                 # ignore ~/.local for THIS shell too

# 1) Park ~/.local site-packages so nothing there can shadow the env (reversible: *.disabled).
for sp in "$HOME"/.local/lib/python3.*/site-packages; do
  [ -d "$sp" ] || continue
  echo ">> parking user-site: $sp -> ${sp}.disabled"
  rm -rf "${sp}.disabled"
  mv "$sp" "${sp}.disabled"
done

# 2) Recreate the conda env from scratch.
echo ">> recreating conda env: $ENV_NAME"
conda env remove -n "$ENV_NAME" -y 2>/dev/null || true
conda create -n "$ENV_NAME" python=3.12 -y || { echo "ERROR: conda create failed"; exit 1; }
source activate "$ENV_NAME"
[ "${CONDA_DEFAULT_ENV:-}" = "$ENV_NAME" ] || { echo "ERROR: could not activate $ENV_NAME"; exit 1; }
echo ">> python: $(command -v python)"

# 3) Install everything INTO the env. torch from the cu124 index; then ONE resolve for the rest +
#    huggingface_hub[hf_xet] so versions stay mutually consistent (e.g. datasets' fsspec<=2026.4.0).
python -m pip install -U pip wheel
python -m pip install torch --index-url https://download.pytorch.org/whl/cu124
python -m pip install -r "$HERE/requirements.txt" "huggingface_hub[hf_xet]"

# 4) Verify every key package imports FROM the env (sys.prefix), with versions.
echo ">> verifying the stack is self-contained ..."
python - <<'PY'
import importlib, sys
crit = ["torch","transformers","accelerate","peft","trl","datasets",
        "safetensors","tokenizers","huggingface_hub","hf_xet"]
ok = True
for m in crit:
    try:
        mod = importlib.import_module(m)
        f = getattr(mod, "__file__", "") or ""
        in_env = f.startswith(sys.prefix)
        ok = ok and in_env
        print(f"  {m:16} {'ENV' if in_env else 'NOT-ENV <--':<11} {getattr(mod,'__version__','?')}")
    except Exception as e:
        ok = False
        print(f"  {m:16} IMPORT-FAIL  {str(e)[:60]}")
try:                                   # bitsandbytes needs CUDA — may not import on the login node
    import bitsandbytes as bnb
    print(f"  {'bitsandbytes':16} {'ENV' if bnb.__file__.startswith(sys.prefix) else 'NOT-ENV':<11} {bnb.__version__}")
except Exception as e:
    print(f"  {'bitsandbytes':16} (import deferred to GPU node: {str(e)[:40]})")
print()
print("RESULT:", "ALL CRITICAL PACKAGES IN ENV ✓" if ok else "*** SOMETHING NOT IN ENV — see NOT-ENV above ***")
PY

cat <<EOF

>> env ready: $ENV_NAME   (your ~/.local was parked to *.disabled; mv it back if you ever need it)

Next:
   source activate $ENV_NAME
   export HF_HOME=/project/smbowdre/dtfalk/models/cache
   ./download_qwen.sh 7b
   MAX_STEPS=20 sbatch submit_job.sh      # sanity, then:  sbatch submit_job.sh
EOF
