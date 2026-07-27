#!/usr/bin/env bash
# prepare_data.sh — Stage 0 data prep, DECOUPLED from training. Produces data/*.jsonl that submit_job.sh
# consumes. Run it ONCE; then launch as many training jobs as you like against the same data.
#
# Run on a Midway3 LOGIN node, NOT as an sbatch GPU job: the --embed/--synth steps call the Gemini API
# (via step_7), and the compute nodes have no internet. It is CPU-light (API/IO bound), not a GPU job.
set -euo pipefail

module load python/miniforge-25.3.0 2>/dev/null || true   # harmless off-cluster
ENV_NAME="${MIMIC_ENV:-solanus-mimicker}"
export PYTHONNOUSERSITE=1
source activate "$ENV_NAME" 2>/dev/null || true           # off-cluster this no-ops; use your local env
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

: "${GEMINI_API_KEY:?set GEMINI_API_KEY — Stage 0 --embed/--synth call Gemini via step_7}"
# Embeddings + datasets cache go to shared storage (same place the training job reads from).
export HF_HOME="${HF_HOME:-/project/smbowdre/dtfalk/models/cache}"

python extract_corpus.py        # step_6 letters -> data/solanus_corpus.jsonl        (free, offline)
python data_prep.py             # segments + causal + instruct + stylometry profile  (free, offline)
python data_prep.py --embed     # RAG index + style centroid (the neural target)     (Gemini)
python data_prep.py --synth     # STRAP neutral->Solanus pairs, centroid-gated        (Gemini)

echo ">> data ready:"
ls -la data/*.jsonl
