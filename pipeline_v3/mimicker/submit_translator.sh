#!/bin/bash
#SBATCH --job-name=solanus-translator
#SBATCH --account=pi-hcn1
#SBATCH --partition=hcn1-gpu
#SBATCH --qos=hcn1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=512G
#SBATCH --gres=gpu:4
#SBATCH --time=24:00:00
#SBATCH --output=logs/%x-%j.out
#SBATCH --error=logs/%x-%j.err

# Standalone launcher for the REVERSE-DESANITIZER (decoupled style translator) — train_translator.py.
# Identical Midway setup to submit_job.sh; it just trains the translator instead of the unified model and
# writes to adapters/translator-<profile>/. It reads data/translator_pairs.jsonl (the augmented, gated,
# inference-matching pairs) automatically, falling back to data/train_synth_pairs.jsonl. NO replay (a pure
# single-task restyler doesn't need general-instruct replay).

# ============================== EDIT ME ==============================
PROFILE="${PROFILE:-qwen_fast}"                                  # qwen_fast=7B | qwen_top=72B | qwen_top16=72B bf16
MODEL="${MODEL:-/project/smbowdre/dtfalk/models/cache/qwen_7b}"  # <-- the model folder to train
# ====================================================================

ENV_NAME="${MIMIC_ENV:-solanus-mimicker}"

module load cuda/12.6
module load python/miniforge-25.3.0
export PYTHONNOUSERSITE=1
source activate "$ENV_NAME"
[ "${CONDA_DEFAULT_ENV:-}" = "$ENV_NAME" ] || { echo "ERROR: conda env '$ENV_NAME' not active — build it with setup_env.sh"; exit 1; }
cd "$SLURM_SUBMIT_DIR"

export BASE_MODEL="$MODEL"
export HF_HOME="${HF_HOME:-/project/smbowdre/dtfalk/models/cache}"
export TORCH_HOME="$HF_HOME/torch"
export PYTHONUNBUFFERED=1
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export PYTORCH_ALLOC_CONF=expandable_segments:True
export CUDA_DEVICE_ORDER=PCI_BUS_ID
export TOKENIZERS_PARALLELISM=false
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export HF_DATASETS_OFFLINE=1

[ -d "$BASE_MODEL" ] || { echo "ERROR: model folder not found: $BASE_MODEL  (download it / fix the MODEL line)"; exit 1; }
echo "=== node $(hostname) | job $SLURM_JOB_ID ==="
echo "profile: $PROFILE | model: $BASE_MODEL  (TRANSLATOR)"
echo "python:  $(command -v python)"
python -c "import transformers, accelerate, huggingface_hub as h; print('transformers', transformers.__version__, '| accelerate', accelerate.__version__, '| hub', h.__version__)"
nvidia-smi

# Optional env: MAX_STEPS=30 (quick sanity)  USE_FSDP=1 (4-way FSDP). No REPLAY for the translator.
ARGS="--profile $PROFILE"
[ -n "${MAX_STEPS:-}" ] && ARGS="$ARGS --max-steps $MAX_STEPS"

if [ "${USE_FSDP:-0}" = "1" ]; then
  # 4-way FSDP (FULL_SHARD; every GPU computes — faster, but fussy with 4-bit device placement).
  NPROC="${SLURM_GPUS_ON_NODE:-4}"
  echo ">> FSDP accelerate launch ($NPROC GPUs): train_translator.py $ARGS"
  # shellcheck disable=SC2086
  "${CONDA_PREFIX}/bin/accelerate" launch --config_file fsdp_config.yaml --num_processes "$NPROC" train_translator.py $ARGS
else
  # DEFAULT: single process. device_map="auto" shards the model across all visible GPUs (naive
  # model-parallel) — slower, but rock-solid with 4-bit/QLoRA (no FSDP device-placement clash).
  echo ">> single-process (device_map=auto over all GPUs): train_translator.py $ARGS"
  # shellcheck disable=SC2086
  python train_translator.py $ARGS
fi

echo "=== done; adapter in adapters/translator-$PROFILE/ ==="
