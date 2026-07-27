#!/bin/bash
#SBATCH --job-name=solanus-qlora
#SBATCH --account=pi-hcn1
#SBATCH --partition=hcn1-gpu
#SBATCH --qos=hcn1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=1000G
#SBATCH --gres=gpu:4
#SBATCH --time=24:00:00
#SBATCH --output=logs/log.out
#SBATCH --error=logs/log.err

# ============================== EDIT ME ==============================
PROFILE="${PROFILE:-qwen_top}"                                  # qwen_fast=7B | qwen_top=72B | qwen_top16=72B bf16
MODEL="${MODEL:-/project/smbowdre/dtfalk/models/cache/qwen_72b}"  # <-- the model folder to train
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
echo "profile: $PROFILE | model: $BASE_MODEL"
echo "python:  $(command -v python)"
python -c "import transformers, accelerate, huggingface_hub as h; print('transformers', transformers.__version__, '| accelerate', accelerate.__version__, '| hub', h.__version__)"
nvidia-smi

# 4-way FSDP (FULL_SHARD over the L40S; every GPU computes). Knobs in fsdp_config.yaml.
# Optional env: MAX_STEPS=20 (quick sanity)  REPLAY="" (turn off replay)  REPLAY=<hf-dataset-id>
NPROC="${SLURM_GPUS_ON_NODE:-4}"
REPLAY="${REPLAY-tatsu-lab/alpaca}"
ARGS="--profile $PROFILE"
[ -n "$REPLAY" ]        && ARGS="$ARGS --replay $REPLAY"
[ -n "${MAX_STEPS:-}" ] && ARGS="$ARGS --max-steps $MAX_STEPS"
echo ">> accelerate launch ($NPROC GPUs): train_qlora.py $ARGS"
# shellcheck disable=SC2086
"${CONDA_PREFIX}/bin/accelerate" launch --config_file fsdp_config.yaml --num_processes "$NPROC" \
    train_qlora.py $ARGS

echo "=== done; adapter in adapters/solanus-$PROFILE/ ==="
