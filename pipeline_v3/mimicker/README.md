# Solanus voice mimicker

Train an open model to write in **Father Solanus Casey's** voice, grounded by RAG over his own letters.
This is a **separate track** from the archival RAG tool (`../step_7`) — it *does* fine-tune (you have
GPUs), unlike the no-fine-tuning David toolkit in `/Style-Mimickry`. It reuses step_7's libraries
(`config`, `vectorstore`, `embed`, `llm`, `costlog`) by putting step_7 on `sys.path`.

Design follows `/Style-Mimickry-Research/style-mimickry-report.md`, adapted to our reality:
- **Corpus:** Solanus's **511 outgoing letters ≈ 124k words** (`text_by_label.src_content` in
  `../step_6/documents.json`). That is the *low-resource* style regime, so we lean on **synthetic
  neutral→styled pairs + RAG grounding**, not brute scale.
- **The hard part is faithfulness, not surface style.** Weights alone will *invent* opinions; so every
  stance-bearing answer is **RAG-grounded in his actual passages** and the model is told to **demur**
  where the passages are silent (report §3). Style is carried by the fine-tune; *stance is carried by
  retrieval.*

## The pipeline

| Step | Script | Cost | What it does |
|---|---|---|---|
| 0a | `extract_corpus.py` | free | pull his letters from the archive → `data/solanus_corpus.jsonl` |
| 0b | `data_prep.py` | free | segment + build `train_causal / train_instruct` + stylometry profile |
| 0c | `data_prep.py --embed` | paid* | embed segments → RAG index (`mimic::model@dim`) + **style centroid** |
| 0d | `data_prep.py --synth` | paid* | STRAP neutral→Solanus pairs, gated by centroid cosine |
| 1 | `generate.py "..."` | paid* | **baseline**: frozen model + retrieved exemplars (the floor) |
| 2 | `train_qlora.py --profile top` | GPU | QLoRA fine-tune on causal + instruct + synth (+ replay) |
| 3 | `generate.py "..." --adapter solanus-top` | GPU | the trained, RAG-grounded voice |
| 4 | `evaluate.py "..."` | free/paid* | centroid cosine + stylometry z-scores + AA margin |

\*“paid” = a few Gemini API calls via step_7 (`GEMINI_API_KEY`); fine-tuning + local inference are on your GPUs.

## Quickstart
```bash
cd pipeline_v3/mimicker
python extract_corpus.py
python data_prep.py                 # free datasets + profile
python data_prep.py --embed         # RAG index + style centroid (the neural target)
python data_prep.py --synth         # synthetic pairs (parallelized; gated)

# on the GPU box (see requirements.txt):
python train_qlora.py --profile fast --max-steps 30   # smoke the loop on one 24GB GPU
python train_qlora.py --profile top                   # the real run (see TRAINING_PLAN.md)
python generate.py "What do you counsel a mother whose child is gravely ill?" --adapter solanus-top
python evaluate.py --file out.txt
```

## Running on Midway3 (SLURM)

The `top` run targets the RCC **Midway3** GPU node (4× L40S = 192 GB; see `SYSTEM.md`). Compute nodes have
no internet, so models + replay data are **staged on a login node** first, then the job runs offline.

Environment is a **named conda env** activated with `source activate solanus-mimicker` (RCC pattern, same
as the solanus-annotation job). Account `pi-hcn1`; project/code under `/project/hcn1/dtfalk/`. Models are
**pre-downloaded as plain folders** under `/project/smbowdre/dtfalk/models/cache/` (`qwen_72b/`,
`llama_70b/`, …) — NOT the HF cache layout, so the trainer loads via `BASE_MODEL` (a folder path).
`submit_job.sh` maps the profile to the folder: `qwen_fast`→`qwen_7b`, `qwen_top*`→`qwen_72b`.

Current build = **Qwen2.5**: smoke on 7B, then the 72B (same family ⇒ the 7B exercises the exact
tokenizer/chat-template/FSDP path the 72B uses).

```bash
# --- on a LOGIN node (has internet), from the repo on /project ---
./setup_env.sh                                       # one-time: conda env "solanus-mimicker" + deps
source activate solanus-mimicker
export HF_HOME=/project/smbowdre/dtfalk/models/cache
export GEMINI_API_KEY=...                            # Stage 0 --embed/--synth call Gemini
./prepare_data.sh                                    # Stage 0 -> data/*.jsonl  (run ONCE)

# Stage the 7B smoke model as a plain folder (ungated — no HF token needed):
hf download Qwen/Qwen2.5-7B-Instruct --local-dir "$HF_HOME/qwen_7b"

# --- submit (offline; 4-way FSDP over the L40S) ---
PROFILE=qwen_fast MAX_STEPS=20 sbatch submit_job.sh  # quick sanity (~minutes), then:
PROFILE=qwen_fast sbatch submit_job.sh               # full 7B smoke
PROFILE=qwen_top  sbatch submit_job.sh               # the 72B run (4-bit QLoRA)
# PROFILE=qwen_top16 sbatch submit_job.sh            # 72B in 16-bit LoRA (uses the 192GB)
squeue -u $USER                                      # tail -f logs/solanus-qlora-*.out
```

**Data prep is decoupled** (`prepare_data.sh`): it runs on the login node because `--embed`/`--synth` need
the Gemini API and compute nodes are offline. Run it once; relaunch training freely against the same data.

`submit_job.sh` pins partition `hcn1-gpu` / account `pi-hcn1` / qos `hcn1`, requests `gpu:4` + 32 CPUs +
512 GB + 24 h, `source activate`s the conda env (aborting if it didn't take), checks the model is staged,
sets `HF_HUB_OFFLINE`/`TRANSFORMERS_OFFLINE`, and launches **FSDP** (`fsdp_config.yaml`,
FULL_SHARD over the 4 L40S — every GPU computes). Output adapter: `adapters/solanus-<profile>/`.
> Precision: base in 4-bit nf4 (`top`) or bf16 (`top16`), **compute always bf16** (never fp16 — bf16's
> fp32 exponent range trains stably with no loss-scaling, and L40S has native bf16). Run plain
> `python train_qlora.py --profile top` for a single-process fallback (model-parallel via `device_map`).

## What's real vs. scaffolded
- **Real & runnable now:** corpus extraction, segmentation, instruction/causal sets, stylometry profile,
  RAG index + centroid, synthetic-pair bootstrap, the QLoRA trainer (TRL/PEFT/bitsandbytes), grounded
  generation (frozen baseline + trained adapter), neural+stylometry evaluation.
- **TODOs (documented in code):** a *trained* authorship-attribution discriminator (`evaluate.py` ships a
  transparent nearest-centroid heuristic), the held-out **stance-faithfulness QA set**, and the optional
  research layer (activation/persona-vector steering; Horikawa-style iterative refinement toward the
  centroid). See `TRAINING_PLAN.md` for where these slot in.

## Ethics
Cloning a real person's voice and views carries impersonation/misinformation risk (report, Caveats). The
serving persona discloses it is a study impression, and grounding+demurral keep it from fabricating his
positions. Keep that framing in any public-facing use.
