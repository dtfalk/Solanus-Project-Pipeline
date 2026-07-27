"""config.py — Solanus voice-mimicker configuration (one editable registry).

This is a SEPARATE track from the archival RAG tool (pipeline_v3/step_7). It trains an open model to
write in Father Solanus Casey's voice, grounded by RAG over his own letters. It reuses step_7's libs
(config / vectorstore / embed / llm / costlog) by putting step_7 on sys.path — so a style index here is
the exact same shape as a step_7 retrieval index.

Design follows Style-Mimickry-Research/style-mimickry-report.md, adapted to:
  • a ~124k-word corpus (Solanus's 511 outgoing letters) — the LOW-RESOURCE style regime, so we lean on
    synthetic neutral->styled pairs + RAG grounding rather than brute scale; and
  • local GPUs available for training (LoRA/QLoRA), unlike the no-fine-tuning David toolkit.

Edit the tokens below; every script consumes them.
"""
from __future__ import annotations
import sys
from pathlib import Path

MIMICKER = Path(__file__).resolve().parent
PIPELINE_V3 = MIMICKER.parent
REPO = PIPELINE_V3.parent
STEP6_DOCS = PIPELINE_V3 / "step_6" / "documents.json"     # Solanus's letters live here
STEP7 = PIPELINE_V3 / "step_7"
if str(STEP7) not in sys.path:                              # reuse step_7's config/vectorstore/embed/llm
    sys.path.insert(0, str(STEP7))

DATA = MIMICKER / "data"
DATA.mkdir(exist_ok=True)
ADAPTERS = MIMICKER / "adapters"                            # trained LoRA adapters land here
ADAPTERS.mkdir(exist_ok=True)

# ---------------------------------------------------------------- corpus extraction
# Solanus authored the correspondence volumes; the letter BODY is text_by_label.src_content. We keep the
# greeting/farewell/recipient/date/sender-location as metadata (useful for instruction-pair prompts and
# for RAG filters). A record counts as "his" if its signature/farewell names Solanus (or it is unsigned
# in a his-letters volume — kept by default; flip KEEP_UNSIGNED to be strict).
SIGNATURE_HINTS = ("solanus", "f.s.c", "fr. casey", "bernard f. casey", "your brother", "o.m. cap", "o.f.m. cap")
KEEP_UNSIGNED = True
MIN_LETTER_WORDS = 12                                       # drop fragments too short to carry style

# ---------------------------------------------------------------- segmentation (for centroid + causal LM)
# The LICW report: stylometric count-features are noisy below ~100 words and diluted above ~180. Segment
# his prose into that band for the style centroid; causal-LM training uses whole letters.
SEG_MIN_WORDS = 40
SEG_MAX_WORDS = 200

# ---------------------------------------------------------------- style/RAG space (reuses step_7 vectorstore)
# A dedicated partition so the mimicker index never collides with the archival index.
STYLE_EMBED_MODEL = "gemini-embedding-001"
STYLE_EMBED_DIM = 1536
STYLE_SPACE = f"mimic::{STYLE_EMBED_MODEL}@{STYLE_EMBED_DIM}"

# ---------------------------------------------------------------- synthetic-pair bootstrap (STRAP-style)
# Neutralize his letters with a capable model, then train neutral->Solanus. Uses step_7's llm.generate.
NEUTRALIZER_LLM = "gemini-2.5-flash"        # cheap, good paraphraser for the neutral side
PAIR_STYLE_GATE = 0.55                       # drop synthetic pairs whose styled side drifts from his centroid

# ---------------------------------------------------------------- training (LoRA / QLoRA)
# Two profiles. "fast" proves the loop on a single 24GB GPU; "top" is the report's recommended run on a
# multi-GPU box. Pick with: python train_qlora.py --profile top
TRAIN = {
    "fast": {                                # iterate quickly, validate the pipeline
        "base_model": "meta-llama/Llama-3.1-8B-Instruct",
        "load_in_4bit": True, "lora_r": 16, "lora_alpha": 32, "lora_dropout": 0.05,
        "epochs": 3, "lr": 2e-4, "batch_size": 4, "grad_accum": 4, "max_seq_len": 1024,
    },
    "top": {                                 # the "top of the line" run (report's core build), 4-bit QLoRA
        "base_model": "meta-llama/Llama-3.3-70B-Instruct",
        "load_in_4bit": True, "lora_r": 32, "lora_alpha": 64, "lora_dropout": 0.05,
        "epochs": 4, "lr": 1e-4, "batch_size": 1, "grad_accum": 16, "max_seq_len": 2048,
    },
    "top16": {                               # same run in 16-bit LoRA (NO 4-bit): higher fidelity, fits
        "base_model": "meta-llama/Llama-3.3-70B-Instruct",   # 70B bf16 (~140GB) sharded by FSDP over the
        "load_in_4bit": False,                                # 4×L40S=192GB box. Use when VRAM allows.
        "lora_r": 32, "lora_alpha": 64, "lora_dropout": 0.05,
        "epochs": 4, "lr": 1e-4, "batch_size": 1, "grad_accum": 16, "max_seq_len": 2048,
    },
    # --- Qwen2.5 track (current build). On Midway set BASE_MODEL to the local folder; submit_job.sh maps
    # qwen_fast->qwen_7b and qwen_top*->qwen_72b automatically. Same family => the 7B smoke faithfully
    # exercises the 72B's tokenizer/chat-template/FSDP path. Qwen2.5 uses the same proj names as Llama.
    "qwen_fast": {                           # Qwen2.5-7B smoke test — validate the loop fast
        "base_model": "Qwen/Qwen2.5-7B-Instruct",
        "load_in_4bit": True, "lora_r": 16, "lora_alpha": 32, "lora_dropout": 0.05,
        "epochs": 3, "lr": 2e-4, "batch_size": 4, "grad_accum": 4, "max_seq_len": 1024,
    },
    "qwen_top": {                            # Qwen2.5-72B, 4-bit QLoRA — the real run
        "base_model": "Qwen/Qwen2.5-72B-Instruct",
        "load_in_4bit": True, "lora_r": 32, "lora_alpha": 64, "lora_dropout": 0.05,
        "epochs": 6, "lr": 1e-4, "batch_size": 1, "grad_accum": 16, "max_seq_len": 2048,
    },
    "qwen_top16": {                          # Qwen2.5-72B, 16-bit LoRA (uses the 192GB headroom)
        "base_model": "Qwen/Qwen2.5-72B-Instruct",
        "load_in_4bit": False, "lora_r": 32, "lora_alpha": 64, "lora_dropout": 0.05,
        "epochs": 6, "lr": 1e-4, "batch_size": 1, "grad_accum": 16, "max_seq_len": 2048,
    },
}
LORA_TARGET_MODULES = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]
# fraction of general-instruct REPLAY mixed in to limit catastrophic forgetting (report §4)
REPLAY_FRACTION = 0.15

# ---------------------------------------------------------------- inference / serving
GEN = {"temperature": 0.7, "top_p": 0.92, "max_new_tokens": 2048, "rag_k": 6}

# data artifacts (written by data_prep.py)
F_CORPUS = DATA / "solanus_corpus.jsonl"          # one row per letter (body + metadata)
F_SEGMENTS = DATA / "segments.jsonl"              # 40-200 word style segments
F_CAUSAL = DATA / "train_causal.jsonl"            # raw letters for continued-pretraining
F_INSTRUCT = DATA / "train_instruct.jsonl"        # (situation prompt -> his letter) pairs
F_SYNTH = DATA / "train_synth_pairs.jsonl"        # neutral -> Solanus pairs (STRAP)
F_CENTROID = DATA / "style_centroid.npy"
F_PROFILE = DATA / "style_profile.json"
