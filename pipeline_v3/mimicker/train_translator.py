"""train_translator.py — the REVERSE DESANITIZER (decoupled style translator, the report's Approach 1).

A SEPARATE, focused model whose only job is restyle: take plain modern English (e.g. a big capable model's
reasoned answer) and rewrite it in Father Solanus Casey's writing voice, preserving the meaning. The big
model owns the substance; this layer owns the voice. Trained ONLY on the synthetic neutral->styled pairs
(data/train_synth_pairs.jsonl) — no roleplay persona, no causal-LM, no instruction pairs. That keeps it a
pure translator rather than a unified author (which is what train_qlora.py builds — left untouched).

Mirrors train_qlora.py's machinery (config.TRAIN profiles, BASE_MODEL env, 4-bit/bf16, single-GPU
device_map or FSDP via accelerate launch) so it runs the same way on Midway. Output: a LoRA adapter under
adapters/translator-<profile>/.

    python train_translator.py --profile qwen_fast --max-steps 30     # smoke the restyle loop
    PROFILE=qwen_top python train_translator.py --profile qwen_top     # the real run (FSDP via accelerate)

To run it through your SLURM script without editing it: point submit_job.sh's training line at this file,
or set TRAIN_SCRIPT=train_translator.py if you add that hook. (I did not modify submit_job.sh.)
"""
from __future__ import annotations
import argparse
import json
import os
import random

import mimic_config as config
import prompts                          # single source of truth for prompts (shared with restyle_flow.py)


def _rows(path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def build_translator_examples() -> list[dict]:
    """ONLY neutral->styled pairs, framed as restyle (neutral in, Solanus-voiced out). Prefers the richer,
    inference-matching, gated dataset from augment_pairs.py (translator_pairs.jsonl) when present; falls
    back to the original STRAP pairs (train_synth_pairs.jsonl)."""
    augmented = config.DATA / "translator_pairs.jsonl"
    src = augmented if augmented.exists() else config.F_SYNTH
    rng = random.Random(0)                # seeded -> reproducible instruction-dropout
    ex = []
    for r in _rows(src):
        if not r.get("neutral") or not r.get("styled"):
            continue
        msgs = [{"role": "user", "content": r["neutral"]},
                {"role": "assistant", "content": r["styled"]}]
        sys_msg = prompts.translator_system(rng)      # varied/empty -> prompt-independent restyle skill
        if sys_msg:
            msgs.insert(0, {"role": "system", "content": sys_msg})
        ex.append({"messages": msgs})
    return ex


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=list(config.TRAIN), default="qwen_fast")
    ap.add_argument("--max-steps", type=int, default=-1, help="cap steps for a quick smoke test")
    a = ap.parse_args()
    cfg = config.TRAIN[a.profile]
    base_model = os.environ.get("BASE_MODEL") or cfg["base_model"]

    import torch
    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
    from trl import SFTTrainer, SFTConfig

    examples = build_translator_examples()
    if not examples:
        raise SystemExit("no neutral->styled pairs — run data_prep.py --synth first (needs the centroid)")
    random.Random(0).shuffle(examples)
    n_eval = max(1, len(examples) // 10) if len(examples) >= 30 else 0
    eval_examples, train_examples = examples[:n_eval], examples[n_eval:]
    print(f"TRANSLATOR profile={a.profile} base={base_model} train={len(train_examples)} eval={len(eval_examples)}")

    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    world = int(os.environ.get("WORLD_SIZE", "1"))
    distributed = world > 1
    if distributed:
        torch.cuda.set_device(local_rank)
    tok = AutoTokenizer.from_pretrained(base_model)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    train_ds = Dataset.from_list(train_examples)
    eval_ds = Dataset.from_list(eval_examples) if n_eval else None

    quant = BitsAndBytesConfig(
        load_in_4bit=True, bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16, bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_storage=torch.bfloat16,
    ) if cfg.get("load_in_4bit") else None

    model = AutoModelForCausalLM.from_pretrained(
        base_model, quantization_config=quant, dtype=torch.bfloat16,
        device_map=None if distributed else "auto")
    if distributed:
        model.enable_input_require_grads()
    elif quant is not None:
        model = prepare_model_for_kbit_training(model)
    model = get_peft_model(model, LoraConfig(
        r=cfg["lora_r"], lora_alpha=cfg["lora_alpha"], lora_dropout=cfg["lora_dropout"],
        target_modules=config.LORA_TARGET_MODULES, task_type="CAUSAL_LM", bias="none"))
    model.print_trainable_parameters()

    grad_accum = max(1, cfg["grad_accum"] // world)
    eff_batch = cfg["batch_size"] * grad_accum * world
    print(f"  world={world} grad_accum={grad_accum} effective_batch={eff_batch} "
          f"({'bf16 LoRA' if quant is None else '4-bit QLoRA'})")

    out_dir = config.ADAPTERS / f"translator-{a.profile}"
    args = SFTConfig(
        output_dir=str(out_dir), num_train_epochs=cfg["epochs"], learning_rate=cfg["lr"],
        per_device_train_batch_size=cfg["batch_size"], gradient_accumulation_steps=grad_accum,
        max_length=cfg["max_seq_len"], max_steps=a.max_steps, logging_steps=10,
        save_strategy="epoch", eval_strategy=("epoch" if eval_ds is not None else "no"),
        bf16=True, gradient_checkpointing=True,
        gradient_checkpointing_kwargs={"use_reentrant": False}, warmup_ratio=0.03,
        lr_scheduler_type="cosine", report_to=[])
    trainer = SFTTrainer(model=model, args=args, train_dataset=train_ds,
                         eval_dataset=eval_ds, processing_class=tok)
    trainer.train()
    trainer.save_model(str(out_dir))
    tok.save_pretrained(str(out_dir))
    print(f"saved translator adapter -> {out_dir}")


if __name__ == "__main__":
    main()
