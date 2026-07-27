"""train_qlora.py — QLoRA supervised fine-tune of an open model into Solanus's voice.

The report's recommended core build (Approach 2, unified model), low-resource variant: LoRA/QLoRA on a
mixture of (a) his whole letters [rhythm/lexicon], (b) situation->letter instruction pairs [first-person
voice], (c) synthetic neutral->Solanus pairs [controllable restyling], plus optional general-instruct
REPLAY to limit catastrophic forgetting. Trains a small adapter; the base model stays frozen in 4-bit.

This needs a GPU box (transformers + peft + trl + bitsandbytes). It is intentionally NOT run by any other
script. Pick a profile from config.TRAIN ("fast" = single 24GB GPU smoke; "top" = the real multi-GPU run).

    python train_qlora.py --profile fast --max-steps 30          # smoke test the loop
    python train_qlora.py --profile top                          # the top-of-the-line run
    python train_qlora.py --profile top --replay tatsu-lab/alpaca # mix in replay to limit forgetting

Output: a LoRA adapter under adapters/solanus-<profile>/ (load it in generate.py).
"""
from __future__ import annotations
import argparse
import json
import os
import random

import mimic_config as config

PERSONA = ("You are Father Solanus Casey, Capuchin friar (1870-1957). Write in your own gentle, humble, "
           "grateful voice ('Deo gratias'), as in your letters.")


def _rows(path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def build_examples() -> list[dict]:
    """Merge the three training sources into chat-format examples (system/user/assistant messages)."""
    ex = []
    for r in _rows(config.F_CAUSAL):
        ex.append({"messages": [{"role": "system", "content": PERSONA},
                                {"role": "user", "content": "Write a letter in your own voice."},
                                {"role": "assistant", "content": r["text"]}]})
    for r in _rows(config.F_INSTRUCT):
        ex.append({"messages": [{"role": "system", "content": PERSONA},
                                {"role": "user", "content": r["prompt"]},
                                {"role": "assistant", "content": r["completion"]}]})
    for r in _rows(config.F_SYNTH):
        ex.append({"messages": [{"role": "system", "content": PERSONA},
                                {"role": "user", "content": "Rewrite this plainly-worded note in your "
                                                            f"own voice:\n\n{r['neutral']}"},
                                {"role": "assistant", "content": r["styled"]}]})
    return ex


def add_replay(examples, replay_id, frac):
    """Mix in a slice of a general-instruct dataset so the model doesn't forget how to follow instructions."""
    try:
        from datasets import load_dataset
        n = int(len(examples) * frac / max(1e-6, 1 - frac))
        ds = load_dataset(replay_id, split=f"train[:{n}]")
        for r in ds:
            instr = r.get("instruction") or r.get("prompt") or ""
            out = r.get("output") or r.get("response") or r.get("completion") or ""
            if instr and out:
                examples.append({"messages": [{"role": "user", "content": instr},
                                              {"role": "assistant", "content": out}]})
        print(f"  + {n} replay examples from {replay_id}")
    except Exception as e:
        print("  replay skipped:", str(e)[:100])
    return examples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=list(config.TRAIN), default="fast")
    ap.add_argument("--replay", default=None, help="HF dataset id for general-instruct replay (optional)")
    ap.add_argument("--max-steps", type=int, default=-1, help="cap steps for a quick smoke test")
    a = ap.parse_args()
    cfg = config.TRAIN[a.profile]
    # BASE_MODEL env overrides the profile's repo id — point at a local 70B folder (e.g. a custom
    # /project/smbowdre/.../llama_70b dir) when it isn't in the standard HF cache layout.
    base_model = os.environ.get("BASE_MODEL") or cfg["base_model"]

    import torch
    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
    from trl import SFTTrainer, SFTConfig

    examples = build_examples()
    if not examples:
        raise SystemExit("no training data — copy data/*.jsonl into place first")
    # Hold out 10% of HIS letters (before mixing in replay) so the per-epoch eval loss is a clean
    # overfitting signal: when it flattens/rises while train loss keeps falling, you've trained enough.
    random.Random(0).shuffle(examples)
    n_eval = max(1, len(examples) // 10) if len(examples) >= 30 else 0
    eval_examples, train_examples = examples[:n_eval], examples[n_eval:]
    if a.replay:
        train_examples = add_replay(train_examples, a.replay, config.REPLAY_FRACTION)
    print(f"profile={a.profile} base={base_model} train={len(train_examples)} eval={len(eval_examples)}")
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    if int(os.environ.get("WORLD_SIZE", "1")) > 1:
        torch.cuda.set_device(local_rank)
    tok = AutoTokenizer.from_pretrained(base_model)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    train_ds = Dataset.from_list(train_examples)
    eval_ds = Dataset.from_list(eval_examples) if n_eval else None

    # WORLD_SIZE>1 means we were started by `accelerate launch` (FSDP path); plain `python` => 1.
    world = int(os.environ.get("WORLD_SIZE", "1"))
    distributed = world > 1

    quant = BitsAndBytesConfig(
        load_in_4bit=True, bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16, bnb_4bit_use_double_quant=True,
        # FSDP shards by flattening params, so the 4-bit STORAGE dtype must match the compute dtype.
        bnb_4bit_quant_storage=torch.bfloat16,
    ) if cfg.get("load_in_4bit") else None

    # Single process (desktop "fast", or a single-GPU run): device_map="auto" splits layers across GPUs.
    # FSDP (accelerate launch --num_processes N): NO device_map — each rank loads its own shard and FSDP
    # wraps the model; the accelerate config's cpu_ram_efficient_loading keeps host RAM in check.
    model = AutoModelForCausalLM.from_pretrained(
        base_model, quantization_config=quant, dtype=torch.bfloat16,
        device_map=None if distributed else "auto")
    if distributed:
        model.enable_input_require_grads()        # let grads reach LoRA through the frozen base under FSDP
    elif quant is not None:
        model = prepare_model_for_kbit_training(model)
    model = get_peft_model(model, LoraConfig(
        r=cfg["lora_r"], lora_alpha=cfg["lora_alpha"], lora_dropout=cfg["lora_dropout"],
        target_modules=config.LORA_TARGET_MODULES, task_type="CAUSAL_LM", bias="none"))
    model.print_trainable_parameters()

    # Under FSDP the ranks are data-parallel, so divide grad-accum by world size to hold the planned
    # effective batch (bs * grad_accum) constant regardless of GPU count.
    grad_accum = max(1, cfg["grad_accum"] // world)
    eff_batch = cfg["batch_size"] * grad_accum * world
    print(f"  world={world} grad_accum={grad_accum} effective_batch={eff_batch} "
          f"({'bf16 LoRA' if quant is None else '4-bit QLoRA'})")

    out_dir = config.ADAPTERS / f"solanus-{a.profile}"
    # args = SFTConfig(
    #     output_dir=str(out_dir), num_train_epochs=cfg["epochs"], learning_rate=cfg["lr"],
    #     per_device_train_batch_size=cfg["batch_size"], gradient_accumulation_steps=grad_accum,
    #     max_length=cfg["max_seq_len"], max_steps=a.max_steps, logging_steps=10,
    #     save_strategy="epoch", eval_strategy=("epoch" if eval_ds is not None else "no"),
    #     bf16=True, gradient_checkpointing=True,
    #     gradient_checkpointing_kwargs={"use_reentrant": False}, warmup_ratio=0.03,
    #     lr_scheduler_type="cosine", report_to=[])
    # trainer = SFTTrainer(model=model, args=args, train_dataset=train_ds,
    #                      eval_dataset=eval_ds, processing_class=tok)
    args = SFTConfig(
        output_dir=str(out_dir), 
        num_train_epochs=cfg["epochs"], 
        learning_rate=cfg["lr"],
        per_device_train_batch_size=cfg["batch_size"], 
        gradient_accumulation_steps=grad_accum,
        max_length=cfg["max_seq_len"], # Corrected from your previous query!
        max_steps=a.max_steps, 
        logging_steps=10,
        save_strategy="epoch", 
        eval_strategy=("epoch" if eval_ds is not None else "no"), # Use full name
        bf16=True, 
        gradient_checkpointing=True,
        gradient_checkpointing_kwargs={"use_reentrant": False}, 
        warmup_ratio=0.03,
        lr_scheduler_type="cosine", 
        report_to=[]
    )

    trainer = SFTTrainer(
        model=model, 
        args=args, 
        train_dataset=train_ds,
        eval_dataset=eval_ds, 
        processing_class=tok # Replaces deprecated tokenizer arg
    )

    trainer.train()
    trainer.save_model(str(out_dir))
    tok.save_pretrained(str(out_dir))
    print(f"saved adapter -> {out_dir}")


if __name__ == "__main__":
    main()
