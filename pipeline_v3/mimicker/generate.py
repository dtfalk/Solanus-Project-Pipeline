"""generate.py — write in Solanus's voice, grounded by RAG over his own letters.

Two paths (the report's Stage-1 baseline vs Stage-2 core build), so you can A/B before/after training:

  • BASELINE (default): a frozen model (step_7 llm.generate) + retrieved real exemplars in the prompt.
    No training; sets the floor. Mirrors the no-fine-tuning David toolkit.
  • TRAINED  (--adapter NAME): the QLoRA-tuned local model from train_qlora.py, still RAG-grounded.

RAG grounding (both paths): retrieve the k passages from his letters nearest the prompt (step_7 vector
store, partition mimic::model@dim), so any stance is anchored to something he actually wrote — and the
model is told to demur rather than invent where the passages are silent (anti-confabulation, report §3).

    python generate.py "What do you counsel a mother whose child is gravely ill?"
    python generate.py "..." --adapter solanus-top
"""
from __future__ import annotations
import argparse
import os

import mimic_config as config

PERSONA = ("You are Father Solanus Casey, Capuchin friar (1870-1957), writing in the FIRST PERSON in your "
           "own gentle, humble, grateful voice. Draw your phrasing and substance from the passages of your "
           "own letters below. Do NOT invent specific facts, people, or events the passages do not "
           "support — where you have not written on a matter, demur humbly. Deo gratias.")


def retrieve(prompt: str, k: int):
    """k nearest passages from his letters (returns [] if the style index isn't built yet)."""
    try:
        from lib import vectorstore
        from lib.providers import embed
        qv, _ = embed.embed_texts([prompt], config.STYLE_EMBED_MODEL, config.STYLE_EMBED_DIM, task="query")
        return vectorstore.search(config.STYLE_SPACE, qv[0], k=k)
    except Exception as e:
        print("  (RAG unavailable:", str(e)[:80], ")")
        return []


def _grounding(hits) -> str:
    if not hits:
        return "(no retrieved passages)"
    return "\n\n".join(f"[{i}] {(h.get('meta', {}).get('text') or '')[:500]}" for i, h in enumerate(hits, 1))


def baseline(prompt, k):
    from lib.providers import llm
    hits = retrieve(prompt, k)
    full = (f"{PERSONA}\n\nPassages from your letters:\n{_grounding(hits)}\n\n"
            f"Now respond to: {prompt}\n\nYour reply, in your own voice:")
    text, _ = llm.generate(full, model=config.NEUTRALIZER_LLM, temperature=config.GEN["temperature"])
    return text.strip(), hits


def trained(prompt, adapter, k):
    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
    from peft import PeftModel
    adir = config.ADAPTERS / adapter
    # The base MUST match what the adapter was trained on. Prefer BASE_MODEL (the local folder used at
    # train time, e.g. .../qwen_7b); else infer from the adapter name (longest key first).
    base = os.environ.get("BASE_MODEL")
    if not base:
        for key in ("qwen_top16", "qwen_top", "qwen_fast", "top16", "top", "fast"):
            if key in adapter:
                base = config.TRAIN[key]["base_model"]
                break
        base = base or config.TRAIN["fast"]["base_model"]
    quant = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_quant_type="nf4",
                               bnb_4bit_compute_dtype=torch.bfloat16, bnb_4bit_use_double_quant=True)
    tok = AutoTokenizer.from_pretrained(str(adir))
    model = AutoModelForCausalLM.from_pretrained(base, device_map="auto", dtype=torch.bfloat16,
                                                 quantization_config=quant)
    model = PeftModel.from_pretrained(model, str(adir))
    hits = retrieve(prompt, k)
    msgs = [{"role": "system", "content": PERSONA},
            {"role": "user", "content": f"Passages from your letters:\n{_grounding(hits)}\n\n{prompt}"}]
    ids = tok.apply_chat_template(msgs, add_generation_prompt=True, return_tensors="pt").to(model.device)
    out = model.generate(ids, max_new_tokens=config.GEN["max_new_tokens"],
                         temperature=config.GEN["temperature"], top_p=config.GEN["top_p"], do_sample=True)
    return tok.decode(out[0][ids.shape[1]:], skip_special_tokens=True).strip(), hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt")
    ap.add_argument("--adapter", default=None, help="adapter dir name under adapters/ (else frozen baseline)")
    ap.add_argument("-k", type=int, default=config.GEN["rag_k"])
    a = ap.parse_args()
    text, hits = (trained(a.prompt, a.adapter, a.k) if a.adapter else baseline(a.prompt, a.k))
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)
    print(f"(grounded in {len(hits)} retrieved passages from his letters)")


if __name__ == "__main__":
    main()
