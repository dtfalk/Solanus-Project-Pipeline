"""restyle_flow.py — the reverse-desanitizer INFERENCE loop (small translator + big verifier).

    plain text (a big model's reasoned answer)
        -> restyle()      small translator rewrites it in Solanus's voice
        -> verify()       a BIG model checks faithfulness: meaning preserved? any added/changed facts?
        -> if consistent  -> return the restyled text
           else           -> repair() (edit to fix the flagged issues, keep the voice) -> verify() ...
        -> loop to convergence (or max_iters); accept the best-scoring attempt.

The translator owns the VOICE, the big model owns the SUBSTANCE and guards FAITHFULNESS — so a modern
reasoning model's content comes out sounding like him, with no hallucinated facts.

restyle() is pluggable so the loop is testable NOW, before the LoRA translator is trained:
  • RESTYLE_BACKEND=frozen  (default) — a frozen big model + real Solanus exemplars as the stand-in restyler
  • RESTYLE_BACKEND=adapter  — load adapters/translator-<profile>/ (the trained reverse desanitizer)

    python restyle_flow.py "Please be kind to people who treat you badly, and pray for them."
    RESTYLE_BACKEND=adapter ADAPTER=translator-qwen_top python restyle_flow.py "..."

NOTE: verifier/repair prompts + thresholds are the pragmatic first version; the research pass
(restyle-data-and-loop workflow) will tune the faithfulness metric + loop policy.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys

import mimic_config as config
# Single source of truth. TRANSLATOR_SYS here is the SAME object train_translator.py used — so the adapter
# is run on-distribution. The judge/repair/cite prompts are shared with the training-data gate.
from prompts import TRANSLATOR_SYS, faithfulness_prompt, repair_prompt, cite_transfer_prompt

CITE = re.compile(r"\[\d+(?:\s*,\s*\d+)*\]")
def _cite_set(t): return set(re.findall(r"\d+", " ".join(CITE.findall(t))))
def _strip_cites(t): return re.sub(r"\s+([.,;:])", r"\1", CITE.sub("", t)).strip()

BIG_MODEL = os.environ.get("BIG_MODEL", "gemini-2.5-pro")     # the verifier/repairer (and frozen restyler)
MAX_ITERS = int(os.environ.get("MAX_ITERS", "3"))


def _exemplars(k=3):
    """A few of his real passages to ground the FROZEN restyler's voice (few-shot)."""
    p = config.F_SEGMENTS
    if not p.exists():
        return []
    rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    return [r["text"] for r in rows[:k]]


# --------------------------------------------------------------------------- restyle backends
def restyle_frozen(text):
    from lib.providers import llm
    ex = "\n\n".join(f"EXAMPLE OF HIS VOICE:\n{e}" for e in _exemplars())
    prompt = (f"{TRANSLATOR_SYS}\n\n{ex}\n\nNow rewrite THIS passage in that same voice, preserving its "
              f"meaning and every fact exactly:\n\n{text}")
    out, _ = llm.generate(prompt, model=BIG_MODEL, temperature=0.6)
    return out.strip()


_ADAPTER = {}
def restyle_adapter(text):
    """Load the trained translator LoRA once and generate locally (the real reverse desanitizer)."""
    if "pipe" not in _ADAPTER:
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM
        from peft import PeftModel
        adir = config.ADAPTERS / os.environ.get("ADAPTER", "translator-qwen_fast")
        base = os.environ.get("BASE_MODEL") or json.loads((adir / "adapter_config.json").read_text()).get("base_model_name_or_path")
        tok = AutoTokenizer.from_pretrained(base)
        model = AutoModelForCausalLM.from_pretrained(base, dtype=torch.bfloat16, device_map="auto")
        model = PeftModel.from_pretrained(model, str(adir))
        _ADAPTER.update(tok=tok, model=model)
    tok, model = _ADAPTER["tok"], _ADAPTER["model"]
    msgs = [{"role": "system", "content": TRANSLATOR_SYS}, {"role": "user", "content": text}]
    ids = tok.apply_chat_template(msgs, add_generation_prompt=True, return_tensors="pt").to(model.device)
    import torch
    with torch.no_grad():
        out = model.generate(ids, max_new_tokens=config.GEN["max_new_tokens"], temperature=0.6,
                             top_p=config.GEN["top_p"], do_sample=True)
    return tok.decode(out[0][ids.shape[1]:], skip_special_tokens=True).strip()


def restyle(text):
    return restyle_adapter(text) if os.environ.get("RESTYLE_BACKEND") == "adapter" else restyle_frozen(text)


# --------------------------------------------------------------------------- verify + repair (big model)
def _json(s):
    m = re.search(r"\{.*\}", s, re.S)
    try:
        return json.loads(m.group(0)) if m else {}
    except Exception:
        return {}


def verify(original, restyled):
    """The SHARED faithfulness judge (same prompt that gates training data): structured scores + the
    specific missing/added items so repair can act. Returns {consistent, score, missing, added}."""
    from lib.providers import llm
    out, _ = llm.generate(faithfulness_prompt(original, restyled), model=BIG_MODEL, temperature=0.0)
    v = _json(out)
    ce, nh = v.get("content_equivalence", 0), v.get("no_hallucination", 0)
    return {"consistent": (v.get("verdict") == "pass") or (ce >= 4 and nh >= 4),
            "score": (ce, nh), "missing": v.get("missing", []), "added": v.get("added", [])}


def repair(original, restyled, missing, added):
    """Edit RESTYLED to restore `missing` + remove `added`, keeping his voice (shared repair prompt)."""
    from lib.providers import llm
    out, _ = llm.generate(repair_prompt(original, restyled, missing, added), model=BIG_MODEL, temperature=0.4)
    return out.strip()


def attach_citations(plain_cited, styled):
    """The check layer re-attaches PLAIN's citation markers onto the styled answer (semantic alignment),
    with a cheap EXACT-SET guarantee: the styled output must end up with the same set of [n] as PLAIN."""
    from lib.providers import llm
    want = _cite_set(plain_cited)
    if not want:
        return styled
    out, _ = llm.generate(cite_transfer_prompt(plain_cited, styled), model=BIG_MODEL, temperature=0.2)
    out = out.strip()
    if _cite_set(out) != want:        # one strict retry, else fall back to the PLAIN CITED text
        markers = ", ".join(f"[{n}]" for n in sorted(want, key=int))
        out2, _ = llm.generate(cite_transfer_prompt(plain_cited, styled) +
                               f"\n\nThe cited STYLED must contain EXACTLY these markers, no more, no fewer: {markers}",
                               model=BIG_MODEL, temperature=0.0)
        # If citation transfer fails twice, DON'T ship voiced-but-uncited prose next to a full Sources
        # panel (silent citation loss). Fall back to the plain cited answer — it carries the exact [n]
        # set, so the guarantee holds: the user-facing answer's [n] always match `plain`.
        out = out2.strip() if _cite_set(out2) == want else plain_cited
    return out


def desanitize(plain_text, max_iters=MAX_ITERS, verbose=False):
    """Full loop: restyle citation-free prose -> verify -> (repair -> verify)* -> RE-ATTACH citations.
    `plain_text` may carry [n] markers; we strip them for the restyle (clean prose in/out) and the check
    layer puts them back at the end, so the user-facing styled answer is citation-complete."""
    cited = plain_text
    plain = _strip_cites(plain_text) if _cite_set(plain_text) else plain_text
    history = []
    current = restyle(plain)
    best = None
    for it in range(max_iters):
        v = verify(plain, current)
        n_issues = len(v["missing"]) + len(v["added"])
        history.append({"iter": it, "text": current, "consistent": v["consistent"],
                        "score": v["score"], "missing": v["missing"], "added": v["added"]})
        if verbose:
            print(f"[iter {it}] consistent={v['consistent']} score={v['score']} "
                  f"missing={len(v['missing'])} added={len(v['added'])}")
        if best is None or n_issues < best[1]:
            best = (current, n_issues, v["consistent"])
        if v["consistent"]:
            best = (current, n_issues, True)
            break
        current = repair(plain, current, v["missing"], v["added"])     # fix and re-check
    styled_cited = attach_citations(cited, best[0])
    return {"final": styled_cited, "consistent": best[2], "iters": len(history),
            "sources_cited": sorted(_cite_set(cited), key=int), "plain": cited, "history": history,
            **({"note": "max_iters reached — most faithful attempt"} if not best[2] else {})}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("text", help="plain-English passage (e.g. a big model's answer) to put in his voice")
    ap.add_argument("--max-iters", type=int, default=MAX_ITERS)
    a = ap.parse_args()
    r = desanitize(a.text, max_iters=a.max_iters, verbose=True)
    print("\n--- RESTYLED (Solanus voice) ---\n" + r["final"])
    print(f"\n[consistent={r['consistent']} iters={r['iters']}"
          + (f"  {r.get('note')}" if r.get("note") else "") + "]")


if __name__ == "__main__":
    main()
