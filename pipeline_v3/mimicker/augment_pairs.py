"""augment_pairs.py — more + BETTER (neutral -> Solanus-styled) pairs for the reverse-desanitizer.

Two upgrades over data_prep.py --synth:
  1. INFERENCE-MATCHING neutral side. At inference the translator receives a big model's modern, reasoned
     answer — not a plain paraphrase of an 1890s letter. So we generate the neutral side to LOOK like
     modern-LLM prose (clear, organised, contemporary), paired with his AUTHENTIC styled passage. Train
     then matches inference.
  2. MULTIPLE neutralizations per passage (variety -> robustness), each passed through two GATES:
       • neutrality      — the neutral side must be FAR from his style centroid (it's actually de-styled)
       • content-preserve — neutral and styled must stay semantically close (meaning preserved, no drift)
Writes data/translator_pairs.jsonl (the translator's dedicated dataset; the original train_synth_pairs.jsonl
is left as-is). train_translator.py prefers this file when present.

    python augment_pairs.py --per 3 --max-cost 10        # 3 neutralizations/passage, gated, parallel (PAID)
    python augment_pairs.py --limit 20                   # smoke
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

import mimic_config as config
import prompts

OUT = config.DATA / "translator_pairs.jsonl"
# The 'modern' neutral should imitate the BIG inference model's answer style (distribution match), so we
# generate it with the big model; 'plain'/'roundtrip' are cheap diversity from the flash neutralizer.
BIG_MODEL = "gemini-2.5-pro"
# Gemini embeddings are TOPIC-dominated, so neutral vs styled differ only a little in absolute cosine to
# his centroid (~0.86 styled vs ~0.82 neutral). So gate on a RELATIVE style move, not an absolute floor:
#   • STYLE_DELTA_MIN — neutralization must pull the text OFF his voice (styled_cos - neutral_cos >= δ)
#   • CONTENT_MIN     — neutral & styled must stay semantically close (meaning preserved, no drift)
# (The research pass is checking whether a dedicated authorship/StyleDistance embedding is a better style
#  discriminator here; these thresholds are the pragmatic version until then.)
STYLE_DELTA_MIN = 0.015
CONTENT_MIN = 0.83

def _segments():
    p = config.F_SEGMENTS
    if not p.exists():
        raise SystemExit(f"{p} missing — run data_prep.py first")
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def _neutralize(text, mode):
    """Neutral side via the SHARED prompts.neutralizer. 'modern' uses the big inference model (distribution
    match); 'plain'/'roundtrip' use the cheap flash neutralizer for diversity."""
    from lib.providers import llm
    model = BIG_MODEL if mode == "modern" else config.NEUTRALIZER_LLM
    out, _ = llm.generate(prompts.neutralizer(text, mode), model=model, temperature=0.7)
    return out.strip()


def work(seg, per):
    """Produce up to `per` gated pairs for one styled passage."""
    from lib.providers import embed
    styled = seg["text"]
    # weight toward inference-matching 'modern'; add 'roundtrip' + 'plain' for diversity (many neutrals->1 real target)
    modes = (["modern", "roundtrip", "plain", "modern"] * 3)[:per]
    cand = []
    for m in modes:
        try:
            cand.append((m, _neutralize(styled, m)))
        except Exception:
            pass
    if not cand:
        return []
    centroid = work.centroid
    texts = [styled] + [c[1] for c in cand]
    vecs, _ = embed.embed_texts(texts, config.STYLE_EMBED_MODEL, config.STYLE_EMBED_DIM, task="document")
    sv = np.asarray(vecs[0])
    styled_cos = float(np.dot(sv, centroid))           # his real passage's distance to his own voice
    pairs = []
    for (mode, neutral), v in zip(cand, vecs[1:]):
        v = np.asarray(v)
        style_delta = styled_cos - float(np.dot(v, centroid))   # >0 = neutral pulled OFF his voice
        content = float(np.dot(v, sv))                          # high = meaning preserved
        if style_delta >= STYLE_DELTA_MIN and content >= CONTENT_MIN:
            pairs.append({"neutral": neutral, "styled": styled, "kind": mode,
                          "doc_id": seg.get("doc_id"), "style_delta": round(style_delta, 3),
                          "content": round(content, 3)})
    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per", type=int, default=3, help="neutralizations attempted per passage")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--max-cost", type=float, default=10.0)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    if not config.F_CENTROID.exists():
        raise SystemExit("run data_prep.py --embed first (need the style centroid for the gates)")
    work.centroid = np.load(config.F_CENTROID)
    segs = _segments()
    if a.limit:
        segs = segs[:a.limit]
    from lib import costlog
    c0 = costlog.snapshot()
    kept, attempted, done = [], 0, 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(work, s, a.per) for s in segs]
        for fut in as_completed(futs):
            try:
                pairs = fut.result()
            except Exception:
                pairs = []
            kept.extend(pairs); attempted += a.per; done += 1
            if done % 40 == 0:
                with OUT.open("w") as f:
                    for p in kept:
                        f.write(json.dumps(p, ensure_ascii=False) + "\n")
                run = costlog.delta(c0, costlog.snapshot()).get("usd", 0.0)
                print(f"  {done}/{len(segs)} passages -> {len(kept)} kept pairs (gate pass-rate "
                      f"{len(kept)/max(1,attempted):.0%}); ${run:.2f}")
            if a.max_cost and costlog.delta(c0, costlog.snapshot()).get("usd", 0.0) > a.max_cost:
                print(f"  cost cap ${a.max_cost} reached (resumable)"); break
    with OUT.open("w") as f:
        for p in kept:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")
    print(f"\nDONE. {len(kept)} gated pairs (style_delta>= {STYLE_DELTA_MIN}, content>= {CONTENT_MIN}) -> {OUT.name}")
    print(f"  vs the original data/train_synth_pairs.jsonl (left untouched).")


if __name__ == "__main__":
    main()
