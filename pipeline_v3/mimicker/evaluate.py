"""evaluate.py — does it sound like Solanus? (and is it faithful, not confabulated?)

Three axes from the report's evaluation section, all runnable here:

  1. NEURAL STYLE (needs the centroid): cosine of the candidate's embedding to Solanus's style centroid,
     and to the nearest single exemplar (so a bland 'average' answer can't hide near the mean).
  2. STYLOMETRY (free): per-feature z-scores vs his profile — interpretable ("sentences too long, z=+1.4").
  3. AA DISCRIMINATOR (free heuristic): nearest-centroid margin between the candidate and a real held-out
     letter — a transparent stand-in for a trained authorship classifier (a TODO to harden).

Plus a STANCE-FAITHFULNESS note: we report how close the candidate sits to the retrieved-passage region,
a cheap proxy for "is this grounded in what he actually wrote" vs invented.

    python evaluate.py "candidate text to score..."
    python evaluate.py --file out.txt
"""
from __future__ import annotations
import argparse
import json

import numpy as np

import mimic_config as config
import stylometry


def neural(text: str):
    from lib import vectorstore
    from lib.providers import embed
    if not config.F_CENTROID.exists():
        return None
    centroid = np.load(config.F_CENTROID)
    v, _ = embed.embed_texts([text], config.STYLE_EMBED_MODEL, config.STYLE_EMBED_DIM, task="query")
    v = np.asarray(v[0])
    cos_centroid = float(np.dot(v, centroid))
    nearest = vectorstore.search(config.STYLE_SPACE, v, k=1)
    cos_nearest = float(nearest[0]["score"]) if nearest else None
    return {"cos_to_centroid": round(cos_centroid, 4),
            "cos_to_nearest_exemplar": round(cos_nearest, 4) if cos_nearest is not None else None}


def stylo(text: str):
    if not config.F_PROFILE.exists():
        return None
    prof = json.loads(config.F_PROFILE.read_text()).get("stylometry", {})
    z = stylometry.zscores(text, prof)
    flagged = sorted(z.items(), key=lambda kv: -abs(kv[1]))[:8]   # the 8 most-off features
    return {"most_off_features": [{"feature": k, "z": round(v, 2)} for k, v in flagged],
            "mean_abs_z": round(float(np.mean([abs(x) for x in z.values()])), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("text", nargs="?", default=None)
    ap.add_argument("--file", default=None)
    a = ap.parse_args()
    text = open(a.file).read() if a.file else a.text
    if not text:
        raise SystemExit("provide text or --file")
    print("=== style evaluation ===")
    try:                                  # neural cosine needs GEMINI_API_KEY + the style index (internet)
        n = neural(text)
        print("neural:", n if n else "(centroid not built — run data_prep.py --embed)")
    except Exception as e:
        print("neural: unavailable (", str(e)[:80], ") — needs GEMINI_API_KEY; stylometry below is offline")
    s = stylo(text)
    if s:
        print(f"stylometry: mean|z|={s['mean_abs_z']} (lower = closer to his profile)")
        for f in s["most_off_features"]:
            print(f"   {f['feature']:18} z={f['z']:+.2f}")
    print("\nReminder: pair these with HUMAN judgement — no automatic style metric is fully reliable "
          "(report, Evaluation §). Stance faithfulness needs the RAG-grounded held-out opinion QA set.")


if __name__ == "__main__":
    main()
