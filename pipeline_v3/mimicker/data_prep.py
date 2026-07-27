"""data_prep.py — turn Solanus's letters into training data + a style target.

Stage 0 of the report's recipe. Builds, from data/solanus_corpus.jsonl:

  FREE (default):
    • segments.jsonl       — 40-200 word style segments (stable stylometry band)
    • train_causal.jsonl   — whole letters for continued-pretraining (rhythm/lexicon)
    • train_instruct.jsonl — (situation prompt -> his letter) pairs (first-person voice)
    • style_profile.json   — interpretable stylometric fingerprint (+ counts)

  PAID (flags):
    • --embed   embed segments into the step_7 vector store (partition mimic::model@dim) and save the
                style CENTROID (mean unit vector) -> data/style_centroid.npy  [RAG + neural eval target]
    • --synth   bootstrap STRAP-style neutral->Solanus pairs: neutralize each segment with a capable
                model, keep (neutral -> original), gated by centroid cosine -> train_synth_pairs.jsonl

Run order:
    python extract_corpus.py
    python data_prep.py                 # free datasets + profile
    python data_prep.py --embed         # + RAG index + centroid (needs GEMINI_API_KEY)
    python data_prep.py --synth         # + synthetic pairs (paid LLM; parallelized)
"""
from __future__ import annotations
import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

import mimic_config as config
import stylometry


def _load_corpus() -> list[dict]:
    if not config.F_CORPUS.exists():
        raise FileNotFoundError(f"{config.F_CORPUS} missing — run extract_corpus.py first")
    return [json.loads(l) for l in config.F_CORPUS.read_text().splitlines() if l.strip()]


def _segments(body: str) -> list[str]:
    """Greedy sentence-packing into SEG_MIN..SEG_MAX word windows."""
    sents = re.split(r"(?<=[.!?])\s+", body)
    out, cur, n = [], [], 0
    for s in sents:
        w = len(s.split())
        if n + w > config.SEG_MAX_WORDS and n >= config.SEG_MIN_WORDS:
            out.append(" ".join(cur)); cur, n = [], 0
        cur.append(s); n += w
    if n >= config.SEG_MIN_WORDS:
        out.append(" ".join(cur))
    return out


def _instruct_prompt(r: dict) -> str:
    """A 'situation' prompt for an instruction pair (we lack the incoming letter, so condition on context)."""
    bits = []
    if r.get("recipient"): bits.append(f"to {r['recipient']}")
    if r.get("sender_location"): bits.append(f"from {r['sender_location']}")
    if r.get("date"): bits.append(f"on {r['date']}")
    ctx = ", ".join(bits) or "to a correspondent"
    return f"Write a letter {ctx}, in your own voice as Father Solanus Casey."


def build_free():
    rows = _load_corpus()
    segs, causal, instruct = [], [], []
    for r in rows:
        body = r["body"]
        causal.append({"id": r["id"], "text": body})
        instruct.append({"id": r["id"], "prompt": _instruct_prompt(r), "completion": body})
        for j, s in enumerate(_segments(body)):
            segs.append({"id": f"{r['id']}#{j}", "doc_id": r["id"], "text": s, "words": len(s.split())})
    _write(config.F_SEGMENTS, segs)
    _write(config.F_CAUSAL, causal)
    _write(config.F_INSTRUCT, instruct)
    prof = {"stylometry": stylometry.profile([s["text"] for s in segs]),
            "n_letters": len(rows), "n_segments": len(segs),
            "total_words": sum(r["words"] for r in rows),
            "space": config.STYLE_SPACE, "embedded": False}
    config.F_PROFILE.write_text(json.dumps(prof, ensure_ascii=False, indent=2))
    print(f"FREE: {len(rows)} letters -> {len(segs)} segments, {len(causal)} causal, "
          f"{len(instruct)} instruct pairs; profile -> {config.F_PROFILE.name}")


def build_embed():
    """Embed segments into the step_7 vector store + save the style centroid (the neural target)."""
    from lib import vectorstore
    from lib.providers import embed
    segs = [json.loads(l) for l in config.F_SEGMENTS.read_text().splitlines() if l.strip()]
    texts = [s["text"] for s in segs]
    vecs, _ = embed.embed_texts(texts, config.STYLE_EMBED_MODEL, config.STYLE_EMBED_DIM, task="document")
    metas = [{"doc_id": s["doc_id"], "text": s["text"]} for s in segs]
    vectorstore.write(config.STYLE_SPACE, [s["id"] for s in segs], np.asarray(vecs), metas)
    centroid = np.asarray(vecs).mean(axis=0)
    centroid /= (np.linalg.norm(centroid) or 1.0)
    np.save(config.F_CENTROID, centroid)
    prof = json.loads(config.F_PROFILE.read_text())
    prof.update({"embedded": True, "centroid_dim": int(centroid.shape[0])})
    config.F_PROFILE.write_text(json.dumps(prof, ensure_ascii=False, indent=2))
    print(f"EMBED: {len(segs)} segments -> {config.STYLE_SPACE}; centroid -> {config.F_CENTROID.name}")


def _neutralize(text: str) -> str:
    """STRAP step 1: strip style, keep meaning (the 'neutral' side of a synthetic pair)."""
    from lib.providers import llm
    prompt = ("Paraphrase the passage below into plain, neutral, modern English. Preserve the meaning and "
              "all facts, but remove any personal voice, era, idiom, or religious phrasing. Return ONLY the "
              f"paraphrase.\n\nPassage:\n{text}")
    out, _ = llm.generate(prompt, model=config.NEUTRALIZER_LLM, temperature=0.3)
    return out.strip()


def build_synth(workers: int = 8):
    """Bootstrap neutral->Solanus pairs, gated by centroid cosine (parallel; resumable-ish)."""
    from lib import vectorstore  # noqa: F401  (ensures step_7 importable)
    from lib.providers import embed
    if not config.F_CENTROID.exists():
        raise FileNotFoundError("run `data_prep.py --embed` first (need the style centroid for the gate)")
    centroid = np.load(config.F_CENTROID)
    segs = [json.loads(l) for l in config.F_SEGMENTS.read_text().splitlines() if l.strip()]
    pairs = []

    def work(s):
        neutral = _neutralize(s["text"])
        return {"id": s["id"], "neutral": neutral, "styled": s["text"]}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, s) for s in segs]
        done = 0
        for fut in as_completed(futs):
            try:
                pairs.append(fut.result())
            except Exception as e:                       # one bad paraphrase shouldn't kill the run
                print("  synth error:", str(e)[:80])
            done += 1
            if done % 25 == 0:
                print(f"  neutralized {done}/{len(segs)}")
    # style gate: the styled side must sit near Solanus's centroid (drop washed-out pairs)
    styled_vecs, _ = embed.embed_texts([p["styled"] for p in pairs], config.STYLE_EMBED_MODEL,
                                       config.STYLE_EMBED_DIM, task="document")
    kept = [p for p, v in zip(pairs, styled_vecs) if float(np.dot(v, centroid)) >= config.PAIR_STYLE_GATE]
    _write(config.F_SYNTH, kept)
    print(f"SYNTH: {len(kept)}/{len(pairs)} pairs kept (gate cos>={config.PAIR_STYLE_GATE}) "
          f"-> {config.F_SYNTH.name}")


def _write(path, rows):
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--embed", action="store_true", help="embed segments + save centroid (paid)")
    ap.add_argument("--synth", action="store_true", help="build synthetic neutral->styled pairs (paid)")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    build_free()
    if a.embed:
        build_embed()
    if a.synth:
        build_synth(a.workers)
