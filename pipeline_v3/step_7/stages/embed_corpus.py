"""stages/embed_corpus.py — chunk the corpus and embed it into one partition per space.

Non-destructive (writes data/vectors/<space>/). Every embedding call is cost-logged. Supports a
small --limit batch for smoke tests/pricing. Default spaces come from config.EMBEDDING_MATRIX.

    python stages/embed_corpus.py --limit 40 --spaces gemini-embedding-001@768 bge-small-en-v1.5@384
    python stages/embed_corpus.py            # full corpus, full matrix (billed — only when told)
    python stages/embed_corpus.py --contextualized   # embed Contextual-Retrieval chunks (ctx space)
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402
from lib import chunks as chunks_lib, vectorstore  # noqa: E402
from lib.providers import embed     # noqa: E402

CTX_PATH = config.DATA / "contextualized_chunks.jsonl"


def parse_space(s: str):
    model, dim = s.rsplit("@", 1)
    return model, int(dim)


def _load_contextualized(limit=None):
    """Read contextualize_chunks' output → (ids, texts, metas) where each text is the situating
    context PREPENDED to the original chunk (Anthropic Contextual Retrieval). The original text is kept
    in meta['text'] so citations still show the source passage, not the synthetic preamble.
    """
    if not CTX_PATH.exists():
        raise FileNotFoundError(f"{CTX_PATH} not found — run contextualize_chunks first")
    rows = [json.loads(l) for l in CTX_PATH.read_text().splitlines() if l.strip()]
    rows = [r for r in rows if r.get("context")]           # skip any empty-context (dry-run) rows
    if limit:
        rows = rows[:limit]
    ids   = [r["id"] for r in rows]
    texts = [f"{r['context']}\n\n{r['text']}" for r in rows]
    metas = [{**r.get("meta", {}), "kind": r.get("kind"), "text": (r.get("text") or "")[:600],
              "contextualized": True} for r in rows]
    return ids, texts, metas


def run(spaces=None, limit=None, contextualized=False):
    """Embed the corpus into one vector partition per space.

    Args:
        spaces: list of "model@dim" space strings; None → config.EMBEDDING_MATRIX (or the ctx space
            when contextualized=True).
        limit: cap chunk count (strided/prefix sample) for a smoke test.
        contextualized: if True, embed the Contextual-Retrieval chunks (context + text) from
            data/contextualized_chunks.jsonl into the "…-ctx" alias space instead of raw chunks.
    """
    config.DATA.mkdir(parents=True, exist_ok=True)
    if contextualized:
        ids, texts, metas = _load_contextualized(limit=limit)
        spaces = spaces or ["gemini-embedding-001-ctx@1536"]
    else:
        cs = chunks_lib.build_chunks(limit=limit)
        texts = [c["text"] for c in cs]
        ids = [c["id"] for c in cs]
        metas = [{**c["meta"], "kind": c["kind"], "text": c["text"][:600]} for c in cs]
        spaces = spaces or [f"{m}@{d}" for (m, d) in config.EMBEDDING_MATRIX]
    for sp in spaces:
        model, dim = parse_space(sp)
        vecs, _ = embed.embed_texts(texts, model, dim)
        vectorstore.write(sp, ids, vecs, metas)
        print(f"  embedded {len(texts)} {'ctx-' if contextualized else ''}chunks -> {sp} (dim {vecs.shape[1]})")
    return len(texts), spaces


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--spaces", nargs="*", default=None, help="model@dim ...")
    ap.add_argument("--contextualized", action="store_true",
                    help="embed contextualized_chunks.jsonl (context+text) into the …-ctx space")
    a = ap.parse_args()
    n, sp = run(spaces=a.spaces, limit=a.limit, contextualized=a.contextualized)
    print(f"done: {n} chunks x {len(sp)} space(s)")
