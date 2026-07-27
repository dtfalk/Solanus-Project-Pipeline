"""lib/vectorstore.py — simple partitioned local vector store (one partition per embedding space).

A "space" is "model@dim" (e.g. "gemini-embedding-001@768"); each gets its own folder under
data/vectors/<space>/ with vectors.npy + ids.json + metas.jsonl. RAG selects a space (a variable)
and queries only that partition. Brute-force cosine (vectors are pre-normalized) — instant at this
corpus size (~9k). Swappable for LanceDB later without changing callers.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import numpy as np

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402


def _dir(space: str) -> Path:
    return config.DATA / "vectors" / space.replace("/", "_")


def write(space: str, ids: list, vectors, metas: list) -> Path:
    d = _dir(space)
    d.mkdir(parents=True, exist_ok=True)
    np.save(d / "vectors.npy", np.asarray(vectors, dtype=np.float32))
    (d / "ids.json").write_text(json.dumps(ids))
    with open(d / "metas.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    return d


def load(space: str):
    d = _dir(space)
    ids = json.loads((d / "ids.json").read_text())
    vecs = np.load(d / "vectors.npy")
    metas = [json.loads(l) for l in open(d / "metas.jsonl")]
    return ids, vecs, metas


def search(space: str, qvec, k: int = 5, where: dict | None = None):
    ids, vecs, metas = load(space)
    q = np.asarray(qvec, dtype=np.float32)
    q = q / (np.linalg.norm(q) or 1.0)
    sims = vecs @ q
    order = np.argsort(-sims)
    out = []
    for i in order:
        if where and any(metas[i].get(kk) != vv for kk, vv in where.items()):
            continue
        out.append({"id": ids[i], "score": float(sims[i]), "meta": metas[i]})
        if len(out) >= k:
            break
    return out


def spaces() -> list:
    base = config.DATA / "vectors"
    return sorted(p.name for p in base.glob("*")) if base.exists() else []
