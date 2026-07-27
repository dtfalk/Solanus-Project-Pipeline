"""enrich_descriptions.py — detailed, BOOK-grounded descriptions for knowledge-graph entities.

The KG descriptions are short generic blurbs (~14 words). This rewrites them into richer, specific
summaries that explain each entity IN THE CONTEXT of Father Solanus Casey, his letters/notebooks, AND the
Crosby biography ("Thank God Ahead of Time", indexed as book-tgat@1536) — using the book as a full
knowledge source. Grounded only in retrieved evidence; never fabricated. These also help retrieval.

Targets the SIGNIFICANT entities (people/places/orgs that recur or connect) — not every one-off favor name.
Parallel LLM, cost-capped, resumable (skips entities already enriched in the sidecar). Writes:
  data/entity_descriptions.json   {entity_id: {label, description, evidence_n}}   (sidecar, resumable)
and patches data/graph.json node.description (backup graph.json.preenrich).

    python stages/enrich_descriptions.py --min-mentions 4 --max-cost 15
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                                    # noqa: E402
from lib import vectorstore, costlog            # noqa: E402
from lib.safeio import backup, atomic_write_text  # noqa: E402
from lib.providers import llm, embed            # noqa: E402

GRAPH = config.DATA / "graph.json"
OUT = config.DATA / "entity_descriptions.json"
STEP6 = config.REPO / "pipeline_v3" / "step_6"
BOOK_SPACE = "book-tgat@1536"
ENTITY_KINDS = {"person", "place", "organization", "condition", "event", "role"}


def _text_map():
    m = {}
    for fn in ("documents.json", "notebooks.json"):
        for rec in json.loads((STEP6 / fn).read_text()):
            t = (rec.get("text_by_label") or {}).get("src_content")
            if rec.get("id") and t:
                m[rec["id"]] = t
    return m


def _book_evidence(label, k=4):
    """Top passages from the Crosby biography about this entity (book as a knowledge source)."""
    try:
        qv, _ = embed.embed_texts([label], "gemini-embedding-001", 1536, task="query")
        hits = vectorstore.search(BOOK_SPACE, qv[0], k=k)
        return [(h.get("meta", {}).get("text") or "")[:500] for h in hits if h.get("score", 0) > 0.5]
    except Exception:
        return []


def enrich(node, edges_by_node, texts, model):
    label = node.get("label") or node["id"]
    # corpus evidence: the documents this entity is MENTIONED_IN
    docs = []
    for e in edges_by_node.get(node["id"], []):
        if (e.get("kind") or "").upper() == "MENTIONED_IN":
            other = e["target"] if e["source"] == node["id"] else e["source"]
            t = texts.get(other) or texts.get(other.split(":")[-1])
            if t:
                docs.append(t[:500])
        if len(docs) >= 5:
            break
    book = _book_evidence(label)
    corpus_txt = "\n\n".join(f"- {d}" for d in docs) or "(no letters/notebook passages found)"
    book_txt = "\n\n".join(f"- {b}" for b in book) or "(no biography passages found)"
    prompt = (
        f"Write a DETAILED, specific description of «{label}» (a {node.get('kind')}) as it pertains to "
        "Father Solanus Casey (Capuchin friar, 1870-1957) and his world. Explain who/what it is, its "
        "connection to Solanus and his correspondence/notebooks, and notable specifics a reader would want "
        "for context. Ground it ONLY in the evidence below — his letters/notebooks AND the Crosby biography "
        "'Thank God Ahead of Time'. Be 2-4 sentences, concrete and accurate; NEVER invent facts. If little "
        "is attested, say concisely what IS known. No citation markers, just clean prose.\n\n"
        f"FROM HIS LETTERS / NOTEBOOKS:\n{corpus_txt}\n\nFROM THE CROSBY BIOGRAPHY:\n{book_txt}")
    try:
        txt, _ = llm.generate(prompt, model=model, temperature=0.3)
        return {"id": node["id"], "label": label, "description": txt.strip(),
                "evidence_n": len(docs) + len(book)}
    except Exception as e:
        return {"id": node["id"], "label": label, "description": None, "error": str(e)[:80]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--min-mentions", type=int, default=4)
    ap.add_argument("--max-cost", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    g = json.loads(GRAPH.read_text())
    edges_by_node = {}
    for e in g["edges"]:
        edges_by_node.setdefault(e["source"], []).append(e)
        edges_by_node.setdefault(e["target"], []).append(e)
    # significant entities: real entity kinds, recurring (mention_count) or well-connected (degree)
    def deg(n): return len(edges_by_node.get(n["id"], []))
    targets = [n for n in g["nodes"] if n.get("kind") in ENTITY_KINDS
               and ((n.get("mention_count") or 0) >= a.min_mentions or deg(n) >= a.min_mentions + 2)]
    targets.sort(key=lambda n: -((n.get("mention_count") or 0) + deg(n)))
    if a.limit:
        targets = targets[:a.limit]
    done = json.loads(OUT.read_text()) if OUT.exists() else {}
    todo = [n for n in targets if n["id"] not in done]
    print(f"enrich: {len(targets)} significant entities, {len(todo)} to do (resume skipped {len(targets)-len(todo)})")
    texts = _text_map()
    c0 = costlog.snapshot()
    n_done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(enrich, n, edges_by_node, texts, a.model) for n in todo]
        for fut in as_completed(futs):
            r = fut.result()
            if r.get("description"):
                done[r["id"]] = {"label": r["label"], "description": r["description"], "evidence_n": r["evidence_n"]}
            n_done += 1
            run_usd = costlog.delta(c0, costlog.snapshot()).get("usd", 0.0)   # THIS run's spend only
            if n_done % 40 == 0:
                OUT.write_text(json.dumps(done, ensure_ascii=False))
                print(f"  {n_done}/{len(todo)} enriched; this run ${run_usd:.2f}")
            if a.max_cost and run_usd > a.max_cost:
                print(f"  cost cap ${a.max_cost} reached at {n_done} (resumable)"); break
    OUT.write_text(json.dumps(done, ensure_ascii=False))
    # patch graph.json descriptions (timestamped backup + atomic write). The descriptions also live in
    # entity_descriptions.json (OUT), so this graph patch is re-appliable for free after any rebuild.
    backup(GRAPH, "enrich")
    for n in g["nodes"]:
        d = done.get(n["id"])
        if d and d.get("description"):
            n["description"] = d["description"]
    atomic_write_text(GRAPH, json.dumps(g, ensure_ascii=False))
    print(f"\nDONE. {len(done)} entities enriched -> {OUT.name}; graph patched (backup graph.json.preenrich)")


if __name__ == "__main__":
    main()
