"""verify_connections.py — LLM-grounded adjudication of CO-OCCURRENCE connections (the same-page problem).

APPEARS_WITH edges are built from raw co-occurrence: two entities recorded in the same document. But a
single Seraphic Mass Association enrollment list or a page of separate favors names many people who are NOT
actually connected. This pass reads the SHARED documents' text and rules, per edge, whether it is a REAL
relationship or merely a same-page co-mention — with a relationship label, evidence quote, and confidence.

Annotates each APPEARS_WITH edge in data/graph.json (backup graph.json.preconnverify) with:
  verified ("real" | "coincidental" | "unknown"), relationship, confidence, evidence[]
so the viz/dossier can show genuine connections and de-emphasize same-page coincidence.

    python stages/verify_connections.py                 # all APPEARS_WITH edges (PAID, parallel)
    python stages/verify_connections.py --min-weight 2  # skip the weakest (single-shared-doc) edges
    python stages/verify_connections.py --limit 50      # smoke a subset
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
import config                       # noqa: E402
from lib.providers import llm       # noqa: E402
from lib.safeio import backup, atomic_write_text   # noqa: E402

GRAPH = config.DATA / "graph.json"
# The paid adjudication is ALSO mirrored here so it survives a graph.json rebuild and can be re-applied
# with zero model calls (--reapply). Without this side-car the verdicts live only inside graph.json and a
# rebuild would force a full paid re-run. Keyed by the unordered {source,target} pair.
VERDICTS = config.DATA / "connection_verdicts.json"
STEP6 = config.REPO / "pipeline_v3" / "step_6"
MAX_RECORDS = 3                     # shared docs to show the LLM per edge
EXCERPT = 600

# The four edge attributes this pass owns — the unit that persists to the side-car and re-applies.
_VERDICT_FIELDS = ("verified", "relationship", "verify_confidence", "verify_evidence")


def _pair(e: dict) -> str:
    """Order-independent key for an APPEARS_WITH edge, so re-apply matches regardless of a rebuild's
    source/target ordering."""
    return "\t".join(sorted((str(e.get("source")), str(e.get("target")))))


def dump_verdicts(g: dict) -> int:
    """Write every adjudicated APPEARS_WITH edge in `g` to the side-car (non-destructive, atomic).
    Returns the number of verdicts persisted."""
    out = []
    for e in g.get("edges", []):
        if e.get("kind") == "APPEARS_WITH" and e.get("verified") is not None:
            out.append({"source": e.get("source"), "target": e.get("target"),
                        **{k: e.get(k) for k in _VERDICT_FIELDS}})
    backup(VERDICTS, "connverify")
    atomic_write_text(VERDICTS, json.dumps({"verdicts": out}, ensure_ascii=False))
    return len(out)


def backfill(src=None) -> None:
    """One-time, FREE: harvest verdicts already baked into a graph JSON into the side-car, so the paid
    adjudication is recoverable without re-running the model. Reads GRAPH by default, or `src` (e.g. a
    data/.backups/ image, or graph.json.preenrich) when the live graph has already been rebuilt/wiped.
    Does not modify graph.json."""
    src = Path(src) if src else GRAPH
    g = json.loads(src.read_text())
    n = dump_verdicts(g)
    print(f"backfilled {n} connection verdicts from {src.name} -> {VERDICTS.name} (no model calls; graph.json untouched)")


def patch_verdicts(g: dict) -> int:
    """Patch `g`'s APPEARS_WITH edges from the side-car IN PLACE (no file I/O on the graph). Returns the
    number of edges patched. Shared by --reapply and the reapply_layers DAG stage. Pairs not present in
    the current graph are simply skipped (e.g. after an entity rebuild changed some ids)."""
    if not VERDICTS.exists():
        return 0
    by_pair = {_pair(v): v for v in json.loads(VERDICTS.read_text()).get("verdicts", [])}
    n = 0
    for e in g.get("edges", []):
        if e.get("kind") != "APPEARS_WITH":
            continue
        v = by_pair.get(_pair(e))
        if v:
            for k in _VERDICT_FIELDS:
                e[k] = v.get(k)
            n += 1
    return n


def reapply() -> None:
    """FREE: re-apply the side-car's verdicts onto graph.json's APPEARS_WITH edges (e.g. after a
    build_graph rebuild). No model calls. Backs up graph.json first, writes atomically."""
    if not VERDICTS.exists():
        print(f"no {VERDICTS.name} to re-apply (run --backfill or a real pass first)"); return
    g = json.loads(GRAPH.read_text())
    n = patch_verdicts(g)
    backup(GRAPH, "reapply")
    atomic_write_text(GRAPH, json.dumps(g, ensure_ascii=False))
    print(f"re-applied {n} verdicts onto {GRAPH.name} (no model calls)")


def _text_map() -> dict:
    m = {}
    for fn in ("documents.json", "notebooks.json"):
        for rec in json.loads((STEP6 / fn).read_text()):
            t = (rec.get("text_by_label") or {}).get("src_content")
            if rec.get("id") and t:
                m[rec["id"]] = t
    return m


def adjudicate(edge: dict, labels: dict, texts: dict, model: str) -> dict:
    a, b = labels.get(edge["source"], edge["source"]), labels.get(edge["target"], edge["target"])
    recs = [r.split(":", 1)[-1] for r in (edge.get("shared_records") or [])][:MAX_RECORDS]
    snippets = []
    for rid in recs:
        t = texts.get(rid)
        if t:
            snippets.append(f"[{rid}] {t[:EXCERPT]}")
    evidence_text = "\n\n".join(snippets) or "(shared-document text unavailable)"
    prompt = (
        f"Two named entities co-occur in the same archival document(s) of Father Solanus Casey's papers:\n"
        f"  A = «{a}»\n  B = «{b}»\n\nSHARED DOCUMENT TEXT:\n{evidence_text}\n\n"
        "Decide: is there a REAL relationship or connection between A and B (they interact, are related, "
        "are addressed/written together, one acts on or refers to the other, etc.), OR do they merely appear "
        "SEPARATELY on the same page with no stated link (e.g., two unrelated names in a Seraphic Mass "
        "Association enrollment list or a list of distinct favors)? Be strict: same-page listing is NOT a "
        "connection. Return JSON: {\"connected\": bool, \"relationship\": \"short phrase or 'co-listed'\", "
        "\"confidence\": 0.0-1.0, \"evidence\": [\"short quote\"], \"reasoning\": \"one sentence\"}")
    try:
        txt, _ = llm.generate(prompt, model=model, json_mode=True, temperature=0.1)
        obj = json.loads(re.sub(r"^```(json)?|```$", "", txt.strip(), flags=re.I | re.M).strip())
    except Exception as e:
        obj = {"connected": None, "relationship": "", "confidence": 0.0, "evidence": [],
               "reasoning": f"error: {str(e)[:60]}"}
    return obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--min-weight", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only-missing", action="store_true",
                    help="adjudicate ONLY APPEARS_WITH edges that lack a verdict — incremental top-up after "
                         "a rebuild+reapply (doesn't re-pay for pairs already restored from the side-car)")
    a = ap.parse_args()
    g = json.loads(GRAPH.read_text())
    labels = {n["id"]: n.get("label") or n["id"] for n in g["nodes"]}
    texts = _text_map()
    targets = [e for e in g["edges"] if e.get("kind") == "APPEARS_WITH" and (e.get("weight") or 1) >= a.min_weight]
    if a.only_missing:
        targets = [e for e in targets if e.get("verified") is None]
    if a.limit:
        targets = targets[:a.limit]
    print(f"adjudicating {len(targets)} co-occurrence edges with {a.model} ...")
    real = coinc = 0
    done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = {ex.submit(adjudicate, e, labels, texts, a.model): e for e in targets}
        for fut in as_completed(futs):
            e = futs[fut]; r = fut.result()
            verdict = "real" if r.get("connected") else ("coincidental" if r.get("connected") is False else "unknown")
            e["verified"] = verdict
            e["relationship"] = r.get("relationship", "")
            e["verify_confidence"] = r.get("confidence")
            e["verify_evidence"] = r.get("evidence", [])[:2]
            if verdict == "real":
                real += 1
            elif verdict == "coincidental":
                coinc += 1
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(targets)} (real={real} coincidental={coinc})")
    backup(GRAPH, "connverify")
    atomic_write_text(GRAPH, json.dumps(g, ensure_ascii=False))
    n_side = dump_verdicts(g)          # mirror to the side-car so a rebuild never forces a paid re-run
    print(f"\nDONE. real={real}  coincidental={coinc}  unknown={len(targets)-real-coinc}")
    print(f"annotated -> {GRAPH.name} (timestamped backup in data/.backups/); {n_side} verdicts -> {VERDICTS.name}")


if __name__ == "__main__":
    if "--backfill" in sys.argv:
        src = None
        if "--from" in sys.argv:
            src = sys.argv[sys.argv.index("--from") + 1]
        backfill(src)
    elif "--reapply" in sys.argv:
        reapply()
    else:
        main()
