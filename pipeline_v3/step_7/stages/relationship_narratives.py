"""relationship_narratives.py — "Solanus and ___" micro-narratives (Feature #9).

For each significant correspondent, write a short, SOURCE-GROUNDED account of that person's relationship
with Father Solanus Casey — how it unfolds across the years — based ONLY on the dated letters/notebook
passages that tie them together. This is the prose overlay for the app's relationship view (/api/relationship);
the correspondence FACTS are always computed live, this just adds the written story on top.

Targets = everyone Solanus is recorded writing to (WROTE_TO) + the in-corpus Casey family, ranked by how
many letters name them. Parallel LLM (Gemini), cost-capped, resumable (skips people already written).

NON-DESTRUCTIVE: writes a NEW side-car, data/relationship_narratives.json, keyed by person id
  {person_id: {title, label, narrative, n_letters, n_records, years:[...], model}}
An existing artifact is backed up to data/.backups/ before being overwritten.

    venv/bin/python stages/relationship_narratives.py --limit 60 --max-cost 10
    venv/bin/python stages/relationship_narratives.py --min-records 3        # everyone with >=3 tying docs
"""
from __future__ import annotations
import argparse
import json
import re
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                                    # noqa: E402
from lib import costlog                          # noqa: E402
from lib.providers import llm                    # noqa: E402

GRAPH = config.DATA / "graph.json"
OUT = config.DATA / "relationship_narratives.json"
FAMILY = config.DATA / "solanus_family.json"
SOLANUS_ID = "person:solanus_capuchin:0006"
_TITLES = re.compile(r"\b(mr|mrs|miss|ms|rev|fr|father|sr|sister|br|brother|mother|very|rt|reverend|"
                     r"monsignor|mgr|msgr|dr|st|saint|o\.?f\.?m|cap|s\.?j)\b\.?", re.I)


def _norm(s: str) -> str:
    s = _TITLES.sub(" ", (s or "").lower())
    s = re.sub(r"[^a-z\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tying_records(pid, edges_by_node, byid):
    """The letter / notebook_entry nodes that name this person, sorted by date (earliest first)."""
    recs, seen = [], set()
    for e in edges_by_node.get(pid, []):
        if (e.get("kind") or "").upper() != "MENTIONED_IN":
            continue
        rid = e["target"] if e.get("source") == pid else e.get("source")
        if not rid or rid in seen or not str(rid).startswith(("letter:", "notebook_entry:")):
            continue
        seen.add(rid)
        n = byid.get(rid, {})
        recs.append({"kind": n.get("kind"), "date": n.get("date_raw"),
                     "edtf": n.get("edtf") or "", "text": (n.get("text") or "").strip()})
    recs.sort(key=lambda r: r["edtf"] or "zzzz")
    return recs


def _pick_excerpts(recs, k=12, cap=520):
    """Up to k dated excerpts spanning the years (even sampling keeps the arc, not just the first months)."""
    good = [r for r in recs if r["text"]]
    if len(good) <= k:
        chosen = good
    else:
        step = len(good) / k
        chosen = [good[int(i * step)] for i in range(k)]
    return [{"when": (r["date"] or r["edtf"] or "undated"), "kind": r["kind"],
             "text": r["text"][:cap]} for r in chosen]


def _clean_narrative(txt: str) -> str:
    """Strip the light markdown the model sometimes adds (bold markers, a leading '# / **title**' heading)
    so the plain-text prose renders cleanly in the app."""
    txt = (txt or "").strip().replace("**", "").replace("__", "")
    lines = txt.split("\n")
    if lines:
        first = lines[0].strip().lstrip("#").strip()
        if _norm(first).startswith("solanus and") and len(first) < 90:
            txt = "\n".join(lines[1:]).strip()
    return txt.strip()


def write_narrative(pid, label, recs, model):
    excerpts = _pick_excerpts(recs)
    if not excerpts:
        return {"id": pid, "narrative": None, "skip": "no text"}
    n_let = sum(1 for r in recs if r["kind"] == "letter")
    years = sorted({r["edtf"][:4] for r in recs if r["edtf"][:4].isdigit()})
    passages = "\n\n".join(f"[{e['when']} · {e['kind']}] {e['text']}" for e in excerpts)
    prompt = (
        f"You are writing for a public archive of Father Solanus Casey (Capuchin friar, 1870–1957, the "
        f"Detroit monastery porter; declared Blessed in 2017).\n\n"
        f"Write a SHORT micro-narrative titled \"Solanus and {label}\" — the story of this person's "
        f"relationship with Father Solanus as it appears across the years. Base it STRICTLY on the dated "
        f"passages below (his letters and notebook entries). Two short paragraphs, ~110 words total. Trace "
        f"how the relationship unfolds over time; name specific years and concrete details that actually "
        f"appear. Warm, factual, archival tone. NEVER invent facts, dates, miracles, or outcomes not in the "
        f"passages; if little is attested, say concisely what IS known. No citation markers, no headings, "
        f"just clean prose.\n\nDATED PASSAGES:\n{passages}")
    try:
        txt, _ = llm.generate(prompt, model=model, temperature=0.35)
        return {"id": pid, "label": label, "title": f"Solanus and {label}",
                "narrative": _clean_narrative(txt), "n_letters": n_let,
                "n_records": len(recs), "years": [int(y) for y in years], "model": model}
    except Exception as e:
        return {"id": pid, "narrative": None, "error": str(e)[:100]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--limit", type=int, default=60, help="max people to write (0 = no cap)")
    ap.add_argument("--min-records", type=int, default=2, help="need at least this many tying documents")
    ap.add_argument("--max-cost", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=10)
    a = ap.parse_args()

    g = json.loads(GRAPH.read_text())
    byid = {n["id"]: n for n in g["nodes"]}
    edges_by_node = {}
    for e in g["edges"]:
        edges_by_node.setdefault(e["source"], []).append(e)
        edges_by_node.setdefault(e["target"], []).append(e)

    # candidate set: everyone Solanus wrote to + the in-corpus Casey family
    cands = set()
    for e in g["edges"]:
        if (e.get("kind") or "").upper() == "WROTE_TO" and e.get("source") == SOLANUS_ID:
            t = e.get("target")
            if t and byid.get(t, {}).get("kind") == "person":
                cands.add(t)
    if FAMILY.exists():
        name2id = {}
        for n in g["nodes"]:
            if n.get("kind") != "person":
                continue
            for nm in [n.get("label"), n.get("canonical_name"), *(n.get("variants") or [])]:
                k = _norm(nm)
                if k and len(k) >= 5:
                    name2id.setdefault(k, n["id"])
        for mem in json.loads(FAMILY.read_text()).get("members", []):
            for c in [mem.get("canonical_name"), mem.get("name"), *(mem.get("aliases") or [])]:
                k = _norm(c)
                if k and k in name2id:
                    cands.add(name2id[k]); break

    cands.discard(SOLANUS_ID)
    scored = []
    for pid in cands:
        recs = _tying_records(pid, edges_by_node, byid)
        if len(recs) >= a.min_records:
            scored.append((pid, recs))
    scored.sort(key=lambda x: -sum(1 for r in x[1] if r["kind"] == "letter") - len(x[1]) / 100.0)
    if a.limit:
        scored = scored[:a.limit]

    done = json.loads(OUT.read_text()) if OUT.exists() else {}
    todo = [(pid, recs) for pid, recs in scored if pid not in done]
    print(f"relationship_narratives: {len(scored)} correspondents, {len(todo)} to write "
          f"(resume skipped {len(scored) - len(todo)})")

    c0 = costlog.snapshot()
    n_done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(write_narrative, pid, byid[pid].get("label") or pid, recs, a.model)
                for pid, recs in todo]
        for fut in as_completed(futs):
            r = fut.result()
            if r.get("narrative"):
                done[r["id"]] = {k: r[k] for k in ("title", "label", "narrative", "n_letters",
                                                   "n_records", "years", "model")}
            n_done += 1
            run_usd = costlog.delta(c0, costlog.snapshot()).get("usd", 0.0)
            if n_done % 20 == 0:
                OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
                print(f"  {n_done}/{len(todo)} written; this run ${run_usd:.3f}")
            if a.max_cost and run_usd > a.max_cost:
                print(f"  cost cap ${a.max_cost} reached at {n_done} (resumable)"); break

    if OUT.exists():
        bkp = config.DATA / ".backups"
        bkp.mkdir(exist_ok=True)
        shutil.copy(OUT, bkp / f"relationship_narratives.{datetime.now():%Y%m%d_%H%M%S}.json")
    OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
    print(f"\nDONE. {len(done)} narratives -> {OUT.name}")


if __name__ == "__main__":
    main()
