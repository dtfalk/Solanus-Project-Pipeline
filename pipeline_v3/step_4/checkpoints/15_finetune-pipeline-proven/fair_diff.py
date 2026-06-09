#!/usr/bin/env python3
"""Fair OLD-vs-gold and NEW-vs-gold comparison under IDENTICAL pure-spatial matching.

The gold descended from the OLD run (shared box ids -> clean id-diff), but the NEW
run has fresh uuids (pure spatial). To compare apples-to-apples we strip ids from
BOTH model sets and the gold, so every pairing is decided by IoU for both — same
regime, honest delta. 'hard' = recat + add + remove + doc (cosmetic resizes excluded).
"""
import json, sys, uuid
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import review_diff as RD

GOLD = HERE.parent / "reviewed" / "Appendix_3"
OLD  = HERE / "old_auto_Appendix_3"          # preserved flash-lite + volume-fewshot
NEW  = HERE.parent / "auto_labeled" / "Appendix_3"  # 3.5-flash + page-type (current)

def _uniq_ids(path: Path) -> Path:
    """Write a temp copy with every box id randomized (no cross-set id can match)."""
    data = json.load(open(path))
    for doc in data.get("documents", {}).values():
        if isinstance(doc, dict):
            for polys in doc.values():
                if isinstance(polys, list):
                    for b in polys:
                        b["id"] = str(uuid.uuid4())
    out = Path("/tmp") / f"fd_{uuid.uuid4().hex}.json"
    json.dump(data, open(out, "w"))
    return out

def tally(model_root: Path):
    agg = {"recat":0,"add":0,"rem":0,"resize":0,"doc":0}
    for pd in sorted(GOLD.glob("page_*")):
        name = pd.name
        m = model_root / name / f"{name}.json"
        g = pd / f"{name}.json"
        if not (m.exists() and g.exists()):
            continue
        d = RD.diff_page(_uniq_ids(m), _uniq_ids(g))  # both id-randomized -> pure spatial
        agg["recat"] += len(d["recat"]); agg["add"] += len(d["added"])
        agg["rem"] += len(d["removed"]); agg["resize"] += len(d["resized"])
        agg["doc"] += 1 if d["doc_count"] else 0
    agg["hard"] = agg["recat"] + agg["add"] + agg["rem"] + agg["doc"]
    return agg

print(f"{'config':42} {'HARD':>5} {'recat':>6} {'add':>5} {'rem':>5} {'doc':>4} {'(resize)':>9}")
for label, root in [("OLD  flash-lite + same-volume few-shot", OLD),
                    ("NEW  gemini-3.5-flash + page-type few-shot", NEW)]:
    a = tally(root)
    print(f"{label:42} {a['hard']:>5} {a['recat']:>6} {a['add']:>5} {a['rem']:>5} {a['doc']:>4} {a['resize']:>9}")
