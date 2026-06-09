"""Bucket review_diff results into held-out vs leaked few-shot pages, for both
the preserved OLD run and the NEW run, vs the gold reviewed/ set."""
import collections
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from review_diff import diff_page, REVIEW_ROOT

HERE = Path(__file__).resolve().parent
VOL = "Appendix_1"
LEAK = {1, 4, 15, 19, 31, 35, 45, 47}
SOURCES = {
    "OLD": HERE / "old_auto_Appendix_1",
    "NEW": HERE.parent / "auto_labeled" / VOL,
}

for tag, src_root in SOURCES.items():
    agg = {"held": collections.Counter(), "leak": collections.Counter()}
    docc = {"held": [], "leak": []}
    for pd in sorted((REVIEW_ROOT / VOL).glob("page_*")):
        name = pd.name
        num = int(name.split("_")[1])
        src = src_root / name / f"{name}.json"
        rev = pd / f"{name}.json"
        if not src.exists():
            continue
        d = diff_page(src, rev)
        b = "leak" if num in LEAK else "held"
        agg[b]["recat"] += len(d["recat"])
        agg[b]["add"] += len(d["added"])
        agg[b]["remove"] += len(d["removed"])
        agg[b]["resize"] += len(d["resized"])
        if d["doc_count"]:
            docc[b].append((name, d["doc_count"]))
    print(f"\n===== {tag} vs gold =====")
    for b, n in (("held", 44), ("leak", 8)):
        a = agg[b]
        print(f"  {b:5} ({n:2}pp): recat={a['recat']:3}  add={a['add']:3}  "
              f"remove={a['remove']:3}  resize={a['resize']:3}  "
              f"doc-count-errs={len(docc[b])} {docc[b] if docc[b] else ''}")
