#!/usr/bin/env python3
"""PROTOTYPE: gold-FREE error detection via model disagreement (Query-by-Committee).

Two independent labelers (flash-lite and 3.5-flash) labeled the same pages. Where
they DISAGREE (a box in one with no IoU match in the other) is a high-probability
error/ambiguity zone — detectable WITHOUT any gold. We then check, against the gold
we happen to have, whether disagreement predicts where the human actually corrected:
if so, a human can review the top-disagreement pages and catch most errors fast.

Run: ./venv/bin/python rerun_compare/disagreement.py
"""
from __future__ import annotations
import json, sys, uuid
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import review_diff as RD

OLD  = HERE / "old_auto_Appendix_3"                  # flash-lite (committee member A)
NEW  = HERE.parent / "auto_labeled" / "Appendix_3"   # 3.5-flash (committee member B)
GOLD = HERE.parent / "reviewed" / "Appendix_3"

def boxes(p):
    return RD._load_boxes(p)[0]

def unmatched(a, b):
    """# boxes in `a` with no IoU>=0.5 partner in `b` (one-directional)."""
    used=set(); n=0
    for x in a:
        best=-1; bi=None
        for i,y in enumerate(b):
            if i in used: continue
            v=RD._iou(x["bbox"],y["bbox"])
            if v>=0.5 and v>best: best,bi=v,i
        if bi is None: n+=1
        else: used.add(bi)
    return n

def hard_vs_gold(model_root, name):
    """id-stripped pure-spatial hard errors of `model_root` page vs gold."""
    def uniq(p):
        d=json.load(open(p))
        for doc in d.get("documents",{}).values():
            if isinstance(doc,dict):
                for ps in doc.values():
                    if isinstance(ps,list):
                        for bb in ps: bb["id"]=str(uuid.uuid4())
        o=Path("/tmp")/f"u{uuid.uuid4().hex}.json"; json.dump(d,open(o,"w")); return o
    m=model_root/name/f"{name}.json"; g=GOLD/name/f"{name}.json"
    if not (m.exists() and g.exists()): return None
    dd=RD.diff_page(uniq(m),uniq(g))
    return len(dd["recat"])+len(dd["added"])+len(dd["removed"])+(1 if dd["doc_count"] else 0)

rows=[]
for pd in sorted(GOLD.glob("page_*")):
    nm=pd.name
    o=OLD/nm/f"{nm}.json"; n=NEW/nm/f"{nm}.json"
    if not (o.exists() and n.exists()): continue
    A,B=boxes(o),boxes(n)
    disagree = unmatched(A,B)+unmatched(B,A)        # symmetric committee disagreement
    need_new = hard_vs_gold(NEW, nm)                # actual 3.5-flash errors vs gold
    rows.append((nm, disagree, len(A), len(B), need_new))

rows.sort(key=lambda r:-r[1])
print(f"{'page':9} {'DISAGREE':>8} {'#flash-lite':>11} {'#3.5flash':>9} {'true err(3.5 vs gold)':>21}")
for nm,dis,na,nb,need in rows:
    print(f"{nm:9} {dis:>8} {na:>11} {nb:>9} {('-' if need is None else need):>21}")

# Does disagreement predict where correction was needed? (rank agreement, top-half recall)
valid=[(d,need) for _,d,_,_,need in rows if need is not None]
n=len(valid)
by_dis=sorted(range(n), key=lambda i:-valid[i][0])
by_need=sorted(range(n), key=lambda i:-valid[i][1])
top=n//2
top_dis=set(by_dis[:top]); top_need=set(by_need[:top])
recall=len(top_dis & top_need)/len(top_need)
tot_need=sum(v[1] for v in valid)
need_in_top_dis=sum(valid[i][1] for i in by_dis[:top])
print(f"\nPages: {n} | total 3.5-flash hard errors vs gold: {tot_need}")
print(f"Reviewing the TOP-{top} DISAGREEMENT pages (gold-free triage) covers "
      f"{need_in_top_dis}/{tot_need} = {need_in_top_dis/tot_need:.0%} of the real errors, "
      f"and {recall:.0%} of the worst-{top} pages.")
print("Pages with ZERO disagreement (both models agree) — candidate auto-accept:",
      [nm for nm,d,_,_,_ in rows if d==0])
