#!/usr/bin/env python3
"""Ink-IoU + Panoptic Quality evaluation — the RIGHT metric for document regions.

Why not area-IoU vs gold (what review_diff used): document boxes are Manhattan
layouts with lots of shared whitespace, and a box jittered a few px drops below an
area-IoU threshold and gets double-counted as a miss+spurious (the page_035 artifact
that inflated "errors" ~1.8x). Instead we match boxes by **shared INK** (the text the
box actually covers), which is robust to boundary padding/jitter, and we decompose
quality into:
    Recognition Quality (RQ) = F1 of matched regions  (did we find each region?)
    Segmentation Quality (SQ) = mean ink-IoU of matches (how tight, where it matters)
    Panoptic Quality   (PQ) = SQ x RQ
Class-agnostic PQ answers "are the regions right?"; the category-confusion table
answers "are the labels right?" — never collapsed into one number. (See MASTER_GUIDE §3.)

Usage:
  ./venv/bin/python panoptic_eval.py <volume> [--pred DIR] [--gold DIR] [--width 1000]
  ./venv/bin/python panoptic_eval.py Appendix_3 --pred auto_labeled --gold reviewed
  ./venv/bin/python panoptic_eval.py Appendix_3 --pred rerun_compare/old_auto_Appendix_3 --flat
"""
from __future__ import annotations
import argparse, json, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import auto_labeler as AL

SCRIPT = Path(__file__).resolve().parent
MATCH_INK_IOU = 0.5


def _boxes(p):
    """[(cat, (x0,y0,x1,y1))] in source-pixel coords."""
    data = json.load(open(p)); out=[]
    for doc in data.get("documents",{}).values():
        if isinstance(doc,dict):
            for cat,polys in doc.items():
                if isinstance(polys,list):
                    for b in polys:
                        vs=b.get("vertices") or []
                        if vs:
                            xs=[v["x"] for v in vs]; ys=[v["y"] for v in vs]
                            out.append((cat,(min(xs),min(ys),max(xs),max(ys))))
    return out, data.get("page_width"), data.get("page_height")


def _ink_integral(pdf, W):
    """Binarize page (Otsu) at width W; return (integral image, scale, w, h).
    integral[y][x] = # ink px in rect (0,0)-(x-1,y-1)."""
    img = AL.render_page(pdf, W)[0].convert("L")
    w,h = img.size; px=img.load()
    thr = AL._otsu_threshold(img.histogram()[:256])
    integ=[[0]*(w+1) for _ in range(h+1)]
    for y in range(h):
        row=integ[y+1]; prow=integ[y]; racc=0
        for x in range(w):
            racc += 1 if px[x,y]<thr else 0
            row[x+1]=prow[x+1]+racc
    return integ,w,h


def _ink(integ,w,h,box,sx,sy):
    x0=max(0,min(w,int(box[0]*sx))); x1=max(0,min(w,int(box[2]*sx)))
    y0=max(0,min(h,int(box[1]*sy))); y1=max(0,min(h,int(box[3]*sy)))
    if x1<=x0 or y1<=y0: return 0
    return integ[y1][x1]-integ[y0][x1]-integ[y1][x0]+integ[y0][x0]


def _inter(a,b):
    ix0=max(a[0],b[0]); iy0=max(a[1],b[1]); ix1=min(a[2],b[2]); iy1=min(a[3],b[3])
    return (ix0,iy0,ix1,iy1) if (ix1>ix0 and iy1>iy0) else None


def eval_volume(volume, pred_root, gold_root, width, flat):
    gold_dir = gold_root/volume
    pages=sorted(p for p in gold_dir.glob("page_*") if p.is_dir())
    TP=FP=FN=0; sq_sum=0.0
    confusion=Counter(); per_cat=defaultdict(lambda:[0,0,0])  # cat -> [tp,fp,fn]
    for pd in pages:
        name=pd.name
        gold_json=pd/f"{name}.json"
        pred_json=(pred_root/name/f"{name}.json") if flat else (pred_root/volume/name/f"{name}.json")
        pdf=AL.POLYGON_PDFS_DIR/volume/"pages"/f"{name}.pdf"
        if not (gold_json.exists() and pred_json.exists() and pdf.exists()): continue
        gboxes,_,_=_boxes(gold_json); pboxes,_,_=_boxes(pred_json)
        integ,w,h=_ink_integral(pdf,width)
        gw=AL.render_page(pdf,None)[3]; sx=w/gw; sy=sx   # source->render scale
        ink=lambda bx:_ink(integ,w,h,bx,sx,sy)
        # greedy match preds to gold by ink-IoU
        used=set()
        for pcat,pb in pboxes:
            best=None;best_iou=MATCH_INK_IOU
            for i,(gcat,gb) in enumerate(gboxes):
                if i in used: continue
                it=_inter(pb,gb)
                if not it: continue
                inter=ink(it); uni=ink(pb)+ink(gb)-inter
                iou=inter/uni if uni>0 else 0
                if iou>=best_iou: best,best_iou=i,iou
            if best is None:
                FP+=1; per_cat[pcat][1]+=1
            else:
                used.add(best); TP+=1; sq_sum+=best_iou
                gcat=gboxes[best][0]; per_cat[gcat][0]+=1
                if gcat!=pcat: confusion[f"{pcat} -> {gcat}"]+=1
        for i,(gcat,gb) in enumerate(gboxes):
            if i not in used: FN+=1; per_cat[gcat][2]+=1
    rq = TP/(TP+0.5*FP+0.5*FN) if (TP+FP+FN) else 0
    sq = sq_sum/TP if TP else 0
    return {"TP":TP,"FP":FP,"FN":FN,"RQ":rq,"SQ":sq,"PQ":sq*rq,
            "confusion":confusion,"per_cat":per_cat}


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("volume")
    ap.add_argument("--pred",default="auto_labeled"); ap.add_argument("--gold",default="reviewed")
    ap.add_argument("--width",type=int,default=1000)
    ap.add_argument("--flat",action="store_true",help="pred dir is <pred>/page_XXX/ (no volume level)")
    a=ap.parse_args()
    pred_root=Path(a.pred) if Path(a.pred).is_absolute() else SCRIPT/a.pred
    gold_root=Path(a.gold) if Path(a.gold).is_absolute() else SCRIPT/a.gold
    r=eval_volume(a.volume,pred_root,gold_root,a.width,a.flat)
    print(f"\n=== Panoptic eval (ink-IoU>={MATCH_INK_IOU}) — {a.volume} — pred={a.pred} ===")
    print(f"  TP={r['TP']}  FP(spurious)={r['FP']}  FN(missed)={r['FN']}")
    print(f"  RQ (recognition F1) = {r['RQ']:.3f}")
    print(f"  SQ (mean ink-IoU)   = {r['SQ']:.3f}")
    print(f"  PQ = SQ*RQ          = {r['PQ']:.3f}")
    if r["confusion"]:
        print("  category confusions (pred -> gold) on matched regions:")
        for k,n in r["confusion"].most_common(8): print(f"     {n:>3}  {k}")
    print("  per-category PQ-ish (cat: TP/FP/FN):")
    for cat,(tp,fp,fn) in sorted(r["per_cat"].items(), key=lambda kv:-(kv[1][1]+kv[1][2])):
        if tp+fp+fn>=3:
            f1=tp/(tp+0.5*fp+0.5*fn) if (tp+fp+fn) else 0
            print(f"     {cat:24} TP{tp:>3} FP{fp:>3} FN{fn:>3}  RQ={f1:.2f}")


if __name__=="__main__":
    main()
