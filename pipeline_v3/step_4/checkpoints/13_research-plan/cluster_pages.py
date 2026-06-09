#!/usr/bin/env python3
"""PROTOTYPE: content-agnostic page clustering (tests "cluster by layout, not text").

Extracts ONLY layout/ink-geometry signals (no OCR, no labels) from each gold page
image, z-scores them, and k-means clusters. Then checks how well the unsupervised
clusters line up with the gold-derived page types (purity / per-cluster makeup) —
i.e. "can visual structure alone identify page type?" Pure Python + PIL (no numpy).

Run: ./venv/bin/python rerun_compare/cluster_pages.py
"""
from __future__ import annotations
import json, math, random, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import auto_labeler as AL

REVIEW = HERE.parent / "reviewed"
PDFROOT = HERE.parent / "polygon_cropped_pdfs"
GRID = 6          # GRID x GRID ink-density signature
W = 384           # render width for feature extraction

def _find_pdf(vol, name):
    p = PDFROOT / vol / "pages" / f"{name}.pdf"
    return p if p.exists() else None

def features(pdf: Path) -> list[float]:
    """~44 content-agnostic layout features from the binarized page."""
    img = AL.render_page(pdf, W)[0].convert("L")
    Wd, Hd = img.size
    px = img.load()
    thr = AL._otsu_threshold(img.histogram()[:256])
    # binary ink map (1 = dark/ink)
    ink = [[1 if px[x, y] < thr else 0 for x in range(Wd)] for y in range(Hd)]
    total = sum(sum(r) for r in ink) or 1

    feats: list[float] = []
    # 1) GRID x GRID ink-density signature (WHERE the ink sits)
    for gy in range(GRID):
        for gx in range(GRID):
            y0, y1 = gy * Hd // GRID, (gy + 1) * Hd // GRID
            x0, x1 = gx * Wd // GRID, (gx + 1) * Wd // GRID
            c = sum(ink[y][x] for y in range(y0, y1) for x in range(x0, x1))
            feats.append(c / max(1, (y1 - y0) * (x1 - x0)))   # local ink fraction
    # 2) horizontal projection -> text-line structure
    rowsum = [sum(r) for r in ink]
    rowthr = 0.04 * Wd
    texty = [1 if s > rowthr else 0 for s in rowsum]
    transitions = sum(1 for i in range(1, Hd) if texty[i] != texty[i-1])
    n_lines = transitions / 2
    # largest empty vertical gap (sparse / multi-block signal)
    maxgap = cur = 0
    for t in texty:
        cur = cur + 1 if t == 0 else 0
        maxgap = max(maxgap, cur)
    feats += [sum(texty) / Hd, n_lines / 40.0, maxgap / Hd]
    # 3) vertical projection -> left margin column (notebook date/page-marker signal)
    colsum = [sum(ink[y][x] for y in range(Hd)) for x in range(Wd)]
    left = sum(colsum[: int(0.13 * Wd)])
    feats.append(left / total)                              # left-margin ink share
    band = colsum[int(0.13 * Wd): int(0.20 * Wd)]           # gap just right of margin
    feats.append(sum(1 for c in band if c < rowthr) / max(1, len(band)))
    # 4) ink center of mass + spread
    cmx = sum(colsum[x] * x for x in range(Wd)) / total / Wd
    cmy = sum(rowsum[y] * y for y in range(Hd)) / total / Hd
    feats += [cmx, cmy, total / (Wd * Hd)]                  # com_x, com_y, density
    return feats

def zscore(rows):
    n, d = len(rows), len(rows[0])
    mean = [sum(r[j] for r in rows) / n for j in range(d)]
    std = [math.sqrt(sum((r[j]-mean[j])**2 for r in rows)/n) or 1 for j in range(d)]
    return [[(r[j]-mean[j])/std[j] for j in range(d)] for r in rows]

def kmeans(rows, k, seed=0, iters=60, restarts=8):
    best = None
    for rs in range(restarts):
        rng = random.Random(seed*100+rs)
        cent = [rows[i][:] for i in rng.sample(range(len(rows)), k)]
        for _ in range(iters):
            asg = [min(range(k), key=lambda c: sum((rows[i][j]-cent[c][j])**2
                       for j in range(len(rows[0])))) for i in range(len(rows))]
            new = []
            for c in range(k):
                mem = [rows[i] for i in range(len(rows)) if asg[i] == c]
                new.append([sum(m[j] for m in mem)/len(mem) for j in range(len(rows[0]))]
                           if mem else cent[c])
            if new == cent: break
            cent = new
        inertia = sum(sum((rows[i][j]-cent[asg[i]][j])**2 for j in range(len(rows[0])))
                      for i in range(len(rows)))
        if best is None or inertia < best[0]:
            best = (inertia, asg)
    return best[1]

def main():
    items = []  # (key, vol, name, type)
    for vol in sorted(p.name for p in REVIEW.iterdir() if p.is_dir()):
        for pd in sorted((REVIEW/vol).glob("page_*")):
            pdf = _find_pdf(vol, pd.name)
            if not pdf: continue
            t = AL.infer_page_type_from_labels(json.load(open(pd/f"{pd.name}.json")))
            items.append((f"{vol}/{pd.name}", vol, pd.name, t, pdf))
    print(f"extracting layout features for {len(items)} gold pages (pure-python, ~1-2 min)...")
    X = []
    for i,(key,vol,name,t,pdf) in enumerate(items):
        X.append(features(pdf))
        if (i+1) % 20 == 0: print(f"  {i+1}/{len(items)}", flush=True)
    Z = zscore(X)
    types = [it[3] for it in items]
    from collections import Counter
    print("\nGOLD type distribution:", dict(Counter(types)))
    for k in (4, 5, 6, 8):
        asg = kmeans(Z, k, seed=42)
        # purity: each cluster -> its majority type
        pur = 0; lines=[]
        for c in range(k):
            mem = [types[i] for i in range(len(types)) if asg[i]==c]
            cc = Counter(mem); maj,_ = cc.most_common(1)[0]
            pur += cc[maj]
            lines.append(f"    cluster {c} (n={len(mem):2}): {dict(cc)}  -> {maj}")
        print(f"\n=== k={k}  PURITY={pur/len(types):.2%} (frac of pages in their cluster's majority type) ===")
        print("\n".join(lines))

if __name__ == "__main__":
    main()
