#!/usr/bin/env python3
"""Objective geometry diagnostic: measure box-fit errors of auto_labeled vs gold
using INK, not judgment. For each auto box matched to a gold box, quantify how much
gold-covered ink the auto box MISSES on each side (clip) and how much extra ink it
GRABS beyond gold (over-coverage). Right-heavy missed-ink = the "long line clipped"
problem; excess ink on a side = over-coverage / neighbor capture.

Renders from auto_labeled/<vol>/<page>/<page>.pdf (committed, matches both auto and
gold coordinate spaces — avoids any local polygon_cropped_pdfs vintage mismatch).

Usage: ./venv/bin/python experiments/geom_diagnose.py Volume_1 Appendix_2 [--width 1400]
"""
from __future__ import annotations
import argparse, json, sys
from collections import Counter, defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import auto_labeler as AL
import panoptic_eval as PE

AUTO = HERE / "auto_labeled"
GOLD = HERE / "reviewed"
EXCLUDE = {("Volume_1", "page_148")}            # editor weirdness — David said skip
MATCH_IOU = 0.30                                 # loose match so a clipped box still pairs to its gold
INK_STRIP_FRAC = 0.004                           # a side "misses/grabs" ink if strip ink > this*box_ink (min abs below)
MIN_GAP_PX = 8                                   # ignore sub-snap edge differences (source px)


import numpy as np


def _ink_integral_for(page_pdf, width):
    img = AL.render_page(page_pdf, width)[0].convert("L")
    w, h = img.size
    thr = AL._otsu_threshold(img.histogram()[:256])
    arr = (np.asarray(img) < thr).astype(np.int32)          # True where ink
    integ = np.zeros((h + 1, w + 1), dtype=np.int32)
    integ[1:, 1:] = arr.cumsum(0).cumsum(1)
    return integ, w, h


def _ink(integ, w, h, box, s):
    x0 = max(0, min(w, int(box[0] * s))); x1 = max(0, min(w, int(box[2] * s)))
    y0 = max(0, min(h, int(box[1] * s))); y1 = max(0, min(h, int(box[3] * s)))
    if x1 <= x0 or y1 <= y0:
        return 0
    return int(integ[y1, x1] - integ[y0, x1] - integ[y1, x0] + integ[y0, x0])


def diagnose(volume, width):
    gdir = GOLD / volume
    pages = sorted(p for p in gdir.glob("page_*") if p.is_dir())
    stat = Counter()
    side_clip = Counter()      # which side gold-ink is missed on
    side_over = Counter()      # which side auto grabs extra ink
    clip_examples = []         # (vol,page,cat,right_gap_px,strip_ink) worst right-clips
    per_cat_clip = defaultdict(lambda: [0, 0])   # cat -> [right_clipped_boxes, total_boxes]
    for pd in pages:
        name = pd.name
        if (volume, name) in EXCLUDE:
            continue
        gj = pd / f"{name}.json"
        aj = AUTO / volume / name / f"{name}.json"
        apdf = AUTO / volume / name / f"{name}.pdf"
        if not (aj.exists() and apdf.exists()):
            continue
        gboxes, gW, _ = PE._boxes(gj)
        aboxes, aW, _ = PE._boxes(aj)
        if not gboxes or not aboxes:
            continue
        integ, w, h = _ink_integral_for(apdf, width)
        srcw = AL.render_page(apdf, None)[3]
        s = w / srcw
        ink = lambda b: _ink(integ, w, h, b, s)
        # greedy match auto->gold by ink-IoU
        used = set()
        for acat, ab in aboxes:
            best, best_iou = None, MATCH_IOU
            for i, (gcat, gb) in enumerate(gboxes):
                if i in used:
                    continue
                it = PE._inter(ab, gb)
                if not it:
                    continue
                inter = ink(it); uni = ink(ab) + ink(gb) - inter
                iou = inter / uni if uni > 0 else 0
                if iou >= best_iou:
                    best, best_iou = i, iou
            if best is None:
                continue
            used.add(best)
            gcat, gb = gboxes[best]
            stat["matched"] += 1
            gx0, gy0, gx1, gy1 = gb
            ax0, ay0, ax1, ay1 = ab
            gink = max(1, ink(gb))
            per_cat_clip[gcat][1] += 1
            # MISSED ink (in gold box, outside auto box) by side
            sides_missed = {}
            if gx1 - ax1 > MIN_GAP_PX:   # gold extends right of auto -> auto clips right
                sides_missed["right"] = (ink((ax1, gy0, gx1, gy1)), gx1 - ax1)
            if ax0 - gx0 > MIN_GAP_PX:
                sides_missed["left"] = (ink((gx0, gy0, ax0, gy1)), ax0 - gx0)
            if ay0 - gy0 > MIN_GAP_PX:
                sides_missed["top"] = (ink((gx0, gy0, gx1, ay0)), ay0 - gy0)
            if gy1 - ay1 > MIN_GAP_PX:
                sides_missed["bottom"] = (ink((gx0, ay1, gx1, gy1)), gy1 - ay1)
            thr = max(40, INK_STRIP_FRAC * gink)
            for side, (sink, gap) in sides_missed.items():
                if sink > thr:
                    side_clip[side] += 1
                    if side == "right":
                        per_cat_clip[gcat][0] += 1
                        clip_examples.append((volume, name, gcat, gap, sink))
            # EXCESS ink (in auto box, outside gold box) by side -> over-coverage
            if ax1 - gx1 > MIN_GAP_PX and ink((gx1, ay0, ax1, ay1)) > thr: side_over["right"] += 1
            if gx0 - ax0 > MIN_GAP_PX and ink((ax0, ay0, gx0, ay1)) > thr: side_over["left"] += 1
            if gy0 - ay0 > MIN_GAP_PX and ink((ax0, ay0, ax1, gy0)) > thr: side_over["top"] += 1
            if ay1 - gy1 > MIN_GAP_PX and ink((ax0, gy1, ax1, ay1)) > thr: side_over["bottom"] += 1
    return stat, side_clip, side_over, clip_examples, per_cat_clip


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volumes", nargs="+")
    ap.add_argument("--width", type=int, default=1400)
    a = ap.parse_args()
    tot = Counter(); tclip = Counter(); tover = Counter(); examples = []
    pcc = defaultdict(lambda: [0, 0])
    for v in a.volumes:
        stat, sc, so, ex, pc = diagnose(v, a.width)
        tot.update(stat); tclip.update(sc); tover.update(so); examples += ex
        for k, (c, t) in pc.items():
            pcc[k][0] += c; pcc[k][1] += t
        print(f"\n=== {v} ===")
        print(f"  matched auto->gold boxes: {stat['matched']}")
        print(f"  MISSED gold-ink by side (auto too small): {dict(sc)}")
        print(f"  EXCESS auto-ink by side (auto too big):  {dict(so)}")
    m = max(1, tot["matched"])
    print(f"\n=== TOTAL ({m} matched boxes) ===")
    print(f"  CLIP (auto misses gold ink):  " +
          ", ".join(f"{k} {tclip[k]} ({100*tclip[k]/m:.1f}%)" for k in ("right","left","top","bottom")))
    print(f"  OVER (auto grabs extra ink):  " +
          ", ".join(f"{k} {tover[k]} ({100*tover[k]/m:.1f}%)" for k in ("right","left","top","bottom")))
    print("\n  RIGHT-CLIP by category (clipped/total, rate):")
    for cat, (c, t) in sorted(pcc.items(), key=lambda kv: -kv[1][0]):
        if c:
            print(f"     {cat:24} {c:4}/{t:<4} {100*c/max(1,t):5.1f}%")
    examples.sort(key=lambda e: -e[4])
    print("\n  Worst right-clips (vol/page cat gap_px strip_ink):")
    for v, p, c, gap, sink in examples[:15]:
        print(f"     {v}/{p} {c} gap={gap}px ink={sink}")


if __name__ == "__main__":
    main()
