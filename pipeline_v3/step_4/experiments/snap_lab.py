#!/usr/bin/env python3
"""Snap-to-ink improvement lab — objective, gold-grounded.

Problem (measured by geom_diagnose): auto boxes are mostly OVER-TALL (top/bottom
over-coverage ~6.5%) and occasionally RIGHT-CLIP long lines (~0.8%, but painful).
Root cause of clipping: the production snap fits horizontal extent from a FULL-HEIGHT
column projection, which washes out a single long line. Research fix (RLSA / per-line
extent): take the horizontal extent PER TEXT ROW and let the longest line drive the
right edge.

snap_v2 reimplements snap with a per-line horizontal fit (numpy) and a tunable margin.
The harness perturbs each GOLD box two ways and measures recovery vs gold:
  • CLIP-RIGHT: pull the right edge in -> does snap recover the longest line?
  • OVER-TALL: push top+bottom out -> does snap tighten back to gold height?
Lower |residual| = closer to David's drawing convention. OLD = production snap.

Usage: ./venv/bin/python experiments/snap_lab.py --pages 90
"""
from __future__ import annotations
import argparse, json, random, sys
from collections import Counter
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import auto_labeler as AL
import panoptic_eval as PE

AUTO = HERE / "auto_labeled"; GOLD = HERE / "reviewed"
EXCLUDE = {("Volume_1", "page_148")}


def _binary(page_gray):
    thr = AL._otsu_threshold(page_gray.histogram()[:256])
    return (np.asarray(page_gray) < thr)            # H x W bool, True = ink


def _runs_1d(mask_row, gap):
    """Indices of ink runs in a boolean row, bridging gaps <= gap. -> list[(a,b)]."""
    idx = np.flatnonzero(mask_row)
    if idx.size == 0:
        return []
    splits = np.where(np.diff(idx) > gap)[0]
    starts = np.concatenate(([0], splits + 1))
    ends = np.concatenate((splits, [idx.size - 1]))
    return [(int(idx[s]), int(idx[e]) + 1) for s, e in zip(starts, ends)]


def snap_v2(vertices, page_gray, fences=(), *, margin_frac_h=0.45, margin_frac_v=0.20,
            h_gap_frac=0.016, v_gap_frac=0.009, min_ink_frac=0.035, fence_margin=6,
            text_margin_frac=0.008, skew_tol=0.12, min_run_frac=0.004,
            vmargin_scale=1.0, vgrow_cap=None):
    """Like production snap but the HORIZONTAL extent is per-line (longest line wins),
    and the vertical margin can be scaled down (vmargin_scale<1 tightens height)."""
    bin_full = _binary(page_gray)
    H, W = bin_full.shape
    xs = [v["x"] for v in vertices]; ys = [v["y"] for v in vertices]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    bw, bh = x1 - x0, y1 - y0
    if bw < 3 or bh < 3:
        return vertices
    if len(vertices) == 4:
        tdx = vertices[1]["x"] - vertices[0]["x"]; tdy = vertices[1]["y"] - vertices[0]["y"]
        if abs(tdx) > 1 and abs(tdy / tdx) > skew_tol:
            return vertices

    left_lim, right_lim, top_lim, bot_lim = 0, W, 0, H
    for fx0, fy0, fx1, fy1 in fences:
        if fy0 < y1 and fy1 > y0:
            if fx1 <= x0: left_lim = max(left_lim, fx1 + fence_margin)
            if fx0 >= x1: right_lim = min(right_lim, fx0 - fence_margin)
        if fx0 < x1 and fx1 > x0:
            if fy1 <= y0: top_lim = max(top_lim, fy1 + fence_margin)
            if fy0 >= y1: bot_lim = min(bot_lim, fy0 - fence_margin)

    mx, my = bw * margin_frac_h, bh * margin_frac_v
    sx0 = max(0, left_lim, int(round(x0 - mx))); sy0 = max(0, top_lim, int(round(y0 - my)))
    sx1 = min(W, right_lim, int(round(x1 + mx))); sy1 = min(H, bot_lim, int(round(y1 + my)))
    sx0, sy0 = int(max(0, min(sx0, x0))), int(max(0, min(sy0, y0)))
    sx1, sy1 = int(min(W, max(sx1, x1))), int(min(H, max(sy1, y1)))
    if sx1 - sx0 < 3 or sy1 - sy0 < 3:
        return vertices

    sub = bin_full[sy0:sy1, sx0:sx1]
    ch, cw = sub.shape
    if sub.mean() < 0.003:
        return vertices
    gap_c = max(4, int(round(W * h_gap_frac)))
    gap_r = max(4, int(round(H * v_gap_frac)))
    min_run = max(6, int(round(W * min_run_frac)))

    # ── vertical extent: row projection (same idea as production) ──
    cutoff = min_ink_frac
    rmask = sub.mean(axis=1) >= cutoff
    rruns = _runs_1d(rmask, gap_r)
    by0r, by1r = (y0 - sy0), (y1 - sy0)
    vr = [(a, b) for a, b in rruns if b > by0r and a < by1r]
    if not vr:
        return vertices
    fy0, fy1 = int(min(a for a, _ in vr)), int(max(b for _, b in vr))

    # ── horizontal extent: PER-LINE within the vertical band (the fix) ──
    band = sub[fy0:fy1]
    lefts, rights = [], []
    bx0r, bx1r = (x0 - sx0), (x1 - sx0)
    for r in range(band.shape[0]):
        runs = [(a, b) for a, b in _runs_1d(band[r], gap_c) if (b - a) >= min_run]
        # keep runs overlapping the box's own x-span (so we follow this block, not a neighbour)
        runs = [(a, b) for a, b in runs if b > bx0r and a < bx1r]
        if not runs:
            continue
        lefts.append(min(a for a, _ in runs)); rights.append(max(b for _, b in runs))
    if not lefts:
        return vertices
    fx0, fx1 = min(lefts), max(rights)

    ex0 = max(left_lim, sx0 + fx0); ex1 = min(right_lim, sx0 + fx1)
    ey0 = max(top_lim, sy0 + fy0);  ey1 = min(bot_lim, sy0 + fy1)
    hmar = max(6, int(round(text_margin_frac * H)))
    vmar = max(4, int(round(text_margin_frac * H * vmargin_scale)))
    nx0 = max(0, ex0 - hmar); nx1 = min(W, ex1 + hmar)
    ny0 = max(0, ey0 - vmar); ny1 = min(H, ey1 + vmar)
    # Vertical growth cap: forbid the box from extending more than vgrow_cap px beyond the
    # model's original top/bottom (prevents neighbour-line capture) — but NEVER inside the
    # box's own ink (text is never lost). vgrow_cap=None disables the cap.
    if vgrow_cap is not None:
        ny0 = int(max(ny0, y0 - vgrow_cap))      # hard cap: box may not start above model_top-cap
        ny1 = int(min(ny1, y1 + vgrow_cap))      # hard cap: box may not end below model_bot+cap
    nx0, ny0, nx1, ny1 = int(nx0), int(ny0), int(nx1), int(ny1)
    if nx1 - nx0 < 3 or ny1 - ny0 < 3:
        return vertices
    return [{"x": nx0, "y": ny0}, {"x": nx1, "y": ny0},
            {"x": nx1, "y": ny1}, {"x": nx0, "y": ny1}]


def _bb(v):
    xs = [p["x"] for p in v]; ys = [p["y"] for p in v]
    return min(xs), min(ys), max(xs), max(ys)


def _perturb(bb, kind):
    x0, y0, x1, y1 = bb; w, h = x1 - x0, y1 - y0
    if kind == "clip_right":                 # realistic: a few words of one long line stick out
        cut = min(0.18 * w, 140.0)
        return [{"x": x0, "y": y0}, {"x": x1 - cut, "y": y0},
                {"x": x1 - cut, "y": y1}, {"x": x0, "y": y1}]
    if kind == "over_tall":                  # realistic: box ~half a line too tall each side
        pad = min(0.12 * h, 70.0)
        return [{"x": x0, "y": y0 - pad}, {"x": x1, "y": y0 - pad},
                {"x": x1, "y": y1 + pad}, {"x": x0, "y": y1 + pad}]
    return [{"x": x0, "y": y0}, {"x": x1, "y": y0}, {"x": x1, "y": y1}, {"x": x0, "y": y1}]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=90)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    pages = []
    for vol in ("Appendix_1", "Appendix_2", "Appendix_3", "Volume_1"):
        for pd in sorted((GOLD / vol).glob("page_*")):
            if (vol, pd.name) not in EXCLUDE and (AUTO / vol / pd.name / f"{pd.name}.pdf").exists():
                pages.append((vol, pd.name))
    rng.shuffle(pages); pages = pages[:a.pages]

    # residuals: positive = still wrong. clip: gold.x1 - snapped.x1 (px). tall: snapped over-height (px).
    METHODS = {
        "OLD":       lambda v, f: AL.snap_polygon_to_ink(v, page_gray, f),
        "V2_v100":   lambda v, f: snap_v2(v, page_gray, f, margin_frac_h=0.45, vmargin_scale=1.00),
        "V2_v085":   lambda v, f: snap_v2(v, page_gray, f, margin_frac_h=0.45, vmargin_scale=0.85),
        "V2_v070":   lambda v, f: snap_v2(v, page_gray, f, margin_frac_h=0.45, vmargin_scale=0.70),
    }
    agg = {m: {"clip": [], "tall_over": [], "tall_under": []} for m in METHODS}
    for vol, name in pages:
        d = json.load(open(GOLD / vol / name / f"{name}.json"))
        pdf = AUTO / vol / name / f"{name}.pdf"
        page_gray = AL.render_page(pdf, None)[0].convert("L")
        boxes = []
        for doc in d["documents"].values():
            for cat, polys in doc.items():
                for p in polys:
                    if len(p.get("vertices", [])) >= 3:
                        boxes.append(_bb(p["vertices"]))
        for i, bb in enumerate(boxes):
            fences = [b for j, b in enumerate(boxes) if j != i]
            gx0, gy0, gx1, gy1 = bb
            pv_clip = _perturb(bb, "clip_right")
            pv_tall = _perturb(bb, "over_tall")
            for m, fn in METHODS.items():
                out = fn([dict(p) for p in pv_clip], fences)
                agg[m]["clip"].append(max(0.0, gx1 - _bb(out)[2]))      # unrecovered right clip
                out = fn([dict(p) for p in pv_tall], fences)
                oy0, oy1 = _bb(out)[1], _bb(out)[3]
                agg[m]["tall_over"].append(max(0.0, gy0 - oy0) + max(0.0, oy1 - gy1))   # taller than gold
                agg[m]["tall_under"].append(max(0.0, oy0 - gy0) + max(0.0, gy1 - oy1))  # shorter (clipped)

    n = len(agg["OLD"]["clip"])
    print(f"\n=== snap recovery on {n} gold boxes / {len(pages)} pages ===")
    print(f"{'method':10} clip_resid  clip>30%  tall_over  over>30%  tall_under  under>30%")
    for m in METHODS:
        c = np.array(agg[m]["clip"]); t = np.array(agg[m]["tall_over"]); u = np.array(agg[m]["tall_under"])
        print(f"{m:10} {c.mean():9.1f}  {100*(c>30).mean():7.1f}  {t.mean():8.1f}  {100*(t>30).mean():6.1f}"
              f"  {u.mean():9.1f}  {100*(u>30).mean():7.1f}")
    print("\nclip_resid  = px of clipped long line still outside box after snap (lower=better)")
    print("tall_over   = px box is TALLER than gold (over-coverage; lower=better)")
    print("tall_under  = px box is SHORTER than gold (new clipping risk; want ~0)")


if __name__ == "__main__":
    main()
