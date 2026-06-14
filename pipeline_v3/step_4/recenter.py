#!/usr/bin/env python3
"""Vertical RECENTER for single-line boxes (the fix for the model's systematic
'every box slid up by the same amount' bias).

Unlike reseat.py (which RESIZED a box to an ink-row cluster and could grab merged
rows -> giant boxes), this ONLY TRANSLATES a box vertically onto its text line and
keeps the box's exact width and height. A pure, bounded translation cannot create
huge boxes or overlaps it didn't already have — it just removes the constant
per-page vertical offset Gemini introduces.

For each single-line box: find the text LINES (ink-row clusters) within a tight
window around the box, in the box's own x-span; pick the line the box-center is
nearest to; shift the box so its center sits on that line center. Multi-line boxes
(taller than ~1.4x the page's single-line height) are left alone. Returns the count
moved. Geometry only; no model call.
"""
from __future__ import annotations
import numpy as np
from PIL import Image
import auto_labeler as AL

SINGLE_LINE = ("archv_commentary", "archv_date", "struct_doc", "struct_id",
               "struct_other", "src_recipient", "src_location_recipient",
               "src_location_sender", "src_date", "src_greeting", "src_signature",
               "archv_other")


def _line_centers(colmask, min_frac, bridge):
    """Centers of ink-row clusters: a row is ink if >min_frac of its width is ink;
    vertical gaps <= bridge merge."""
    rows = colmask.mean(axis=1) > min_frac
    out, i, n = [], 0, len(rows)
    while i < n:
        if not rows[i]:
            i += 1
            continue
        j = i
        while j < n:
            if rows[j]:
                j += 1
            else:
                k = j
                while k < n and not rows[k]:
                    k += 1
                if k < n and (k - j) <= bridge:
                    j = k
                else:
                    break
        out.append((i + j) / 2.0)
        i = j
    return out


def recenter_vertical(documents: dict, gray: Image.Image) -> int:
    W, H = gray.size
    arr = np.asarray(gray)
    thr = AL._otsu_threshold(gray.histogram()[:256])
    ink = arr < thr

    # collect single-line box refs + the page's typical single-line height
    refs, heights = [], []
    for doc in documents.values():
        if not isinstance(doc, dict):
            continue
        for cat, ps in doc.items():
            if cat not in SINGLE_LINE or not isinstance(ps, list):
                continue
            for p in ps:
                vs = p.get("vertices", [])
                if len(vs) >= 3:
                    xs = [v["x"] for v in vs]; ys = [v["y"] for v in vs]
                    bb = (min(xs), min(ys), max(xs), max(ys))
                    refs.append((p, bb)); heights.append(bb[3] - bb[1])
    if not refs:
        return 0
    typ_h = float(np.median(heights))

    moved = 0
    for p, (x0, y0, x1, y1) in refs:
        bh = y1 - y0
        if bh > 1.4 * typ_h:           # multi-line box (e.g. wrapped description) — leave it
            continue
        bc = (y0 + y1) / 2.0
        cx0, cx1 = max(0, int(x0)), min(W, int(x1))
        if cx1 - cx0 < 4:
            continue
        # search a tight window: enough to catch a ~half-line shift, not the next row
        win = int(round(0.9 * bh))
        wy0, wy1 = max(0, int(y0 - win)), min(H, int(y1 + win))
        centers = _line_centers(ink[wy0:wy1, cx0:cx1], 0.05, max(2, int(bh * 0.25)))
        if not centers:
            continue
        centers = [wy0 + c for c in centers]
        target = min(centers, key=lambda c: abs(c - bc))
        dy = target - bc
        if abs(dy) < 2 or abs(dy) > 0.7 * bh:   # already on it, or too far (would cross rows)
            continue
        ny0, ny1 = int(round(y0 + dy)), int(round(y1 + dy))
        p["vertices"] = [{"x": int(x0), "y": ny0}, {"x": int(x1), "y": ny0},
                         {"x": int(x1), "y": ny1}, {"x": int(x0), "y": ny1}]
        moved += 1
    return moved


if __name__ == "__main__":
    import json, sys
    from pathlib import Path
    raw = Path(sys.argv[1])
    pdf = raw.with_name(raw.name.replace(".raw.json", ".pdf"))
    d = json.loads(raw.read_text())
    g = AL.render_page(pdf, None)[0].convert("L")
    n = recenter_vertical(d["documents"], g)
    out = raw.with_name("recentered.json"); out.write_text(json.dumps(d, indent=2))
    print(f"recentered {n} boxes -> {out}")
