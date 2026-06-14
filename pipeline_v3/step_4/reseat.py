#!/usr/bin/env python3
"""Vertical re-seat for single-line boxes that the model placed off their text line
(David: "shifted up", sometimes by more than one line). Geometry only.

For each single-line box: search a vertical window around it (clamped by the
nearest neighbour boxes above/below that share its x-span, so it can't cross into
another row's territory), find the text LINES (ink-row clusters) in that window,
and move the box onto the line it should bound — the line it most OVERLAPS, or if
it overlaps none (shifted clean off), the NEAREST line. Horizontal extent is left
as-is (the contents note already excludes the leader dots). In place; returns the
count moved.
"""
from __future__ import annotations
import numpy as np
from PIL import Image
import auto_labeler as AL

SINGLE_LINE = ("archv_commentary", "archv_date", "struct_doc", "struct_id",
               "struct_other", "src_recipient", "src_location_recipient",
               "src_location_sender", "src_date", "src_greeting", "src_signature",
               "archv_date", "archv_other")


def _lines(colmask, min_frac, bridge):
    """Row clusters (a,b) where a row counts as ink if >min_frac of width is ink;
    gaps <= bridge are merged."""
    rows = colmask.mean(axis=1) > min_frac
    out, i, n = [], 0, len(rows)
    while i < n:
        if not rows[i]:
            i += 1; continue
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
        out.append((i, j)); i = j
    return out


def reseat_vertical(documents: dict, gray: Image.Image) -> int:
    W, H = gray.size
    arr = np.asarray(gray)
    thr = AL._otsu_threshold(gray.histogram()[:256])
    ink = arr < thr
    refs = []
    for doc in documents.values():
        if not isinstance(doc, dict):
            continue
        for cat, ps in doc.items():
            if not isinstance(ps, list):
                continue
            for p in ps:
                vs = p.get("vertices", [])
                if len(vs) >= 3:
                    xs = [v["x"] for v in vs]; ys = [v["y"] for v in vs]
                    refs.append((p, cat, (min(xs), min(ys), max(xs), max(ys))))
    moved = 0
    for p, cat, (x0, y0, x1, y1) in refs:
        if cat not in SINGLE_LINE:
            continue
        bh = max(1, y1 - y0)
        # fence: nearest box above / below that overlaps this box's x-span
        above, below = 0, H
        for q, _c, (fx0, fy0, fx1, fy1) in refs:
            if q is p or fx1 <= x0 or fx0 >= x1:
                continue
            if fy1 <= y0:
                above = max(above, fy1)
            if fy0 >= y1:
                below = min(below, fy0)
        win0 = max(above, y0 - int(2.5 * bh)); win1 = min(below, y1 + int(2.5 * bh))
        if win1 - win0 < 4 or x1 - x0 < 4:
            continue
        col = ink[win0:win1, max(0, x0):min(W, x1)]
        lines = _lines(col, 0.05, max(2, int(bh * 0.25)))
        if not lines:
            continue
        blo, bhi = y0 - win0, y1 - win0
        bc = (blo + bhi) / 2
        def score(ln):
            a, b = ln
            ov = max(0, min(b, bhi) - max(a, blo))
            return (ov, -abs((a + b) / 2 - bc))
        a, b = max(lines, key=score)
        # GUARD: never grow a box much taller than it was — if the matched cluster
        # is >1.4x the original height, the line-clustering merged adjacent rows;
        # moving there would make a giant box, so leave this box alone.
        if (b - a) > 1.4 * bh:
            continue
        m = max(3, int((b - a) * 0.15))
        ny0 = max(0, win0 + a - m); ny1 = min(H, win0 + b + m)
        if abs(ny0 - y0) <= 3 and abs(ny1 - y1) <= 3:
            continue
        p["vertices"] = [{"x": x0, "y": ny0}, {"x": x1, "y": ny0},
                         {"x": x1, "y": ny1}, {"x": x0, "y": ny1}]
        moved += 1
    return moved


if __name__ == "__main__":
    import json, sys
    from pathlib import Path
    raw = Path(sys.argv[1])               # a *.raw.json
    pdf = raw.with_name(raw.name.replace(".raw.json", ".pdf"))
    d = json.loads(raw.read_text())
    g = AL.render_page(pdf, None)[0].convert("L")
    n = reseat_vertical(d["documents"], g)
    out = raw.with_name("reseated.json"); out.write_text(json.dumps(d, indent=2))
    print(f"reseated {n} boxes -> {out}")
