#!/usr/bin/env python3
"""Read-only QA / diagnostic harness for auto-labeled pages.

Flags the recurring labeling failures (missing boxes, clipped boxes, oversize /
overlapping boxes, schema problems) so a full run can be measured and a human
review can be targeted instead of page-by-page blind. Output JSON has no text
field — each box is pure geometry — so every "missing text" issue is really a
coverage problem this script can detect from the ink alone.

Usage:
    python qa_report.py Appendix_1 [Volume_1 ...]   # one or more volumes
    python qa_report.py --all                        # every labeled volume
    python qa_report.py Appendix_1 --no-overlays     # skip overlay PNGs

Writes qa_output/<volume>/qa_report.json + qa_output/<volume>/overlays/page_NNN.png
and prints an aggregate summary. Never modifies auto_labeled/.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw

from auto_labeler import (
    ALLOWED_EDGE_PAIRS,
    AUTO_LABELED_DIR,
    SCRIPT_DIR,
    _bbox_of,
    _otsu_threshold,
    render_page,
)

QA_OUTPUT_DIR = SCRIPT_DIR / "qa_output"

# ── Tunables (scale-invariant fractions of page W/H unless noted) ───────────────
INK_MIN_FRAC      = 0.04    # a coarse cell counts as "ink" above this mean coverage
COVER_DILATE_FRAC = 0.004   # grow each box by this (frac of W) before subtracting -> ignores edge noise
GRID_CELL_FRAC    = 0.010   # coarse-grid cell size as frac of W (uncovered-ink clustering)
UNCOVERED_MIN_W   = 0.030   # an uncovered cluster must be at least this wide (frac of W) ...
UNCOVERED_MIN_H   = 0.012   # ... OR this tall (frac of H) to be flagged
UNCOVERED_MIN_INK = 0.00010 # ... and hold at least this many ink px (frac of W*H)
CLIP_BAND_FRAC    = 0.006   # width of the just-outside-edge band probed for clipped text (frac of W)
CLIP_OUT_THRESH   = 0.18    # outer-band ink coverage above this (and ink inside) -> clipped edge
CLIP_IN_THRESH    = 0.08    # require this much ink in the inner band (text actually reaches the edge)
OVERLAP_THRESH    = 0.50    # intersection / min(area) above this -> overlap flag. Stacked line-boxes
                            # (recipient over location, content over date, ...) naturally overlap ~40%
                            # vertically; only a genuine "swallow" (>50% of the smaller box) is a problem.
LOOSE_SLACK_FRAC  = 0.030   # blank slack between a box edge and its own ink, as frac of H -> loose box
DEGEN_MIN_PX      = 5       # box side smaller than this -> degenerate

SEVERITY = {
    "uncovered_ink":  "high",
    "clipped_edge":   "medium",
    "overlap":        "medium",
    "loose_box":      "low",
    "bad_edge":       "high",
    "degenerate_box": "high",
    "out_of_bounds":  "high",
    "empty_document": "low",
    "doc_count":      "low",
}

FLAG_COLOR = {
    "uncovered_ink":  (255, 0, 255),    # magenta
    "clipped_edge":   (255, 0, 0),      # red
    "overlap":        (255, 140, 0),    # orange
    "loose_box":      (255, 215, 0),    # gold
    "degenerate_box": (255, 0, 0),
    "out_of_bounds":  (255, 0, 0),
}


# ── Geometry helpers ────────────────────────────────────────────────────────────

def _iter_boxes(documents: dict):
    """Yield (doc_id, category, box_dict, bbox) for every polygon on the page."""
    for doc_id, doc in documents.items():
        if not isinstance(doc, dict):
            continue
        for cat, polys in doc.items():
            if not isinstance(polys, list):
                continue
            for box in polys:
                yield doc_id, cat, box, _bbox_of(box["vertices"])


def _inter_over_min(a, b) -> float:
    ix = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    area_a = (a[2] - a[0]) * (a[3] - a[1])
    area_b = (b[2] - b[0]) * (b[3] - b[1])
    m = min(area_a, area_b)
    return inter / m if m > 0 else 0.0


def _ink_mask(page_gray: Image.Image) -> Image.Image:
    """Global-Otsu binary ink mask (mode 'L', 255 = ink). Archival scans are
    high-contrast so a single page threshold matches snap's local Otsu closely."""
    thr = _otsu_threshold(page_gray.histogram())
    return page_gray.point(lambda p: 255 if p <= thr else 0)


def _strip_ink_frac(ink: Image.Image, box: tuple, W: int, H: int) -> float:
    """Mean ink coverage (0..1) inside a clamped pixel box of the ink mask."""
    x0 = max(0, min(W, box[0])); x1 = max(0, min(W, box[2]))
    y0 = max(0, min(H, box[1])); y1 = max(0, min(H, box[3]))
    if x1 - x0 < 1 or y1 - y0 < 1:
        return 0.0
    crop = ink.crop((x0, y0, x1, y1))
    return crop.histogram()[255] / ((x1 - x0) * (y1 - y0))   # ink mask is 0/255


# ── Checks ───────────────────────────────────────────────────────────────────────

def check_schema(documents: dict, num_documents, W: int, H: int) -> list[dict]:
    flags = []
    if isinstance(num_documents, int) and num_documents != len(documents):
        flags.append({"type": "doc_count",
                      "detail": f"num_documents={num_documents} but {len(documents)} doc keys"})
    ids = {}
    for doc_id, cat, box, bb in _iter_boxes(documents):
        ids[box.get("id")] = (doc_id, cat)
        w, h = bb[2] - bb[0], bb[3] - bb[1]
        if w < DEGEN_MIN_PX or h < DEGEN_MIN_PX:
            flags.append({"type": "degenerate_box", "category": cat, "bbox": bb,
                          "detail": f"{w}x{h}px"})
        if bb[0] < 0 or bb[1] < 0 or bb[2] > W or bb[3] > H:
            flags.append({"type": "out_of_bounds", "category": cat, "bbox": bb})
    for doc_id, doc in documents.items():
        if isinstance(doc, dict) and not any(
                isinstance(v, list) and v for v in doc.values()):
            flags.append({"type": "empty_document", "detail": doc_id})
    # connection edges: endpoints must exist and the category-pair must be allowed
    for doc_id, cat, box, bb in _iter_boxes(documents):
        for conn in box.get("connections", []):
            other_id = conn.get("id") if isinstance(conn, dict) else conn
            if other_id not in ids:
                flags.append({"type": "bad_edge", "category": cat, "bbox": bb,
                              "detail": f"edge to unknown id {other_id}"})
                continue
            pair = tuple(sorted((cat, ids[other_id][1])))
            if pair not in ALLOWED_EDGE_PAIRS:
                flags.append({"type": "bad_edge", "category": cat, "bbox": bb,
                              "detail": f"disallowed pair {pair}"})
    return flags


def check_overlap_loose(documents: dict, ink: Image.Image, W: int, H: int) -> list[dict]:
    flags = []
    boxes = [(doc_id, cat, bb) for doc_id, cat, _b, bb in _iter_boxes(documents)]
    heights = sorted(b[3] - b[1] for _d, _c, b in boxes)
    line_h = heights[len(heights) // 2] if heights else max(1, int(0.015 * H))
    for i in range(len(boxes)):
        di, ci, bi = boxes[i]
        for j in range(i + 1, len(boxes)):
            dj, cj, bj = boxes[j]
            if _inter_over_min(bi, bj) <= OVERLAP_THRESH:
                continue
            # Only flag a "real" overlap: the SHARED region must hold a full word of
            # ink (>= ~half a text line tall AND wide) that belongs to a DIFFERENT box
            # (different category or document) — i.e. cropping this box would capture
            # text that isn't its own. Whitespace and clipped-character (thin-band)
            # overlaps are benign (a text-extraction prompt weeds out half-words), and
            # a box's own sub-line inside its same-category same-doc neighbour is fine.
            if (di, ci) == (dj, cj):
                continue
            ix0, iy0 = int(max(bi[0], bj[0])), int(max(bi[1], bj[1]))
            ix1, iy1 = int(min(bi[2], bj[2])), int(min(bi[3], bj[3]))
            if ix1 <= ix0 or iy1 <= iy0:
                continue
            inkbb = ink.crop((ix0, iy0, ix1, iy1)).getbbox()
            if not inkbb:
                continue
            h_ink, w_ink = inkbb[3] - inkbb[1], inkbb[2] - inkbb[0]
            if h_ink >= 0.5 * line_h and w_ink >= 0.5 * line_h:
                flags.append({"type": "overlap", "category": f"{ci}+{cj}",
                              "bbox": [min(bi[0], bj[0]), min(bi[1], bj[1]),
                                       max(bi[2], bj[2]), max(bi[3], bj[3])],
                              "detail": f"~{h_ink / line_h:.1f}-line of {cj}/{ci} text in the shared region"})
    # loose: a box edge sits far from its own ink (large blank band inside the box)
    slack = int(round(LOOSE_SLACK_FRAC * H))
    for _d, cat, _box, bb in _iter_boxes(documents):
        sides = _blank_sides(ink, bb, slack, W, H)
        if sides:
            flags.append({"type": "loose_box", "category": cat, "bbox": bb,
                          "detail": "blank " + ",".join(sides)})
    return flags


def _blank_sides(ink: Image.Image, bb: tuple, slack: int, W: int, H: int) -> list[str]:
    """Return edges ('left'/'right'/'top'/'bottom') whose outer `slack` band inside
    the box is essentially blank — i.e. the box overshoots its own ink there."""
    out = []
    x0, y0, x1, y1 = bb
    if x1 - x0 < 3 * slack or y1 - y0 < 3 * slack:
        return out
    probes = {
        "left":   (x0, y0, x0 + slack, y1),
        "right":  (x1 - slack, y0, x1, y1),
        "top":    (x0, y0, x1, y0 + slack),
        "bottom": (x0, y1 - slack, x1, y1),
    }
    for side, band in probes.items():
        if _strip_ink_frac(ink, band, W, H) < 0.003:
            out.append(side)
    return out


def check_clipped(documents: dict, ink: Image.Image, W: int, H: int) -> list[dict]:
    """Flag box edges where text continues just outside the box (clipped)."""
    flags = []
    band = max(6, int(round(CLIP_BAND_FRAC * W)))
    others = [bb for _d, _c, _b, bb in _iter_boxes(documents)]
    for _d, cat, _box, bb in _iter_boxes(documents):
        x0, y0, x1, y1 = bb
        edges = {
            "left":   ((x0 - band, y0, x0, y1),       (x0, y0, x0 + band, y1)),
            "right":  ((x1, y0, x1 + band, y1),       (x1 - band, y0, x1, y1)),
            "top":    ((x0, y0 - band, x1, y0),       (x0, y0, x1, y0 + band)),
            "bottom": ((x0, y1, x1, y1 + band),       (x0, y1 - band, x1, y1)),
        }
        clipped = []
        for side, (outer, inner) in edges.items():
            # skip if a neighbouring box covers most of the outer band (that ink
            # is the neighbour's, not a clip of this box)
            if any(o is not bb and _inter_over_min(outer, o) > 0.5 for o in others):
                continue
            if (_strip_ink_frac(ink, outer, W, H) > CLIP_OUT_THRESH and
                    _strip_ink_frac(ink, inner, W, H) > CLIP_IN_THRESH):
                clipped.append(side)
        if clipped:
            flags.append({"type": "clipped_edge", "category": cat, "bbox": bb,
                          "detail": ",".join(clipped)})
    return flags


def _flood(grid: list[list[bool]], rows: int, cols: int):
    """4-connected components over a boolean coarse grid -> list of cell sets."""
    seen = [[False] * cols for _ in range(rows)]
    clusters = []
    for r in range(rows):
        for c in range(cols):
            if not grid[r][c] or seen[r][c]:
                continue
            stack = [(r, c)]
            seen[r][c] = True
            cells = []
            while stack:
                cr, cc = stack.pop()
                cells.append((cr, cc))
                for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                    nr, nc = cr + dr, cc + dc
                    if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] and not seen[nr][nc]:
                        seen[nr][nc] = True
                        stack.append((nr, nc))
            clusters.append(cells)
    return clusters


def check_uncovered_ink(documents: dict, ink: Image.Image, W: int, H: int) -> list[dict]:
    """Flag clusters of ink not enclosed by any box (likely missing boxes)."""
    # coverage mask: filled (slightly dilated) box rectangles
    cover = Image.new("L", (W, H), 0)
    draw = ImageDraw.Draw(cover)
    dil = int(round(COVER_DILATE_FRAC * W))
    for _d, _c, _b, bb in _iter_boxes(documents):
        draw.rectangle([bb[0] - dil, bb[1] - dil, bb[2] + dil, bb[3] + dil], fill=255)
    # uncovered ink = ink AND NOT cover, judged on a coarse grid (Image.BOX averages)
    cell = max(16, int(round(GRID_CELL_FRAC * W)))
    cols = max(1, W // cell); rows = max(1, H // cell)
    ink_small   = ink.resize((cols, rows), Image.BOX).load()
    cover_small = cover.resize((cols, rows), Image.BOX).load()
    cutoff = 255 * INK_MIN_FRAC
    grid = [[ink_small[c, r] >= cutoff and cover_small[c, r] < 128
             for c in range(cols)] for r in range(rows)]
    flags = []
    min_ink = UNCOVERED_MIN_INK * (W * H)
    for cells in _flood(grid, rows, cols):
        rs = [c[0] for c in cells]; cs = [c[1] for c in cells]
        gx0, gy0 = min(cs) * cell, min(rs) * cell
        gx1, gy1 = (max(cs) + 1) * cell, (max(rs) + 1) * cell
        bb = (gx0, gy0, min(W, gx1), min(H, gy1))
        # actual uncovered ink in the cluster's pixel bbox
        ink_px = (_strip_ink_frac(ink, bb, W, H)
                  * (bb[2] - bb[0]) * (bb[3] - bb[1]))
        wide = (bb[2] - bb[0]) >= UNCOVERED_MIN_W * W
        tall = (bb[3] - bb[1]) >= UNCOVERED_MIN_H * H
        if ink_px >= min_ink and (wide or tall):
            flags.append({"type": "uncovered_ink", "bbox": list(bb),
                          "detail": f"~{int(ink_px)} ink px, no box"})
    return flags


# ── Per-page driver ────────────────────────────────────────────────────────────

def qa_page(json_path: Path, pdf_path: Path, make_overlay: bool, overlay_dir: Path) -> dict:
    data = json.load(open(json_path))
    documents = data.get("documents", {})
    W = data["page_width"]; H = data["page_height"]

    img, rw, rh, _sw, _sh = render_page(pdf_path, None)   # full-res, matches JSON pixel space
    gray = img.convert("L")
    ink = _ink_mask(gray)

    flags = []
    flags += check_schema(documents, data.get("num_documents"), W, H)
    flags += check_uncovered_ink(documents, ink, W, H)
    flags += check_clipped(documents, ink, W, H)
    flags += check_overlap_loose(documents, ink, W, H)

    if make_overlay and flags:
        _draw_overlay(img, documents, flags, overlay_dir / f"{json_path.stem}.png")

    return {"page": data.get("page_number"), "json": str(json_path),
            "num_flags": len(flags), "flags": flags}


def _draw_overlay(img: Image.Image, documents: dict, flags: list[dict], out_path: Path,
                  overlay_width: int = 1600) -> None:
    W, H = img.size
    sf = overlay_width / W
    small = img.convert("RGB").resize((overlay_width, int(round(H * sf))), Image.LANCZOS)
    d = ImageDraw.Draw(small)

    def S(bb):
        return [bb[0] * sf, bb[1] * sf, bb[2] * sf, bb[3] * sf]

    for _doc_id, _cat, _box, bb in _iter_boxes(documents):
        d.rectangle(S(bb), outline=(60, 160, 60), width=1)
    for f in flags:
        col = FLAG_COLOR.get(f["type"], (0, 120, 255))
        bb = f.get("bbox")
        if bb:
            d.rectangle(S(bb), outline=col, width=3)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    small.save(out_path)


def qa_volume(volume: str, make_overlay: bool) -> dict:
    vol_dir = AUTO_LABELED_DIR / volume
    page_dirs = sorted(p for p in vol_dir.iterdir() if p.is_dir() and p.name.startswith("page_"))
    out_dir = QA_OUTPUT_DIR / volume
    overlay_dir = out_dir / "overlays"

    pages = []
    for pd in page_dirs:
        jp = pd / f"{pd.name}.json"
        pp = pd / f"{pd.name}.pdf"
        if not jp.exists() or not pp.exists():
            continue
        pages.append(qa_page(jp, pp, make_overlay, overlay_dir))

    pages.sort(key=lambda p: -p["num_flags"])
    agg = {}
    for pg in pages:
        for f in pg["flags"]:
            agg[f["type"]] = agg.get(f["type"], 0) + 1
    report = {"volume": volume, "pages_checked": len(pages),
              "aggregate": agg, "pages": pages}
    out_dir.mkdir(parents=True, exist_ok=True)
    json.dump(report, open(out_dir / "qa_report.json", "w"), indent=2)
    return report


def _print_summary(report: dict) -> None:
    vol = report["volume"]
    agg = report["aggregate"]
    total = sum(agg.values())
    print(f"\n=== {vol}: {report['pages_checked']} pages, {total} flags ===")
    for t in sorted(agg, key=lambda k: -agg[k]):
        print(f"  {SEVERITY.get(t, '?'):7} {t:16} {agg[t]}")
    flagged = [p for p in report["pages"] if p["num_flags"]]
    print(f"  pages with flags: {len(flagged)}/{report['pages_checked']}")
    for p in flagged[:15]:
        types = ",".join(sorted({f["type"] for f in p["flags"]}))
        print(f"    page {p['page']:>3}: {p['num_flags']:>2} flags  [{types}]")


def main() -> None:
    ap = argparse.ArgumentParser(description="QA diagnostic for auto-labeled pages.")
    ap.add_argument("volumes", nargs="*", help="Volume/Appendix names (e.g. Appendix_1).")
    ap.add_argument("--all", action="store_true", help="Check every labeled volume.")
    ap.add_argument("--no-overlays", action="store_true", help="Skip overlay PNGs.")
    args = ap.parse_args()

    if args.all:
        volumes = sorted(p.name for p in AUTO_LABELED_DIR.iterdir() if p.is_dir())
    else:
        volumes = args.volumes
    if not volumes:
        ap.error("give one or more volume names, or --all")

    for vol in volumes:
        report = qa_volume(vol, make_overlay=not args.no_overlays)
        _print_summary(report)
    print(f"\nwrote reports under {QA_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
