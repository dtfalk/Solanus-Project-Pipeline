#!/usr/bin/env python3
"""Render labeled_examples pages with every box annotated by its CATEGORY, so a
human or agent can audit label CONSISTENCY across the corpus.

The label JSONs carry geometry only (no text), so you must see the box drawn on
the page to know what content got which label. Also writes label_review/manifest.json
(per page: categories present + counts, num_documents, edge count) to target review.

Usage:
    ./venv/bin/python labeled_overlay.py            # all volumes
    ./venv/bin/python labeled_overlay.py Volume_1   # one volume
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw

from auto_labeler import render_page, SCRIPT_DIR, _bbox_of

EX_DIR   = SCRIPT_DIR / "labeled_examples"
OUT_DIR  = SCRIPT_DIR / "label_review" / "overlays"
MANIFEST = SCRIPT_DIR / "label_review" / "manifest.json"

PALETTE = [
    (228, 26, 28), (55, 126, 184), (77, 175, 74), (152, 78, 163), (255, 127, 0),
    (166, 86, 40), (247, 129, 191), (120, 120, 120), (0, 160, 160), (100, 65, 165),
    (200, 160, 0), (0, 90, 200), (140, 20, 90), (90, 140, 20), (20, 90, 140),
    (210, 90, 40), (40, 140, 110), (140, 40, 210), (110, 110, 40), (40, 40, 140),
]

ABBR = {"src_": "s.", "struct_": "st.", "archv_": "a."}


def _abbr(cat: str) -> str:
    for k, v in ABBR.items():
        if cat.startswith(k):
            return v + cat[len(k):]
    return cat


def load_boxes(data):
    for _did, doc in data.get("documents", {}).items():
        if not isinstance(doc, dict):
            continue
        for cat, polys in doc.items():
            if not isinstance(polys, list):
                continue
            for b in polys:
                if b.get("vertices"):
                    yield cat, _bbox_of(b["vertices"]), b.get("connections") or []


def overlay(pdf, data, out_path, order):
    img, *_ = render_page(pdf, None)
    W, H = img.size
    width = 1600
    sf = width / W
    canvas = img.convert("RGB").resize((width, int(H * sf)), Image.LANCZOS)
    d = ImageDraw.Draw(canvas)

    def S(bb):
        return [bb[0] * sf, bb[1] * sf, bb[2] * sf, bb[3] * sf]

    for cat, bb, conns in load_boxes(data):
        col = PALETTE[order.index(cat) % len(PALETTE)] if cat in order else (255, 0, 255)
        x0, y0, x1, y1 = S(bb)
        d.rectangle([x0, y0, x1, y1], outline=col, width=2)
        label = _abbr(cat) + (f" *{len(conns)}" if conns else "")
        ty = max(0.0, y0 - 12)
        d.rectangle([x0, ty, x0 + 6 * len(label) + 4, ty + 11], fill=col)
        d.text((x0 + 2, ty), label, fill=(255, 255, 255))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volume", nargs="?")
    args = ap.parse_args()
    vols = ([args.volume] if args.volume
            else [p.name for p in sorted(EX_DIR.iterdir()) if p.is_dir()])

    cats, pages = set(), []
    for vol in vols:
        for pd in sorted((EX_DIR / vol).glob("page_*")):
            jp, pp = pd / f"{pd.name}.json", pd / f"{pd.name}.pdf"
            if jp.exists() and pp.exists():
                data = json.load(open(jp))
                for cat, _, _ in load_boxes(data):
                    cats.add(cat)
                pages.append((vol, pd.name, pp, jp, data))

    order = sorted(cats)
    manifest = {}
    for vol, name, pp, _jp, data in pages:
        overlay(pp, data, OUT_DIR / vol / f"{name}.png", order)
        catcount, edges = {}, 0
        for cat, _, conns in load_boxes(data):
            catcount[cat] = catcount.get(cat, 0) + 1
            edges += len(conns)
        manifest[f"{vol}/{name}"] = {
            "num_documents": data.get("num_documents"),
            "categories": catcount, "edges": edges,
            "overlay": str((OUT_DIR / vol / f"{name}.png").relative_to(SCRIPT_DIR)),
        }

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    json.dump({"category_order": order, "pages": manifest}, open(MANIFEST, "w"), indent=2)
    print(f"rendered {len(pages)} pages, {len(order)} categories -> {OUT_DIR}")
    print("categories:", order)


if __name__ == "__main__":
    main()
