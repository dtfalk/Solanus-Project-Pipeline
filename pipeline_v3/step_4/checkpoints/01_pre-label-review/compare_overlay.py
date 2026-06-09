#!/usr/bin/env python3
"""Per-page visual comparison of two label sets over the same page image.

Draws GOLD (reviewed/) boxes in green and the NEW auto_labeled/ boxes in red on
the rendered page, so recall misses (lone green) and spurious boxes (lone red)
are visible at a glance. A banner top-left shows gold vs new num_documents and
whether the page is held-out or a leaked few-shot example.

Usage:
    ./venv/bin/python compare_overlay.py Appendix_1
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw

from auto_labeler import render_page, SCRIPT_DIR
from qa_report import _iter_boxes

GOLD_ROOT = SCRIPT_DIR / "reviewed"
NEW_ROOT  = SCRIPT_DIR / "auto_labeled"
OUT_ROOT  = SCRIPT_DIR / "rerun_compare" / "overlays"

# Appendix_1 pages that ARE in the few-shot pool minus EXCLUDED_EXAMPLES, i.e.
# pages fed as their own answer key on a rerun (leakage — discount them).
LEAKED = {"Appendix_1": {1, 4, 15, 19, 31, 35, 45, 47}}

GREEN = (40, 170, 40)
RED   = (220, 40, 40)


def _load(path):
    data = json.load(open(path, encoding="utf-8"))
    return data.get("documents", {}), data.get("num_documents")


def overlay_page(pdf_path, gold_json, new_json, out_path, leaked) -> bool:
    img, *_ = render_page(pdf_path, None)   # full-res, matches JSON pixel space
    W, _H = img.size
    width = 1600
    sf = width / W
    canvas = img.convert("RGB").resize((width, int(img.size[1] * sf)), Image.LANCZOS)
    d = ImageDraw.Draw(canvas)

    def S(bb):
        return [bb[0] * sf, bb[1] * sf, bb[2] * sf, bb[3] * sf]

    gdocs, gnd = _load(gold_json)
    ndocs, nnd = _load(new_json)
    for _id, _cat, _box, bb in _iter_boxes(gdocs):
        d.rectangle(S(bb), outline=GREEN, width=2)
    for _id, _cat, _box, bb in _iter_boxes(ndocs):
        d.rectangle(S(bb), outline=RED, width=1)

    mism = gnd != nnd
    tag = "LEAKED few-shot" if leaked else "held-out"
    banner = f"green=GOLD red=NEW | docs gold={gnd} new={nnd}{'  <<MISMATCH' if mism else ''} | {tag}"
    d.rectangle([0, 0, 9 * len(banner) + 10, 20], fill=(0, 0, 0))
    d.text((5, 5), banner, fill=(255, 90, 90) if mism else (130, 255, 130))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out_path)
    return mism


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("volume")
    args = ap.parse_args()
    vol = args.volume

    gold_dir = GOLD_ROOT / vol
    new_dir  = NEW_ROOT / vol
    leaked_pages = LEAKED.get(vol, set())

    pages = sorted(p for p in gold_dir.iterdir() if p.is_dir() and p.name.startswith("page_"))
    mismatches = []
    for pd in pages:
        name = pd.name
        num = int(name.split("_")[1])
        gold_json = pd / f"{name}.json"
        new_json  = new_dir / name / f"{name}.json"
        pdf       = new_dir / name / f"{name}.pdf"
        if not pdf.exists():
            pdf = pd / f"{name}.pdf"
        if not (gold_json.exists() and new_json.exists() and pdf.exists()):
            print(f"  skip {name} (missing file)")
            continue
        leaked = num in leaked_pages
        out = OUT_ROOT / vol / f"{name}{'_LEAK' if leaked else ''}.png"
        if overlay_page(pdf, gold_json, new_json, out, leaked):
            mismatches.append((name, leaked))

    print(f"\nwrote {len(pages)} overlays to {OUT_ROOT / vol}")
    print(f"doc-count mismatches: {len(mismatches)}")
    for name, leaked in mismatches:
        print(f"   {name} {'(leaked)' if leaked else '(HELD-OUT)'}")


if __name__ == "__main__":
    main()
