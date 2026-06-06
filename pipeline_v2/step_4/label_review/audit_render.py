#!/usr/bin/env python3
"""Render a gold page with category-colored, numbered boxes for visual audit.

Usage:
  ./venv/bin/python label_review/audit_render.py <Volume> <page_NNN> -o /tmp/out.png
  ./venv/bin/python label_review/audit_render.py <Volume> <page_NNN> --zoom X0 Y0 X1 Y1 -o /tmp/out.png
(--zoom takes label-space pixel coords; pads 60px. Prints a numbered box legend to stdout.)
"""
import argparse, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import auto_labeler as AL
from PIL import ImageDraw

PALETTE = [(255,0,0),(0,140,255),(0,170,0),(200,0,200),(255,140,0),(0,190,190),
           (120,80,255),(170,170,0),(255,0,120),(0,90,160),(90,160,0),(160,60,0)]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volume"); ap.add_argument("page")
    ap.add_argument("--zoom", nargs=4, type=float, default=None)
    ap.add_argument("--root", default="reviewed", help="label root dir (default reviewed)")
    ap.add_argument("--width", type=int, default=1400)
    ap.add_argument("-o", "--out", required=True)
    a = ap.parse_args()

    here = Path(__file__).resolve().parent.parent
    jp = here/a.root/a.volume/a.page/f"{a.page}.json"
    d = json.load(open(jp))
    pdf = AL.POLYGON_PDFS_DIR/a.volume/"pages"/f"{a.page}.pdf"
    img = AL.render_page(pdf, a.width)[0].convert("RGB")
    s = img.width / d["page_width"]
    dr = ImageDraw.Draw(img)
    colors, legend, n = {}, [], 0
    for doc_name in sorted(d["documents"]):
        for cat, polys in d["documents"][doc_name].items():
            if cat not in colors: colors[cat] = PALETTE[len(colors) % len(PALETTE)]
            for p in polys:
                n += 1
                xs = [v["x"] for v in p["vertices"]]; ys = [v["y"] for v in p["vertices"]]
                bbox = (min(xs), min(ys), max(xs), max(ys))
                pts = [(v["x"]*s, v["y"]*s) for v in p["vertices"]]
                dr.polygon(pts, outline=colors[cat], width=4)
                dr.text((min(xs)*s+4, min(ys)*s+4), str(n), fill=colors[cat])
                legend.append((n, doc_name, cat, [round(b) for b in bbox]))
    if a.zoom:
        x0, y0, x1, y1 = a.zoom
        pad = 60
        img = img.crop((max(0,int(x0*s)-pad), max(0,int(y0*s)-pad),
                        min(img.width,int(x1*s)+pad), min(img.height,int(y1*s)+pad)))
    img.save(a.out)
    print(f"saved {a.out}  ({len(legend)} boxes, page {d['page_width']}x{d['page_height']})")
    for n, doc, cat, bbox in legend:
        print(f"  #{n:<3} {doc:<12} {cat:<22} bbox={bbox}")

if __name__ == "__main__":
    main()
