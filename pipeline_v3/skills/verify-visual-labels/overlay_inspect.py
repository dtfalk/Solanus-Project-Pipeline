#!/usr/bin/env python3
"""Crop-zoom-overlay inspector for the verify-visual-labels skill.

Renders a labeled page as HIGH-ZOOM region crops with thick colour-coded boxes,
category names, a margin OUTSIDE each box (so bleed into neighbours is visible),
and connection lines — so box-level errors are actually perceptible (a full-page
render downscales them to sub-pixel; see SKILL.md). Read each /tmp/inspect_*.png.

Labels JSON: the step_4 schema — {"page_width","page_height","documents":{doc_k:{
category:[{"vertices":[{x,y}...],"connections":[{id|...}],"id"}]}}}. Works on
reviewed/auto/raw/variant JSONs alike.

Usage:
  overlay_inspect.py <page.pdf> <labels.json>                 # 8 stacked strips
  overlay_inspect.py <page.pdf> <labels.json> --tiles 12      # finer strips
  overlay_inspect.py <page.pdf> <labels.json> --box doc_1:archv_date:4
  overlay_inspect.py <page.pdf> <labels.json> --gold <gold.json>   # overlay a reference (dashed)
"""
from __future__ import annotations
import argparse, json, colorsys
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image, ImageDraw, ImageFont

DPI = 200            # render high; crops keep glyphs large when displayed
OUT = Path("/tmp")

PALETTE = {}
def color(cat):
    if cat not in PALETTE:
        h = (len(PALETTE) * 0.13) % 1.0
        r, g, b = colorsys.hsv_to_rgb(h, 0.85, 0.9)
        PALETTE[cat] = (int(r*255), int(g*255), int(b*255))
    return PALETTE[cat]

def boxes(data):
    out = []
    for dk, doc in data.get("documents", {}).items():
        if not isinstance(doc, dict): continue
        for cat, ps in doc.items():
            if not isinstance(ps, list): continue
            for i, p in enumerate(ps):
                vs = p.get("vertices", [])
                if len(vs) >= 3:
                    out.append((dk, cat, i, p, vs))
    return out

def draw(dr, vs, sx, sy, col, width, dash=False, label=None, font=None):
    pts = [(v["x"]*sx, v["y"]*sy) for v in vs]
    if dash:
        for j in range(len(pts)):
            a, b = pts[j], pts[(j+1) % len(pts)]
            n = max(1, int(((a[0]-b[0])**2+(a[1]-b[1])**2)**.5/14))
            for k in range(0, n, 2):
                dr.line([(a[0]+(b[0]-a[0])*k/n, a[1]+(b[1]-a[1])*k/n),
                         (a[0]+(b[0]-a[0])*(k+1)/n, a[1]+(b[1]-a[1])*(k+1)/n)], fill=col, width=width)
    else:
        dr.polygon(pts, outline=col, width=width)
    if label:
        dr.text((min(p[0] for p in pts), min(p[1] for p in pts)-13), label, fill=col, font=font)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf"); ap.add_argument("labels")
    ap.add_argument("--tiles", type=int, default=8)
    ap.add_argument("--box", default=None, help="doc:category:index — zoom one box + neighbours")
    ap.add_argument("--gold", default=None, help="reference JSON to overlay dashed")
    a = ap.parse_args()
    data = json.loads(Path(a.labels).read_text())
    img = convert_from_path(a.pdf, dpi=DPI)[0].convert("RGB")
    W, H = img.size
    sx, sy = W / data["page_width"], H / data["page_height"]
    try: font = ImageFont.truetype("DejaVuSansMono.ttf", 13)
    except Exception: font = ImageFont.load_default()

    base = img.copy(); dr = ImageDraw.Draw(base)
    cen = {}
    for dk, cat, i, p, vs in boxes(data):
        draw(dr, vs, sx, sy, color(cat), 3, label=f"{cat}", font=font)
        xs=[v["x"]*sx for v in vs]; ys=[v["y"]*sy for v in vs]
        cen[p.get("id")] = (sum(xs)/len(xs), sum(ys)/len(ys))
    for dk, cat, i, p, vs in boxes(data):
        for c in p.get("connections", []):
            t = cen.get(c.get("id") if isinstance(c, dict) else c)
            if t and p.get("id") in cen:
                dr.line([cen[p["id"]], t], fill=(255,0,255), width=1)
    if a.gold:
        g = json.loads(Path(a.gold).read_text())
        for dk, cat, i, p, vs in boxes(g):
            draw(dr, vs, sx, sy, (0,0,0), 2, dash=True)

    if a.box:
        dk, cat, idx = a.box.split(":"); idx = int(idx)
        vs = data["documents"][dk][cat][idx]["vertices"]
        xs=[v["x"]*sx for v in vs]; ys=[v["y"]*sy for v in vs]
        m = max(80, int((max(xs)-min(xs))*0.6))
        crop = base.crop((max(0,int(min(xs)-m)), max(0,int(min(ys)-m*2)),
                          min(W,int(max(xs)+m)), min(H,int(max(ys)+m*2))))
        out = OUT/"inspect_box.png"; crop.save(out); print(f"  {out}  ({a.box})")
        return

    n = a.tiles; step = H // n
    print(f"{Path(a.pdf).name}: {len(boxes(data))} boxes -> {n} zoom strips:")
    for t in range(n):
        y0 = max(0, t*step - 12); y1 = min(H, (t+1)*step + 12)
        out = OUT/f"inspect_{t:02d}.png"
        base.crop((0, y0, W, y1)).save(out)
        print(f"  {out}  (rows y {y0}-{y1} of {H})")

if __name__ == "__main__":
    main()
