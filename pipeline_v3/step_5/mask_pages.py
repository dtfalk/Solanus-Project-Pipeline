#!/usr/bin/env python3
"""Mask each source page down to its labeled polygons.

For every page in 2_src_organized/<sec>/1_source_pages/page_*/:
  - render the page PDF to a raster at the label render_dpi (matches the polygon
    coordinate space exactly, via the same poppler path the editor uses),
  - white-out everything OUTSIDE the union of all labeled polygons (with a small
    pad so tight boxes don't clip handwriting),
  - write page_NNN.masked.pdf beside the source page_NNN.pdf / page_NNN.json.

The original page PDF + gold JSON are never touched; the mask is a new artifact.

Usage:
  ./venv/bin/python mask_pages.py --sample 8        # strided sample, report timing
  ./venv/bin/python mask_pages.py                   # all sections
  ./venv/bin/python mask_pages.py Volume_2 --workers 16 --pad 4
"""
from __future__ import annotations
import argparse, io, json, os, subprocess, sys, tempfile, time
from pathlib import Path
from multiprocessing import Pool

import numpy as np
from PIL import Image, ImageDraw, ImageFilter

Image.MAX_IMAGE_PIXELS = None  # these scans are ~36 MP; disable the decompression-bomb guard

ROOT = Path(__file__).resolve().parent / "2_src_organized"
# the source the gold labels were drawn on = the step_4 editor's SOURCE_DIR (auto_labeled/),
# NOT polygon_cropped_pdfs/ (a cropped variant missing the archival framing).
AUTO = Path(__file__).resolve().parent.parent / "step_4" / "auto_labeled"
SECTIONS = ["Volume_1", "Volume_2", "Volume_3", "Volume_4",
            "Appendix_1", "Appendix_2", "Appendix_3"]
SUBS = ["0_table_of_contents", "1_source_pages", "2_post_pages"]


def ensure_src_pdf(sec, pf, js):
    """Link the page PDF from auto_labeled/ — the source the gold labels were drawn on. ALWAYS
    re-links, so a stale/wrong source PDF (e.g. an old polygon_cropped one) gets corrected."""
    num = js.stem.split("_")[1]
    dst = pf / f"page_{num}.pdf"
    src = AUTO / sec / f"page_{num}" / f"page_{num}.pdf"
    if src.exists():
        if dst.exists():
            dst.unlink()
        try:
            os.link(src, dst)
        except OSError:
            import shutil; shutil.copy2(src, dst)
    return dst if dst.exists() else None


def is_done(pf, stem):
    """A page is complete only if BOTH masked artifacts exist and the png is real
    (guards against a page half-written when a prior run was killed)."""
    mpdf = pf / f"{stem}.masked.pdf"
    mpng = pf / f"{stem}.masked.png"
    return mpdf.exists() and mpng.exists() and mpng.stat().st_size > 10_000


def find_pages(sections, skip_done=True):
    pages = []
    for sec in sections:
        for sub in SUBS:
            base = ROOT / sec / sub
            if not base.is_dir():
                continue
            for pf in sorted(base.glob("page_*")):
                js = list(pf.glob("page_*.json"))
                if not js:
                    continue
                pdf = ensure_src_pdf(sec, pf, js[0])
                if not pdf:
                    continue
                if skip_done and is_done(pf, pdf.stem):
                    continue
                pages.append((sec, pf, js[0], pdf))
    return pages


def render(pdf: Path, dpi: int) -> Image.Image:
    """PDF page -> RGB image at dpi, via poppler (same renderer as the editor)."""
    fd, tmp = tempfile.mkstemp(suffix="", prefix="mask_")
    os.close(fd); os.unlink(tmp)
    subprocess.run(["pdftoppm", "-png", "-singlefile", "-r", str(dpi),
                    "-f", "1", "-l", "1", str(pdf), tmp],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    png = tmp + ".png"
    try:
        with Image.open(png) as im:
            return im.convert("RGB")
    finally:
        if os.path.exists(png):
            os.unlink(png)


def build_mask(W: int, H: int, documents: dict, sx: float, sy: float, pad: int) -> Image.Image:
    mask = Image.new("L", (W, H), 0)
    d = ImageDraw.Draw(mask)
    for doc in documents.values():
        for polys in (doc or {}).values():
            for p in polys or []:
                pts = [(v["x"] * sx, v["y"] * sy) for v in p.get("vertices", [])]
                if len(pts) >= 3:
                    d.polygon(pts, fill=255)
    if pad > 0:
        mask = mask.filter(ImageFilter.MaxFilter(2 * pad + 1))
    return mask


def mask_one(args):
    sec, pf, js, pdf, pad = args
    t0 = time.time()
    meta = json.loads(js.read_text())
    docs = meta.get("documents", {})
    npoly = sum(len(p or []) for doc in (docs or {}).values() for p in (doc or {}).values())
    if npoly == 0:                                # nothing labeled -> would mask to blank; skip
        return (sec, f"{sec}/{pf.parent.name}/{pf.name}", -1.0, 0, 0, 0)
    dpi = int(meta.get("render_dpi", 150))
    pw, ph = int(meta["page_width"]), int(meta["page_height"])
    img = render(pdf, dpi)
    W, H = img.size
    sx, sy = W / pw, H / ph                       # ~1.0; guards any poppler rounding
    mask = build_mask(W, H, docs, sx, sy, pad)
    arr = np.asarray(img)
    m = np.asarray(mask) > 0
    out = np.where(m[:, :, None], arr, 255).astype(np.uint8)
    pil = Image.fromarray(out)
    dst_pdf = pf / f"{pdf.stem}.masked.pdf"
    dst_png = pf / f"{pdf.stem}.masked.png"
    pil.save(dst_pdf, "PDF", resolution=float(dpi))
    pil.save(dst_png, "PNG", optimize=False)
    sz = dst_pdf.stat().st_size + dst_png.stat().st_size
    return (sec, pf.name, time.time() - t0, sz, int(m.sum()), W * H)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sections", nargs="*", default=None)
    ap.add_argument("--sample", type=int, default=0, help="process a strided sample of N pages")
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--pad", type=int, default=4, help="dilate mask by this many px")
    ap.add_argument("--force", action="store_true", help="re-mask pages even if already done")
    a = ap.parse_args()

    try:
        sys.stdout.reconfigure(line_buffering=True)   # stream the log live even when piped
    except Exception:
        pass

    secs = a.sections or SECTIONS
    pages = find_pages(secs, skip_done=not a.force)
    full_n = len(pages)
    if full_n == 0:
        print("nothing to do — all pages already masked (use --force to redo)"); return
    if a.sample and a.sample < len(pages):
        step = len(pages) / a.sample
        pages = [pages[int(i * step)] for i in range(a.sample)]
    tasks = [(s, pf, js, pdf, a.pad) for (s, pf, js, pdf) in pages]
    n = len(tasks)
    print(f"{n} page(s) (corpus total {full_n}), {a.workers} workers, pad={a.pad}")

    t0 = time.time()
    times, sizes, skipped = [], [], []
    with Pool(a.workers) as pool:
        for i, (sec, name, dt, sz, inside, total) in enumerate(pool.imap_unordered(mask_one, tasks), 1):
            if dt < 0:
                skipped.append(name); continue
            times.append(dt); sizes.append(sz)
            if a.sample or i % 100 == 0:
                print(f"  [{i}/{n}] {sec}/{name}  {dt:.2f}s  out={sz/1e6:.1f}MB  kept={100*inside/total:.0f}%")
    wall = time.time() - t0
    if times:
        avg = sum(times) / len(times)
        rate = len(times) / wall
        mb = sum(sizes) / len(sizes) / 1e6
        print(f"\ndone: {len(times)} masked, {len(skipped)} skipped (no labels), in {wall:.1f}s wall  "
              f"|  per-page avg {avg:.2f}s (cpu)  |  throughput {rate:.1f} pages/s")
        print(f"output: avg {mb:.1f}MB/page (pdf+png), total {sum(sizes)/1e9:.2f}GB for these")
        print(f"PROJECTION for all {full_n} remaining @ {a.workers} workers: "
              f"~{full_n/rate/60:.1f} min, ~{full_n*mb/1e3:.1f}GB output")
    else:
        print(f"\ndone: 0 masked, {len(skipped)} skipped (no labels), in {wall:.1f}s wall")
    if skipped:
        print(f"SKIPPED (zero labeled polygons — may need attention): {skipped}")


if __name__ == "__main__":
    main()
