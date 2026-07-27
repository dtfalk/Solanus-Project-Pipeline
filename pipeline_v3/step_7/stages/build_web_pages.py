#!/usr/bin/env python3
"""Build transport-friendly WEB page images from the OCR-grade masked scans.

WHY: the deep-zoom citation viewer currently serves ``page_NNN.masked.png`` — the page rendered at
OCR resolution (~5313x6875 px, ~4.6 MB each, ~6.5 GB across the corpus). That resolution was only
ever needed for OCR *clarity*; for on-screen reading + citation highlighting a much smaller image is
indistinguishable and an order of magnitude lighter to ship/host.

WHAT: for every ``page_NNN.masked.png`` this writes, in the SAME folder:
  * ``page_NNN.web.webp``  — the scan downscaled so its long edge <= --maxdim, re-encoded as WebP.
  * ``page_NNN.web.json``  — {w, h, src_w, src_h, scale, bytes, src_bytes} so the server can serve
                             the small image AND rescale the polygon ``vertices`` onto it (the OCR
                             boxes are in source-pixel space; scale = web_w / src_w maps them down).

NON-DESTRUCTIVE + SAFE: derived from the *masked* png (so any redaction/masking is preserved — we
never re-expose the raw source), and the original .masked.png/.pdf are left untouched. Serving stays
backward-compatible: the server falls back to .masked.png wherever a .web.webp hasn't been built.

RESUMABLE: skips a page whose .web.webp + .web.json already exist and are newer than the source png
(unless --force). Parallel across pages (PIL's C encoders release the GIL).

    python stages/build_web_pages.py                 # all sections, default 2500px / q85
    python stages/build_web_pages.py --section Volume_3 --maxdim 2200 --workers 8
    python stages/build_web_pages.py --dry           # report the size win without writing
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # step_7/ on path for `config`
import config  # noqa: E402

from PIL import Image  # noqa: E402

SRC_SUFFIX = ".masked.png"
WEB_IMG = "{stem}.web.webp"
WEB_META = "{stem}.web.json"


def _iter_source_pngs(section: str | None):
    """Yield every page folder's masked.png under the enriched tree (optionally one section)."""
    root = config.ENRICHED
    sections = [root / section] if section else sorted(p for p in root.iterdir() if p.is_dir())
    for sec in sections:
        if not sec.is_dir():
            continue
        # buckets: 1_source_pages / 2_post_pages / 0_table_of_contents (mirrors the server's resolver)
        for png in sorted(sec.glob("*/page_*/*" + SRC_SUFFIX)):
            yield png


def _is_current(png: Path, web: Path, meta: Path) -> bool:
    """True if the web image + sidecar already cover this source png (newer than it)."""
    if not (web.exists() and meta.exists()):
        return False
    try:
        src_m = png.stat().st_mtime
        return web.stat().st_mtime >= src_m and meta.stat().st_mtime >= src_m
    except OSError:
        return False


def _convert(png: Path, maxdim: int, quality: int, force: bool, dry: bool) -> dict:
    """Downscale + re-encode ONE page. Returns a stats dict (also used for the run summary)."""
    stem = png.name[: -len(SRC_SUFFIX)]            # "page_079"
    folder = png.parent
    web = folder / WEB_IMG.format(stem=stem)
    meta = folder / WEB_META.format(stem=stem)
    src_bytes = png.stat().st_size

    if not force and _is_current(png, web, meta):
        m = json.loads(meta.read_text())
        return {"page": str(png), "skipped": True, "src_bytes": src_bytes,
                "web_bytes": m.get("bytes", web.stat().st_size if web.exists() else 0)}

    with Image.open(png) as im:
        im = im.convert("RGB")
        sw, sh = im.size
        long_edge = max(sw, sh)
        if long_edge > maxdim:
            f = maxdim / float(long_edge)
            nw, nh = max(1, round(sw * f)), max(1, round(sh * f))
            resized = im.resize((nw, nh), Image.LANCZOS)
        else:
            nw, nh = sw, sh                          # already small — just re-encode png -> webp
            resized = im
        scale = nw / float(sw)                       # uniform; == nh/sh (used to rescale OCR polygons)

        if dry:
            # estimate: encode to memory to measure bytes without writing to disk
            import io
            buf = io.BytesIO()
            resized.save(buf, format="WEBP", quality=quality, method=6)
            return {"page": str(png), "skipped": False, "dry": True,
                    "src_bytes": src_bytes, "web_bytes": buf.tell(),
                    "src_dim": (sw, sh), "web_dim": (nw, nh)}

        tmp = web.with_suffix(".webp.tmp")
        resized.save(tmp, format="WEBP", quality=quality, method=6)
        tmp.replace(web)                             # atomic swap into place

    web_bytes = web.stat().st_size
    meta.write_text(json.dumps({
        "w": nw, "h": nh, "src_w": sw, "src_h": sh,
        "scale": scale, "bytes": web_bytes, "src_bytes": src_bytes,
        "maxdim": maxdim, "quality": quality,
    }, indent=2))
    return {"page": str(png), "skipped": False, "src_bytes": src_bytes, "web_bytes": web_bytes}


def main():
    ap = argparse.ArgumentParser(description="Build transport-friendly WEB page images (webp) + polygon-scale sidecars.")
    ap.add_argument("--section", default=None, help="only this section, e.g. Volume_3 (default: all)")
    ap.add_argument("--maxdim", type=int, default=2500, help="max long-edge px for the web image (default 2500)")
    ap.add_argument("--quality", type=int, default=85, help="WebP quality 1-100 (default 85)")
    ap.add_argument("--workers", type=int, default=8, help="parallel workers (default 8)")
    ap.add_argument("--force", action="store_true", help="rebuild even if a current web image exists")
    ap.add_argument("--dry", action="store_true", help="measure the size win without writing anything")
    args = ap.parse_args()

    pngs = list(_iter_source_pngs(args.section))
    if not pngs:
        print(f"No {SRC_SUFFIX} pages found under {config.ENRICHED}"
              + (f" for section {args.section}" if args.section else ""))
        return 1

    print(f"[web-pages] {len(pngs)} source pages | maxdim={args.maxdim} q={args.quality} "
          f"workers={args.workers}{' DRY' if args.dry else ''}{' FORCE' if args.force else ''}")
    t0 = time.perf_counter()
    done = skipped = failed = 0
    src_total = web_total = 0
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_convert, p, args.maxdim, args.quality, args.force, args.dry): p for p in pngs}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                r = fut.result()
            except Exception as e:                   # keep going; report at the end
                failed += 1
                errors.append(f"{futs[fut]}: {type(e).__name__}: {e}")
                continue
            src_total += r.get("src_bytes", 0)
            web_total += r.get("web_bytes", 0)
            if r.get("skipped"):
                skipped += 1
            else:
                done += 1
            if i % 100 == 0 or i == len(pngs):
                print(f"  {i}/{len(pngs)} (built {done}, skipped {skipped}, failed {failed})")

    dt = time.perf_counter() - t0
    mb = 1024 * 1024
    ratio = (web_total / src_total) if src_total else 0
    print(f"[web-pages] {'DRY ' if args.dry else ''}done in {dt:.1f}s — built {done}, skipped {skipped}, failed {failed}")
    print(f"           source {src_total/mb:,.0f} MB -> web {web_total/mb:,.0f} MB "
          f"({ratio*100:.1f}% of original, {src_total/max(web_total,1):.1f}x smaller)")
    if errors:
        print(f"[web-pages] {len(errors)} error(s):")
        for e in errors[:20]:
            print("   " + e)
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
