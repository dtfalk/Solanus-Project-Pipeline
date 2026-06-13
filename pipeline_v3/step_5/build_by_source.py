#!/usr/bin/env python3
"""STEP_5 Folder A: by_source/ — a faithful per-source-page bundle (STEP_5_PLAN §2).

For every gold page in step_4/reviewed/, bundle: the page PDF (hardlinked from the
step_4 crops), the gold labels verbatim, and page_meta.json (dims, num_documents,
and the step_2 crop polygons when cleanly resolvable via frontmatter_map.json).
Deterministic, read-only w.r.t. step_4 (hardlinks/copies out), no API.

Run AFTER the front matter is reviewed into gold so title/ToC/appendix are included.

Usage:
  ./venv/bin/python step_5/build_by_source.py            # all volumes
  ./venv/bin/python step_5/build_by_source.py Volume_2   # one volume
"""
from __future__ import annotations
import json, os, sys
from pathlib import Path

STEP4 = Path(__file__).resolve().parent.parent / "pipeline_v3" / "step_4" \
    if (Path(__file__).resolve().parent.parent / "pipeline_v3").exists() \
    else Path(__file__).resolve().parent.parent / "step_4"
# step_5 sits beside step_4 under pipeline_v3
STEP4 = Path(__file__).resolve().parent.parent / "step_4"
REVIEW = STEP4 / "reviewed"
CROPS  = STEP4 / "polygon_cropped_pdfs"
QA     = STEP4 / "qa_output"
STEP2  = Path(__file__).resolve().parents[2] / "step_2" / "polygon_page_data"
OUT    = Path(__file__).resolve().parent / "by_source"


def source_to_cropidx(vol: str) -> dict[int, int]:
    """Invert frontmatter_map (cropidx->source) so we can find the step_2 polygon
    file (stored in docs_only/cropidx numbering) for a given source page. Empty
    for volumes that were never renumbered (source == stored step_2 number)."""
    fm = QA / vol / "frontmatter_map.json"
    if not fm.exists():
        return {}
    m = json.loads(fm.read_text())["cropidx_to_source"]
    return {int(src): int(ci) for ci, src in m.items()}


def crop_polygons(vol: str, source_page: int, inv: dict[int, int]):
    cidx = inv.get(source_page, source_page)        # fall back to identity
    for cand in (cidx, source_page):
        p = STEP2 / vol / "polygons" / f"page_{cand:03d}.json"
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                return None
    return None


def build_volume(vol: str) -> int:
    src_dir = REVIEW / vol
    if not src_dir.is_dir():
        print(f"  {vol}: no reviewed/ — skip"); return 0
    inv = source_to_cropidx(vol)
    out_vol = OUT / vol
    out_vol.mkdir(parents=True, exist_ok=True)
    pages = sorted(int(d.name.split("_")[1]) for d in src_dir.glob("page_*"))
    index = []
    for n in pages:
        name = f"page_{n:03d}"
        labels = json.loads((src_dir / name / f"{name}.json").read_text())
        pd = out_vol / name
        pd.mkdir(exist_ok=True)
        (pd / "labels.json").write_text(json.dumps(labels, indent=2))
        # page pdf: hardlink from crops (saves space), else copy
        crop_pdf = CROPS / vol / "pages" / f"{name}.pdf"
        dst_pdf = pd / f"{name}.pdf"
        if crop_pdf.exists() and not dst_pdf.exists():
            try:
                os.link(crop_pdf, dst_pdf)
            except OSError:
                import shutil; shutil.copy2(crop_pdf, dst_pdf)
        meta = {
            "volume": vol, "source_page": n,
            "width": labels.get("page_width"), "height": labels.get("page_height"),
            "render_dpi": labels.get("render_dpi"),
            "num_documents": labels.get("num_documents", 1),
            "has_pdf": dst_pdf.exists(),
            "crop_polygons": crop_polygons(vol, n, inv),
        }
        (pd / "page_meta.json").write_text(json.dumps(meta, indent=2))
        index.append({"page": name, "source_page": n,
                      "num_documents": meta["num_documents"],
                      "has_crop_polygons": meta["crop_polygons"] is not None})
    (out_vol / f"{vol}.index.json").write_text(json.dumps(
        {"volume": vol, "pages": len(index), "page_index": index}, indent=2))
    npoly = sum(1 for e in index if e["has_crop_polygons"])
    print(f"  {vol}: {len(index)} pages bundled ({npoly} with step_2 crop polygons)")
    return len(index)


def main():
    vols = sys.argv[1:] or sorted(d.name for d in REVIEW.iterdir() if d.is_dir())
    OUT.mkdir(parents=True, exist_ok=True)
    total = sum(build_volume(v) for v in vols)
    print(f"by_source: {total} pages across {len(vols)} volume(s) -> {OUT}")


if __name__ == "__main__":
    main()
