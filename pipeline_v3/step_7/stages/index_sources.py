"""stages/index_sources.py — Tier-2/4 searchable sources (FTS5 + IIIF + text-layer PDFs).

This stage turns the corpus into the three artifacts a scholar (or a librarian's discovery
system) actually needs to *find and cite* a passage, all free and fully local:

  (1) **A full-text search index.** A SQLite **FTS5** database over the cleaned page OCR text, so a
      query like ``"inflammatory rheumatism"`` instantly returns the exact pages — and, for each
      hit, the volume + the page PDF you'd open to read it. SQLite ships inside Python's stdlib and
      FTS5 is compiled into nearly every modern build, so there is *nothing to install* and the
      index is a single portable file (``data/fts.sqlite``).

  (2) **IIIF Presentation 3.0 manifests + Web Annotations.** IIIF (the "International Image
      Interoperability Framework") is the museum/library standard that lets *any* deep-zoom viewer
      (Mirador, Universal Viewer, Clover) open our page images, and — crucially — lets us paint a
      box on the page for every labelled region we extracted. We already have the geometry: each
      record in ``documents.json`` / ``notebooks.json`` carries ``regions[].vertices`` (the pixel
      polygon David's gold labels drew). We turn each region into a **W3C Web Annotation** whose
      target is an ``#xywh=`` media-fragment on the page canvas — i.e. "this text lives in this
      rectangle of this page." That is exactly the provenance contract the RESEARCH_PLAN asks for:
      a citation that points back to ``doc_id + rid + page + vertices`` → an on-image region.

  (3) **Text-layer ("searchable") PDFs.** The scanned page PDFs are just images — you cannot
      select or Ctrl-F the text. We fix that by laying an *invisible* text layer on top, one word
      at a time, positioned over the word's OCR box (from Azure's per-word ``polygon``). The result
      looks identical but is now selectable, copy-pasteable, and findable by any PDF reader or
      Google. We do this with **reportlab** when it is installed; if it is not, we emit a ready-to-
      run **ocrmypdf** command file instead (and leave a clear TODO), so the recipe is never lost.

Design rules honored here (see STYLE_GUIDE.md + the stage template):
  - **Non-destructive.** We only ever *write new* artifacts under ``data/`` and ``searchable_pdfs/``;
    we never touch the step_6 source pages.
  - **Cost-logged.** None of this calls a paid model — it's all local string/geometry work — but we
    still record a ``$0.00`` row per artifact-kind through ``lib.costlog.log(...)`` so the cost
    ledger tells the *whole* story of what the pipeline did, paid or not.
  - **Idempotent.** Re-running with unchanged inputs reproduces byte-identical JSON/SQL.

    python stages/index_sources.py --limit 25      # smoke test on a few pages (fast, free)
    python stages/index_sources.py                 # full corpus (still free — all local)
"""
from __future__ import annotations

# ==================================================================
# Imports — grouped the way David groups them
# ==================================================================
# Core Python Imports
import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

# Local File Imports (the step_7 root must be importable so `config`/`lib` resolve, exactly like
# every other module in this package — see lib/chunks.py for the same dance).
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402  (paths + model registry)
from lib import costlog             # noqa: E402  (always record every artifact, even free ones)

# `logging` rather than bare print() so progress reads well when this runs inside the DAG.
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("index_sources")


# ==================================================================
# Shared helpers — find a page's folder, its image size, its word boxes
# ==================================================================
# The enriched tree is laid out as:
#     3_enriched/<section>/1_source_pages/page_<NNN>/
#         page_<NNN>.json            (page_width, page_height, render_dpi)
#         extract_merged.json        (cleaned/merged OCR regions -> our FTS text)
#         extract_azure.raw.json     (per-word polygons -> the PDF text layer)
#         page_<NNN>.masked.pdf      (the scanned page image, as a 1-page PDF)
#         page_<NNN>.masked.png      (the scanned page image, as a PNG)
# `pdf_page_number` (1-based over the whole volume PDF) is the stable key that ties a
# documents.json / notebooks.json record to its folder, so we resolve folders by that number.

# table-of-contents/source/post page subfolders we might have to look inside (source pages first —
# that's where the letters and notebook entries live; TOC/post pages rarely carry labelled text).
_PAGE_BUCKETS = ("1_source_pages", "2_post_pages", "0_table_of_contents")


def _page_folder(section: str, pdf_page: int) -> Path | None:
    """Resolve the on-disk folder for one page.

    Args:
        section:  e.g. "Volume_1" (the record's ``section``).
        pdf_page: the 1-based PDF page number over the whole volume.

    Returns:
        The ``page_<NNN>`` folder Path if it exists, else None (we skip pages we can't locate
        instead of crashing — the corpus is large and a few strays shouldn't fail the build).
    """
    name = f"page_{pdf_page:03d}"
    base = config.ENRICHED / section
    for bucket in _PAGE_BUCKETS:
        cand = base / bucket / name
        if cand.is_dir():
            return cand
    return None


def _read_json(path: Path) -> dict | None:
    """Tolerant JSON read — returns None if the file is missing or unparseable (we'd rather skip a
    bad page than abort indexing the other 9,000)."""
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _page_dims(folder: Path, pdf_page: int) -> tuple[int, int]:
    """The page image's pixel (width, height).

    We prefer the page JSON's recorded ``page_width``/``page_height`` (authoritative, written by the
    renderer at a known DPI). If that's somehow absent we fall back to reading the PNG header with
    Pillow, and finally to a sane default — IIIF + PDF coordinates both need *some* canvas size, and
    a slightly-wrong size is far better than no manifest at all.
    """
    pj = _read_json(folder / f"page_{pdf_page:03d}.json") or {}
    w, h = pj.get("page_width"), pj.get("page_height")
    if w and h:
        return int(w), int(h)
    # Fallback: ask the PNG itself. Pillow only reads the header for .size, so this is cheap.
    try:
        from PIL import Image
        with Image.open(folder / f"page_{pdf_page:03d}.masked.png") as im:
            return im.size
    except Exception:
        return 5313, 6875                              # the corpus's common 150-DPI letter size


def _bbox_from_vertices(vertices: list) -> tuple[int, int, int, int]:
    """Collapse a polygon (list of [x, y] points) to an axis-aligned (x, y, w, h) box.

    Our gold regions are quadrilaterals; IIIF media-fragments and a PDF text rectangle both want a
    plain rectangle, so we take the min/max envelope. (If we later want pixel-perfect rotated boxes
    we can switch the annotation target to an SVG selector — noted in the IIIF README we emit.)
    """
    xs = [p[0] for p in vertices]
    ys = [p[1] for p in vertices]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    return int(x0), int(y0), int(x1 - x0), int(y1 - y0)


# ==================================================================
# (1) FULL-TEXT SEARCH — a SQLite FTS5 index over cleaned page OCR text
# ==================================================================
# WHY FTS5: it gives us BM25 ranking, prefix/phrase queries, and snippet highlighting in a single
# stdlib-backed file with zero services to run. In the RESEARCH_PLAN this is the BM25 / lexical half
# of the eventual *hybrid* retriever (BM25 + dense, fused with RRF) — so building it now is also a
# down payment on Tier-0 retrieval, not just a discovery convenience.

def _page_ocr_text(folder: Path) -> str:
    """The cleaned OCR text for a page, assembled from ``extract_merged.json``.

    Each merged region carries a ``merged_text`` (the human-preferred reconciliation of the two OCR
    engines). We join them in file order with newlines — that's the "cleaned" page text the task
    asks us to index. If the merged file is missing we return "" and the caller skips the page.
    """
    em = _read_json(folder / "extract_merged.json")
    if not em:
        return ""
    parts = [r.get("merged_text", "").strip() for r in em.get("regions", [])]
    return "\n".join(p for p in parts if p).strip()


def build_fts(records: list, out_path: Path) -> int:
    """Build ``data/fts.sqlite`` mapping query -> page -> page PDF.

    Args:
        records: a flat list of page descriptors, each
                 ``{id, section, kind, pdf_page, page_label, notebook, folder}`` (built by run()).
        out_path: where to write the SQLite DB (overwritten fresh each run for reproducibility).

    Returns:
        The number of pages indexed.

    The schema is one **FTS5 virtual table** ``pages`` whose searchable column is ``text`` and whose
    *un-indexed* companion columns (``UNINDEXED``) carry the provenance we hand back on a hit:
    the doc id, the volume, the page numbers, and — the payload the task names explicitly — the
    relative path to the page PDF you'd open to read the result.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()                              # non-destructive to *sources*; we own this file

    con = sqlite3.connect(out_path)
    try:
        # Confirm FTS5 is compiled in *before* we promise it — most builds have it, but failing
        # loudly here beats a confusing error later.
        try:
            con.execute("CREATE VIRTUAL TABLE _fts5_probe USING fts5(x)")
            con.execute("DROP TABLE _fts5_probe")
        except sqlite3.OperationalError as e:
            raise RuntimeError(
                "This SQLite build lacks the FTS5 extension. Install a python with FTS5 (most do), "
                "or `pip install sqlite-utils` / use a system sqlite3 that has it."
            ) from e

        con.execute("""
            CREATE VIRTUAL TABLE pages USING fts5(
                text,                                   -- the only INDEXED (searchable) column
                doc_id      UNINDEXED,                  -- e.g. "Volume_1__p001"
                section     UNINDEXED,                  -- volume / appendix
                kind        UNINDEXED,                  -- "letter" | "notebook"
                pdf_page    UNINDEXED,                  -- 1-based page in the volume PDF
                page_label  UNINDEXED,                  -- "Page 178 Cont." etc. (notebooks)
                pdf_path    UNINDEXED,                  -- <- query result -> the page PDF to open
                tokenize = 'porter unicode61'           -- stemming + unicode folding for recall
            )
        """)
        n = 0
        for r in records:
            text = _page_ocr_text(r["folder"])
            if not text:
                continue
            # Relative to the repo root keeps the DB portable if the project is moved/zipped.
            try:
                pdf_rel = str((r["folder"] / f"page_{r['pdf_page']:03d}.masked.pdf").relative_to(config.REPO))
            except ValueError:
                pdf_rel = str(r["folder"] / f"page_{r['pdf_page']:03d}.masked.pdf")
            con.execute(
                "INSERT INTO pages (text, doc_id, section, kind, pdf_page, page_label, pdf_path) "
                "VALUES (?,?,?,?,?,?,?)",
                (text, r["id"], r["section"], r["kind"], r["pdf_page"], r.get("page_label", ""), pdf_rel),
            )
            n += 1
        con.commit()
        # A tiny convenience view so a curious human can `SELECT * FROM page_pdfs` without FTS syntax.
        con.execute("CREATE VIEW page_pdfs AS SELECT doc_id, section, pdf_page, pdf_path FROM pages")
        con.commit()
    finally:
        con.close()

    # Cost ledger: free, but recorded so the ledger reflects the whole pipeline.
    costlog.log(provider="local", model="sqlite-fts5", op="index", items=n, usd=0.0,
                meta=f"fts.sqlite pages={n}")
    log.info("  FTS5: indexed %d pages -> %s", n, out_path)
    return n


def fts_search(query: str, k: int = 10, db_path: Path | None = None) -> list:
    """Convenience query helper (also what an app/agent would call).

    Returns up to ``k`` hits as ``{doc_id, section, kind, pdf_page, page_label, pdf_path, snippet}``
    ranked by BM25, with the matched terms wrapped in «...» inside ``snippet`` for display.
    """
    db_path = db_path or (config.DATA / "fts.sqlite")
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT doc_id, section, kind, pdf_page, page_label, pdf_path, "
            "       snippet(pages, 0, '«', '»', ' … ', 12) "
            "FROM pages WHERE pages MATCH ? ORDER BY bm25(pages) LIMIT ?",
            (query, k),
        ).fetchall()
    finally:
        con.close()
    keys = ["doc_id", "section", "kind", "pdf_page", "page_label", "pdf_path", "snippet"]
    return [dict(zip(keys, row)) for row in rows]


# ==================================================================
# (2) IIIF Presentation 3.0 manifests + W3C Web Annotations for labelled regions
# ==================================================================
# The recipe (documented here AND in the README we emit so a future maintainer can follow it):
#
#   * One **Manifest** per volume  (iiif/<Volume>/manifest.json) — the "book".
#   * One **Canvas** per page       — a page-sized coordinate plane (width/height = image pixels).
#   * One **painting Annotation**   per canvas — paints the page image onto the canvas.
#   * One **AnnotationPage** of "supplementing" Web Annotations per canvas — one annotation per
#     labelled region, whose `body` is the OCR/gold text (a TextualBody) and whose `target` is the
#     canvas URI plus an `#xywh=x,y,w,h` media-fragment (the region's bounding box). That fragment
#     is the standard way IIIF viewers know *where on the page* to draw the highlight.
#
# We use a **file:// base IRI** by default (so the manifests open locally with no server). When the
# images are later published behind a IIIF Image API server, only `IIIF_BASE` needs changing — every
# id is derived from it — which is why we centralize it as a single knob.

# TODO(deploy): point IIIF_BASE at the public IIIF Image API endpoint once images are hosted, and
# switch each canvas's painting body to a `service` block (level0/level2 Image API) for deep zoom.
IIIF_BASE = "file://" + str((config.SEARCHABLE_PDFS / "iiif").resolve())


def _iiif_canvas(rec: dict, folder: Path, base: str) -> dict:
    """Build one IIIF v3 Canvas (+ its painting annotation + region annotations) for a page.

    Args:
        rec:    the documents.json / notebooks.json record for this page.
        folder: the page's on-disk folder (for the image + dimensions).
        base:   the volume's base IRI (everything hangs off this for stable, relocatable ids).

    Returns:
        A Presentation-3.0 Canvas dict.
    """
    section, pdf_page = rec["section"], rec["pdf_page_number"]
    w, h = _page_dims(folder, pdf_page)
    canvas_id = f"{base}/canvas/p{pdf_page}"
    png_rel = f"file://{(folder / f'page_{pdf_page:03d}.masked.png').resolve()}"

    # --- the painting annotation: put the page image on the canvas ---
    painting = {
        "id":         f"{canvas_id}/painting",
        "type":       "Annotation",
        "motivation": "painting",
        "body": {
            "id":     png_rel,
            "type":   "Image",
            "format": "image/png",
            "width":  w,
            "height": h,
        },
        "target": canvas_id,
    }

    # --- one supplementing annotation per labelled region (the gold boxes) ---
    region_annos = []
    for reg in rec.get("regions", []):
        verts = reg.get("vertices")
        if not verts:
            continue
        x, y, bw, bh = _bbox_from_vertices(verts)
        rid = reg.get("rid", "")
        region_annos.append({
            "id":         f"{canvas_id}/anno/{rid}",
            "type":       "Annotation",
            "motivation": "supplementing",            # text that supplements the painted image
            "body": {
                "type":     "TextualBody",
                "language": "en",
                "format":   "text/plain",
                "value":    reg.get("text", ""),
                # carry the gold label + OCR confidence as purpose/metadata for the viewer/tooltip
                "purpose":  reg.get("category", "transcribing"),
            },
            # target = this exact rectangle of this exact canvas; this is the citation anchor.
            "target":     f"{canvas_id}#xywh={x},{y},{bw},{bh}",
            # non-standard but harmless extension so downstream tools keep our provenance:
            "_rid":       rid,
            "_min_conf":  reg.get("min_conf"),
        })

    canvas = {
        "id":     canvas_id,
        "type":   "Canvas",
        "height": h,
        "width":  w,
        "label":  {"en": [rec.get("page_label") or f"p. {rec.get('page_number_in_type', pdf_page)}"]},
        "items":  [{
            "id":    f"{canvas_id}/page",
            "type":  "AnnotationPage",
            "items": [painting],
        }],
    }
    if region_annos:
        # "annotations" (vs "items") is where supplementing/commenting annotations live in v3.
        canvas["annotations"] = [{
            "id":    f"{canvas_id}/annos",
            "type":  "AnnotationPage",
            "items": region_annos,
        }]
    return canvas


def build_iiif(records_by_section: dict, out_dir: Path) -> int:
    """Write one IIIF v3 manifest per volume into ``iiif/<Volume>/manifest.json``.

    Args:
        records_by_section: {section -> [page records...]} (pages in pdf-page order).
        out_dir:            the ``searchable_pdfs/iiif`` root.

    Returns:
        The number of manifests written.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    total_regions = 0
    for section, recs in sorted(records_by_section.items()):
        base = f"{IIIF_BASE}/{section}"
        canvases = []
        for rec in recs:
            folder = _page_folder(section, rec["pdf_page_number"])
            if folder is None:
                continue                                # can't locate the image -> skip this page
            canvas = _iiif_canvas(rec, folder, base)
            canvases.append(canvas)
            total_regions += sum(len(ap.get("items", [])) for ap in canvas.get("annotations", []))
        if not canvases:
            continue
        manifest = {
            "@context": "http://iiif.io/api/presentation/3/context.json",
            "id":       f"{base}/manifest.json",
            "type":     "Manifest",
            "label":    {"en": [section.replace("_", " ")]},
            "summary":  {"en": ["Solanus Casey archival sources — page images with labelled-region "
                                "annotations (W3C Web Annotations) for citation/deep-zoom."]},
            "items":    canvases,
        }
        sec_dir = out_dir / section
        sec_dir.mkdir(parents=True, exist_ok=True)
        (sec_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
        written += 1
        log.info("  IIIF: %s -> %d canvases", section, len(canvases))

    _write_iiif_readme(out_dir)
    costlog.log(provider="local", model="iiif-presentation-3", op="manifest",
                items=total_regions, usd=0.0, meta=f"manifests={written} region_annos={total_regions}")
    log.info("  IIIF: wrote %d manifest(s), %d region annotations", written, total_regions)
    return written


def _write_iiif_readme(out_dir: Path) -> None:
    """Drop a short recipe next to the manifests so the format is self-documenting."""
    (out_dir / "README.md").write_text(
        "# IIIF Presentation 3.0 manifests + Web Annotations\n\n"
        "One `manifest.json` per volume (the book); one **Canvas** per page (a pixel-sized plane);\n"
        "one **painting** annotation puts the page PNG on the canvas; an **AnnotationPage** of\n"
        "**supplementing** [W3C Web Annotations](https://www.w3.org/TR/annotation-model/) — one per\n"
        "labelled region — carries the gold/OCR `text` and targets the region's box via an\n"
        "`#xywh=x,y,w,h` media-fragment. Each annotation also keeps `_rid` + `_min_conf` so a\n"
        "citation resolves to `doc_id + rid + page + box`.\n\n"
        "## How to view\n"
        "Open a manifest in a IIIF v3 viewer — e.g. drag its path into the **Mirador** "
        "(<https://projectmirador.org/embed/>) or **Clover** demo, or load it in the\n"
        "Universal Viewer. The region boxes appear as highlight annotations.\n\n"
        "## Coordinates\n"
        "Targets are axis-aligned bounding boxes of the gold polygons (min/max of `vertices`,\n"
        "in image pixels). For pixel-perfect rotated regions, swap the target to an **SVG selector**\n"
        "([Web Annotation SvgSelector](https://www.w3.org/TR/annotation-model/#svg-selector)).\n\n"
        "## Publishing (TODO)\n"
        "Ids use a `file://` base so manifests open locally with no server. To publish: set\n"
        "`IIIF_BASE` in `stages/index_sources.py` to your public IIIF **Image API** endpoint and\n"
        "give each canvas's painting body a `service` block (Image API level0/level2) for deep zoom.\n",
    )


# ==================================================================
# (3) Text-layer ("searchable") PDFs — invisible OCR words over the scan
# ==================================================================
# WHY an *invisible* text layer: we keep the beautiful scan exactly as-is for the eye, and add a
# transparent word layer the *machine* can read — select, copy, Ctrl-F, and let Google index it.
# Each word is drawn at render mode 3 ("invisible") at its OCR box, scaled from image pixels to the
# PDF's point grid (PDF user space is 72 points/inch; the image was rendered at `render_dpi`, so the
# scale is 72 / dpi). PDF's origin is bottom-left, the image's is top-left, so we flip y.
#
# Two paths, picked at runtime:
#   * reportlab present  -> we build the layer ourselves and STAMP it onto the page PDF (full impl).
#   * reportlab absent   -> we WRITE a runnable ocrmypdf command file + leave a TODO (documented).

def _azure_words(folder: Path, pdf_page: int) -> list:
    """Per-word OCR boxes for a page from ``extract_azure.raw.json``.

    Returns a list of ``{text, polygon}`` where ``polygon`` is Azure's 8-number quad in image
    pixels (``[x0,y0,x1,y1,x2,y2,x3,y3]``). These are the spans we lay the invisible text over.
    """
    raw = _read_json(folder / "extract_azure.raw.json")
    if not raw:
        return []
    pages = (raw.get("full_page") or {}).get("pages") or []
    if not pages:
        return []
    words = []
    for wd in pages[0].get("words", []):
        poly, content = wd.get("polygon"), wd.get("content")
        if poly and content:
            words.append({"text": content, "polygon": poly})
    return words


def _try_import_reportlab():
    """Return the reportlab pieces we need, or None if reportlab isn't installed."""
    try:
        from reportlab.pdfgen import canvas as rl_canvas       # noqa: F401
        from reportlab.lib.colors import Color                 # noqa: F401
        from reportlab.pdfbase.pdfmetrics import stringWidth   # noqa: F401
        return {"canvas": rl_canvas, "Color": Color, "stringWidth": stringWidth}
    except Exception:
        return None


def _try_import_pypdf():
    """Return a (reader, writer) pair from pypdf or PyPDF2, or (None, None) if neither is present.

    We need a PDF library only to *stamp* (merge) our text layer onto the existing page PDF. reportlab
    can author a PDF but not merge into one, so this is the second ingredient of the full path.
    """
    try:
        from pypdf import PdfReader, PdfWriter
        return PdfReader, PdfWriter
    except Exception:
        try:
            from PyPDF2 import PdfReader, PdfWriter
            return PdfReader, PdfWriter
        except Exception:
            return None, None


def _build_text_layer(rl, words: list, img_w: int, img_h: int, dpi: int) -> bytes:
    """Author a 1-page PDF containing only the invisible OCR words, sized to match the page.

    Args:
        rl:    the dict from `_try_import_reportlab()`.
        words: ``[{text, polygon}]`` in image pixels.
        img_w, img_h: page image size in pixels.
        dpi:   the DPI the image was rendered at (to convert pixels -> PDF points).

    Returns:
        The text-layer PDF as bytes (a single page, transparent words, no visible marks).
    """
    import io
    scale = 72.0 / float(dpi or 150)                   # pixels -> points (PDF = 72 pt/inch)
    pw_pt, ph_pt = img_w * scale, img_h * scale
    buf = io.BytesIO()
    c = rl["canvas"].Canvas(buf, pagesize=(pw_pt, ph_pt))
    # NB: render mode (3 == invisible: present to search, unseen to eye) is set per text object below
    # via ``t.setTextRenderMode(3)`` — reportlab exposes it on the text object, not the Canvas.

    for wd in words:
        poly = wd["polygon"]
        xs = poly[0::2]
        ys = poly[1::2]
        x0, x1 = min(xs), max(xs)
        y0, y1 = min(ys), max(ys)
        box_w_pt = (x1 - x0) * scale
        box_h_pt = (y1 - y0) * scale
        if box_w_pt <= 0 or box_h_pt <= 0:
            continue
        # Font size ~= the box height (a hair smaller so descenders sit inside the box).
        font_size = max(1.0, box_h_pt * 0.9)
        # Width-match: scale the glyphs horizontally so our text spans the same width the ink did —
        # this keeps the selectable text aligned with what the reader sees underneath.
        natural = rl["stringWidth"](wd["text"], "Helvetica", font_size) or box_w_pt
        h_scale = (box_w_pt / natural) * 100.0 if natural else 100.0
        # Flip y: image origin is top-left, PDF origin is bottom-left.
        x_pt = x0 * scale
        y_pt = ph_pt - (y1 * scale)                    # baseline ~ bottom of the box
        t = c.beginText()
        t.setTextRenderMode(3)
        t.setFont("Helvetica", font_size)
        t.setHorizScale(h_scale)
        t.setTextOrigin(x_pt, y_pt)
        t.textOut(wd["text"])
        c.drawText(t)
    c.showPage()
    c.save()
    return buf.getvalue()


def _stamp(reader_cls, writer_cls, base_pdf: Path, overlay_bytes: bytes, out_pdf: Path) -> bool:
    """Merge the invisible text layer on top of the scanned page PDF -> a searchable PDF.

    Returns True on success, False if anything goes wrong (we then leave the page un-stamped rather
    than crash the whole run).
    """
    import io
    try:
        base = reader_cls(str(base_pdf))
        overlay = reader_cls(io.BytesIO(overlay_bytes))
        writer = writer_cls()
        page = base.pages[0]
        page.merge_page(overlay.pages[0])              # text layer ON TOP of the image
        writer.add_page(page)
        out_pdf.parent.mkdir(parents=True, exist_ok=True)
        with open(out_pdf, "wb") as f:
            writer.write(f)
        return True
    except Exception as e:                             # noqa: BLE001 — log + skip, never abort the build
        log.warning("    stamp failed for %s: %s", base_pdf.name, e)
        return False


def build_searchable_pdfs(records: list, out_dir: Path) -> dict:
    """Produce text-layer searchable PDFs (reportlab path) OR an ocrmypdf recipe (fallback).

    Args:
        records: page descriptors (same shape as build_fts's input).
        out_dir: the ``searchable_pdfs/`` root; per-volume PDFs land under ``<Volume>/``.

    Returns:
        A small status dict {mode, written, planned} for the run summary.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rl = _try_import_reportlab()
    reader_cls, writer_cls = _try_import_pypdf()

    # ---------- Full path: reportlab + a PDF merge library are both available ----------
    if rl and reader_cls and writer_cls:
        written = 0
        for r in records:
            folder = r["folder"]
            pdf_page = r["pdf_page"]
            base_pdf = folder / f"page_{pdf_page:03d}.masked.pdf"
            if not base_pdf.exists():
                continue
            words = _azure_words(folder, pdf_page)
            if not words:
                continue                                # nothing to make searchable on this page
            img_w, img_h = _page_dims(folder, pdf_page)
            pj = _read_json(folder / f"page_{pdf_page:03d}.json") or {}
            dpi = int(pj.get("render_dpi") or 150)
            overlay = _build_text_layer(rl, words, img_w, img_h, dpi)
            out_pdf = out_dir / r["section"] / f"page_{pdf_page:03d}.searchable.pdf"
            if _stamp(reader_cls, writer_cls, base_pdf, overlay, out_pdf):
                written += 1
        costlog.log(provider="local", model="reportlab-textlayer", op="ocr_pdf",
                    items=written, usd=0.0, meta=f"searchable_pdfs={written}")
        log.info("  PDFs: wrote %d searchable PDF(s) via reportlab -> %s", written, out_dir)
        return {"mode": "reportlab", "written": written, "planned": 0}

    # ---------- Fallback: emit a runnable ocrmypdf recipe + leave a clear TODO ----------
    # We don't have reportlab (and/or a merge lib), so we DON'T silently skip the feature — we write
    # a shell script that regenerates the searchable PDFs from the per-volume source PDFs using
    # ocrmypdf, which is the standard, robust tool for exactly this. Run it after `pip install`.
    missing = []
    if not rl:
        missing.append("reportlab")
    if not (reader_cls and writer_cls):
        missing.append("pypdf")
    sections = sorted({r["section"] for r in records})
    _write_ocrmypdf_recipe(out_dir, sections, missing)
    log.warning("  PDFs: %s not installed -> wrote ocrmypdf recipe instead (see %s)",
                " & ".join(missing), out_dir / "MAKE_SEARCHABLE_PDFS.sh")
    costlog.log(provider="local", model="ocrmypdf-recipe", op="ocr_pdf",
                items=len(sections), usd=0.0, meta=f"deferred missing={','.join(missing)}")
    return {"mode": "ocrmypdf-recipe", "written": 0, "planned": len(sections)}


def _write_ocrmypdf_recipe(out_dir: Path, sections: list, missing: list) -> None:
    """Write a self-contained shell script + README documenting the ocrmypdf path.

    ocrmypdf re-OCRs and adds a high-quality invisible text layer to a PDF. For our *whole-volume*
    source PDFs (which live in ``source_data/``), one command per volume produces a fully searchable
    copy — no per-page stamping needed. We point it at those source PDFs and write the searchable
    copies here under ``searchable_pdfs/``.
    """
    src_dir = config.REPO / "source_data"              # Volume_1.pdf ... Volume_4.pdf live here
    lines = [
        "#!/usr/bin/env bash",
        "# Generate text-layer (searchable) PDFs for the Solanus volumes.",
        "#",
        "# TODO: reportlab (and/or pypdf) is not installed in the step_7 venv, so the in-pipeline",
        "#       per-page stamping path was skipped. This script reproduces the SAME artifact with",
        "#       ocrmypdf, the standard tool for adding an invisible OCR text layer to a PDF.",
        "#",
        f"#   missing python deps : {', '.join(missing)}",
        "#   install either path :",
        "#       pip install reportlab pypdf      # then re-run: python stages/index_sources.py",
        "#       # OR use ocrmypdf (system tool): ",
        "#       sudo apt-get install ocrmypdf tesseract-ocr   # Debian/Ubuntu",
        "#       pip install ocrmypdf                            # (still needs the tesseract binary)",
        "",
        "set -euo pipefail",
        f'SRC_DIR="{src_dir}"',
        f'OUT_DIR="{out_dir}"',
        'mkdir -p "$OUT_DIR"',
        "",
        "# --redo-ocr: keep the page image, (re)build the text layer. --optimize 1: light squeeze.",
    ]
    for sec in sections:
        lines.append(
            f'ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/{sec}.pdf" '
            f'"$OUT_DIR/{sec}.searchable.pdf"   # whole-volume searchable PDF'
        )
    lines.append("")
    lines.append('echo "Done. Searchable PDFs in $OUT_DIR"')
    script = out_dir / "MAKE_SEARCHABLE_PDFS.sh"
    script.write_text("\n".join(lines) + "\n")
    script.chmod(0o755)

    (out_dir / "README.md").write_text(
        "# Searchable (text-layer) PDFs\n\n"
        "These PDFs show the original scan but carry an **invisible OCR text layer** on top, so the\n"
        "text is selectable / copy-pastable / Ctrl-F-able and indexable by search engines.\n\n"
        "## Two ways to build them\n"
        "1. **In-pipeline (preferred), per page.** `stages/index_sources.py` stamps each word from\n"
        "   Azure's `extract_azure.raw.json` (per-word `polygon`) onto the page's `*.masked.pdf` at\n"
        "   render mode 3 (invisible), scaling image pixels -> PDF points by `72 / render_dpi`.\n"
        "   Requires `reportlab` (authoring) + `pypdf` (stamping):\n"
        "   ```bash\n   pip install reportlab pypdf\n   python stages/index_sources.py\n   ```\n\n"
        "2. **Fallback, per volume, with ocrmypdf.** If those deps are absent, run the generated\n"
        "   `MAKE_SEARCHABLE_PDFS.sh` (it OCRs the whole-volume source PDFs in `source_data/`).\n"
        "   ```bash\n   ./MAKE_SEARCHABLE_PDFS.sh\n   ```\n\n"
        "TODO: install `reportlab`+`pypdf` (or `ocrmypdf`) to actually produce the PDFs — until then\n"
        "only this recipe is written (no PDFs), by design (non-destructive, no heavy install here).\n",
    )


# ==================================================================
# run() — the stage entry point the DAG calls
# ==================================================================
def _collect_records(limit: int | None = None) -> tuple[list, dict]:
    """Read documents.json + notebooks.json into a flat page list + a {section -> [recs]} map.

    Returns:
        (records, by_section) where each record has the keys build_fts / build_searchable_pdfs need
        (id, section, kind, pdf_page, page_label, notebook, folder) and ``by_section`` keeps the full
        original records (with ``regions``) in pdf-page order for the IIIF builder.
    """
    records: list = []
    by_section: dict = {}

    def _ingest(raw_records: list, kind: str):
        for rec in raw_records:
            section = rec.get("section")
            pdf_page = rec.get("pdf_page_number")
            if section is None or pdf_page is None:
                continue
            folder = _page_folder(section, pdf_page)
            if folder is None:
                continue
            # page_label only exists on notebook *entries*; use struct_doc on the page if present.
            page_label = rec.get("text_by_label", {}).get("struct_doc", "") if kind == "notebook" else ""
            records.append({
                "id":         rec["id"],
                "section":    section,
                "kind":       kind,
                "pdf_page":   pdf_page,
                "page_label": page_label,
                "notebook":   rec.get("notebook", ""),
                "folder":     folder,
            })
            by_section.setdefault(section, []).append(rec)

    _ingest(json.loads(config.DOCUMENTS.read_text()), "letter")
    _ingest(json.loads(config.NOTEBOOKS.read_text()), "notebook")

    # Sort each section's IIIF records by pdf-page so manifests read front-to-back.
    for sec in by_section:
        by_section[sec].sort(key=lambda r: r.get("pdf_page_number", 0))

    if limit:
        # Stride-sample so a smoke test still spans multiple volumes (mirrors chunks.build_chunks).
        if limit < len(records):
            step = len(records) / limit
            records = [records[int(i * step)] for i in range(limit)]
        keep = {r["section"] for r in records}
        seen_pages = {(r["section"], r["pdf_page"]) for r in records}
        by_section = {
            s: [r for r in recs if (s, r["pdf_page_number"]) in seen_pages]
            for s, recs in by_section.items() if s in keep
        }
    return records, by_section


def run(limit: int | None = None) -> dict:
    """Build all three searchable-source artifacts (FTS + IIIF + searchable PDFs). All local/free.

    Args:
        limit: if set, only process a strided sample of pages (smoke test). None = full corpus.

    Returns:
        A summary dict {fts_pages, iiif_manifests, pdfs:{...}}.
    """
    config.DATA.mkdir(parents=True, exist_ok=True)
    config.SEARCHABLE_PDFS.mkdir(parents=True, exist_ok=True)

    log.info("index_sources: collecting page records%s ...", f" (limit={limit})" if limit else "")
    records, by_section = _collect_records(limit=limit)
    log.info("  %d page records across %d section(s)", len(records), len(by_section))

    # (1) FTS5 over cleaned page OCR text.
    fts_pages = build_fts(records, config.DATA / "fts.sqlite")

    # (2) IIIF v3 manifests + Web Annotations for labelled regions.
    iiif_manifests = build_iiif(by_section, config.SEARCHABLE_PDFS / "iiif")

    # (3) Text-layer searchable PDFs (reportlab) OR an ocrmypdf recipe (fallback).
    pdfs = build_searchable_pdfs(records, config.SEARCHABLE_PDFS)

    # ------------------------------------------------------------------ SUMMARY (per STYLE_GUIDE)
    print("=" * 60)
    print("index_sources SUMMARY")
    print("=" * 60)
    print(f"  FTS5 pages indexed     : {fts_pages}")
    print(f"  IIIF manifests written : {iiif_manifests}")
    print(f"  searchable PDFs        : {pdfs['mode']}  (written={pdfs['written']}, planned={pdfs['planned']})")
    print(f"  fts.sqlite             : {config.DATA / 'fts.sqlite'}")
    print(f"  iiif/                  : {config.SEARCHABLE_PDFS / 'iiif'}")
    print(f"  searchable_pdfs/       : {config.SEARCHABLE_PDFS}")
    print("=" * 60)
    return {"fts_pages": fts_pages, "iiif_manifests": iiif_manifests, "pdfs": pdfs}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build FTS5 + IIIF + searchable PDFs (all local/free).")
    ap.add_argument("--limit", type=int, default=None, help="strided sample of pages for a smoke test")
    ap.add_argument("--search", type=str, default=None, help="just run an FTS query against data/fts.sqlite")
    ap.add_argument("-k", type=int, default=10, help="results for --search")
    a = ap.parse_args()
    if a.search:
        for hit in fts_search(a.search, k=a.k):
            print(json.dumps(hit, ensure_ascii=False))
    else:
        run(limit=a.limit)
