"""
Auto-label scanned pages with Gemini using few-shot examples.

Reads labeled_examples/ as few-shot data, processes every PDF in
polygon_cropped_pdfs/, and writes labeled JSONs (+ a copy of the PDF)
to auto_labeled/{doc}/page_XXX/ matching the labeled_examples/ layout.

Usage (run from any directory):
    python auto_labeler.py                          # process everything (skip already-done)
    python auto_labeler.py --volume Volume_1        # one document only
    python auto_labeler.py --start 10 --end 30      # page range (1-indexed, within volume)
    python auto_labeler.py --dry-run 3              # just process 3 pages and stop
    python auto_labeler.py --image-width 2048       # send larger images to Gemini
    python auto_labeler.py --image-width full       # send images at full 150 DPI render
    python auto_labeler.py --num-fewshot 6
    python auto_labeler.py --model gemini-3-flash-preview
    python auto_labeler.py --delay 1.5
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import shutil
import sys
import time
from io import BytesIO
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from pdf2image import convert_from_path
from PIL import Image
from pydantic import BaseModel, Field, ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from google import genai
from google.genai import types

from pricing import append_run_summary

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR           = Path(__file__).resolve().parent
LABELED_EXAMPLES_DIR = SCRIPT_DIR / "labeled_examples"
POLYGON_PDFS_DIR     = SCRIPT_DIR / "polygon_cropped_pdfs"
AUTO_LABELED_DIR     = SCRIPT_DIR / "auto_labeled"
ENV_PATH             = SCRIPT_DIR / ".env"
USAGE_CSV            = SCRIPT_DIR / "usage.csv"
URI_MAP_PATH         = SCRIPT_DIR / "file_uris.json"  # written by upload_examples.py

# ── Constants ─────────────────────────────────────────────────────────────────
RENDER_DPI = 150  # matches polygon_cropped_pdfs source resolution

# Categories actually present in the labeled_examples corpus.
CATEGORIES: tuple[str, ...] = (
    "src_content", "src_origin", "src_recipient",
    "src_location_recipient", "src_location_sender", "src_date",
    "src_greeting", "src_farewell", "src_signature", "src_other",
    "archv_commentary", "archv_format_note", "archv_date",
    "archv_possessor", "archv_other",
    "struct_id", "struct_doc", "struct_commentary", "struct_other",
    "other",
)

CATEGORY_DESCRIPTIONS = {
    "src_content":            "main body text of the source document (the actual letter/journal/note content)",
    "src_origin":             "author/sender name or institution at top of source",
    "src_recipient":          "recipient/addressee name",
    "src_location_recipient": "recipient's city / address",
    "src_location_sender":    "sender's city / location line",
    "src_date":               "date written by the source author",
    "src_greeting":           "opening salutation (e.g. 'Dear Sister Mary')",
    "src_farewell":           "closing formula (e.g. 'Sincerely yours')",
    "src_signature":          "signature block",
    "src_other":              "other source-document elements",
    "archv_commentary":       "archivist-added notes/commentary (e.g. transcription notice)",
    "archv_format_note":      "archivist description of physical format ('written on an envelope')",
    "archv_date":             "date stamp added by archivist",
    "archv_possessor":        "ownership stamp / 'Original in Possession of...' block",
    "archv_other":            "other archivist-added annotations",
    "struct_id":              "document identifier / catalog number (e.g. 'NOTEBOOK NO. 1')",
    "struct_doc":             "document boundary markers, page numbers, 'Page X Cont.'",
    "struct_commentary":      "structural commentary by archivist",
    "struct_other":           "other structural elements",
    "other":                  "anything that doesn't fit the categories above",
}

# ── Pydantic schemas ──────────────────────────────────────────────────────────
# The schema sent to Gemini covers ONLY the polygon data. All page metadata
# (page_number, source_file, page_width, page_height, render_dpi) is filled
# in locally. UUIDs and connections=[] are added locally after parsing.


class Vertex(BaseModel):
    x: int = Field(..., description="x coordinate normalized to [0, 1000] (0=left, 1000=right)")
    y: int = Field(..., description="y coordinate normalized to [0, 1000] (0=top, 1000=bottom)")


class Region(BaseModel):
    vertices: list[Vertex] = Field(
        ...,
        description="Polygon vertices (4 corners) with coords in [0, 1000] normalized space",
    )


class DocumentLabels(BaseModel):
    """One labeled document's regions, grouped by category.

    All 20 category fields are present; categories with no regions are empty lists.
    A list-of-fixed-fields shape (rather than dict[str, list]) lets Gemini's
    structured-output engine constrain the response reliably.
    """

    src_content:            list[Region]
    src_origin:             list[Region]
    src_recipient:          list[Region]
    src_location_recipient: list[Region]
    src_location_sender:    list[Region]
    src_date:               list[Region]
    src_greeting:           list[Region]
    src_farewell:           list[Region]
    src_signature:          list[Region]
    src_other:              list[Region]
    archv_commentary:       list[Region]
    archv_format_note:      list[Region]
    archv_date:             list[Region]
    archv_possessor:        list[Region]
    archv_other:            list[Region]
    struct_id:              list[Region]
    struct_doc:             list[Region]
    struct_commentary:      list[Region]
    struct_other:           list[Region]
    other:                  list[Region]


class DocumentEntry(BaseModel):
    """One document on a page. name is 'doc_1', 'doc_2', ..."""
    name:   str
    labels: DocumentLabels


class DocumentsResponse(BaseModel):
    """API response (pass 1). A page may contain 1+ logical documents (letters)."""
    documents: list[DocumentEntry]


class ConnectionEdge(BaseModel):
    """One bidirectional link between two polygons identified by short ids."""
    from_id: str = Field(..., description="Short id like 'p1' from the polygon table")
    to_id:   str = Field(..., description="Short id like 'p3' from the polygon table")


class ConnectionsResponse(BaseModel):
    """API response (pass 2). List of edges; empty list if no connections."""
    edges: list[ConnectionEdge]


# ── Logging ───────────────────────────────────────────────────────────────────

log = logging.getLogger("auto_labeler")


def _setup_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )


# ── Coordinate scaling ────────────────────────────────────────────────────────
# Gemini emits bounding-box coordinates in a NORMALIZED [0, 1000] space,
# regardless of the actual input image dimensions. This is documented Gemini
# behavior and overrides whatever the prompt says about pixel space.
#
# So:
#   - Few-shot JSON coords  : scaled FROM source-page space TO [0, 1000]
#                             (so the model sees consistent training in its
#                              native coordinate frame).
#   - Model output coords   : decoded FROM [0, 1000] TO source-page space
#                             (using the TARGET page's source dimensions).
#
# Image-render width still controls how much detail the model can SEE, but it
# no longer affects coordinate accuracy. Render at 1024 or full — coords are
# in 0-1000 either way.

NORM_RANGE = 1000


def _scale_regions_to_render(
    page_data: dict,
    rendered_width:  int,    # kept for signature compat; unused now
    rendered_height: int,    # kept for signature compat; unused now
) -> dict:
    """Return a copy of page_data['documents'] with coords scaled to [0, 1000]
    normalized space. Used to construct the JSON payload shown alongside each
    few-shot example image."""
    src_w = page_data["page_width"]
    src_h = page_data["page_height"]
    sx = NORM_RANGE / src_w
    sy = NORM_RANGE / src_h

    docs_list = []
    for doc_name in sorted(page_data["documents"].keys()):
        doc = page_data["documents"][doc_name]
        labels = {}
        for cat in CATEGORIES:
            polys = doc.get(cat, [])
            labels[cat] = [
                {
                    "vertices": [
                        {"x": int(round(v["x"] * sx)),
                         "y": int(round(v["y"] * sy))}
                        for v in poly["vertices"]
                    ]
                }
                for poly in polys
            ]
        docs_list.append({"name": doc_name, "labels": labels})
    return {"documents": docs_list}


def _scale_response_to_original(
    response: DocumentsResponse,
    rendered_width:  int,    # kept for signature compat; unused now
    rendered_height: int,    # kept for signature compat; unused now
    page_width:      int,
    page_height:     int,
) -> dict:
    """Convert API response (coords in [0, 1000] normalized) back into the
    labeled_examples JSON format with coordinates in original page pixel
    space, plus locally-generated UUIDs and empty connections lists."""
    sx = page_width  / NORM_RANGE
    sy = page_height / NORM_RANGE

    documents: dict[str, dict] = {}
    for entry in response.documents:
        doc_dict: dict[str, list] = {}
        labels = entry.labels.model_dump()
        for cat in CATEGORIES:
            polys = labels[cat]
            doc_dict[cat] = [
                {
                    "vertices": [
                        {"x": int(round(v["x"] * sx)),
                         "y": int(round(v["y"] * sy))}
                        for v in poly["vertices"]
                    ],
                    "connections": [],
                    "id": str(uuid4()),
                }
                for poly in polys
            ]
        documents[entry.name] = doc_dict
    return documents


# ── PDF rendering with cache ──────────────────────────────────────────────────

_render_cache: dict[tuple[str, int], tuple[Image.Image, int, int, int, int]] = {}
# key: (pdf_path, image_width); value: (PIL Image, rendered_w, rendered_h, page_w, page_h)

# Filled in main() from file_uris.json if present. Maps "Volume_1/page_004" ->
# "files/abc123xyz" (the Files API resource name). Empty dict = inline images.
_file_uri_map: dict[str, str] = {}
_file_obj_cache: dict = {}

# Set in main() after we have an API key. Used by _get_uploaded_file().
_client: genai.Client | None = None


def _load_file_uri_map(image_width_arg: str) -> dict[str, str]:
    """Load file_uris.json if present and width matches; else empty dict."""
    if not URI_MAP_PATH.exists():
        return {}
    data = json.loads(URI_MAP_PATH.read_text())
    if data.get("image_width") != image_width_arg:
        log.warning(
            "file_uris.json was uploaded with --image-width=%s but you're running "
            "with %s. Ignoring URI map and inlining images. Re-run "
            "upload_examples.py with --image-width=%s to fix.",
            data.get("image_width"), image_width_arg, image_width_arg,
        )
        return {}
    log.info("Loaded %d uploaded file URIs from %s",
             len(data.get("uris", {})), URI_MAP_PATH.name)
    return data.get("uris", {})


def _get_uploaded_file(page_dir: Path):
    """Return the Gemini File object for this labeled example, or None if not uploaded.

    Lazy: only calls client.files.get() the first time each file is needed.
    """
    key = f"{page_dir.parent.name}/{page_dir.name}"
    file_name = _file_uri_map.get(key)
    if not file_name:
        return None
    if file_name not in _file_obj_cache:
        try:
            _file_obj_cache[file_name] = _client.files.get(name=file_name)
        except Exception as exc:
            log.warning("File URI %s unreachable (%s); falling back to inline.",
                        file_name, exc)
            _file_obj_cache[file_name] = None
    return _file_obj_cache[file_name]


def render_page(pdf_path: Path, image_width: int | None) -> tuple[Image.Image, int, int, int, int]:
    """Render a single-page PDF to a PIL image.

    Args:
        pdf_path: path to a single-page PDF.
        image_width: target width in pixels for the rendered image.
                     None means render at full RENDER_DPI with no resizing.

    Returns:
        (image, rendered_w, rendered_h, source_page_w, source_page_h)
        where source_* are the dimensions at the original RENDER_DPI render.
    """
    cache_key = (str(pdf_path), image_width or 0)
    if cache_key in _render_cache:
        return _render_cache[cache_key]

    pages = convert_from_path(pdf_path, dpi=RENDER_DPI, first_page=1, last_page=1)
    full = pages[0]
    source_w, source_h = full.width, full.height

    if image_width is None or image_width >= source_w:
        rendered = full
    else:
        new_h = int(round(source_h * (image_width / source_w)))
        rendered = full.resize((image_width, new_h), Image.LANCZOS)

    result = (rendered, rendered.width, rendered.height, source_w, source_h)
    _render_cache[cache_key] = result
    return result


# ── Few-shot example selection ────────────────────────────────────────────────


def _discover_labeled_examples() -> list[Path]:
    """Find every labeled example page directory under LABELED_EXAMPLES_DIR."""
    examples: list[Path] = []
    for doc_dir in sorted(LABELED_EXAMPLES_DIR.iterdir()):
        if not doc_dir.is_dir():
            continue
        for page_dir in sorted(doc_dir.glob("page_*")):
            if (page_dir / f"{page_dir.name}.json").exists() and \
               (page_dir / f"{page_dir.name}.pdf").exists():
                examples.append(page_dir)
    return examples


def _classify_examples_by_num_docs(all_examples: list[Path]) -> set[Path]:
    """Return the subset of examples whose JSON has num_documents >= 2.

    Computed once at startup so select_few_shot can do fast set lookups.
    """
    multi: set[Path] = set()
    for page_dir in all_examples:
        try:
            data = json.load(open(page_dir / f"{page_dir.name}.json"))
            if data.get("num_documents", 1) >= 2:
                multi.add(page_dir)
        except Exception:
            pass
    return multi


def select_few_shot(
    all_examples:   list[Path],
    multi_doc_set:  set[Path],
    target_doc:     str,
    num_fewshot:    int,
    min_multi_doc:  int,
    rng:            random.Random,
) -> list[Path]:
    """Pick few-shot examples with two simultaneous biases:
      1. Same volume as target_doc (visual style match).
      2. At least `min_multi_doc` multi-doc examples (forces the model to
         see and emit doc_2/doc_3 patterns instead of defaulting to 1).

    Multi-doc quota is filled first (preferring same-volume multi). Remaining
    slots are filled biased toward same-volume regardless of doc count.
    """
    # Split into four buckets: (same/other) × (multi/single)
    same_multi   = [p for p in all_examples if p.parent.name == target_doc and p in multi_doc_set]
    same_single  = [p for p in all_examples if p.parent.name == target_doc and p not in multi_doc_set]
    other_multi  = [p for p in all_examples if p.parent.name != target_doc and p in multi_doc_set]
    other_single = [p for p in all_examples if p.parent.name != target_doc and p not in multi_doc_set]
    for lst in (same_multi, same_single, other_multi, other_single):
        rng.shuffle(lst)

    picks: list[Path] = []

    # Step 1: hit the multi-doc quota (capped by what's available + num_fewshot).
    multi_target = min(min_multi_doc, num_fewshot, len(same_multi) + len(other_multi))
    for pool in (same_multi, other_multi):
        while len(picks) < multi_target and pool:
            picks.append(pool.pop(0))

    # Step 2: fill remaining slots, preferring same-volume.
    fill_order = (same_single, same_multi, other_single, other_multi)
    while len(picks) < num_fewshot and any(fill_order):
        for pool in fill_order:
            if pool and len(picks) < num_fewshot:
                picks.append(pool.pop(0))

    return picks


# ── Prompt construction ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are labeling regions in scanned archival document pages.

Each region is a polygon (4 vertices forming a quadrilateral) covering a \
contiguous block of content on the page, tagged with one of these categories:

{categories}

CRITICAL — count documents FIRST:
Pages often contain MULTIPLE distinct documents (1, 2, or even 3 letters / \
notes on the same physical page). Before assigning any labels, scan the entire \
page top-to-bottom and count the distinct documents you see. Signs of a new \
document starting:
  - A separate date appearing mid-page (e.g. "December 29th" below an earlier letter)
  - A separate signature / closing block ("Sincerely, Br. Francis ...")
  - A visible whitespace gap or horizontal break between content blocks
  - A new salutation ("Dear Margaret:") appearing after another letter's signature
  - Different handwriting style or letter formatting

Each distinct document gets its OWN entry: doc_1, doc_2, doc_3. Never lump \
two separate letters into one doc — doing so causes labels to drift into the \
empty gap between them, far from the actual text they should be on.

Coordinates MUST be NORMALIZED to the range [0, 1000] for BOTH x and y, \
regardless of the input image's actual pixel dimensions. x=0 is the left \
edge of the page, x=1000 is the right edge; y=0 is the top, y=1000 is the \
bottom. So x=500, y=500 means horizontally and vertically centered. \
Vertices should go roughly clockwise from the top-left of each region.

Below are several example pages with their correct labels, followed by a \
new page you must label using the same schema.
"""


def build_prompt_parts(
    target_image: Image.Image,
    fewshot_pairs: list[tuple[Image.Image, dict]],
) -> list:
    """Build the multipart list passed to model.generate_content().

    Each few-shot pair contributes an EXAMPLE i header, image, labels JSON.
    Target image is sent last with a 'PAGE TO LABEL' header.
    """
    categories_block = "\n".join(
        f"  - {cat}: {CATEGORY_DESCRIPTIONS[cat]}" for cat in CATEGORIES
    )
    parts: list = [SYSTEM_PROMPT.format(categories=categories_block)]
    for i, (img, payload) in enumerate(fewshot_pairs, start=1):
        parts.append(f"EXAMPLE {i}:")
        parts.append(img)
        parts.append("Labels:\n" + json.dumps(payload, separators=(",", ":")))
    parts.append("PAGE TO LABEL:")
    parts.append(target_image)
    parts.append(
        "Return labels for this page in the same JSON schema as the examples. "
        "Use the same coordinate frame as the input image."
    )
    return parts


# ── Gemini call ───────────────────────────────────────────────────────────────


class TransientAPIError(Exception):
    """Raised for errors that should trigger a retry."""


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def call_gemini(
    client:       genai.Client,
    model_name:   str,
    prompt_parts: list,
) -> tuple[DocumentsResponse, int, int]:
    """Call Gemini with structured output. Retries on transient errors and on
    schema-validation failures (Gemini sometimes returns malformed JSON).

    Returns (parsed_response, input_tokens, output_tokens).
    """
    try:
        resp = client.models.generate_content(
            model    = model_name,
            contents = prompt_parts,
            config   = types.GenerateContentConfig(
                response_mime_type = "application/json",
                response_schema    = DocumentsResponse,
                temperature        = 0.0,
            ),
        )
    except Exception as exc:
        # Network / 5xx / rate limit issues — retry
        raise TransientAPIError(str(exc)) from exc

    raw_text = (resp.text or "").strip()
    try:
        parsed_json = json.loads(raw_text)
        parsed_resp = DocumentsResponse.model_validate(parsed_json)
    except (json.JSONDecodeError, ValidationError) as exc:
        # Schema validation failure — retry (low temp + retries usually fix this)
        raise TransientAPIError(f"validation failed: {exc}") from exc

    usage = getattr(resp, "usage_metadata", None)
    input_tokens  = int(getattr(usage, "prompt_token_count",     0) or 0)
    output_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
    return parsed_resp, input_tokens, output_tokens


# ── Pass 2: connections ───────────────────────────────────────────────────────
# Pass 1 returns polygons with locally-assigned UUIDs and empty connections.
# Pass 2 asks the model "which of these polygons should be linked?" using a
# compact text representation of the polygons (short ids + bboxes) plus the
# target image. Few-shot examples for pass 2 are TEXT-ONLY — no images — since
# (a) connection patterns are highly stylized (95% of edges are
#     src_content↔src_date or src_content↔struct_doc), and
# (b) it keeps the per-page cost low.

PASS2_SYSTEM_PROMPT = """\
You are linking labeled polygons in a scanned archival document page.

Polygons have already been detected and classified. Each has a short id \
(p1, p2, ...) and a bounding box. Decide which polygons should be linked \
with a bidirectional connection.

Rules:
- Connections only link polygons WITHIN the same logical document (doc_1 \
polygons link only to other doc_1 polygons, never to doc_2).
- Each pair is one edge — do NOT emit both 'p1 <-> p2' and 'p2 <-> p1'.
- If a page has no connections, return an empty edges list.

Common edge patterns:
- src_content <-> src_date     (the date written ON or NEAR a body of text)
- src_content <-> struct_doc   (the page-number / 'Page X' marker FOR that body)
- src_content <-> archv_*      (archivist annotation about THAT specific body)

IMPORTANT — pattern is not enough. When a page has multiple candidate dates / \
struct_doc / src_content blocks, you must use the image to decide WHICH date \
or marker belongs to WHICH text. Use spatial proximity, handwriting, layout \
breaks, and what each region actually says. Bbox alone is insufficient.

Each example below shows the page image, the polygon table, and the correct \
connections.
"""


def _bbox_of(vertices: list[dict]) -> tuple[int, int, int, int]:
    """Return (xmin, ymin, xmax, ymax) of a list of {x,y} vertices, as ints."""
    xs = [v["x"] for v in vertices]
    ys = [v["y"] for v in vertices]
    return int(min(xs)), int(min(ys)), int(max(xs)), int(max(ys))


def polygons_to_short_id_text(
    documents: dict,
) -> tuple[str, dict[str, tuple[str, str, str]]]:
    """Render `documents` as a compact text table and return a short-id map.

    Returns:
        text:    multi-line string like
                   doc_1:
                     p1: src_content, bbox=(112,902,4582,5165)
                     p2: src_date,    bbox=(3299,177,3800,438)
                 listing every polygon under its document.
        mapping: short_id -> (doc_name, category, uuid)
    """
    lines: list[str] = []
    mapping: dict[str, tuple[str, str, str]] = {}
    counter = 1
    for doc_name in sorted(documents.keys()):
        lines.append(f"{doc_name}:")
        empty = True
        for cat in CATEGORIES:
            for poly in documents[doc_name].get(cat, []):
                short = f"p{counter}"
                counter += 1
                bbox = _bbox_of(poly["vertices"])
                lines.append(
                    f"  {short}: {cat}, bbox=({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]})"
                )
                mapping[short] = (doc_name, cat, poly["id"])
                empty = False
        if empty:
            lines.append("  (no polygons)")
    return "\n".join(lines), mapping


def _load_pass2_example(
    page_dir:    Path,
    image_width: int | None,
):
    """Load a labeled example for pass 2: (image_or_file, polygons_text, connections_text).

    image_or_file is a Gemini File object when the URI map has an entry,
    otherwise the rendered PIL image (from the cache).
    """
    data = json.load(open(page_dir / f"{page_dir.name}.json"))
    documents = data["documents"]

    img, _rw, _rh, _sw, _sh = render_page(
        page_dir / f"{page_dir.name}.pdf", image_width,
    )
    uploaded = _get_uploaded_file(page_dir)
    image_part = uploaded if uploaded is not None else img

    polys_text, mapping = polygons_to_short_id_text(documents)
    uuid_to_short = {uuid: short for short, (_, _, uuid) in mapping.items()}

    edges_seen: set[tuple[str, str]] = set()
    edge_lines: list[str] = []
    for doc_name in sorted(documents.keys()):
        for cat in CATEGORIES:
            for poly in documents[doc_name].get(cat, []):
                src_short = uuid_to_short.get(poly.get("id"))
                if not src_short:
                    continue
                for conn in poly.get("connections", []):
                    tgt_short = uuid_to_short.get(conn.get("id"))
                    if not tgt_short:
                        continue
                    pair = tuple(sorted([src_short, tgt_short]))
                    if pair in edges_seen:
                        continue
                    edges_seen.add(pair)
                    edge_lines.append(f"  {pair[0]} <-> {pair[1]}")

    conns_text = "\n".join(edge_lines) if edge_lines else "  (none)"
    return image_part, polys_text, conns_text


def discover_pass2_pool(all_examples: list[Path]) -> list[Path]:
    """Return only example dirs whose JSON has at least one connection."""
    pool: list[Path] = []
    for page_dir in all_examples:
        data = json.load(open(page_dir / f"{page_dir.name}.json"))
        for doc in data["documents"].values():
            found = False
            for cat in CATEGORIES:
                for poly in doc.get(cat, []):
                    if poly.get("connections"):
                        pool.append(page_dir)
                        found = True
                        break
                if found:
                    break
            if found:
                break
    return pool


def select_pass2_fewshot(
    pool:        list[Path],
    target_doc:  str,
    num_fewshot: int,
    rng:         random.Random,
) -> list[Path]:
    """Pick pass-2 examples biased toward same volume as target_doc."""
    if not pool:
        return []
    same_vol  = [p for p in pool if p.parent.name == target_doc]
    other_vol = [p for p in pool if p.parent.name != target_doc]
    rng.shuffle(same_vol)
    rng.shuffle(other_vol)
    same_quota = min(len(same_vol), max(2, num_fewshot // 2))
    picked = same_vol[:same_quota] + other_vol[: num_fewshot - same_quota]
    if len(picked) < num_fewshot:
        leftover = [p for p in same_vol[same_quota:] if p not in picked]
        picked += leftover[: num_fewshot - len(picked)]
    return picked[:num_fewshot]


def build_pass2_prompt_parts(
    target_image:      Image.Image,
    target_polys_text: str,
    fewshot_triples:   list[tuple[Image.Image, str, str]],
) -> list:
    """Build the multipart list for pass 2.

    Each few-shot is (image, polygon table, connection list) so the model can
    learn how visual layout determines which polygons connect.
    """
    parts: list = [PASS2_SYSTEM_PROMPT]
    for i, (img, polys_text, conns_text) in enumerate(fewshot_triples, start=1):
        parts.append(f"EXAMPLE {i}:")
        parts.append(img)
        parts.append(f"Polygons:\n{polys_text}\nConnections:\n{conns_text}")
    parts.append("PAGE TO LABEL:")
    parts.append(target_image)
    parts.append(
        f"Polygons:\n{target_polys_text}\n"
        "Return an edges list. Use the short ids from the table. "
        "Empty list if no connections."
    )
    return parts


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def call_gemini_connections(
    client:       genai.Client,
    model_name:   str,
    prompt_parts: list,
) -> tuple[ConnectionsResponse, int, int]:
    """Pass-2 API call. Returns (edges_response, input_tokens, output_tokens)."""
    try:
        resp = client.models.generate_content(
            model    = model_name,
            contents = prompt_parts,
            config   = types.GenerateContentConfig(
                response_mime_type = "application/json",
                response_schema    = ConnectionsResponse,
                temperature        = 0.0,
            ),
        )
    except Exception as exc:
        raise TransientAPIError(str(exc)) from exc

    raw_text = (resp.text or "").strip()
    try:
        parsed_json = json.loads(raw_text)
        parsed_resp = ConnectionsResponse.model_validate(parsed_json)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise TransientAPIError(f"validation failed: {exc}") from exc

    usage = getattr(resp, "usage_metadata", None)
    input_tokens  = int(getattr(usage, "prompt_token_count",     0) or 0)
    output_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
    return parsed_resp, input_tokens, output_tokens


def _find_polygon(
    documents: dict,
    doc_name: str,
    category: str,
    uuid:     str,
) -> dict | None:
    for poly in documents.get(doc_name, {}).get(category, []):
        if poly.get("id") == uuid:
            return poly
    return None


def apply_edges(
    documents: dict,
    edges:     list[ConnectionEdge],
    id_map:    dict[str, tuple[str, str, str]],
) -> int:
    """Mutate `documents` to add bidirectional connections. Returns edges applied.

    Silently drops edges that:
      - Reference unknown short ids (model hallucination).
      - Link polygons in different documents (rule violation).
      - Duplicate an already-present edge.
    """
    applied = 0
    for edge in edges:
        a = id_map.get(edge.from_id)
        b = id_map.get(edge.to_id)
        if a is None or b is None:
            continue
        a_doc, a_type, a_uuid = a
        b_doc, b_type, b_uuid = b
        if a_doc != b_doc or a_uuid == b_uuid:
            continue

        a_poly = _find_polygon(documents, a_doc, a_type, a_uuid)
        b_poly = _find_polygon(documents, b_doc, b_type, b_uuid)
        if a_poly is None or b_poly is None:
            continue

        a_conns = a_poly.setdefault("connections", [])
        b_conns = b_poly.setdefault("connections", [])
        if any(c.get("id") == b_uuid for c in a_conns):
            continue  # already linked

        a_conns.append({"doc": b_doc, "type": b_type, "id": b_uuid})
        b_conns.append({"doc": a_doc, "type": a_type, "id": a_uuid})
        applied += 1
    return applied


# ── Page processing ───────────────────────────────────────────────────────────


def load_example(page_dir: Path, image_width: int | None):
    """Render an example page and build its labels payload in render-space coords.

    Returns (image_or_file, payload). image_or_file is a Gemini File object
    when the URI map has an entry for this page, otherwise the PIL image.
    Dims used for coord scaling come from local rendering either way.
    """
    page_name = page_dir.name
    pdf_path  = page_dir / f"{page_name}.pdf"
    json_path = page_dir / f"{page_name}.json"

    img, rw, rh, _src_w, _src_h = render_page(pdf_path, image_width)
    page_data = json.load(open(json_path))
    payload   = _scale_regions_to_render(page_data, rw, rh)

    uploaded = _get_uploaded_file(page_dir)
    return (uploaded if uploaded is not None else img), payload


def process_page(
    pdf_path:           Path,
    doc_name:           str,
    page_number:        int,
    client:             genai.Client,
    model_name:         str,
    fewshot_dirs:       list[Path],
    image_width:        int | None,
    output_path:        Path,
    pass2_fewshot_dirs: list[Path] | None = None,
) -> tuple[int, int, int, int]:
    """Label one page and save its JSON + copied PDF.

    Two passes:
      1. Detect + classify polygons.
      2. (optional) Identify connections between polygons.

    Returns (in_tok_pass1, out_tok_pass1, in_tok_pass2, out_tok_pass2).
    Pass-2 tokens are 0 if pass2_fewshot_dirs is None or pass 2 failed.
    """
    target_img, render_w, render_h, source_w, source_h = render_page(pdf_path, image_width)
    fewshot_pairs = [load_example(d, image_width) for d in fewshot_dirs]
    prompt_parts  = build_prompt_parts(target_img, fewshot_pairs)

    response, input_tokens, output_tokens = call_gemini(client, model_name, prompt_parts)
    documents = _scale_response_to_original(
        response,
        rendered_width  = render_w,
        rendered_height = render_h,
        page_width      = source_w,
        page_height     = source_h,
    )

    # ── Pass 2: connections (best-effort) ─────────────────────────────────────
    p2_input_tokens, p2_output_tokens = 0, 0
    if pass2_fewshot_dirs is not None:
        target_polys_text, id_map = polygons_to_short_id_text(documents)
        if id_map:  # only call pass 2 if there's at least one polygon to link
            pass2_triples = [_load_pass2_example(d, image_width) for d in pass2_fewshot_dirs]
            pass2_parts   = build_pass2_prompt_parts(target_img, target_polys_text, pass2_triples)
            try:
                edges_resp, p2_input_tokens, p2_output_tokens = call_gemini_connections(
                    client, model_name, pass2_parts,
                )
                n = apply_edges(documents, edges_resp.edges, id_map)
                log.info("  pass 2: %d edge(s) applied", n)
            except Exception as exc:
                log.warning("  pass 2 (connections) failed: %s", exc)

    full_label = {
        "page_number":   page_number,
        "source_file":   str(POLYGON_PDFS_DIR / doc_name / f"{doc_name}.pdf"),
        "page_width":    source_w,
        "page_height":   source_h,
        "render_dpi":    RENDER_DPI,
        "num_documents": len(documents),
        "documents":     documents,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(full_label, f, indent=2)

    # Copy the PDF next to the JSON to match labeled_examples layout.
    pdf_dst = output_path.parent / output_path.name.replace(".json", ".pdf")
    if not pdf_dst.exists():
        shutil.copy2(pdf_path, pdf_dst)

    return input_tokens, output_tokens, p2_input_tokens, p2_output_tokens


# ── Page enumeration ──────────────────────────────────────────────────────────


def discover_target_pages(volume_filter: str | None) -> list[tuple[str, int, Path]]:
    """Return (doc_name, page_number, pdf_path) for every page to consider.

    Filtering by --start/--end is applied per-volume by the caller.
    """
    pages: list[tuple[str, int, Path]] = []
    for doc_dir in sorted(POLYGON_PDFS_DIR.iterdir()):
        if not doc_dir.is_dir():
            continue
        if volume_filter is not None and doc_dir.name != volume_filter:
            continue
        pages_dir = doc_dir / "pages"
        if not pages_dir.is_dir():
            continue
        for pdf in sorted(pages_dir.glob("page_*.pdf")):
            try:
                num = int(pdf.stem.split("_", 1)[1])
            except (ValueError, IndexError):
                continue
            pages.append((doc_dir.name, num, pdf))
    return pages


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--volume",      type=str,   default=None,
                   help="Process only this document (e.g. Volume_1, Appendix_2).")
    p.add_argument("--start",       type=int,   default=None,
                   help="Start page number (inclusive). Within each volume.")
    p.add_argument("--end",         type=int,   default=None,
                   help="End page number (inclusive). Within each volume.")
    p.add_argument("--dry-run",     type=int,   default=None,
                   help="Process at most N pages then stop.")
    p.add_argument("--num-fewshot", type=int,   default=8,
                   help="Number of few-shot examples per API call (default: 8).")
    p.add_argument("--min-fewshot-multi-doc", type=int, default=3,
                   help="Guarantee at least this many multi-document examples in each "
                        "pass-1 few-shot call (default: 3). Helps the model detect "
                        "pages that contain 2+ letters.")
    p.add_argument("--image-width", default="1024",
                   help="Resize images to this width (px) before sending. "
                        "Use 'full' to send original render. Default: 1024.")
    p.add_argument("--model",       type=str,   default="gemini-3.1-flash-lite",
                   help="Gemini model name (default: gemini-3.1-flash-lite). "
                        "Must match a key in pricing.PRICING for cost tracking.")
    p.add_argument("--delay",       type=float, default=1.0,
                   help="Seconds to sleep between API calls (default: 1.0).")
    p.add_argument("--api-key",     type=str,   default=None,
                   help="Override GEMINI_API_KEY from .env / environment.")
    p.add_argument("--seed",        type=int,   default=42,
                   help="RNG seed for few-shot selection (default: 42).")
    p.add_argument("--overwrite",   action="store_true",
                   help="Re-label pages that already have output JSONs.")
    p.add_argument("--no-connections", action="store_true",
                   help="Skip pass 2 (connection inference). Polygons only.")
    p.add_argument("--num-fewshot-pass2", type=int, default=6,
                   help="Number of few-shot examples for pass 2 (default: 6).")
    return p.parse_args()


def resolve_image_width(arg: str) -> int | None:
    if arg.strip().lower() == "full":
        return None
    try:
        w = int(arg)
        if w < 256:
            raise ValueError
        return w
    except ValueError:
        raise SystemExit(f"--image-width must be 'full' or an int >= 256, got {arg!r}")


def main() -> None:
    _setup_logging()
    args = parse_args()

    image_width = resolve_image_width(args.image_width)

    load_dotenv(ENV_PATH)
    api_key = args.api_key or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise SystemExit(
            "No API key. Set GEMINI_API_KEY in step_4/.env or pass --api-key."
        )

    client = genai.Client(api_key=api_key)

    global _file_uri_map, _client
    _client       = client
    _file_uri_map = _load_file_uri_map(args.image_width)

    all_examples = _discover_labeled_examples()
    if len(all_examples) < args.num_fewshot:
        log.warning(
            "Only %d labeled examples available; reducing few-shot count to that.",
            len(all_examples),
        )
        args.num_fewshot = len(all_examples)
    multi_doc_examples = _classify_examples_by_num_docs(all_examples)
    log.info("Loaded %d labeled examples for few-shot pool (%d multi-doc).",
             len(all_examples), len(multi_doc_examples))
    if args.min_fewshot_multi_doc > len(multi_doc_examples):
        log.warning(
            "--min-fewshot-multi-doc=%d but only %d multi-doc examples exist; "
            "the quota will be capped to what's available.",
            args.min_fewshot_multi_doc, len(multi_doc_examples),
        )

    pass2_pool: list[Path] = []
    if not args.no_connections:
        pass2_pool = discover_pass2_pool(all_examples)
        if not pass2_pool:
            log.warning("No labeled examples have connections; pass 2 will be skipped.")
        else:
            if len(pass2_pool) < args.num_fewshot_pass2:
                args.num_fewshot_pass2 = len(pass2_pool)
            log.info(
                "Pass 2 enabled: %d examples in connection-fewshot pool.",
                len(pass2_pool),
            )

    rng = random.Random(args.seed)

    pages = discover_target_pages(args.volume)
    if args.start is not None:
        pages = [p for p in pages if p[1] >= args.start]
    if args.end is not None:
        pages = [p for p in pages if p[1] <= args.end]
    log.info("Found %d target pages to consider.", len(pages))

    succeeded: list[tuple[str, int]] = []
    failed:    list[tuple[str, int, str]] = []
    skipped:   list[tuple[str, int]] = []
    processed = 0
    total_input_tokens  = 0
    total_output_tokens = 0

    for doc_name, page_num, pdf_path in pages:
        if args.dry_run is not None and processed >= args.dry_run:
            log.info("Reached --dry-run limit (%d). Stopping.", args.dry_run)
            break

        out_path = AUTO_LABELED_DIR / doc_name / f"page_{page_num:03d}" / f"page_{page_num:03d}.json"
        if out_path.exists() and not args.overwrite:
            skipped.append((doc_name, page_num))
            continue

        fewshot_dirs = select_few_shot(
            all_examples, multi_doc_examples, doc_name,
            args.num_fewshot, args.min_fewshot_multi_doc, rng,
        )
        pass2_dirs   = (select_pass2_fewshot(pass2_pool, doc_name, args.num_fewshot_pass2, rng)
                        if pass2_pool else None)
        log.info("Labeling %s/page_%03d (%d pass-1 + %d pass-2 examples)",
                 doc_name, page_num,
                 len(fewshot_dirs),
                 len(pass2_dirs) if pass2_dirs else 0)

        try:
            in1, out1, in2, out2 = process_page(
                pdf_path           = pdf_path,
                doc_name           = doc_name,
                page_number        = page_num,
                client             = client,
                model_name         = args.model,
                fewshot_dirs       = fewshot_dirs,
                image_width        = image_width,
                output_path        = out_path,
                pass2_fewshot_dirs = pass2_dirs,
            )
            total_input_tokens  += in1 + in2
            total_output_tokens += out1 + out2
            succeeded.append((doc_name, page_num))
            processed += 1
            if args.delay > 0:
                time.sleep(args.delay)
        except Exception as exc:
            log.error("Failed %s/page_%03d: %s", doc_name, page_num, exc)
            failed.append((doc_name, page_num, str(exc)))
            processed += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Done. Succeeded: %d  |  Failed: %d  |  Skipped: %d",
             len(succeeded), len(failed), len(skipped))
    if failed:
        log.info("Failed pages:")
        for doc, pg, err in failed:
            log.info("  %s/page_%03d: %s", doc, pg, err[:120])

    if len(succeeded) > 0:
        append_run_summary(
            USAGE_CSV, args.model,
            pages         = len(succeeded),
            input_tokens  = total_input_tokens,
            output_tokens = total_output_tokens,
        )
        log.info("Wrote run summary to %s (%d pages, %d in + %d out tokens)",
                 USAGE_CSV.name, len(succeeded),
                 total_input_tokens, total_output_tokens)


if __name__ == "__main__":
    main()
