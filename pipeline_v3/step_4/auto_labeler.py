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
    python auto_labeler.py --num-fewshot 12
    python auto_labeler.py --model gemini-3-flash-preview
    python auto_labeler.py --delay 1.5
    python auto_labeler.py --concurrency 8          # pages labeled in parallel (default 4)
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import random
import shutil
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Literal
from uuid import uuid4

from dotenv import load_dotenv
from pdf2image import convert_from_path
from PIL import Image
from pydantic import BaseModel, Field, ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from google import genai
from google.genai import types

from pricing import append_run_summary

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR           = Path(__file__).resolve().parent
LABELED_EXAMPLES_DIR = SCRIPT_DIR / "labeled_examples"
POLYGON_PDFS_DIR     = SCRIPT_DIR / "polygon_cropped_pdfs"
AUTO_LABELED_DIR     = SCRIPT_DIR / "auto_labeled"

# Concurrency lever: how many pages are labeled in flight at once (override with
# --concurrency). Few-shot selection stays deterministic regardless (it is
# precomputed sequentially in the main thread), and every API call retries with
# jittered exponential backoff, so rate-limit bursts degrade to waiting, not failures.
MAX_CONCURRENCY      = 4
ENV_PATH             = SCRIPT_DIR / ".env"
USAGE_CSV            = SCRIPT_DIR / "usage.csv"
URI_MAP_PATH         = SCRIPT_DIR / "file_uris.json"  # written by upload_examples.py
PAGE_TYPE_CACHE_DIR  = SCRIPT_DIR / "page_type_cache"  # cached per-page VLM page-type calls

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
    "src_content":            "Main body/source text written by the letter author. One generous polygon may swallow multiple paragraphs, embedded verse, inline reference numbers, and even inline dates; it is the largest region and the only source category that participates in connections (links to its struct_doc page marker and/or its src_date). NOTEBOOK / JOURNAL / LEDGER GRANULARITY: emit exactly ONE generous src_content polygon per GOVERNING-ANCHOR SPAN — i.e. per left-margin page marker (struct_doc) and per left-margin/inline margin-date (src_date). Merge ALL consecutive entries, named persons, paragraphs, quotations, itemized lines, and dateless/ditto continuation lines that fall within one span into that single polygon (from the anchor first line down to just above the next margin date or next Page-N marker). Do NOT emit one polygon per entry, per person, per quotation, or per itemized line; whitespace between entries is NOT a reason to split. Start a new src_content polygon ONLY when a new left-margin date or a new Page-N marker begins the next span. MASS-ENROLLMENT / SERAPHIC MASS ASSOCIATION CARD (decisive role split): there is normally NO src_greeting — label by role: (1) the addressee NAME line ('Wanda Urbanick', 'Mr. and Mrs. M.H. Bennett and Family') = src_recipient; (2) ALL middle lines between the name and the sign-off — the intention/dedication phrase (a short dashed/underlined tag '- for peace of mind -' OR a full running-prose intention sentence), any in-body feast word, and any closing blessing/well-wish ('God bless all and each.') — = src_content; (3) the trailing author sign-off ('Fr. Sol.', 'Fr. Solanus, O.F.M.Cap.') = src_signature, ALWAYS peeled out. EXCEPTIONS: a year-first top date stays archv_date; an explicit normal-order closing dateline on its own line ('Apr. 22, 1942') = src_date.",
    "src_origin":             "The SENDER's name or institution at the TOP of the source as a letterhead/header (e.g. 'St. Bonaventure's Monastery', 'SERAPHIC MASS ASSOCIATION'). vs src_signature: src_origin is the originating institution/letterhead at the TOP, never a person's sign-off at the bottom. vs src_location_sender: src_origin is the named INSTITUTION line; the street/city/state lines below it are src_location_sender (even inside a centered letterhead, a city/state/street on a SEPARATE line below the institution name splits off as src_location_sender; keep whole only when name and place are fused on the SAME physical line).",
    "src_recipient":          "The addressee's name line (e.g. 'Mrs. Margaret T. LaDoux', 'Rev. Mother Augustine'). Labeled separately only when the addressee is on its own line; if the name is embedded in the salutation it stays inside src_greeting. Never participates in connections.",
    "src_location_recipient": "The RECIPIENT's city/address line(s), directly below the recipient name in the inside-address block (e.g. 'Portland, Oregon', 'The Pines / Chatham, Ontario'). vs src_location_sender: this is the place the letter is sent TO (paired with src_recipient), whereas src_location_sender is where it is sent FROM.",
    "src_location_sender":    "The SENDER's geographic place line — city/state and/or street address from which the letter was written (e.g. 'Brooklyn, N.Y.', 'Detroit, Michigan', '1740 Mt. Elliott Avenue / Detroit, Mich.'). May be the place-half of a mixed dateline ('Yonkers, N.Y.,' split from 'Christmas Day 1913') or a stacked letterhead line below the institution name. A bare dateline place with no institution is still src_location_sender. vs src_location_recipient: sender = written FROM; recipient = sent TO.",
    "src_date":               "A date WRITTEN BY the source author — the letter's own dateline ('Christmas Day 1913', 'June 3, 1927'), an inline date opening a paragraph, a numeric shorthand ('3/20/97'), or a per-entry margin date in a notebook/journal column ('Nov. 8th', '26th', bare day numbers like '17'). vs archv_date: src_date is in NATURAL order (or a bare day/slash token), part of the source text, in the letterhead/body/margin, and CAN connect to src_content; archv_date is the archivist's normalized 'YYYY, Month DD' header floated TOP-CORNER and never connects. If a date appears both top-corner (year-first) and in-text (natural order), the top-corner is archv_date and the in-text is src_date. A ditto or repeat mark in the date column (a ditto sign, a repeat tick, or the abbreviation do.) is NOT a src_date — it means same date as the row above; do not emit a src_date polygon for it, and fold its entry into the content block governed by the date it repeats. A ditto/repeat mark emits NO polygon of its own AND never begins a new src_content span — extend the repeated date's polygon downward; do NOT open a second polygon at the ditto row and do NOT treat the whitespace before it as a span boundary. PARTIAL-DITTO EXCEPTION: when the mark replaces only the MONTH/YEAR but an EXPLICIT day number is written beside it differing from the row above (e.g. 'March 16' then 'do. 17'), that row IS a normal per-entry src_date — emit a separate src_date polygon and a separate src_content block. Fold ONLY when the entire date (month AND day) is a bare ditto with no explicit day of its own.",
    "src_greeting":           "The opening salutation line (e.g. 'My Dear Sister M. Therese:-', 'Dear Mrs. Plunkett:'). May absorb an embedded recipient name or the leading words of a mixed date+greeting line. A set-apart devotional doxology/blessing that OPENS the message ('Blessed be God in all His designs', 'God bless you ...') is src_greeting even when centered above the salutation, and a single letter may carry MORE THAN ONE src_greeting (doxology plus personal salutation), each its own polygon. POSITION DECIDES: the doxology/blessing -> src_greeting rule applies ONLY to a blessing that OPENS the message (above or at the salutation, before the body); the same phrase appearing AFTER the body and near/above the signature is the valediction -> src_farewell, not src_greeting. vs src_other: an opening blessing addressed to the reader is src_greeting; an impersonal liturgical motto is src_other. On a Mass-enrollment / Seraphic Mass Association card there is normally NO src_greeting — reserve src_greeting for an explicit salutation ('Dear ...:'); see src_content for the card role-split.",
    "src_farewell":           "The closing/valediction phrase before the signature (e.g. 'Sincerely in the Sacred Heart', 'Your Brother', 'Sincerely yours,'). When a closing/well-wishing/apologetic SENTENCE or lead-in (e.g. 'With best wishes ... I beg to remain . . .', 'With kindest regards ... I remain', 'Please pardon brevity - stolen time.', or a sentence ending in a hanging connective '...I remain' / '...I am') sits IMMEDIATELY ABOVE a short valediction line ('Sincerely in the Sacred Heart', 'Your Brother') with NO body text and NO signature between them, MERGE BOTH into ONE src_farewell polygon spanning from the closing sentence down to the valediction line — do NOT leave the closing sentence in src_content and peel off only the short valediction (a closing lead-in is part of src_farewell even when a blank line separates it from the valediction word); on a shared closing line it is the LEFT portion, split from the signature on the right. Distinct from src_signature, which is the actual name/title. A set-apart devotional blessing or doxology in the CLOSING slot — after the body and immediately above the signature — is src_farewell even when its wording reads like a doxology (e.g. 'Blessed be God in all His designs.', 'Praised be Jesus Christ.', 'God bless all.'). A short closing/well-wishing sentence, courteous word, or apologetic aside on its OWN line directly above the signature is src_farewell even with NO classic valediction word (e.g. 'Hope this helps someway.', 'Please pardon delay ....', 'Pl. pardon brevity ....', 'Congratulations.', 'God bless all.', or a trailing time-stamp aside like '10 40 p.m.'). MANDATORY: the LAST line of a letter body, sitting on its own directly above the signature and reading as an apology, time-stamp, or one-line blessing/well-wish (even when it begins with leader dots '. . . .'), is ALWAYS its own src_farewell polygon — peel it off the bottom of src_content, never fold it in. CARVE-OUTS: the peel-off applies ONLY when a src_signature (or other closing element) follows on the line below with no body between. (1) If a one-line closing blessing/doxology is the ABSOLUTE LAST line of the document with NO signature beneath it (only an archivist block follows), it stays INSIDE the preceding src_content. (2) On a page with TWO sign-offs (body signature + later P.S. signature), src_farewell is the courteous well-wish IMMEDIATELY above each signature; an apologetic aside sitting above the P.S. SENTENCE (not above a signature) is the lead-in of that P.S. src_content block, not src_farewell. A motto/doxology positioned BELOW the signature name (after the sign-off) is NOT a valediction — it stays src_other, not src_farewell.",
    "src_signature":          "The author's signature name + religious-order suffix at the close (e.g. 'Fr. Solanus Casey, O.F.M.Cap.', 'Fr. Sol.'). vs src_origin: signature is the sign-off NAME at the BOTTOM/after the body; src_origin is the sender institution at the TOP. May occur multiple times per document (after body and after postscripts); on a shared closing line it is the RIGHT portion, split from src_farewell on the left.",
    "src_other":              "Source-text elements that are not body/greeting/farewell/signature/date — e.g. an IMPERSONAL liturgical motto set apart ('PRAISED BE JESUS CHRIST!', 'DEO GRATIAS'), a Scripture/verse epigraph typographically SET APART from the body (own band, separated by whitespace, or centered like a motto) heading a DISTINCT addressed letter/document, a Latin incipit, a care-of addressing line, or a dated marginal remark by the author. POSITION OVERRIDES MOTTO applies ONLY to a liturgical motto sitting ON or ABOVE the signature line (the valediction slot) — that is src_farewell. A motto/doxology positioned BELOW the signature name (after the sign-off), e.g. a trailing 'DEO GRATIAS', 'PAX ET BONUM', 'Praised be Jesus!', or 'Praised Be The Most Blessed Sacrament!', is NOT in the valediction slot and remains src_other, never src_farewell; and a leading verse/motto/epigraph stays INSIDE the single src_content polygon whenever it heads a SINGLE-DOCUMENT continuous-prose page (one author, one body, NO greeting/recipient/dateline between it and the body) — EVEN when it is centered and separated from the first body sentence by whitespace; a mere blank-line gap on a single-prose page is NOT a set-apart band. It is src_other ONLY when (a) it heads a DISTINCT addressed letter/sub-document (a greeting, recipient, or fresh dateline follows it before the body), (b) it follows the body/signature, or (c) it is a bare standalone liturgical motto (e.g. a trailing DEO GRATIAS). vs src_greeting: an opening blessing addressed to the reader ('Blessed be God in all His designs') is src_greeting, not src_other. vs 'other': src_other is part of the SOURCE document; 'other' is the role-neutral catch-all. On a Mass-enrollment card, a third-party enrollment-attribution block naming who enrolled whom (e.g. 'special enrollment of Mrs. X by Daughter Y') is src_other, not the author's src_content body.",
    "archv_commentary":       "The archivist/transcriber's framing PROSE in the editor's voice — most often the 'The following is a faithful transcription of...' preamble, or notes like 'Here begins the notes... in his hand.' ALSO every entry of an archivist's typed CONTENTS-INVENTORY/index of a notebook or scrapbook: terse third-person descriptions of artifacts listed beside page-number markers ('Newspaper copy of poem, Wishing.', 'Typed copy of poem by his sister Margaret...', 'Names and addresses of parishioners at Sacred Heart Church...', 'Donations beside the names on pg. 4.'). These DESCRIBE what is on the pages rather than being the author's own words — each description line/block is archv_commentary, NEVER src_content (the page-number markers beside them keep their struct_* role). DECISIVE TEST: a line that names or describes a document/artifact in the THIRD PERSON ('Newspaper copy of...', 'Names and addresses of...', 'List of...') is the archivist cataloguing, not the author writing — archv_commentary. vs struct_commentary: archv_commentary describes the document's provenance/nature; struct_commentary marks an intra-document structural transition. vs archv_other: archv_commentary is descriptive sentences; archv_other is a terse parenthetical gloss or status note.",
    "archv_format_note":      "An archivist note describing the PHYSICAL MEDIUM/format or location of the original (e.g. '/POST CARD/', '[AUTOGRAPH ON BACK OF A PHOTO]', 'Photo of Fr. Solanus -', 'Inside front cover.'). Rare. Distinguished from archv_commentary by being about the artifact's form/location, not its transcription provenance.",
    "archv_date":             "The archivist's normalized cataloguing date, almost always TOP-CORNER, in 'YYYY, Month DD' (year-first, day optional — e.g. '1937, October') order or a circa estimate ('c. 1944', 'c. 1918 -'); appears once per document (so possibly >1 per multi-doc page), and a doc may have none. Usually typed, not an ink stamp. vs src_date: archv_date is archivist-supplied, year-first, top-corner, and NEVER connects; src_date is author-written, natural order, in letterhead/body/margin, and CAN connect to src_content.",
    "archv_possessor":        "The archivist's ownership/provenance block: an 'Original in possession of:' (or 'Copy of card in possession of:') lead-in plus the current owner's name and modern mailing address (present-day state + ZIP). Always typed, one polygon for the whole block, placed lower-left. Never participates in connections. Place text inside it stays in archv_possessor.",
    "archv_other":            "Short archivist annotations that are neither the transcription preamble, a format/provenance note, nor a date — e.g. 'Blank' / 'Page 2 and 3 are blank' (original-page status), '(no signature)', '(to Mr. Patrick Casey)', '(Final Printed Version)'. It also covers a terse archivist DESCRIPTION of what a specific numbered or skipped source page contains when that page is NOT a transcription of the author words (e.g. 'Writing not of Fr. Solanus! Describes the rules and dues...', a skipped-range summary, or an end-of-source status line 'END OF NOTEBOOK NO. 6.') — such a page-summary gloss beside a 'Page N' or page-range marker is archv_other (it CAN connect via archv_other<->struct_doc), NOT src_content and NOT struct_commentary. SCOPE LIMIT: this page-summary-gloss rule applies only to ONE or a FEW interspersed gloss lines describing skipped/non-transcribed pages WITHIN an otherwise transcribed notebook body; it does NOT apply to a CONTENTS / INDEX / page-listing page whose ENTIRE body is a numbered list of per-page descriptions (Pg. 1 ... Pg. N) — there, treat each per-page description as archv_commentary (one polygon per Pg. N span, connected to its struct_doc page marker; NEVER src_content — the archivist is cataloguing, not the author writing). VOICE TIEBREAKER: a gloss is archv_other ONLY in the modern transcriber/archivist third-person voice ('Writing not of Fr. Solanus!', 'Page 2 and 3 are blank'); if the entry is itself something Fr. Solanus wrote describing material he copied in, it is his own src_content. Participates in connections via the rare archv_other<->struct_doc 'Page N' edge (archv_commentary likewise connects to struct_doc on contents/index pages; other archv_* categories never connect). vs archv_commentary: archv_other is terse glosses/status notes; archv_commentary is full provenance prose.",
    "struct_id":              "A document/source IDENTIFIER or title: the catalog/notebook name and number ('FATHER SOLANUS NOTEBOOK NO. 11', 'FR. SOLANUS, NOTEBOOK NO. 5.') or a centered underlined piece-title. Appears once near the top, underlined, never connects. vs struct_doc: struct_id names the WHOLE document/notebook; struct_doc is a within-document page/section marker linked to a specific body block.",
    "struct_doc":             "A within-document structural marker that anchors a body block: a typed/marginal page or continuation number ('Page 2', 'Pg. 37.', 'Page 102 Cont.'), a notebook page label, or (in retreat/list notes) a margin section/item marker ('C.I.', 'I.', 'A)'). Sits in the left margin and is the dominant connection partner of src_content (one marker may fan out to many paragraphs). vs struct_id: struct_doc = per-page/per-section marker linked to content; struct_id = the overall notebook/catalog title (unlinked).",
    "struct_commentary":      "Rare structural/editorial note marking a transition or layout break WITHIN the document — e.g. 'On the reverse side... continues written by hand.', '(Continued on the reverse of Card)', side labels 'Front'/'Back' on a photo. vs archv_commentary: struct_commentary marks an intra-document structural junction; archv_commentary is provenance/transcription framing. CAUTION: frequently overused — apply only for genuine structural section breaks.",
    "struct_other":           "Other structural elements not covered above — e.g. a running header at the top of a continuation page (the repeated recipient name above 'Page 2'). DECISIVE RULE for continuation pages: when a recipient/addressee name is REPRINTED as a running header in the continuation-page header zone, directly above OR below a 'Page N' marker (e.g. 'Mr. Charles Bracken' one line above 'Page 2'), label it struct_other (running header), NOT src_recipient. Reserve src_recipient for an addressee name inside a genuine fresh inside-address block of an opening letter (with a sending dateline/greeting below it), never a name repeated as a continuation-page running header.",
    "other":                  "Role-neutral catch-all for content belonging to NEITHER the source author NOR the archivist — e.g. pre-printed Mass-card boilerplate ('has been enrolled in the Mass Association'), a stray editorial proofing mark, or an unclassifiable fragment. Extremely rare. vs src_other: 'other' is role-neutral leftover; src_other is a source-document flourish/element.",
}

# Edge category-pairs observed in the labeled corpus. Pass-2 edges whose
# endpoint categories fall outside this set are dropped in apply_edges()
# (the connection scheme is strict — see PASS2_SYSTEM_PROMPT). Each tuple is
# stored sorted so it compares against tuple(sorted((a_type, b_type))).
ALLOWED_EDGE_PAIRS: frozenset[tuple[str, str]] = frozenset({
    ("src_content", "struct_doc"),
    ("src_content", "src_date"),
    ("archv_other", "struct_doc"),
    ("archv_format_note", "src_content"),
    ("other", "struct_doc"),
    # David's contents/index-page convention (Iter 12, 2026-06-06): each catalog
    # description connects to its struct_doc page marker — 88 edges in gold
    # (A2 74 / V1 10 / A3 4). The pass-1 rule was flipped in Iter 12 but this
    # whitelist (and the pass-2 prompt) were missed; completed 2026-06-09
    # (EPISTEMIC_AUDIT.md finding 13).
    ("archv_commentary", "struct_doc"),
})


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


def _normalize_quad(verts: list[dict]) -> list[dict]:
    """Return a clean 4-vertex clockwise quad.

    A polygon that already has exactly 4 non-self-intersecting vertices is returned
    unchanged (preserving any legitimate slant). Anything else collapses to its
    axis-aligned bounding rectangle — the model occasionally emits an 8-vertex
    multi-part polygon (e.g. two stacked rectangles for a body split across a blank
    line), which otherwise renders as a self-intersecting "bowtie" with a stray
    diagonal. Squaring it to the bounding box yields one clean rectangle.
    """
    if len(verts) == 4:
        p = [(v["x"], v["y"]) for v in verts]

        def _ccw(a, b, c):
            return (c[1] - a[1]) * (b[0] - a[0]) > (b[1] - a[1]) * (c[0] - a[0])

        def _cross(a, b, c, d):
            return _ccw(a, c, d) != _ccw(b, c, d) and _ccw(a, b, c) != _ccw(a, b, d)

        if not (_cross(p[0], p[1], p[2], p[3]) or _cross(p[1], p[2], p[3], p[0])):
            return verts  # already a clean quad — keep its exact (possibly slanted) shape

    xs = [v["x"] for v in verts]
    ys = [v["y"] for v in verts]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    return [{"x": x0, "y": y0}, {"x": x1, "y": y0},
            {"x": x1, "y": y1}, {"x": x0, "y": y1}]


def _scale_response_to_original(
    response: DocumentsResponse,
    rendered_width:  int,    # kept for signature compat; unused now
    rendered_height: int,    # kept for signature compat; unused now
    page_width:      int,
    page_height:     int,
) -> dict:
    """Convert API response (coords in [0, 1000] normalized) back into the
    labeled_examples JSON format with coordinates in original page pixel
    space, plus locally-generated UUIDs and empty connections lists.

    Every polygon is normalized to a clean 4-vertex quad (see _normalize_quad).
    """
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
                    "vertices": _normalize_quad([
                        {"x": int(round(v["x"] * sx)),
                         "y": int(round(v["y"] * sy))}
                        for v in poly["vertices"]
                    ]),
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
    # Cache only DOWNSIZED renders (few-shot example images, reused across every
    # page). Full-res renders (~110 MB each) are used once per page; caching them
    # unbounded would OOM a 300+ page volume (52 pages already held ~5 GB).
    if image_width is not None:
        _render_cache[cache_key] = result
    return result


# ── Few-shot example selection ────────────────────────────────────────────────

# Pages excluded from the few-shot pool due to internally inconsistent or
# contradictory labels that would confuse the model. Each entry is
# "Volume_or_Appendix/page_NNN". Shrinks as gold is corrected — the label-agreement
# review (2026-06-02) rehabilitated page_002 / page_001 / page_218 after fixing the
# exact defects that had excluded them.
EXCLUDED_EXAMPLES: frozenset[str] = frozenset({
    # Mass-card greeting/recipient mislabels
    "Appendix_1/page_028",
    "Appendix_1/page_042",
    "Appendix_3/page_021",   # was page_019 before the 2026-06-05 A3 renumbering (+2)
    # ("Volume_2/page_075" — same mislabel class — left the pool entirely on
    #  2026-06-10 with the stale-seed quarantine (labeled_examples_quarantine/).
    #  If that page is ever restored to the pool, re-add it here first.)
    # Continuation-page structure merged/mislabeled
    "Volume_1/page_279",
    # (Volume_4/page_001 & page_007 were excluded for entry-merging; David re-labeled
    #  them to the per-person convention 2026-06-08, so they are now valid demos.)
    # Same-page inconsistent treatment of identical closing elements
    "Appendix_2/page_020",
    # Idiosyncratic outline/RETREAT split; unlabeled C.I.
    "Volume_1/page_250",
})


def _discover_labeled_examples() -> list[Path]:
    """Find every labeled example page directory under LABELED_EXAMPLES_DIR.

    Pages in EXCLUDED_EXAMPLES are silently skipped — their labels are
    internally inconsistent and would mislead the model if used as few-shot.
    """
    examples: list[Path] = []
    for doc_dir in sorted(LABELED_EXAMPLES_DIR.iterdir()):
        if not doc_dir.is_dir():
            continue
        for page_dir in sorted(doc_dir.glob("page_*")):
            key = f"{doc_dir.name}/{page_dir.name}"
            if key in EXCLUDED_EXAMPLES:
                continue
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


# ── Page-type classification (routes few-shot to same-TYPE demonstrations) ────
# This corpus mixes visually distinct page types WITHIN a single volume (the
# Appendices especially: formal letters + Mass-enrollment cards + dense
# notebook/ledger pages). Selecting few-shot by VOLUME alone hands a dense
# notebook page mostly letter demos — and (because notebooks are single-doc) the
# multi-doc quota then fills the rest with letters/cards too, so the page that
# most needs notebook demonstrations sees the fewest. Typing each page and routing
# same-type demos in front of the model fixes both. Examples are typed offline
# from their gold labels (deterministic); a target page is typed by one cheap call.

PAGE_TYPES: tuple[str, ...] = ("letter", "mass_card", "notebook", "other")
# Only these STRONG types trigger type-routing. "other" is a grab-bag (title pages,
# preambles, sparse/odd pages) AND the classifier's fallback when it's unsure — so a
# page typed "other" routes by the original same-VOLUME selection instead, which can
# never regress a page below its pre-page-type behavior. (Measured: the classifier is
# perfect on the dense notebook pages that matter; its misses cluster on front-half
# letter/preamble pages, which this fallback sends right back to same-volume demos.)
STRONG_PAGE_TYPES: frozenset[str] = frozenset({"letter", "mass_card", "notebook"})
# Page types that can legitimately carry 2+ documents (so the multi-doc quota is
# worth spending slots on). Notebook/other pages are single-document — forcing
# multi-doc demos on them only crowds out same-type examples.
MULTI_DOC_PAGE_TYPES: frozenset[str] = frozenset({"letter", "mass_card"})


class PageTypeResponse(BaseModel):
    page_type: Literal["letter", "mass_card", "notebook", "other"]


def _example_category_counts(data: dict) -> dict[str, int]:
    cc: dict[str, int] = {}
    for doc in data.get("documents", {}).values():
        if isinstance(doc, dict):
            for cat, polys in doc.items():
                if isinstance(polys, list):
                    cc[cat] = cc.get(cat, 0) + sum(1 for b in polys if b.get("vertices"))
    return cc


def infer_page_type_from_labels(data: dict) -> str:
    """Deterministically type a LABELED example from its gold structure.

    notebook  = dense multi-entry (page markers / margin-date column / many bodies)
    mass_card = enrollment card(s): recipient + signature, no salutation, no letterhead
    letter    = salutation and/or letterhead with a sign-off
    other     = title / preamble / index / sparse
    """
    cc = _example_category_counts(data)
    nd = data.get("num_documents", 1) or 1
    g = lambda k: cc.get(k, 0)
    content, sdate = g("src_content"), g("src_date")
    # NOTEBOOK / JOURNAL / LEDGER / INDEX — the dense multi-entry structure
    if content >= 3 or sdate >= 3 or (g("struct_doc") and content >= 2):
        return "notebook"
    # MASS-ENROLLMENT CARD — recipient + signature, no salutation, no letterhead
    if g("src_recipient") and g("src_signature") and not g("src_greeting") \
       and not g("src_origin") and not g("src_location_sender"):
        return "mass_card"
    if nd >= 2 and g("src_recipient") and g("src_signature") \
       and not g("struct_doc") and not g("src_origin"):
        return "mass_card"
    # FORMAL LETTER — salutation and/or letterhead + a sign-off
    if g("src_greeting") or g("src_origin") or g("src_location_sender") \
       or g("src_signature") or g("src_farewell"):
        return "letter"
    return "other"


def classify_examples_by_type(all_examples: list[Path]) -> dict[Path, str]:
    """Type every few-shot example from its gold labels (offline, deterministic)."""
    out: dict[Path, str] = {}
    for page_dir in all_examples:
        try:
            data = json.load(open(page_dir / f"{page_dir.name}.json"))
            out[page_dir] = infer_page_type_from_labels(data)
        except Exception:
            out[page_dir] = "other"
    return out


PAGE_TYPE_PROMPT = """\
Classify this scanned archival page into EXACTLY ONE type by the kind of DOCUMENT it carries. Most pages here are transcriptions: a typed archivist date in a top corner and short framing notes ("The following is a faithful transcription...", "Original in possession of...") commonly WRAP the real document — IGNORE that archival framing and classify by the document inside it.

- "letter": a formal letter — a letterhead or place/date line near the top, a salutation ("Dear ___"), one or more body paragraphs, and a signature. Pick this whenever the page's main content is a letter, EVEN when archivist notes frame it. One (occasionally two) letters.
- "mass_card": one or more Seraphic Mass Association ENROLLMENT CARDS — each short: a person's name, a one-line intention/dedication, a feast or closing date, and a "Fr. Solanus" signature. No real body paragraphs, no salutation. Often several stacked on one page.
- "notebook": a dense NOTEBOOK / JOURNAL / LEDGER page — a left-hand column of short dates and/or "Page N" markers, with many short dated entries running down the page (sometimes a money/donation ledger with amount columns, or a numbered list / contents index).
- "other": ONLY when there is no letter, card, or notebook content at all — a pure title/cover page, a stand-alone preamble page with no transcribed document beneath it, or a (near-)blank page. When in doubt between "other" and one of the three document types, choose the document type.

Return JSON: {"page_type": "<letter|mass_card|notebook|other>"}.
"""


def classify_page_type(
    client, model_name: str, pdf_path: Path,
    cache_dir: Path | None = None, image_width: int = 768,
) -> tuple[str, int, int]:
    """Type the TARGET page (no labels yet) with one cheap VLM call. Cached to disk.

    Returns (page_type, input_tokens, output_tokens) — tokens 0 on cache hit/failure.
    """
    cache = (cache_dir / f"{pdf_path.stem}.json") if cache_dir else None
    if cache and cache.exists():
        try:
            return json.load(open(cache)).get("page_type", "other"), 0, 0
        except Exception:
            pass
    img = render_page(pdf_path, image_width)[0]
    in_tok = out_tok = 0
    try:
        resp = client.models.generate_content(
            model    = model_name,
            contents = [PAGE_TYPE_PROMPT, img, "Return only the JSON object for THIS page."],
            config   = types.GenerateContentConfig(
                response_mime_type = "application/json",
                response_schema    = PageTypeResponse,
                temperature        = 0.0,
            ),
        )
        pt = PageTypeResponse.model_validate_json((resp.text or "").strip()).page_type
        usage = getattr(resp, "usage_metadata", None)
        in_tok  = int(getattr(usage, "prompt_token_count",     0) or 0)
        out_tok = int(getattr(usage, "candidates_token_count", 0) or 0)
    except Exception as exc:
        log.warning("  page-type classify failed (%s); defaulting 'other'", exc)
        pt = "other"
    if cache:
        cache.parent.mkdir(parents=True, exist_ok=True)
        json.dump({"page_type": pt}, open(cache, "w"))
    return pt, in_tok, out_tok


_layout_desc_cache: dict[str, object] = {}


def _example_pdf(page_dir: Path) -> Path | None:
    return next(page_dir.glob("*.pdf"), None)


def _layout_descriptor(pdf_path: Path | None):
    """Cheap layout fingerprint: a 12x16 ink-density grid of the page, L2-normalized.
    Used to rank few-shot demos by how much their page layout resembles the target —
    beats random-within-type (validated 2026-06-08: PQ 0.639->0.681, recall 0.676->0.717,
    experiments/fewshot_ab.py). Cached by path; returns None if it can't render."""
    if pdf_path is None:
        return None
    import numpy as np
    key = str(pdf_path)
    if key in _layout_desc_cache:
        return _layout_desc_cache[key]
    try:
        img = render_page(pdf_path, 256)[0].convert("L")
        thr = _otsu_threshold(img.histogram()[:256])
        a = (np.asarray(img) < thr).astype("float32")
        h, w = a.shape
        rows, cols = 12, 16
        g = np.zeros((rows, cols), "float32")
        for r in range(rows):
            for c in range(cols):
                g[r, c] = a[r * h // rows:(r + 1) * h // rows, c * w // cols:(c + 1) * w // cols].mean()
        v = g.flatten()
        n = float((v * v).sum()) ** 0.5
        v = v / n if n > 0 else v
    except Exception:
        v = None
    _layout_desc_cache[key] = v
    return v


def select_few_shot(
    all_examples:   list[Path],
    multi_doc_set:  set[Path],
    target_doc:     str,
    num_fewshot:    int,
    min_multi_doc:  int,
    rng:            random.Random,
    target_page:    str | None = None,
    target_type:    str | None = None,
    type_of:        dict[Path, str] | None = None,
    target_desc:    object = None,            # layout fingerprint of the TARGET page
    desc_cache:     dict | None = None,       # {example page_dir: layout fingerprint}
    pinned:         list | None = None,       # examples ALWAYS included (minus the target itself)
) -> list[Path]:
    """Pick few-shot examples biased toward (in priority order):
      1. Same PAGE TYPE as the target (when target_type/type_of are given) —
         puts the right segmentation granularity in front of the model.
      2. Same volume as target_doc (visual style match).
      3. At least `min_multi_doc` MULTI-doc examples (so the model still emits
         doc_2/doc_3 on multi-letter / multi-card pages) — but this quota is
         relaxed for single-document page types (notebook/other) so it can't
         crowd out same-type demos.

    With target_type=None the behavior is the original same-volume + multi-doc
    selection (unchanged), so callers that don't classify pages are unaffected.
    """
    # No leakage: hold out the exact page being labeled from its own few-shot set.
    if target_page is not None:
        all_examples = [p for p in all_examples
                        if not (p.parent.name == target_doc and p.name == target_page)]

    # Pinned examples are ALWAYS included (minus the target page), at the front; the
    # normal selection fills the remaining slots from the rest of the pool.
    forced: list = []
    if pinned:
        pin_keys = {(p.parent.name, p.name) for p in pinned}
        if target_page is not None:
            pin_keys.discard((target_doc, target_page))
        forced = [p for p in all_examples if (p.parent.name, p.name) in pin_keys]
        all_examples = [p for p in all_examples if (p.parent.name, p.name) not in pin_keys]
        num_fewshot = max(0, num_fewshot - len(forced))

    # Order a candidate list: by LAYOUT SIMILARITY to the target when fingerprints are
    # available (the validated win), else random by seed (original behaviour / tests).
    def _order(lst: list) -> None:
        if target_desc is not None and desc_cache:
            import numpy as np
            def _sim(p):
                d = desc_cache.get(p)
                return float(np.dot(d, target_desc)) if d is not None else -1.0
            lst.sort(key=_sim, reverse=True)
        else:
            rng.shuffle(lst)

    # ── Original same-volume path (no page typing, or an unsure "other") ──────
    if target_type is None or target_type not in STRONG_PAGE_TYPES:
        same_multi   = [p for p in all_examples if p.parent.name == target_doc and p in multi_doc_set]
        same_single  = [p for p in all_examples if p.parent.name == target_doc and p not in multi_doc_set]
        other_multi  = [p for p in all_examples if p.parent.name != target_doc and p in multi_doc_set]
        other_single = [p for p in all_examples if p.parent.name != target_doc and p not in multi_doc_set]
        for lst in (same_multi, same_single, other_multi, other_single):
            _order(lst)
        picks: list[Path] = []
        multi_target = min(min_multi_doc, num_fewshot, len(same_multi) + len(other_multi))
        for pool in (same_multi, other_multi):
            while len(picks) < multi_target and pool:
                picks.append(pool.pop(0))
        fill_order = (same_single, same_multi, other_single, other_multi)
        while len(picks) < num_fewshot and any(fill_order):
            for pool in fill_order:
                if pool and len(picks) < num_fewshot:
                    picks.append(pool.pop(0))
        return forced + picks

    # ── Page-type-aware path ──────────────────────────────────────────────────
    type_of = type_of or {}
    same_type = lambda p: type_of.get(p) == target_type
    same_vol  = lambda p: p.parent.name == target_doc
    # priority bucket: same-type & same-vol > same-type > same-vol > rest
    def bucket(p) -> int:
        st, sv = same_type(p), same_vol(p)
        return 0 if (st and sv) else 1 if st else 2 if sv else 3
    pools: dict[int, list[Path]] = {0: [], 1: [], 2: [], 3: []}
    for p in all_examples:
        pools[bucket(p)].append(p)
    for lst in pools.values():
        _order(lst)

    # Single-document page types don't need multi-doc demos — relax the quota so
    # same-type examples aren't displaced.
    eff_min_multi = min_multi_doc if target_type in MULTI_DOC_PAGE_TYPES else min(1, min_multi_doc)

    picks: list[Path] = []
    picked: set[Path] = set()
    # Multi-doc quota first, preferring higher-priority (same-type) buckets.
    multi_avail = [p for b in (0, 1, 2, 3) for p in pools[b] if p in multi_doc_set]
    quota = min(eff_min_multi, num_fewshot, len(multi_avail))
    for p in multi_avail:
        if len(picks) >= quota:
            break
        picks.append(p); picked.add(p)
    # Fill remaining slots strictly by priority bucket (same-type leads).
    for b in (0, 1, 2, 3):
        for p in pools[b]:
            if len(picks) >= num_fewshot:
                break
            if p not in picked:
                picks.append(p); picked.add(p)
        if len(picks) >= num_fewshot:
            break
    return forced + picks[:num_fewshot]


# ── Prompt construction ───────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are labeling regions in scanned archival document pages.

Each region is a polygon (4 vertices forming a quadrilateral) covering a contiguous block of content on the page, tagged with one of these categories:

{categories}

CRITICAL — count documents FIRST:
Pages often contain MULTIPLE distinct documents (1, 2, or even 3 letters / notes on the same physical page), but MOST pages hold exactly ONE. Default to 1 document. Before assigning any labels, scan the entire page top-to-bottom and count the distinct documents.

A new document begins only when a prior message has CLOSED and a new one OPENS. Increment the count when a CLUSTER of these strong signals appears together:
  - A NEW archivist date header introducing the next block (typed, top-of-block, year-first like "1953, January 7"). Strongest signal in this corpus.
  - A fresh "The following is a faithful transcription of..." provenance note starting a new block (one per transcribed document).
  - A complete farewell+signature pair followed below by a NEW message — INCLUDING a later-dated block that carries ITS OWN signature (e.g. a "December 29th" resumption that is itself signed is a new document even with no new greeting or recipient).
  - A new self-contained recipient + date + body + signature set (e.g. 4 Mass-card enrollments = 4 docs).
  - A grid / 2x2 layout of parallel drafts (each quadrant a doc).
  - A large vertical whitespace gap between two complete message blocks (corroborating only — never split on whitespace alone).

Do NOT start a new document for:
  - A postscript / "P.S." / later-dated note that sits under the same signature and has NO signature of its own.
  - A "Page 2/3", "...Cont.", or "Continued on the reverse" continuation — keep it ONE document even if the recipient name is reprinted in a running header.
  - A new paragraph, an embedded poem/quotation, or a new feast/section heading.
  - A new "Pg. N" notebook page marker. An ENTIRE notebook / journal / ledger page is ONE document regardless of how many source "Pg. N" sections, dates, or itemized entries it contains.

Each distinct document gets its OWN entry: doc_1, doc_2, doc_3. Never lump two separate letters into one doc — labels then drift into the empty gap between them. Conversely, never split one continued letter or one notebook page into multiple docs.

HEADER / DATELINE SEGMENTATION — split mixed lines by ROLE, not by line:
Within a letter's header zone, draw one polygon per role: sending institution name -> src_origin; sender city/state or street -> src_location_sender; the writing date -> src_date; addressee name -> src_recipient; addressee place -> src_location_recipient; salutation -> src_greeting.

1. WHEN ONE PHYSICAL LINE CONTAINS A PLACE FOLLOWED BY A DATE (e.g. "Yonkers, N. Y., Christmas Day 1913" or "Brooklyn, N.Y., August 18th, 1915"), emit TWO polygons on the SAME horizontal band:
   - LEFT polygon = the place tokens, ending at the comma after the state -> src_location_sender (text: "Yonkers, N. Y.,")
   - RIGHT polygon = everything after that comma -> src_date (text: "Christmas Day 1913")
   The two boxes are horizontally adjacent, share the same y-range, and must NOT overlap. CUT POINT: the location keeps BOTH the city AND the state/abbreviation together (e.g. "Yonkers, N. Y.,", "Brooklyn, N.Y.,", "Detroit, Mich.,"); cut at the comma AFTER the state, NOT after the city. The state ("N.Y.", "Mich.", "Ind.") belongs to src_location_sender, never to src_date — the date box begins at the month / day / feast word ("January 3rd", "Holy Thursday", "August 18th") or a slash-date token ("16/7/18"). Each box must tightly bound its own visible words. NO-COMMA CASE: split even when only a SPACE (no comma) separates the place from the date, e.g. "Yonkers N.Y. 16/7/18" -> src_location_sender "Yonkers N.Y." + src_date "16/7/18". The place keeps the city+state; any trailing time-reference becomes src_date.

2. A DATE IS ANY TIME-REFERENCE, even with no digits and no month name. Treat ALL of these as src_date (the right-hand polygon):
   - Holidays / feasts / liturgical times: "Christmas Day 1913", "Easter 1913", "Joyful Easter", "Holy Thursday", "Feast of the Presentation", "Epiphany", "Eastertide", "Festa SSi Rosarii", "New Years"
   - Year-only or day-only: "1913", "June 26", "Jan. 18th", "26th", "the 25"
   - Numeric shorthand: "3/20/97" and other slash dates in EITHER M/D/YY or day-first D/M/YY order — e.g. "16/7/18" (= 16 July 1918); a leading number greater than 12 just means it is day-first, it is STILL a date (these are the author's, not the archivist's)
   - Date + time: "10 A.M. Dec. 27, 1927"
   If a place is immediately followed by ANY such reference on one line, split it off as src_date. Do not require a standard numeric date to trigger the split.

3. WHEN PLACE AND DATE ARE ON SEPARATE PHYSICAL LINES, do NOT cut within a line — label each line by its role top-to-bottom. Example: "St. Bonaventure's Monastery" -> src_origin, "1740 Mt. Elliott Avenue / Detroit, Mich." -> src_location_sender, "June 3, 1927" -> src_date. A bare city/state dateline with NO institution (e.g. "Detroit, Michigan") is still src_location_sender — src_origin may be absent.

4. LETTERHEAD = src_origin (the NAME) + src_location_sender (the ADDRESS) — they are DIFFERENT regions, split them. The institution/order NAME is src_origin: keep the whole name in ONE box even when it spans several stylized lines, including glue words ("and"/"&") and a devotional cross ("+") — e.g. "St. Michael's Church / and / Capuchin Monastery" is ONE src_origin; do NOT emit one polygon per name line. The ADDRESS lines printed BELOW the name — street number/name, city, state (e.g. "1740 Mt. Elliott Ave. / Detroit, Mich.", "225 Jerome Street.", "Shonnard Place") — are NOT part of src_origin: split them off into a SEPARATE src_location_sender box. So a stacked letterhead becomes NAME -> src_origin, ADDRESS lines -> src_location_sender, and any dateline date -> src_date (exactly as in rule 3). When the institution name and its place sit FUSED on a SINGLE line, split that line by role too: the named institution -> src_origin (left), the city/state/street -> src_location_sender (right). src_location_sender therefore covers BOTH the letterhead address AND the place on a dateline before the date (e.g. "Yonkers, N.Y.," in "Yonkers, N.Y., Christmas Day 1913"; see rule 1).

5. ARCHIVIST DATE vs SOURCE DATE — the decisive signal is the archivist date's TOP-CORNER position together with year-first or circa form, NOT mere right/left alignment:
   - A TOP-CORNER date in YEAR-FIRST order ("1913, December 25", "1924, January 25", "1937, October" — day optional) or a circa estimate ("c. 1944", "c. 1918 -") is the archivist's header -> archv_date. It appears on nearly every page (typed), once per document. Do NOT split a location off it and do NOT treat it as src_date. FORM IS DISPOSITIVE: a date in year-first "YYYY, Month [DD]" order or circa form is archv_date EVEN when it floats mid-page (a centered or right-margin section divider between notebook sections, e.g. "1934, January", "1917, November") rather than in the top corner, and it NEVER connects. (Exception: on a page that already has a top-corner archv_date, a SECOND lower year-first date heading each per-enrollment / Mass-card block remains src_date. This affects only archv_date-vs-src_date classification; Mass-card src_dates are closing/feast datelines and remain UNLINKED.)
   - Any date that is the letter's own dateline, an inline date opening a paragraph, or a per-entry margin date in a notebook (left OR right margin, e.g. "3/20/97" beside an entry) is src_date. Right-margin placement alone does NOT make a date the archivist's.

6. EDGE CASES:
   - A date-only line with no place -> single src_date polygon; do not hallucinate a src_location_sender.
   - A recipient address block (e.g. "Dease Lake / British Col. / Canada") is src_location_recipient; the addressee NAME on its own line is src_recipient — split name from place when they are on separate lines.
   - In notebooks/journals, place names embedded inside an entry stay inside src_content; only letterhead/dateline zones get the location+date split.
   - Place text inside an archv_possessor block ("Sacramento, CA. 95817") stays in archv_possessor; never relabel it as a location.
   - A set-apart devotional doxology/blessing that OPENS the message ("Blessed be God in all His designs") is src_greeting, not src_other — even when centered or offset above the salutation. A single letter may carry MORE THAN ONE src_greeting (an opening doxology plus a personal salutation), each its own polygon. Emit SEPARATE src_greeting polygons only when the opening blessing is clearly SET APART on its own physical line/band (typically centered above the salutation); when the blessing and the personal salutation share the same physical line, or sit on directly adjacent lines with no body text between them, keep them in ONE polygon.

STRUCTURE vs CONTENT:
  - struct_doc = a typed left-margin page/section pointer ("Pg. 24", "Page 2", "Page 102 Cont.", page ranges, or outline markers "C.I."/"I."/"A)"). It sits in the far-left margin, left of the body column, aligned with the first line of the block it labels.
  - struct_id = the underlined document/notebook title ("FR. SOLANUS, NOTEBOOK NO. 5."), near the top, once per page.
  - Numbers INSIDE the body column (verse numbers, "#608", "4)") and underlined section sub-headings within the body stay in src_content, not struct_doc / struct_id.
  - SINGLE-DOCUMENT PROSE PAGES (letters / transcriptions): emit exactly ONE src_content polygon spanning the entire contiguous body — all paragraphs, embedded verse/quotation, inline reference numbers, a leading centered motto/epigraph that runs into the body, and underlined in-body sub-headings. Per-block src_content polygons are ONLY for multi-entry notebook/journal/ledger pages. This ONE box includes a leading centered verse, hymn, or Scripture epigraph that opens the body even when a blank line or whitespace gap separates the epigraph from the first body sentence — fold it in as the first lines. Whitespace separation ALONE never promotes a same-document leading epigraph to src_other; reserve src_other only for an epigraph/motto that heads a DISTINCT addressed letter/sub-document (its own greeting/recipient/dateline follows) or sits after the body and signature.
  - NOTEBOOK / JOURNAL / LEDGER PAGES: draw ONE src_content polygon per page-marker/date span (per struct_doc "Page N" and per left-margin/inline margin-date), never one per entry or per line.
  - NOTEBOOK DATE-COLUMN TEST: a date is a per-entry src_date span-anchor ONLY when it sits in the dedicated FAR-LEFT margin date column, LEFT of the body text (e.g. "Nov. 26", "29", "Dec. 8th"). A date written at the SAME left edge as the body (a sentence-style heading inside the body column, e.g. "For December 1st 1937"), a feast/holiday word inside the body column, or a date appended to the RIGHT END of an entry title/header line, is inline body text -> keep it INSIDE the surrounding src_content polygon and do NOT emit a separate src_date or start a new span. Reserve the inline-date-opening-a-paragraph -> src_date rule for letter bodies, not notebook body-column section headings.

Coordinates MUST be NORMALIZED to the range [0, 1000] for BOTH x and y, regardless of the input image's actual pixel dimensions. x=0 is the left edge of the page, x=1000 is the right edge; y=0 is the top, y=1000 is the bottom. So x=500, y=500 means horizontally and vertically centered. Vertices should go roughly clockwise from the top-left of each region. Use EXACTLY 4 vertices per region (a quadrilateral). If a block's paragraphs are separated by a blank line, still enclose the whole block in ONE 4-corner rectangle (it may include the gap) — never emit more than 4 vertices or an L-shaped / multi-part polygon.

TIGHT BOUNDING — hug the ink, leave no slack: each polygon's TOP edge sits just above the tallest letters of the block's first line, its BOTTOM edge just below the lowest descenders of the last line, and its LEFT/RIGHT edges at the outermost strokes. Do NOT pad a block with blank margin, and do NOT let ANY glyph fall outside the polygon — every character you are bounding must lie inside it (clipping off the first/last letters or the top/bottom line is the most common error). If a block is slanted or skewed, tilt the quadrilateral to follow the baseline rather than drawing a loose upright box around it.

COMPLETENESS — leave no written text unlabeled: before finishing, scan the page once more and confirm EVERY line of writing falls inside some polygon. Text that is commonly missed and that you MUST capture: a postscript's wrapped SECOND / THIRD lines (extend the P.S. box to cover the whole postscript, not just its first line), a closing blessing or well-wish standing on its own line ("God bless you all!", "Praised be Jesus Christ!"), a short line added later in a different hand, and any line stranded between blocks. Give each its best-fit category — extend the adjacent same-category polygon when the text is a continuation of it, rather than emitting a sliver box; use "other" only when no category fits. Do NOT box non-text marks (ink stamps, library / call numbers, punch holes, stray specks) and do NOT split a coherent block merely to cover a trailing line.

Below are several example pages with their correct labels, followed by a new page you must label using the same schema.
"""


# Per-volume prompt addenda appended to the system prompt when labeling that volume.
# Use sparingly — for a convention that legitimately DIFFERS from the general rules.
#
# DAVID-EDITABLE FILE OVERRIDE (staged HITL flow, 2026-06-09): if
# volume_notes/<Volume>.md exists and is non-empty, its text REPLACES the python
# constant below for that volume — so David edits a plain text file, never code.
# Optional per-cluster addenda: volume_notes/<Volume>.cluster_<N>.md is appended
# for pages that qa_output/<Volume>/clusters.json maps to cluster N.
VOLUME_NOTES_DIR = SCRIPT_DIR / "volume_notes"
_EXTRA_NOTE = ""   # one-run prompt addendum set by --extra-note (or --extra-note-file)

VOLUME_PROMPT_NOTES = {
    "Volume_4": (
        "\n\nVOLUME-SPECIFIC OVERRIDE (this page is from Volume_4 — READ CAREFULLY, it changes the "
        "content-granularity rules): Volume_4 is Fr. Solanus Casey's casebook about INDIVIDUAL "
        "PEOPLE — each journal entry records ONE person: their name and details about them (age, "
        "ailment/condition, intention, what happened, sometimes a later outcome note). A typical "
        "entry reads like 'FirstName LastName - 63 - stroke ... on way to Florida' or 'Mrs. X - 42 "
        "- ... operation revealed ...'; an entry may run several lines, and not every entry follows "
        "the exact same surface pattern — segment by MEANING: a new entry begins where a NEW PERSON "
        "is introduced. Emit exactly ONE src_content polygon PER ENTRY (per person): keep ALL lines "
        "of one person's note together in that single box (continuation lines, outcome notes, "
        "details), and NEVER put two different people in the same box — even consecutive short "
        "entries, even within the same date-span or page-section. This OVERRIDES the general "
        "notebook rule of merging a whole date-span into one generous polygon: here the unit is "
        "the PERSON/ENTRY, not the date-span. Do NOT split one person's entry into multiple boxes "
        "by line or by sentence. Dates: emit src_date ONLY for dates explicitly written on the "
        "page (margin dates like 'Nov. 24', 'July 13'); NEVER infer, assume, or invent a date. "
        "struct_doc page markers ('Page 7', 'Page 8 Cont.') and archivist material are labeled "
        "per the normal rules. The few-shot examples are all from this volume and were "
        "hand-labeled to this exact convention — match their granularity precisely."
    ),
}


_volume_note_cache: dict[str, str] = {}
_page_cluster_cache: dict[str, dict] = {}


def load_volume_note(doc_name: str, page_name: str | None = None) -> str:
    """Per-volume convention note for the prompt, David-editable on disk.

    Precedence: volume_notes/<doc_name>.md (David's file) > VOLUME_PROMPT_NOTES
    (legacy python constant). With a page_name and a clusters.json for the
    volume, a matching volume_notes/<doc_name>.cluster_<N>.md is appended.
    """
    if doc_name not in _volume_note_cache:
        note = VOLUME_PROMPT_NOTES.get(doc_name, "")
        f = VOLUME_NOTES_DIR / f"{doc_name}.md"
        if f.exists():
            text = f.read_text().strip()
            if text:
                note = (f"\n\nVOLUME-SPECIFIC NOTE (this page is from {doc_name}; "
                        f"these conventions override the general rules where they "
                        f"conflict):\n{text}")
        _volume_note_cache[doc_name] = note
    note = _volume_note_cache[doc_name]

    if page_name:
        if doc_name not in _page_cluster_cache:
            cpath = SCRIPT_DIR / "qa_output" / doc_name / "clusters.json"
            mapping = {}
            if cpath.exists():
                try:
                    mapping = json.loads(cpath.read_text()).get("page_to_cluster", {})
                except Exception:
                    mapping = {}
            _page_cluster_cache[doc_name] = mapping
        c = _page_cluster_cache[doc_name].get(page_name)
        if c is not None:
            cf = VOLUME_NOTES_DIR / f"{doc_name}.cluster_{c}.md"
            if cf.exists():
                text = cf.read_text().strip()
                if text:
                    note += (f"\n\nPAGE-GROUP NOTE (this page belongs to a visual "
                             f"group of {doc_name} with its own convention):\n{text}")
    if _EXTRA_NOTE:
        note += f"\n\n{_EXTRA_NOTE}"
    return note


def build_prompt_parts(
    target_image: Image.Image,
    fewshot_pairs: list[tuple[Image.Image, dict]],
    volume_note: str = "",
    contrast_pairs: list | None = None,
) -> list:
    """Build the multipart list passed to model.generate_content().

    Each few-shot pair contributes an EXAMPLE i header, image, labels JSON.
    `contrast_pairs` (optional, C-ICL-style — David's diff idea, HITL §6.1)
    additionally shows the SAME page twice: the model's earlier INCORRECT
    attempt next to the human-corrected gold, so the model sees its own
    failure mode on this volume. Target image is sent last. `volume_note` is
    an optional per-volume convention addendum appended to the system prompt.
    """
    categories_block = "\n".join(
        f"  - {cat}: {CATEGORY_DESCRIPTIONS[cat]}" for cat in CATEGORIES
    )
    parts: list = [SYSTEM_PROMPT.format(categories=categories_block) + volume_note]
    for i, (img, payload) in enumerate(fewshot_pairs, start=1):
        parts.append(f"EXAMPLE {i}:")
        parts.append(img)
        parts.append("Labels:\n" + json.dumps(payload, separators=(",", ":")))
    for j, cp in enumerate(contrast_pairs or [], start=1):
        parts.append(
            f"COMMON MISTAKE {j} — the SAME page labeled twice: first the model's "
            f"earlier INCORRECT attempt on this volume, then the CORRECT human-fixed "
            f"labels. Study the difference and do NOT repeat the mistake:")
        parts.append(cp["image"])
        parts.append("INCORRECT (model's attempt):\n"
                     + json.dumps(cp["wrong"], separators=(",", ":")))
        parts.append("CORRECT (human gold):\n"
                     + json.dumps(cp["right"], separators=(",", ":")))
    parts.append("PAGE TO LABEL:")
    parts.append(target_image)
    parts.append(
        "Return labels for this page in the same JSON schema as the examples. "
        "Use the same coordinate frame as the input image."
    )
    return parts


def build_contrast_pairs(doc_name: str, n: int, image_width: int | None) -> list:
    """Collect ALL correction-bearing (auto vs gold) candidate pairs for
    `doc_name` from the pool — pages whose human-corrected pool copy differs
    from the model's original auto_labeled attempt — each with its layout
    descriptor attached. The per-target selection (which n of these a given
    page actually sees) happens inside process_page: the MOST LAYOUT-SIMILAR
    corrected pages win, so every page is shown the mistakes made on pages
    that look like it (`n` only logs intent here). Image loads from the Files
    API when uploaded (same object as the demo), else inline render."""
    scored = []
    for page_dir in sorted((LABELED_EXAMPLES_DIR / doc_name).glob("page_*")):
        gold_p = page_dir / f"{page_dir.name}.json"
        auto_p = AUTO_LABELED_DIR / doc_name / page_dir.name / f"{page_dir.name}.json"
        if not (gold_p.exists() and auto_p.exists()):
            continue
        try:
            gold = json.load(open(gold_p))
            auto = json.load(open(auto_p))
        except Exception:
            continue
        g_ids = {p["id"]: (cat, p["vertices"]) for d in gold.get("documents", {}).values()
                 if isinstance(d, dict) for cat, ps in d.items() if isinstance(ps, list)
                 for p in ps if isinstance(p, dict) and "id" in p}
        a_ids = {p["id"]: (cat, p["vertices"]) for d in auto.get("documents", {}).values()
                 if isinstance(d, dict) for cat, ps in d.items() if isinstance(ps, list)
                 for p in ps if isinstance(p, dict) and "id" in p}
        added   = len(set(g_ids) - set(a_ids))
        removed = len(set(a_ids) - set(g_ids))
        changed = sum(1 for i in set(g_ids) & set(a_ids) if g_ids[i] != a_ids[i])
        score = 3 * (added + removed) + changed
        if score > 0:
            scored.append((score, page_dir, auto, gold))
    scored.sort(key=lambda t: (-t[0], t[1].name))
    pairs = []
    for score, page_dir, auto, gold in scored:
        img = _get_uploaded_file(page_dir)
        if img is None:
            pdf = _example_pdf(page_dir)
            if pdf is None:
                continue
            img = render_page(pdf, image_width)[0]
        pairs.append({
            "key": f"{doc_name}/{page_dir.name}",
            "image": img,
            "wrong": _scale_regions_to_render(auto, 0, 0),
            "right": _scale_regions_to_render(gold, 0, 0),
            "score": score,
            "desc": _layout_descriptor(_example_pdf(page_dir)),
        })
    log.info("  contrast candidates: %d corrected page(s); each target gets its "
             "%d most layout-similar.", len(pairs), n)
    return pairs


def _pick_contrast_for_target(candidates: list, target_key: str,
                              target_desc, n: int) -> list:
    """The n candidates most layout-similar to the target page (never itself);
    falls back to correction-size order when descriptors are unavailable."""
    cands = [c for c in candidates if c["key"] != target_key]
    if target_desc is not None:
        import numpy as np
        cands.sort(key=lambda c: (float(np.dot(c["desc"], target_desc))
                                  if c.get("desc") is not None else -2.0),
                   reverse=True)
    else:
        cands.sort(key=lambda c: -c["score"])
    return cands[:n]


# ── Gemini call ───────────────────────────────────────────────────────────────


class TransientAPIError(Exception):
    """Raised for errors that should trigger a retry."""


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    stop=stop_after_attempt(6),
    wait=wait_random_exponential(multiplier=2, max=60),
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
# compact text table of the polygons (short ids + bboxes) plus the page image,
# with few-shot examples that each pair an image with its polygon table and the
# correct edges — connections are only decidable from the visual layout.
# The scheme is highly stylized. Edge types observed across the labeled corpus:
#   src_content<->struct_doc 164, src_content<->src_date 131,
#   archv_other<->struct_doc 8, archv_format_note<->src_content 1,
#   other<->struct_doc 1.  (apply_edges drops anything outside this set.)

PASS2_SYSTEM_PROMPT = """\
You are deciding which labeled polygons on a scanned archival page LINK (connect) to each other. Polygons have already been detected and classified. Each has a short id (p1, p2, ...), a category, text, and a bounding box, grouped by document. Decide which polygons should be linked with a bidirectional connection.

FIRST decide the page TYPE — it governs everything:
- NOTEBOOK / JOURNAL / LEDGER / retreat-notes / multi-entry pages: connections are EXPECTED. Left-margin page numbers (struct_doc) and left-margin or inline per-entry dates (src_date) anchor body blocks, often one marker/date fanning out to several blocks.
- FORMAL LETTER (greeting + body + signature, with a letterhead dateline): connections are RARE. The dateline is a formal header above the body and usually does NOT connect — most formal letters have ZERO edges. When unsure, prefer NO edge.
- FORMAL-LETTER "Page N" EXCEPTION: a "Page 2" / "...Cont." continuation marker at the TOP of a letter or continuous-prose page (above the body, often beside a reprinted recipient running header) is a page HEADER, not a notebook anchor — it does NOT connect. A connecting struct_doc must sit in the FAR-LEFT margin LEVEL WITH the first body line of a genuine MULTI-ENTRY notebook/journal/ledger. A single "Page N" header above one continuous body (one subject, one signature, even with an embedded numbered list/recipe) is NOT multi-entry: return ZERO edges.
- MASS-CARD / ENROLLMENT-CARD PAGES ARE NOT NOTEBOOKS: a Seraphic Mass Association enrollment card (recipient name + intention + a feast/closing dateline + "Fr. Solanus" signature) is a short formal note, not a notebook/journal/ledger. Its dates are formal datelines / closing dates, NOT per-entry margin dates, and do NOT connect. A page composed wholly of such enrollment cards returns an EMPTY edges list, even when several cards appear on one page.

EDGE INVENTORY — these are the ONLY edge types that exist:
1. src_content <-> struct_doc        (a body of text linked to its marginal page/section number — MOST COMMON)
2. src_content <-> src_date          (a body linked to a margin/inline ENTRY date in a notebook/journal/ledger. A formal letterhead dateline is NOT this edge — leave it UNLINKED.)
3. archv_commentary <-> struct_doc   (CONTENTS/INDEX pages only: each archivist catalog-description block linked to the page-number marker it describes — same fan-out geometry as edge 1)
4. archv_other <-> struct_doc        (an archivist note about a numbered source page — RARE)
5. archv_format_note <-> src_content (a non-numbered location label heading a body block — VERY RARE)
6. other <-> struct_doc              (a degenerate duplicate-label artifact — do NOT seek it out)
If a candidate edge is not one of these six category-pairs, DO NOT emit it.

CATEGORIES THAT NEVER CONNECT: src_location_sender, src_location_recipient, src_recipient, src_greeting, src_farewell, src_signature, src_origin, archv_date, archv_possessor, struct_id, struct_commentary, struct_other. (archv_commentary connects ONLY on a contents/index page, to struct_doc — edge 3; elsewhere it never connects.)

HARD RULES:
- Same document only. Both endpoints must share the same doc id; never link across documents.
- One undirected edge per pair — do NOT emit both 'p1 <-> p2' and 'p2 <-> p1', and no duplicates.
- src_date links to src_content and NOTHING else.
- struct_doc links to src_content / archv_other / other and NEVER to src_date or archv_date.
- There are NO src_content <-> src_content edges (a P.S. block and a body block are never linked).
- If a page has no connections, return an empty edges list.

VISUAL CUE — src_content <-> struct_doc:
- struct_doc markers are short typed labels in the FAR-LEFT margin, left of the body column: "Pg. 4.", "Page 2", "Page 102 Cont.", page ranges, or left-margin outline numerals ("C.I.", "I.", "A)").
- A marker aligns with the FIRST line of the body block to its right and governs every body block below it down to the next marker.
- PAGE-MARKER FAN-OUT: a struct_doc "Page N" (or "Page N Cont.") span is terminated ONLY by the next "Page N" marker or the bottom of the page — NEVER by an intervening margin date. Emit a struct_doc<->src_content edge from that one "Page N" marker to EVERY content block in its vertical span, INCLUDING blocks that begin under a new margin date and blocks that also carry their own src_date edge. A single notebook content block routinely carries TWO independent edges: one to its left-margin src_date AND one to its governing "Page N" marker; emitting the date edge does NOT excuse omitting the page-marker edge. Example: if "Page 10" covers margin-dates Dec., Dec. 12th, 15, 16, 17 before "Page 11", emit FIVE struct_doc<->src_content edges (one per date-block) PLUS each block's own src_date edge. The "one box per date-span, not per entry" rule only forbids splitting a SINGLE date's stacked sub-entries; it never collapses distinct margin-date blocks and never reduces the number of fan-out edges.
- Do NOT treat as struct_doc: the underlined title line (struct_id), or numbers embedded inside the body text (verse numbers, "#608", "4)"). POSITION TEST: if "Page N" sits up in the top header band (above the body) rather than in the far-left margin level with a body line, it is a continuation header and does NOT connect.

VISUAL CUE — src_content <-> src_date (notebook / journal / ledger only):
- Dates sit in a LEFT-MARGIN date column ("Nov. 8th, 1923", "Dec. 3rd", or bare day numbers "24th", "26"), each aligned with the first line of the entry to its right. A right-margin per-entry date ("3/20/97") behaves the same way.
- One date may head a GROUP of stacked entries — connect it to every block under it until the next date (fan-out).
- One content block spanning two date rows connects to BOTH dates. Assign a boundary date to the block whose FIRST line it aligns with, not the entry whose text it merely trails.
- An inline date opening a paragraph (with an L-shaped / notched content polygon) also connects to that paragraph.
- Do NOT connect a formal letterhead dateline — a date at the top / right / center of a LETTER, physically separated above the body, stays UNLINKED.
- Before linking a notebook src_date, confirm it lies in the FAR-LEFT margin date column (left of the body text). A date typed at the body's own left margin (a sentence-style "For <Month> <day>" heading), a feast word inside the body column, or a lone date trailing an entry TITLE/header line forms NO edge — those entries connect only to the explicit margin date / Page-N marker already governing the span.

DATE DISAMBIGUATION (critical):
A date links ONLY if it is src_date AND a per-entry margin/inline date. The page's top-corner catalog date — archv_date, year-first ("1937, July 16") or circa ("c. 1944") — never connects, even when its value matches a real source date. struct_doc and src_date may share the left margin but NEVER connect to each other. A left-margin ditto/repeat mark is not a date and never forms an edge; entries under a ditto connect to the explicit date above it. A year-first "YYYY, Month" date is archv_date even if it sits lower on the page or was mislabeled src_date — do NOT link it. A ditto-governed entry belongs to the SAME content block as the repeated date, so it adds NO second src_content block and NO extra edges — one ditto continuation = zero new edges. PARTIAL-DITTO EXCEPTION: when the ditto replaces only the month/year and an explicit, different day number is written beside it, that row IS its own block — emit its normal src_content<->src_date edge and its src_content<->struct_doc page-marker edge.

RARE-EDGE CUES:
- A note like "Blank." / "Page 2 and 3 are blank" on the same line as a "Page N" left-margin marker -> archv_other <-> struct_doc (the note is archv_other, not src_content).
- A non-numbered location label ("Inside front cover.") sitting where a page number would, above a content block -> archv_format_note <-> src_content.
- A note OR a transcriber page-summary gloss to the right of a "Page N" / "Page N to M" left-margin marker -> archv_other <-> struct_doc, not src_content <-> struct_doc.

BBOX ALONE IS INSUFFICIENT. When a page has multiple candidate dates / markers / content blocks, use the IMAGE — spatial proximity, first-line vertical alignment, left-margin position, layout breaks, and what each region actually says — to decide WHICH date or marker belongs to WHICH text.

Before emitting each edge, check:
1. Same doc?
2. One of the five allowed category-pairs?
3. For a date: is it a per-entry margin/inline src_date (not archv_date, not a formal letterhead dateline)?
4. For a marker/date heading a group: have I emitted an edge to every governed content block?
5. No duplicates.

Each example below shows the page image, the polygon table, and the correct connections.
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
    target_page: str | None = None,
) -> list[Path]:
    """Pick pass-2 examples biased toward same volume as target_doc."""
    if not pool:
        return []
    if target_page is not None:
        pool = [p for p in pool if not (p.parent.name == target_doc and p.name == target_page)]
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
    stop=stop_after_attempt(6),
    wait=wait_random_exponential(multiplier=2, max=60),
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
        if tuple(sorted((a_type, b_type))) not in ALLOWED_EDGE_PAIRS:
            continue  # category-pair not in the observed connection scheme

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


# ── Snap-to-ink (deterministic, grow-only polygon repair) ──────────────────────
# Gemini normalizes input images internally, so it cannot place pixel-tight
# polygon edges (IoU testing showed image width has no effect). This step fixes the
# specific failure the model makes — boxes that CLIP the text — without disturbing
# well-placed boxes. Using the full-resolution render, each edge is pushed OUTWARD
# only where ink sits right at it and continues (clipped text), stopping at the
# first whitespace gap so it cannot bleed into a neighbor. Edges are never pulled
# inward (a symmetric shrink-to-ink version measured WORSE vs the human labels,
# which carry a small consistent margin), and strongly-slanted boxes are skipped.


def _otsu_threshold(hist: list[int]) -> int:
    """Otsu's method on a 256-bin grayscale histogram → threshold (0-255)."""
    total = sum(hist)
    if total == 0:
        return 128
    sum_all = sum(i * hist[i] for i in range(256))
    w_b = 0
    sum_b = 0.0
    best_t, best_var = 128, -1.0
    for t in range(256):
        w_b += hist[t]
        if w_b == 0:
            continue
        w_f = total - w_b
        if w_f == 0:
            break
        sum_b += t * hist[t]
        m_b = sum_b / w_b
        m_f = (sum_all - sum_b) / w_f
        var = w_b * w_f * (m_b - m_f) ** 2
        if var > best_var:
            best_var, best_t = var, t
    return best_t


def _ink_runs(binp: list, gap: int) -> list:
    """Group True cells of `binp` into runs, bridging blank gaps of <= `gap` cells.
    Returns list of (start, end) half-open intervals."""
    runs = []
    n = len(binp)
    i = 0
    while i < n:
        if not binp[i]:
            i += 1
            continue
        start = i
        last = i
        j = i + 1
        while j < n:
            if binp[j]:
                last = j
                j += 1
            else:
                k = j
                while k < n and not binp[k]:
                    k += 1
                if k < n and (k - j) <= gap:      # short gap -> bridge it
                    j = k
                else:
                    break
        runs.append((start, last + 1))
        i = last + 1
    return runs


def _fit_extent(binp: list, gap: int, alo: float, ahi: float):
    """Return (lo, hi) covering every ink-run that overlaps [alo, ahi). None if no ink
    overlaps the box — i.e. the tight extent of the contiguous text the box sits on."""
    inter = [(a, b) for a, b in _ink_runs(binp, gap) if b > alo and a < ahi]
    if not inter:
        return None
    return min(a for a, _ in inter), max(b for _, b in inter)


def _per_line_x_extent(ink_crop, y_lo, y_hi, bx0, bx1, gap, min_run):
    """PER-LINE horizontal extent: the leftmost/rightmost ink across rows [y_lo,y_hi),
    following only runs that overlap the box's own x-span [bx0,bx1). This lets a SINGLE
    long line drive the right edge (a full-height column projection washes it out — the
    cause of right-edge clipping), while a separate neighbour block (gap before it) is
    NOT grabbed. Returns (lo, hi) in crop-pixel coords, or None. (`ink_crop`: thresholded
    'L' image, 255 = ink.)"""
    import numpy as np
    arr = np.asarray(ink_crop) > 0
    H, W = arr.shape
    y_lo = max(0, int(y_lo)); y_hi = min(H, int(y_hi))
    bx0 = max(0, int(bx0)); bx1 = min(W, int(bx1))
    lo = hi = None
    for r in range(y_lo, y_hi):
        idx = np.flatnonzero(arr[r])
        if idx.size == 0:
            continue
        splits = np.where(np.diff(idx) > gap)[0]
        starts = np.concatenate(([0], splits + 1)); ends = np.concatenate((splits, [idx.size - 1]))
        rl = rr = None
        for s, e in zip(starts, ends):
            a = int(idx[s]); b = int(idx[e]) + 1
            if b - a < min_run:
                continue
            if b > bx0 and a < bx1:               # run is part of THIS block's line
                rl = a if rl is None else min(rl, a)
                rr = b if rr is None else max(rr, b)
        if rl is None:
            continue
        lo = rl if lo is None else min(lo, rl)
        hi = rr if hi is None else max(hi, rr)
    return (lo, hi) if lo is not None else None


def snap_polygon_to_ink(
    vertices:      list[dict],
    page_gray:     Image.Image,
    fences:        list = (),      # (x0,y0,x1,y1) of OTHER polygons — fit stays clear of these
    margin_frac_h: float = 0.45,   # horizontal search reach beyond box (frac of box width) — wide
                                   # enough to SEE a clipped long line (validated, snap_lab.py)
    margin_frac_v: float = 0.22,   # vertical search reach beyond box (frac of box height)
    h_gap_frac:    float = 0.016,  # horizontal blank run bridged when fitting (fraction of W)
    v_gap_frac:    float = 0.009,  # vertical blank run bridged when fitting (fraction of H)
    min_ink_frac:  float = 0.035,  # a row counts as "ink" above this mean coverage
    min_run_frac:  float = 0.004,  # per-line ink run shorter than this*W is noise (ignored)
    fence_margin:  int   = 6,      # stay this many px clear of a neighbouring box
    text_margin_frac: float = 0.008,  # blank margin around text as frac of page height (~55px @150dpi)
    vmargin_scale: float = 1.0,    # scale the vertical margin only (1.0 keeps it safe; <1 risks clip)
    skew_tol:      float = 0.12,   # skip clean quads whose top edge slopes more than this
) -> list[dict]:
    """FIT-TO-INK: resize the box to tightly and completely bound the contiguous text
    it sits on — tightening loose edges AND extending clipped ones, so the result
    perfectly bounds its text. Fenced by the neighbouring labeled boxes (`fences`) so
    it never grabs an adjacent block, and gap-aware so it spans all lines of its own
    block without crossing block-separating whitespace. Returns a clean axis-aligned
    rectangle, or the ORIGINAL vertices when the region is blank / no ink overlaps the
    box / a clean quad is strongly slanted. Coords are page_gray's pixel space.
    """
    W, H = page_gray.size
    xs = [v["x"] for v in vertices]
    ys = [v["y"] for v in vertices]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    bw, bh = x1 - x0, y1 - y0
    if bw < 3 or bh < 3:
        return vertices

    is_quad = len(vertices) == 4
    if is_quad:
        tdx = vertices[1]["x"] - vertices[0]["x"]
        tdy = vertices[1]["y"] - vertices[0]["y"]
        if abs(tdx) > 1 and abs(tdy / tdx) > skew_tol:
            return vertices

    left_lim, right_lim, top_lim, bot_lim = 0, W, 0, H
    for fx0, fy0, fx1, fy1 in fences:
        if fy0 < y1 and fy1 > y0:
            if fx1 <= x0:
                left_lim = max(left_lim, fx1 + fence_margin)
            if fx0 >= x1:
                right_lim = min(right_lim, fx0 - fence_margin)
        if fx0 < x1 and fx1 > x0:
            if fy1 <= y0:
                top_lim = max(top_lim, fy1 + fence_margin)
            if fy0 >= y1:
                bot_lim = min(bot_lim, fy0 - fence_margin)

    mx, my = bw * margin_frac_h, bh * margin_frac_v
    sx0 = max(0, left_lim, int(round(x0 - mx)))
    sy0 = max(0, top_lim, int(round(y0 - my)))
    sx1 = min(W, right_lim, int(round(x1 + mx)))
    sy1 = min(H, bot_lim, int(round(y1 + my)))
    sx0, sy0 = min(sx0, x0), min(sy0, y0)
    sx1, sy1 = max(sx1, x1), max(sy1, y1)
    sx0, sy0 = max(0, sx0), max(0, sy0)
    sx1, sy1 = min(W, sx1), min(H, sy1)
    if sx1 - sx0 < 3 or sy1 - sy0 < 3:
        return vertices

    crop = page_gray.crop((sx0, sy0, sx1, sy1))
    cw, ch = crop.size
    thr = _otsu_threshold(crop.histogram())
    ink = crop.point(lambda p: 255 if p <= thr else 0)
    if (sum(ink.getdata()) / 255) / (cw * ch) < 0.003:
        return vertices

    cutoff = 255 * min_ink_frac
    rbin = [v >= cutoff for v in ink.resize((1, ch), Image.BOX).getdata()]
    gap_c = max(4, int(round(W * h_gap_frac)))
    gap_r = max(4, int(round(H * v_gap_frac)))

    # Vertical extent: row projection (tight, as before). Horizontal extent: PER LINE
    # within that vertical band, so a single long line drives the right edge instead of
    # being washed out by a full-height column projection (fixes right-edge clipping).
    fy = _fit_extent(rbin, gap_r, y0 - sy0, y1 - sy0)
    if fy is None:
        return vertices
    min_run = max(6, int(round(W * min_run_frac)))
    fx = _per_line_x_extent(ink, fy[0], fy[1], x0 - sx0, x1 - sx0, gap_c, min_run)
    if fx is None:
        return vertices

    # Fit extent = the box's OWN text, separated from neighbours by the fences.
    ex0 = max(left_lim, sx0 + fx[0]); ex1 = min(right_lim, sx0 + fx[1])
    ey0 = max(top_lim, sy0 + fy[0]);  ey1 = min(bot_lim, sy0 + fy[1])
    # Add the human-style margin AROUND the text, clamped only to the page — small
    # overlaps with neighbours are expected (the human labels overlap on 41/71 pages).
    hmargin = max(6, int(round(text_margin_frac * H)))
    vmargin = max(4, int(round(text_margin_frac * H * vmargin_scale)))
    nx0 = max(0, ex0 - hmargin); nx1 = min(W, ex1 + hmargin)
    ny0 = max(0, ey0 - vmargin); ny1 = min(H, ey1 + vmargin)
    nx0, ny0, nx1, ny1 = int(nx0), int(ny0), int(nx1), int(ny1)
    if nx1 - nx0 < 3 or ny1 - ny0 < 3:
        return vertices
    if is_quad and (nx0, ny0, nx1, ny1) == (x0, y0, x1, y1):
        return vertices
    return [{"x": nx0, "y": ny0}, {"x": nx1, "y": ny0},
            {"x": nx1, "y": ny1}, {"x": nx0, "y": ny1}]


def snap_all_polygons(documents: dict, page_gray: Image.Image) -> int:
    """Fit-to-ink every polygon in `documents` in place; returns count changed.

    Fences are each polygon's original bbox (computed once), so fitting is judged
    against neighbours' original positions and is order-independent.
    """
    # Category-specific fit overrides. A letterhead (src_origin) spans several
    # centered lines with larger inter-line gaps, so it needs a longer reach and a
    # bigger bridgeable gap to capture the WHOLE block (e.g. the "...Church of..."
    # top line the model sometimes leaves out) — still neighbour-fenced.
    OVERRIDES = {
        # Letterhead spans several centered lines with larger inter-line gaps: reach
        # farther vertically and bridge a bigger vertical gap to capture the whole block.
        "src_origin": dict(margin_frac_v=0.75, v_gap_frac=0.022),
    }
    refs = []
    for doc in documents.values():
        for cat, polys in doc.items():
            if not isinstance(polys, list):
                continue
            for poly in polys:
                refs.append((poly, cat, _bbox_of(poly["vertices"])))

    changed = 0
    for i, (poly, cat, _bb) in enumerate(refs):
        fences = [b for j, (_q, _c, b) in enumerate(refs) if j != i]
        new = snap_polygon_to_ink(poly["vertices"], page_gray, fences,
                                  **OVERRIDES.get(cat, {}))
        if new is not poly["vertices"] and new != poly["vertices"]:
            poly["vertices"] = new
            changed += 1
    return changed


def resolve_overlaps(documents: dict) -> int:
    """Make src_location_sender and src_date disjoint within each document.

    The dateline ("Yonkers, N. Y., Christmas Day 1913") must split into a location
    box (left) and a date box (right) that do NOT overlap. The model sometimes draws
    the location box across the whole line, swallowing the date (double-coverage).
    Here we trim the location box back to the date's near edge so the two carry
    distinct, non-overlapping information. Returns count of boxes trimmed.
    """
    def overlaps(a, b):
        return a[2] > b[0] and b[2] > a[0] and a[3] > b[1] and b[3] > a[1]

    def contain_frac(a, b):
        ix = max(0, min(a[2], b[2]) - max(a[0], b[0]))
        iy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
        ab = (b[2] - b[0]) * (b[3] - b[1])
        return (ix * iy) / ab if ab > 0 else 0

    trimmed = 0
    for doc in documents.values():
        locs = doc.get("src_location_sender", [])
        dates = doc.get("src_date", [])
        for L in locs:
            lb = _bbox_of(L["vertices"])
            for D in dates:
                db = _bbox_of(D["vertices"])
                # Only fix the PATHOLOGICAL case where the location box swallows the
                # date (>50% of the date inside location). Small overlaps are normal —
                # human labels overlap location/date up to ~22%, so leave those alone.
                if not overlaps(lb, db) or contain_frac(lb, db) <= 0.5:
                    continue
                if db[0] > lb[0]:                       # date sits to the right -> trim location's right
                    new_r = db[0] - 2
                    if new_r - lb[0] > 3:
                        L["vertices"] = [{"x": lb[0], "y": lb[1]}, {"x": new_r, "y": lb[1]},
                                         {"x": new_r, "y": lb[3]}, {"x": lb[0], "y": lb[3]}]
                        trimmed += 1
                        lb = _bbox_of(L["vertices"])
                elif db[2] < lb[2]:                     # date sits to the left -> trim location's left
                    new_l = db[2] + 2
                    if lb[2] - new_l > 3:
                        L["vertices"] = [{"x": new_l, "y": lb[1]}, {"x": lb[2], "y": lb[1]},
                                         {"x": lb[2], "y": lb[3]}, {"x": new_l, "y": lb[3]}]
                        trimmed += 1
                        lb = _bbox_of(L["vertices"])
    return trimmed


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


# ── Coverage backstop (general, content-driven recall recovery) ───────────────

BACKSTOP_PROMPT = """\
You previously labeled this scanned archival page, but an automatic coverage check found written TEXT that no polygon encloses. Your job now is ONLY to box that missing text — do NOT relabel, move, or re-emit anything already correct.

These regions (normalized [0,1000]; each given as top-left (x0,y0) to bottom-right (x1,y1)) currently contain UNBOXED ink:
{regions}

For EACH region that holds written source/letter/archival text, emit a polygon (EXACTLY 4 vertices, coordinates in [0,1000]) tightly bounding that text, tagged with its correct category. A region that is a wrapped continuation or trailing remainder of adjacent text should still get a box with the best-fit category of that adjacent text. Do NOT box non-text marks (ink stamps, library/call numbers, punch holes, stray specks) — skip those regions. Return the SAME schema; include ONLY the new boxes; assign each to the document (doc_1, doc_2, ...) it belongs to."""


def build_backstop_prompt_parts(target_image: Image.Image, regions: list[dict]) -> list:
    """Prompt the model to box only the regions a coverage check left uncovered."""
    cats = "\n".join(f"  - {cat}: {CATEGORY_DESCRIPTIONS[cat]}" for cat in CATEGORIES)
    reg = "\n".join(f"  - ({r['x0']},{r['y0']}) to ({r['x1']},{r['y1']})" for r in regions)
    return [
        "CATEGORIES:\n" + cats,
        BACKSTOP_PROMPT.format(regions=reg),
        "PAGE (already partially labeled):",
        target_image,
    ]


def _box_bbox(b):
    return _bbox_of(b["vertices"])


def _union_quad(b1, b2):
    xs = [v["x"] for v in b1["vertices"]] + [v["x"] for v in b2["vertices"]]
    ys = [v["y"] for v in b1["vertices"]] + [v["y"] for v in b2["vertices"]]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    return [{"x": x0, "y": y0}, {"x": x1, "y": y0}, {"x": x1, "y": y1}, {"x": x0, "y": y1}]


def _line_scale(documents) -> float:
    """Median box height — the 'one line' scale for adjacency tests (corpus-agnostic)."""
    hs = sorted(_box_bbox(b)[3] - _box_bbox(b)[1]
                for doc in documents.values() for ps in doc.values() for b in ps)
    return hs[len(hs) // 2] if hs else 40.0


def abuts(R, B, near) -> bool:
    """True if region bbox R abuts box bbox B within ~one line along a shared edge
    (so R is the next line below/above, or a trailing word beside, box B)."""
    rx0, ry0, rx1, ry1 = R
    bx0, by0, bx1, by1 = B
    rw, rh = max(rx1 - rx0, 1), max(ry1 - ry0, 1)
    ix = max(0, min(rx1, bx1) - max(rx0, bx0))
    iy = max(0, min(ry1, by1) - max(ry0, by0))
    hov = ix / min(rw, bx1 - bx0) if min(rw, bx1 - bx0) > 0 else 0
    vov = iy / min(rh, by1 - by0) if min(rh, by1 - by0) > 0 else 0
    if hov >= 0.6 and (-0.5 * near <= ry0 - by1 < 1.3 * near or -0.5 * near <= by0 - ry1 < 1.3 * near):
        return True
    if vov >= 0.6 and (-0.5 * near <= rx0 - bx1 < 1.5 * near or -0.5 * near <= bx0 - rx1 < 1.5 * near):
        return True
    return False


def _merge_added_boxes(documents: dict, new_docs: dict) -> tuple[int, int]:
    """Merge model-found boxes into `documents`, CATEGORY-SAFELY. A new box that
    abuts an existing box of the SAME category EXTENDS it (the original box was just
    too short / narrow — your "extend" case); otherwise it is added as a NEW box
    (assigned to the nearest document, never creating one — your "new label" case).

    The category is always the model's, so this never changes a label — it only
    decides whether a box grows or a new box appears. Adjacent same-category
    additions coalesce (a wrapped block becomes one box, not slivers).
    Returns (n_new_boxes_added, n_existing_boxes_extended)."""
    if not documents:
        return 0, 0
    near = _line_scale(documents)
    spans = {}
    for dname, doc in documents.items():
        ys = [v for _c, ps in doc.items() for b in ps
              for v in (_box_bbox(b)[1], _box_bbox(b)[3])]
        if ys:
            spans[dname] = (min(ys), max(ys))
    default_doc = next(iter(documents))
    added = extended = 0
    for _dn, ndoc in new_docs.items():
        for cat, polys in ndoc.items():
            for nb in polys:
                if not nb.get("vertices"):
                    continue
                R = _box_bbox(nb)
                host = None
                for doc in documents.values():
                    for b in doc.get(cat, []):           # SAME category only
                        if abuts(R, _box_bbox(b), near):
                            host = b
                            break
                    if host:
                        break
                if host is not None:
                    host["vertices"] = _union_quad(host, nb)   # EXTEND in place
                    extended += 1
                else:
                    yc = (R[1] + R[3]) / 2
                    tgt, best = default_doc, None
                    for dname, (y0, y1) in spans.items():
                        dist = 0 if y0 <= yc <= y1 else min(abs(yc - y0), abs(yc - y1))
                        if best is None or dist < best:
                            best, tgt = dist, dname
                    documents[tgt].setdefault(cat, []).append(nb)   # NEW box
                    added += 1
    return added, extended


def coverage_backstop(
    documents:   dict,
    page_gray:   Image.Image,
    target_img:  Image.Image,
    client:      genai.Client,
    model_name:  str,
    page_width:  int,
    page_height: int,
) -> tuple[int, int, int, int]:
    """Second-pass recall recovery: re-prompt the model to box any ink its OWN
    output left uncovered. General and content-driven — fires only where real ink
    is unboxed, with zero page-specific knowledge, and only ADDS boxes (existing
    labels and num_documents are never touched). Each missing region either EXTENDS an
    adjacent same-category box or becomes a new box. Returns (n_added, n_extended, in_tok, out_tok)."""
    import qa_report  # lazy: qa_report imports this module at load time
    ink = qa_report._ink_mask(page_gray)
    W, H = page_gray.size
    flags = qa_report.check_uncovered_ink(documents, ink, W, H)
    if not flags:
        return 0, 0, 0, 0
    regions = [{
        "x0": round(f["bbox"][0] / W * NORM_RANGE), "y0": round(f["bbox"][1] / H * NORM_RANGE),
        "x1": round(f["bbox"][2] / W * NORM_RANGE), "y1": round(f["bbox"][3] / H * NORM_RANGE),
    } for f in flags]
    parts = build_backstop_prompt_parts(target_img, regions)
    resp, in_tok, out_tok = call_gemini(client, model_name, parts)
    new_docs = _scale_response_to_original(resp, 0, 0, page_width, page_height)
    snap_all_polygons(new_docs, page_gray)   # snap ONLY the new boxes
    added, extended = _merge_added_boxes(documents, new_docs)
    return added, extended, in_tok, out_tok


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
    snap:               bool = True,
    backstop:           bool = True,
    contrast_pairs:     list | None = None,   # candidate pool from build_contrast_pairs
    num_contrast:       int = 2,              # how many a target actually sees
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
    page_name = pdf_path.stem
    cps = []
    if contrast_pairs and num_contrast > 0:
        cps = _pick_contrast_for_target(contrast_pairs, f"{doc_name}/{page_name}",
                                        _layout_descriptor(pdf_path), num_contrast)
    prompt_parts  = build_prompt_parts(target_img, fewshot_pairs,
                                        load_volume_note(doc_name, page_name),
                                        contrast_pairs=cps)

    response, input_tokens, output_tokens = call_gemini(client, model_name, prompt_parts)
    documents = _scale_response_to_original(
        response,
        rendered_width  = render_w,
        rendered_height = render_h,
        page_width      = source_w,
        page_height     = source_h,
    )

    # ── Geometry cleanup (full-res render; before pass 2 so edges use the result) ─
    if snap:
        n_trimmed = resolve_overlaps(documents)          # de-overlap location/date first
        full_gray = render_page(pdf_path, None)[0].convert("L")
        n_snapped = snap_all_polygons(documents, full_gray)
        log.info("  resolved %d overlap(s); fit %d polygon(s) to ink", n_trimmed, n_snapped)

        # Coverage backstop: box any ink the model's own output left uncovered.
        if backstop:
            try:
                n_add, n_ext, bs_in, bs_out = coverage_backstop(
                    documents, full_gray, target_img, client, model_name, source_w, source_h)
                input_tokens  += bs_in
                output_tokens += bs_out
                if n_add or n_ext:
                    log.info("  coverage backstop: extended %d box(es), added %d new for uncovered ink",
                             n_ext, n_add)
            except Exception as exc:
                log.warning("  coverage backstop failed: %s", exc)

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
    p.add_argument("--pages",       type=str,   default=None,
                   help="Comma-separated page numbers and ranges within --volume "
                        "(e.g. '3,17,42-45'). Requires --volume. Composes with "
                        "--start/--end. pick_representatives.py emits this string "
                        "(HITL_BOOTSTRAP.md PHASE 2).")
    p.add_argument("--dry-run",     type=int,   default=None,
                   help="Process at most N pages then stop.")
    p.add_argument("--num-fewshot", type=int,   default=12,
                   help="Number of few-shot examples per API call (default: 8).")
    p.add_argument("--min-fewshot-multi-doc", type=int, default=4,
                   help="Guarantee at least this many multi-document examples in each "
                        "pass-1 few-shot call (default: 3). Helps the model detect "
                        "pages that contain 2+ letters.")
    p.add_argument("--image-width", default="1024",
                   help="Resize images to this width (px) before sending. "
                        "Use 'full' to send original render. Default: 1024. "
                        "(Note: Gemini normalizes images internally — widths >1024 "
                        "did NOT improve polygon tightness in IoU testing.)")
    p.add_argument("--model",       type=str,   default="gemini-3.5-flash",
                   help="Gemini model name (default: gemini-3.5-flash). On the hard "
                        "dense-notebook pages this cut hard label errors ~77%% vs "
                        "gemini-3.1-flash-lite (138->32 on the 7-page A/B) and even edged "
                        "out gemini-3.1-pro — at ~6x flash-lite's per-page cost (~$4 vs "
                        "~$0.66 per ~45-page volume), trivial against the review hour it "
                        "saves. Use --model gemini-3.1-flash-lite to go cheap (then keep "
                        "--page-type-fewshot ON: it recovers most of the gap, 138->52). "
                        "Must match a key in pricing.PRICING for cost tracking.")
    p.add_argument("--page-type-fewshot", action=argparse.BooleanOptionalAction, default=True,
                   help="Type each page (cheap VLM call) and route same-TYPE few-shot "
                        "examples — letters get letter demos, notebooks get notebook demos. "
                        "On by default; --no-page-type-fewshot reverts to same-volume selection.")
    p.add_argument("--page-type-model", type=str, default="gemini-3.1-flash-lite",
                   help="Model used for the cheap page-type pre-classification "
                        "(default: gemini-3.1-flash-lite; cached to page_type_cache/).")
    p.add_argument("--delay",       type=float, default=1.0,
                   help="Seconds to sleep between API calls (default: 1.0).")
    p.add_argument("--concurrency", type=int,   default=MAX_CONCURRENCY,
                   help=f"Pages labeled in parallel (default: {MAX_CONCURRENCY}; 1 = serial).")
    p.add_argument("--api-key",     type=str,   default=None,
                   help="Override GEMINI_API_KEY from .env / environment.")
    p.add_argument("--seed",        type=int,   default=42,
                   help="RNG seed for few-shot selection (default: 42).")
    p.add_argument("--overwrite",   action="store_true",
                   help="Re-label pages that already have output JSONs.")
    p.add_argument("--extra-note-file", type=str, default="",
                   help="Path to a text file whose contents are appended to the "
                        "prompt for THIS run only (e.g. a contents-page convention "
                        "applied to specific --pages). Not stored as a volume note.")
    p.add_argument("--no-connections", action="store_true",
                   help="Skip pass 2 (connection inference). Polygons only.")
    p.add_argument("--num-fewshot-pass2", type=int, default=6,
                   help="Number of few-shot examples for pass 2 (default: 6).")
    p.add_argument("--no-layout-sim", action="store_true",
                   help="Disable layout-similarity few-shot ranking (revert to random-within-type).")
    p.add_argument("--contrast-pairs", type=int, default=0, metavar="N",
                   help="Include N contrastive pairs in every prompt: the volume's "
                        "most-corrected pool pages shown as INCORRECT (model's "
                        "original attempt) vs CORRECT (human gold), C-ICL-style. "
                        "Default 0 (off) — A/B-gated, see HITL_BOOTSTRAP §6.1.")
    p.add_argument("--pin-examples", type=str, default=None,
                   help="Comma-separated pool examples (e.g. 'Volume_4/page_001,Volume_4/page_002') "
                        "ALWAYS included in every page's few-shot set.")
    p.add_argument("--no-snap", action="store_true",
                   help="Skip snap-to-ink polygon tightening (keep raw model polygons).")
    p.add_argument("--no-backstop", action="store_true",
                   help="Skip the coverage-backstop second pass (no re-prompt on uncovered ink).")
    return p.parse_args()


def parse_pages_arg(arg: str) -> set[int]:
    """Parse a --pages string like '3,17,42-45' into {3, 17, 42, 43, 44, 45}."""
    out: set[int] = set()
    for part in arg.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            if "-" in part:
                lo, hi = (int(s) for s in part.split("-", 1))
                if hi < lo:
                    raise ValueError
                out.update(range(lo, hi + 1))
            else:
                out.add(int(part))
        except ValueError:
            raise SystemExit(f"--pages: cannot parse {part!r} (expected N or N-M)")
    if not out:
        raise SystemExit("--pages parsed to an empty set")
    return out


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

    global _EXTRA_NOTE
    if args.extra_note_file:
        _EXTRA_NOTE = Path(args.extra_note_file).read_text().strip()
        _volume_note_cache.clear()
        log.info("Extra prompt note loaded from %s (%d chars).",
                 args.extra_note_file, len(_EXTRA_NOTE))

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

    # Page-type-aware few-shot: type the pool once (offline, from gold labels).
    type_of: dict[Path, str] | None = None
    if args.page_type_fewshot:
        type_of = classify_examples_by_type(all_examples)
        dist = {t: sum(1 for v in type_of.values() if v == t) for t in PAGE_TYPES}
        log.info("Page-type-aware few-shot ON. Pool by type: %s", dist)

    # Layout-similarity few-shot: fingerprint the pool ONCE, then rank demos by how much
    # their layout resembles each target page (validated 2026-06-08: PQ +0.04, recall +0.04
    # over random-within-type, at the same demo count). On by default; --no-layout-sim reverts.
    desc_cache: dict | None = None
    if not args.no_layout_sim:
        log.info("Layout-similarity few-shot ON. Fingerprinting %d pool pages...", len(all_examples))
        desc_cache = {p: _layout_descriptor(_example_pdf(p)) for p in all_examples}

    # Pinned examples: always present in every page's few-shot set.
    pinned: list[Path] | None = None
    if args.pin_examples:
        want = {s.strip() for s in args.pin_examples.split(",") if s.strip()}
        pinned = [p for p in all_examples if f"{p.parent.name}/{p.name}" in want]
        missing = want - {f"{p.parent.name}/{p.name}" for p in pinned}
        if missing:
            log.warning("--pin-examples not found in pool: %s", sorted(missing))
        log.info("Pinned %d example(s) into every few-shot set: %s",
                 len(pinned), [f"{p.parent.name}/{p.name}" for p in pinned])

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
    if args.pages is not None:
        if args.volume is None:
            raise SystemExit("--pages requires --volume (page numbers are per-volume).")
        wanted = parse_pages_arg(args.pages)
        pages = [p for p in pages if p[1] in wanted]
        missing = wanted - {p[1] for p in pages}
        if missing:
            log.warning("--pages numbers not found in %s: %s", args.volume, sorted(missing))
    log.info("Found %d target pages to consider.", len(pages))

    succeeded: list[tuple[str, int]] = []
    failed:    list[tuple[str, int, str]] = []
    skipped:   list[tuple[str, int]] = []
    total_input_tokens  = 0
    total_output_tokens = 0

    # Build the work list FIRST: few-shot selection runs sequentially in the main
    # thread (same rng order as a serial run), so seed reproducibility survives any
    # concurrency setting.
    work = []
    for doc_name, page_num, pdf_path in pages:
        if args.dry_run is not None and len(work) >= args.dry_run:
            log.info("Reached --dry-run limit (%d).", args.dry_run)
            break
        out_path = AUTO_LABELED_DIR / doc_name / f"page_{page_num:03d}" / f"page_{page_num:03d}.json"
        if out_path.exists() and not args.overwrite:
            skipped.append((doc_name, page_num))
            continue
        page_name = f"page_{page_num:03d}"
        # Type the target page (cheap, cached) so few-shot can route same-type demos.
        target_type = None
        if args.page_type_fewshot:
            target_type, pt_in, pt_out = classify_page_type(
                client, args.page_type_model, pdf_path,
                cache_dir=PAGE_TYPE_CACHE_DIR / doc_name)
            total_input_tokens  += pt_in
            total_output_tokens += pt_out
            log.info("  %s/%s -> page_type=%s", doc_name, page_name, target_type)
        target_desc = _layout_descriptor(pdf_path) if desc_cache is not None else None
        fewshot_dirs = select_few_shot(
            all_examples, multi_doc_examples, doc_name,
            args.num_fewshot, args.min_fewshot_multi_doc, rng,
            target_page=page_name, target_type=target_type, type_of=type_of,
            target_desc=target_desc, desc_cache=desc_cache, pinned=pinned,
        )
        pass2_dirs = (select_pass2_fewshot(pass2_pool, doc_name, args.num_fewshot_pass2, rng,
                                           target_page=page_name)
                      if pass2_pool else None)
        work.append((doc_name, page_num, pdf_path, fewshot_dirs, pass2_dirs, out_path))

    concurrency = max(1, args.concurrency)
    log.info("Processing %d page(s) with concurrency=%d.", len(work), concurrency)

    contrast = []
    if args.contrast_pairs > 0:
        contrast = build_contrast_pairs(args.volume, args.contrast_pairs, image_width)
        log.info("Contrastive prompting ON: %d pair(s).", len(contrast))

    def _label_one(item):
        doc_name, page_num, pdf_path, fewshot_dirs, pass2_dirs, out_path = item
        log.info("Labeling %s/page_%03d (%d pass-1 + %d pass-2 examples)",
                 doc_name, page_num, len(fewshot_dirs),
                 len(pass2_dirs) if pass2_dirs else 0)
        return process_page(
            pdf_path           = pdf_path,
            doc_name           = doc_name,
            page_number        = page_num,
            client             = client,
            model_name         = args.model,
            fewshot_dirs       = fewshot_dirs,
            image_width        = image_width,
            output_path        = out_path,
            pass2_fewshot_dirs = pass2_dirs,
            snap               = not args.no_snap,
            backstop           = not args.no_backstop,
            contrast_pairs     = contrast,
            num_contrast       = args.contrast_pairs,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool_exec:
        futures = {}
        for item in work:
            futures[pool_exec.submit(_label_one, item)] = (item[0], item[1])
            if args.delay > 0:
                time.sleep(args.delay)      # stagger submissions: a gentle rate ramp
        for fut in concurrent.futures.as_completed(futures):
            doc_name, page_num = futures[fut]
            try:
                in1, out1, in2, out2 = fut.result()
                total_input_tokens  += in1 + in2
                total_output_tokens += out1 + out2
                succeeded.append((doc_name, page_num))
            except Exception as exc:
                log.error("Failed %s/page_%03d: %s", doc_name, page_num, exc)
                failed.append((doc_name, page_num, str(exc)))

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
