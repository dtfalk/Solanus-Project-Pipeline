# TODO: FINALIZE THE SCHEMA AND REVIEW
from pathlib import Path

# =====================================================
# STEP X CONFIGURATION — Document Labeling Workflow
# =====================================================

# ── Input / Output Directories ────────────────────────────────────────────────
LABEL_INPUT_DIR       = Path("step_2") / "polygon_cropped_pdfs" / "Volume_4"
LABEL_CONFIG_DIR      = Path("step_x") / "label_page_data"
LABEL_CROP_OUTPUT_DIR = Path("step_x") / "label_cropped_pdfs"

# Source of pre-existing polygon data used during initialization.
# The initializer seeds each page's doc_1 / first LABEL_INFO_TYPES polygon from here.
LABEL_INIT_SOURCE_DIR = Path("step_2") / "polygon_page_data"

# ── Document Info Types ───────────────────────────────────────────────────────
# Edit this list to add, remove, or rename info types.
# Order determines the display order in the editor AND the number shortcut (1-9).
#
# Zone taxonomy — two namespaces:
#   src_*   = Solanus-authored content (included in fine-tuning / RAG / search)
#   archv_* = Archivist-added content  (excluded from all pipelines by default)
#   doc_*   = Document structure markers (excluded from all pipelines)
#
# src_content           — main body of authored text
# src_origin            — sender name / monastery / institution block
# src_recipient         — recipient name
# src_location_recipient— recipient city / address
# src_location_sender   — sender city / location line
# src_date              — date written by Solanus
# src_greeting          — opening salutation ("My Dear Sister M...")
# src_farewell          — closing formula ("I remain sincerely...")
# src_signature         — signature block
# src_margin_note       — Solanus's own marginal additions (spatial, not recoverable later)
# src_insertion         — text inserted between lines (spatial, not recoverable later)
# archv_commentary      — standard transcription notice ("faithful transcription of...")
# archv_format_note     — archivist description of physical format ("written on an envelope", "typed on a Christmas card")
# archv_date            — date added by archivist
# archv_possessor       — "Original in Possession of:" block
# doc_id                — document identification header ("FR. SOLANUS, NOTEBOOK NO. X")
# doc_struct            — page numbers, section breaks, "Page X Cont." markers
# struct_commentary     — archivist notes about document structure
# other                 — anything that doesn't fit above

# Editor overlay color for each info type (CSS hex strings).
# Visually grouped: reds/oranges = authored, blues/greens = archivist, greys = structural
LABEL_INFO_TYPES = [
    "src_content",
    "src_origin",
    "src_recipient",
    "src_location_recipient",
    "src_location_sender",
    "src_date",
    "src_greeting",
    "src_farewell",
    "src_signature",
    "src_margin_note",
    "src_insertion",
    "src_other",
    "archv_commentary",
    "archv_format_note",
    "archv_date",
    "archv_possessor",
    "archv_other",
    "struct_id",
    "struct_doc",
    "struct_commentary",
    "struct_other",
    "other"
]

LABEL_INFO_TYPE_COLORS = {
    # Authored content — warm spectrum
    "src_content":            "#ff4d4d",   # red
    "src_origin":             "#ff8c42",   # orange
    "src_recipient":          "#ffb347",   # light orange
    "src_location_recipient": "#ff4fa3",   # pink
    "src_location_sender":    "#f0a500",   # amber
    "src_date":               "#ffd700",   # gold
    "src_greeting":           "#4df523",   # green
    "src_farewell":           "#2397f5",   # blue
    "src_signature":          "#56c8f5",   # light blue
    "src_margin_note":        "#c084fc",   # purple
    "src_insertion":          "#e879f9",   # pink-purple
    "src_other":              "#e879f9",   # pink-purple
    # Archivist content — teal/cyan spectrum
    "archv_commentary":       "#66ffe6",   # teal
    "archv_format_note":      "#00bfaf",   # dark teal
    "archv_date":             "#7d66ff",   # indigo
    "archv_possessor":        "#3ddc97",   # mint
    "archv_other":            "#033f25",   # mint
    # Structural — neutral
    "struct_id":                 "#814343",   # white
    "struct_doc":             "#aaaaaa",   # grey
    "struct_commentary":      "#ff4fa3",   # hot pink
    "struct_other":                  "#9e9e9e",   # dark grey
    "other":                  "#5225b9",   # dark grey
}
# Fallback color for any type not listed above.
LABEL_DEFAULT_COLOR = "#ffffff"

# ── Workflow Filters ──────────────────────────────────────────────────────────
# Restrict to a subset of PDFs. Empty list = process all PDFs.
LABEL_FILES_TO_RUN     = []

# Exclude specific PDFs. Applied after LABEL_FILES_TO_RUN.
LABEL_FILES_TO_EXCLUDE = []

# Only overwrite specific pages in the crop pipeline (1-indexed).
# Empty list = process every page.
LABEL_TARGET_PAGES = []

# ── Editor Settings ───────────────────────────────────────────────────────────
LABEL_EDITOR_DOCUMENT   = "Volume_4.pdf"
LABEL_EDITOR_START_PAGE = 1

# ── Cropper Settings ──────────────────────────────────────────────────────────
LABEL_PAGE_CHUNK_SIZE = 50

# ── Image Processing ──────────────────────────────────────────────────────────
RENDER_DPI = 150

# ── Pipeline inclusion rules ──────────────────────────────────────────────────
# Which zone types feed into each downstream pipeline.
PIPELINE_INCLUDE = {
    "finetuning": {
        "src_content", "src_greeting", "src_farewell", "src_signature",
        "src_origin", "src_date", "src_margin_note", "src_insertion",
    },
    "search": {
        "src_content", "src_greeting", "src_farewell", "src_signature",
        "src_origin", "src_recipient", "src_location_recipient",
        "src_location_sender", "src_date", "src_margin_note", "src_insertion",
        "archv_commentary", "archv_format_note", "archv_date", "archv_possessor",
        "struct_commentary",
    },
    "rag": {
        "src_content", "src_greeting", "src_farewell", "src_signature",
        "src_origin", "src_recipient", "src_location_recipient",
        "src_location_sender", "src_date", "src_margin_note", "src_insertion",
        "archv_commentary", "archv_format_note", "archv_date", "archv_possessor",
        "struct_commentary",
    },
    "knowledge_graph": {
        "src_content", "src_greeting", "src_farewell", "src_origin",
        "src_recipient", "src_location_recipient", "src_location_sender",
        "src_date", "src_margin_note", "src_insertion",
        "archv_commentary", "archv_format_note", "archv_date", "archv_possessor",
    },
}