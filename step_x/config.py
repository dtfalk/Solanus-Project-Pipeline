from pathlib import Path


# =====================================================
# STEP X CONFIGURATION — Document Labeling Workflow
# =====================================================


# ── Input / Output Directories ────────────────────────────────────────────────

LABEL_INPUT_DIR       = Path("step_1") / "cleaned_pdfs"
LABEL_CONFIG_DIR      = Path("step_x") / "label_page_data"
LABEL_CROP_OUTPUT_DIR = Path("step_x") / "label_cropped_pdfs"

# Source of pre-existing polygon data used during initialization.
# The initializer seeds each page's doc_1 / first LABEL_INFO_TYPES polygon from here.
LABEL_INIT_SOURCE_DIR = Path("step_2") / "polygon_page_data"


# ── Document Info Types ───────────────────────────────────────────────────────
# Edit this list to add, remove, or rename info types.
# Order determines the display order in the editor.
LABEL_INFO_TYPES = [
    "src_content",
    "src_metadata",
    "src_salutation",
    "archv_commentary",
    "doc_struct",
    "struct_commentary",
    "possessor",
    "other",
]

# Editor / overlay color for each info type (CSS hex strings).
# Add an entry here whenever you add a new type above.
LABEL_INFO_TYPE_COLORS = {
    "src_content":       "#ff4d4d",
    "src_metadata":      "#f5a623",
    "src_salutation":      "#f5a623",
    "archv_commentary":  "#ffd166",
    "doc_struct":        "#4f70ff",
    "struct_commentary": "#ff4fa3",
    "possessor":         "#3ddc97",
    "other":             "#9e9e9e",
}

# Fallback color for any type not listed in LABEL_INFO_TYPE_COLORS.
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

LABEL_EDITOR_DOCUMENT   = "Volume_1.pdf"
LABEL_EDITOR_START_PAGE = 1


# ── Cropper Settings ──────────────────────────────────────────────────────────

LABEL_PAGE_CHUNK_SIZE = 50


# ── Image Processing ──────────────────────────────────────────────────────────

RENDER_DPI = 150
