"""
Step 4: Auto-label Editor — interactive editor for auto-labeled pages.

Self-contained (stdlib + Pillow + pdf2image; no project imports). Reads
PDF + label JSON pairs from auto_labeled/<doc>/page_xxx/ and saves edited
output to a separate reviewed/ dir on page change or exit, so re-running
auto_labeler.py --overwrite never clobbers your manual edits.

Only pages present in the source (auto_labeled) directory are available.
If a page has already been saved to reviewed/, the editor loads from there
instead (preserving previous edits).

A per-page Review Queue (bottom panel) surfaces qa_report.py flags with a
suggested action per flag: "Extend" grows the nearest box over uncovered ink
(the box-too-short case), "Add box" creates a new one, and "Dismiss" persists
across sessions (qa_output/<doc>/dismissed.json). Selecting a flag auto-zooms
to it and shows a cyan ghost preview of the proposed fix. Queue keys:
Up/Down navigate, Enter = suggested action, e/a/d = extend/add/dismiss, f = fit.

Canvas: mouse-wheel zoom (anchored on the cursor), middle-drag pan, f = fit.
Documents: "Merge ▲" folds the current doc into the previous one; "Split ▼"
duplicates it into a new next doc for keep/delete partitioning (both renumber
later documents and remap connection references).

For each page you can:
  - Set how many documents appear on the page (top toolbar spinner).
  - Select each document from the left panel.
  - For the active document, click an info type in the right panel to make it
    active, then use "+ Add Polygon" to draw a new polygon for that type.
  - Drag individual vertices or the whole polygon of the selected polygon.
  - Delete the selected polygon with "Delete Selected".

Canvas controls:
  - Click to add vertices while in draw mode.
  - Right-click or press Enter to close the current polygon (≥ 3 vertices).
  - Press Escape to cancel drawing.
  - Click a polygon to select it (auto-switches info type if needed).
  - Drag a red handle to move a vertex.
  - Drag inside the selected polygon to move it.
  - With a polygon selected, arrow keys pull one edge inward to tighten the box
    (Up=bottom edge up, Down=top edge down, Left=right edge left, Right=left edge
    right); Ctrl+arrow pushes the opposite edge outward to expand (Ctrl+Up=top
    edge up, Ctrl+Down=bottom edge down, Ctrl+Left=left edge left, Ctrl+Right=
    right edge right); Shift+arrow slides the whole box one step in that direction.
  - With NO polygon selected, Left / Right arrow keys navigate pages (auto-saves).
  - Ctrl+D / Ctrl+A go to the next / previous page from anywhere (auto-saves),
    even with a polygon selected; inert while typing in a text field.
  - On page load the TOPMOST src_content is auto-selected; Ctrl+S steps DOWN the
    page's src_content ladder (top→bottom, wrapping), panning it into view if
    you are zoomed — so tighten-with-arrows → Ctrl+S → repeat, hands on keys.
  - Ctrl+V duplicates the selected polygon (same category, offset slightly) and
    auto-selects the copy, so you can slide it into place with the arrow keys.
  - Shift-click a category in the right panel to RE-LABEL the selected polygon to
    that category (connection links are repointed automatically).
  - "Show connections" (top toolbar) overlays EVERY src_content connection in the
    current document at once: each src_content box is colored through a rainbow by
    its height on the page (top = red ... bottom = violet) with stippled lines and
    dashed rings on its partners — review all links without clicking each box.

Usage:
    python step_3/normalized_editor.py
"""

import json
import math
import os
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
from uuid import uuid4

from pdf2image import convert_from_path
from PIL import ImageTk

# Document we are intending to review/edit
TARGET_DOCUMENT = "Volume_2"

SCRIPT_DIR = Path(__file__).resolve().parent

# ── Inlined config (was in step_x/config.py; mirrors normalized_viewer.py) ─────

RENDER_DPI = 150

# Arrow-key box editing on the selected polygon (source-px per press).
NUDGE_STEP = 15        # plain arrow shrinks an edge / Shift+arrow slides the box
NUDGE_MIN_SIZE = 8     # never shrink a box below this width/height

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
    "other",
]

LABEL_INFO_TYPE_COLORS = {
    "src_content":            "#ff4d4d",
    "src_origin":             "#ff8c42",
    "src_recipient":          "#ffb347",
    "src_location_recipient": "#ff4fa3",
    "src_location_sender":    "#f0a500",
    "src_date":               "#ffd700",
    "src_greeting":           "#4df523",
    "src_farewell":           "#2397f5",
    "src_signature":          "#56c8f5",
    "src_margin_note":        "#c084fc",
    "src_insertion":          "#e879f9",
    "src_other":              "#e879f9",
    "archv_commentary":       "#66ffe6",
    "archv_format_note":      "#00bfaf",
    "archv_date":             "#7d66ff",
    "archv_possessor":        "#3ddc97",
    "archv_other":            "#033f25",
    "struct_id":              "#814343",
    "struct_doc":             "#aaaaaa",
    "struct_commentary":      "#ff4fa3",
    "struct_other":           "#9e9e9e",
    "other":                  "#5225b9",
}
LABEL_DEFAULT_COLOR = "#ffffff"


def _get_env_path(env_name, default_path):
    """Return a path override from environment, or the provided default path.

    Args:
        env_name: Environment variable name to inspect.
        default_path: Fallback path used when variable is not set.

    Returns:
        Path from environment override or default_path.
    """
    env_value = os.getenv(env_name, "").strip()
    if not env_value:
        return default_path
    return Path(env_value).expanduser().resolve()


def _get_env_int(env_name, default_value):
    """Return integer override from environment with safe fallback.

    Args:
        env_name: Environment variable name to inspect.
        default_value: Fallback integer or None when unset/invalid.

    Returns:
        Integer override or default_value when parsing fails.
    """
    env_value = os.getenv(env_name, "").strip()
    if not env_value:
        return default_value

    try:
        return int(env_value)
    except ValueError:
        print(f"WARNING: Invalid integer for {env_name}: {env_value}")
        return default_value


# ── Paths ──────────────────────────────────────────────────────────────────────

# Source of auto-labeled pages (PDF + JSON per page). Read-only reference.
# Each page lives at {SOURCE_DIR}/{doc}/page_xxx/page_xxx.{pdf,json}.
SOURCE_DIR = _get_env_path(
    env_name     = "EDITOR_SOURCE_DIR",
    default_path = SCRIPT_DIR / "auto_labeled",
)

# Destination for human-reviewed label JSONs (read-write output). Kept separate
# from auto_labeled/ so re-running auto_labeler.py --overwrite cannot clobber edits.
OUTPUT_DIR = _get_env_path(
    env_name     = "EDITOR_OUTPUT_DIR",
    default_path = SCRIPT_DIR / "reviewed",
)

# QA flag reports consumed by the Review Queue (written by qa_report.py).
QA_OUTPUT_DIR = _get_env_path(
    env_name     = "EDITOR_QA_OUTPUT_DIR",
    default_path = SCRIPT_DIR / "qa_output",
)

# ── Editor Settings (edit these) ──────────────────────────────────────────────
# name inside auto_labeled/ to edit (e.g. "Volume_1", "Appendix_1").
EDITOR_DOCUMENT = os.getenv("EDITOR_DOCUMENT", f"{TARGET_DOCUMENT}").strip() or f"{TARGET_DOCUMENT}"

# Starting page number. None = start at the first available page.
EDITOR_START_PAGE = _get_env_int(
    env_name      = "EDITOR_START_PAGE",
    default_value = None,
)

HANDLE_RADIUS   = 7   # px — hit radius for vertex handles on canvas
DRAW_DOT_RADIUS = 4   # px — dot drawn at each vertex while in draw mode


# ── Geometry helpers ───────────────────────────────────────────────────────────

def _point_in_polygon(x, y, polygon):
    """Ray-cast test: True if (x, y) lies inside the polygon (list of {x,y} dicts)."""
    inside = False
    n = len(polygon)
    for i in range(n):
        x1, y1 = polygon[i]["x"], polygon[i]["y"]
        x2, y2 = polygon[(i + 1) % n]["x"], polygon[(i + 1) % n]["y"]
        if ((y1 > y) != (y2 > y)) and (
            x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-9) + x1
        ):
            inside = not inside
    return inside


# ── Connection-overlay palette ────────────────────────────────────────────────
# A ROYGBIV sweep in OKLCh space: equal hue steps in OKLCh are close to
# perceptually even (unlike HSV, where greens/yellows compress), so n boxes get
# colors that FEEL evenly spaced. When n grows large enough that adjacent steps
# drop below comfortable at-a-glance distinguishability, the same palette is
# dealt outside-in (1st, last, 2nd, 2nd-last, 3rd, ...) so vertically adjacent
# boxes stay maximally far apart in color.

GRAD_HUE_START = 29.0    # OKLCh hue: red
GRAD_HUE_END   = 315.0   # OKLCh hue: violet
GRAD_L, GRAD_C = 0.65, 0.16
# Min OKLab ΔE between vertically ADJACENT boxes before switching to the
# outside-in deal. Strict laboratory JND is ~0.02; overlay rings on a busy scan
# need more separation to read at a glance, hence the higher working value.
GRAD_MIN_DELTA_E = 0.06


def _oklch_to_hex(L, C, h_deg):
    """OKLCh -> sRGB hex (Ottosson's OKLab transform), channel-clamped to gamut."""
    h = math.radians(h_deg)
    a, b = C * math.cos(h), C * math.sin(h)
    l_ = L + 0.3963377774 * a + 0.2158037573 * b
    m_ = L - 0.1055613458 * a - 0.0638541728 * b
    s_ = L - 0.0894841775 * a - 1.2914855480 * b
    l, m, s = l_ ** 3, m_ ** 3, s_ ** 3
    rgb_lin = (
        +4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
        -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
        -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s,
    )
    out = []
    for c in rgb_lin:
        c = max(0.0, min(1.0, c))
        c = 12.92 * c if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055
        out.append(int(round(max(0.0, min(1.0, c)) * 255)))
    return "#{:02x}{:02x}{:02x}".format(*out)


def connection_palette(n):
    """n hex colors for n src_content boxes ordered top->bottom on the page.

    Plain red->violet gradient while adjacent steps stay distinguishable; once
    the per-step OKLab ΔE falls below GRAD_MIN_DELTA_E, the same colors are
    assigned outside-in (gradient positions 1, n, 2, n-1, 3, ...) per David's
    spec, so neighbouring boxes never share a near-identical hue.
    """
    if n <= 0:
        return []
    if n == 1:
        return [_oklch_to_hex(GRAD_L, GRAD_C, GRAD_HUE_START)]
    hues = [GRAD_HUE_START + (GRAD_HUE_END - GRAD_HUE_START) * i / (n - 1)
            for i in range(n)]
    colors = [_oklch_to_hex(GRAD_L, GRAD_C, h) for h in hues]
    # Adjacent-step ΔE on a constant-L,C hue circle is the chord 2*C*sin(Δh/2).
    step = math.radians((GRAD_HUE_END - GRAD_HUE_START) / (n - 1))
    if 2 * GRAD_C * math.sin(step / 2) >= GRAD_MIN_DELTA_E:
        return colors
    dealt = []
    for i in range(n):
        idx = i // 2 if i % 2 == 0 else n - 1 - i // 2
        dealt.append(colors[idx])
    return dealt


# ── Main application ───────────────────────────────────────────────────────────

class NormalizedEditorApp:
    def __init__(self):
        self.document_name = EDITOR_DOCUMENT

        # Directory of auto-labeled source pages for this document.
        self.source_doc_dir = SOURCE_DIR / self.document_name

        # Directory where reviewed JSONs are saved for this document.
        self.output_doc_dir = OUTPUT_DIR / self.document_name

        # Discover which pages are available in the source dir.
        # This is the authoritative list — only these pages can be edited.
        self.page_numbers = self._discover_pages()
        # EDITOR_PAGES="6,12,40" restricts the session to those pages (the
        # post-rerun review round); EDITOR_PREFER_AUTO=1 loads auto_labeled even
        # when a reviewed/ copy exists (so a re-run is visible instead of being
        # shadowed by the old gold — saving still writes reviewed/ as usual).
        only = os.getenv("EDITOR_PAGES", "").strip()
        if only:
            wanted = {int(t) for t in only.replace(" ", "").split(",") if t}
            self.page_numbers = [n for n in self.page_numbers if n in wanted]
        self.prefer_auto = os.getenv("EDITOR_PREFER_AUTO", "").strip() in ("1", "true", "yes")
        self.total_pages  = len(self.page_numbers)

        if self.total_pages == 0:
            raise FileNotFoundError(
                f"No page directories found in {self.source_doc_dir}"
            )

        # Persisted dismissals (false-alarm flags the user chose to ignore) + zoom state.
        self.dismissed_path = QA_OUTPUT_DIR / self.document_name / "dismissed.json"
        self.dismissed_keys = self._load_dismissed()
        # Pages marked for a machine re-label (bootstrap --rerun-marked). Kept in
        # a SEPARATE sidecar file by design — never inside the page JSONs.
        self.rerun_marks_path = QA_OUTPUT_DIR / self.document_name / "rerun_marks.json"
        self.rerun_marks = self._load_rerun_marks()
        # View state: None => fit whole page; ("flag", bbox) => auto-zoom to a QA flag;
        # ("custom", scale, off_x, off_y) => user wheel-zoom / pan.
        self.view = None
        self._pan_anchor = None
        # Per-page Review Queue: {page_number: [flag dict, ...]} from qa_report.py.
        self.qa_flags_by_page = self._load_qa_flags()
        self.queue_flags    = []     # flags for the current page
        self.selected_flag  = None   # the flag currently highlighted on canvas

        # Determine starting position in the page list.
        if EDITOR_START_PAGE is not None and EDITOR_START_PAGE in self.page_numbers:
            self.page_index = self.page_numbers.index(EDITOR_START_PAGE)
        else:
            self.page_index = 0
        self.current_page = self.page_numbers[self.page_index]

        self.current_doc           = "doc_1"
        self.current_info_type     = LABEL_INFO_TYPES[0]
        self.selected_polygon_idx  = None   # index within current doc+type polygon list

        # Draw mode state
        self.draw_mode     = False
        self.draw_vertices = []   # list of {"x": float, "y": float}
        self.mouse_x       = 0
        self.mouse_y       = 0

        # Drag state
        self.drag_mode       = None   # "vertex" | "polygon"
        self.drag_vertex_idx = None
        self.drag_last_x     = None
        self.drag_last_y     = None

        # Connect mode state.
        # connect_from holds the address {"doc", "type", "id", "index"} of the first
        # polygon picked when building a connection edge.
        self.connect_mode = False
        self.connect_from = None

        # Page / render state
        self.page_data      = {}
        self.page_image     = None
        self.tk_image       = None
        self.original_width  = 1
        self.original_height = 1
        self.display_scale   = 1.0
        self.image_offset_x  = 0
        self.image_offset_y  = 0
        self.rendered_width  = 1
        self.rendered_height = 1

        self._build_ui()
        self.load_page(self.current_page)

    # ── Bootstrap helpers ──────────────────────────────────────────────────────

    def _discover_pages(self):
        """Scan the source directory for page_* folders and return sorted page numbers."""
        if not self.source_doc_dir.exists():
            return []
        page_dirs = sorted(self.source_doc_dir.glob("page_*"))
        numbers = []
        for d in page_dirs:
            if d.is_dir():
                try:
                    num = int(d.name.split("_", 1)[1])
                    numbers.append(num)
                except (ValueError, IndexError):
                    continue
        return sorted(numbers)

    def _resolve_page_pdf(self, page_number):
        """Return the single-page PDF path for the given page number.

        The PDF sits alongside the JSON in the auto_labeled page folder; if a
        reviewed/ copy carries its own PDF, prefer that.
        """
        page_name = f"page_{page_number:03d}"
        output_pdf = self.output_doc_dir / page_name / f"{page_name}.pdf"
        source_pdf = self.source_doc_dir / page_name / f"{page_name}.pdf"

        if output_pdf.exists():
            return output_pdf
        if source_pdf.exists():
            return source_pdf
        raise FileNotFoundError(
            f"Page PDF not found for {page_name} in reviewed/ or source."
        )

    # ── UI construction ────────────────────────────────────────────────────────

    def _build_ui(self):
        self.root = tk.Tk()
        self.root.title(f"Auto-label Editor — {self.document_name}")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.configure(bg="#1e1e1e")
        self.root.geometry("1400x900")

        self._build_top_toolbar()
        self._build_queue_panel()   # packed BOTTOM before main so layout stays stable
        self._build_main_area()
        self._bind_keys()

    def _build_top_toolbar(self):
        top = tk.Frame(self.root, bg="#2d2d2d", pady=5)
        top.pack(side=tk.TOP, fill=tk.X)

        ttk.Button(top, text="◀ Prev", command=self.previous_page).pack(side=tk.LEFT, padx=(8, 2))
        ttk.Button(top, text="Next ▶", command=self.next_page).pack(side=tk.LEFT, padx=2)

        tk.Label(top, text="Page:", bg="#2d2d2d", fg="#cccccc").pack(side=tk.LEFT, padx=(14, 3))
        self.page_entry = ttk.Entry(top, width=6)
        self.page_entry.pack(side=tk.LEFT)
        ttk.Button(top, text="Go", command=self.go_to_page).pack(side=tk.LEFT, padx=(3, 16))

        tk.Label(top, text="Docs on page:", bg="#2d2d2d", fg="#cccccc").pack(side=tk.LEFT, padx=(0, 3))
        self.num_docs_var = tk.IntVar(value=1)
        self.num_docs_spinbox = ttk.Spinbox(
            top, from_=1, to=20, width=4,
            textvariable=self.num_docs_var,
            command=self._on_num_docs_changed,
        )
        self.num_docs_spinbox.pack(side=tk.LEFT)
        self.num_docs_spinbox.bind("<FocusOut>", lambda e: self._on_num_docs_changed())

        # Toggle: overlay EVERY src_content connection at once (rainbow by height)
        # instead of having to click each box to see its links.
        self.show_conns_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Show connections",
                        variable=self.show_conns_var,
                        command=self._draw_scene).pack(side=tk.LEFT, padx=(16, 0))

        self.rerun_button = tk.Button(top, text="⟳ Mark for rerun", bg="#2d2d2d",
                                      fg="#ffaa00", bd=1, command=self._toggle_rerun_mark)
        self.rerun_button.pack(side=tk.LEFT, padx=(16, 0))
        if self.prefer_auto:
            tk.Button(top, text="✓ Approve → gold", bg="#2d2d2d", fg="#7be07b",
                      bd=1, command=self._approve_to_gold).pack(side=tk.LEFT, padx=(10, 0))

        self.mode_label = tk.Label(top, text="", bg="#2d2d2d", fg="#ffaa00",
                                   font=("TkDefaultFont", 10, "bold"))
        self.mode_label.pack(side=tk.LEFT, padx=14)

        self.status_label = tk.Label(top, text="", bg="#2d2d2d", fg="#888888")
        self.status_label.pack(side=tk.RIGHT, padx=10)

    def _build_main_area(self):
        main = tk.Frame(self.root, bg="#1e1e1e")
        main.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        # ── Left panel: document selector ─────────────────────────────────────
        self.left_panel = tk.Frame(main, bg="#252526", width=100)
        self.left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0), pady=4)
        self.left_panel.pack_propagate(False)

        tk.Label(self.left_panel, text="Documents", bg="#252526", fg="#aaaaaa",
                 font=("TkDefaultFont", 8, "bold")).pack(pady=(8, 4))
        ttk.Button(self.left_panel, text="Merge ▲",
                   command=self._merge_doc_into_previous).pack(side=tk.BOTTOM, pady=(2, 8))
        ttk.Button(self.left_panel, text="Split ▼",
                   command=self._split_doc_duplicate).pack(side=tk.BOTTOM, pady=(2, 2))
        self.doc_buttons_frame = tk.Frame(self.left_panel, bg="#252526")
        self.doc_buttons_frame.pack(fill=tk.BOTH, expand=True, pady=2)

        # ── Canvas ────────────────────────────────────────────────────────────
        self.canvas = tk.Canvas(main, bg="#1e1e1e", highlightthickness=0)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # ── Right panel: info type selector ───────────────────────────────────
        self.right_panel = tk.Frame(main, bg="#252526", width=168)
        self.right_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 4), pady=4)
        self.right_panel.pack_propagate(False)

        tk.Label(self.right_panel, text="Label Type", bg="#252526", fg="#aaaaaa",
                 font=("TkDefaultFont", 8, "bold")).pack(pady=(8, 4))

        self.info_type_frame = tk.Frame(self.right_panel, bg="#252526")
        self.info_type_frame.pack(fill=tk.X, padx=4)
        self.info_type_buttons = {}
        self._rebuild_info_type_buttons()

        ttk.Separator(self.right_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=6, pady=10)

        ttk.Button(self.right_panel, text="+ Add Polygon",
                   command=self._start_draw_mode).pack(fill=tk.X, padx=6, pady=2)
        ttk.Button(self.right_panel, text="Delete Selected",
                   command=self._delete_selected_polygon).pack(fill=tk.X, padx=6, pady=2)
        ttk.Button(self.right_panel, text="Connect",
                   command=self._start_connect_mode).pack(fill=tk.X, padx=6, pady=2)

        ttk.Separator(self.right_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=6, pady=10)

        hint_lines = [
            "Draw mode hints:",
            "• Click → add vertex",
            "• Right-click / Enter",
            "  → close polygon",
            "• Escape → cancel",
            "",
            "Shortcuts:",
            "• 1-9 → switch label type",
            "• Shift+W → add polygon",
            "• Shift+C → connect",
            "• Ctrl+D / Ctrl+A →",
            "  next / prev page",
            "• Ctrl+S → next src_content",
            "  down the page (wraps)",
            "",
            "Selected box (arrows):",
            "• Arrow → pull that edge in",
            "  (↑ bottom, ↓ top,",
            "   ← right, → left)",
            "• Ctrl+Arrow → push edge out",
            "  (↑ top, ↓ bottom,",
            "   ← left, → right)",
            "• Shift+Arrow → slide box",
            "• Ctrl+V → duplicate box",
            "• Shift-click a category",
            "  → relabel selected box",
            "",
            "Show connections (top bar):",
            "• all src_content links,",
            "  rainbow = top→bottom",
        ]
        for line in hint_lines:
            tk.Label(self.right_panel, text=line, bg="#252526",
                     fg="#555555", font=("TkDefaultFont", 8),
                     anchor=tk.W, justify=tk.LEFT).pack(fill=tk.X, padx=8)

    def _rebuild_info_type_buttons(self):
        for w in self.info_type_frame.winfo_children():
            w.destroy()
        self.info_type_buttons = {}
        for info_type in LABEL_INFO_TYPES:
            color = LABEL_INFO_TYPE_COLORS.get(info_type, LABEL_DEFAULT_COLOR)
            row = tk.Frame(self.info_type_frame, bg="#333333", cursor="hand2")
            row.pack(fill=tk.X, pady=1)

            # Color swatch
            tk.Label(row, bg=color, width=2).pack(side=tk.LEFT)
            label = tk.Label(
                row,
                text=info_type.replace("_", " "),
                bg="#333333",
                fg="#cccccc",
                anchor=tk.W,
                padx=6,
                pady=4,
                font=("TkDefaultFont", 9),
            )
            label.pack(side=tk.LEFT, fill=tk.X, expand=True)

            for widget in (row, label):
                widget.bind("<Button-1>", lambda e, t=info_type: self._select_info_type(t))
                # Shift-click a category to RE-LABEL the currently selected polygon to it.
                widget.bind("<Shift-Button-1>", lambda e, t=info_type: self._recategorize_selected(t))

            self.info_type_buttons[info_type] = row
        self._highlight_info_type_button()

    def _rebuild_doc_buttons(self):
        for w in self.doc_buttons_frame.winfo_children():
            w.destroy()
        num_docs = self.page_data.get("num_documents", 1)
        for i in range(1, num_docs + 1):
            doc_key    = f"doc_{i}"
            is_active  = doc_key == self.current_doc
            bg = "#0066cc" if is_active else "#3a3a3a"
            fg = "white"
            font_weight = "bold" if is_active else "normal"
            btn = tk.Button(
                self.doc_buttons_frame,
                text=f"Doc {i}",
                bg=bg, fg=fg,
                activebackground="#0077dd",
                activeforeground="white",
                relief=tk.FLAT,
                pady=10,
                font=("TkDefaultFont", 9, font_weight),
                cursor="hand2",
                command=lambda k=doc_key: self._select_doc(k),
            )
            btn.pack(fill=tk.X, padx=4, pady=2)

    def _highlight_info_type_button(self):
        for info_type, row in self.info_type_buttons.items():
            bg = "#444444" if info_type == self.current_info_type else "#333333"
            row.configure(bg=bg)
            for child in row.winfo_children():
                if isinstance(child, tk.Label) and child.cget("width") != 2:
                    child.configure(bg=bg)

    def _bind_keys(self):
        self.root.bind("<Left>",  lambda e: self._on_arrow_key("left"))
        self.root.bind("<Right>", lambda e: self._on_arrow_key("right"))
        self.root.bind("<Up>",    lambda e: self._on_arrow_key("up"))
        self.root.bind("<Down>",  lambda e: self._on_arrow_key("down"))
        self.root.bind("<Shift-Left>",  lambda e: self._on_arrow_key("left",  move=True))
        self.root.bind("<Shift-Right>", lambda e: self._on_arrow_key("right", move=True))
        self.root.bind("<Shift-Up>",    lambda e: self._on_arrow_key("up",    move=True))
        self.root.bind("<Shift-Down>",  lambda e: self._on_arrow_key("down",  move=True))
        self.root.bind("<Control-Left>",  lambda e: self._on_arrow_key("left",  expand=True))
        self.root.bind("<Control-Right>", lambda e: self._on_arrow_key("right", expand=True))
        self.root.bind("<Control-Up>",    lambda e: self._on_arrow_key("up",    expand=True))
        self.root.bind("<Control-Down>",  lambda e: self._on_arrow_key("down",  expand=True))
        self.root.bind("<Control-v>", lambda e: self._duplicate_selected_polygon())
        self.root.bind("<Control-V>", lambda e: self._duplicate_selected_polygon())
        self.root.bind("<Control-d>", lambda e: self._on_ctrl_page(+1))
        self.root.bind("<Control-D>", lambda e: self._on_ctrl_page(+1))
        self.root.bind("<Control-a>", lambda e: self._on_ctrl_page(-1))
        self.root.bind("<Control-A>", lambda e: self._on_ctrl_page(-1))
        self.root.bind("<Control-s>", lambda e: self._on_ctrl_s())
        self.root.bind("<Control-S>", lambda e: self._on_ctrl_s())
        self.root.bind("<Return>", self._on_enter_key)
        self.root.bind("<Escape>", self._on_escape_key)
        self.root.bind("<Shift-W>", self._on_shift_w)
        self.root.bind("<Shift-w>", self._on_shift_w)
        self.root.bind("<Shift-C>", self._on_shift_c)
        self.root.bind("<Shift-c>", self._on_shift_c)

        # Number shortcuts: 1-9 select label type by configured order.
        for n in range(1, 10):
            self.root.bind(f"<KeyPress-{n}>", lambda e, i=n: self._on_info_type_number(i))
            self.root.bind(f"<KP_{n}>", lambda e, i=n: self._on_info_type_number(i))

        self.canvas.bind("<ButtonPress-1>",   self._on_canvas_click)
        self.canvas.bind("<ButtonPress-3>",   self._on_canvas_right_click)
        self.canvas.bind("<B1-Motion>",       self._on_canvas_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_canvas_release)
        self.canvas.bind("<Motion>",          self._on_canvas_motion)
        self.canvas.bind("<Configure>",       self._on_canvas_resize)
        self.canvas.bind("<Button-4>",        lambda e: self._on_zoom_wheel(e, 1))    # X11 wheel up
        self.canvas.bind("<Button-5>",        lambda e: self._on_zoom_wheel(e, -1))   # X11 wheel down
        self.canvas.bind("<MouseWheel>",      self._on_zoom_wheel)                    # Win / macOS
        self.canvas.bind("<ButtonPress-2>",   self._on_pan_press)                     # middle-drag pan
        self.canvas.bind("<B2-Motion>",       self._on_pan_motion)
        self.root.bind("f",                   self._zoom_fit)                         # fit page

    # ── Page loading / saving ──────────────────────────────────────────────────

    def load_page(self, page_number):
        self.current_page = page_number
        self.page_index   = self.page_numbers.index(page_number)
        self._load_page_data(page_number)
        self._load_page_image(page_number)

        # Keep current_doc valid
        num_docs = self.page_data.get("num_documents", 1)
        valid_keys = {f"doc_{i}" for i in range(1, num_docs + 1)}
        if self.current_doc not in valid_keys:
            self.current_doc = "doc_1"

        self.selected_polygon_idx = None
        self.view         = None
        self.draw_mode    = False
        self.draw_vertices = []
        self.drag_mode    = None
        self.connect_mode = False
        self.connect_from = None

        self.num_docs_var.set(num_docs)
        self._rebuild_doc_buttons()
        self._highlight_info_type_button()
        self._refresh_queue()
        self._refresh_render_metrics()
        self._refresh_image()
        self._draw_scene()

        # Auto-select the topmost src_content so review starts hands-on-keys
        # (Ctrl+S then walks the ladder downward).
        self._auto_select_top_src_content()
        self._draw_scene()
        self._update_rerun_button()

        self.page_entry.delete(0, tk.END)
        self.page_entry.insert(0, str(page_number))
        self.mode_label.configure(text="")
        self._update_status()

    def _load_page_data(self, page_number):
        # If a reviewed version already exists, load that so previous edits are
        # preserved.  Otherwise fall back to the auto_labeled source.
        # (EDITOR_PREFER_AUTO=1 inverts this for the post-rerun review round.)
        output_path = self.output_doc_dir / f"page_{page_number:03d}" / f"page_{page_number:03d}.json"
        source_path = self.source_doc_dir / f"page_{page_number:03d}" / f"page_{page_number:03d}.json"

        if self.prefer_auto and source_path.exists():
            path = source_path
        elif output_path.exists():
            path = output_path
        elif source_path.exists():
            path = source_path
        else:
            raise FileNotFoundError(
                f"Label JSON not found for page {page_number} in reviewed/ or source."
            )

        with open(path, "r", encoding="utf-8") as f:
            self.page_data = json.load(f)

        legacy_key_map = {
            "document_content": "src_content",
            "header_data": "src_metadata",
            "commentary": "archv_commentary",
            "possessor_notes": "possessor",
        }

        # Ensure every info type exists in every document record
        for doc_record in self.page_data.get("documents", {}).values():
            # Backward compatibility: surface legacy polygons under the new keys.
            for legacy_key, new_key in legacy_key_map.items():
                legacy_polys = doc_record.get(legacy_key, [])
                new_polys = doc_record.get(new_key, [])
                if legacy_polys and not new_polys:
                    doc_record[new_key] = legacy_polys
            for info_type in LABEL_INFO_TYPES:
                if info_type not in doc_record:
                    doc_record[info_type] = []

        # Migrate polygons from the old flat vertex-list format to the current
        # {"vertices": [...], "connections": [...]} format.  This runs on every
        # load so both normalized source files and older final/ output files are
        # handled transparently without any manual conversion step.
        for doc_record in self.page_data.get("documents", {}).values():
            for info_type in LABEL_INFO_TYPES:
                polys = doc_record.get(info_type, [])
                for i, poly in enumerate(polys):
                    if isinstance(poly, list):
                        polys[i] = {"vertices": poly, "connections": []}

        # Upgrade polygon identity + connection addressing so links stay stable
        # even when polygon list indices change after insertions/deletions.
        self._upgrade_polygon_identity_and_connections()

        # Snapshot the loaded state: save_page_data() skips writing when nothing
        # changed, so merely BROWSING a page never copies machine labels into
        # reviewed/ (the 2026-06-09 Volume_4 pollution bug — browse-saves were later
        # mistaken for human-reviewed gold).
        self._loaded_snapshot = json.dumps(self.page_data, sort_keys=True)

    def _upgrade_polygon_identity_and_connections(self):
        """Ensure each polygon has a stable id and normalize connections to id refs.

        Why this exists:
        - The original connection format used {doc, type, index}, which breaks when
          a polygon is deleted because list indices shift.
        - Stable polygon ids prevent edges from accidentally sliding to a different
          polygon after edits.
        """
        docs = self.page_data.get("documents", {})

        # First pass: ensure every polygon has an id and record index→id mapping.
        index_to_id = {}
        for doc_key, doc_record in docs.items():
            for info_type in LABEL_INFO_TYPES:
                polys = doc_record.get(info_type, [])
                for idx, poly in enumerate(polys):
                    if not poly.get("id"):
                        poly["id"] = str(uuid4())
                    index_to_id[(doc_key, info_type, idx)] = poly["id"]

        # Second pass: migrate old connection entries that only had an index.
        # We preserve doc/type and add id whenever the indexed target exists.
        for doc_key, doc_record in docs.items():
            for info_type in LABEL_INFO_TYPES:
                polys = doc_record.get(info_type, [])
                for poly in polys:
                    migrated = []
                    for conn in poly.get("connections", []):
                        c_doc  = conn.get("doc")
                        c_type = conn.get("type")
                        c_id   = conn.get("id")
                        c_idx  = conn.get("index")

                        if c_id:
                            migrated.append({"doc": c_doc, "type": c_type, "id": c_id})
                            continue

                        if c_doc is None or c_type is None or c_idx is None:
                            continue

                        target_id = index_to_id.get((c_doc, c_type, c_idx))
                        if target_id:
                            migrated.append({"doc": c_doc, "type": c_type, "id": target_id})

                    # Remove duplicates while preserving order.
                    seen = set()
                    deduped = []
                    for conn in migrated:
                        key = (conn["doc"], conn["type"], conn["id"])
                        if key in seen:
                            continue
                        seen.add(key)
                        deduped.append(conn)
                    poly["connections"] = deduped

    def _load_page_image(self, page_number):
        page_pdf = self._resolve_page_pdf(page_number)
        images = convert_from_path(
            page_pdf,
            dpi        = RENDER_DPI,
            first_page = 1,
            last_page  = 1,
        )
        if self.page_image:
            self.page_image.close()
        self.page_image = images[0]
        self.original_width  = self.page_image.width
        self.original_height = self.page_image.height

    def save_page_data(self):
        self._prepare_page_data_for_save()
        # Dirty-check: only write when the page actually changed since load, so
        # browsing never creates fake "reviewed" copies of machine labels.
        current = json.dumps(self.page_data, sort_keys=True)
        if current == getattr(self, "_loaded_snapshot", None):
            return
        page_dir = self.output_doc_dir / f"page_{self.current_page:03d}"
        page_dir.mkdir(parents=True, exist_ok=True)
        path = page_dir / f"page_{self.current_page:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.page_data, f, indent=2)
        self._loaded_snapshot = current

    def _prepare_page_data_for_save(self):
        """Normalize page data before writing JSON.

        Order of operations:
        1) Persist the current number of documents.
        2) Remove document entries above that count.
        """
        try:
            num_docs = int(self.num_docs_var.get())
        except (ValueError, tk.TclError):
            num_docs = int(self.page_data.get("num_documents", 1))
        num_docs = max(1, min(20, num_docs))
        self.page_data["num_documents"] = num_docs

        docs = self.page_data.setdefault("documents", {})
        max_allowed_idx = num_docs
        for doc_key in list(docs.keys()):
            if not doc_key.startswith("doc_"):
                continue
            try:
                idx = int(doc_key.split("_", 1)[1])
            except ValueError:
                continue
            if idx > max_allowed_idx:
                del docs[doc_key]

    # ── Navigation ─────────────────────────────────────────────────────────────

    def previous_page(self):
        self._cancel_draw_if_active()
        self.save_page_data()
        if self.page_index > 0:
            self.load_page(self.page_numbers[self.page_index - 1])

    def next_page(self):
        self._cancel_draw_if_active()
        self.save_page_data()
        if self.page_index < self.total_pages - 1:
            self.load_page(self.page_numbers[self.page_index + 1])

    def _on_ctrl_page(self, direction):
        """Ctrl+D = next page, Ctrl+A = previous page (auto-saves, like the
        buttons). Inert while a text field has focus so Ctrl+A keeps its normal
        select-all behaviour in the page-number / doc-count entries."""
        if self._focus_is_text_input():
            return None
        (self.next_page if direction > 0 else self.previous_page)()
        return "break"

    # ── src_content ladder (top-down review) ──────────────────────────────────

    def _src_content_ladder(self):
        """Indices of the current doc's src_content polygons, ordered by
        vertical position (top of page first; ties broken left-to-right)."""
        polys = (self.page_data.get("documents", {})
                 .get(self.current_doc, {}).get("src_content", []))
        order = []
        for idx, p in enumerate(polys):
            vs = p.get("vertices", [])
            if len(vs) >= 3:
                cy = sum(v["y"] for v in vs) / len(vs)
                cx = sum(v["x"] for v in vs) / len(vs)
                order.append((cy, cx, idx))
        order.sort()
        return [idx for _cy, _cx, idx in order]

    def _select_src_content(self, idx):
        self._cancel_draw_if_active()
        self.current_info_type    = "src_content"
        self.selected_polygon_idx = idx
        self._highlight_info_type_button()
        self._ensure_selected_visible()
        self._draw_scene()

    def _auto_select_top_src_content(self):
        """On page load: select the highest src_content so arrow-key tightening
        can start immediately. No-op on pages without src_content."""
        ladder = self._src_content_ladder()
        if ladder:
            self.current_info_type    = "src_content"
            self.selected_polygon_idx = ladder[0]
            self._highlight_info_type_button()

    def _on_ctrl_s(self):
        """Ctrl+S: step DOWN the page's src_content ladder (wraps to the top
        after the bottom box). Starts at the top if nothing relevant is selected."""
        if self._focus_is_text_input():
            return None
        ladder = self._src_content_ladder()
        if not ladder:
            return "break"
        if self.current_info_type == "src_content" and self.selected_polygon_idx in ladder:
            nxt = ladder[(ladder.index(self.selected_polygon_idx) + 1) % len(ladder)]
        else:
            nxt = ladder[0]
        self._select_src_content(nxt)
        return "break"

    def _ensure_selected_visible(self):
        """Pan (never zoom) so the selected polygon's center is on-screen —
        keeps the Ctrl+S ladder usable while zoomed in for tightening."""
        polys = self._current_polygons()
        if self.selected_polygon_idx is None or self.selected_polygon_idx >= len(polys):
            return
        cx, cy = self._centroid_canvas(polys[self.selected_polygon_idx]["vertices"])
        w = self.canvas.winfo_width()
        h = self.canvas.winfo_height()
        if w <= 1 or h <= 1:
            return
        margin = 40
        if not (margin <= cx <= w - margin and margin <= cy <= h - margin):
            self.image_offset_x += w / 2 - cx
            self.image_offset_y += h / 2 - cy
            self.view = ("custom", self.display_scale,
                         self.image_offset_x, self.image_offset_y)

    def go_to_page(self):
        self._cancel_draw_if_active()
        try:
            requested = int(self.page_entry.get())
        except ValueError:
            return

        # If the exact page exists in our list, jump to it.
        # Otherwise jump to the nearest available page.
        if requested in self.page_numbers:
            target = requested
        else:
            target = min(self.page_numbers, key=lambda p: abs(p - requested))

        self.save_page_data()
        self.load_page(target)

    # ── Selection: doc / info type ─────────────────────────────────────────────

    def _select_doc(self, doc_key):
        if doc_key == self.current_doc:
            return
        self._cancel_draw_if_active()
        self.current_doc = doc_key
        self.selected_polygon_idx = None
        self._rebuild_doc_buttons()
        self._draw_scene()
        self._update_status()

    def _select_info_type(self, info_type):
        self._cancel_draw_if_active()
        self.current_info_type    = info_type
        self.selected_polygon_idx = None
        self._highlight_info_type_button()
        self._draw_scene()
        self._update_status()

    def _select_info_type_by_number(self, number):
        idx = number - 1
        if 0 <= idx < len(LABEL_INFO_TYPES):
            self._select_info_type(LABEL_INFO_TYPES[idx])

    def _on_num_docs_changed(self, *_):
        try:
            num_docs = int(self.num_docs_var.get())
        except (ValueError, tk.TclError):
            return
        num_docs = max(1, min(20, num_docs))
        self.num_docs_var.set(num_docs)
        self.page_data["num_documents"] = num_docs

        docs = self.page_data.setdefault("documents", {})
        for i in range(1, num_docs + 1):
            key = f"doc_{i}"
            if key not in docs:
                docs[key] = {info_type: [] for info_type in LABEL_INFO_TYPES}

        # Keep current_doc within the new range
        valid_keys = {f"doc_{i}" for i in range(1, num_docs + 1)}
        if self.current_doc not in valid_keys:
            self.current_doc = "doc_1"

        self._rebuild_doc_buttons()
        self._draw_scene()
        self._update_status()

    def _merge_doc_into_previous(self):
        """Merge the CURRENT document into the previous one (doc_N -> doc_N-1).

        For pages over-split into documents (e.g. front/back of one card): all of
        doc_N's boxes move into doc_N-1, every later document is renumbered down by
        one, and connection {doc,...} references are remapped to match. Adjacent
        merge only — to merge doc_2 and doc_3, select doc_3 and Merge ▲."""
        try:
            n = int(self.current_doc.split("_", 1)[1])
        except (ValueError, IndexError):
            return
        if n <= 1:
            messagebox.showinfo("Merge docs", "doc_1 has no previous document to merge into.")
            return
        docs = self.page_data.get("documents", {})
        src_name, dst_name = f"doc_{n}", f"doc_{n - 1}"
        if src_name not in docs:
            return
        if not messagebox.askyesno(
                "Merge docs",
                f"Merge {src_name} into {dst_name}?\n\nAll of its boxes move into {dst_name} "
                f"and later documents are renumbered down by one."):
            return
        self._cancel_draw_if_active()
        # 1) move every polygon into the previous document
        dst = docs.setdefault(dst_name, {})
        for cat, polys in (docs.get(src_name) or {}).items():
            if isinstance(polys, list) and polys:
                dst.setdefault(cat, []).extend(polys)
        del docs[src_name]
        # 2) renumber all later documents down by one
        total = int(self.page_data.get("num_documents", len(docs) + 1))
        rename = {src_name: dst_name}
        for i in range(n + 1, total + 1):
            old, new = f"doc_{i}", f"doc_{i - 1}"
            if old in docs:
                docs[new] = docs.pop(old)
                rename[old] = new
        # 3) remap connection doc references everywhere on the page
        for doc in docs.values():
            if not isinstance(doc, dict):
                continue
            for polys in doc.values():
                if not isinstance(polys, list):
                    continue
                for p in polys:
                    for cn in (p.get("connections") or []):
                        if cn.get("doc") in rename:
                            cn["doc"] = rename[cn["doc"]]
        # 4) keep keys in doc_1..doc_N order for clean JSON
        def _idx(k):
            try:
                return int(k.split("_", 1)[1])
            except (ValueError, IndexError):
                return 999
        self.page_data["documents"] = {k: docs[k] for k in sorted(docs, key=_idx)}
        # 5) bookkeeping
        self.page_data["num_documents"] = max(1, total - 1)
        self.num_docs_var.set(self.page_data["num_documents"])
        self.current_doc = dst_name
        self.selected_polygon_idx = None
        self._rebuild_doc_buttons()
        self._draw_scene()
        self._update_status()

    def _split_doc_duplicate(self):
        """Dual of Merge ▲: insert a new doc_N+1 holding a COPY of every box in the
        current doc_N (fresh ids, no connections). For a page that should be MORE
        documents (two notes written on one thing): split, then in each of doc_N /
        doc_N+1 just delete the boxes that belong to the other note — no redrawing.
        Later documents are renumbered up by one (connection refs remapped)."""
        try:
            n = int(self.current_doc.split("_", 1)[1])
        except (ValueError, IndexError):
            return
        docs = self.page_data.get("documents", {})
        src_name = f"doc_{n}"
        if src_name not in docs:
            return
        total = int(self.page_data.get("num_documents", len(docs)))
        if total >= 20:
            messagebox.showinfo("Split doc", "Max 20 documents per page.")
            return
        if not messagebox.askyesno(
                "Split doc",
                f"Duplicate {src_name} into a new doc_{n + 1}?\n\nEvery box is copied into "
                f"both; delete from each until the two documents are partitioned."):
            return
        self._cancel_draw_if_active()
        # 1) renumber later documents UP by one (descending, to avoid collisions)
        rename = {}
        for i in range(total, n, -1):
            old, new = f"doc_{i}", f"doc_{i + 1}"
            if old in docs:
                docs[new] = docs.pop(old)
                rename[old] = new
        # 2) new doc_{n+1} = copy of doc_n with fresh ids and no connections
        copy_doc = {}
        for cat, polys in (docs.get(src_name) or {}).items():
            if not isinstance(polys, list):
                continue
            copy_doc[cat] = [{"id": str(uuid4()),
                              "vertices": [dict(v) for v in p.get("vertices", [])],
                              "connections": []}
                             for p in polys]
        docs[f"doc_{n + 1}"] = copy_doc
        # 3) remap connection doc references for the renumbered docs
        for doc in docs.values():
            if not isinstance(doc, dict):
                continue
            for polys in doc.values():
                if not isinstance(polys, list):
                    continue
                for p in polys:
                    for cn in (p.get("connections") or []):
                        if cn.get("doc") in rename:
                            cn["doc"] = rename[cn["doc"]]
        # 4) keep keys ordered + bookkeeping
        def _idx2(k):
            try:
                return int(k.split("_", 1)[1])
            except (ValueError, IndexError):
                return 999
        self.page_data["documents"] = {k: docs[k] for k in sorted(docs, key=_idx2)}
        self.page_data["num_documents"] = total + 1
        self.num_docs_var.set(total + 1)
        self.current_doc = f"doc_{n + 1}"
        self.selected_polygon_idx = None
        self._rebuild_doc_buttons()
        self._draw_scene()
        self._update_status()

    # ── Polygon management ─────────────────────────────────────────────────────

    def _current_polygons(self):
        """Return the mutable list of polygons for the active doc + info type."""
        return (
            self.page_data
            .get("documents", {})
            .get(self.current_doc, {})
            .get(self.current_info_type, [])
        )

    def _start_draw_mode(self):
        self.draw_mode    = True
        self.draw_vertices = []
        self.selected_polygon_idx = None
        self.mode_label.configure(text="DRAW MODE")
        self._draw_scene()

    def _cancel_draw_if_active(self):
        if self.draw_mode:
            self._cancel_draw()

    def _cancel_draw(self):
        self.draw_mode    = False
        self.draw_vertices = []
        self.mode_label.configure(text="")
        self._draw_scene()

    def _close_polygon(self):
        if len(self.draw_vertices) < 3:
            return
        vertices = [{"x": int(round(v["x"])), "y": int(round(v["y"]))}
                    for v in self.draw_vertices]
        polygon  = {"id": str(uuid4()), "vertices": vertices, "connections": []}
        polygons = self._current_polygons()
        polygons.append(polygon)
        self.selected_polygon_idx = len(polygons) - 1
        self.draw_mode    = False
        self.draw_vertices = []
        self.mode_label.configure(text="")
        self._draw_scene()
        self._update_status()

    def _delete_selected_polygon(self):
        if self.selected_polygon_idx is None:
            return
        polygons = self._current_polygons()
        if 0 <= self.selected_polygon_idx < len(polygons):
            # Remove all incoming/outgoing edges for the polygon before deleting it.
            doomed = polygons[self.selected_polygon_idx]
            doomed_addr = {
                "doc": self.current_doc,
                "type": self.current_info_type,
                "id": doomed.get("id"),
                "index": self.selected_polygon_idx,
            }
            self._remove_all_references_to_addr(doomed_addr)

            polygons.pop(self.selected_polygon_idx)
            self.selected_polygon_idx = None

            # If connect mode was waiting on this polygon as source, clear it.
            if self.connect_from is not None and self._addr_equal(self.connect_from, doomed_addr):
                self.connect_from = None
                self.mode_label.configure(text="CONNECT — click first polygon")

            self._draw_scene()
            self._update_status()

    # ── Duplicate / recategorize ─────────────────────────────────────────────────
    def _duplicate_selected_polygon(self):
        """Ctrl+V: copy the selected polygon (same category), offset slightly, and
        auto-select the copy so it can be slid into place with the arrow keys."""
        if self._focus_is_text_input():
            return
        polygons = self._current_polygons()
        idx = self.selected_polygon_idx
        if idx is None or not (0 <= idx < len(polygons)):
            return
        src = polygons[idx]
        off = 30
        maxx, maxy = self.original_width - 1, self.original_height - 1
        new_verts = [{"x": float(min(maxx, v["x"] + off)), "y": float(min(maxy, v["y"] + off))}
                     for v in src["vertices"]]
        polygons.append({"id": str(uuid4()), "vertices": new_verts, "connections": []})
        self.selected_polygon_idx = len(polygons) - 1     # auto-select the copy
        self._draw_scene()
        self._update_status()

    def _recategorize_selected(self, new_type):
        """Move the selected polygon to a different category (Shift-click a category
        in the right panel), updating incoming connection refs so links survive."""
        idx = self.selected_polygon_idx
        if idx is None:
            return
        polygons = self._current_polygons()
        if not (0 <= idx < len(polygons)) or new_type == self.current_info_type:
            return
        old_type = self.current_info_type
        poly = polygons.pop(idx)
        pid = poly.get("id")
        doc_rec = self.page_data.setdefault("documents", {}).setdefault(self.current_doc, {})
        doc_rec.setdefault(new_type, []).append(poly)
        # repoint any connection that addressed this polygon under its OLD category
        if pid:
            for dr in self.page_data.get("documents", {}).values():
                for it in LABEL_INFO_TYPES:
                    for p in dr.get(it, []):
                        for c in p.get("connections", []):
                            if (c.get("doc") == self.current_doc and c.get("type") == old_type
                                    and c.get("id") == pid):
                                c["type"] = new_type
        self.current_info_type = new_type                 # follow the polygon
        self.selected_polygon_idx = len(doc_rec[new_type]) - 1
        self._highlight_info_type_button()
        self._draw_scene()
        self._update_status()

    # ── Arrow-key box editing ────────────────────────────────────────────────────
    def _on_arrow_key(self, direction, move=False, expand=False):
        """Arrow keys edit the SELECTED box:
          • plain arrow  → shrink: pull the FAR edge inward toward the arrow
            (up=bottom edge up, down=top edge down, left=right edge left,
            right=left edge right);
          • Ctrl+arrow   → expand: push the NEAR edge (the opposite pair) outward
            in the arrow's direction (Ctrl+up=top edge up, Ctrl+down=bottom edge
            down, Ctrl+left=left edge left, Ctrl+right=right edge right);
          • Shift+arrow  → translate the whole box one step in that direction.
        With no box selected, Left/Right still navigate pages (unchanged). Text
        fields and the review-queue list keep their native arrow behavior."""
        focused = self.root.focus_get()
        if self._focus_is_text_input() or isinstance(focused, tk.Listbox):
            return
        polygons = self._current_polygons()
        idx = self.selected_polygon_idx
        if (idx is not None and 0 <= idx < len(polygons)
                and len(polygons[idx]["vertices"]) >= 3 and not self.draw_mode):
            if move:
                self._translate_selected_polygon(direction)
            else:
                self._resize_selected_edge(direction, expand=expand)
            return "break"
        # No selection: preserve page navigation (only plain Left/Right).
        if not move and not expand:
            if direction == "left":
                self.previous_page()
            elif direction == "right":
                self.next_page()
        return "break"

    def _resize_selected_edge(self, direction, expand=False):
        """Move one edge of the selected box by NUDGE_STEP. Plain (expand=False)
        pulls the far edge inward (shrink); expand=True pushes the near edge
        outward (grow). Shrink is clamped to a minimum box size; expand is clamped
        to the page bounds."""
        verts = self._current_polygons()[self.selected_polygon_idx]["vertices"]
        xs = [v["x"] for v in verts]; ys = [v["y"] for v in verts]
        x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        midx, midy = (x0 + x1) / 2.0, (y0 + y1) / 2.0
        maxx, maxy = self.original_width - 1, self.original_height - 1

        if direction in ("up", "down"):
            if expand:                               # near edge grows outward in arrow dir
                if direction == "up":                # top pair up
                    step = min(NUDGE_STEP, y0); near, d = lambda v: v["y"] < midy, -1
                else:                                # bottom pair down
                    step = min(NUDGE_STEP, maxy - y1); near, d = lambda v: v["y"] > midy, 1
            else:                                    # far edge pulled in toward arrow
                step = min(NUDGE_STEP, max(0.0, (y1 - y0) - NUDGE_MIN_SIZE))
                if direction == "up":                # bottom pair up
                    near, d = lambda v: v["y"] > midy, -1
                else:                                # top pair down
                    near, d = lambda v: v["y"] < midy, 1
            if step <= 0:
                return
            for v in verts:
                if near(v): v["y"] += d * step
        else:
            if expand:
                if direction == "left":              # left pair left
                    step = min(NUDGE_STEP, x0); near, d = lambda v: v["x"] < midx, -1
                else:                                # right pair right
                    step = min(NUDGE_STEP, maxx - x1); near, d = lambda v: v["x"] > midx, 1
            else:
                step = min(NUDGE_STEP, max(0.0, (x1 - x0) - NUDGE_MIN_SIZE))
                if direction == "left":              # right pair left
                    near, d = lambda v: v["x"] > midx, -1
                else:                                # left pair right
                    near, d = lambda v: v["x"] < midx, 1
            if step <= 0:
                return
            for v in verts:
                if near(v): v["x"] += d * step
        self._draw_scene()
        self._update_status()

    def _translate_selected_polygon(self, direction):
        verts = self._current_polygons()[self.selected_polygon_idx]["vertices"]
        dx, dy = 0.0, 0.0
        if   direction == "left":  dx = -NUDGE_STEP
        elif direction == "right": dx =  NUDGE_STEP
        elif direction == "up":    dy = -NUDGE_STEP
        elif direction == "down":  dy =  NUDGE_STEP
        # Clamp the slide so the whole box stays on the page (shape unchanged).
        xs = [v["x"] for v in verts]; ys = [v["y"] for v in verts]
        maxx, maxy = self.original_width - 1, self.original_height - 1
        if dx < 0: dx = -min(-dx, min(xs))
        if dx > 0: dx =  min(dx, maxx - max(xs))
        if dy < 0: dy = -min(-dy, min(ys))
        if dy > 0: dy =  min(dy, maxy - max(ys))
        if dx == 0 and dy == 0:
            return
        for v in verts:
            v["x"] += dx; v["y"] += dy
        self._draw_scene()
        self._update_status()

    # ── Connections ────────────────────────────────────────────────────────────
    # Each polygon is stored as {"vertices": [...], "connections": [...]}.
    # A connection entry is {"doc": "doc_1", "type": "src_content", "id": "..."},
    # which is the address of the OTHER endpoint.  Every connection is stored
    # bidirectionally, so both polygons record the edge.

    def _addr_equal(self, a, b):
        """True when two polygon addresses point to the same polygon.

        Prefers stable id matching.  Falls back to index matching only when one
        or both addresses do not carry ids (legacy compatibility path).
        """
        if a.get("doc") != b.get("doc") or a.get("type") != b.get("type"):
            return False

        a_id = a.get("id")
        b_id = b.get("id")
        if a_id and b_id:
            return a_id == b_id

        return a.get("index") == b.get("index")

    def _start_connect_mode(self):
        self._cancel_draw_if_active()
        self.connect_mode = True
        self.connect_from = None
        self.mode_label.configure(text="CONNECT — click first polygon")
        self._draw_scene()

    def _cancel_connect(self):
        self.connect_mode = False
        self.connect_from = None
        self.mode_label.configure(text="")
        self._draw_scene()

    def _get_polygon_by_addr(self, addr):
        """Return the polygon dict at address {"doc", "type", "id"|"index"}, or None."""
        polys = (
            self.page_data
            .get("documents", {})
            .get(addr["doc"], {})
            .get(addr["type"], [])
        )

        # Prefer id lookup so references remain valid across list reordering.
        target_id = addr.get("id")
        if target_id:
            for poly in polys:
                if poly.get("id") == target_id:
                    return poly

        # Legacy fallback for old in-memory addresses that still use index.
        idx = addr.get("index")
        if isinstance(idx, int) and 0 <= idx < len(polys):
            return polys[idx]
        return None

    def _find_polygon_at(self, ex, ey):
        """Return the address {"doc", "type", "id", "index"} of whichever polygon in the
        currently visible document contains canvas point (ex, ey), or None.
        Only the current doc is searched because the other docs are not rendered.
        """
        doc_record = self.page_data.get("documents", {}).get(self.current_doc, {})
        for info_type in LABEL_INFO_TYPES:
            for idx, polygon in enumerate(doc_record.get(info_type, [])):
                vertices = polygon["vertices"]
                if len(vertices) < 3:
                    continue
                if _point_in_polygon(ex, ey, self._poly_to_canvas(vertices)):
                    return {
                        "doc": self.current_doc,
                        "type": info_type,
                        "id": polygon.get("id"),
                        "index": idx,
                    }
        return None

    def _remove_all_references_to_addr(self, doomed_addr):
        """Remove every connection edge pointing to the provided polygon address."""
        docs = self.page_data.get("documents", {})
        for doc_record in docs.values():
            for info_type in LABEL_INFO_TYPES:
                for poly in doc_record.get(info_type, []):
                    conns = poly.get("connections", [])
                    poly["connections"] = [
                        c for c in conns
                        if not self._addr_equal(c, doomed_addr)
                    ]

    def _toggle_connection(self, addr_a, addr_b):
        """Add a bidirectional edge between two polygon addresses, or remove it if
        it already exists (toggle).  The connection is recorded in each polygon's
        own connections list so the graph can be traversed from either endpoint.
        """
        poly_a = self._get_polygon_by_addr(addr_a)
        poly_b = self._get_polygon_by_addr(addr_b)
        if poly_a is None or poly_b is None:
            return

        conns_a = poly_a.setdefault("connections", [])
        conns_b = poly_b.setdefault("connections", [])
        already = any(self._addr_equal(c, addr_b) for c in conns_a)

        if already:
            # Remove the edge from both sides.
            poly_a["connections"] = [c for c in conns_a if not self._addr_equal(c, addr_b)]
            poly_b["connections"] = [c for c in conns_b if not self._addr_equal(c, addr_a)]
        else:
            # Add the edge to both sides.
            conns_a.append({"doc": addr_b["doc"], "type": addr_b["type"], "id": addr_b.get("id")})
            conns_b.append({"doc": addr_a["doc"], "type": addr_a["type"], "id": addr_a.get("id")})

    def _handle_connect_click(self, event):
        """Handle a canvas click while in connect mode.
        First click picks the source polygon; second click creates/toggles the edge.
        Clicking the same polygon twice deselects it.  Stays in connect mode after
        each successful connection so the user can link multiple pairs in a row.
        """
        clicked = self._find_polygon_at(event.x, event.y)
        if clicked is None:
            return  # clicked empty space — nothing to do

        # Keep the UI selection synchronized with whatever polygon the user just
        # clicked in connect mode.  This ensures connection lines are drawn for
        # the polygon currently being operated on, not a stale prior selection.
        self.current_doc          = clicked["doc"]
        self.current_info_type    = clicked["type"]
        self.selected_polygon_idx = clicked["index"]
        self._rebuild_doc_buttons()
        self._highlight_info_type_button()

        if self.connect_from is None:
            # First click: mark this polygon as the source.
            self.connect_from = clicked
            self.mode_label.configure(text="CONNECT — click second polygon")
            self._draw_scene()
            self._update_status()
            return

        # Check for same polygon (cancel source selection without creating an edge).
        if self._addr_equal(clicked, self.connect_from):
            self.connect_from = None
            self.mode_label.configure(text="CONNECT — click first polygon")
            self._draw_scene()
            self._update_status()
            return

        # Second click on a different polygon — toggle the connection.
        self._toggle_connection(self.connect_from, clicked)
        self.connect_from = None
        self.mode_label.configure(text="CONNECT — click first polygon")
        self._draw_scene()
        self._update_status()

    def _centroid_canvas(self, vertices):
        """Return the canvas-space (cx, cy) centroid of a list of original-coord vertices."""
        n  = len(vertices)
        ox = sum(v["x"] for v in vertices) / n
        oy = sum(v["y"] for v in vertices) / n
        return self._original_to_canvas(ox, oy)

    def _draw_all_connections(self):
        """Overlay EVERY connection involving a src_content polygon in the current
        document at once ("Show connections" toolbar toggle) — no clicking through
        boxes. Each src_content box is ranked by its vertical position and colored
        from connection_palette(): a perceptually-even red->violet sweep, dealt
        outside-in when entries are too many for adjacent hues to differ; its
        connection lines, endpoint dots, and partner-box rings share that color.
        Lines are stippled and rings dashed so the underlying page stays readable
        with everything visible at once. Read-only drawing — touches no data.
        """
        doc_record = self.page_data.get("documents", {}).get(self.current_doc, {})
        ranked = []
        for poly in doc_record.get("src_content", []):
            verts = poly.get("vertices", [])
            if len(verts) >= 3 and poly.get("connections"):
                ranked.append((sum(v["y"] for v in verts) / len(verts), poly))
        if not ranked:
            return
        ranked.sort(key=lambda t: t[0])

        drawn_pairs = set()   # a content<->content edge lives on both endpoints
        palette = connection_palette(len(ranked))
        for rank, (_cy, poly) in enumerate(ranked):
            color = palette[rank]
            sx, sy = self._centroid_canvas(poly["vertices"])

            for conn in poly.get("connections", []):
                target = self._get_polygon_by_addr(conn)
                if target is None:
                    continue
                tverts = target.get("vertices", [])
                if len(tverts) < 3:
                    continue
                pair = frozenset((poly.get("id"), target.get("id")))
                if pair in drawn_pairs:
                    continue
                drawn_pairs.add(pair)
                tx, ty = self._centroid_canvas(tverts)
                self.canvas.create_line(sx, sy, tx, ty,
                                        fill=color, width=4, stipple="gray50")
                for cx2, cy2 in ((sx, sy), (tx, ty)):
                    self.canvas.create_oval(cx2 - 4, cy2 - 4, cx2 + 4, cy2 + 4,
                                            fill=color, outline="")
                # dashed ring around the partner box so the grouping reads at a glance
                self.canvas.create_polygon(
                    self._flatten(self._poly_to_canvas(tverts)),
                    fill="", outline=color, width=2, dash=(3, 3),
                )
            # solid thin ring around the src_content box itself
            self.canvas.create_polygon(
                self._flatten(self._poly_to_canvas(poly["vertices"])),
                fill="", outline=color, width=2,
            )

    def _draw_connections(self):
        """Draw connection lines for the currently selected polygon.
        Lines run between centroids.  Only connections whose target polygon is also
        in the currently visible document are rendered (since other docs are hidden).
        """
        if self.selected_polygon_idx is None:
            return
        polygons = self._current_polygons()
        if self.selected_polygon_idx >= len(polygons):
            return

        active_polygon = polygons[self.selected_polygon_idx]
        connections    = active_polygon.get("connections", [])
        if not connections:
            return

        src_cx, src_cy = self._centroid_canvas(active_polygon["vertices"])

        for conn in connections:
            target = self._get_polygon_by_addr(conn)
            if target is None:
                continue
            vertices = target.get("vertices", [])
            if len(vertices) < 3:
                continue
            tgt_cx, tgt_cy = self._centroid_canvas(vertices)

            # Dashed line between the two polygon centroids.
            self.canvas.create_line(
                src_cx, src_cy, tgt_cx, tgt_cy,
                fill="#00ffcc", width=2, dash=(6, 4),
            )
            # Small dot at each endpoint so it is clear where the line terminates.
            r = 5
            for cx, cy in ((src_cx, src_cy), (tgt_cx, tgt_cy)):
                self.canvas.create_oval(
                    cx - r, cy - r, cx + r, cy + r,
                    fill="#00ffcc", outline="white", width=1,
                )

    def _draw_connect_rubber_band(self):
        """While in connect mode with a first polygon already chosen, draw a live
        dashed line from that polygon's centroid to the mouse cursor so the user
        can see what they are about to connect.
        """
        poly = self._get_polygon_by_addr(self.connect_from)
        if poly is None:
            return
        vertices = poly.get("vertices", [])
        if len(vertices) < 3:
            return
        src_cx, src_cy = self._centroid_canvas(vertices)
        self.canvas.create_line(
            src_cx, src_cy, self.mouse_x, self.mouse_y,
            fill="#00ffcc", width=1, dash=(4, 4),
        )

    # ── Coordinate conversion ──────────────────────────────────────────────────

    def _canvas_to_original(self, cx, cy):
        return (
            (cx - self.image_offset_x) / self.display_scale,
            (cy - self.image_offset_y) / self.display_scale,
        )

    def _original_to_canvas(self, ox, oy):
        return (
            ox * self.display_scale + self.image_offset_x,
            oy * self.display_scale + self.image_offset_y,
        )

    def _poly_to_canvas(self, polygon):
        return [
            {
                "x": p["x"] * self.display_scale + self.image_offset_x,
                "y": p["y"] * self.display_scale + self.image_offset_y,
            }
            for p in polygon
        ]

    def _clamp(self, ox, oy):
        return (
            max(0.0, min(float(self.original_width  - 1), ox)),
            max(0.0, min(float(self.original_height - 1), oy)),
        )

    # ── Render metrics ─────────────────────────────────────────────────────────

    def _refresh_render_metrics(self):
        cw = max(1, self.canvas.winfo_width())
        ch = max(1, self.canvas.winfo_height())
        fit = min((cw - 16) / max(1, self.original_width),
                  (ch - 16) / max(1, self.original_height))
        cap = min(4000 / max(1, self.original_width),           # cap rendered image (~<16MP)
                  4000 / max(1, self.original_height))
        view = getattr(self, "view", None)
        if view and view[0] == "flag":
            bx0, by0, bx1, by1 = view[1]
            bw, bh = max(bx1 - bx0, 1), max(by1 - by0, 1)
            z = min((cw * 0.45) / bw, (ch * 0.30) / bh)        # flag ~30-45% of the canvas
            z = min(max(fit, min(z, fit * 8)), cap)             # never below fit
            self.display_scale   = z
            self.rendered_width  = max(1, int(round(self.original_width  * z)))
            self.rendered_height = max(1, int(round(self.original_height * z)))
            self.image_offset_x  = int(cw / 2 - (bx0 + bx1) / 2 * z)
            self.image_offset_y  = int(ch / 2 - (by0 + by1) / 2 * z)
        elif view and view[0] == "custom":
            z = min(max(view[1], fit * 0.5), cap)               # user wheel-zoom level
            self.display_scale   = z
            self.rendered_width  = max(1, int(round(self.original_width  * z)))
            self.rendered_height = max(1, int(round(self.original_height * z)))
            self.image_offset_x  = int(view[2])
            self.image_offset_y  = int(view[3])
        else:
            self.display_scale   = fit
            self.rendered_width  = max(1, int(round(self.original_width  * fit)))
            self.rendered_height = max(1, int(round(self.original_height * fit)))
            self.image_offset_x  = max(0, (cw - self.rendered_width)  // 2)
            self.image_offset_y  = max(0, (ch - self.rendered_height) // 2)

    def _refresh_image(self):
        self._refresh_render_metrics()
        resized = self.page_image.resize((self.rendered_width, self.rendered_height))
        self.tk_image = ImageTk.PhotoImage(resized)

    def _zoom_to_flag(self, bbox):
        self.view = ("flag", tuple(bbox))
        self._refresh_image()
        self._draw_scene()

    def _zoom_fit(self, event=None):
        if self.view is not None:
            self.view = None
            self._refresh_image()
            self._draw_scene()
        return "break"

    def _on_zoom_wheel(self, event, direction=None):
        """Mouse-wheel zoom, anchored on the cursor (the page point under the
        pointer stays put). 'f' fits the whole page again."""
        delta = direction if direction is not None else (1 if getattr(event, "delta", 0) > 0 else -1)
        factor = 1.25 if delta > 0 else 0.8
        old = self.display_scale
        new = old * factor
        px = (event.x - self.image_offset_x) / old
        py = (event.y - self.image_offset_y) / old
        self.view = ("custom", new, event.x - px * new, event.y - py * new)
        self._refresh_image()
        self._draw_scene()
        return "break"

    def _on_pan_press(self, event):
        self._pan_anchor = (event.x, event.y, self.image_offset_x, self.image_offset_y)

    def _on_pan_motion(self, event):
        if not self._pan_anchor:
            return
        sx, sy, ox, oy = self._pan_anchor
        self.image_offset_x = ox + (event.x - sx)
        self.image_offset_y = oy + (event.y - sy)
        self.view = ("custom", self.display_scale, self.image_offset_x, self.image_offset_y)
        self._draw_scene()

    # ── Drawing ────────────────────────────────────────────────────────────────

    def _flatten(self, canvas_poly):
        flat = []
        for p in canvas_poly:
            flat.extend([p["x"], p["y"]])
        return flat

    def _draw_scene(self):
        self.canvas.delete("all")
        if self.tk_image:
            self.canvas.create_image(
                self.image_offset_x, self.image_offset_y,
                anchor=tk.NW, image=self.tk_image,
            )
        self._draw_all_polygons()
        if self.show_conns_var.get():
            self._draw_all_connections()
        self._draw_connections()
        self._draw_queue_highlight()
        if self.draw_mode:
            self._draw_in_progress()
        if self.connect_mode and self.connect_from is not None:
            self._draw_connect_rubber_band()

    def _draw_all_polygons(self):
        docs = self.page_data.get("documents", {})
        doc_record = docs.get(self.current_doc, {})

        # Only render polygons for the active document so other documents stay hidden.
        for info_type in LABEL_INFO_TYPES:
            color = LABEL_INFO_TYPE_COLORS.get(info_type, LABEL_DEFAULT_COLOR)
            polygons = doc_record.get(info_type, [])
            is_active_type = info_type == self.current_info_type

            for poly_idx, polygon in enumerate(polygons):
                vertices = polygon["vertices"]
                if len(vertices) < 3:
                    continue

                is_selected = is_active_type and poly_idx == self.selected_polygon_idx
                canvas_poly = self._poly_to_canvas(vertices)
                flat = self._flatten(canvas_poly)

                # Keep selected polygon obvious without fully covering underlying text.
                if is_selected:
                    stipple = "gray50"
                    outline_col = "white"
                    width = 3
                elif is_active_type:
                    stipple = "gray50"
                    outline_col = color
                    width = 2
                else:
                    stipple = "gray25"
                    outline_col = "#555555"
                    width = 1

                self.canvas.create_polygon(
                    flat,
                    fill=color,
                    stipple=stipple,
                    outline=outline_col,
                    width=width,
                )

                # Vertex handles only for the selected polygon
                if is_selected:
                    for pt in canvas_poly:
                        self.canvas.create_oval(
                            pt["x"] - HANDLE_RADIUS, pt["y"] - HANDLE_RADIUS,
                            pt["x"] + HANDLE_RADIUS, pt["y"] + HANDLE_RADIUS,
                            fill="#ff5f5f", outline="white", width=1,
                        )

                # Bright outline on the connect_from polygon so the user can see
                # which polygon they picked as the source of the pending connection.
                if (self.connect_mode and self.connect_from is not None
                        and self.connect_from["doc"]   == self.current_doc
                        and self.connect_from["type"]  == info_type
                        and self._addr_equal(self.connect_from, {
                            "doc": self.current_doc,
                            "type": info_type,
                            "id": polygon.get("id"),
                            "index": poly_idx,
                        })):
                    self.canvas.create_polygon(
                        flat,
                        fill="",
                        outline="#00ffcc",
                        width=3,
                    )

    def _draw_in_progress(self):
        color = LABEL_INFO_TYPE_COLORS.get(self.current_info_type, LABEL_DEFAULT_COLOR)
        canvas_verts = [
            self._original_to_canvas(v["x"], v["y"])
            for v in self.draw_vertices
        ]

        # Dots and connecting lines for placed vertices
        if len(canvas_verts) >= 2:
            flat = []
            for cx, cy in canvas_verts:
                flat.extend([cx, cy])
            self.canvas.create_line(flat, fill=color, width=2, dash=(4, 3))

        for cx, cy in canvas_verts:
            self.canvas.create_oval(
                cx - DRAW_DOT_RADIUS, cy - DRAW_DOT_RADIUS,
                cx + DRAW_DOT_RADIUS, cy + DRAW_DOT_RADIUS,
                fill=color, outline="white",
            )

        # Rubber-band line to mouse cursor
        if canvas_verts:
            lx, ly = canvas_verts[-1]
            self.canvas.create_line(
                lx, ly, self.mouse_x, self.mouse_y,
                fill=color, width=1, dash=(2, 4),
            )

        # Closing-hint circle on first vertex (when ≥ 3 vertices placed)
        if len(canvas_verts) >= 3:
            fx, fy = canvas_verts[0]
            self.canvas.create_oval(
                fx - HANDLE_RADIUS, fy - HANDLE_RADIUS,
                fx + HANDLE_RADIUS, fy + HANDLE_RADIUS,
                fill="white", outline=color, width=2,
            )

    # ── Canvas event handlers ──────────────────────────────────────────────────

    def _on_canvas_motion(self, event):
        self.mouse_x = event.x
        self.mouse_y = event.y
        if self.draw_mode or (self.connect_mode and self.connect_from is not None):
            self._draw_scene()

    def _on_canvas_click(self, event):
        if self.draw_mode:
            self._handle_draw_click(event)
        elif self.connect_mode:
            self._handle_connect_click(event)
        else:
            ox, oy = self._canvas_to_original(event.x, event.y)
            self._handle_normal_click(event, ox, oy)

    def _handle_draw_click(self, event):
        # Clicking near the first vertex closes the polygon
        if len(self.draw_vertices) >= 3:
            fx, fy = self._original_to_canvas(
                self.draw_vertices[0]["x"], self.draw_vertices[0]["y"]
            )
            if abs(event.x - fx) <= HANDLE_RADIUS * 2 and abs(event.y - fy) <= HANDLE_RADIUS * 2:
                self._close_polygon()
                return

        ox, oy = self._canvas_to_original(event.x, event.y)
        ox, oy = self._clamp(ox, oy)
        self.draw_vertices.append({"x": ox, "y": oy})
        self._draw_scene()

    def _handle_normal_click(self, event, ox, oy):
        polygons = self._current_polygons()

        # 1. Check vertex handles of the currently selected polygon
        if self.selected_polygon_idx is not None:
            idx = self.selected_polygon_idx
            if idx < len(polygons):
                canvas_poly = self._poly_to_canvas(polygons[idx]["vertices"])
                for vi, pt in enumerate(canvas_poly):
                    if (abs(pt["x"] - event.x) <= HANDLE_RADIUS * 2 and
                            abs(pt["y"] - event.y) <= HANDLE_RADIUS * 2):
                        self.drag_mode       = "vertex"
                        self.drag_vertex_idx = vi
                        self.drag_last_x, self.drag_last_y = ox, oy
                        return

                # 2. Click inside the selected polygon → drag it
                if _point_in_polygon(event.x, event.y, canvas_poly):
                    self.drag_mode   = "polygon"
                    self.drag_last_x = ox
                    self.drag_last_y = oy
                    return

        # 3. Try to select another polygon in current doc + type
        for poly_idx, polygon in enumerate(polygons):
            if len(polygon["vertices"]) < 3:
                continue
            if _point_in_polygon(event.x, event.y, self._poly_to_canvas(polygon["vertices"])):
                self.selected_polygon_idx = poly_idx
                self.drag_mode   = "polygon"
                self.drag_last_x = ox
                self.drag_last_y = oy
                self._draw_scene()
                self._update_status()
                return

        # 4. Try to select from other info types in the same doc (auto-switch type)
        docs       = self.page_data.get("documents", {})
        doc_record = docs.get(self.current_doc, {})
        for info_type in LABEL_INFO_TYPES:
            if info_type == self.current_info_type:
                continue
            for poly_idx, polygon in enumerate(doc_record.get(info_type, [])):
                if len(polygon["vertices"]) < 3:
                    continue
                if _point_in_polygon(event.x, event.y, self._poly_to_canvas(polygon["vertices"])):
                    self.current_info_type    = info_type
                    self.selected_polygon_idx = poly_idx
                    self._highlight_info_type_button()
                    self.drag_mode   = "polygon"
                    self.drag_last_x = ox
                    self.drag_last_y = oy
                    self._draw_scene()
                    self._update_status()
                    return

        # 5. Nothing hit — deselect
        self.selected_polygon_idx = None
        self.drag_mode = None
        self._draw_scene()

    def _on_canvas_drag(self, event):
        if self.drag_mode is None or self.selected_polygon_idx is None:
            return

        ox, oy = self._canvas_to_original(event.x, event.y)
        ox, oy = self._clamp(ox, oy)

        polygons = self._current_polygons()
        if self.selected_polygon_idx >= len(polygons):
            return
        polygon  = polygons[self.selected_polygon_idx]
        vertices = polygon["vertices"]

        if self.drag_mode == "vertex" and self.drag_vertex_idx is not None:
            if self.drag_vertex_idx < len(vertices):
                vertices[self.drag_vertex_idx]["x"] = ox
                vertices[self.drag_vertex_idx]["y"] = oy

        elif self.drag_mode == "polygon" and self.drag_last_x is not None:
            dx = ox - self.drag_last_x
            dy = oy - self.drag_last_y
            for pt in vertices:
                pt["x"], pt["y"] = self._clamp(pt["x"] + dx, pt["y"] + dy)

        self.drag_last_x = ox
        self.drag_last_y = oy
        self._draw_scene()

    def _on_canvas_release(self, event):
        self.drag_mode       = None
        self.drag_vertex_idx = None
        self.drag_last_x     = None
        self.drag_last_y     = None

    def _on_canvas_right_click(self, event):
        if self.draw_mode:
            self._close_polygon()

    def _on_canvas_resize(self, event):
        if event.width <= 1 or event.height <= 1:
            return
        if self.page_image:
            self._refresh_image()
            self._draw_scene()
        self._update_status()

    # ── Key handlers ───────────────────────────────────────────────────────────

    def _on_enter_key(self, event):
        focused = self.root.focus_get()
        if focused == self.page_entry:
            self.go_to_page()
        elif focused == self.num_docs_spinbox:
            self._on_num_docs_changed()
        elif self.draw_mode:
            self._close_polygon()

    def _on_escape_key(self, event):
        if self.draw_mode:
            self._cancel_draw()
        elif self.connect_mode:
            self._cancel_connect()

    def _focus_is_text_input(self):
        focused = self.root.focus_get()
        return isinstance(focused, (tk.Entry, ttk.Entry, tk.Spinbox, ttk.Spinbox))

    def _on_info_type_number(self, number):
        if self._focus_is_text_input():
            return
        self._select_info_type_by_number(number)

    def _on_shift_w(self, event):
        if self._focus_is_text_input():
            return
        self._start_draw_mode()

    def _on_shift_c(self, event):
        if self._focus_is_text_input():
            return
        self._start_connect_mode()

    # ── Status ─────────────────────────────────────────────────────────────────

    def _update_status(self):
        num_polys = len(self._current_polygons())
        conn_info = ""
        if self.selected_polygon_idx is not None:
            polygons = self._current_polygons()
            if self.selected_polygon_idx < len(polygons):
                num_conns = len(polygons[self.selected_polygon_idx].get("connections", []))
                if num_conns:
                    conn_info = f"  │  {num_conns} connection(s)"
        self.status_label.configure(
            text=(
                f"Page {self.current_page} ({self.page_index + 1}/{self.total_pages})  │  "
                f"{self.current_doc}  │  "
                f"{self.current_info_type.replace('_', ' ')}  │  "
                f"{num_polys} polygon(s){conn_info}"
            )
        )

    # ── Review queue (qa_report.py flags) ───────────────────────────────────────

    def _build_queue_panel(self):
        # Sized to content (no fixed height) and packed BOTTOM before the canvas,
        # so every button is visible and the panel never overlaps the editor.
        panel = tk.Frame(self.root, bg="#2a2a2a")
        panel.pack(side=tk.BOTTOM, fill=tk.X)

        header = tk.Frame(panel, bg="#2a2a2a")
        header.pack(side=tk.TOP, fill=tk.X)
        self.queue_label = tk.Label(
            header, text="Review Queue", bg="#2a2a2a", fg="#cccccc",
            font=("TkDefaultFont", 9, "bold"))
        self.queue_label.pack(side=tk.LEFT, padx=8, pady=(4, 0))
        tk.Label(
            header,
            text="Uncovered-ink: 'Extend' grows the nearest box (it was too short); 'Add box' "
                 "makes a new box in the selected type. Other flags: Go To, fix by hand, Dismiss.",
            bg="#2a2a2a", fg="#777777", font=("TkDefaultFont", 8),
        ).pack(side=tk.LEFT, padx=10)

        listrow = tk.Frame(panel, bg="#2a2a2a")
        listrow.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(2, 0))
        self.queue_list = tk.Listbox(
            listrow, height=4, bg="#1e1e1e", fg="#dddddd",
            selectbackground="#0066cc", activestyle="none",
            highlightthickness=0, font=("TkDefaultFont", 9), exportselection=False)
        self.queue_list.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.queue_list.bind("<<ListboxSelect>>", self._on_queue_select)
        for seq, fn in (("<Up>",     lambda e: self._queue_nav(-1)),
                        ("<Down>",   lambda e: self._queue_nav(1)),
                        ("<Return>", self._queue_apply_default),
                        ("<KP_Enter>", self._queue_apply_default),
                        ("e", lambda e: self._queue_extend()),
                        ("a", lambda e: self._queue_accept()),
                        ("d", lambda e: self._queue_dismiss()),
                        ("f", self._zoom_fit)):
            self.queue_list.bind(seq, fn)
        sb = ttk.Scrollbar(listrow, orient=tk.VERTICAL, command=self.queue_list.yview)
        sb.pack(side=tk.LEFT, fill=tk.Y)
        self.queue_list.configure(yscrollcommand=sb.set)

        # Buttons in a horizontal row so they are always visible regardless of height.
        btnrow = tk.Frame(panel, bg="#2a2a2a")
        btnrow.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(3, 6))
        ttk.Button(btnrow, text="Go To",     command=self._queue_goto).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btnrow, text="Extend",    command=self._queue_extend).pack(side=tk.LEFT, padx=6)
        ttk.Button(btnrow, text="Add box",   command=self._queue_accept).pack(side=tk.LEFT, padx=6)
        ttk.Button(btnrow, text="Dismiss",   command=self._queue_dismiss).pack(side=tk.LEFT, padx=6)
        ttk.Button(btnrow, text="Reload QA", command=self._queue_reload).pack(side=tk.LEFT, padx=6)

    def _flag_key(self, page, flag):
        bb = flag.get("bbox") or []
        return f"{page}|{flag.get('type')}|" + ",".join(str(int(round(v))) for v in bb)

    def _load_dismissed(self):
        try:
            return set(json.load(open(self.dismissed_path, encoding="utf-8")))
        except (OSError, ValueError):
            return set()

    def _save_dismissed(self):
        self.dismissed_path.parent.mkdir(parents=True, exist_ok=True)
        json.dump(sorted(self.dismissed_keys), open(self.dismissed_path, "w", encoding="utf-8"))

    def _load_rerun_marks(self):
        try:
            return set(json.load(open(self.rerun_marks_path, encoding="utf-8"))["pages"])
        except (OSError, ValueError, KeyError):
            return set()

    def _save_rerun_marks(self):
        self.rerun_marks_path.parent.mkdir(parents=True, exist_ok=True)
        json.dump({"pages": sorted(self.rerun_marks)},
                  open(self.rerun_marks_path, "w", encoding="utf-8"), indent=2)

    def _toggle_rerun_mark(self):
        """Mark/unmark the current page for a machine re-label (sidecar file
        only — the page JSON is never touched). bootstrap.py <Vol>
        --rerun-marked relabels every marked page and hands back a filtered
        editor session."""
        name = f"page_{self.current_page:03d}"
        if name in self.rerun_marks:
            self.rerun_marks.discard(name)
        else:
            self.rerun_marks.add(name)
        self._save_rerun_marks()
        self._update_rerun_button()

    def _update_rerun_button(self):
        name = f"page_{self.current_page:03d}"
        marked = name in self.rerun_marks
        self.rerun_button.configure(
            text=(f"⟳ Marked for rerun ✓ ({len(self.rerun_marks)})" if marked
                  else f"⟳ Mark for rerun ({len(self.rerun_marks)})"))

    def _approve_to_gold(self):
        """EDITOR_PREFER_AUTO mode: explicitly accept the displayed (re-run)
        labels as gold — writes reviewed/ even with zero edits. Deliberate
        button press only; never automatic (browse-pollution lesson)."""
        self._prepare_page_data_for_save()
        page_dir = self.output_doc_dir / f"page_{self.current_page:03d}"
        page_dir.mkdir(parents=True, exist_ok=True)
        with open(page_dir / f"page_{self.current_page:03d}.json", "w",
                  encoding="utf-8") as f:
            json.dump(self.page_data, f, indent=2)
        self._loaded_snapshot = json.dumps(self.page_data, sort_keys=True)
        self.status_label.configure(text=f"page {self.current_page} approved → gold")

    def _load_qa_flags(self):
        """Read qa_output/<doc>/qa_report.json into {page_number: [flag, ...]},
        dropping any flag the user has persistently dismissed."""
        report = QA_OUTPUT_DIR / self.document_name / "qa_report.json"
        by_page = {}
        if not report.exists():
            return by_page
        try:
            data = json.load(open(report, "r", encoding="utf-8"))
        except (ValueError, OSError):
            return by_page
        for pg in data.get("pages", []):
            num = pg.get("page")
            flags = [f for f in pg.get("flags", [])
                     if self._flag_key(num, f) not in self.dismissed_keys]
            if num is not None and flags:
                by_page[num] = list(flags)
        return by_page

    def _flag_label(self, flag):
        bb  = flag.get("bbox")
        loc = ""
        if bb:
            loc = f"  @ ({(bb[0] + bb[2]) // 2},{(bb[1] + bb[3]) // 2})"
        parts = [flag.get("type", "?")]
        if flag.get("type") == "uncovered_ink" and bb:
            host = self._nearest_abutting_box(bb)
            parts.append(f"abuts {host[1]} → Extend" if host else "orphan → Add box")
        if flag.get("category"):
            parts.append(flag["category"])
        if flag.get("detail"):
            parts.append(flag["detail"])
        return "  ·  ".join(parts) + loc

    def _repopulate_queue_list(self):
        self.queue_list.delete(0, tk.END)
        for flag in self.queue_flags:
            self.queue_list.insert(tk.END, self._flag_label(flag))
        self.queue_label.configure(
            text=f"Review Queue — page {self.current_page} ({len(self.queue_flags)} flag(s))")

    def _refresh_queue(self):
        """Load the current page's flags into the queue list."""
        self.queue_flags   = list(self.qa_flags_by_page.get(self.current_page, []))
        self.selected_flag = None
        self._repopulate_queue_list()

    def _selected_queue_index(self):
        sel = self.queue_list.curselection()
        return sel[0] if sel else None

    def _on_queue_select(self, event=None):
        idx = self._selected_queue_index()
        self.selected_flag = (
            self.queue_flags[idx] if idx is not None and idx < len(self.queue_flags) else None)
        if self.selected_flag and self.selected_flag.get("bbox"):
            self._zoom_to_flag(self.selected_flag["bbox"])   # auto-center-zoom
        else:
            self._draw_scene()

    def _queue_goto(self):
        self._on_queue_select()

    def _queue_nav(self, delta):
        if not self.queue_flags:
            return "break"
        idx = self._selected_queue_index()
        idx = 0 if idx is None else max(0, min(len(self.queue_flags) - 1, idx + delta))
        self.queue_list.selection_clear(0, tk.END)
        self.queue_list.selection_set(idx)
        self.queue_list.activate(idx)
        self.queue_list.see(idx)
        self._on_queue_select()
        return "break"

    def _queue_apply_default(self, event=None):
        """Enter = the suggested action: Extend if the region abuts a box, else Add box."""
        idx = self._selected_queue_index()
        if idx is None or idx >= len(self.queue_flags):
            return "break"
        flag = self.queue_flags[idx]; bb = flag.get("bbox")
        if flag.get("type") == "uncovered_ink" and bb and self._nearest_abutting_box(bb):
            self._queue_extend()
        else:
            self._queue_accept()
        return "break"

    # ── Extend-vs-new recognition (mirrors auto_labeler's coverage backstop) ──
    def _poly_bbox(self, poly):
        xs = [v["x"] for v in poly["vertices"]]; ys = [v["y"] for v in poly["vertices"]]
        return (min(xs), min(ys), max(xs), max(ys))

    def _abuts_score(self, R, B, near):
        """How strongly region R abuts box B along a shared edge (0 = not). A
        continuation sits within ~one line of B and overlaps it on the shared edge."""
        rx0, ry0, rx1, ry1 = R; bx0, by0, bx1, by1 = B
        rw, rh = max(rx1 - rx0, 1), max(ry1 - ry0, 1)
        ix = max(0, min(rx1, bx1) - max(rx0, bx0)); iy = max(0, min(ry1, by1) - max(ry0, by0))
        hov = ix / min(rw, bx1 - bx0) if min(rw, bx1 - bx0) > 0 else 0
        vov = iy / min(rh, by1 - by0) if min(rh, by1 - by0) > 0 else 0
        if hov >= 0.5 and (-0.5 * near <= ry0 - by1 < 1.4 * near or -0.5 * near <= by0 - ry1 < 1.4 * near):
            return hov
        if vov >= 0.5 and (-0.5 * near <= rx0 - bx1 < 1.6 * near or -0.5 * near <= bx0 - rx1 < 1.6 * near):
            return vov
        return 0.0

    def _nearest_abutting_box(self, bbox):
        """Return (doc, info_type, index, polygon) of the box best abutting bbox, else None."""
        docs = self.page_data.get("documents", {})
        heights = [self._poly_bbox(p)[3] - self._poly_bbox(p)[1]
                   for d in docs.values() if isinstance(d, dict)
                   for ps in d.values() if isinstance(ps, list) for p in ps]
        near = sorted(heights)[len(heights) // 2] if heights else 40
        best = None
        for doc, types in docs.items():
            if not isinstance(types, dict):
                continue
            for itype, polys in types.items():
                if not isinstance(polys, list):
                    continue
                for i, p in enumerate(polys):
                    s = self._abuts_score(tuple(bbox), self._poly_bbox(p), near)
                    if s > 0 and (best is None or s > best[0]):
                        best = (s, doc, itype, i, p)
        return None if best is None else (best[1], best[2], best[3], best[4])

    def _queue_extend(self):
        """Grow the nearest abutting box to cover the selected uncovered-ink region
        (the 'box was too short / narrow' case — one click, no new box, label kept)."""
        idx = self._selected_queue_index()
        if idx is None or idx >= len(self.queue_flags):
            return
        flag = self.queue_flags[idx]; bb = flag.get("bbox")
        if flag.get("type") != "uncovered_ink" or not bb:
            messagebox.showinfo("Extend", "Extend grows the nearest box to cover an uncovered-ink region.")
            return
        host = self._nearest_abutting_box(bb)
        if not host:
            messagebox.showinfo("Extend", "No adjacent box found — use 'Add box' for a separate new region.")
            return
        doc, itype, hidx, box = host
        x0, y0, x1, y1 = (int(round(v)) for v in bb)
        xs = [v["x"] for v in box["vertices"]] + [x0, x1]
        ys = [v["y"] for v in box["vertices"]] + [y0, y1]
        nx0, ny0, nx1, ny1 = min(xs), min(ys), max(xs), max(ys)
        box["vertices"] = [{"x": nx0, "y": ny0}, {"x": nx1, "y": ny0},
                           {"x": nx1, "y": ny1}, {"x": nx0, "y": ny1}]
        self.current_doc = doc; self.current_info_type = itype
        self.selected_polygon_idx = hidx
        self._remove_queue_flag(idx); self._draw_scene(); self._update_status()

    def _queue_accept(self):
        """Apply the selected proposed edit. Only uncovered_ink has a concrete
        auto-fix (add a box at the flagged region for the active label type)."""
        idx = self._selected_queue_index()
        if idx is None or idx >= len(self.queue_flags):
            return
        flag = self.queue_flags[idx]
        bb   = flag.get("bbox")
        if flag.get("type") == "uncovered_ink" and bb:
            x0, y0, x1, y1 = (int(round(v)) for v in bb)
            polygon = {
                "id": str(uuid4()),
                "vertices": [{"x": x0, "y": y0}, {"x": x1, "y": y0},
                             {"x": x1, "y": y1}, {"x": x0, "y": y1}],
                "connections": [],
            }
            polys = self._current_polygons()
            polys.append(polygon)
            self.selected_polygon_idx = len(polys) - 1
            self._remove_queue_flag(idx)
            self._draw_scene()
            self._update_status()
        else:
            messagebox.showinfo(
                "Review Queue",
                f"'{flag.get('type')}' has no automatic fix.\n\n"
                "Use 'Go To' to locate it, correct the boxes by hand, then 'Dismiss'.")

    def _queue_dismiss(self):
        idx = self._selected_queue_index()
        if idx is None or idx >= len(self.queue_flags):
            return
        self.dismissed_keys.add(self._flag_key(self.current_page, self.queue_flags[idx]))
        self._save_dismissed()                                # persists across sessions
        self._remove_queue_flag(idx)
        self._draw_scene()

    def _remove_queue_flag(self, idx):
        if not (0 <= idx < len(self.queue_flags)):
            return
        flag = self.queue_flags.pop(idx)
        page_flags = self.qa_flags_by_page.get(self.current_page, [])
        if flag in page_flags:
            page_flags.remove(flag)   # shared dict object — drop so it won't reappear this session
        self.selected_flag = None
        self._repopulate_queue_list()

    def _queue_reload(self):
        """Re-read qa_report.json from disk (after re-running qa_report.py)."""
        self.qa_flags_by_page = self._load_qa_flags()
        self._refresh_queue()
        self._draw_scene()

    def _draw_queue_highlight(self):
        for flag in self.queue_flags:
            bb = flag.get("bbox")
            if not bb:
                continue
            cx0, cy0 = self._original_to_canvas(bb[0], bb[1])
            cx1, cy1 = self._original_to_canvas(bb[2], bb[3])
            if flag is self.selected_flag:
                self.canvas.create_rectangle(cx0, cy0, cx1, cy1, outline="#ff00ff", width=3)
            else:
                self.canvas.create_rectangle(cx0, cy0, cx1, cy1, outline="#ff00ff",
                                             width=1, dash=(4, 3))
        # Ghost preview (cyan) of what Extend / Add box would produce for the selected flag.
        sf = self.selected_flag
        if sf and sf.get("type") == "uncovered_ink" and sf.get("bbox"):
            bb = sf["bbox"]
            host = self._nearest_abutting_box(bb)
            if host:
                hb = self._poly_bbox(host[3])
                gx0, gy0 = min(hb[0], bb[0]), min(hb[1], bb[1])
                gx1, gy1 = max(hb[2], bb[2]), max(hb[3], bb[3])
                label = f"Extend → {host[1]}"
            else:
                gx0, gy0, gx1, gy1 = bb
                label = "New box"
            px0, py0 = self._original_to_canvas(gx0, gy0)
            px1, py1 = self._original_to_canvas(gx1, gy1)
            self.canvas.create_rectangle(px0, py0, px1, py1, outline="#00e5ff", width=2, dash=(6, 3))
            self.canvas.create_text(px0 + 3, py0 - 8, anchor=tk.W, fill="#00e5ff",
                                    text=label, font=("TkDefaultFont", 9))

    # ── Close ──────────────────────────────────────────────────────────────────

    def _on_close(self):
        self._cancel_draw_if_active()
        self.save_page_data()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def main():
    try:
        app = NormalizedEditorApp()
        app.run()
    except Exception as exc:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Normalized Editor Error", str(exc))
        root.destroy()
        raise


if __name__ == "__main__":
    main()
