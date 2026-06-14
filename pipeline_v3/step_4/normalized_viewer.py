"""
Step 4: Self-contained read-only viewer for auto-labeled pages.

Opens PDF + JSON pairs from:
    {VIEWER_DIR}/{doc}/page_{xxx}/page_{xxx}.{pdf,json}

Defaults to viewing ./auto_labeled/, but VIEWER_DIR overrides for inspecting
labeled_examples/ or any other folder using the same layout.

Self-contained: no imports from the wider project. Stdlib + Pillow + pdf2image.

Environment overrides (all optional):
    VIEWER_DIR         path to the root folder containing {doc}/page_xxx/ dirs
                       (default: ./auto_labeled)
    VIEWER_DOCUMENT    document folder to open (default: Appendix_1)
    VIEWER_START_PAGE  starting page number (default: first available)

Canvas controls:
    - Two-finger scroll = pan; Ctrl + two-finger scroll = zoom (anchored on the
      cursor); Ctrl +/- also zoom; middle-drag pans; f = fit page.
    - Click a polygon to select it (shows connections + type).
    - Left / Right arrows: navigate pages.
    - 1-9: switch active label type in the right panel.
    - Escape: deselect.

Usage:
    ./venv/bin/python normalized_viewer.py
    VIEWER_DOCUMENT=Volume_1 ./venv/bin/python normalized_viewer.py
    VIEWER_DIR=./labeled_examples ./venv/bin/python normalized_viewer.py
"""
import json
import os
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from pdf2image import convert_from_path
from PIL import ImageTk

SCRIPT_DIR = Path(__file__).resolve().parent

# ── Inlined config (was in step_x/config.py) ──────────────────────────────────

RENDER_DPI = 150

PAN_STEP = 80          # canvas px shifted per two-finger-scroll notch (trackpad pan)

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

HANDLE_RADIUS = 7


# ── Env / paths ────────────────────────────────────────────────────────────────

def _get_env_path(env_name, default_path):
    val = os.getenv(env_name, "").strip()
    return Path(val).expanduser().resolve() if val else default_path


def _get_env_int(env_name, default_value):
    val = os.getenv(env_name, "").strip()
    if not val:
        return default_value
    try:
        return int(val)
    except ValueError:
        print(f"WARNING: Invalid integer for {env_name}: {val}")
        return default_value


VIEWER_DIR        = _get_env_path("VIEWER_DIR", SCRIPT_DIR / "auto_labeled")
VIEWER_DOCUMENT   = os.getenv("VIEWER_DOCUMENT", "Appendix_1").strip() or "Appendix_1"
VIEWER_START_PAGE = _get_env_int("VIEWER_START_PAGE", None)


# ── Geometry helper ───────────────────────────────────────────────────────────

def _point_in_polygon(x, y, polygon):
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


# ── Main application ──────────────────────────────────────────────────────────

class ViewerApp:
    def __init__(self):
        self.document_name = VIEWER_DOCUMENT
        self.doc_dir       = VIEWER_DIR / self.document_name

        self.page_numbers = self._discover_pages()
        self.total_pages  = len(self.page_numbers)
        if self.total_pages == 0:
            raise FileNotFoundError(
                f"No page directories found in {self.doc_dir}\n"
                f"(VIEWER_DIR={VIEWER_DIR}, VIEWER_DOCUMENT={VIEWER_DOCUMENT})"
            )

        if VIEWER_START_PAGE is not None and VIEWER_START_PAGE in self.page_numbers:
            self.page_index = self.page_numbers.index(VIEWER_START_PAGE)
        else:
            self.page_index = 0
        self.current_page = self.page_numbers[self.page_index]

        self.current_doc          = "doc_1"
        self.current_info_type    = LABEL_INFO_TYPES[0]
        self.selected_polygon_idx = None

        self.page_data       = {}
        self.page_image      = None
        self.tk_image        = None
        self.original_width  = 1
        self.original_height = 1
        self.display_scale   = 1.0
        self.image_offset_x  = 0
        self.image_offset_y  = 0
        self.rendered_width  = 1
        self.rendered_height = 1
        self.view            = None   # None => fit; ("custom", scale, off_x, off_y) => wheel/pan
        self._pan_anchor     = None

        self._build_ui()
        self.load_page(self.current_page)

    # ── Discovery / resolution ────────────────────────────────────────────────

    def _discover_pages(self):
        if not self.doc_dir.exists():
            return []
        numbers = []
        for d in sorted(self.doc_dir.glob("page_*")):
            if d.is_dir():
                try:
                    numbers.append(int(d.name.split("_", 1)[1]))
                except (ValueError, IndexError):
                    continue
        return sorted(numbers)

    def _resolve_page_pdf(self, page_number):
        page_name = f"page_{page_number:03d}"
        pdf = self.doc_dir / page_name / f"{page_name}.pdf"
        if pdf.exists():
            return pdf
        raise FileNotFoundError(f"Page PDF not found: {pdf}")

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.root = tk.Tk()
        self.root.title(f"[VIEW ONLY] {self.document_name} — {VIEWER_DIR.name}")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.configure(bg="#1e1e1e")
        self.root.geometry("1400x900")

        self._build_top_toolbar()
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
        self.num_docs_label = tk.Label(top, text="1", bg="#2d2d2d", fg="#ffaa00",
                                       font=("TkDefaultFont", 10, "bold"), width=4)
        self.num_docs_label.pack(side=tk.LEFT)

        tk.Label(top, text="VIEW ONLY — no edits saved",
                 bg="#2d2d2d", fg="#888888",
                 font=("TkDefaultFont", 9)).pack(side=tk.LEFT, padx=14)

        self.status_label = tk.Label(top, text="", bg="#2d2d2d", fg="#888888")
        self.status_label.pack(side=tk.RIGHT, padx=10)

    def _build_main_area(self):
        main = tk.Frame(self.root, bg="#1e1e1e")
        main.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.left_panel = tk.Frame(main, bg="#252526", width=100)
        self.left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0), pady=4)
        self.left_panel.pack_propagate(False)
        tk.Label(self.left_panel, text="Documents", bg="#252526", fg="#aaaaaa",
                 font=("TkDefaultFont", 8, "bold")).pack(pady=(8, 4))
        self.doc_buttons_frame = tk.Frame(self.left_panel, bg="#252526")
        self.doc_buttons_frame.pack(fill=tk.BOTH, expand=True, pady=2)

        self.canvas = tk.Canvas(main, bg="#1e1e1e", highlightthickness=0)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

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
        tk.Label(self.right_panel, text="VIEW ONLY", bg="#252526", fg="#555555",
                 font=("TkDefaultFont", 9, "italic")).pack(fill=tk.X, padx=6, pady=4)
        ttk.Separator(self.right_panel, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=6, pady=10)

        for line in (
            "Shortcuts:",
            "• ← / → navigate pages",
            "• 1-9 switch label type",
            "• Click polygon to select",
            "• Escape → deselect",
        ):
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
            tk.Label(row, bg=color, width=2).pack(side=tk.LEFT)
            label = tk.Label(row, text=info_type.replace("_", " "),
                             bg="#333333", fg="#cccccc",
                             anchor=tk.W, padx=6, pady=4,
                             font=("TkDefaultFont", 9))
            label.pack(side=tk.LEFT, fill=tk.X, expand=True)
            for widget in (row, label):
                widget.bind("<Button-1>", lambda e, t=info_type: self._select_info_type(t))
            self.info_type_buttons[info_type] = row
        self._highlight_info_type_button()

    def _rebuild_doc_buttons(self):
        for w in self.doc_buttons_frame.winfo_children():
            w.destroy()
        num_docs = self.page_data.get("num_documents", 1)
        for i in range(1, num_docs + 1):
            doc_key   = f"doc_{i}"
            is_active = doc_key == self.current_doc
            btn = tk.Button(
                self.doc_buttons_frame,
                text=f"Doc {i}",
                bg="#0066cc" if is_active else "#3a3a3a", fg="white",
                activebackground="#0077dd", activeforeground="white",
                relief=tk.FLAT, pady=10,
                font=("TkDefaultFont", 9, "bold" if is_active else "normal"),
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
        self.root.bind("<Left>",   lambda e: self.previous_page())
        self.root.bind("<Right>",  lambda e: self.next_page())
        self.root.bind("<Return>", self._on_enter_key)
        self.root.bind("<Escape>", self._on_escape_key)
        for n in range(1, 10):
            self.root.bind(f"<KeyPress-{n}>", lambda e, i=n: self._on_info_type_number(i))
            self.root.bind(f"<KP_{n}>",       lambda e, i=n: self._on_info_type_number(i))
        self.canvas.bind("<ButtonPress-1>", self._on_canvas_click)
        self.canvas.bind("<Configure>",     self._on_canvas_resize)
        # ── Trackpad: two-finger scroll = pan · Ctrl + two-finger scroll = zoom ──
        # X11/XWayland deliver two-finger scroll as Button-4/5 (vert) & 6/7 (horiz);
        # Win/macOS use <MouseWheel> / <Shift-MouseWheel>.
        self.canvas.bind("<Button-4>",           lambda e: self._pan_by(0,  PAN_STEP))   # scroll up
        self.canvas.bind("<Button-5>",           lambda e: self._pan_by(0, -PAN_STEP))   # scroll down
        self.canvas.bind("<MouseWheel>",         lambda e: self._pan_by(0,  PAN_STEP if e.delta > 0 else -PAN_STEP))
        self.canvas.bind("<Shift-MouseWheel>",   lambda e: self._pan_by(PAN_STEP if e.delta > 0 else -PAN_STEP, 0))
        # horizontal two-finger scroll arrives as button 6/7; the <Button-6/7> bind
        # pattern is rejected by some Tk builds, so dispatch the generic <Button>.
        self.canvas.bind("<Button>", lambda e: self._pan_by(PAN_STEP, 0) if e.num == 6
                         else (self._pan_by(-PAN_STEP, 0) if e.num == 7 else None))
        self.canvas.bind("<Control-Button-4>",   lambda e: self._on_zoom_wheel(e, 1))    # Ctrl+scroll / pinch = zoom in
        self.canvas.bind("<Control-Button-5>",   lambda e: self._on_zoom_wheel(e, -1))   # Ctrl+scroll / pinch = zoom out
        self.canvas.bind("<Control-MouseWheel>", self._on_zoom_wheel)
        self.canvas.bind("<ButtonPress-2>", self._on_pan_press)                    # middle-drag pan
        self.canvas.bind("<B2-Motion>",     self._on_pan_motion)
        self.root.bind("f",                 self._zoom_fit)                        # fit page
        self.root.bind("<Control-plus>",        lambda e: self._zoom_keyboard(1))  # keyboard / injected zoom
        self.root.bind("<Control-equal>",       lambda e: self._zoom_keyboard(1))
        self.root.bind("<Control-minus>",       lambda e: self._zoom_keyboard(-1))
        self.root.bind("<Control-KP_Add>",      lambda e: self._zoom_keyboard(1))
        self.root.bind("<Control-KP_Subtract>", lambda e: self._zoom_keyboard(-1))

    # ── Page loading ──────────────────────────────────────────────────────────

    def load_page(self, page_number):
        self.view         = None
        self.current_page = page_number
        self.page_index   = self.page_numbers.index(page_number)
        self._load_page_data(page_number)
        self._load_page_image(page_number)

        num_docs = self.page_data.get("num_documents", 1)
        valid_keys = {f"doc_{i}" for i in range(1, num_docs + 1)}
        if self.current_doc not in valid_keys:
            self.current_doc = "doc_1"
        self.selected_polygon_idx = None

        self.num_docs_label.configure(text=str(num_docs))
        self._rebuild_doc_buttons()
        self._highlight_info_type_button()
        self._refresh_render_metrics()
        self._refresh_image()
        self._draw_scene()

        self.page_entry.delete(0, tk.END)
        self.page_entry.insert(0, str(page_number))
        self._update_status()

    def _load_page_data(self, page_number):
        page_name = f"page_{page_number:03d}"
        path = self.doc_dir / page_name / f"{page_name}.json"
        if not path.exists():
            raise FileNotFoundError(f"JSON not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            self.page_data = json.load(f)

        for doc_record in self.page_data.get("documents", {}).values():
            for info_type in LABEL_INFO_TYPES:
                doc_record.setdefault(info_type, [])
            for info_type in LABEL_INFO_TYPES:
                polys = doc_record.get(info_type, [])
                for i, poly in enumerate(polys):
                    if isinstance(poly, list):
                        polys[i] = {"vertices": poly, "connections": []}

    def _load_page_image(self, page_number):
        page_pdf = self._resolve_page_pdf(page_number)
        images = convert_from_path(page_pdf, dpi=RENDER_DPI, first_page=1, last_page=1)
        if self.page_image:
            self.page_image.close()
        self.page_image      = images[0]
        self.original_width  = self.page_image.width
        self.original_height = self.page_image.height

    # ── Navigation ────────────────────────────────────────────────────────────

    def previous_page(self):
        if self.page_index > 0:
            self.load_page(self.page_numbers[self.page_index - 1])

    def next_page(self):
        if self.page_index < self.total_pages - 1:
            self.load_page(self.page_numbers[self.page_index + 1])

    def go_to_page(self):
        try:
            requested = int(self.page_entry.get())
        except ValueError:
            return
        target = requested if requested in self.page_numbers else \
                 min(self.page_numbers, key=lambda p: abs(p - requested))
        self.load_page(target)

    # ── Selection ─────────────────────────────────────────────────────────────

    def _select_doc(self, doc_key):
        if doc_key == self.current_doc:
            return
        self.current_doc = doc_key
        self.selected_polygon_idx = None
        self._rebuild_doc_buttons()
        self._draw_scene()
        self._update_status()

    def _select_info_type(self, info_type):
        self.current_info_type    = info_type
        self.selected_polygon_idx = None
        self._highlight_info_type_button()
        self._draw_scene()
        self._update_status()

    def _select_info_type_by_number(self, number):
        idx = number - 1
        if 0 <= idx < len(LABEL_INFO_TYPES):
            self._select_info_type(LABEL_INFO_TYPES[idx])

    # ── Polygon helpers ───────────────────────────────────────────────────────

    def _current_polygons(self):
        return (self.page_data.get("documents", {})
                .get(self.current_doc, {})
                .get(self.current_info_type, []))

    def _get_polygon_by_addr(self, addr):
        polys = (self.page_data.get("documents", {})
                 .get(addr.get("doc", ""), {})
                 .get(addr.get("type", ""), []))
        target_id = addr.get("id")
        if target_id:
            for poly in polys:
                if poly.get("id") == target_id:
                    return poly
        idx = addr.get("index")
        if isinstance(idx, int) and 0 <= idx < len(polys):
            return polys[idx]
        return None

    # ── Coordinate conversion ─────────────────────────────────────────────────

    def _original_to_canvas(self, ox, oy):
        return (ox * self.display_scale + self.image_offset_x,
                oy * self.display_scale + self.image_offset_y)

    def _poly_to_canvas(self, polygon):
        return [{"x": p["x"] * self.display_scale + self.image_offset_x,
                 "y": p["y"] * self.display_scale + self.image_offset_y}
                for p in polygon]

    # ── Render metrics ────────────────────────────────────────────────────────

    def _refresh_render_metrics(self):
        cw = max(1, self.canvas.winfo_width())
        ch = max(1, self.canvas.winfo_height())
        fit = min((cw - 16) / max(1, self.original_width),
                  (ch - 16) / max(1, self.original_height))
        cap = min(4000 / max(1, self.original_width),
                  4000 / max(1, self.original_height))
        view = getattr(self, "view", None)
        if view and view[0] == "custom":
            z = min(max(view[1], fit * 0.5), cap)
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

    def _zoom_fit(self, event=None):
        if self.view is not None:
            self.view = None
            self._refresh_image()
            self._draw_scene()
        return "break"

    def _on_zoom_wheel(self, event, direction=None):
        """Mouse-wheel zoom anchored on the cursor; 'f' fits the whole page."""
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

    def _pan_by(self, dx, dy):
        """Shift the view by (dx, dy) canvas px — two-finger-scroll panning."""
        self.image_offset_x += dx
        self.image_offset_y += dy
        self.view = ("custom", self.display_scale, self.image_offset_x, self.image_offset_y)
        self._draw_scene()
        return "break"

    def _zoom_keyboard(self, direction):
        """Zoom anchored on the canvas centre — keyboard Ctrl +/- or an injected
        pinch gesture (libinput-gestures → Ctrl+= / Ctrl+-)."""
        cx = self.canvas.winfo_width() / 2
        cy = self.canvas.winfo_height() / 2
        ev = type("E", (), {"x": cx, "y": cy, "delta": 0})()
        return self._on_zoom_wheel(ev, direction)

    # ── Drawing ───────────────────────────────────────────────────────────────

    def _flatten(self, canvas_poly):
        flat = []
        for p in canvas_poly:
            flat.extend([p["x"], p["y"]])
        return flat

    def _draw_scene(self):
        self.canvas.delete("all")
        if self.tk_image:
            self.canvas.create_image(self.image_offset_x, self.image_offset_y,
                                     anchor=tk.NW, image=self.tk_image)
        self._draw_all_polygons()
        self._draw_connections()

    def _draw_all_polygons(self):
        doc_record = self.page_data.get("documents", {}).get(self.current_doc, {})
        for info_type in LABEL_INFO_TYPES:
            color    = LABEL_INFO_TYPE_COLORS.get(info_type, LABEL_DEFAULT_COLOR)
            polygons = doc_record.get(info_type, [])
            is_active_type = info_type == self.current_info_type

            for poly_idx, polygon in enumerate(polygons):
                vertices = polygon["vertices"]
                if len(vertices) < 3:
                    continue
                is_selected = is_active_type and poly_idx == self.selected_polygon_idx
                canvas_poly = self._poly_to_canvas(vertices)
                flat = self._flatten(canvas_poly)

                if is_selected:
                    stipple, outline_col, width = "gray50", "white", 3
                elif is_active_type:
                    stipple, outline_col, width = "gray50", color, 2
                else:
                    stipple, outline_col, width = "gray25", "#555555", 1

                self.canvas.create_polygon(flat, fill=color, stipple=stipple,
                                           outline=outline_col, width=width)

                if is_selected:
                    for pt in canvas_poly:
                        r = HANDLE_RADIUS
                        self.canvas.create_oval(pt["x"] - r, pt["y"] - r,
                                                pt["x"] + r, pt["y"] + r,
                                                fill="#ff5f5f", outline="white", width=1)

    def _centroid_canvas(self, vertices):
        n  = len(vertices)
        ox = sum(v["x"] for v in vertices) / n
        oy = sum(v["y"] for v in vertices) / n
        return self._original_to_canvas(ox, oy)

    def _draw_connections(self):
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
            self.canvas.create_line(src_cx, src_cy, tgt_cx, tgt_cy,
                                    fill="#00ffcc", width=2, dash=(6, 4))
            for cx, cy in ((src_cx, src_cy), (tgt_cx, tgt_cy)):
                self.canvas.create_oval(cx - 5, cy - 5, cx + 5, cy + 5,
                                        fill="#00ffcc", outline="white", width=1)

    # ── Canvas events ─────────────────────────────────────────────────────────

    def _on_canvas_click(self, event):
        doc_record = self.page_data.get("documents", {}).get(self.current_doc, {})
        for poly_idx, polygon in enumerate(self._current_polygons()):
            if len(polygon["vertices"]) < 3:
                continue
            if _point_in_polygon(event.x, event.y, self._poly_to_canvas(polygon["vertices"])):
                self.selected_polygon_idx = poly_idx
                self._draw_scene()
                self._update_status()
                return
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
                    self._draw_scene()
                    self._update_status()
                    return
        self.selected_polygon_idx = None
        self._draw_scene()

    def _on_canvas_resize(self, event):
        if event.width <= 1 or event.height <= 1:
            return
        if self.page_image:
            self._refresh_image()
            self._draw_scene()
        self._update_status()

    # ── Key handlers ──────────────────────────────────────────────────────────

    def _on_enter_key(self, event):
        if self.root.focus_get() == self.page_entry:
            self.go_to_page()

    def _on_escape_key(self, event):
        self.selected_polygon_idx = None
        self._draw_scene()
        self._update_status()

    def _on_info_type_number(self, number):
        if isinstance(self.root.focus_get(), (tk.Entry, ttk.Entry)):
            return
        self._select_info_type_by_number(number)

    # ── Status ────────────────────────────────────────────────────────────────

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
            text=f"Page {self.current_page} ({self.page_index + 1}/{self.total_pages})  │  "
                 f"{self.current_doc}  │  "
                 f"{self.current_info_type.replace('_', ' ')}  │  "
                 f"{num_polys} polygon(s){conn_info}"
        )

    def _on_close(self):
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def main():
    try:
        ViewerApp().run()
    except Exception as exc:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Viewer Error", str(exc))
        root.destroy()
        raise


if __name__ == "__main__":
    main()
