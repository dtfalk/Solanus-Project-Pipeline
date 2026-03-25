"""
Step X: Label Editor — interactive per-page document labeling tool.

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
  - Left / Right arrow keys navigate pages (auto-saves).

Usage:
    python step_x/editor.py
"""

import json
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from pdf2image import convert_from_path
from PIL import ImageTk

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR   = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_x.config import (
    LABEL_CONFIG_DIR,
    LABEL_DEFAULT_COLOR,
    LABEL_EDITOR_DOCUMENT,
    LABEL_EDITOR_START_PAGE,
    LABEL_INFO_TYPE_COLORS,
    LABEL_INFO_TYPES,
    LABEL_INPUT_DIR,
    RENDER_DPI,
)

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
INPUT_DIR  = ROOT_DIR / LABEL_INPUT_DIR
CONFIG_DIR = ROOT_DIR / LABEL_CONFIG_DIR

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


# ── Main application ───────────────────────────────────────────────────────────

class LabelEditorApp:
    def __init__(self):
        self.document_path = self._resolve_document_path()
        self.document_name = self.document_path.stem
        self.config_dir    = CONFIG_DIR / self.document_name
        self.total_pages   = self._count_pages()

        self.current_page          = max(1, min(LABEL_EDITOR_START_PAGE, self.total_pages))
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

    def _resolve_document_path(self):
        if LABEL_EDITOR_DOCUMENT:
            p = INPUT_DIR / LABEL_EDITOR_DOCUMENT
            if not p.exists():
                raise FileNotFoundError(f"Editor document not found: {p}")
            return p
        all_pdfs = sorted(p for p in INPUT_DIR.iterdir() if p.suffix.lower() == ".pdf")
        if len(all_pdfs) == 1:
            return all_pdfs[0]
        raise FileNotFoundError(
            "Multiple PDFs found. Set LABEL_EDITOR_DOCUMENT in step_x/config.py."
        )

    def _count_pages(self):
        pages = list(self.config_dir.glob("page_*.json"))
        if not pages:
            raise FileNotFoundError(
                f"No label JSON files found in {self.config_dir}. "
                "Run step_x/initializer.py first."
            )
        return len(pages)

    # ── UI construction ────────────────────────────────────────────────────────

    def _build_ui(self):
        self.root = tk.Tk()
        self.root.title(f"Label Editor — {self.document_name}")
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
        self.num_docs_var = tk.IntVar(value=1)
        self.num_docs_spinbox = ttk.Spinbox(
            top, from_=1, to=20, width=4,
            textvariable=self.num_docs_var,
            command=self._on_num_docs_changed,
        )
        self.num_docs_spinbox.pack(side=tk.LEFT)
        self.num_docs_spinbox.bind("<FocusOut>", lambda e: self._on_num_docs_changed())

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
        self.root.bind("<Left>",  lambda e: self.previous_page())
        self.root.bind("<Right>", lambda e: self.next_page())
        self.root.bind("<Return>", self._on_enter_key)
        self.root.bind("<Escape>", self._on_escape_key)
        self.root.bind("<Shift-W>", self._on_shift_w)
        self.root.bind("<Shift-w>", self._on_shift_w)

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

    # ── Page loading / saving ──────────────────────────────────────────────────

    def load_page(self, page_number):
        self.current_page = page_number
        self._load_page_data(page_number)
        self._load_page_image(page_number)

        # Keep current_doc valid
        num_docs = self.page_data.get("num_documents", 1)
        valid_keys = {f"doc_{i}" for i in range(1, num_docs + 1)}
        if self.current_doc not in valid_keys:
            self.current_doc = "doc_1"

        self.selected_polygon_idx = None
        self.draw_mode    = False
        self.draw_vertices = []
        self.drag_mode    = None

        self.num_docs_var.set(num_docs)
        self._rebuild_doc_buttons()
        self._highlight_info_type_button()
        self._refresh_render_metrics()
        self._refresh_image()
        self._draw_scene()

        self.page_entry.delete(0, tk.END)
        self.page_entry.insert(0, str(page_number))
        self.mode_label.configure(text="")
        self._update_status()

    def _load_page_data(self, page_number):
        path = self.config_dir / f"page_{page_number:03d}.json"
        if not path.exists():
            raise FileNotFoundError(f"Label JSON not found: {path}")
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

    def _load_page_image(self, page_number):
        images = convert_from_path(
            self.document_path,
            dpi=RENDER_DPI,
            first_page=page_number,
            last_page=page_number,
        )
        if self.page_image:
            self.page_image.close()
        self.page_image = images[0]
        self.original_width  = self.page_image.width
        self.original_height = self.page_image.height

    def save_page_data(self):
        self._prepare_page_data_for_save()
        path = self.config_dir / f"page_{self.current_page:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.page_data, f, indent=2)

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
        if self.current_page > 1:
            self.load_page(self.current_page - 1)

    def next_page(self):
        self._cancel_draw_if_active()
        self.save_page_data()
        if self.current_page < self.total_pages:
            self.load_page(self.current_page + 1)

    def go_to_page(self):
        self._cancel_draw_if_active()
        try:
            requested = int(self.page_entry.get())
        except ValueError:
            return
        requested = max(1, min(self.total_pages, requested))
        self.save_page_data()
        self.load_page(requested)

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
        polygon = [{"x": int(round(v["x"])), "y": int(round(v["y"]))}
                   for v in self.draw_vertices]
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
            polygons.pop(self.selected_polygon_idx)
            self.selected_polygon_idx = None
            self._draw_scene()
            self._update_status()

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
        scale_x = (cw - 16) / max(1, self.original_width)
        scale_y = (ch - 16) / max(1, self.original_height)
        self.display_scale   = min(scale_x, scale_y)
        self.rendered_width  = max(1, int(round(self.original_width  * self.display_scale)))
        self.rendered_height = max(1, int(round(self.original_height * self.display_scale)))
        self.image_offset_x  = max(0, (cw - self.rendered_width)  // 2)
        self.image_offset_y  = max(0, (ch - self.rendered_height) // 2)

    def _refresh_image(self):
        self._refresh_render_metrics()
        resized = self.page_image.resize((self.rendered_width, self.rendered_height))
        self.tk_image = ImageTk.PhotoImage(resized)

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
        if self.draw_mode:
            self._draw_in_progress()

    def _draw_all_polygons(self):
        docs = self.page_data.get("documents", {})
        doc_record = docs.get(self.current_doc, {})

        # Only render polygons for the active document so other documents stay hidden.
        for info_type in LABEL_INFO_TYPES:
            color = LABEL_INFO_TYPE_COLORS.get(info_type, LABEL_DEFAULT_COLOR)
            polygons = doc_record.get(info_type, [])
            is_active_type = info_type == self.current_info_type

            for poly_idx, polygon in enumerate(polygons):
                if len(polygon) < 3:
                    continue

                is_selected = is_active_type and poly_idx == self.selected_polygon_idx
                canvas_poly = self._poly_to_canvas(polygon)
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
        if self.draw_mode:
            self._draw_scene()

    def _on_canvas_click(self, event):
        if self.draw_mode:
            self._handle_draw_click(event)
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
                canvas_poly = self._poly_to_canvas(polygons[idx])
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
            if len(polygon) < 3:
                continue
            if _point_in_polygon(event.x, event.y, self._poly_to_canvas(polygon)):
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
                if len(polygon) < 3:
                    continue
                if _point_in_polygon(event.x, event.y, self._poly_to_canvas(polygon)):
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
        polygon = polygons[self.selected_polygon_idx]

        if self.drag_mode == "vertex" and self.drag_vertex_idx is not None:
            if self.drag_vertex_idx < len(polygon):
                polygon[self.drag_vertex_idx]["x"] = ox
                polygon[self.drag_vertex_idx]["y"] = oy

        elif self.drag_mode == "polygon" and self.drag_last_x is not None:
            dx = ox - self.drag_last_x
            dy = oy - self.drag_last_y
            for pt in polygon:
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

    # ── Status ─────────────────────────────────────────────────────────────────

    def _update_status(self):
        num_polys = len(self._current_polygons())
        self.status_label.configure(
            text=(
                f"Page {self.current_page}/{self.total_pages}  │  "
                f"{self.current_doc}  │  "
                f"{self.current_info_type.replace('_', ' ')}  │  "
                f"{num_polys} polygon(s)"
            )
        )

    # ── Close ──────────────────────────────────────────────────────────────────

    def _on_close(self):
        self._cancel_draw_if_active()
        self.save_page_data()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def main():
    try:
        app = LabelEditorApp()
        app.run()
    except Exception as exc:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Label Editor Error", str(exc))
        root.destroy()
        raise


if __name__ == "__main__":
    main()
