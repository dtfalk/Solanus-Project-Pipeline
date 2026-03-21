"""
Step 2: Edit Polygon Configs — interactive editor for per-page crop polygons.

This script opens a single document page at a time, overlays the configured crop
polygon, and lets you drag either individual vertices or the whole polygon.
When you save, the polygon JSON file for that page is updated in place.

Controls:
  - Drag a corner handle to move one vertex
  - Drag inside the polygon to move the whole polygon
  - Save button or S key to write changes back to JSON
  - Previous / Next buttons or Left / Right arrow keys to change pages

Usage:
    python step_2/polygon_editor.py
"""

import json
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from pdf2image import convert_from_path
from PIL import ImageTk

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_2.config import (
    POLYGON_CONFIG_DIR,
    POLYGON_EDITOR_DOCUMENT,
    POLYGON_EDITOR_MAX_PREVIEW_DIMENSION,
    POLYGON_EDITOR_START_PAGE,
    POLYGON_INPUT_DIR,
    RENDER_DPI,
)


# ── File paths (all relative to this script's directory) ───────────────────────
SCRIPT_DIR  = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPT_DIR.parent
INPUT_DIR   = ROOT_DIR / POLYGON_INPUT_DIR
CONFIG_DIR  = ROOT_DIR / POLYGON_CONFIG_DIR


HANDLE_RADIUS = 6


def scale_polygon(polygon, scale_factor):
    """Scale a polygon by the given scale factor."""
    return [
        {
            "x": point["x"] * scale_factor,
            "y": point["y"] * scale_factor,
        }
        for point in polygon
    ]


def point_in_polygon(x_coordinate, y_coordinate, polygon):
    """Return True if the given point lies inside the polygon."""
    inside = False
    point_count = len(polygon)

    for point_index in range(point_count):
        x1 = polygon[point_index]["x"]
        y1 = polygon[point_index]["y"]
        x2 = polygon[(point_index + 1) % point_count]["x"]
        y2 = polygon[(point_index + 1) % point_count]["y"]

        intersects = ((y1 > y_coordinate) != (y2 > y_coordinate)) and (
            x_coordinate < (x2 - x1) * (y_coordinate - y1) / (y2 - y1 + 1e-9) + x1
        )
        if intersects:
            inside = not inside

    return inside


class PolygonEditorApp:
    def __init__(self):
        self.document_path = self._get_document_path()
        self.document_name = self.document_path.stem
        self.page_sizes_dir = CONFIG_DIR / self.document_name / "page_sizes"
        self.polygons_dir = CONFIG_DIR / self.document_name / "polygons"
        self.total_pages = self._get_total_pages()
        self.current_page = max(1, min(POLYGON_EDITOR_START_PAGE, self.total_pages))

        self.root = tk.Tk()
        self.root.title(f"Polygon Editor — {self.document_name}")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.canvas = tk.Canvas(self.root, bg = "#1e1e1e", highlightthickness = 0)
        self.canvas.pack(fill = tk.BOTH, expand = True)

        controls = ttk.Frame(self.root)
        controls.pack(fill = tk.X, padx = 8, pady = 8)

        ttk.Button(controls, text = "Previous", command = self.previous_page).pack(side = tk.LEFT)
        ttk.Button(controls, text = "Save", command = self.save_current_polygon).pack(side = tk.LEFT, padx = 8)
        ttk.Button(controls, text = "Next", command = self.next_page).pack(side = tk.LEFT)

        ttk.Label(controls, text = "Page:").pack(side = tk.LEFT, padx = (12, 4))
        self.page_entry = ttk.Entry(controls, width = 8)
        self.page_entry.pack(side = tk.LEFT)
        ttk.Button(controls, text = "Go", command = self.go_to_page).pack(side = tk.LEFT, padx = (4, 0))

        self.status_label = ttk.Label(controls, text = "")
        self.status_label.pack(side = tk.LEFT, padx = 12)

        self.root.bind("<Left>", lambda event: self.previous_page())
        self.root.bind("<Right>", lambda event: self.next_page())
        self.root.bind("<Return>", lambda event: self.go_to_page())
        self.root.bind("<s>", lambda event: self.save_current_polygon())
        self.root.bind("<S>", lambda event: self.save_current_polygon())

        self.canvas.bind("<ButtonPress-1>", self.on_mouse_down)
        self.canvas.bind("<B1-Motion>", self.on_mouse_drag)
        self.canvas.bind("<ButtonRelease-1>", self.on_mouse_up)

        self.tk_image = None
        self.preview_polygon = []
        self.scale_factor = 1.0
        self.drag_vertex_index = None
        self.drag_polygon = False
        self.last_drag_position = None

        self.load_page(self.current_page)

    def _get_document_path(self):
        if POLYGON_EDITOR_DOCUMENT:
            document_path = INPUT_DIR / POLYGON_EDITOR_DOCUMENT
            if not document_path.exists():
                raise FileNotFoundError(f"Configured editor document does not exist: {document_path}")
            return document_path

        all_pdfs = sorted(pdf_path for pdf_path in INPUT_DIR.iterdir() if pdf_path.suffix.lower() == ".pdf")
        if len(all_pdfs) == 1:
            return all_pdfs[0]

        raise FileNotFoundError(
            "Set POLYGON_EDITOR_DOCUMENT in step_2/config.py so the editor knows which PDF to open."
        )

    def _get_total_pages(self):
        if not self.page_sizes_dir.exists():
            raise FileNotFoundError(
                f"Page-size JSON directory not found: {self.page_sizes_dir}. Run polygon_initializer.py first."
            )

        return len(list(self.page_sizes_dir.glob("page_*.json")))

    def _load_single_page_image(self, page_number):
        pil_images = convert_from_path(
            self.document_path,
            dpi = RENDER_DPI,
            first_page = page_number,
            last_page = page_number,
        )
        return pil_images[0]

    def _load_polygon_record(self, page_number):
        polygon_path = self.polygons_dir / f"page_{page_number:03d}.json"
        if not polygon_path.exists():
            raise FileNotFoundError(f"Polygon JSON not found: {polygon_path}")

        with open(polygon_path, "r", encoding = "utf-8") as file:
            return json.load(file), polygon_path

    def _fit_preview_scale(self, width, height):
        max_dimension = max(width, height)
        if max_dimension <= POLYGON_EDITOR_MAX_PREVIEW_DIMENSION:
            return 1.0
        return POLYGON_EDITOR_MAX_PREVIEW_DIMENSION / max_dimension

    def _flatten_polygon(self):
        flattened = []
        for point in self.preview_polygon:
            flattened.extend([point["x"], point["y"]])
        return flattened

    def _draw_scene(self):
        self.canvas.delete("all")
        self.canvas.create_image(0, 0, anchor = tk.NW, image = self.tk_image)
        self.canvas.create_polygon(
            self._flatten_polygon(),
            fill = "#00ffff",
            stipple = "gray25",
            outline = "#00ffff",
            width = 2,
        )

        for point in self.preview_polygon:
            self.canvas.create_oval(
                point["x"] - HANDLE_RADIUS,
                point["y"] - HANDLE_RADIUS,
                point["x"] + HANDLE_RADIUS,
                point["y"] + HANDLE_RADIUS,
                fill = "#ff5f5f",
                outline = "white",
                width = 1,
            )

    def _find_handle_index(self, x_coordinate, y_coordinate):
        for point_index, point in enumerate(self.preview_polygon):
            if abs(point["x"] - x_coordinate) <= HANDLE_RADIUS * 2 and abs(point["y"] - y_coordinate) <= HANDLE_RADIUS * 2:
                return point_index
        return None

    def _clamp_preview_point(self, point):
        point["x"] = max(0, min(self.preview_width - 1, point["x"]))
        point["y"] = max(0, min(self.preview_height - 1, point["y"]))

    def load_page(self, page_number):
        self.current_page = page_number
        self.page_image = self._load_single_page_image(page_number)
        polygon_record, self.current_polygon_path = self._load_polygon_record(page_number)
        self.current_polygon_record = polygon_record

        self.original_width = self.page_image.width
        self.original_height = self.page_image.height
        self.scale_factor = self._fit_preview_scale(self.original_width, self.original_height)
        self.preview_width = int(round(self.original_width * self.scale_factor))
        self.preview_height = int(round(self.original_height * self.scale_factor))

        preview_image = self.page_image.resize((self.preview_width, self.preview_height))
        self.tk_image = ImageTk.PhotoImage(preview_image)
        self.preview_polygon = scale_polygon(self.current_polygon_record["polygon"], self.scale_factor)

        self.canvas.config(width = self.preview_width, height = self.preview_height)
        self._draw_scene()
        self.page_entry.delete(0, tk.END)
        self.page_entry.insert(0, str(self.current_page))
        self.status_label.config(
            text = (
                f"Page {self.current_page}/{self.total_pages}   "
                f"Original: {self.original_width} x {self.original_height}   "
                f"Preview: {self.preview_width} x {self.preview_height}"
            )
        )

    def save_current_polygon(self):
        saved_polygon = []
        inverse_scale = 1.0 / self.scale_factor

        for point in self.preview_polygon:
            saved_polygon.append(
                {
                    "x": int(round(point["x"] * inverse_scale)),
                    "y": int(round(point["y"] * inverse_scale)),
                }
            )

        self.current_polygon_record["polygon"] = saved_polygon

        with open(self.current_polygon_path, "w", encoding = "utf-8") as file:
            json.dump(self.current_polygon_record, file, indent = 2)

        self.status_label.config(
            text = (
                f"Saved page {self.current_page}/{self.total_pages}   "
                f"Original: {self.original_width} x {self.original_height}   "
                f"Preview: {self.preview_width} x {self.preview_height}"
            )
        )

    def previous_page(self):
        self.save_current_polygon()
        if self.current_page > 1:
            self.load_page(self.current_page - 1)

    def next_page(self):
        self.save_current_polygon()
        if self.current_page < self.total_pages:
            self.load_page(self.current_page + 1)

    def go_to_page(self):
        """Jump directly to the requested page number."""
        try:
            requested_page = int(self.page_entry.get())
        except ValueError:
            return

        requested_page = max(1, min(self.total_pages, requested_page))
        self.save_current_polygon()
        self.load_page(requested_page)

    def on_mouse_down(self, event):
        self.drag_vertex_index = self._find_handle_index(event.x, event.y)
        self.drag_polygon = False
        self.last_drag_position = {"x": event.x, "y": event.y}

        if self.drag_vertex_index is None and point_in_polygon(event.x, event.y, self.preview_polygon):
            self.drag_polygon = True

    def on_mouse_drag(self, event):
        if self.drag_vertex_index is not None:
            self.preview_polygon[self.drag_vertex_index]["x"] = event.x
            self.preview_polygon[self.drag_vertex_index]["y"] = event.y
            self._clamp_preview_point(self.preview_polygon[self.drag_vertex_index])
            self._draw_scene()
            return

        if self.drag_polygon and self.last_drag_position is not None:
            delta_x = event.x - self.last_drag_position["x"]
            delta_y = event.y - self.last_drag_position["y"]

            for point in self.preview_polygon:
                point["x"] += delta_x
                point["y"] += delta_y
                self._clamp_preview_point(point)

            self.last_drag_position = {"x": event.x, "y": event.y}
            self._draw_scene()

    def on_mouse_up(self, event):
        self.drag_vertex_index = None
        self.drag_polygon = False
        self.last_drag_position = None

    def on_close(self):
        """Save the current polygon before closing the editor window."""
        self.save_current_polygon()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def main():
    """Entry point: open the interactive polygon editor."""
    try:
        app = PolygonEditorApp()
        app.run()
    except Exception as exception:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Polygon Editor Error", str(exception))
        root.destroy()
        raise


if __name__ == "__main__":
    main()