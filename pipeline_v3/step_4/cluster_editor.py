#!/usr/bin/env python3
"""Cluster Editor — David's review tool for bootstrap stage B (cluster confirm).

A file-browser-style reviewer for qa_output/<Vol>/clusters.json:

  HOME       clusters as folder tiles (name, size, preview). Double-click opens.
  GRID       the cluster's pages as a scrollable grid of REAL thumbnails (like
             a file manager). Double-click a page to drop into it.
  PAGE       full-size page view. Scroll wheel / Ctrl+D / Ctrl+A (or ←/→) walk
             the cluster's pages starting wherever you double-clicked.
             M (or the button) flags the page as MISPLACED.
  MISPLACED  open the ⚑ Misplaced tile on home: each flagged page is shown big
             with one button per cluster — click where it belongs, or
             [+ New cluster…] if it fits nowhere. Skip leaves it for later.

Save (button, or prompted on close) writes the new memberships back to
clusters.json (timestamped .bak first). Unresolved misplaced pages persist in
qa_output/<Vol>/cluster_review.json so you can stop mid-review; bootstrap warns
at --confirm-clusters while any remain. The page-label JSONs are NEVER touched.

Usage:
    ./venv/bin/python cluster_editor.py Volume_3
"""
from __future__ import annotations

import json
import queue
import re
import sys
import threading
from datetime import datetime
from pathlib import Path

import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from pdf2image import convert_from_path
from PIL import Image, ImageTk

SCRIPT_DIR = Path(__file__).resolve().parent
QA_DIR     = SCRIPT_DIR / "qa_output"
CROPS_DIR  = SCRIPT_DIR / "polygon_cropped_pdfs"
THUMB_DIR  = SCRIPT_DIR / "label_review" / "thumb_cache"

THUMB_W   = 210          # grid thumbnail width (readable, file-manager-like)
TILE_W    = 230
BG, PANEL, FG = "#1e1e1e", "#2d2d2d", "#dddddd"
ACCENT, WARN  = "#4ea1ff", "#ff5555"


# ── pure model (unit-tested in run_all_tests F11) ─────────────────────────────

def load_model(clusters_path: Path, review_path: Path) -> dict:
    """Model: {"volume", "names" {cid: display}, "members" {cid: [page,...]},
    "misplaced" [page,...], "origin" {page: cid it was flagged from}}."""
    data = json.loads(clusters_path.read_text())
    members = {cid: sorted(e["pages"]) for cid, e in data["clusters"].items()}
    names = {cid: e.get("display_name", "") for cid, e in data["clusters"].items()}
    misplaced, origin = [], {}
    if review_path.exists():
        r = json.loads(review_path.read_text())
        misplaced = r.get("misplaced", [])
        origin = r.get("origin", {})
        for p in misplaced:                      # a flagged page sits in the pen, not its cluster
            for cid in members:
                if p in members[cid]:
                    origin.setdefault(p, cid)
                    members[cid].remove(p)
    return {"volume": data["volume"], "raw": data, "names": names,
            "members": members, "misplaced": misplaced, "origin": origin}


def flag_misplaced(model: dict, cid: str, page: str) -> None:
    if page in model["members"].get(cid, []):
        model["members"][cid].remove(page)
        model["misplaced"].append(page)
        model["origin"][page] = cid


def unflag(model: dict, page: str) -> None:
    """Return a flagged page to the cluster it came from."""
    if page in model["misplaced"]:
        model["misplaced"].remove(page)
        cid = model["origin"].pop(page, None)
        if cid in model["members"]:
            model["members"][cid].append(page)
            model["members"][cid].sort()


def assign(model: dict, page: str, cid: str) -> None:
    """Resolve a misplaced page into cluster `cid` (existing or new)."""
    if page in model["misplaced"]:
        model["misplaced"].remove(page)
        model["origin"].pop(page, None)
    model["members"].setdefault(cid, [])
    model["names"].setdefault(cid, "")
    if page not in model["members"][cid]:
        model["members"][cid].append(page)
        model["members"][cid].sort()


def new_cluster_id(model: dict, label: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "_", label.strip()).strip("_").lower() or "new"
    cid, i = base, 2
    while cid in model["members"]:
        cid, i = f"{base}_{i}", i + 1
    return cid


def save_model(model: dict, clusters_path: Path, review_path: Path) -> str:
    """Write memberships back to clusters.json (.bak first); unresolved
    misplaced pages persist in the review sidecar. Returns the backup path."""
    bak = clusters_path.with_suffix(f".bak-{datetime.now().strftime('%H%M%S%f')}")
    bak.write_bytes(clusters_path.read_bytes())
    data = model["raw"]
    data["clusters"] = {
        cid: {"display_name": model["names"].get(cid, ""),
              "size": len(pages), "pages": sorted(pages),
              "most_central": sorted(pages)[:3]}
        for cid, pages in model["members"].items() if pages
    }
    data["page_to_cluster"] = {p: cid for cid, pages in model["members"].items()
                               for p in pages}
    data["k"] = len(data["clusters"])
    data["edited_by_david"] = datetime.now().isoformat(timespec="seconds")
    clusters_path.write_text(json.dumps(data, indent=2) + "\n")
    if model["misplaced"]:
        review_path.write_text(json.dumps(
            {"misplaced": model["misplaced"], "origin": model["origin"]},
            indent=2) + "\n")
    elif review_path.exists():
        review_path.unlink()
    return str(bak)


# ── the app ───────────────────────────────────────────────────────────────────

class ClusterEditor:
    def __init__(self, volume: str):
        self.volume = volume
        self.cpath = QA_DIR / volume / "clusters.json"
        self.rpath = QA_DIR / volume / "cluster_review.json"
        if not self.cpath.exists():
            raise SystemExit(f"{self.cpath} missing — run: ./venv/bin/python bootstrap.py {volume}")
        self.model = load_model(self.cpath, self.rpath)
        self.dirty = False

        self.thumb_dir = THUMB_DIR / volume
        self.thumb_dir.mkdir(parents=True, exist_ok=True)
        self._thumb_q: "queue.Queue[str]" = queue.Queue()
        self._thumb_done: "queue.Queue[str]" = queue.Queue()
        self._photo_refs: dict = {}              # keep PhotoImages alive
        self._page_render_cache: dict = {}
        threading.Thread(target=self._thumb_worker, daemon=True).start()

        self.root = tk.Tk()
        self.root.title(f"Cluster Editor — {volume}")
        self.root.configure(bg=BG)
        self.root.geometry("1500x950")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.header = tk.Frame(self.root, bg=PANEL, pady=6)
        self.header.pack(side=tk.TOP, fill=tk.X)
        self.body = tk.Frame(self.root, bg=BG)
        self.body.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.root.bind("<Escape>", lambda e: self._go_back())
        self.root.bind("<Control-d>", lambda e: self._nav(+1))
        self.root.bind("<Control-a>", lambda e: self._nav(-1))
        self.root.bind("<Right>", lambda e: self._nav(+1))
        self.root.bind("<Left>",  lambda e: self._nav(-1))
        self.root.bind("<Key-m>", lambda e: self._toggle_misplaced())
        self.root.bind("<Key-M>", lambda e: self._toggle_misplaced())

        self.view = None          # ("home") | ("grid", cid) | ("page", cid, i) | ("resolve", i)
        self.show_home()
        self.root.after(200, self._poll_thumbs)
        self.root.mainloop()

    # ── shared plumbing ────────────────────────────────────────────────────────

    def _pdf(self, page: str) -> Path:
        return CROPS_DIR / self.volume / "pages" / f"{page}.pdf"

    def _thumb_worker(self):
        while True:
            page = self._thumb_q.get()
            out = self.thumb_dir / f"{page}.png"
            if not out.exists():
                try:
                    img = convert_from_path(self._pdf(page), dpi=36)[0]
                    img.thumbnail((THUMB_W, THUMB_W * 2))
                    img.save(out)
                except Exception:
                    Image.new("RGB", (THUMB_W, int(THUMB_W * 1.3)), "#555").save(out)
            self._thumb_done.put(page)

    def _poll_thumbs(self):
        updated = False
        try:
            while True:
                page = self._thumb_done.get_nowait()
                lbl = self._photo_refs.get(("pending", page))
                if lbl is not None and lbl.winfo_exists():
                    self._set_thumb(lbl, page)
                    updated = True
        except queue.Empty:
            pass
        if updated:
            self.root.update_idletasks()
        self.root.after(200, self._poll_thumbs)

    def _set_thumb(self, lbl, page):
        try:
            ph = ImageTk.PhotoImage(Image.open(self.thumb_dir / f"{page}.png"))
            self._photo_refs[("img", page)] = ph
            lbl.configure(image=ph, text="")
        except Exception:
            pass

    def _clear(self):
        for w in self.header.winfo_children():
            w.destroy()
        for w in self.body.winfo_children():
            w.destroy()
        self._photo_refs = {k: v for k, v in self._photo_refs.items() if k[0] == "img"}

    def _hbtn(self, text, cmd, side=tk.LEFT, color=FG):
        b = tk.Button(self.header, text=text, command=cmd, bg=PANEL, fg=color,
                      activebackground="#3d3d3d", activeforeground=color, bd=1)
        b.pack(side=side, padx=6)
        return b

    def _htext(self, text, color=FG, size=11, bold=True):
        tk.Label(self.header, text=text, bg=PANEL, fg=color,
                 font=("TkDefaultFont", size, "bold" if bold else "normal")
                 ).pack(side=tk.LEFT, padx=10)

    def _mark_dirty(self):
        self.dirty = True
        self.root.title(f"Cluster Editor — {self.volume}  ● unsaved")

    def _save(self):
        bak = save_model(self.model, self.cpath, self.rpath)
        self.dirty = False
        self.root.title(f"Cluster Editor — {self.volume}")
        messagebox.showinfo("Saved", f"clusters.json updated (backup: {Path(bak).name})."
                            + (f"\n{len(self.model['misplaced'])} page(s) still in Misplaced."
                               if self.model["misplaced"] else
                               "\nNext: ./venv/bin/python bootstrap.py "
                               f"{self.volume} --confirm-clusters"))

    def _on_close(self):
        if self.dirty:
            if messagebox.askyesno("Unsaved changes", "Save cluster edits before closing?"):
                self._save()
        self.root.destroy()

    def _go_back(self):
        if self.view[0] == "page":
            self.show_grid(self.view[1])
        elif self.view[0] in ("grid", "resolve"):
            self.show_home()

    def _nav(self, step):
        if self.view[0] == "page":
            _v, cid, i = self.view
            pages = self.model["members"].get(cid, [])
            if pages:
                self.show_page(cid, (i + step) % len(pages))
        elif self.view[0] == "resolve":
            n = len(self.model["misplaced"])
            if n:
                self.show_resolve((self.view[1] + step) % n)
        return "break"

    # ── HOME ──────────────────────────────────────────────────────────────────

    def show_home(self):
        self.view = ("home",)
        self._clear()
        self._htext(f"{self.volume} — clusters", size=13)
        self._hbtn("💾 Save", self._save, side=tk.RIGHT, color=ACCENT)
        tk.Label(self.header, text="double-click a folder · Esc backs out",
                 bg=PANEL, fg="#888").pack(side=tk.RIGHT, padx=10)

        grid = tk.Frame(self.body, bg=BG)
        grid.pack(padx=20, pady=20, anchor="nw")
        tiles = sorted(self.model["members"].items())
        if self.model["misplaced"]:
            tiles.append(("⚑ MISPLACED", self.model["misplaced"]))
        for i, (cid, pages) in enumerate(tiles):
            f = tk.Frame(grid, bg=PANEL, bd=1, relief=tk.RIDGE,
                         width=TILE_W, height=TILE_W + 40)
            f.grid(row=i // 5, column=i % 5, padx=12, pady=12)
            f.grid_propagate(False)
            is_pen = cid == "⚑ MISPLACED"
            color = WARN if is_pen else ACCENT
            tk.Label(f, text="🗂" if not is_pen else "⚑", font=("TkDefaultFont", 34),
                     bg=PANEL, fg=color).pack(pady=(14, 2))
            nm = self.model["names"].get(cid, "")
            tk.Label(f, text=cid, bg=PANEL, fg=FG,
                     font=("TkDefaultFont", 11, "bold")).pack()
            tk.Label(f, text=(nm[:30] + "\n" if nm else "") + f"{len(pages)} pages",
                     bg=PANEL, fg="#999").pack()
            tgt = (lambda e, c=cid: self.show_resolve(0)) if is_pen else \
                  (lambda e, c=cid: self.show_grid(c))
            for w in (f, *f.winfo_children()):
                w.bind("<Double-Button-1>", tgt)

    # ── GRID ──────────────────────────────────────────────────────────────────

    def show_grid(self, cid: str):
        self.view = ("grid", cid)
        self._clear()
        self._hbtn("← Clusters", self.show_home)
        pages = self.model["members"].get(cid, [])
        self._htext(f"🗂 {cid} — {len(pages)} pages")
        tk.Label(self.header, text="double-click a page to open it",
                 bg=PANEL, fg="#888").pack(side=tk.RIGHT, padx=10)

        canvas = tk.Canvas(self.body, bg=BG, highlightthickness=0)
        sb = ttk.Scrollbar(self.body, orient=tk.VERTICAL, command=canvas.yview)
        inner = tk.Frame(canvas, bg=BG)
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=sb.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        for ev in ("<Button-4>", "<Button-5>", "<MouseWheel>"):
            canvas.bind_all(ev, lambda e: canvas.yview_scroll(
                -1 if (getattr(e, "num", 0) == 4 or getattr(e, "delta", 0) > 0) else 1, "units"))

        cols = 6
        for i, page in enumerate(pages):
            cell = tk.Frame(inner, bg=PANEL, bd=1, relief=tk.SOLID)
            cell.grid(row=i // cols, column=i % cols, padx=8, pady=8)
            lbl = tk.Label(cell, text="rendering…", bg="#333", fg="#777",
                           width=26, height=14)
            lbl.pack()
            cap = "⚑ " if page in self.model["misplaced"] else ""
            tk.Label(cell, text=f"{cap}{page}", bg=PANEL, fg=FG).pack(fill=tk.X)
            if (self.thumb_dir / f"{page}.png").exists():
                self._set_thumb(lbl, page)
            else:
                self._photo_refs[("pending", page)] = lbl
                self._thumb_q.put(page)
            for w in (cell, lbl, *cell.winfo_children()):
                w.bind("<Double-Button-1>",
                       lambda e, c=cid, idx=i: self.show_page(c, idx))

    # ── PAGE ──────────────────────────────────────────────────────────────────

    def _render_page(self, page: str, max_h: int) -> ImageTk.PhotoImage:
        key = (page, max_h)
        if key not in self._page_render_cache:
            img = convert_from_path(self._pdf(page), dpi=110)[0]
            scale = min(1.0, max_h / img.height)
            img = img.resize((int(img.width * scale), int(img.height * scale)),
                             Image.LANCZOS)
            self._page_render_cache.clear()       # one big render at a time
            self._page_render_cache[key] = ImageTk.PhotoImage(img)
        return self._page_render_cache[key]

    def show_page(self, cid: str, idx: int):
        pages = self.model["members"].get(cid, [])
        if not pages:
            return self.show_grid(cid)
        idx %= len(pages)
        self.view = ("page", cid, idx)
        page = pages[idx]
        self._clear()
        self._hbtn("← Grid", lambda: self.show_grid(cid))
        self._htext(f"🗂 {cid}   {page}   ({idx + 1}/{len(pages)})")
        flagged = page in self.model["misplaced"]
        self._hbtn("⚑ MISPLACED — undo" if flagged else "⚑ Mark MISPLACED (M)",
                   self._toggle_misplaced, side=tk.RIGHT,
                   color=WARN)
        tk.Label(self.header, text="wheel / Ctrl+D / Ctrl+A / ←→ = pages · Esc = grid",
                 bg=PANEL, fg="#888").pack(side=tk.RIGHT, padx=8)

        holder = tk.Frame(self.body, bg=BG)
        holder.pack(fill=tk.BOTH, expand=True)
        self.root.update_idletasks()
        ph = self._render_page(page, max_h=self.body.winfo_height() or 850)
        lbl = tk.Label(holder, image=ph, bg=BG)
        lbl.pack(pady=4)
        if flagged:
            tk.Label(holder, text="⚑ flagged as misplaced", bg=WARN, fg="white",
                     font=("TkDefaultFont", 11, "bold")).pack(fill=tk.X)
        for ev in ("<Button-4>", "<Button-5>", "<MouseWheel>"):
            lbl.bind(ev, lambda e: self._nav(
                +1 if (getattr(e, "num", 0) == 5 or getattr(e, "delta", 0) < 0) else -1))

    def _toggle_misplaced(self):
        if self.view[0] != "page":
            return
        _v, cid, idx = self.view
        pages = self.model["members"].get(cid, [])
        if not pages:
            return
        page = pages[idx % len(pages)]
        if page in self.model["misplaced"]:
            unflag(self.model, page)
        else:
            flag_misplaced(self.model, cid, page)
            idx = idx % max(1, len(self.model["members"][cid]) or 1)
        self._mark_dirty()
        if self.model["members"][cid]:
            self.show_page(cid, min(idx, len(self.model["members"][cid]) - 1))
        else:
            self.show_home()

    # ── RESOLVE (the misplaced pen) ───────────────────────────────────────────

    def show_resolve(self, idx: int):
        pen = self.model["misplaced"]
        if not pen:
            return self.show_home()
        idx %= len(pen)
        self.view = ("resolve", idx)
        page = pen[idx]
        self._clear()
        self._hbtn("← Clusters", self.show_home)
        self._htext(f"⚑ Misplaced   {page}   ({idx + 1}/{len(pen)})", color=WARN)
        tk.Label(self.header, text="pick its real cluster · Ctrl+D skips · Esc home",
                 bg=PANEL, fg="#888").pack(side=tk.RIGHT, padx=8)

        bar = tk.Frame(self.body, bg=PANEL)
        bar.pack(side=tk.TOP, fill=tk.X)
        tk.Label(bar, text="Belongs in:", bg=PANEL, fg=FG,
                 font=("TkDefaultFont", 10, "bold")).pack(side=tk.LEFT, padx=(10, 4), pady=6)
        for cid in sorted(self.model["members"]):
            tk.Button(bar, text=f"{cid} ({len(self.model['members'][cid])})",
                      bg="#3a3a3a", fg=ACCENT, bd=1,
                      command=lambda c=cid, p=page: self._resolve_to(p, c)
                      ).pack(side=tk.LEFT, padx=3, pady=4)
        tk.Button(bar, text="+ New cluster…", bg="#3a3a3a", fg="#7be07b", bd=1,
                  command=lambda p=page: self._resolve_new(p)).pack(side=tk.LEFT, padx=10)
        tk.Button(bar, text="↩ back to original", bg="#3a3a3a", fg="#bbb", bd=1,
                  command=lambda p=page: self._resolve_unflag(p)).pack(side=tk.RIGHT, padx=8)

        holder = tk.Frame(self.body, bg=BG)
        holder.pack(fill=tk.BOTH, expand=True)
        self.root.update_idletasks()
        ph = self._render_page(page, max_h=(self.body.winfo_height() or 850) - 50)
        tk.Label(holder, image=ph, bg=BG).pack(pady=4)

    def _resolve_to(self, page, cid):
        assign(self.model, page, cid)
        self._mark_dirty()
        self.show_resolve(0) if self.model["misplaced"] else self.show_home()

    def _resolve_new(self, page):
        label = simpledialog.askstring("New cluster",
                                       "Name for the new cluster (e.g. 'index pages'):",
                                       parent=self.root)
        if not label:
            return
        cid = new_cluster_id(self.model, label)
        self.model["names"][cid] = label
        assign(self.model, page, cid)
        self._mark_dirty()
        self.show_resolve(0) if self.model["misplaced"] else self.show_home()

    def _resolve_unflag(self, page):
        unflag(self.model, page)
        self._mark_dirty()
        self.show_resolve(0) if self.model["misplaced"] else self.show_home()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: ./venv/bin/python cluster_editor.py <Volume>")
    ClusterEditor(sys.argv[1])
