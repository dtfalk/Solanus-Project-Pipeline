#!/usr/bin/env python3
"""Apply post-processing (snap + connections) to SAVED RAW model output — no model call.

process_page now saves the raw model boxes (pre-snap) next to each page as
page_NNN.raw.json. This tool reads those and applies a chosen snap mode (+ the
contents triangle, or nothing) deterministically, so you can compare/adopt snap
modes instantly and fairly (identical raw boxes) without re-labeling.

  ./venv/bin/python apply_post.py Volume_2:6 Appendix_2:3,4,5 --snap-mode gentle --contents
      -> writes snap_compare/gentle/<Vol>/page_NNN  (view via EDITOR_SOURCE_DIR)
  ./venv/bin/python apply_post.py Volume_1:2,3,4,5 --snap-mode gentle --contents --adopt
      -> writes the result straight into auto_labeled/<Vol> (replaces the final)

--snap-mode: raw (no snap) | current (aggressive) | gentle (tight single-line).
--contents : wire the contents triangle (contents_connect) instead of leaving
             connections empty. (Letter/notebook pass-2 connections need the model,
             so they are NOT re-derived here — use the original run for those.)
"""
from __future__ import annotations
import argparse, json, shutil, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUTO = HERE / "auto_labeled"
import auto_labeler as AL
from auto_labeler import _blend_documents as _blend
from contents_connect import connect_contents


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("specs", nargs="+", metavar="Vol:pages")
    ap.add_argument("--snap-mode", choices=["raw","current","soft","medium","gentle"], default="soft")
    ap.add_argument("--contents", action="store_true", help="wire the contents triangle")
    ap.add_argument("--adopt", action="store_true",
                    help="write into auto_labeled/ (adopt) instead of snap_compare/<mode>/")
    ap.add_argument("--recenter", action="store_true",
                    help="translate single-line boxes onto their text line (fixes the "
                         "model's constant per-page upward bias). Auto-on with --contents.")
    ap.add_argument("--no-recenter", action="store_true",
                    help="disable the vertical recenter even for --contents.")
    ap.add_argument("--reseat", action="store_true",
                    help="OLD behaviour: RESIZE boxes to ink-row clusters (can inflate "
                         "boxes). Prefer --recenter, which only translates.")
    a = ap.parse_args()

    out_root = AUTO if a.adopt else (HERE / "snap_compare" / a.snap_mode)
    for spec in a.specs:
        vol, pages = spec.split(":")
        for n in sorted(AL.parse_pages_arg(pages)):
            name = f"page_{n:03d}"
            pdir = AUTO / vol / name
            raw_p = pdir / f"{name}.raw.json"
            if not raw_p.exists():
                print(f"  {vol}/{name}: NO raw saved — re-run the labeler once to capture it.")
                continue
            raw = json.loads(raw_p.read_text())
            docs = raw["documents"]
            if a.snap_mode in ("soft", "medium"):
                # LITERALLY between raw and gentle: snap a copy with gentle, then
                # move each box a fraction of the way from its raw position to its
                # gentle position.  soft = 0.25 (barely fitted), medium = 0.5.
                t = 0.25 if a.snap_mode == "soft" else 0.5
                gray = AL.render_page(pdir / f"{name}.pdf", None)[0].convert("L")
                gentle = json.loads(json.dumps(docs))
                AL.resolve_overlaps(gentle)
                AL.snap_all_polygons(gentle, gray, mode="gentle")
                _blend(docs, gentle, t)
            elif a.snap_mode != "raw":
                gray = AL.render_page(pdir / f"{name}.pdf", None)[0].convert("L")
                AL._SNAP_MODE = a.snap_mode
                AL.resolve_overlaps(docs)
                AL.snap_all_polygons(docs, gray)
            # vertical RECENTER: corrects Gemini's systematic "every box slid up by
            # the same amount" bias by TRANSLATING each single-line box onto its text
            # line (keeps box size -> can't make huge boxes, unlike the old reseat).
            # On by default for contents; --no-recenter to skip, --reseat for the old
            # resize behaviour.
            if a.reseat:
                from reseat import reseat_vertical
                g2 = AL.render_page(pdir / f"{name}.pdf", None)[0].convert("L")
                reseat_vertical(docs, g2)
            elif (a.contents or a.recenter) and not a.no_recenter:
                from recenter import recenter_vertical
                g2 = AL.render_page(pdir / f"{name}.pdf", None)[0].convert("L")
                recenter_vertical(docs, g2)
            edges = connect_contents(docs) if a.contents else 0
            full = {"page_number": raw["page_number"], "page_width": raw["page_width"],
                    "page_height": raw["page_height"], "render_dpi": raw["render_dpi"],
                    "num_documents": len(docs), "documents": docs}
            dest = out_root / vol / name
            dest.mkdir(parents=True, exist_ok=True)
            (dest / f"{name}.json").write_text(json.dumps(full, indent=2))
            if not (dest / f"{name}.pdf").exists():
                shutil.copy2(pdir / f"{name}.pdf", dest / f"{name}.pdf")
            print(f"  {vol}/{name}: snap={a.snap_mode} contents-edges={edges} -> "
                  f"{dest.relative_to(HERE)}")

    if not a.adopt:
        vols = sorted({s.split(':')[0] for s in a.specs})
        print(f"\nVIEW '{a.snap_mode}':")
        for v in vols:
            pgs = ",".join(p for s in a.specs if s.split(':')[0] == v for p in [s.split(':')[1]])
            print(f"  EDITOR_SOURCE_DIR=snap_compare/{a.snap_mode} EDITOR_DOCUMENT={v} "
                  f"EDITOR_PAGES={pgs} EDITOR_PREFER_AUTO=1 ./venv/bin/python normalized_editor.py")


if __name__ == "__main__":
    main()
