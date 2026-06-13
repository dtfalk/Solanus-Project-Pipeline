#!/usr/bin/env python3
"""Relabel CONTENTS / INDEX pages to the V2 page_006 template (2026-06-13).

Contents pages were labeled inconsistently across the corpus (some as src_content,
some archv_commentary, dates present or not, only 1 of the 3 connection types). This
unifies them to David's chosen template: per-entry archv_commentary description +
archv_date + struct_doc page-number, with the full row TRIANGLE of connections
(commentary<->date, commentary<->struct_doc, date<->struct_doc).

How: pass-1 label each page with the contents-page note (volume_notes/_contents_page.md)
and the gold template pinned as a demo (Volume_2/page_006), connections OFF; then wire
the triangle geometrically (contents_connect.connect_contents — reproduces the template's
gold connections at ~95%). Writes auto_labeled/ ONLY — reviewed/ gold is never touched;
David reviews/approves (use EDITOR_PREFER_AUTO for pages already in reviewed).

Usage:
  ./venv/bin/python relabel_contents.py Volume_2:2,3,4,5,7,8,9,10 Volume_1:2,3,4,5 \
      Appendix_1:3 Appendix_2:3,4,5 Appendix_3:2
"""
from __future__ import annotations
import argparse, json, shutil, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = str(HERE / "venv" / "bin" / "python")
NOTE = HERE / "volume_notes" / "_contents_page.md"
TEMPLATE_PIN = "Volume_2/page_006"
from contents_connect import connect_contents


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("specs", nargs="+", metavar="Vol:pages", help="e.g. Volume_2:2,3,4")
    ap.add_argument("--snap-mode", choices=["current", "gentle", "raw"], default=None,
                    help="Compare box post-processing (see auto_labeler --snap-mode). "
                         "When given, the run writes to snap_compare/<mode>/ (so raw, "
                         "current and gentle coexist for side-by-side viewing); view each "
                         "with the EDITOR_SOURCE_DIR command printed below. Omit this flag "
                         "for the normal production relabel into auto_labeled/.")
    a = ap.parse_args()
    from auto_labeler import parse_pages_arg

    sandbox = a.snap_mode is not None        # explicit mode -> compare sandbox
    # Contents categories are all single-line, so GENTLE snap is correct here:
    # the strengthened note makes the model exclude the leader dots, and gentle
    # snap preserves that (current/aggressive would re-balloon the box across them).
    mode = a.snap_mode or "gentle"
    out_root = (HERE / "snap_compare" / mode) if sandbox else (HERE / "auto_labeled")

    for spec in a.specs:
        vol, pages = spec.split(":")
        print(f"\n=== {vol}: pages {pages}  (snap-mode {mode}) ===")
        cmd = [PY, "auto_labeler.py", "--volume", vol, "--pages", pages,
               "--overwrite", "--no-connections", "--no-layout-sim",
               "--pin-examples", TEMPLATE_PIN, "--extra-note-file", str(NOTE),
               "--contrast-pairs", "0", "--snap-mode", mode]
        r = subprocess.run(cmd, cwd=HERE)
        if r.returncode != 0:
            print(f"  labeling FAILED for {vol} — skipping connect"); continue
        for n in sorted(parse_pages_arg(pages)):
            name = f"page_{n:03d}"
            src = HERE / "auto_labeled" / vol / name           # auto_labeler always writes here
            if not (src / f"{name}.json").exists():
                print(f"  {name}: no output"); continue
            d = json.loads((src / f"{name}.json").read_text())
            edges = connect_contents(d["documents"])
            if sandbox:                                        # copy into the mode sandbox
                dest = out_root / vol / name
                dest.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src / f"{name}.pdf", dest / f"{name}.pdf")
                (dest / f"{name}.json").write_text(json.dumps(d, indent=2))
                jp = dest / f"{name}.json"
            else:
                jp = src / f"{name}.json"
                jp.write_text(json.dumps(d, indent=2))
            cats = {}
            for doc in d["documents"].values():
                for c, ps in doc.items():
                    if isinstance(ps, list) and ps:
                        cats[c] = cats.get(c, 0) + len(ps)
            print(f"  page_{n:03d}: "
                  f"commentary={cats.get('archv_commentary',0)} date={cats.get('archv_date',0)} "
                  f"structdoc={cats.get('struct_doc',0)} -> {edges} triangle edges")

    if sandbox:
        vols = sorted({s.split(':')[0] for s in a.specs})
        print(f"\nVIEW the '{mode}' version (boxes shown over the page):")
        for v in vols:
            pgs = ",".join(p for s in a.specs if s.split(':')[0] == v for p in [s.split(':')[1]])
            print(f"  EDITOR_SOURCE_DIR=snap_compare/{mode} EDITOR_DOCUMENT={v} "
                  f"EDITOR_PAGES={pgs} EDITOR_PREFER_AUTO=1 ./venv/bin/python normalized_editor.py")


if __name__ == "__main__":
    main()
