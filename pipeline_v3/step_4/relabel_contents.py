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
import json, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = str(HERE / "venv" / "bin" / "python")
NOTE = HERE / "volume_notes" / "_contents_page.md"
TEMPLATE_PIN = "Volume_2/page_006"
from contents_connect import connect_contents


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: relabel_contents.py <Vol>:<pages> [<Vol>:<pages> ...]")
    specs = [s.split(":") for s in sys.argv[1:]]
    for vol, pages in specs:
        print(f"\n=== {vol}: pages {pages} ===")
        # pass-1 label with the contents note + pinned template, connections off
        cmd = [PY, "auto_labeler.py", "--volume", vol, "--pages", pages,
               "--overwrite", "--no-connections", "--no-layout-sim",
               "--pin-examples", TEMPLATE_PIN, "--extra-note-file", str(NOTE),
               "--contrast-pairs", "0"]
        r = subprocess.run(cmd, cwd=HERE)
        if r.returncode != 0:
            print(f"  labeling FAILED for {vol} — skipping connect"); continue
        # wire the triangle geometrically on each relabeled page
        from auto_labeler import parse_pages_arg
        for n in sorted(parse_pages_arg(pages)):
            jp = HERE / "auto_labeled" / vol / f"page_{n:03d}" / f"page_{n:03d}.json"
            if not jp.exists():
                print(f"  page_{n:03d}: no output"); continue
            d = json.loads(jp.read_text())
            edges = connect_contents(d["documents"])
            jp.write_text(json.dumps(d, indent=2))
            cats = {}
            for doc in d["documents"].values():
                for c, ps in doc.items():
                    if isinstance(ps, list) and ps:
                        cats[c] = cats.get(c, 0) + len(ps)
            print(f"  page_{n:03d}: "
                  f"commentary={cats.get('archv_commentary',0)} date={cats.get('archv_date',0)} "
                  f"structdoc={cats.get('struct_doc',0)} -> {edges} triangle edges")


if __name__ == "__main__":
    main()
