#!/usr/bin/env python3
"""Open the editor on exactly this volume's CONTENTS pages, showing the NEW
relabel (not the old gold) so corrections you save land in reviewed/.

It just launches normalized_editor.py with the right env so you don't type it:
  EDITOR_DOCUMENT=<Vol> EDITOR_PAGES=<contents pages> EDITOR_PREFER_AUTO=1

In the editor: A/D = page nav; edit normally (saves to reviewed/ on page change);
if a page is already correct hit "✓ Approve → gold" to copy it into reviewed/
unchanged. That is how a reviewed contents page ends up in reviewed/.

Usage:
  ./venv/bin/python review_contents.py Volume_2
  ./venv/bin/python review_contents.py            # list volumes that have contents pages
"""
from __future__ import annotations
import json, os, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
LIST = HERE / "qa_output" / "contents_pages.json"


def main():
    pages_by_vol = json.loads(LIST.read_text())
    if len(sys.argv) < 2:
        print("Contents pages to review — run: ./venv/bin/python review_contents.py <Volume>")
        for v, ps in pages_by_vol.items():
            print(f"  {v}: {','.join(map(str, ps))}")
        return
    vol = sys.argv[1]
    if vol not in pages_by_vol:
        raise SystemExit(f"{vol} has no contents pages. Options: {list(pages_by_vol)}")
    env = {**os.environ,
           "EDITOR_DOCUMENT": vol,
           "EDITOR_PAGES": ",".join(map(str, pages_by_vol[vol])),
           "EDITOR_PREFER_AUTO": "1"}
    print(f"Opening {vol} contents pages {pages_by_vol[vol]} — showing the new relabel; "
          f"your saves go to reviewed/.")
    subprocess.run([str(HERE / "venv" / "bin" / "python"),
                    str(HERE / "normalized_editor.py")], cwd=HERE, env=env)


if __name__ == "__main__":
    main()
