#!/usr/bin/env python3
"""Review CONTENTS pages by flipping between snap variants in the editor.

Generates the three post-processing versions of each contents page from the SAVED
RAW model output (apply_post — instant, no model calls): raw, soft, medium. Then
opens the editor in VARIANT MODE on those pages, where you:
  • press Tab (or the ◀ variant ▶ button) to flip raw → soft → medium (→ your
    reviewed copy if one exists),
  • edit the active version like any page,
  • click "✓ Save this → gold" to write the version you picked into reviewed/.

  • or pass --scratch to skip the variants entirely and label each page from a
    BLANK slate yourself (pages already in reviewed/ still load your saved work,
    so finished pages are never clobbered).

Usage:
  ./venv/bin/python review_variants.py Appendix_2
  ./venv/bin/python review_variants.py Volume_2
  ./venv/bin/python review_variants.py Volume_2 --scratch
"""
from __future__ import annotations
import json, os, subprocess, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = str(HERE / "venv" / "bin" / "python")
MODES = ["raw", "soft", "medium"]


def main():
    argv = sys.argv[1:]
    scratch = "--scratch" in argv
    positional = [a for a in argv if not a.startswith("-")]
    if not positional or argv[0] in ("-h", "--help"):
        pages = json.loads((HERE / "qa_output" / "contents_pages.json").read_text())
        print(__doc__)
        print("Volumes with contents pages: " + ", ".join(pages))
        return
    vol = positional[0]
    pages = json.loads((HERE / "qa_output" / "contents_pages.json").read_text())
    if vol not in pages:
        raise SystemExit(f"{vol} has no contents pages. Options: {list(pages)}")

    if scratch:
        print(f"SCRATCH mode — opening {vol} contents pages {pages[vol]} in the editor. "
              f"Pages you've already reviewed load your saved work; the rest start blank "
              f"so you label them yourself. Auto-saves to reviewed/ as usual.")
        env = {**os.environ,
               "EDITOR_DOCUMENT": vol,
               "EDITOR_PAGES": ",".join(map(str, pages[vol])),
               "EDITOR_SCRATCH": "1"}
        subprocess.run([PY, str(HERE / "normalized_editor.py")], cwd=HERE, env=env)
        return

    spec = f"{vol}:{','.join(map(str, pages[vol]))}"

    # build the three variants from the saved raw (instant)
    for m in MODES:
        r = subprocess.run([PY, "apply_post.py", spec, "--snap-mode", m, "--contents"],
                           cwd=HERE, capture_output=True, text=True)
        if "NO raw saved" in r.stdout:
            raise SystemExit(f"Some pages have no saved raw — re-run the labeler once:\n"
                             f"  ./venv/bin/python relabel_contents.py {spec}")
    print(f"built raw/soft/medium for {vol} pages {pages[vol]} — opening the editor "
          f"(Tab to flip, '✓ Save this → gold' to keep one).")
    env = {**os.environ,
           "EDITOR_DOCUMENT": vol,
           "EDITOR_PAGES": ",".join(map(str, pages[vol])),
           "EDITOR_VARIANTS": ",".join(f"{m}:snap_compare/{m}" for m in MODES)}
    subprocess.run([PY, str(HERE / "normalized_editor.py")], cwd=HERE, env=env)


if __name__ == "__main__":
    main()
