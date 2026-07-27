#!/usr/bin/env python3
"""STEP_6 setup — bring step_5/3_enriched in here as the segmentation input.

Hardlinks each file when possible (same filesystem -> ~zero extra disk; same trick as
enrich_pages.py), copies across filesystems. Idempotent: re-run to fill gaps; --fresh wipes
first. step_6 work is non-destructive (reads these, writes NEW artifacts), so hardlinks are safe.

Usage:
  python3 copy_input.py                 # ../step_5/3_enriched -> ./3_enriched
  python3 copy_input.py --fresh         # delete ./3_enriched first, then rebuild
"""
from __future__ import annotations
import os, shutil, argparse
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="../step_5/3_enriched")
    ap.add_argument("--dst", default="3_enriched")
    ap.add_argument("--fresh", action="store_true", help="delete dst first, then rebuild")
    a = ap.parse_args()
    src = (HERE / a.src).resolve()
    dst = (HERE / a.dst).resolve()
    if not src.is_dir():
        raise SystemExit(f"no source dir: {src}")
    if dst == src or src in dst.parents:
        raise SystemExit("dst must be a separate folder from src")
    if a.fresh and dst.exists():
        if HERE not in dst.parents:
            raise SystemExit(f"refusing --fresh on {dst} (not under {HERE})")
        shutil.rmtree(dst); print(f"[--fresh] removed {dst}")

    linked = copied = skipped = 0
    for s in src.rglob("*"):
        if s.is_dir():
            continue
        d = dst / s.relative_to(src)
        d.parent.mkdir(parents=True, exist_ok=True)
        if d.exists():
            skipped += 1; continue
        try:
            os.link(s, d); linked += 1
        except OSError:
            shutil.copy2(s, d); copied += 1
    print(f"done: {linked} hardlinked, {copied} copied, {skipped} already present")
    print(f"  src: {src}")
    print(f"  dst: {dst}")


if __name__ == "__main__":
    main()
