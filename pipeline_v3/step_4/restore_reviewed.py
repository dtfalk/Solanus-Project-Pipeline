#!/usr/bin/env python3
"""Restore a gold page from the editor's automatic backups.

Every time the editor overwrites reviewed/<Vol>/page_NNN/page_NNN.json it first
copies the previous file to reviewed/<Vol>/page_NNN/.backups/page_NNN.<ts>.json
(newest 40 kept). This tool lists those and restores one — the safety net so a
bad save is never permanent.

  ./venv/bin/python restore_reviewed.py list Volume_1 3      # show backups (newest last)
  ./venv/bin/python restore_reviewed.py Volume_1 3           # restore the NEWEST backup
  ./venv/bin/python restore_reviewed.py Volume_1 3 --ts 20260614-141233
"""
from __future__ import annotations
import argparse, shutil, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REVIEWED = HERE / "reviewed"
SAFE = HERE / "reviewed_backups"     # out-of-tree backups (survive folder deletion)


def backups_dir(vol: str, page: int) -> Path:
    return REVIEWED / vol / f"page_{page:03d}" / ".backups"


def list_backups(vol: str, page: int):
    """Backups from BOTH the in-folder .backups/ and the out-of-tree
    reviewed_backups/ tree (the latter survives a deleted page folder), newest last."""
    stem = f"page_{page:03d}"
    bdir = backups_dir(vol, page)
    out = list(bdir.glob(f"{stem}.*.json")) if bdir.exists() else []
    sdir = SAFE / vol / f"page_{page:03d}"
    if sdir.exists():
        out += list(sdir.glob(f"{stem}.*.json"))
    # sort by the timestamp embedded in the name
    return sorted(out, key=lambda p: p.name.split(".")[-2])


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cmd_or_vol")
    ap.add_argument("vol_or_page")
    ap.add_argument("page", nargs="?")
    ap.add_argument("--ts", help="restore the backup with this timestamp")
    a = ap.parse_args()

    if a.cmd_or_vol == "list":
        vol, page = a.vol_or_page, int(a.page)
        bks = list_backups(vol, page)
        if not bks:
            print(f"No backups for {vol}/page_{page:03d}.")
            return
        print(f"Backups for {vol}/page_{page:03d} (oldest → newest):")
        for b in bks:
            ts = b.name.split(".")[-2]
            print(f"  {ts}   {b}")
        print("\nRestore newest:  ./venv/bin/python restore_reviewed.py "
              f"{vol} {page}")
        return

    vol, page = a.cmd_or_vol, int(a.vol_or_page)
    bks = list_backups(vol, page)
    if not bks:
        raise SystemExit(f"No backups for {vol}/page_{page:03d}.")
    if a.ts:
        match = [b for b in bks if f".{a.ts}." in b.name]
        if not match:
            raise SystemExit(f"No backup with ts={a.ts}. Try: list {vol} {page}")
        src = match[0]
    else:
        src = bks[-1]

    dest = REVIEWED / vol / f"page_{page:03d}" / f"page_{page:03d}.json"
    # back up the current (about-to-be-replaced) file too, so restore is reversible
    if dest.exists():
        bdir = backups_dir(vol, page); bdir.mkdir(exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        shutil.copy2(dest, bdir / f"page_{page:03d}.{ts}.json")
    shutil.copy2(src, dest)
    print(f"Restored {src.name} → {dest.relative_to(HERE)}")


if __name__ == "__main__":
    main()
