"""safeio.py — non-destructive writes for pipeline stages.

Two primitives every stage that overwrites a curated or derived artifact should use, so a re-run (or a
crash mid-write) can never destroy the only copy:

  backup(path, tag)        copy path -> <dir>/.backups/<stem>.<tag>.<YYYYMMDD_HHMMSS><suffix>  (timestamped,
                           so a second run can't clobber the previous recovery point — unlike a single-slot
                           `.pre*` sibling). No-op if the file is absent or empty.
  atomic_write_text(p, s)  write to a temp file in the same dir then os.replace() — a partial/crashed write
                           can never truncate the live file (which would then look "present but corrupt").

Backups land next to the file, in a sibling `.backups/` dir (data/graph.json -> data/.backups/), matching
the convention name_authority.py / fix_display_names.py already use.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path


def backup(path, tag: str = "") -> Path | None:
    """Timestamped copy of `path` into a sibling `.backups/` dir before it's overwritten. Returns the
    backup path, or None if there was nothing to back up."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return None
    bdir = path.parent / ".backups"
    bdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    t = f"{tag}." if tag else ""
    dest = bdir / f"{path.stem}.{t}{ts}{path.suffix}"
    shutil.copy2(path, dest)
    return dest


def atomic_write_text(path, text: str) -> None:
    """Write `text` to `path` atomically (temp file in the same dir + os.replace), so a crash mid-write
    leaves the previous file intact rather than a truncated one."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
