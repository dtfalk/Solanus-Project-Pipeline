#!/usr/bin/env python3
"""Recover which source pages step_1 dropped to make cleaned_pdfs_docs_only.

cleaned_pdfs/<Vol>.pdf      = full deskewed volume (S pages, == source order)
cleaned_pdfs_docs_only/<Vol>.pdf = the SAME images with front/back matter pages
                              removed (D pages); this is what step_2 cropped 1:1
                              into polygon_cropped_pdfs/<Vol>/pages.

docs_only is a strict, order-preserving SUBSET of cleaned, so a greedy monotonic
content alignment (page fingerprints, corr) recovers, for every kept page, its
true SOURCE page number — and the unmatched cleaned pages are exactly the dropped
front/back matter. Deterministic; corr≈1.0 on matches makes it unambiguous
(unlike type-sequence alignment, which failed on V2).

Writes qa_output/<Vol>/frontmatter_map.json:
  { source_pages, kept_pages, dropped_source_pages,
    cropidx_to_source: {"1": 3, "2": 4, ...} }   # crop/docs_only page -> source page
Prints a human summary. Changes NOTHING else.

Usage: ./venv/bin/python experiments/recover_frontmatter_map.py Volume_1 [Volume_2 ...]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
import numpy as np
from pdf2image import convert_from_path

ROOT = Path(__file__).resolve().parent.parent.parent.parent      # repo root
HERE = Path(__file__).resolve().parent.parent
CLEAN = ROOT / "step_1" / "cleaned_pdfs"
DOCS  = ROOT / "step_1" / "cleaned_pdfs_docs_only"
MATCH_THRESH = 0.92


def fingerprints(pdf: Path, dpi: int = 50) -> list[np.ndarray]:
    out = []
    for im in convert_from_path(pdf, dpi=dpi):
        a = np.asarray(im.convert("L").resize((64, 84)), dtype=np.float32).flatten()
        a -= a.mean()
        n = np.linalg.norm(a)
        out.append(a / n if n > 0 else a)
    return out


def align(full: list[np.ndarray], sub: list[np.ndarray]):
    """Greedy monotonic: walk full; consume a sub page when it matches. Returns
    (cropidx_to_source dict 1-based, dropped_source list 1-based)."""
    i = j = 0
    mapping, dropped = {}, []
    while i < len(full) and j < len(sub):
        if float(full[i] @ sub[j]) >= MATCH_THRESH:
            mapping[j + 1] = i + 1            # docs_only/crop page (1-based) -> source page
            i += 1; j += 1
        else:
            dropped.append(i + 1)
            i += 1
    while i < len(full):                      # tail of full = trailing dropped pages
        dropped.append(i + 1); i += 1
    return mapping, dropped, j


def main():
    vols = sys.argv[1:] or ["Volume_1", "Volume_2", "Volume_3"]
    for vol in vols:
        full = fingerprints(CLEAN / f"{vol}.pdf")
        sub  = fingerprints(DOCS / f"{vol}.pdf")
        mapping, dropped, consumed = align(full, sub)
        ok = consumed == len(sub) and len(dropped) == len(full) - len(sub)
        # contiguous-run summary of dropped source pages
        runs, run = [], []
        for p in dropped:
            if run and p == run[-1] + 1:
                run.append(p)
            else:
                run and runs.append(run); run = [p]
        run and runs.append(run)
        runs_str = ", ".join(f"{r[0]}" if len(r) == 1 else f"{r[0]}-{r[-1]}" for r in runs)
        out = {
            "volume": vol, "source_pages": len(full), "kept_pages": len(sub),
            "dropped_source_pages": dropped, "dropped_runs": runs_str,
            "cropidx_to_source": {str(k): v for k, v in mapping.items()},
            "alignment_ok": ok, "match_thresh": MATCH_THRESH,
        }
        p = HERE / "qa_output" / vol / "frontmatter_map.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(out, indent=2) + "\n")
        # shift profile: source - cropidx (how much each kept page moves)
        shifts = sorted({v - int(k) for k, v in out["cropidx_to_source"].items()})
        print(f"\n{vol}: source {len(full)} → docs_only/crops {len(sub)}  "
              f"({'OK' if ok else 'MISALIGNED!'})")
        print(f"  dropped source pages ({len(dropped)}): {runs_str}")
        print(f"  per-page shift (source# − crop#) values seen: {shifts}")
        print(f"  crop page 1 → source {mapping.get(1)};  crop {len(sub)} → source {mapping.get(len(sub))}")
        print(f"  wrote {p.relative_to(HERE)}")


if __name__ == "__main__":
    main()
