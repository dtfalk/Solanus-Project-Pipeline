#!/usr/bin/env python3
"""STEP_5 — merge the two Azure OCR passes per region into one best transcription.

NON-DESTRUCTIVE. Reads each page's extract_azure.json and NEVER modifies it or
extract_azure.raw.json — every byte of original per-pass output is kept. Writes a NEW
file extract_merged.json beside them, plus a corpus-level merge_report.json.

For every region BOTH passes are carried through verbatim, plus a chosen merged text and
its provenance, so the pick is fully auditable and re-runnable under a different policy:
  - agree    : the passes match (whitespace/case-normalized) -> that text,  source="agree"
  - disagree : pick by --prefer, needs_review=True, and keep full_page + per_polygon as-is

Nothing is ever thrown away here — this is a convenience "best text" view layered on top of
the untouched extract_azure.json / .raw.json archive.

Policies (--prefer):
  per_polygon (default) : trust the tight native-resolution crop pass (best small-glyph fidelity)
  full_page             : trust the whole-page pass
  longer                : keep whichever retained more characters (guards dropped runs, e.g. the
                          long ellipsis / dot-leader strings the full-page pass tends to swallow)

Usage:
  venv/bin/python merge_passes.py                                   # all OCR'd pages under 3_enriched
  venv/bin/python merge_passes.py --prefer longer
  venv/bin/python merge_passes.py Volume_2/1_source_pages/page_252  # specific page dirs
"""
from __future__ import annotations
import json, argparse
from pathlib import Path

HERE = Path(__file__).resolve().parent


def norm(s):
    return " ".join((s or "").split()).lower()


def choose(a: str, b: str, prefer: str):
    """Disagreeing region -> (merged_text, source). Pure selection; never edits the text."""
    if prefer == "full_page":
        return a, "full_page"
    if prefer == "longer":
        return (a, "full_page") if len(a or "") >= len(b or "") else (b, "per_polygon")
    if prefer == "shorter":                                  # TOC: drops dot-leader / stray-punct noise
        return (a, "full_page") if len(a or "") <= len(b or "") else (b, "per_polygon")
    return b, "per_polygon"                                  # default = per_polygon


def merge_page(js: Path, prefer: str, toc_prefer: str) -> dict:
    d = json.loads(js.read_text())
    eff = toc_prefer if "0_table_of_contents" in js.parts else prefer   # TOC gets its own policy
    regions, n_agree, n_review = [], 0, 0
    for r in d.get("regions", []):
        a, b = r.get("full_page", ""), r.get("per_polygon", "")
        agree = norm(a) == norm(b)
        if agree:
            merged, source = a, "agree"; n_agree += 1
        else:
            merged, source = choose(a, b, eff); n_review += 1
        regions.append({
            "rid": r.get("rid"), "doc": r.get("doc"), "category": r.get("category"),
            "merged_text": merged, "source": source, "needs_review": not agree,
            "full_page": a, "per_polygon": b,                # BOTH passes kept verbatim
            "min_conf": r.get("min_conf"),
        })
    out = {"page": d.get("page"), "engine": d.get("engine"), "merge_prefer": eff,
           "n_regions": len(regions), "n_agree": n_agree, "n_needs_review": n_review,
           "regions": regions}
    (js.parent / "extract_merged.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pages", nargs="*", help="page dirs relative to --root (default: every OCR'd page)")
    ap.add_argument("--root", default="3_enriched")
    ap.add_argument("--prefer", choices=["per_polygon", "full_page", "longer", "shorter"], default="per_polygon",
                    help="which pass wins when they disagree (default: per_polygon)")
    ap.add_argument("--toc-prefer", choices=["per_polygon", "full_page", "longer", "shorter"], default="shorter",
                    help="policy for table_of_contents pages (default: shorter — drops dot-leader noise)")
    a = ap.parse_args()
    root = (HERE / a.root).resolve()

    if a.pages:
        jsons = [root / p / "extract_azure.json" for p in a.pages]
    else:
        jsons = sorted(root.rglob("extract_azure.json"))

    npages = tot_r = tot_a = tot_rev = missing = 0
    for js in jsons:
        if not js.exists():
            missing += 1; continue
        o = merge_page(js, a.prefer, a.toc_prefer)
        npages += 1; tot_r += o["n_regions"]; tot_a += o["n_agree"]; tot_rev += o["n_needs_review"]

    report = {"prefer": a.prefer, "pages": npages, "regions": tot_r,
              "agree": tot_a, "needs_review": tot_rev, "missing_extracts": missing}
    (root / "merge_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"merged {npages} pages | {tot_r} regions | {tot_a} agree | "
          f"{tot_rev} need review ({100*tot_rev/max(tot_r,1):.1f}%) | prefer={a.prefer}")
    if missing:
        print(f"  ({missing} page dir(s) had no extract_azure.json — not OCR'd yet)")
    print(f"  wrote extract_merged.json per page (inputs untouched) + {root.name}/merge_report.json")


if __name__ == "__main__":
    main()
