#!/usr/bin/env python3
"""STEP_6 — health check over documents.json + notebooks.json vs 3_enriched source pages.

KEEP-EVERYTHING gate (airtight, REGION level): every text-bearing labelled region in the source
must have its exact text present in some output record (a letter covering the page, or the
notebook page record). Not just per-category — per region. Exits 1 on FAIL.

Note: the OUTPUTS are text views. Geometry (vertices), OCR confidence, graph connections, and the
two raw OCR passes are NOT copied into documents.json/notebooks.json — they live, untouched, in
3_enriched (extract_azure.raw.json etc.). So nothing is lost anywhere; this gate proves the text.
"""
from __future__ import annotations
import json, glob, os, re, sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE / "3_enriched"
GOLD_RE = re.compile(r"page_\d+\.json")
CORR = ["Volume_1", "Volume_2", "Appendix_1", "Appendix_2", "Appendix_3"]
NB_SEC = ["Volume_3", "Volume_4"]
norm = lambda s: " ".join((s or "").split())


def src_index():
    out = {}
    for jp in sorted(glob.glob(str(ROOT / "*" / "1_source_pages" / "page_*" / "page_*.json"))):
        if not GOLD_RE.fullmatch(os.path.basename(jp)):
            continue
        m = json.loads(Path(jp).read_text())
        out[(m.get("source_file", Path(jp).parts[-4]), m["page_number_in_type"])] = os.path.dirname(jp)
    return out


def region_texts(pdir):
    """{rid: text} for regions with OCR text on this page (cleaned preferred)."""
    for n in ("extract_cleaned.json", "extract_merged.json"):
        p = os.path.join(pdir, n)
        if os.path.exists(p):
            return {r["rid"]: (r.get("cleaned_text") or r.get("merged_text") or "")
                    for r in json.load(open(p)).get("regions", [])
                    if (r.get("cleaned_text") or r.get("merged_text") or "").strip()}
    return {}


def main():
    docs = json.loads((HERE / "documents.json").read_text())
    nb = json.loads((HERE / "notebooks.json").read_text())
    idx = src_index()

    # output text blob per (section, page)
    blob = defaultdict(list)
    for d in docs:
        vals = list(d.get("text_by_label", {}).values())
        for pn in d.get("pages", []):
            blob[(d["section"], pn)] += vals
    for r in nb:
        blob[(r["section"], r["page_number_in_type"])] += list(r.get("text_by_label", {}).values())
    blobn = {k: norm(" ".join(v)) for k, v in blob.items()}

    print("=== KEEP-EVERYTHING (region-level: every region's text present in outputs) ===")
    total = blank = 0
    missing = []
    by_sec = defaultdict(lambda: [0, 0])              # sec -> [regions checked, regions missing]
    for (sec, pn), pdir in idx.items():
        regs = region_texts(pdir)
        if not regs:
            blank += 1
            continue
        b = blobn.get((sec, pn), "")
        for rid, t in regs.items():
            total += 1; by_sec[sec][0] += 1
            if norm(t) not in b:
                missing.append((sec, pn, rid, norm(t)[:50])); by_sec[sec][1] += 1
    for sec in CORR + NB_SEC:
        c, m = by_sec[sec]
        print(f"  {sec}: {c} regions checked, {m} missing")
    print(f"  checked {total} regions total; blank pages: {blank}")
    for m in missing[:15]:
        print("     MISSING:", m)

    print("\n=== LETTERS (", len(docs), ") ===")
    print(f"  no src_content: {sum(1 for d in docs if not d.get('text_by_label', {}).get('src_content', '').strip())}"
          f" | no date: {sum(1 for d in docs if not (d.get('date') or '').strip())}"
          f" | >3pp letters: {sum(1 for d in docs if len(d.get('pages', [])) > 3 and d['type'] == 'letter')}")
    print("  types:", dict(Counter(d["type"] for d in docs)))
    n_entries = sum(len(r.get("entries", [])) for r in nb)
    print(f"=== NOTEBOOKS ({len(nb)} page records, {n_entries} entries) ===")
    print(f"  pages w/o name: {sum(1 for r in nb if not (r.get('notebook') or '').strip())}"
          f" | entries w/o date: {sum(1 for r in nb for e in r.get('entries', []) if not (e.get('date') or '').strip())}")

    print("\n=== KEEP-EVERYTHING ASSERTION ===")
    if not missing:
        print(f"  PASS — all {total} text-bearing regions are represented in the outputs.")
        return 0
    print(f"  FAIL — {len(missing)} regions missing: " +
          ", ".join(f"{s}:{m}" for s, (c, m) in by_sec.items() if m))
    return 1


if __name__ == "__main__":
    sys.exit(main())
