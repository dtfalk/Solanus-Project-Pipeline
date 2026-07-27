#!/usr/bin/env python3
"""STEP_6 — segment NOTEBOOKS into per-page records -> notebooks.json (the separate track).

Scope:
  - all of Volume_3, Volume_4 (the notebook volumes), AND
  - notebook-typed spans embedded in the correspondence sections (e.g. "Notebook No. 9
    Spiritual Notes" in Volume_1), found via the TOC entry typing in segment_documents.py.

KEEP EVERYTHING: each record is one notebook PAGE and carries `text_by_label` = ALL categories'
text on that page (archv_commentary, struct_id, etc.) — symmetric with the letter records — plus
`entries` = the per-`src_content` diary units (each scoped by its DEGREE-1 connections: its
`src_date`, `struct_doc` page marker, and any linked `src_content`). So no labelled polygon is
ever dropped (a page like a clippings cover with only archv_commentary still appears, with text).

Output notebooks.json (NON-DESTRUCTIVE). Reads 3_enriched (copied via copy_input.py).
"""
from __future__ import annotations
import json, glob, argparse
from collections import defaultdict
from pathlib import Path
import segment_documents as S            # reuse gold_json, textmap, page_graph, toc_entries, content_index

HERE = Path(__file__).resolve().parent
NOTEBOOK_SECTIONS = ["Volume_3", "Volume_4"]


def struct_id_name(meta, T):
    """A page's struct_id text (often the notebook's own label), else ''."""
    for dk, doc in meta.get("documents", {}).items():
        for i in range(len((doc or {}).get("struct_id", []))):
            t = T.get(f"{dk}.struct_id.{i}", "").strip()
            if t:
                return t
    return ""


def all_text_by_label(meta, T):
    """{category: joined text} across ALL docs on the page — keeps every labelled polygon."""
    out = defaultdict(list)
    for dk, doc in meta.get("documents", {}).items():
        for cat, polys in (doc or {}).items():
            for i in range(len(polys or [])):
                t = T.get(f"{dk}.{cat}.{i}", "").strip()
                if t:
                    out[cat].append(t)
    return {c: " ".join(v) for c, v in out.items()}


def page_record(meta, T, section, notebook, cmap):
    """One notebook page -> a record with full text_by_label + per-src_content entries."""
    U, adj = S.page_graph(meta)
    cat = lambda v: U[v][1] if v in U else None
    txt = lambda v: T.get(U[v][0], "").strip() if v in U else ""
    entries = []
    for u, (rid, c, dk) in U.items():
        if c != "src_content":
            continue
        nbr = sorted(adj.get(u, set()))             # sort for determinism (set order varies by run)
        entries.append({
            "rid": rid, "text": txt(u),
            "date": "; ".join(txt(v) for v in nbr if cat(v) == "src_date" and txt(v)),
            "page_label": "; ".join(txt(v) for v in nbr if cat(v) == "struct_doc" and txt(v)),
            "linked": [U[v][0] for v in nbr if cat(v) == "src_content"],
        })
    regions = []
    for dk in meta.get("documents", {}):
        regions += S.region_rows(meta, dk, T, cmap, meta["page_number_in_type"])
    return {
        "id": f"{section}__p{meta['page_number_in_type']:03d}",
        "section": section, "notebook": notebook,
        "pdf_page_number": meta["pdf_page_number"], "page_number_in_type": meta["page_number_in_type"],
        "text_by_label": all_text_by_label(meta, T),         # KEEP EVERYTHING
        "entries": entries,
        "regions": regions,                                  # per-polygon geometry + min_conf + text
    }


def jobs(root):
    """Yield (section, pdir, meta, notebook_name) for every notebook page."""
    out = []
    for sec in NOTEBOOK_SECTIONS:                            # whole notebook volumes
        for pdir in sorted(glob.glob(str(root / sec / "1_source_pages" / "page_*"))):
            gj = S.gold_json(pdir)
            if gj:
                meta = json.load(open(gj))
                out.append((sec, pdir, meta, struct_id_name(meta, S.textmap(pdir))))
    for sec in S.CORRESPONDENCE:                             # notebook-typed spans inside correspondence
        secdir = root / sec
        idx = S.content_index(str(secdir))
        if not idx:
            continue
        maxpn = max(idx)
        entries = []
        for pdir in sorted(glob.glob(str(secdir / "0_table_of_contents" / "page_*"))):
            gj = S.gold_json(pdir)
            if gj:
                entries += S.toc_entries(json.load(open(gj)), S.textmap(pdir))
        seen = {}
        for e in entries:
            seen.setdefault(e["page_number"], e)
        entries = sorted(seen.values(), key=lambda e: e["page_number"])
        for i, e in enumerate(entries):
            if e["type"] != "notebook":
                continue
            start = e["page_number"]
            nxt = entries[i + 1]["page_number"] if i + 1 < len(entries) else maxpn + 1
            for pn in range(start, max(start + 1, nxt)):
                if pn in idx:
                    out.append((sec, idx[pn][0], idx[pn][2], e["parent_doc"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="3_enriched")
    ap.add_argument("--out", default="notebooks.json")
    a = ap.parse_args()
    root = (HERE / a.root).resolve()

    records, seen_pages, stats = [], set(), defaultdict(int)
    n_entries = 0
    for (sec, pdir, meta, notebook) in jobs(root):
        if pdir in seen_pages:
            continue
        seen_pages.add(pdir)
        rec = page_record(meta, S.textmap(pdir), sec, notebook, S.conf_map(pdir))
        records.append(rec)
        stats[f"{sec}_pages"] += 1
        n_entries += len(rec["entries"])

    (HERE / a.out).write_text(json.dumps(records, indent=2, ensure_ascii=False))
    print(f"wrote {len(records)} notebook PAGE records ({n_entries} src_content entries) -> {a.out}")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")
    for r in records[:1]:
        print(f"\n  e.g. {r['id']} notebook={r['notebook']!r}")
        print(f"    text_by_label categories: {list(r['text_by_label'])}")
        print(f"    {len(r['entries'])} entries; first: {r['entries'][0]['text'][:50]!r}" if r['entries'] else "    (no src_content entries — page still kept via text_by_label)")


if __name__ == "__main__":
    main()
