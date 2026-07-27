#!/usr/bin/env python3
"""STEP_6 — segment the CORRESPONDENCE sections into individual documents -> documents.json.

Sections with real TOCs: Volume_1, Volume_2, Appendix_1, Appendix_2, Appendix_3.
(Volume_3/4 are notebooks — a SEPARATE track, not processed here.)

ATOMIC ANCHOR = each `struct_doc` (a printed page number) in a table_of_contents page's
connection graph. One struct_doc == one document. Its degree-1 neighbours give:
  - the date (an `archv_date`),
  - the commentary(ies): a series HEADER (an `archv_commentary` linked to MANY pages) -> parent_doc,
    and/or a LEAF recipient commentary (linked to exactly one page) -> recipient.
This captures header-only series (e.g. "Letters to Muriel Krausman": header + 20 dates + 20 pages,
no per-letter commentary) that an commentary-anchored pass would miss.

Each entry is TYPED from its title (letter / notebook / card / autograph / poem / copy / other) so
notebooks embedded in the volumes can be routed to the notebook track.

Span (multi-page): the printed page maps to a source page (page_number_in_type); a document runs
from its start page up to the next captured entry's start page. Text is assembled BY LABEL from the
start page's opening doc_N plus continuation pages.

Output: a single documents.json (NON-DESTRUCTIVE). Reads 3_enriched (copied via copy_input.py).
"""
from __future__ import annotations
import json, os, re, glob, argparse
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
CORRESPONDENCE = ["Volume_1", "Volume_2", "Appendix_1", "Appendix_2", "Appendix_3"]
GOLD_RE = re.compile(r"page_\d+\.json")
INT_RE = re.compile(r"\d+")
START_CATS = ("src_recipient", "src_greeting")


def classify(title):
    t = (title or "").strip().lower()
    if t.startswith("letter") or t.startswith("to "):           return "letter"
    if "notebook" in t or t.startswith("note") or "spiritual notes" in t or "diary" in t: return "notebook"
    if "autograph" in t:                                        return "autograph"
    if "poem" in t or "verse" in t:                            return "poem"
    if t.startswith("card") or "postcard" in t:                return "card"
    if "printed" in t or "clipping" in t:                      return "printed"
    if t.startswith("cop") or "transcript" in t:               return "copy"
    return "other"


def gold_json(pdir):
    for f in os.listdir(pdir):
        if GOLD_RE.fullmatch(f):
            return os.path.join(pdir, f)
    return None


def textmap(pdir):
    for name in ("extract_cleaned.json", "extract_merged.json"):
        p = os.path.join(pdir, name)
        if os.path.exists(p):
            return {r["rid"]: r.get("cleaned_text", r.get("merged_text", ""))
                    for r in json.load(open(p)).get("regions", [])}
    return {}


def page_graph(meta):
    U, adj = {}, defaultdict(set)
    for dk, doc in meta.get("documents", {}).items():
        for cat, polys in (doc or {}).items():
            for i, p in enumerate(polys or []):
                U[p["id"]] = (f"{dk}.{cat}.{i}", cat, dk)
                for c in p.get("connections", []):
                    adj[p["id"]].add(c["id"]); adj[c["id"]].add(p["id"])
    return U, adj


def pagenum(s):
    m = INT_RE.search(s or "")
    return int(m.group()) if m else None


def toc_entries(meta, T):
    """One TOC page -> [{recipient, date, page_number, parent_doc, type}] anchored on struct_doc."""
    U, adj = page_graph(meta)
    cat = lambda v: U[v][1] if v in U else None
    txt = lambda v: T.get(U[v][0], "").strip() if v in U else ""
    nstruct = lambda v: sum(1 for w in adj.get(v, set()) if cat(w) == "struct_doc")
    out = []
    for u, (rid, c, dk) in U.items():
        if c != "struct_doc":
            continue
        pn = pagenum(txt(u))
        if pn is None:
            continue
        nbr = sorted(adj.get(u, set()))             # sort for determinism (set order varies by run)
        dates = [txt(v) for v in nbr if cat(v) == "archv_date" and txt(v)]
        comms = [v for v in nbr if cat(v) == "archv_commentary"]
        header = next((v for v in comms if nstruct(v) > 1), None)     # series header (many pages)
        leaf = next((v for v in comms if nstruct(v) == 1), None)      # per-letter recipient
        parent = txt(header) if header is not None else (txt(leaf) if leaf is not None else "")
        recipient = txt(leaf) if leaf is not None else parent
        title = parent or recipient
        out.append({"recipient": recipient, "date": "; ".join(dates), "page_number": pn,
                    "parent_doc": parent or recipient, "type": classify(title)})
    return out


def content_index(secdir):
    idx = {}
    for pdir in sorted(glob.glob(os.path.join(secdir, "1_source_pages", "page_*"))):
        gj = gold_json(pdir)
        if gj:
            m = json.load(open(gj))
            idx[m["page_number_in_type"]] = (pdir, m["pdf_page_number"], m)
    return idx


def doc_text_by_label(meta, dk, T):
    out = {}
    for c, polys in (meta["documents"].get(dk) or {}).items():
        parts = [T.get(f"{dk}.{c}.{i}", "").strip() for i in range(len(polys or []))]
        parts = [p for p in parts if p]
        if parts:
            out[c] = " ".join(parts)
    return out


_CONF_CACHE = {}
def conf_map(pdir):
    """rid -> min_conf (OCR confidence) from extract_merged.json; memoized per page."""
    if pdir not in _CONF_CACHE:
        p = os.path.join(pdir, "extract_merged.json")
        _CONF_CACHE[pdir] = ({r["rid"]: r.get("min_conf") for r in json.load(open(p)).get("regions", [])}
                             if os.path.exists(p) else {})
    return _CONF_CACHE[pdir]


def region_rows(meta, dk, T, cmap, page):
    """Per-polygon geometry (vertices, render-dpi px) + OCR min_conf + text for one doc on a page.
    Optional detail tacked onto each record's `regions` field — use it or ignore it."""
    rows = []
    for c, polys in (meta["documents"].get(dk) or {}).items():
        for i, p in enumerate(polys or []):
            rid = f"{dk}.{c}.{i}"
            rows.append({"rid": rid, "category": c, "page_number_in_type": page,
                         "min_conf": cmap.get(rid),
                         "vertices": [[round(v["x"]), round(v["y"])] for v in p.get("vertices", [])],
                         "text": T.get(rid, "").strip()})
    return rows


def has_start(meta, dk, T):
    """True if doc_N OPENS a document (recipient/greeting text present)."""
    doc = meta["documents"].get(dk) or {}
    return any(any(T.get(f"{dk}.{c}.{i}", "").strip() for i in range(len(doc.get(c, []))))
               for c in START_CATS)


def split_span(span, idx):
    """Walk a span's pages and doc_N in order; start a new sub-document at each opening doc
    (recipient/greeting). Things with no such signals (notebooks, poems) stay as ONE sub-doc."""
    out, cur = [], None
    for pn in span:
        pdir, pdf, meta = idx[pn]
        T = textmap(pdir)
        for dk in meta["documents"]:
            if has_start(meta, dk, T) or cur is None:
                cur = []; out.append(cur)
            cur.append((pn, dk, meta, T))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="3_enriched")
    ap.add_argument("--out", default="documents.json")
    a = ap.parse_args()
    root = (HERE / a.root).resolve()

    records, stats, types = [], defaultdict(int), defaultdict(int)
    for sec in CORRESPONDENCE:
        secdir = root / sec
        if not secdir.is_dir():
            continue
        idx = content_index(str(secdir))
        maxpn = max(idx) if idx else 0
        entries = []
        for pdir in sorted(glob.glob(str(secdir / "0_table_of_contents" / "page_*"))):
            gj = gold_json(pdir)
            if gj:
                entries += toc_entries(json.load(open(gj)), textmap(pdir))
        # dedup by page_number (OCR can list a page twice); keep the first
        seenpn = {}
        for e in entries:
            seenpn.setdefault(e["page_number"], e)
        entries = sorted(seenpn.values(), key=lambda e: e["page_number"])
        stats[f"{sec}_entries"] = len(entries)

        for i, e in enumerate(entries):
            start = e["page_number"]
            nxt = entries[i + 1]["page_number"] if i + 1 < len(entries) else maxpn + 1
            span = [pn for pn in range(start, max(start + 1, nxt)) if pn in idx]
            types[e["type"]] += 1
            if e["type"] == "notebook":
                stats["notebook_entries_deferred"] += 1
                continue                                     # handled by segment_notebooks.py
            if start not in idx:
                stats["missing_start_page"] += 1
                continue
            subs = [s for s in split_span(span, idx) if s]   # per-letter split via content signals
            if len(subs) > 1:
                stats["entries_split_per_letter"] += 1
            for sl in subs:
                spages = sorted({pn for (pn, dk, meta, T) in sl})
                sp = sl[0][0]
                tbl = defaultdict(list); rrows = []
                for (pn, dk, meta, T) in sl:
                    for c, t in doc_text_by_label(meta, dk, T).items():
                        tbl[c].append(t)
                    rrows += region_rows(meta, dk, T, conf_map(idx[pn][0]), pn)
                tb = {c: " ".join(v) for c, v in tbl.items()}
                if len(spages) > 1:
                    stats["multi_page_docs"] += 1
                records.append({
                    "id": f"{sec}__p{sp:03d}", "section": sec, "type": e["type"],
                    "parent_doc": e["parent_doc"],
                    "recipient": tb.get("src_recipient", e["recipient"]),
                    "date": tb.get("src_date", e["date"]),
                    "page_number_in_type": sp, "pdf_page_number": idx[sp][1],
                    "pages": spages, "fragments": [[pn, dk] for (pn, dk, meta, T) in sl],
                    "text_by_label": tb, "regions": rrows,
                })

    seen = defaultdict(int)
    for r in records:
        seen[r["id"]] += 1
        if seen[r["id"]] > 1:
            r["id"] = f"{r['id']}__{seen[r['id']]}"

    (HERE / a.out).write_text(json.dumps(records, indent=2, ensure_ascii=False))
    print(f"wrote {len(records)} documents -> {a.out}")
    print("  by type:", dict(types))
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")
    long = sorted(records, key=lambda r: -len(r["pages"]))[:6]
    print("longest spans (sanity):")
    for r in long:
        print(f"  {len(r['pages']):3}pp [{r['type']}] {r['section']} p{r['page_number_in_type']}: {r['parent_doc'][:42]!r}")


if __name__ == "__main__":
    main()
