#!/usr/bin/env python3
"""Geometric triangle-connector for CONTENTS / INDEX pages (the V2 page_006 schema).

On a contents page each ROW is: archv_commentary description | archv_date | struct_doc
page-number, and a description HEADING governs every (date, page-number) row beneath it
until the next heading (same fan-out as notebook page markers). This wires the connections
David draws by hand — per row a triangle commentary<->date, commentary<->struct_doc,
date<->struct_doc — deterministically from box positions. No model, no text.

Used by relabel_contents.py after pass-1 labeling; also importable. Operates on one
page's `documents` dict in place (adds bidirectional `connections`); returns edge count.
"""
from __future__ import annotations


def _bbox(p):
    xs = [v["x"] for v in p["vertices"]]; ys = [v["y"] for v in p["vertices"]]
    return min(xs), min(ys), max(xs), max(ys)


def _yc(p):
    _, y0, _, y1 = _bbox(p); return (y0 + y1) / 2


def _link(a, b, catA, catB):
    a.setdefault("connections", []); b.setdefault("connections", [])
    if not any(c.get("id") == b["id"] for c in a["connections"]):
        a["connections"].append({"doc": "doc_1", "type": catB, "id": b["id"]})
    if not any(c.get("id") == a["id"] for c in b["connections"]):
        b["connections"].append({"doc": "doc_1", "type": catA, "id": a["id"]})


def connect_contents(documents: dict) -> int:
    """Wire the contents triangle for every doc on the page. Returns edges added."""
    edges = 0
    for doc in documents.values():
        if not isinstance(doc, dict):
            continue
        C = doc.get("archv_commentary", [])
        D = doc.get("archv_date", [])
        S = doc.get("struct_doc", [])
        if not (C and S):
            continue
        Cs = sorted(C, key=lambda p: _yc(p))               # headings, top→down
        def governing(row_yc):
            # a heading ON this row (1:1 contents) governs it; else the nearest
            # heading ABOVE governs (fan-out: a dateless continuation row).
            on_row = [c for c in Cs if abs(_yc(c) - row_yc) < 22]
            if on_row:
                return min(on_row, key=lambda c: abs(_yc(c) - row_yc))
            above = [c for c in Cs if _yc(c) <= row_yc]
            return above[-1] if above else Cs[0]
        # pair each page-number S with the nearest date D to its LEFT on the same row
        for s in S:
            s_yc = _yc(s); s_x0 = _bbox(s)[0]
            same_row_d = sorted(
                [d for d in D if abs(_yc(d) - s_yc) < 22 and _bbox(d)[0] < s_x0],
                key=lambda d: abs(_yc(d) - s_yc))
            d = same_row_d[0] if same_row_d else None
            gov = governing(s_yc)
            before = len(gov.get("connections", [])) + (len(d.get("connections", [])) if d else 0) \
                + len(s.get("connections", []))
            _link(gov, s, "archv_commentary", "struct_doc")
            if d is not None:
                _link(gov, d, "archv_commentary", "archv_date")
                _link(d, s, "archv_date", "struct_doc")
            after = len(gov.get("connections", [])) + (len(d.get("connections", [])) if d else 0) \
                + len(s.get("connections", []))
            edges += (after - before)
    return edges // 2


if __name__ == "__main__":
    import json, sys
    from collections import Counter
    # validation: reproduce a gold contents page's connections from its categories
    path = sys.argv[1] if len(sys.argv) > 1 else "reviewed/Volume_2/page_006/page_006.json"
    gold = json.load(open(path))
    def edgeset(docs):
        catof = {p["id"]: c for d in docs.values() if isinstance(d, dict)
                 for c, ps in d.items() if isinstance(ps, list) for p in ps}
        es = set()
        for d in docs.values():
            if isinstance(d, dict):
                for c, ps in d.items():
                    if isinstance(ps, list):
                        for p in ps:
                            for cn in p.get("connections", []):
                                es.add(frozenset((p["id"], cn["id"])))
        return es, catof
    gold_es, catof = edgeset(gold["documents"])
    stripped = json.loads(json.dumps(gold["documents"]))
    for d in stripped.values():
        for c, ps in (d.items() if isinstance(d, dict) else []):
            if isinstance(ps, list):
                for p in ps: p["connections"] = []
    connect_contents(stripped)
    new_es, _ = edgeset(stripped)
    inter = gold_es & new_es
    pair = lambda fs: tuple(sorted(catof.get(i, "?") for i in fs))
    print(f"gold edges {len(gold_es)}, derived {len(new_es)}, match {len(inter)} "
          f"({len(inter)/max(1,len(gold_es)):.0%})")
    print("  missed:", Counter(pair(e) for e in gold_es - new_es))
    print("  extra :", Counter(pair(e) for e in new_es - gold_es))
