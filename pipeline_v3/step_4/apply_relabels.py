#!/usr/bin/env python3
"""Apply the high-confidence, verifier-confirmed CATEGORY relabels to
labeled_examples/ (from the label-agreement review, workflow w8dxot79q).

CATEGORY swaps only (the label-correctness fixes) — geometry is untouched, so
box position/size never changes. Boxes are matched by stable id prefix (unique).
Run:  ./venv/bin/python apply_relabels.py        # apply + print before/after
      ./venv/bin/python apply_relabels.py --dry  # show what would change only
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
EX = SCRIPT_DIR / "labeled_examples"

# (vol/page, id-prefix, from_category, to_category, drop_edges)
OPS = [
    ("Volume_2/page_002",  "f78db5b1", "archv_other",   "archv_possessor", False),
    ("Volume_2/page_002",  "999b4da9", "src_content",   "src_origin",      False),
    ("Volume_3/page_001",  "a4fed063", "struct_doc",    "src_date",        False),
    ("Volume_2/page_075",  "11171b3e", "src_greeting",  "src_content",     False),
    ("Appendix_3/page_006","5bbe9a86", "other",         "struct_other",    False),
    ("Appendix_3/page_019","c8bfead6", "other",         "src_content",     False),
    ("Volume_3/page_218",  "54580432", "other",         "src_content",     True),
    ("Appendix_1/page_031","c6836003", "src_recipient", "src_greeting",    False),
]


def _find(documents, idprefix, cat):
    hits = []
    for did, doc in documents.items():
        for c, polys in doc.items():
            if c != cat:
                continue
            for i, b in enumerate(polys):
                if b.get("id", "").startswith(idprefix):
                    hits.append((did, c, i, b))
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    by_page = {}
    for vp, idp, frm, to, drop in OPS:
        by_page.setdefault(vp, []).append((idp, frm, to, drop))

    for vp, ops in by_page.items():
        jp = EX / vp / f"{vp.split('/')[1]}.json"
        data = json.load(open(jp))
        docs = data["documents"]
        for idp, frm, to, drop in ops:
            hits = _find(docs, idp, frm)
            if len(hits) != 1:
                print(f"  !! {vp} {idp} {frm}->{to}: matched {len(hits)} boxes — SKIPPED")
                continue
            did, c, i, b = hits[0]
            full_id = b["id"]
            if not args.dry:
                docs[did][frm].pop(i)
                if drop:
                    b["connections"] = []
                    for _dd, d in docs.items():
                        for _cc, polys in d.items():
                            for bb in polys:
                                bb["connections"] = [cn for cn in (bb.get("connections") or [])
                                                     if cn.get("id") != full_id]
                docs[did].setdefault(to, []).append(b)
            edge_note = " + dropped edges" if drop else ""
            print(f"  {vp}: {did} {frm} -> {to}  (id {idp}){edge_note}")
        if not args.dry:
            json.dump(data, open(jp, "w"), indent=2)
    print("\nDONE" + (" (dry run — nothing written)" if args.dry else ""))


if __name__ == "__main__":
    main()
