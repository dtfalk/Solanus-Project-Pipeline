"""lib/chunks.py — build retrieval chunks from step_6 documents.json + notebooks.json (free).

One chunk per letter (all its text_by_label joined) and one per notebook entry. For notebooks we
prefer the STITCHED *logical entries* (data/stitched_notebooks.json) when present, so a cross-page
entry is ONE chunk spanning several pages — one source over multiple pages, not several fragmentary
"sources" (the "baked beans" problem). Each chunk keeps provenance metadata (doc_id, rid(s), page(s),
…) so retrieval can cite back to the source region/page. Pure/stdlib; no model calls.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402
from lib import textclean  # noqa: E402  (de-hyphenate OCR line breaks before chunking)

_PREF = ["src_recipient", "src_greeting", "src_content", "src_farewell", "src_signature",
         "src_location_recipient", "src_location_sender", "src_origin", "src_date", "archv_commentary"]


def _letter_text(d: dict) -> str:
    tbl = d.get("text_by_label", {})
    ordered = [tbl[k] for k in _PREF if tbl.get(k)] + [v for k, v in tbl.items() if k not in _PREF and v]
    return "\n".join(ordered).strip()


def build_chunks(limit: int | None = None, kinds: list | None = None) -> list:
    chunks = []
    for d in json.loads(config.DOCUMENTS.read_text()):
        t = textclean.clean(_letter_text(d))
        if t:
            chunks.append({"id": d["id"], "kind": "letter", "text": t,
                           "meta": {"doc_id": d["id"], "section": d["section"], "type": d.get("type"),
                                    "recipient": d.get("recipient", ""), "parent_doc": d.get("parent_doc", ""),
                                    "date": d.get("date", ""), "page": d.get("page_number_in_type"),
                                    "pdf_page": d.get("pdf_page_number")}})
    # notebook page metadata, for resolving a fragment's page id -> page/pdf-page numbers
    nb_pages = json.loads(config.NOTEBOOKS.read_text())
    page_meta = {p["id"]: {"section": p.get("section"), "notebook": p.get("notebook", ""),
                           "page": p.get("page_number_in_type"), "pdf_page": p.get("pdf_page_number")}
                 for p in nb_pages}

    stitched = config.DATA / "stitched_notebooks.json"
    if stitched.exists():
        # ONE chunk per LOGICAL entry (cross-page continuations already merged upstream). A multi-page
        # entry therefore becomes a single source whose provenance lists every page/rid it spans.
        sd = json.loads(stitched.read_text())
        for le in sd.get("logical_entries", []):
            t = textclean.clean((le.get("text") or "").strip())
            if not t:
                continue
            frags = le.get("member_fragments", [])                  # rich dicts (doc_id, rid, page, vertices…)
            f0 = frags[0] if frags else {}
            # compact per-page provenance the citation modal can iterate (every page this entry spans)
            spans = [{"doc_id": f.get("doc_id"), "rid": f.get("rid"), "page": f.get("page"),
                      "pdf_page": f.get("pdf_page"), "vertices": f.get("vertices"),
                      "min_conf": f.get("min_conf"), "page_label": f.get("page_label")} for f in frags]
            chunks.append({"id": le["logical_entry_id"], "kind": "notebook_entry", "text": t,
                           "meta": {"doc_id": f0.get("doc_id", ""), "section": le.get("section"),
                                    "notebook": le.get("notebook", ""),
                                    "rid": f0.get("rid", ""), "vertices": f0.get("vertices"),
                                    "min_conf": f0.get("min_conf"),
                                    "rids": [f.get("rid") for f in frags], "spans": spans,
                                    "date": le.get("date", ""), "archival_year": le.get("archival_year"),
                                    "page": f0.get("page"), "pdf_page": f0.get("pdf_page"),
                                    "pages": [f.get("page") for f in frags],
                                    "pdf_pages": [f.get("pdf_page") for f in frags],
                                    "is_multi_page": bool(le.get("is_multi_page")),
                                    "n_fragments": le.get("n_fragments", 1)}})
    else:
        # fallback (stitching not built yet): one chunk per raw page entry
        for page in nb_pages:
            for e in page.get("entries", []):
                t = textclean.clean((e.get("text") or "").strip())
                if t:
                    chunks.append({"id": f"{page['id']}::{e['rid']}", "kind": "notebook_entry", "text": t,
                                   "meta": {"doc_id": page["id"], "section": page["section"],
                                            "notebook": page.get("notebook", ""), "rid": e["rid"],
                                            "date": e.get("date", ""), "page": page.get("page_number_in_type"),
                                            "pdf_page": page.get("pdf_page_number")}})
    if kinds:
        chunks = [c for c in chunks if c["kind"] in kinds]
    if limit and limit < len(chunks):
        step = len(chunks) / limit                      # strided sample -> spans letters + entries
        chunks = [chunks[int(i * step)] for i in range(limit)]
    return chunks


def corpus_tokens() -> int:
    """Approx total tokens across all chunks (~chars/4) — for free cost projection."""
    return max(1, sum(len(c["text"]) for c in build_chunks()) // 4)


if __name__ == "__main__":
    cs = build_chunks()
    from collections import Counter
    print("chunks:", len(cs), dict(Counter(c["kind"] for c in cs)), "~tokens:", corpus_tokens())
