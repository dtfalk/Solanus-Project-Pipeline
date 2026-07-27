"""app/tools/pdf_fulltext_search.py — full-text (BM25) search over the page PDFs.

Where ``vector_search`` works on *chunks* (one letter / one notebook entry) and can reason about
meaning, this tool searches the **cleaned page OCR text** in the SQLite **FTS5** index that
``stages/index_sources.py`` built (``data/fts.sqlite``). Two reasons the agent wants both:

  * **Exact-string power.** FTS5 does true phrase/prefix matching with Porter stemming, so a literal
    lookup — a name spelled a particular way, an address, ``"inflammatory rheumatism"`` — lands on
    the precise page even when a semantic embedding would smear it. It's the lexical complement to
    the dense tool.
  * **Page-level + PDF-level provenance.** Each hit comes back with the **page PDF path** you'd open
    to read it (the payload the index stores explicitly) and a highlighted snippet, so a citation can
    point a human straight at the scanned page — exactly the "open the source PDF" affordance the
    citation modal offers.

This is the same function ``/api/search_pdf`` exposes directly; wiring it as a *tool* too lets the
agent decide on its own to do a literal page search mid-reasoning. It is 100% local and free
(SQLite stdlib), and still cost-logged for ledger completeness via the underlying stage helper.
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (wires step_7 onto sys.path), then the stage helper
# ==================================================================
from pathlib import Path

from . import base                            # importing base sets up the step_7 import path
import config                                 # step_7 config (paths) — resolves thanks to base
from stages import index_sources             # reuse the FTS builder/searcher we already wrote


# ==================================================================
# TOOL_SPEC — name / description / JSON-Schema params the LLM sees
# ==================================================================
TOOL_SPEC = {
    "name": "pdf_fulltext_search",
    "description": (
        "Literal full-text search over the scanned page OCR text (SQLite FTS5). Best for EXACT "
        "strings — a specific name spelling, an address, a quoted phrase, a date as written — where "
        "you want the precise page and its source PDF. Complements vector_search (which is better for "
        "meaning/paraphrase). Returns page hits with a highlighted snippet and the PDF to open."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "FTS5 query. Plain words are ANDed; wrap an exact phrase in double "
                               "quotes; append * for prefix match.",
            },
            "k": {
                "type": "integer",
                "description": "Max page hits to return (default 8).",
            },
        },
        "required": ["query"],
    },
}


# ==================================================================
# run — the single args-dict entry point (the tool's source of truth)
# ==================================================================
def run(args: dict) -> dict:
    """Full-text BM25 search over page OCR text; returns pages + their source PDF + a snippet.

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`:
            ``query`` (required FTS5 string — plain words are ANDed; quote a phrase for an exact
            match like ``"Seraphic Mass Association"``; ``term*`` does prefix matching) and ``k``
            (max page hits, default 8).

    Returns:
        ``{"query": str, "available": bool, "hits": [...]}`` where each hit is
        ``{doc_id, section, kind, pdf_page, page_label, pdf_path, snippet, provenance}``. The
        ``snippet`` wraps matched terms in « » for display; ``provenance`` mirrors the shape the
        citation layer expects (here it's page-level — ``rid`` is None because FTS indexes whole
        pages, not individual regions). ``available`` is False (with empty hits) if the index hasn't
        been built yet, so the agent can fall back to ``vector_search`` instead of erroring.
    """
    query = (args.get("query") or "").strip()
    k = args.get("k", 8)
    if not query:
        return {"query": "", "available": True, "hits": [], "note": "empty query"}
    db_path: Path = config.DATA / "fts.sqlite"
    # The index is an optional upstream artifact (index_sources stage). If it isn't there, say so
    # honestly rather than throwing — the agent loop treats a tool error as fatal, but an empty,
    # well-formed result just nudges it toward another tool.
    if not db_path.exists():
        return {"query": query, "available": False, "hits": [],
                "note": "data/fts.sqlite not built yet — run stages/index_sources.py "
                        "(free, local) to enable full-text PDF search."}

    rows = index_sources.fts_search(query, k=max(1, int(k)), db_path=db_path)

    # Reshape each FTS row into the project's hit/provenance contract so the citation layer and the
    # source modal treat a PDF hit the same way they treat a vector hit (page-level, no region box).
    hits = []
    for r in rows:
        hits.append({
            "doc_id":     r.get("doc_id"),
            "section":    r.get("section"),
            "kind":       r.get("kind"),
            "pdf_page":   r.get("pdf_page"),
            "page_label": r.get("page_label"),
            "pdf_path":   r.get("pdf_path"),       # <- the page PDF to open (the index's payload)
            "snippet":    r.get("snippet"),
            "provenance": {
                "doc_id":   r.get("doc_id"),
                "rid":      None,                  # FTS is page-level; no single region id
                "page":     r.get("pdf_page"),
                "pdf_page": r.get("pdf_page"),
                "section":  r.get("section"),
                "pdf_path": r.get("pdf_path"),
                "vertices": None,
                "min_conf": None,
            },
        })
    return {"query": query, "available": True, "hits": hits}


# ==================================================================
# Register the tool
# ==================================================================
TOOL = base.tool_from_module(TOOL_SPEC, run, default_on=True, cost_note="free, local (SQLite FTS5)")
