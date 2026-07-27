"""app/tools/book_search.py — search the external biography (Crosby, *Thank God Ahead of Time*).

A toggleable tool that lets the agent consult a published biography of Fr. Solanus (ingested by
``stages/ingest_book.py`` into its OWN vector partition ``book-tgat@1536``). It is OFF by default — the
archive's own letters/notebooks are the primary source — but turning it on lets answers draw on (and
cite, by page) the secondary literature. Kept in a separate space so it never silently masquerades as
a primary archival source; every hit is tagged ``source="book"`` with its page.
"""
from __future__ import annotations

from . import base                                # wires step_7 onto sys.path
from lib import vectorstore                       # the book partition lives here
from lib.providers import embed                   # to embed the query in the book's space

_SPACE = "book-tgat@1536"
_MODEL, _DIM = "gemini-embedding-001", 1536

TOOL_SPEC = {
    "name": "book_search",
    "description": (
        "Search a published BIOGRAPHY of Fr. Solanus Casey (Michael Crosby, 'Thank God Ahead of "
        "Time') for background, context, or facts not in the letters/notebooks. Use this for "
        "biographical or interpretive questions ('why was he transferred', 'what was his role as "
        "porter'), or to corroborate. Returns passages each with a BOOK PAGE to cite. This is "
        "SECONDARY literature — prefer the archive for primary facts."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string",
                      "description": "What to look up in the biography (a focused phrase)."},
            "top_k": {"type": "integer", "description": "How many passages to return (default 5)."},
        },
        "required": ["query"],
    },
}


def run(args: dict) -> dict:
    """Semantic search over the biography. Returns {query, available, hits:[{text, page, title, score}]}."""
    query = (args.get("query") or "").strip()
    top_k = max(1, int(args.get("top_k", 5)))
    if not query:
        return {"query": "", "available": True, "hits": []}
    if _SPACE not in set(vectorstore.spaces()):
        return {"query": query, "available": False, "hits": [],
                "note": "book index not built — run stages/ingest_book.py"}
    qvec, _ = embed.embed_texts([query], _MODEL, _DIM, task="query")
    hits = vectorstore.search(_SPACE, qvec[0], k=top_k)
    out = []
    for h in hits:
        m = h.get("meta", {})
        out.append({"text": (h.get("text") or m.get("text") or "")[:600],
                    "page": m.get("page"), "page_start": m.get("page_start"), "page_end": m.get("page_end"),
                    "title": m.get("title", "Thank God Ahead of Time"),
                    "source": "book", "score": round(h.get("score", 0.0), 4)})
    return {"query": query, "available": True, "hits": out}


# Register: OFF by default (secondary source; the archive is primary). One source of truth.
TOOL = base.tool_from_module(
    TOOL_SPEC, run,
    default_on=False,
    cost_note="free (local book index); the query embedding is a small paid+logged call",
)
