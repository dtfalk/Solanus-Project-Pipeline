"""app/tools/vector_search.py — the agent's semantic/hybrid retrieval tool.

This is the workhorse the agent reaches for to *find passages*. It is a thin shell over
``lib.retrieval`` — the retrieval brain built earlier — so all the real cleverness (BM25 + dense
fused with RRF, optional rerank, the free Self-RAG/CRAG grade) lives there, and this file only:

  1. accepts the three model "variables" the request carries (embedding space + reranker), plus the
     query and a few cheap knobs, and maps them onto a :class:`lib.retrieval.RetrievalConfig`;
  2. runs the FREE-by-default hybrid path (it degrades to BM25-only if no dense partition exists for
     the chosen space, exactly like the retrieval CLI smoke test) and
  3. shapes the result into the tool contract: a ``"hits"`` list whose items each carry the
     ``provenance`` (doc_id / rid / page / vertices / min_conf) the citation layer needs, plus the
     retrieval ``grade`` so the generator knows whether to answer, caveat, or abstain.

Cost discipline: with the default arguments this makes ZERO paid calls (lexical + local dense). It
only spends if the request selected a hosted embedding space or turned the reranker on — and every
such call is cost-logged *inside* lib.retrieval / lib.providers, never here.
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (it wires step_7 onto sys.path), then lib
# ==================================================================
from . import base                            # importing base sets up the step_7 import path
from lib import retrieval                     # the retrieval brain (BM25+dense+RRF+rerank+grade)
from lib import vectorstore                   # to check which embedding partitions actually exist


# ==================================================================
# TOOL_SPEC — the name / description / JSON-Schema params the LLM sees
# ==================================================================
# The JSON-Schema is intentionally small: the agent picks the query (and rarely a kind/top_k). The
# model "variables" (embedding space, reranker) are NOT in the schema — they're set per-REQUEST by the
# human in the UI, not chosen turn-by-turn by the LLM; the server passes them through `args`. Keeping
# the model's decision surface to "what should I search for?" makes its tool-use far more reliable.
TOOL_SPEC = {
    "name": "vector_search",
    "description": (
        "Search the Solanus Casey archive (letters + notebook entries) for passages relevant to a "
        "query, using hybrid lexical+semantic retrieval. Use this for almost any factual lookup over "
        "the corpus. Returns ranked passages each with exact source provenance (document, region, "
        "page) you can cite. Pass a focused query naming what you're looking for."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Focused search query naming the thing to find (e.g. 'cancer cure "
                               "reported 1933', 'who did Solanus write to in Detroit').",
            },
            "kinds": {
                "type": "array",
                "items": {"type": "string", "enum": ["letter", "notebook_entry"]},
                "description": "Optional restriction to letters or notebook entries; omit to search both.",
            },
            "top_k": {
                "type": "integer",
                "description": "How many passages to return (default 6).",
            },
            "use_rerank": {
                "type": "boolean",
                "description": "Set true to apply a cross-encoder reranker for a sharper final order.",
            },
        },
        "required": ["query"],
    },
}


# ==================================================================
# run — the single args-dict entry point (the tool's source of truth)
# ==================================================================
def run(args: dict) -> dict:
    """Hybrid (lexical + semantic) retrieval over the corpus, returning cited passages.

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`. Recognized keys:
            ``query`` (required), ``kinds``, ``top_k``, ``use_rerank``, and — passed by the SERVER,
            not the LLM — the per-request model "variables" ``embedding_model`` / ``embedding_dim`` /
            ``reranker``. The agent normally supplies only ``query``.

    Returns:
        A JSON-able dict::

            {
              "query": str,
              "space": "model@dim",                 # which dense partition was (or would be) used
              "dense_available": bool,              # False -> ran BM25-only (still useful, free)
              "grade": {...},                       # Self-RAG/CRAG verdict: answer|caveat|abstain
              "hits": [ {id, score, kind, text, provenance:{doc_id,rid,page,vertices,min_conf}}, ...]
            }

        Each hit's ``provenance`` is exactly what the citation modal / ``/api/source`` need to deep-
        zoom to the handwritten region.
    """
    # ---- read args (the args-dict contract) ----------------------------------
    query           = (args.get("query") or "").strip()
    kinds           = args.get("kinds") or None
    top_k           = args.get("top_k", 6)
    use_rerank      = bool(args.get("use_rerank", False))
    embedding_model = args.get("embedding_model")          # server-supplied per-request "variable"
    embedding_dim   = args.get("embedding_dim")            # server-supplied per-request "variable"
    reranker        = args.get("reranker")                 # server-supplied per-request "variable"
    if not query:
        return {"query": "", "available": True, "hits": [],
                "grade": {"verdict": "abstain", "reason": "empty query"}}

    # ---- map the request "variables" onto a RetrievalConfig -------------------
    # Start from the library defaults (the free hybrid path), then override only what the caller set.
    cfg = retrieval.RetrievalConfig()
    if embedding_model:
        cfg.embedding_model = embedding_model
    if embedding_dim:
        cfg.embedding_dim = int(embedding_dim)
    cfg.top_k = max(1, int(top_k))
    cfg.use_rerank = use_rerank
    if reranker:
        cfg.reranker = reranker

    # ---- only enable dense if a partition for this space was actually built ---
    # lib.retrieval already degrades gracefully if dense can't run, but we surface the fact explicitly
    # so the agent (and the trace) can SEE whether semantic search contributed or we fell back to
    # pure lexical — that's exactly the kind of "under the hood" signal the dev tool exists to show.
    space = f"{cfg.embedding_model}@{cfg.embedding_dim}"
    dense_available = space in set(vectorstore.spaces())
    cfg.use_dense = dense_available
    cfg.use_bm25 = True                                    # lexical is always on (free, local)

    # ---- run the adaptive retrieve (route -> retrieve+fuse -> grade) ---------
    # route_and_retrieve never auto-spends (multi-query/HyDE stay off unless the cfg turns them on),
    # so the default call here is zero-cost. It returns hits already as plain dicts with provenance.
    result = retrieval.route_and_retrieve(query, cfg, kinds=kinds)

    return {
        "query":            query,
        "space":            space,
        "dense_available":  dense_available,
        "route":            result.get("route", {}),
        "grade":            result.get("grade", {}),
        "hits":             result.get("hits", []),
    }


# ==================================================================
# Register the tool (runs at import time -> app.tools picks it up)
# ==================================================================
# tool_from_module builds a base.Tool from (TOOL_SPEC, run): one source of truth, so the LLM's
# function declaration and the runtime validation can never drift from the implementation.
TOOL = base.tool_from_module(
    TOOL_SPEC, run,
    default_on=True,
    cost_note="free (lexical + local dense); a hosted embedding space or reranker is paid + logged",
)
