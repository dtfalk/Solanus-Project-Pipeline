"""lib/providers/rerank.py — model-agnostic cross-encoder reranking (the reranker axis).

rerank(query, docs, model=None, top_k=None) -> list[(idx, score)] sorted best-first. A reranker is
a *second-stage* judge: first-stage retrieval (BM25 / dense embeddings) is cheap and casts a wide
net, then a cross-encoder reads the query and each candidate *together* and scores true relevance.
Because it re-reads every candidate jointly it is far sharper but far more expensive, so we only
ever feed it the top handful that the cheap stage already surfaced ("retrieve 50, rerank to 5").

This is the third swappable model axis alongside lib/providers/embed.py (embedding) and
lib/providers/llm.py (generation). Like those, it routes by `config.RERANKERS[model].provider`:
  - "local"  — a real cross-encoder via fastembed's TextCrossEncoder. FREE, offline, runs NOW.
  - "voyage" / "cohere" — real adapters, but **PAID**: they raise a clear error unless the API key
    is present, and every call is cost-logged. Nothing is billed until you actually call them.

Every call routes through lib.costlog so a reranked run shows up in costs/usage.csv exactly like an
embedding or generation call would. Returning (index, score) tuples (not the docs themselves) keeps
this a pure scoring function: the caller maps indices back to its own ids/metas and owns provenance.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped so it's obvious what is stdlib vs. local vs. third-party
# ============================================================================

# Core Python Imports
import os
import sys
from pathlib import Path

# Local File Imports — same sys.path dance every step_7 module uses so that
# `import config` and `from lib import costlog` resolve no matter who imports us.
STEP7 = Path(__file__).resolve().parents[2]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config            # noqa: E402
from lib import costlog  # noqa: E402

# Load API keys exactly like embed.py/llm.py do: step_7/.env first, then step_4/.env (where the
# project's keys already live). VOYAGE_API_KEY / COHERE_API_KEY belong in step_7/.env.
try:
    from dotenv import load_dotenv
    load_dotenv(config.STEP7 / ".env")
    load_dotenv(config.REPO / "pipeline_v3" / "step_4" / ".env")
except Exception:
    pass

# A small cache so we only pay the (slow) weight-load cost for a local cross-encoder once per
# process — exactly the trick embed.py uses for its TextEmbedding instances.
_LOCAL_CACHE: dict = {}

# Map our friendly config names to the actual weights fastembed packages. fastembed ships a *fixed*
# catalogue of reranker ONNX weights (TextCrossEncoder.list_supported_models()); our config default
# is the upstream name "bge-reranker-v2-m3", which fastembed 0.8.x does NOT package — so we map it to
# the closest packaged substitute, "BAAI/bge-reranker-base".
#
# ⚠️ Consequence: the LOCAL path runs bge-reranker-*base*, not the multilingual v2-m3 named in
#    config. They are different models with different quality. If you specifically need v2-m3 (or any
#    weight fastembed doesn't ship) locally, use the FlagEmbedding / sentence-transformers path
#    documented in `_local_todo()` — it loads the exact HuggingFace weights, at the cost of a heavy
#    torch dependency we intentionally do NOT auto-install here.
FASTEMBED_RERANK_NAMES = {
    "bge-reranker-v2-m3":   "BAAI/bge-reranker-base",          # closest fastembed-packaged substitute
    "bge-reranker-base":    "BAAI/bge-reranker-base",
    "ms-marco-MiniLM-L-6":  "Xenova/ms-marco-MiniLM-L-6-v2",
    "ms-marco-MiniLM-L-12": "Xenova/ms-marco-MiniLM-L-12-v2",
    "jina-reranker-v2":     "jinaai/jina-reranker-v2-base-multilingual",
}
_DEFAULT_FASTEMBED_RERANKER = "BAAI/bge-reranker-base"


# ============================================================================
# Small helpers shared by the adapters
# ============================================================================
def _doc_text(d) -> str:
    """Accept either a plain string or a chunk dict (the shape lib/chunks.py emits).

    Retrieval results in this project are dicts like {"id", "text", "meta", ...}; callers may also
    hand us bare strings. We normalize to the text the cross-encoder should read, so callers never
    have to pre-flatten their candidates.

    Args:
        d: A document — a str, or a dict with a "text" (or "content") field.

    Returns:
        The document's text as a string.
    """
    if isinstance(d, str):
        return d
    if isinstance(d, dict):
        return str(d.get("text") or d.get("content") or "")
    return str(d)


def _est_tokens(query: str, texts) -> int:
    """Rough token count for cost logging when a provider returns no usage figure.

    A cross-encoder reads the query *together with each document*, so the billable text is roughly
    (query repeated per doc) + every doc. We use the project's ~4-chars-per-token rule (same as
    embed.py) — good enough for a projection, never for an invoice.

    Args:
        query: The search query string.
        texts: The candidate document strings being scored.

    Returns:
        An estimated total token count (at least 1).
    """
    total = len(query) * max(1, len(texts)) + sum(len(t) for t in texts)
    return max(1, total // 4)


# ============================================================================
# Public entry point — pick a provider, score, return a sorted (idx, score) ranking
# ============================================================================
def rerank(query: str, docs, model: str | None = None,
           top_k: int | None = None, batch: int = 64):
    """Score each candidate against the query with a cross-encoder reranker, best-first.

    This is the single function the rest of step_7 calls. It looks up the provider for `model` in
    config.RERANKERS and dispatches to the matching adapter, mirroring embed.embed_texts().

    Args:
        query: The user's question (or a transformed query — HyDE/multi-query produce these).
        docs: Candidate passages to re-score, in any order — bare strings, or chunk dicts with a
            "text" field. These are the first-stage hits; the caller maps indices back to ids/metas.
        model: A key in config.RERANKERS. Defaults to config.DEFAULTS["reranker"].
        top_k: If given, keep only the best `top_k` after scoring (a reranker's whole job is to cut a
            big candidate set down to a precise few — top_k is how you ask for that few).
        batch: Per-call batch size for the local cross-encoder (ignored by the cloud APIs, which
            batch server-side; kept so every adapter has a uniform signature).

    Returns:
        A list of (original_index, score) tuples sorted highest-score-first, where `original_index`
        points back into the input `docs` list. Higher score == more relevant for every adapter.

    Raises:
        ValueError:          If `model` isn't in config.RERANKERS.
        RuntimeError:        If a cloud provider's API key is missing.
        NotImplementedError: If a provider is named in config but has no adapter here.
    """
    model = model or config.DEFAULTS["reranker"]
    if model not in config.RERANKERS:
        raise ValueError(f"unknown reranker '{model}' (add it to config.RERANKERS)")

    # Empty candidate set: nothing to rank. Return early so adapters never see len(docs)==0 (and so
    # we never log a $0-but-pointless API call).
    texts = [_doc_text(d) for d in docs]
    if not texts:
        return []

    # ---- route to the provider adapter (mirrors embed.py's dispatch table) ----
    prov = config.RERANKERS[model]["provider"]
    dispatch = {"local": _local, "voyage": _voyage, "cohere": _cohere}
    if prov not in dispatch:
        raise NotImplementedError(f"reranker provider '{prov}' not wired yet (model {model})")
    ranked = dispatch[prov](query, texts, model, batch)

    # A reranker's whole job is to *re-order*, so always hand back sorted best-first, then truncate.
    ranked.sort(key=lambda pair: pair[1], reverse=True)
    return ranked[:top_k] if top_k else ranked


# ============================================================================
# Local adapter — FREE cross-encoder via fastembed, the default "no paid call" path
# ============================================================================
def _local(query: str, texts: list, model: str, batch: int):
    """Score with a local cross-encoder via fastembed (no network, $0).

    fastembed 0.8.x ships `fastembed.rerank.cross_encoder.TextCrossEncoder`, which downloads a small
    ONNX cross-encoder once and then scores (query, doc) pairs entirely offline. `.rerank()` returns
    one raw relevance score per document, in INPUT order (NOT pre-sorted) — the public `rerank()`
    sorts. Scores are unbounded logits (a cross-encoder's raw output), so treat them as a relative
    ordering, not a probability.

    If fastembed can't supply a reranker (too old / missing submodule), we fall back to the
    documented local TODO path so the caller gets actionable next steps, not a cryptic ImportError.

    Args:
        query: Search query.
        texts: Candidate document texts (already flattened by `rerank()`).
        model: Config reranker name (mapped to a fastembed weight via FASTEMBED_RERANK_NAMES).
        batch: Cross-encoder batch size (how many (query, doc) pairs per forward pass).

    Returns:
        List of (original_index, score) tuples (unsorted; the caller sorts).
    """
    try:
        from fastembed.rerank.cross_encoder import TextCrossEncoder
    except Exception as e:        # fastembed too old / rerank submodule absent
        return _local_todo(query, texts, model, batch, reason=repr(e))

    name = FASTEMBED_RERANK_NAMES.get(model, _DEFAULT_FASTEMBED_RERANKER)
    ce = _LOCAL_CACHE.get(name)
    if ce is None:
        # First use downloads the ONNX weights (a one-time, offline-after cost). Cached thereafter.
        ce = _LOCAL_CACHE[name] = TextCrossEncoder(model_name=name)

    # .rerank yields one score per doc in input order; materialize so we can index it.
    scores = list(ce.rerank(query, texts, batch_size=batch))

    # Local == free, but we STILL cost-log every model call (usd=0.0) so the run ledger is complete
    # and the local/cloud trade-off stays visible in costlog.summary().
    costlog.log("local", model, "rerank", items=len(texts), usd=0.0,
                meta=f"fastembed={name} q_len={len(query)}")
    return [(i, float(s)) for i, s in enumerate(scores)]


def _local_todo(query, texts, model, batch, reason: str = ""):
    """Fallback LOCAL path for weights fastembed does not package (e.g. bge-reranker-v2-m3).

    fastembed's cross-encoder catalogue is fixed, so a model named in config but absent from
    FASTEMBED_RERANK_NAMES (or a too-old fastembed) lands here. The real implementation is a few
    lines on top of FlagEmbedding (the reference impl for the BGE rerankers) or
    sentence-transformers' CrossEncoder, either of which loads the *exact* HuggingFace weights:

        # TODO(local-reranker): wire the exact HF weights. Two equivalent backends —
        #
        #   # (a) FlagEmbedding — the reference implementation for bge-reranker-*:
        #   #     pip install FlagEmbedding          # pulls torch; heavier than fastembed
        #   from FlagEmbedding import FlagReranker
        #   rr = FlagReranker("BAAI/bge-reranker-v2-m3", use_fp16=True)   # cache like _LOCAL_CACHE
        #   scores = rr.compute_score([[query, t] for t in texts], normalize=True)
        #
        #   # (b) sentence-transformers — same idea, different API:
        #   #     pip install sentence-transformers
        #   from sentence_transformers import CrossEncoder
        #   rr = CrossEncoder("BAAI/bge-reranker-v2-m3")
        #   scores = rr.predict([(query, t) for t in texts]).tolist()
        #
        #   if not isinstance(scores, list):           # a single pair returns a bare float
        #       scores = [scores]
        #   costlog.log("local", model, "rerank", items=len(texts), usd=0.0, meta="flag/sbert")
        #   return [(i, float(s)) for i, s in enumerate(scores)]

    We DELIBERATELY do not auto-`pip install` a heavy torch dependency or download large weights here
    — per project rules, heavy/paid steps are deferred with a clear TODO. So we raise with exact next
    steps rather than silently doing something slow.

    Args:
        query, texts, batch: The rerank request (unused until the TODO above is wired).
        model:  The config reranker name that fastembed couldn't serve.
        reason: Why we fell back (e.g. the import error), for a clearer message.

    Raises:
        NotImplementedError: Always — with the precise wiring instructions above.
    """
    raise NotImplementedError(
        f"local reranker '{model}' is not packaged by fastembed"
        + (f" ({reason})" if reason else "")
        + ". Either map it to a fastembed-shipped weight in FASTEMBED_RERANK_NAMES "
          "(e.g. 'BAAI/bge-reranker-base'), or wire FlagEmbedding / sentence-transformers per the "
          "TODO in rerank._local_todo(). The heavy torch download is intentionally deferred — "
          "install it explicitly before using this path.")


# ============================================================================
# Voyage adapter — PAID; real call, but gated behind the API key
# ============================================================================
def _voyage(query: str, texts: list, model: str, batch: int):
    """Voyage rerank (e.g. rerank-2.5). Real implementation, but it costs money — so it raises
    unless the key is present, and it cost-logs the call. (Nothing here runs until you call it.)

    Voyage returns a RerankingObject whose `.results` are already sorted best-first, each with the
    original `.index` (into our input) and a `.relevance_score` in [0, 1]; Voyage also reports
    `.total_tokens`. We re-shape to our (idx, score) contract; the public `rerank()` re-sorts (a
    harmless no-op since Voyage pre-sorts) and applies top_k.

    Args:
        query: Search query.
        texts: Candidate document texts (already flattened).
        model: Config reranker name (== Voyage model id).
        batch: Unused (Voyage batches server-side); kept for a uniform adapter signature.

    Returns:
        List of (original_index, score) tuples.

    Raises:
        RuntimeError: If VOYAGE_API_KEY is not set.
    """
    key = os.environ.get(config.API_KEY_ENV["voyage"])
    if not key:
        raise RuntimeError(
            f"{config.API_KEY_ENV['voyage']} not set — Voyage rerank is a PAID call; "
            "add the key to step_7/.env or use the free local reranker.")

    # Imported lazily (like embed.py) so this module imports fine without voyageai installed.
    import voyageai
    vo = voyageai.Client(api_key=key)

    # ⚠️ PAID CALL. Voyage's reranker is billed per token (query + docs); we log real usage below.
    r = vo.rerank(query, texts, model=model, top_k=len(texts))

    toks = getattr(r, "total_tokens", None) or _est_tokens(query, texts)
    costlog.log("voyage", model, "rerank", input_tokens=toks, items=len(texts),
                meta=f"q_len={len(query)}")
    return [(int(res.index), float(res.relevance_score)) for res in r.results]


# ============================================================================
# Cohere adapter — PAID; real call, but gated behind the API key
# ============================================================================
def _cohere(query: str, texts: list, model: str, batch: int):
    """Cohere Rerank (e.g. rerank-4-pro). Same gating + cost-logging contract as Voyage.

    Cohere's v2 client exposes `client.rerank(model=, query=, documents=, top_n=)` and returns a
    response whose `.results` are sorted best-first, each with `.index` (into our input) and a
    `.relevance_score` in [0, 1]. Cohere bills per *search* (one call), not per token, so the
    per-1M-token estimator in costlog can't price it — we pass an explicit `usd` from the config
    per-search price.

    Args:
        query: Search query.
        texts: Candidate document texts (already flattened).
        model: Config reranker name (== Cohere model id).
        batch: Unused (server-side batching); kept for a uniform adapter signature.

    Returns:
        List of (original_index, score) tuples.

    Raises:
        RuntimeError: If COHERE_API_KEY is not set.
    """
    key = os.environ.get(config.API_KEY_ENV["cohere"])
    if not key:
        raise RuntimeError(
            f"{config.API_KEY_ENV['cohere']} not set — Cohere rerank is a PAID call; "
            "add the key to step_7/.env or use the free local reranker.")

    # Lazy import (like embed.py) so the module loads without the cohere SDK present.
    import cohere
    co = cohere.ClientV2(api_key=key)

    # ⚠️ PAID CALL. One rerank() == one billable "search"; we price it from config (per search).
    r = co.rerank(model=model, query=query, documents=texts, top_n=len(texts))

    per_search = config.RERANKERS.get(model, {}).get("price", 0.0)
    costlog.log("cohere", model, "rerank", items=len(texts), usd=per_search,
                meta=f"q_len={len(query)} searches=1")
    return [(int(res.index), float(res.relevance_score)) for res in r.results]


# ============================================================================
# Smoke check — `python lib/providers/rerank.py` (LOCAL only, free, offline)
# ============================================================================
if __name__ == "__main__":
    # A tiny, FREE sanity check that the LOCAL adapter wires up end-to-end. It does NOT touch any
    # paid provider (those need keys and are guarded). First run downloads the small cross-encoder
    # weights once; after that it is fully offline.
    demo_query = "Did Father Solanus record a cancer cure?"
    demo_docs = [
        {"text": "Enrolled the sick man in the Seraphic Mass Association; reports a cancer cured."},
        {"text": "A letter thanking Father Solanus for his kindness during a long winter."},
        {"text": "Notebook entry: weather was cold; the chapel needed repairs this month."},
    ]
    ranking = rerank(demo_query, demo_docs, top_k=2)   # default reranker from config.DEFAULTS
    print("query :", demo_query)
    for pos, (idx, score) in enumerate(ranking, start=1):
        print(f"  #{pos}  doc[{idx}]  score={score:+.4f}  | {demo_docs[idx]['text'][:60]}")
    print("\nNOTE: cloud rerankers (voyage/cohere) are key-guarded and were not called.")
