"""lib/retrieval.py — the retrieval brain (model-agnostic, every stage toggleable).

This is the module the RAG agent's "find me the relevant pages" tool is built on. Everything is a
*pure function returning ranked hits with provenance* — give it a query and some toggles, get back a
list of {id, score, text, meta, provenance} dicts. Nothing here mutates the corpus or writes files;
it only reads the local index that earlier stages built, and optionally calls a model (always
cost-logged, always gated so the FREE path needs no paid keys).

The pieces, in the order a query flows through them:

  1. ROUTER — look at the query and decide how hard it is. "When was Solanus born?" is *simple*
     (maybe no retrieval needed); "List every cancer favor reported in 1933" is *complex* and wants
     multi-query decomposition. A cheap heuristic does this for free; an LLM classifier is available
     but gated.
  2. QUERY TRANSFORMS — optionally rewrite the query to retrieve better:
       • HyDE: ask an LLM to *hallucinate a plausible answer*, then embed THAT. A fake answer lives
         in answer-space, which is closer to the real passages than a terse question is. (Paid → gated.)
       • Multi-query / RAG-Fusion: generate several paraphrases, retrieve for each, fuse. (Paid → gated.)
  3. HYBRID RETRIEVAL — run BOTH a sparse lexical search (BM25, great at exact names/dates like
     "Panyard" or "Nov. 8th") AND a dense semantic search (embeddings, great at paraphrase), then
     fuse them with Reciprocal Rank Fusion (RRF). Lexical + semantic each catch what the other
     misses; RRF needs no score calibration between them.
  4. RERANK — optionally hand the fused top-N to a cross-encoder that re-reads (query, passage)
     pairs for a sharper final order. (lib.providers.rerank; local is free, voyage/cohere paid.)
  5. GRADE / ABSTAIN (Self-RAG / CRAG style) — score how trustworthy the retrieved set is (using
     real signals we already have: rank scores, OCR min_conf on the source regions, lexical
     overlap). If it's weak, say so, so the generator can abstain instead of confabulating — vital
     for an *archive*, where a confident wrong answer is worse than "not found".

FREE PATH (no paid call, no key): router-heuristic → BM25 + local dense → RRF → grade. That alone
is a solid hybrid retriever; the LLM/rerank stages are pure upside you toggle on when you've decided
to spend.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped (stdlib / third-party / local) the way the other modules do
# ============================================================================

# Core Python Imports
import json
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

# Third-Party Imports
import numpy as np

# Local File Imports — the same sys.path bootstrap every lib/ module uses, so
# `import config` and `from lib import ...` resolve regardless of the caller's cwd.
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402
from lib import chunks as _chunks   # noqa: E402
from lib import costlog             # noqa: E402
from lib import vectorstore         # noqa: E402
from lib.providers import embed     # noqa: E402
from lib.providers import llm       # noqa: E402
from lib.providers import rerank as _rerank  # noqa: E402


# ============================================================================
# Hit + RetrievalConfig — the two data shapes that flow through everything
# ============================================================================
# A "Hit" is one retrieved chunk plus the provenance you need to CITE it back to the exact scanned
# region (doc_id + rid -> vertices -> IIIF deep-zoom box). We mirror the chunk shape from
# lib/chunks.py exactly (id / kind / text / meta), then add the fused score and a provenance dict.
@dataclass
class Hit:
    """One ranked retrieval result, carrying everything needed to cite the source region."""
    id: str                                   # chunk id, e.g. "doc_1::doc_1.src_content.0"
    score: float                              # final fused/reranked score (higher = better)
    text: str                                 # the chunk text that was retrieved
    kind: str = ""                            # "letter" | "notebook_entry"
    meta: dict = field(default_factory=dict)  # provenance metadata copied from the chunk
    provenance: dict = field(default_factory=dict)  # citation-ready: doc_id, rid, page, vertices, min_conf
    components: dict = field(default_factory=dict)   # debug: per-retriever rank/score that fed RRF

    def to_dict(self) -> dict:
        """Plain-dict form (so callers can json.dumps a result set for logging/eval)."""
        return {"id": self.id, "score": self.score, "text": self.text, "kind": self.kind,
                "meta": self.meta, "provenance": self.provenance, "components": self.components}


# Every knob in one place. Pass a RetrievalConfig (or rely on the defaults) so a caller — the dev
# harness, an eval sweep, the agent — can flip any technique on/off without touching the functions.
@dataclass
class RetrievalConfig:
    """All retrieval toggles. The defaults describe the FREE path (no paid call required)."""
    # which embedding space (a partition in lib.vectorstore). Defaults to config.DEFAULTS.
    embedding_model: str = config.DEFAULTS["embedding"][0]
    embedding_dim: int = config.DEFAULTS["embedding"][1]

    # core hybrid retrieval
    use_bm25: bool = True                      # sparse lexical (free, local)
    use_dense: bool = True                     # dense semantic (free if local embeddings)
    rrf_k: int = 60                            # RRF damping constant; 60 is the field-standard default
    candidate_k: int = 50                      # how many to pull from EACH retriever before fusing
    top_k: int = 8                             # how many fused hits to return

    # reranking (paid unless reranker provider == "local")
    use_rerank: bool = False
    reranker: str = config.DEFAULTS["reranker"]
    rerank_pool: int = 30                      # how many fused hits to feed the cross-encoder

    # query transforms (ALL gated — they call an LLM, so off by default for the free path)
    use_hyde: bool = False
    use_multiquery: bool = False
    multiquery_n: int = 4                      # how many paraphrases RAG-Fusion generates

    # routing
    use_llm_router: bool = False              # off => cheap heuristic router (free)
    auto_apply_route: bool = False            # if True, let the router ESCALATE to paid transforms
                                              # (e.g. multi-query on a complex query). OFF keeps the
                                              # default path at zero paid calls; the agent flips it on.

    # metadata pre-filter passed straight to the vector store / applied to BM25 hits
    where: Optional[dict] = None

    # model selection for the gated LLM steps
    llm_model: Optional[str] = None           # None => config.DEFAULTS["llm"]


# ============================================================================
# Corpus loading — read the chunk texts + the per-region geometry ONCE, cache
# ============================================================================
# We hold a small in-process cache so repeated queries in one session don't re-parse 9k chunks or
# rebuild the BM25 index. It's keyed by (kinds, where-fingerprint) so different filters get their
# own index. Purely an optimization — semantics are unchanged if you ignore it.
_CORPUS_CACHE: dict = {}
_REGION_INDEX: Optional[dict] = None


def _region_index() -> dict:
    """Map (doc_id, rid) -> region record {vertices, min_conf, category, ...} for provenance.

    The vertices + OCR confidence we need to *cite* a hit (and to GRADE it) live in each source
    record's `regions` list, not in the chunk. We build a lookup once so attaching provenance to a
    hit is O(1). Reads the same step_6 json the chunker reads — no model call, no write.

    Returns:
        dict keyed by "<doc_id>::<rid>" -> the region dict (with vertices parsed to a list).
    """
    global _REGION_INDEX
    if _REGION_INDEX is not None:
        return _REGION_INDEX
    idx: dict = {}
    for src in (config.DOCUMENTS, config.NOTEBOOKS):
        for rec in json.loads(src.read_text()):
            doc_id = rec["id"]
            for reg in rec.get("regions", []):
                rid = reg.get("rid")
                if rid is None:
                    continue
                # vertices arrive as a JSON-ish string in some records; normalize to a real list.
                verts = reg.get("vertices")
                if isinstance(verts, str):
                    try:
                        verts = json.loads(verts)
                    except (json.JSONDecodeError, TypeError):
                        verts = None
                idx[f"{doc_id}::{rid}"] = {
                    "rid":         rid,
                    "category":    reg.get("category"),
                    "min_conf":    _to_float(reg.get("min_conf")),
                    "vertices":    verts,
                    "page":        reg.get("page_number_in_type"),
                }
    _REGION_INDEX = idx
    return idx


def _to_float(x) -> Optional[float]:
    """Coerce min_conf (sometimes a string like '0.986') to float; None if it can't."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _load_corpus(kinds: Optional[list] = None) -> list[dict]:
    """The chunk list from lib.chunks (one dict per letter / notebook entry), cached per `kinds`."""
    key = tuple(kinds) if kinds else ("__all__",)
    if key not in _CORPUS_CACHE:
        _CORPUS_CACHE[key] = _chunks.build_chunks(kinds=kinds)
    return _CORPUS_CACHE[key]


def _provenance_for(chunk: dict) -> dict:
    """Build the citation-ready provenance dict for a chunk (doc_id, rid, page, vertices, min_conf).

    For a notebook entry the chunk's meta.rid points straight at one region. For a letter (which is
    several regions joined) there's no single rid, so we record the doc-level location and leave
    vertices to the per-region detail the UI can resolve from the source record. Either way the
    citation can always reach the right page; entries additionally reach the exact box.
    """
    meta = chunk.get("meta", {})
    doc_id = meta.get("doc_id")
    rid = meta.get("rid")                       # present for notebook_entry chunks
    prov = {"doc_id": doc_id, "rid": rid,
            "page": meta.get("page"), "pdf_page": meta.get("pdf_page"),
            "section": meta.get("section"), "vertices": None, "min_conf": None}
    if doc_id and rid:
        reg = _region_index().get(f"{doc_id}::{rid}")
        if reg:
            prov["vertices"] = reg["vertices"]
            prov["min_conf"] = reg["min_conf"]
    return prov


# ============================================================================
# BM25 — sparse lexical retrieval (free, local; great at exact names + dates)
# ============================================================================
# rank-bm25 (the `rank_bm25` package) is the standard pure-Python BM25. If it isn't installed we
# DON'T crash the whole module — we fall back to a tiny built-in BM25Okapi so the free path keeps
# working. (The fallback is correct BM25; rank-bm25 is just faster/better-tested. TODO: once the
# venv has `pip install rank-bm25`, the import below uses it automatically.)
_TOKEN_RE = re.compile(r"[A-Za-z0-9']+")


def _tokenize(text: str) -> list[str]:
    """Lowercase word/number tokens. Deliberately simple + deterministic so BM25 is reproducible.
    Keeps apostrophes (O'Donnell) and digits (dates like '1933') — both matter in this corpus."""
    return _TOKEN_RE.findall(text.lower())


class _FallbackBM25:
    """A small, correct BM25Okapi — used only if the `rank_bm25` package isn't installed.

    BM25 ranks a doc by, for each query term: (term frequency, saturated so repeats matter less) ×
    (inverse document frequency, so rare words count more) × (a length normalization, so long docs
    don't win just by being long). k1 controls TF saturation; b controls length normalization. We
    keep the standard defaults so behavior matches rank-bm25 closely.
    """

    def __init__(self, corpus_tokens: list[list[str]], k1: float = 1.5, b: float = 0.75):
        self.k1, self.b = k1, b
        self.docs = corpus_tokens
        self.N = len(corpus_tokens)
        self.doc_len = [len(d) for d in corpus_tokens]
        self.avgdl = (sum(self.doc_len) / self.N) if self.N else 0.0
        # document frequency: in how many docs each term appears (for IDF).
        df: dict = {}
        self.tf: list[dict] = []
        for toks in corpus_tokens:
            counts: dict = {}
            for t in toks:
                counts[t] = counts.get(t, 0) + 1
            self.tf.append(counts)
            for t in counts:
                df[t] = df.get(t, 0) + 1
        # idf with the standard BM25 smoothing (+0.5) and a floor so it never goes negative.
        self.idf = {t: math.log(1 + (self.N - n + 0.5) / (n + 0.5)) for t, n in df.items()}

    def get_scores(self, query_tokens: list[str]) -> np.ndarray:
        scores = np.zeros(self.N, dtype=np.float32)
        for t in query_tokens:
            idf = self.idf.get(t)
            if idf is None:
                continue                          # term never appears in the corpus → contributes 0
            for i in range(self.N):
                f = self.tf[i].get(t, 0)
                if not f:
                    continue
                denom = f + self.k1 * (1 - self.b + self.b * self.doc_len[i] / (self.avgdl or 1))
                scores[i] += idf * (f * (self.k1 + 1)) / denom
        return scores


_BM25_CACHE: dict = {}


def _bm25_index(corpus: list[dict]):
    """Build (and cache) a BM25 index over the corpus chunk texts. Prefers `rank_bm25`."""
    key = id(corpus)
    if key in _BM25_CACHE:
        return _BM25_CACHE[key]
    tokenized = [_tokenize(c["text"]) for c in corpus]
    try:
        from rank_bm25 import BM25Okapi          # the preferred, well-tested implementation
        index = BM25Okapi(tokenized)
    except ImportError:
        # Free path still works without the dependency — just a bit slower on the full corpus.
        index = _FallbackBM25(tokenized)
    _BM25_CACHE[key] = index
    return index


def bm25_search(query: str, corpus: list[dict], k: int = 50,
                where: Optional[dict] = None) -> list[dict]:
    """Sparse lexical retrieval. Returns [{id, score, idx}] best-first (idx -> corpus position).

    Args:
        query: The search string (already routed/transformed by the caller, if at all).
        corpus: The chunk list to search (from _load_corpus).
        k: How many top hits to return.
        where: Optional metadata equality filter (e.g. {"kind": "notebook_entry"}); applied to the
            chunk meta before ranking, so filters cost nothing extra.

    Returns:
        Ranked list of {"id", "score", "idx"} (idx is the position in `corpus`).
    """
    index = _bm25_index(corpus)
    scores = index.get_scores(_tokenize(query))
    order = np.argsort(-np.asarray(scores))
    out = []
    for i in order:
        c = corpus[int(i)]
        if where and not _passes_filter(c, where):
            continue
        s = float(scores[int(i)])
        if s <= 0:                                # no lexical overlap at all → not a real hit
            break
        out.append({"id": c["id"], "score": s, "idx": int(i)})
        if len(out) >= k:
            break
    return out


def _passes_filter(chunk: dict, where: dict) -> bool:
    """Equality metadata filter against a chunk's meta (plus the top-level 'kind').

    Each key in `where` must match. A list value means "any of these" (membership). We also accept
    a callable for power users (e.g. a year predicate over the date string).
    """
    for key, want in where.items():
        have = chunk.get("kind") if key == "kind" else chunk.get("meta", {}).get(key)
        if callable(want):
            if not want(have):
                return False
        elif isinstance(want, (list, tuple, set)):
            if have not in want:
                return False
        elif have != want:
            return False
    return True


# ============================================================================
# Dense retrieval — semantic search over a chosen embedding space (free if local)
# ============================================================================
def dense_search(query: str, cfg: RetrievalConfig, k: int = 50) -> list[dict]:
    """Embed the query in the configured space and cosine-search the vector store partition.

    The embedding call is cost-logged inside embed.embed_texts. If you picked a LOCAL embedding
    model (e.g. bge-large-en-v1.5) this costs $0; a hosted model (gemini/openai/voyage) is a paid
    call and needs its key. The vector store does the math (vectors are pre-normalized → dot==cosine).

    Args:
        query: The (possibly transformed) query text.
        cfg: RetrievalConfig — selects the embedding space + metadata filter.
        k: How many nearest neighbors to return.

    Returns:
        Ranked list of {"id", "score", "meta"} from lib.vectorstore.search.
    """
    space = f"{cfg.embedding_model}@{cfg.embedding_dim}"
    # task="query" so providers use the asymmetric query-side encoding (matters for gemini/voyage).
    qvec, _ = embed.embed_texts([query], cfg.embedding_model, cfg.embedding_dim, task="query")
    return vectorstore.search(space, qvec[0], k=k, where=cfg.where)


# ============================================================================
# RRF — Reciprocal Rank Fusion: combine ranked lists WITHOUT score calibration
# ============================================================================
def reciprocal_rank_fusion(ranked_lists: list[list[dict]], k: int = 60,
                           weights: Optional[list[float]] = None) -> list[dict]:
    """Fuse several ranked lists into one by summing 1/(k + rank) across lists.

    Why RRF instead of adding the raw scores? BM25 scores and cosine similarities live on totally
    different scales — adding them would let whichever retriever happens to have bigger numbers
    dominate. RRF throws the scores away and uses only each item's *rank position*, so a document
    near the top of either list gets a big boost. `k` (default 60, the published default) damps the
    influence of the very top ranks so a single retriever can't unilaterally decide the winner.

    Args:
        ranked_lists: A list of ranked result lists; each item must have an "id".
        k: RRF damping constant.
        weights: Optional per-list weight (e.g. trust dense a bit more than BM25). Defaults to 1.0.

    Returns:
        One fused ranked list of {"id", "score", "components"} best-first, where `components`
        records each contributing list's rank (handy for debugging "why did this rank here?").
    """
    weights = weights or [1.0] * len(ranked_lists)
    fused: dict = {}
    for li, results in enumerate(ranked_lists):
        w = weights[li]
        for rank, r in enumerate(results):           # rank is 0-based here
            rid = r["id"]
            contrib = w / (k + rank + 1)             # +1 so the top item uses rank=1, not 0
            agg = fused.setdefault(rid, {"id": rid, "score": 0.0, "components": {}})
            agg["score"] += contrib
            agg["components"][f"list{li}"] = {"rank": rank + 1, "score": r.get("score")}
    return sorted(fused.values(), key=lambda x: x["score"], reverse=True)


# ============================================================================
# Query transforms — HyDE & multi-query / RAG-Fusion (LLM-backed → GATED)
# ============================================================================
def hyde_query(query: str, cfg: RetrievalConfig) -> str:
    """HyDE: ask the LLM to write a short hypothetical ANSWER, which we then embed instead of the
    question. Intuition: a question and its answer use different words ("Who cured?" vs. "Mrs.
    Panyard's cancer was reported cured after enrollment…"); the fake answer lands nearer the real
    passages in embedding space, lifting dense recall. This is a PAID LLM call → only run when the
    caller has opted in (cfg.use_hyde) and accepted the cost. The call is cost-logged in llm.generate.

    Args:
        query: The original user question.
        cfg: RetrievalConfig (selects the LLM model).

    Returns:
        The hypothetical-answer text to embed. (Caller decides to embed this instead of `query`.)
    """
    system = ("You are helping search a historical archive of Father Solanus Casey's letters and "
              "notebooks (1920s–1950s). Write a brief, plausible passage that would directly answer "
              "the question, in the style of such a document. Do not hedge; do not say you are unsure.")
    prompt = f"Question: {query}\n\nWrite a 2-4 sentence hypothetical answer passage:"
    text, _ = llm.generate(prompt, model=cfg.llm_model, system=system, temperature=0.3)
    return text.strip() or query                    # fall back to the raw query if the LLM is empty


def multi_queries(query: str, cfg: RetrievalConfig) -> list[str]:
    """RAG-Fusion / multi-query: ask the LLM for a few alternate phrasings of the question, so we
    can retrieve for each and fuse. Different phrasings surface different true passages (one says
    "favor", another "miracle", another "answered prayer"). PAID LLM call → gated; cost-logged.

    Args:
        query: The original question.
        cfg: RetrievalConfig (LLM model + how many variants via cfg.multiquery_n).

    Returns:
        A list that always includes the original query plus up to cfg.multiquery_n paraphrases.
    """
    system = ("You rewrite a search query into diverse alternative phrasings for a historical "
              "archive search. Return ONLY a JSON array of strings, no prose.")
    prompt = (f"Original query: {query}\n\nGive {cfg.multiquery_n} alternative phrasings as a JSON "
              f"array of strings (vary vocabulary, specificity, and framing).")
    text, _ = llm.generate(prompt, model=cfg.llm_model, system=system, json_mode=True, temperature=0.5)
    variants: list[str] = []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            variants = [str(v).strip() for v in parsed if str(v).strip()]
    except (json.JSONDecodeError, TypeError):
        variants = []                               # be forgiving: a bad parse just means no extras
    # Always keep the original; dedupe while preserving order.
    seen, out = set(), []
    for q in [query, *variants][: cfg.multiquery_n + 1]:
        if q.lower() not in seen:
            seen.add(q.lower())
            out.append(q)
    return out


# ============================================================================
# ROUTER — decide how hard a query is (cheap heuristic free; LLM classifier gated)
# ============================================================================
# Three buckets, matching the research plan:
#   "simple"   → likely answerable directly / a single fact; minimal retrieval.
#   "moderate" → a normal single-shot retrieval is enough.
#   "complex"  → multi-part / aggregate / cross-document → wants multi-query / multi-hop.
_COMPLEX_MARKERS = ("list all", "every", "all the", "how many", "compare", "across", "timeline",
                    "over time", "between", "relationship", "connected", "trace", "summarize",
                    "themes", "patterns", "and ", " or ")  # conjunctions hint at multi-part asks
_SIMPLE_MARKERS = ("who is", "who was", "when was", "where was", "what is", "define", "born",
                   "died", "date of")


def route_query(query: str, cfg: RetrievalConfig) -> dict:
    """Classify a query's complexity and recommend a retrieval strategy.

    The cheap heuristic (free, default) reads length, question words, conjunctions, and aggregate
    markers — enough to separate "When was he born?" from "List every favor reported in 1933 and
    how each turned out." If cfg.use_llm_router is set, an LLM makes the call instead (PAID,
    cost-logged) for ambiguous phrasing.

    Args:
        query: The user's question.
        cfg: RetrievalConfig (use_llm_router toggles the LLM path).

    Returns:
        {"complexity": "simple"|"moderate"|"complex", "strategy": str, "reason": str,
         "suggest": {"use_multiquery": bool, "multihop": bool}} — a recommendation the caller may
        apply (route_and_retrieve does). It RECOMMENDS, it doesn't silently override your cfg.
    """
    if cfg.use_llm_router:
        return _llm_route(query, cfg)

    q = query.lower().strip()
    n_words = len(q.split())
    has_complex = any(m in q for m in _COMPLEX_MARKERS)
    has_simple = any(q.startswith(m) or m in q[:24] for m in _SIMPLE_MARKERS)

    if has_complex or n_words > 22:
        complexity, reason = "complex", "aggregate/multi-part markers or a long, compound question"
        suggest = {"use_multiquery": True, "multihop": True}
        strategy = "decompose → multi-query retrieval + RRF (+ rerank), then multi-hop synthesis"
    elif has_simple and n_words <= 8:
        complexity, reason = "simple", "short factual lookup phrasing"
        suggest = {"use_multiquery": False, "multihop": False}
        strategy = "single retrieval, small top_k (a direct answer may already be in one region)"
    else:
        complexity, reason = "moderate", "ordinary single-intent question"
        suggest = {"use_multiquery": False, "multihop": False}
        strategy = "single hybrid retrieval + RRF (+ optional rerank)"
    return {"complexity": complexity, "strategy": strategy, "reason": reason, "suggest": suggest}


def _llm_route(query: str, cfg: RetrievalConfig) -> dict:
    """LLM query-complexity classifier (PAID, gated, cost-logged). Falls back to the heuristic on
    any parse failure so the router can never become a hard dependency on the model."""
    system = ("Classify a search query's complexity for a historical-archive RAG system. "
              "Reply ONLY as JSON: {\"complexity\": \"simple|moderate|complex\", \"reason\": \"...\"}. "
              "simple=single fact/no retrieval; moderate=one retrieval; complex=multi-part/aggregate/multi-hop.")
    try:
        text, _ = llm.generate(f"Query: {query}", model=cfg.llm_model, system=system,
                               json_mode=True, temperature=0.0)
        parsed = json.loads(text)
        comp = parsed.get("complexity", "moderate")
        if comp not in ("simple", "moderate", "complex"):
            comp = "moderate"
        suggest = {"use_multiquery": comp == "complex", "multihop": comp == "complex"}
        return {"complexity": comp, "strategy": f"LLM-routed: {comp}",
                "reason": parsed.get("reason", "llm"), "suggest": suggest}
    except Exception:
        # Network/parse hiccup must not break retrieval — degrade to the free heuristic.
        cfg_free = RetrievalConfig(**{**cfg.__dict__, "use_llm_router": False})
        return route_query(query, cfg_free)


# ============================================================================
# Self-RAG / CRAG graders — "is this context good enough, or should we abstain?"
# ============================================================================
# In an archive, a confident-but-unsupported answer is the worst outcome. So after retrieval we
# GRADE the result set with signals we already have — no model needed for the free grader:
#   • top fused score and the gap to the rest (a clear winner is reassuring),
#   • lexical overlap between the query and the top hits (did we even match the words?),
#   • OCR min_conf on the cited regions (low-confidence transcription = shaky evidence).
# CRAG's idea is exactly this: classify the retrieval as Correct / Ambiguous / Incorrect and act
# (answer / caveat / abstain or re-retrieve).
def grade_context(query: str, hits: list[Hit],
                  min_score: float = 0.0, min_overlap: float = 0.10) -> dict:
    """Score how trustworthy a retrieved set is and recommend answer / caveat / abstain.

    Args:
        query: The original question (for lexical-overlap signal).
        hits: The ranked Hits to judge (use the FINAL set you'd hand the generator).
        min_score: Floor on the top hit's fused score below which we get suspicious.
        min_overlap: Floor on query-term coverage by the top hits.

    Returns:
        {"verdict": "answer"|"caveat"|"abstain", "confidence": float in [0,1], "signals": {...},
         "reason": str}. The generator should ABSTAIN ("I couldn't find this in the archive") on
        "abstain", and add an uncertainty note on "caveat".
    """
    if not hits:
        return {"verdict": "abstain", "confidence": 0.0,
                "signals": {}, "reason": "no hits retrieved"}

    qterms = set(_tokenize(query))
    top = hits[: min(5, len(hits))]
    # lexical overlap: fraction of distinct query terms that appear in the top hits' text.
    covered = qterms & {t for h in top for t in _tokenize(h.text)}
    overlap = (len(covered) / len(qterms)) if qterms else 0.0
    # OCR confidence on the cited regions (only entries carry a region min_conf).
    confs = [h.provenance.get("min_conf") for h in top if h.provenance.get("min_conf") is not None]
    avg_conf = (sum(confs) / len(confs)) if confs else None
    top_score = hits[0].score
    # score margin: how much the #1 beats the #3 (a flat distribution => ambiguous retrieval).
    margin = (hits[0].score - hits[min(2, len(hits) - 1)].score) if len(hits) > 1 else hits[0].score

    signals = {"top_score": round(top_score, 6), "score_margin": round(margin, 6),
               "lexical_overlap": round(overlap, 3),
               "avg_ocr_conf": (round(avg_conf, 3) if avg_conf is not None else None),
               "n_hits": len(hits)}

    # Decide. These thresholds are deliberately conservative (archive = prefer "not found").
    if top_score <= min_score or overlap < min_overlap:
        verdict, reason = "abstain", "top score and/or lexical overlap too low — likely no good match"
    elif (avg_conf is not None and avg_conf < 0.60) or margin <= 0:
        verdict, reason = "caveat", "evidence present but shaky (low OCR confidence or flat ranking)"
    else:
        verdict, reason = "answer", "strong top match with adequate overlap"

    # A rough confidence number for sorting/eval dashboards (not a probability — a heuristic blend).
    confidence = max(0.0, min(1.0, 0.5 * min(overlap / max(min_overlap, 1e-6), 1.0)
                              + 0.3 * (1.0 if margin > 0 else 0.0)
                              + 0.2 * (avg_conf if avg_conf is not None else 0.7)))
    return {"verdict": verdict, "confidence": round(confidence, 3),
            "signals": signals, "reason": reason}


def grade_document(query: str, hit: Hit, cfg: RetrievalConfig) -> dict:
    """Self-RAG style PER-DOCUMENT relevance grade via the LLM (PAID, gated, cost-logged).

    Self-RAG asks the model to emit "ISREL" (is this retrieved passage relevant?) tokens. We expose
    the same idea as an optional helper: hand the LLM one (query, passage) pair and get a relevance
    label + reason. Useful to FILTER a reranked set before generation. Only call this if you've
    accepted per-document LLM cost; the free `grade_context` already gives a good set-level signal.

    Args:
        query: The question.
        hit: A single Hit to judge.
        cfg: RetrievalConfig (LLM model).

    Returns:
        {"relevant": bool, "label": "relevant"|"partial"|"irrelevant", "reason": str}.
    """
    system = ("Judge whether a retrieved archive passage is relevant to the question. Reply ONLY "
              "as JSON: {\"label\": \"relevant|partial|irrelevant\", \"reason\": \"...\"}.")
    prompt = f"Question: {query}\n\nPassage:\n{hit.text[:1500]}"
    try:
        text, _ = llm.generate(prompt, model=cfg.llm_model, system=system,
                               json_mode=True, temperature=0.0)
        parsed = json.loads(text)
        label = parsed.get("label", "partial")
        return {"relevant": label in ("relevant", "partial"), "label": label,
                "reason": parsed.get("reason", "")}
    except Exception:
        # If the judge call fails, don't drop the doc — treat as 'partial' and let humans decide.
        return {"relevant": True, "label": "partial", "reason": "grader unavailable; kept by default"}


# ============================================================================
# Assemble Hits — turn fused {id,score} rows into provenance-carrying Hit objects
# ============================================================================
def _hits_from_fused(fused: list[dict], corpus: list[dict], top_k: int) -> list[Hit]:
    """Map fused {id, score, components} rows back to full Hits (text + provenance).

    We need the chunk text + meta to build a Hit and to attach geometry, so we index the corpus by
    id once and look each fused id up. Anything we can't find (shouldn't happen) is skipped.
    """
    by_id = {c["id"]: c for c in corpus}
    out: list[Hit] = []
    for row in fused[:top_k]:
        c = by_id.get(row["id"])
        if c is None:
            continue
        out.append(Hit(id=c["id"], score=row["score"], text=c["text"], kind=c.get("kind", ""),
                       meta=c.get("meta", {}), provenance=_provenance_for(c),
                       components=row.get("components", {})))
    return out


# ============================================================================
# rerank_hits — the optional cross-encoder reorder hook (free local / paid hosted)
# ============================================================================
def rerank_hits(query: str, hits: list[Hit], cfg: RetrievalConfig) -> list[Hit]:
    """Re-order Hits with a cross-encoder reranker (lib.providers.rerank). Pure: returns a NEW list.

    We feed the reranker only cfg.rerank_pool candidates (cross-encoders are expensive), get back
    indices+scores, overwrite each Hit's score with the reranker's, and re-sort. The cost (local=$0,
    voyage/cohere=paid) is logged inside the rerank provider.
    """
    if not hits:
        return hits
    pool = hits[: cfg.rerank_pool]
    # lib.providers.rerank returns (original_index, score) tuples sorted best-first; index points
    # back into the list of texts we passed in (same order as `pool`).
    try:
        ranked = _rerank.rerank(query, [h.text for h in pool], model=cfg.reranker)
    except RuntimeError as e:
        # A paid reranker (Voyage/Cohere) raises when its API key is absent. Rather than fail the whole
        # query, degrade gracefully to the FREE local cross-encoder: the model-agnostic promise is
        # "swap any model", not "crash without a key". (Explicit local choice still raises — that means
        # the local backend itself is broken, which the caller should see.)
        local = next((k for k, v in config.RERANKERS.items() if v.get("provider") == "local"), None)
        if not local or local == cfg.reranker:
            raise
        print(f"[retrieval] reranker {cfg.reranker!r} unavailable ({e}); "
              f"falling back to free local {local!r}", file=sys.stderr)
        ranked = _rerank.rerank(query, [h.text for h in pool], model=local)
    reordered: list[Hit] = []
    for idx, score in ranked:
        h = pool[idx]
        h.score = score                             # the reranker is the authority now
        h.components = {**h.components, "rerank": score}
        reordered.append(h)
    # Any candidates beyond the rerank pool keep their fused order, appended after the reranked ones.
    reordered.extend(hits[cfg.rerank_pool:])
    return reordered


# ============================================================================
# retrieve — the one-stop hybrid retriever (the FREE path lives here)
# ============================================================================
def retrieve(query: str, cfg: Optional[RetrievalConfig] = None,
             kinds: Optional[list] = None) -> list[Hit]:
    """Hybrid BM25 + dense retrieval, fused with RRF, optionally reranked. Returns ranked Hits.

    This is the workhorse. With the default cfg it's the FREE path: BM25 (+ local dense if you built
    a local embedding partition) → RRF → top_k Hits with full provenance. Toggle cfg.use_rerank /
    cfg.use_hyde to spend on quality. It does NOT route or multi-query on its own — that's
    `route_and_retrieve`, which orchestrates this; keeping `retrieve` single-shot makes it a clean
    building block (and trivially testable).

    Args:
        query: The (already routed/transformed) query text to retrieve for.
        cfg: RetrievalConfig of toggles; None → defaults (free hybrid path).
        kinds: Optional restriction to chunk kinds (["notebook_entry"] / ["letter"]).

    Returns:
        Ranked list[Hit], best-first, length ≤ cfg.top_k.
    """
    cfg = cfg or RetrievalConfig()
    corpus = _load_corpus(kinds)

    # ---- gather candidate lists from each enabled retriever ------------------
    ranked_lists: list[list[dict]] = []
    if cfg.use_bm25:
        ranked_lists.append(bm25_search(query, corpus, k=cfg.candidate_k, where=cfg.where))
    if cfg.use_dense:
        try:
            dense = dense_search(query, cfg, k=cfg.candidate_k)
            ranked_lists.append([{"id": d["id"], "score": d["score"]} for d in dense])
        except (FileNotFoundError, RuntimeError):
            # Dense can't run *here* and we shouldn't crash the whole retriever for it:
            #   • FileNotFoundError → no vector partition built yet for this space
            #     (TODO: build it via the embedding stage before enabling dense), or
            #   • RuntimeError → a hosted embedding model with no API key on this machine.
            # Either way we degrade to BM25-only so the free/offline path keeps working. (A genuine
            # bug like a bad model name still surfaces as ValueError — we deliberately don't swallow it.)
            pass

    if not ranked_lists:
        return []                                   # nothing enabled / available → empty, honestly

    # ---- fuse with RRF (or pass through if only one retriever ran) -----------
    if len(ranked_lists) == 1:
        fused = [{"id": r["id"], "score": r["score"], "components": {"single": {"score": r["score"]}}}
                 for r in ranked_lists[0]]
    else:
        fused = reciprocal_rank_fusion(ranked_lists, k=cfg.rrf_k)

    # Build Hits for a generous slice so rerank has a real pool to work with, then truncate.
    slice_n = max(cfg.top_k, cfg.rerank_pool if cfg.use_rerank else cfg.top_k)
    hits = _hits_from_fused(fused, corpus, top_k=slice_n)

    # ---- optional cross-encoder rerank --------------------------------------
    if cfg.use_rerank:
        hits = rerank_hits(query, hits, cfg)

    return hits[: cfg.top_k]


# ============================================================================
# route_and_retrieve — the adaptive top-level: router + transforms + retrieve + grade
# ============================================================================
def route_and_retrieve(query: str, cfg: Optional[RetrievalConfig] = None,
                       kinds: Optional[list] = None) -> dict:
    """The adaptive entry point: route → (optionally) transform → retrieve+fuse → grade.

    This wires the whole brain together the way the agent would call it:
      1. ROUTE the query (free heuristic unless cfg.use_llm_router). The route may *suggest*
         multi-query for complex asks; we honor it unless the caller already forced the toggle.
      2. TRANSFORM: if HyDE is on, retrieve using the hypothetical answer; if multi-query is on (or
         suggested), retrieve for several phrasings and RRF-fuse their results (RAG-Fusion).
      3. RETRIEVE + fuse via `retrieve` (which itself does BM25+dense+RRF+optional rerank).
      4. GRADE the final set (free Self-RAG/CRAG signal) so the generator knows whether to answer,
         caveat, or abstain.

    Every model touchpoint (router/HyDE/multi-query/rerank) is individually gated and cost-logged;
    with default cfg this whole function makes ZERO paid calls.

    Args:
        query: The user's raw question.
        cfg: RetrievalConfig (None → free defaults).
        kinds: Optional chunk-kind restriction.

    Returns:
        {"query", "route", "queries_used", "hits": [Hit.to_dict()...], "grade"} — a complete,
        serializable retrieval result with provenance and a trust verdict.
    """
    cfg = cfg or RetrievalConfig()

    # ---- 1. route -----------------------------------------------------------
    route = route_query(query, cfg)
    # IMPORTANT: the router only *recommends* (route["suggest"]); it never auto-spends. Multi-query
    # is a PAID LLM call, so we run it ONLY when the caller explicitly set cfg.use_multiquery. (If
    # cfg.auto_apply_route is on, the caller has opted into letting the router escalate to paid
    # transforms — that's how an agent flips on multi-query for complex asks without a human.) This
    # keeps the DEFAULT/free path at exactly zero paid calls even on a "complex" query.
    use_mq = cfg.use_multiquery or (cfg.auto_apply_route and route["suggest"].get("use_multiquery", False))

    # ---- 2. choose the query/queries to actually retrieve with --------------
    queries_used: list[str] = []
    if use_mq:
        queries_used = multi_queries(query, cfg)            # PAID (gated by reaching this branch)
    elif cfg.use_hyde:
        queries_used = [hyde_query(query, cfg)]             # PAID; embed the hypothetical answer
    else:
        queries_used = [query]                              # FREE default

    # ---- 3. retrieve per query, then RRF-fuse across queries (RAG-Fusion) ---
    if len(queries_used) == 1:
        hits = retrieve(queries_used[0], cfg, kinds=kinds)
    else:
        per_query_lists = []
        for q in queries_used:
            qhits = retrieve(q, cfg, kinds=kinds)
            per_query_lists.append([{"id": h.id, "score": h.score} for h in qhits])
        fused = reciprocal_rank_fusion(per_query_lists, k=cfg.rrf_k)
        hits = _hits_from_fused(fused, _load_corpus(kinds), top_k=cfg.top_k)
        # Re-attach a rerank pass over the cross-query fusion if requested (sharper final order).
        if cfg.use_rerank:
            hits = rerank_hits(query, hits, cfg)[: cfg.top_k]

    # ---- 4. grade (free Self-RAG/CRAG verdict) ------------------------------
    grade = grade_context(query, hits)

    return {"query": query, "route": route, "queries_used": queries_used,
            "hits": [h.to_dict() for h in hits], "grade": grade}


# ============================================================================
# CLI smoke test — FREE path only (no paid call, no model download)
# ============================================================================
# Run `python -m lib.retrieval "your question"` to sanity-check BM25 + RRF + grading against the
# real corpus. Dense retrieval only kicks in if a local embedding partition already exists; if not,
# it silently degrades to lexical-only so this stays a zero-cost smoke test.
if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else "What favors were reported for cancer?"
    free_cfg = RetrievalConfig(use_dense=bool(vectorstore.spaces()))   # dense only if a space exists
    result = route_and_retrieve(q, free_cfg)
    print(f"QUERY: {q}")
    print(f"ROUTE: {result['route']['complexity']} — {result['route']['reason']}")
    print(f"GRADE: {result['grade']['verdict']} (conf={result['grade']['confidence']}) "
          f"— {result['grade']['reason']}")
    print(f"SIGNALS: {result['grade']['signals']}")
    print("-" * 60)
    for i, h in enumerate(result["hits"], 1):
        prov = h["provenance"]
        cite = f"{prov['doc_id']} rid={prov['rid']} pg={prov['page']}"
        print(f"[{i}] score={h['score']:.4f}  {h['kind']:<14} {cite}")
        print(f"     {h['text'][:120].replace(chr(10), ' ')}…")
    # Show the cost ledger so a "free" run visibly logged $0 (local) and nothing paid.
    print("=" * 60)
    print("COST SUMMARY:", json.dumps(costlog.summary(), indent=2))
