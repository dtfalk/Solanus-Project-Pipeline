#!/usr/bin/env python3
"""step_7/eval/eval.py — the RAG / answer evaluation harness (RAGAS + ARES tradition).

WHAT THIS IS, in plain language.
    Once you can retrieve passages and write a grounded answer, the very next question is the hard
    one: *is the answer any good, and which knobs make it better?* This file is the scoreboard. It
    takes a set of GOLDEN questions (golden_questions.json), runs each one through the real
    retrieval + (optional) generation stack, and scores the result on the four metrics the RAG-eval
    literature has settled on — then it does that for **every combination** of the three swappable
    model axes (LLM x embedding-space x reranker) so you can read off, empirically, which
    configuration to ship.

    The metrics, with the intuition first (this is the RAGAS/ARES playbook):

      • FAITHFULNESS — "does the answer actually follow from the retrieved context, or did the model
        make something up?" We DECOMPOSE the answer into atomic claims, then check each claim
        against the retrieved passages. faithfulness = supported_claims / all_claims. In an archive,
        an unsupported claim is the cardinal sin, so this is the headline number. (Threshold 0.75.)

      • CONTEXT PRECISION — "of the passages we retrieved, what fraction are actually relevant?"
        High precision means we're not drowning the generator in noise. (Threshold 0.70.)

      • CONTEXT RECALL — "of the passages we SHOULD have retrieved, what fraction did we get?"
        High recall means we're not missing the evidence. The classic precision/recall tension.
        (Threshold 0.80.)

      • ANSWER RELEVANCY — "does the answer actually address the question that was asked?" (vs. being
        true-but-off-topic). RAGAS estimates this by asking a model to generate questions the answer
        *would* answer, then measuring their embedding similarity to the real question.

    TWO GRADING BACKENDS for every metric — and the FREE one is the default:
      • FREE / heuristic (no model, no key, runs now): claim-splitting by sentence + lexical-overlap
        support; precision/recall against hand-labeled relevant_doc_ids OR an expected_where slice;
        relevancy by query<->answer token overlap. This is a *proxy* — coarser than an LLM judge —
        but it is deterministic, costs $0, and is enough to A/B retrieval configs offline.
      • LLM-JUDGE (RAGAS-faithful, PAID, GATED): an LLM decomposes claims and judges support /
        relevance, exactly like RAGAS/ARES. Every judge call routes through lib.costlog. It is OFF
        unless you pass --use-llm-judge AND --i-accept-cost, so importing or running this file never
        bills David. (See `LLM JUDGE` section + the COST gate in main().)

    OBSERVABILITY (Arize Phoenix / OpenInference). RAGAS and ARES both ride on OpenTelemetry traces;
    Arize Phoenix is the open-source viewer for them. We emit a Phoenix-compatible span per
    (question x config) when `arize-phoenix` + `openinference-instrumentation` are installed and
    --phoenix is passed — otherwise we no-op, so the harness has zero hard dependency on it. See
    `maybe_start_phoenix()` for the (documented, deferred) wiring.

NON-DESTRUCTIVE. Reads golden_questions.json + the local index that earlier stages built; writes a
single new artifact, data/eval_results.json (per-config metric tables + a threshold verdict). It
never edits the corpus or the gold set.

USAGE
    python eval/eval.py                         # FREE: heuristic metrics, default config only
    python eval/eval.py --sweep                 # FREE: sweep all {llm? x embed x rerank} combos*
    python eval/eval.py --generate              # also GENERATE answers (PAID LLM) — needs --i-accept-cost
    python eval/eval.py --use-llm-judge --i-accept-cost   # RAGAS-faithful LLM judging (PAID)
    python eval/eval.py --synthesize 20 --i-accept-cost   # mint synthetic golden Qs (PAID, gated)
    python eval/eval.py --phoenix               # also stream OpenInference spans to Arize Phoenix

    (* the EMBEDDING axis of the sweep only includes spaces that already exist in the local vector
       store, and dense retrieval is auto-skipped for a space whose partition isn't built — so the
       sweep stays on the FREE/offline path unless you explicitly enable a hosted model + key.)
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped (stdlib / third-party / local) the way every step_7 module is
# ============================================================================

# Core Python Imports
import argparse
import itertools
import json
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

# Third-Party Imports
import numpy as np

# Local File Imports — the same sys.path bootstrap every step_7 module uses, so `import config` and
# `from lib import ...` resolve no matter which directory the harness is launched from. eval/ is one
# level below step_7, so the root is parents[1] (exactly like lib/chunks.py does it).
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                              # noqa: E402
from lib import chunks as _chunks          # noqa: E402
from lib import costlog                    # noqa: E402
from lib import retrieval                  # noqa: E402
from lib import vectorstore                # noqa: E402
from lib.providers import embed            # noqa: E402
from lib.providers import llm              # noqa: E402

# ----------------------------------------------------------------- paths + thresholds
EVAL_DIR = STEP7 / "eval"
GOLDEN_PATH = EVAL_DIR / "golden_questions.json"
RESULTS_PATH = config.DATA / "eval_results.json"

# The pass/fail bar for each metric, straight from RESEARCH_PLAN.md (Part E). A config "passes" only
# if its mean score clears EVERY threshold. These are intentionally strict — this is an archive, so
# we would rather flag a borderline config than ship a confabulating one.
THRESHOLDS = {
    "faithfulness":      0.75,   # supported claims / all claims  (the headline number)
    "context_precision": 0.70,   # relevant retrieved / all retrieved
    "context_recall":    0.80,   # relevant retrieved / all relevant
    "answer_relevancy":  0.70,   # answer-on-topic for the question (no hard threshold in the plan;
                                 #   we set a sensible default so it can gate too)
}

# A reusable token splitter (mirrors retrieval._tokenize so the heuristic graders agree with the
# retriever about what a "word" is — apostrophes for O'Donnell, digits for years like 1933).
_TOKEN_RE = re.compile(r"[A-Za-z0-9']+")
# Very common words carry no retrieval signal; ignoring them makes the lexical-overlap proxy far
# less generous (so a "supported" verdict really means content words matched, not just "the/and").
_STOP = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "was", "were", "is",
    "are", "be", "by", "at", "as", "it", "that", "this", "his", "her", "him", "she", "he", "they",
    "who", "what", "when", "where", "which", "did", "do", "does", "had", "has", "have", "from",
    "about", "all", "any", "some", "there",
}


def _tokens(text: str) -> set:
    """Lowercased content tokens (stopwords dropped). Used by every FREE/heuristic grader so they
    judge on words that actually carry meaning."""
    return {t for t in _TOKEN_RE.findall((text or "").lower()) if t not in _STOP and len(t) > 1}


# ============================================================================
# Golden set — load the hand-written archival questions (the trusted yardstick)
# ============================================================================
def load_golden(path: Path = GOLDEN_PATH) -> list[dict]:
    """Load golden_questions.json -> the list of question dicts.

    The file has a "_README" key (documentation) and a "questions" list; we return just the list.
    Each item is the schema documented in golden_questions.json (id, question, expected_where,
    relevant_doc_ids, etc.). Pure read, no model call.

    Args:
        path: Path to the golden-question JSON (defaults to eval/golden_questions.json).

    Returns:
        list[dict] of question records.
    """
    data = json.loads(path.read_text())
    return data.get("questions", [])


# ============================================================================
# The thing under test — retrieve (free) + optionally generate a grounded answer
# ============================================================================
# A RAG system has two halves: RETRIEVE the evidence, then GENERATE the answer from it. The eval
# scores both, so for each question we run retrieval (always — it's free) and, only if asked + paid,
# generate an answer to score for faithfulness/relevancy. With generation OFF we still score the
# RETRIEVAL metrics (precision/recall), which is exactly what you want for an A/B of retrieval knobs.
def _grounded_answer_prompt(question: str, hits: list) -> tuple[str, str]:
    """Build the (system, user) prompt for STRICTLY-GROUNDED, cited answer generation.

    The whole point of an archival assistant is that it answers ONLY from the retrieved regions and
    cites them — so the prompt forbids outside knowledge and demands a citation per claim, and tells
    the model to abstain when the context doesn't contain the answer. (This mirrors the abstain
    behavior the retrieval grader already recommends in lib.retrieval.grade_context.)

    Args:
        question: The user's question.
        hits: The retrieved context (list of retrieval.Hit, or their .to_dict() form).

    Returns:
        (system_instruction, user_prompt) strings.
    """
    system = (
        "You are an archival assistant for the letters and notebooks of Father Solanus Casey "
        "(1920s-1950s). Answer ONLY from the provided context passages. Do not use outside "
        "knowledge. Cite the source of every claim as [doc_id]. If the context does not contain "
        "the answer, reply exactly: 'I could not find this in the archive.'"
    )
    blocks = []
    for i, h in enumerate(hits, 1):
        # Accept either a Hit object or its dict form, so callers can pass whichever they have.
        text = h.text if hasattr(h, "text") else h.get("text", "")
        meta = h.meta if hasattr(h, "meta") else h.get("meta", {})
        doc_id = meta.get("doc_id", "?")
        blocks.append(f"[{i}] (doc_id={doc_id})\n{text}")
    context = "\n\n".join(blocks) if blocks else "(no passages retrieved)"
    user = f"Context passages:\n{context}\n\nQuestion: {question}\n\nGrounded, cited answer:"
    return system, user


def generate_answer(question: str, hits: list, llm_model: Optional[str] = None) -> str:
    """Generate a grounded answer from retrieved context (PAID LLM call, cost-logged in llm.generate).

    Only called when the caller has opted into generation (`--generate --i-accept-cost`). The single
    LLM call is logged to costs/usage.csv exactly like any other generation in the project.

    Args:
        question: The question to answer.
        hits: Retrieved context (Hit objects or dicts).
        llm_model: Which generation model (None -> config.DEFAULTS["llm"]).

    Returns:
        The grounded answer text (may be the abstain sentence).
    """
    system, user = _grounded_answer_prompt(question, hits)
    text, _usage = llm.generate(user, model=llm_model, system=system, temperature=0.1)
    return text.strip()


# ============================================================================
# METRIC 1 — FAITHFULNESS (claim decomposition -> support check)  [RAGAS core]
# ============================================================================
# The RAGAS recipe: break the answer into atomic CLAIMS, then ask "is each claim entailed by the
# retrieved context?". The score is the fraction supported. We implement BOTH backends:
#   • FREE: split the answer into sentence-claims; a claim is "supported" if a large enough share of
#     its content words appear in the union of the retrieved passages. Lexical entailment is a crude
#     stand-in for real entailment, but it reliably catches the failure mode we care about most —
#     an answer that talks about things NOT in the context (hallucination).
#   • LLM: ask the judge model to extract claims and label each supported/unsupported (true RAGAS).
def split_claims(answer: str) -> list[str]:
    """Decompose an answer into atomic claims (FREE heuristic = sentence segmentation).

    A 'claim' in RAGAS is one verifiable statement. The faithful (LLM) decomposition is finer than
    sentences, but sentences are a serviceable free proxy: each is checked independently, so an
    answer that mixes one grounded and one ungrounded sentence is correctly scored ~0.5.

    Args:
        answer: The generated answer text.

    Returns:
        list[str] of claim strings (sentences, trimmed; empty/degenerate ones dropped).
    """
    # Split on sentence-ending punctuation followed by whitespace; keep it deterministic + stdlib.
    parts = re.split(r"(?<=[.!?])\s+", (answer or "").strip())
    return [p.strip() for p in parts if len(_tokens(p)) >= 1]


def faithfulness_free(answer: str, hits: list, support_threshold: float = 0.5) -> dict:
    """FREE faithfulness: fraction of answer-claims whose content words are covered by the context.

    For each claim we compute |claim_words ∩ context_words| / |claim_words|; the claim counts as
    SUPPORTED when that coverage clears `support_threshold`. The score is supported/total.

    Args:
        answer: The generated answer.
        hits: Retrieved context (Hit objects or dicts).
        support_threshold: Coverage fraction above which a claim is judged supported.

    Returns:
        {"score": float, "n_claims": int, "supported": int, "per_claim": [...]} — per_claim keeps
        the coverage so a low score is debuggable.
    """
    claims = split_claims(answer)
    if not claims:
        # No answer (e.g. generation was off, or the model returned nothing) -> faithfulness is
        # undefined; report None so it's excluded from means rather than counted as a 0.
        return {"score": None, "n_claims": 0, "supported": 0, "per_claim": []}

    # The "context" is the union of every retrieved passage's words — what the answer is allowed to
    # lean on. We build it once.
    ctx_words: set = set()
    for h in hits:
        text = h.text if hasattr(h, "text") else h.get("text", "")
        ctx_words |= _tokens(text)

    supported, per_claim = 0, []
    for c in claims:
        cw = _tokens(c)
        cov = (len(cw & ctx_words) / len(cw)) if cw else 0.0
        ok = cov >= support_threshold
        supported += int(ok)
        per_claim.append({"claim": c[:160], "coverage": round(cov, 3), "supported": ok})
    return {"score": supported / len(claims), "n_claims": len(claims),
            "supported": supported, "per_claim": per_claim}


def faithfulness_llm(answer: str, hits: list, llm_model: Optional[str] = None) -> dict:
    """RAGAS-faithful faithfulness via an LLM judge (PAID, gated, cost-logged in llm.generate).

    Asks the judge to (1) extract atomic claims from the answer and (2) label each as supported by
    the context or not. Score = supported / total. Falls back to the FREE grader on any parse error,
    so a flaky judge never crashes the sweep (and never silently scores 0).

    Args:
        answer: The generated answer.
        hits: Retrieved context.
        llm_model: Judge model (None -> config.DEFAULTS["llm"]).

    Returns:
        Same shape as faithfulness_free, plus "backend": "llm".
    """
    if not (answer or "").strip():
        return {"score": None, "n_claims": 0, "supported": 0, "per_claim": [], "backend": "llm"}
    context = "\n\n".join((h.text if hasattr(h, "text") else h.get("text", "")) for h in hits)
    system = (
        "You are a strict RAG faithfulness judge. Decompose the ANSWER into atomic factual claims, "
        "then decide for EACH whether it is directly supported by the CONTEXT. Reply ONLY as JSON: "
        "{\"claims\": [{\"claim\": \"...\", \"supported\": true|false}]}."
    )
    prompt = f"CONTEXT:\n{context[:8000]}\n\nANSWER:\n{answer}"
    try:
        text, _ = llm.generate(prompt, model=llm_model, system=system, json_mode=True, temperature=0.0)
        parsed = json.loads(text)
        claims = parsed.get("claims", [])
        if not claims:
            return faithfulness_free(answer, hits) | {"backend": "llm(empty->free)"}
        supported = sum(1 for c in claims if c.get("supported"))
        per = [{"claim": str(c.get("claim", ""))[:160], "supported": bool(c.get("supported"))}
               for c in claims]
        return {"score": supported / len(claims), "n_claims": len(claims),
                "supported": supported, "per_claim": per, "backend": "llm"}
    except Exception:
        # Judge unavailable / unparseable -> degrade to the deterministic free grader. We never let
        # the eval bill-and-then-crash; a missing judge just means a coarser, free number.
        return faithfulness_free(answer, hits) | {"backend": "llm(error->free)"}


# ============================================================================
# METRICS 2 & 3 — CONTEXT PRECISION & RECALL (how good was the *retrieval*?)
# ============================================================================
# Precision/recall need a notion of "which retrieved passages are RELEVANT". We support two levels:
#   • EXACT (best): the question carries hand-labeled `relevant_doc_ids` (David fills these in as he
#     verifies). Then precision = relevant∩retrieved / retrieved, recall = relevant∩retrieved /
#     relevant — the textbook definitions, no model needed.
#   • PROXY (default, free): no labels yet, so we use the `expected_where` slice as a soft relevance
#     oracle — "a retrieved hit is plausibly relevant if it falls inside the slice the question
#     should be answered from (e.g. kind=notebook_entry)". This can't measure recall against a true
#     denominator, so for recall we use the retrieval grader's lexical-overlap signal as a proxy.
# An optional LLM judge (gated) labels each retrieved passage relevant/not, RAGAS-style, when you
# want a sharper precision number without hand labels.
def context_precision_recall(question: str, hits: list, gold: dict,
                             use_llm_judge: bool = False,
                             llm_model: Optional[str] = None) -> dict:
    """Context precision + recall for one question's retrieved set.

    Args:
        question: The question (for the proxy/LLM relevance signal).
        hits: The retrieved Hits (or dicts) to grade.
        gold: The golden-question record (provides relevant_doc_ids and/or expected_where).
        use_llm_judge: If True, label each retrieved passage relevant/not via the LLM (PAID).
        llm_model: Judge/grader model.

    Returns:
        {"context_precision", "context_recall", "mode", "n_retrieved", "n_relevant_retrieved", ...}.
        Scores are floats in [0,1]; `mode` records which oracle was used so the number is honest.
    """
    retrieved_ids = [(h.meta if hasattr(h, "meta") else h.get("meta", {})).get("doc_id")
                     for h in hits]
    n = len(retrieved_ids)
    if n == 0:
        return {"context_precision": 0.0, "context_recall": 0.0, "mode": "no_hits",
                "n_retrieved": 0, "n_relevant_retrieved": 0}

    gold_ids = gold.get("relevant_doc_ids") or []

    # ---- EXACT mode: hand-labeled relevant doc ids exist -> textbook precision/recall ----
    if gold_ids:
        gold_set = set(gold_ids)
        hit_relevant = [d in gold_set for d in retrieved_ids]
        n_rel_ret = sum(hit_relevant)
        precision = n_rel_ret / n
        recall = n_rel_ret / len(gold_set)            # true denominator -> a REAL recall number
        return {"context_precision": round(precision, 4), "context_recall": round(recall, 4),
                "mode": "exact_labeled", "n_retrieved": n, "n_relevant_retrieved": n_rel_ret,
                "n_relevant_total": len(gold_set)}

    # ---- LLM-judge mode: ask the model which retrieved passages are relevant (PAID) ----
    if use_llm_judge:
        labels = [_judge_passage_relevant(question, h, llm_model) for h in hits]
        n_rel_ret = sum(labels)
        precision = n_rel_ret / n
        # Without a labeled total we can't form a true recall denominator; report the share of
        # judged-relevant among retrieved as a precision-proxy recall and flag the mode honestly.
        return {"context_precision": round(precision, 4),
                "context_recall": round(n_rel_ret / n, 4), "mode": "llm_judge",
                "n_retrieved": n, "n_relevant_retrieved": n_rel_ret}

    # ---- PROXY mode (FREE default): expected_where slice + lexical-overlap recall proxy ----
    where = gold.get("expected_where")
    if where:
        # A retrieved hit is "plausibly relevant" if its metadata matches the expected slice. We
        # reuse retrieval._passes_filter so this agrees exactly with how the retriever filters.
        in_slice = []
        for h in hits:
            chunk = {"kind": (h.kind if hasattr(h, "kind") else h.get("kind", "")),
                     "meta": (h.meta if hasattr(h, "meta") else h.get("meta", {}))}
            in_slice.append(retrieval._passes_filter(chunk, where))
        precision = sum(in_slice) / n
    else:
        # No slice constraint -> precision can't be bounded by metadata; use the lexical-overlap
        # signal (does the hit text actually share content words with the question?).
        precision = _lexical_relevance(question, hits)

    # Recall proxy: did the TOP retrieved passages cover the question's content words? This is the
    # same intuition lib.retrieval.grade_context uses; it correlates with "we found the evidence".
    recall = _lexical_recall_proxy(question, hits)
    return {"context_precision": round(precision, 4), "context_recall": round(recall, 4),
            "mode": "proxy_where" if where else "proxy_lexical",
            "n_retrieved": n, "n_relevant_retrieved": int(round(precision * n))}


def _lexical_relevance(question: str, hits: list) -> float:
    """Fraction of retrieved hits whose text shares >=1 content word with the question (free proxy)."""
    qw = _tokens(question)
    if not qw or not hits:
        return 0.0
    rel = 0
    for h in hits:
        text = h.text if hasattr(h, "text") else h.get("text", "")
        if qw & _tokens(text):
            rel += 1
    return rel / len(hits)


def _lexical_recall_proxy(question: str, hits: list, top: int = 5) -> float:
    """Proxy recall: fraction of the question's content words that appear in the top retrieved hits.

    Intuition: if we retrieved the right passages, the words that matter in the question (a name, a
    condition, a year) should show up in what we got back. It is a PROXY — it can't see passages we
    failed to retrieve — but it tracks the real failure ("we never surfaced the evidence") well.
    """
    qw = _tokens(question)
    if not qw:
        return 0.0
    covered: set = set()
    for h in hits[:top]:
        text = h.text if hasattr(h, "text") else h.get("text", "")
        covered |= (qw & _tokens(text))
    return len(covered) / len(qw)


def _judge_passage_relevant(question: str, hit, llm_model: Optional[str]) -> bool:
    """LLM relevance label for ONE retrieved passage (PAID; reuses retrieval.grade_document)."""
    cfg = retrieval.RetrievalConfig(llm_model=llm_model)
    h = hit if isinstance(hit, retrieval.Hit) else retrieval.Hit(
        id=hit.get("id", ""), score=hit.get("score", 0.0), text=hit.get("text", ""),
        kind=hit.get("kind", ""), meta=hit.get("meta", {}))
    return retrieval.grade_document(question, h, cfg).get("relevant", True)


# ============================================================================
# METRIC 4 — ANSWER RELEVANCY (is the answer on-topic for the question?)  [RAGAS]
# ============================================================================
# RAGAS's trick: ask a model to generate the questions this ANSWER would answer, then measure how
# close those are to the REAL question (embedding cosine). On-topic answers regenerate near-identical
# questions; off-topic answers don't. The FREE proxy skips the model and uses query<->answer content
# overlap directly — coarser, but $0 and good enough to rank configs.
def answer_relevancy_free(question: str, answer: str) -> Optional[float]:
    """FREE answer-relevancy proxy = content-word overlap between question and answer.

    Returns None when there's no answer to judge (so it's excluded from means, not counted as 0).
    """
    if not (answer or "").strip():
        return None
    qw, aw = _tokens(question), _tokens(answer)
    if not qw:
        return None
    # Coverage of the QUESTION's content words by the answer — "did the answer engage the ask?".
    return round(len(qw & aw) / len(qw), 4)


def answer_relevancy_llm(question: str, answer: str, llm_model: Optional[str] = None,
                         n_gen: int = 3, embed_model: Optional[str] = None,
                         embed_dim: Optional[int] = None) -> Optional[float]:
    """RAGAS-faithful answer-relevancy (PAID, gated): generate questions the answer implies, embed
    them + the real question, average the cosine similarity.

    Both the generation and the embedding are paid + cost-logged. Falls back to the free proxy on
    any error. Off unless the caller wired --use-llm-judge (this is the answer-side LLM metric).

    Args:
        question: The real question.
        answer: The generated answer.
        llm_model: Model that back-generates questions.
        n_gen: How many questions to generate (RAGAS uses a few, then averages).
        embed_model/embed_dim: Embedding space for the similarity (None -> config defaults).

    Returns:
        Mean cosine similarity in roughly [0,1], or None if there's nothing to judge.
    """
    if not (answer or "").strip():
        return None
    em = embed_model or config.DEFAULTS["embedding"][0]
    ed = embed_dim or config.DEFAULTS["embedding"][1]
    system = ("Given an ANSWER, output ONLY a JSON array of the "
              f"{n_gen} most likely questions it answers. No prose.")
    try:
        text, _ = llm.generate(f"ANSWER:\n{answer}", model=llm_model, system=system,
                               json_mode=True, temperature=0.3)
        gen_qs = [str(q).strip() for q in json.loads(text) if str(q).strip()]
        if not gen_qs:
            return answer_relevancy_free(question, answer)
        # Embed the real question + the back-generated ones in one space; cosine each gen->real.
        vecs, _ = embed.embed_texts([question, *gen_qs], em, ed, task="query")
        qv, gen_v = vecs[0], vecs[1:]
        sims = [float(np.dot(qv, g)) for g in gen_v]      # vectors are L2-normalized -> dot == cosine
        return round(sum(sims) / len(sims), 4)
    except Exception:
        return answer_relevancy_free(question, answer)


# ============================================================================
# Per-question scoring — run retrieval (+gen) for ONE config and score all metrics
# ============================================================================
@dataclass
class EvalOptions:
    """Everything that controls HOW the eval runs (separate from WHICH model config it sweeps).

    The cost gates live here so there is exactly one place that decides whether a paid call may fire,
    and `main()` is the only thing that flips them on (after seeing --i-accept-cost).
    """
    generate: bool = False          # generate grounded answers (PAID) -> enables faithfulness/relevancy
    use_llm_judge: bool = False     # RAGAS-faithful LLM judging of claims/passages/relevancy (PAID)
    phoenix: bool = False           # stream OpenInference spans to Arize Phoenix if installed
    cost_accepted: bool = False     # HARD gate: no paid call fires unless this is True


def score_question(gold: dict, rcfg: retrieval.RetrievalConfig, opts: EvalOptions) -> dict:
    """Run the full pipeline for ONE golden question under ONE retrieval config and score it.

    Steps: route+retrieve (free) -> optionally generate a grounded answer (PAID, gated) -> score
    faithfulness, context precision/recall, answer relevancy (free proxies by default; LLM judge if
    gated on). Returns one tidy record per question for the results table.

    Args:
        gold: The golden-question record.
        rcfg: The RetrievalConfig (embedding space etc.) being evaluated.
        opts: EvalOptions (cost gates + generation/judge toggles).

    Returns:
        A dict with the question id, the retrieval grade, the four metrics, and debug fields.
    """
    question = gold["question"]
    kinds = None  # the question's expected_where already constrains kind if it wants to; let the
                  #   retriever see the whole corpus so precision is a fair test.

    # ---- 1. RETRIEVE (free path; dense auto-skips if the space isn't built) ----
    result = retrieval.route_and_retrieve(question, rcfg, kinds=kinds)
    hits = result["hits"]                                # list of Hit.to_dict()

    # ---- 2. (optional) GENERATE a grounded answer — PAID, only if accepted ----
    answer = ""
    judge = opts.use_llm_judge and opts.cost_accepted
    if opts.generate and opts.cost_accepted:
        answer = generate_answer(question, hits, llm_model=rcfg.llm_model)

    # ---- 3. SCORE the four metrics ----
    if judge and answer:
        faith = faithfulness_llm(answer, hits, llm_model=rcfg.llm_model)
        relevancy = answer_relevancy_llm(question, answer, llm_model=rcfg.llm_model,
                                         embed_model=rcfg.embedding_model, embed_dim=rcfg.embedding_dim)
    else:
        faith = faithfulness_free(answer, hits)
        relevancy = answer_relevancy_free(question, answer)
    cpr = context_precision_recall(question, hits, gold,
                                   use_llm_judge=judge, llm_model=rcfg.llm_model)

    # Emit an observability span (no-op unless Phoenix is wired + installed + enabled).
    _emit_span(question, gold, rcfg, hits, answer,
               {"faithfulness": faith.get("score"), **cpr, "answer_relevancy": relevancy})

    return {
        "id": gold.get("id"),
        "question": question,
        "difficulty": gold.get("difficulty"),
        "route": result["route"]["complexity"],
        "grade": result["grade"]["verdict"],            # the retriever's own answer/caveat/abstain
        "n_hits": len(hits),
        "answer": answer,                               # "" when generation is off
        "metrics": {
            "faithfulness":      faith.get("score"),
            "context_precision": cpr["context_precision"],
            "context_recall":    cpr["context_recall"],
            "answer_relevancy":  relevancy,
        },
        "detail": {"faithfulness": faith, "context": cpr},
        "retrieved_doc_ids": [h["meta"].get("doc_id") for h in hits],
    }


# ============================================================================
# The SWEEP — score every {llm x embedding-space x reranker} combination
# ============================================================================
# This is the whole reason the three model axes are config variables: we can ask, empirically, which
# combination wins on OUR golden set. The sweep builds a RetrievalConfig per cell of the grid, runs
# the golden set through it, and aggregates. To keep the DEFAULT run FREE and offline we (a) only
# include embedding spaces that already exist in the local vector store, and (b) only iterate the LLM
# axis when generation/judging (paid) is on — otherwise the LLM doesn't affect the (free) metrics, so
# sweeping it would be wasted work.
def build_sweep(opts: EvalOptions, embed_only: Optional[list] = None,
                rerankers: Optional[list] = None, llms: Optional[list] = None) -> list[dict]:
    """Construct the list of {llm, embedding, reranker} cells to evaluate.

    Args:
        opts: EvalOptions — decides whether the LLM axis matters (only if paid steps are on).
        embed_only: Restrict the embedding axis to these "model@dim" spaces (default: those present
            in the local vector store; falls back to config.DEFAULTS if none are built yet).
        rerankers: Restrict the reranker axis (default: the LOCAL/free reranker only, so the sweep
            stays $0; pass paid reranker names explicitly to include them).
        llms: Restrict the LLM axis (default: config default only).

    Returns:
        list of {"llm", "embedding_model", "embedding_dim", "reranker", "use_rerank"} cells.
    """
    # --- embedding axis: prefer built spaces so dense retrieval actually runs ---
    built = vectorstore.spaces()                          # e.g. ["gemini-embedding-001@768", ...]
    if embed_only:
        spaces = embed_only
    elif built:
        spaces = built
    else:
        # Nothing embedded yet -> evaluate the default space name; dense will gracefully skip and the
        # sweep measures BM25-only retrieval (still a meaningful, free baseline).
        spaces = [f"{config.DEFAULTS['embedding'][0]}@{config.DEFAULTS['embedding'][1]}"]

    # --- reranker axis: free local only by default (paid rerankers must be opted in) ---
    if rerankers is None:
        rerankers = [r for r, m in config.RERANKERS.items() if m["provider"] == "local"] or \
                    [config.DEFAULTS["reranker"]]

    # --- llm axis: only matters when a paid step (generate/judge) is on ---
    if llms is None:
        if (opts.generate or opts.use_llm_judge) and opts.cost_accepted:
            llms = list(config.LLMS.keys())
        else:
            llms = [config.DEFAULTS["llm"]]              # free metrics don't depend on the LLM

    cells = []
    for lm, space, rr in itertools.product(llms, spaces, rerankers):
        model, _, dim = space.rpartition("@")
        cells.append({
            "llm":             lm,
            "embedding_model": model or config.DEFAULTS["embedding"][0],
            "embedding_dim":   int(dim) if dim.isdigit() else config.DEFAULTS["embedding"][1],
            "reranker":        rr,
            # Turn reranking ON in the sweep so the reranker axis is actually exercised. The local
            # reranker is free; a paid reranker only fires if the caller put it in `rerankers`.
            "use_rerank":      True,
        })
    return cells


def _dense_is_free(embedding_model: str, cost_accepted: bool) -> bool:
    """Decide whether DENSE retrieval may run for this embedding space without spending money.

    Subtle but important: dense retrieval embeds the *query* in the chosen space on every question.
    For a LOCAL embedding model (bge-*) that is $0. For a HOSTED model (gemini/openai/voyage) it is a
    PAID call — small per query, but real, and the project rule is "no paid call unless asked". So:
      • local space  -> always free -> dense ON.
      • hosted space -> dense ON only if the caller accepted cost (--i-accept-cost); otherwise we run
        BM25-only for that cell. (Lexical-only is still a meaningful, free retrieval baseline.)
    This is exactly why a default `python eval/eval.py` bills exactly $0 even though the default
    config's space is a hosted Gemini one.
    """
    provider = config.EMBEDDINGS.get(embedding_model, {}).get("provider")
    return provider == "local" or cost_accepted


def _cfg_for_cell(cell: dict, opts: EvalOptions) -> retrieval.RetrievalConfig:
    """Translate one sweep cell into a RetrievalConfig (the toggles the retriever understands)."""
    # Dense only helps if the partition exists; retrieve() degrades to BM25 if not. We additionally
    # gate it on cost: a hosted embedding space embeds the query (PAID) unless cost is accepted, so
    # the FREE path stays $0 by falling back to BM25-only for hosted spaces. Everything else paid
    # (HyDE/multiquery/LLM-router) stays OFF here regardless.
    use_dense = _dense_is_free(cell["embedding_model"], opts.cost_accepted)
    return retrieval.RetrievalConfig(
        embedding_model = cell["embedding_model"],
        embedding_dim   = cell["embedding_dim"],
        reranker        = cell["reranker"],
        use_rerank      = cell["use_rerank"],
        llm_model       = cell["llm"],
        use_dense       = use_dense,
        use_bm25        = True,
    )


def aggregate(per_question: list[dict]) -> dict:
    """Average each metric across questions (ignoring None = "not measured"), and apply thresholds.

    Args:
        per_question: The list of score_question() records for ONE config.

    Returns:
        {"means": {metric: float|None}, "n": int, "passes": {metric: bool|None},
         "passed_all": bool} — the at-a-glance verdict for a config.
    """
    means, passes = {}, {}
    for metric in ("faithfulness", "context_precision", "context_recall", "answer_relevancy"):
        vals = [q["metrics"][metric] for q in per_question if q["metrics"][metric] is not None]
        mean = round(sum(vals) / len(vals), 4) if vals else None
        means[metric] = mean
        thr = THRESHOLDS.get(metric)
        passes[metric] = (mean >= thr) if (mean is not None and thr is not None) else None
    # "passed_all" only considers metrics we actually measured (None metrics don't fail a config —
    # e.g. faithfulness is None when generation was off, and that shouldn't sink a retrieval-only run).
    measured = [v for v in passes.values() if v is not None]
    return {"means": means, "n": len(per_question),
            "passes": passes, "passed_all": bool(measured) and all(measured)}


def run_eval(opts: EvalOptions, sweep: bool = False, limit: Optional[int] = None,
             embed_only: Optional[list] = None, rerankers: Optional[list] = None,
             llms: Optional[list] = None) -> dict:
    """Top-level: score the golden set under one config (default) or the full sweep, then write JSON.

    Args:
        opts: EvalOptions (cost gates + toggles).
        sweep: If True, evaluate every {llm x embedding x reranker} cell; else just the default cfg.
        limit: Optionally cap the number of golden questions (smoke testing).
        embed_only/rerankers/llms: Restrict the sweep axes (see build_sweep).

    Returns:
        The full results dict (also written to data/eval_results.json).
    """
    golden = load_golden()
    if limit:
        golden = golden[:limit]

    if sweep:
        cells = build_sweep(opts, embed_only=embed_only, rerankers=rerankers, llms=llms)
    else:
        d = config.DEFAULTS
        cells = [{"llm": d["llm"], "embedding_model": d["embedding"][0],
                  "embedding_dim": d["embedding"][1], "reranker": d["reranker"],
                  "use_rerank": False}]

    configs_out = []
    for cell in cells:
        rcfg = _cfg_for_cell(cell, opts)
        label = f"{cell['llm']} | {cell['embedding_model']}@{cell['embedding_dim']} | {cell['reranker']}"
        per_q = [score_question(g, rcfg, opts) for g in golden]
        configs_out.append({"config": cell, "label": label,
                            "aggregate": aggregate(per_q), "per_question": per_q})

    # Rank configs by faithfulness (headline) then context_recall, putting the best first. None means
    # "not measured" -> sort it last so a measured config always outranks an unmeasured one.
    def _key(c):
        m = c["aggregate"]["means"]
        return (m["faithfulness"] if m["faithfulness"] is not None else -1.0,
                m["context_recall"] if m["context_recall"] is not None else -1.0)
    configs_out.sort(key=_key, reverse=True)

    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "config_fingerprint": config.fingerprint(),
        "thresholds": THRESHOLDS,
        "options": asdict(opts),
        "n_questions": len(golden),
        "n_configs": len(configs_out),
        "configs": configs_out,
        "cost_summary": costlog.summary(),               # what THIS run actually billed (often $0)
    }
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    return out


# ============================================================================
# SYNTHETIC golden-question generator  (ARES-style)  — GATED, NOT RUN BY DEFAULT
# ============================================================================
# ARES and RAGAS both bootstrap an eval set by SYNTHESIZING questions from the corpus: sample a
# passage, ask an LLM to write a question that the passage answers, and you instantly have a (q,
# relevant-passage) pair. It is a force-multiplier for coverage — BUT, per project rule, David's
# labels are the authority, so synthetic questions NEVER silently join the gold set. This function:
#   • is PAID (an LLM call per sampled passage) and GATED behind --synthesize N --i-accept-cost,
#   • writes to a SEPARATE file (eval/synthetic_questions.json), clearly marked machine-generated,
#     for David to review and promote by hand. It does not touch golden_questions.json.
def synthesize_golden_questions(n: int, opts: EvalOptions,
                                llm_model: Optional[str] = None,
                                seed: int = 0) -> list[dict]:
    """Generate N synthetic archival questions from sampled corpus passages (PAID, GATED).

    For each sampled chunk we ask the LLM for one specific question the passage answers, and we
    record the source doc_id as the (machine-proposed) relevant_doc_id. The output mirrors the
    golden-question schema so a reviewed item can be pasted straight into golden_questions.json — but
    we write it to a SEPARATE review file and never auto-merge (gold is David-only).

    Args:
        n: How many questions to synthesize.
        opts: EvalOptions (the cost gate must be accepted).
        llm_model: Generation model (None -> config default).
        seed: RNG seed for the strided corpus sample (reproducible selection).

    Returns:
        list[dict] of synthetic question records (also written to eval/synthetic_questions.json).

    Raises:
        RuntimeError: If the cost gate isn't accepted (so this can never bill by accident).
    """
    if not opts.cost_accepted:
        raise RuntimeError(
            "synthesize_golden_questions is a PAID LLM step and is GATED. Re-run with "
            "--synthesize N --i-accept-cost once you've decided to spend. (Per project rule, the "
            "result is written to eval/synthetic_questions.json for David to review — it is NEVER "
            "merged into the golden set automatically.)")

    # Strided sample across the whole corpus so questions span letters AND notebook entries.
    corpus = _chunks.build_chunks()
    rng = np.random.default_rng(seed)
    idx = sorted(rng.choice(len(corpus), size=min(n, len(corpus)), replace=False).tolist())
    system = (
        "You write ONE specific, answerable question that the given archival passage (from Father "
        "Solanus Casey's letters/notebooks) directly answers. Prefer questions about people, places, "
        "dates, conditions, or reported favors. Reply ONLY as JSON: {\"question\": \"...\"}."
    )
    out = []
    for i in idx:
        c = corpus[i]
        try:
            text, _ = llm.generate(f"PASSAGE:\n{c['text'][:1500]}", model=llm_model,
                                   system=system, json_mode=True, temperature=0.4)
            q = json.loads(text).get("question", "").strip()
        except Exception:
            q = ""
        if not q:
            continue
        out.append({
            "id": f"syn_{c['meta'].get('doc_id', i)}",
            "question": q,
            "category": "synthetic",
            "difficulty": "moderate",
            "ground_truth": c["text"][:300],            # the source passage is the reference answer
            "expected_where": {"kind": c["kind"]},
            "relevant_doc_ids": [c["meta"].get("doc_id")],   # MACHINE-PROPOSED — review before trusting
            "stresses": "synthetic_coverage",
            "_machine_generated": True,
            "_source_chunk_id": c["id"],
        })
    review_path = EVAL_DIR / "synthetic_questions.json"
    review_path.write_text(json.dumps(
        {"_README": "MACHINE-GENERATED — review and hand-promote into golden_questions.json. "
                    "Gold is David-only; nothing here is trusted until reviewed.",
         "questions": out}, indent=2, ensure_ascii=False))
    return out


# ============================================================================
# Arize Phoenix / OpenInference observability — documented, deferred, no-op safe
# ============================================================================
# RAGAS + ARES emit OpenTelemetry traces; Arize Phoenix is the open-source UI that reads them, and
# OpenInference is the semantic convention for LLM/retrieval spans. Wiring it lets you SEE every
# retrieval and judgment in a timeline (embedding drift, per-query relevance, which passages a config
# retrieved) instead of only the aggregate numbers. We keep it OPTIONAL: if the packages aren't
# installed or --phoenix wasn't passed, both helpers below are safe no-ops, so the harness has no
# hard dependency on a tracing stack.
_PHOENIX = {"on": False, "tracer": None}


def maybe_start_phoenix(opts: EvalOptions) -> None:
    """Start a local Arize Phoenix session + OpenInference tracer IF --phoenix and the deps exist.

    The real wiring (documented; intentionally not auto-installed — it pulls otel + phoenix):

        # TODO(phoenix): enable the OpenInference/OTel trace pipeline once these are installed:
        #   pip install arize-phoenix openinference-instrumentation opentelemetry-sdk
        import phoenix as px
        from phoenix.otel import register
        px.launch_app()                                  # local UI at http://localhost:6006
        tracer_provider = register(project_name="solanus-rag-eval")
        # Auto-instrument the providers we use, so every llm/embed/rerank call becomes a span:
        #   from openinference.instrumentation.litellm import LiteLLMInstrumentor  # or per-SDK
        #   LiteLLMInstrumentor().instrument(tracer_provider=tracer_provider)
        _PHOENIX["tracer"] = tracer_provider.get_tracer(__name__)
        _PHOENIX["on"] = True

    We attempt it softly: any ImportError just leaves Phoenix off (the eval still runs + writes JSON).

    Args:
        opts: EvalOptions (only acts when opts.phoenix is True).
    """
    if not opts.phoenix:
        return
    try:
        import phoenix as px                              # noqa: F401
        from phoenix.otel import register                 # type: ignore
        px.launch_app()
        tracer_provider = register(project_name="solanus-rag-eval")
        _PHOENIX["tracer"] = tracer_provider.get_tracer(__name__)
        _PHOENIX["on"] = True
        print("[phoenix] tracing on — UI at http://localhost:6006")
    except Exception as e:
        # Deferred dependency: don't fail the eval just because the tracing stack isn't installed.
        print(f"[phoenix] not enabled ({e.__class__.__name__}); install arize-phoenix to use --phoenix")


def _emit_span(question: str, gold: dict, rcfg: retrieval.RetrievalConfig,
               hits: list, answer: str, metrics: dict) -> None:
    """Emit ONE OpenInference span for a (question x config) evaluation, if Phoenix is on.

    Safe no-op when tracing isn't enabled. When it is, we record the OpenInference-conventional
    attributes (input, output, retrieved doc ids, the metric scores) so the Phoenix UI can chart
    them. We keep this tolerant of any otel API hiccup — observability must never break the eval.
    """
    if not _PHOENIX["on"] or _PHOENIX["tracer"] is None:
        return
    try:
        with _PHOENIX["tracer"].start_as_current_span("rag.eval") as span:
            span.set_attribute("openinference.span.kind", "EVALUATOR")
            span.set_attribute("input.value", question)
            span.set_attribute("output.value", answer or "")
            span.set_attribute("eval.config", f"{rcfg.embedding_model}@{rcfg.embedding_dim}")
            span.set_attribute("retrieval.documents",
                               ",".join(str(h["meta"].get("doc_id")) for h in hits))
            for k, v in metrics.items():
                if isinstance(v, (int, float)):
                    span.set_attribute(f"eval.{k}", float(v))
    except Exception:
        pass


# ============================================================================
# Pretty console report — so a FREE run is readable without opening the JSON
# ============================================================================
def print_report(out: dict) -> None:
    """Print a compact leaderboard + the threshold verdict for the best config (David-friendly)."""
    print("=" * 72)
    print(f"RAG EVAL — {out['n_questions']} golden questions x {out['n_configs']} config(s)")
    print(f"thresholds: " + ", ".join(f"{k}>={v}" for k, v in out["thresholds"].items()))
    print("=" * 72)
    for c in out["configs"]:
        m = c["aggregate"]["means"]
        verdict = "PASS" if c["aggregate"]["passed_all"] else "----"
        def _f(x):
            return f"{x:.3f}" if isinstance(x, (int, float)) else "  n/a"
        print(f"[{verdict}] {c['label']}")
        print(f"        faith={_f(m['faithfulness'])}  ctx_prec={_f(m['context_precision'])}  "
              f"ctx_rec={_f(m['context_recall'])}  ans_rel={_f(m['answer_relevancy'])}")
    print("-" * 72)
    print(f"wrote: {RESULTS_PATH}")
    print("=" * 72)
    print("COST SUMMARY:", json.dumps(out["cost_summary"], indent=2))


# ============================================================================
# CLI — FREE by default; every paid path is double-gated (flag + --i-accept-cost)
# ============================================================================
def main() -> None:
    ap = argparse.ArgumentParser(description="step_7 RAG/answer eval harness (RAGAS/ARES tradition)")
    ap.add_argument("--sweep", action="store_true",
                    help="evaluate every {llm x embedding-space x reranker} combination")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of golden questions")
    ap.add_argument("--generate", action="store_true",
                    help="GENERATE grounded answers (PAID LLM) so faithfulness/relevancy are real")
    ap.add_argument("--use-llm-judge", action="store_true",
                    help="RAGAS-faithful LLM judging of claims/passages/relevancy (PAID)")
    ap.add_argument("--synthesize", type=int, metavar="N", default=0,
                    help="synthesize N golden questions to eval/synthetic_questions.json (PAID, GATED)")
    ap.add_argument("--phoenix", action="store_true",
                    help="stream OpenInference spans to a local Arize Phoenix UI (if installed)")
    ap.add_argument("--embed-only", nargs="*", default=None,
                    help="restrict the embedding axis to these 'model@dim' spaces")
    ap.add_argument("--rerankers", nargs="*", default=None,
                    help="restrict the reranker axis (default: free local reranker only)")
    ap.add_argument("--llms", nargs="*", default=None,
                    help="restrict the LLM axis (default: config default unless paid steps are on)")
    ap.add_argument("--i-accept-cost", action="store_true",
                    help="REQUIRED to let any PAID call fire (generate / judge / synthesize)")
    a = ap.parse_args()

    opts = EvalOptions(generate=a.generate, use_llm_judge=a.use_llm_judge,
                       phoenix=a.phoenix, cost_accepted=a.i_accept_cost)

    # ---- the cost guard: warn loudly if a paid flag was set without accepting cost ----
    wants_paid = a.generate or a.use_llm_judge or a.synthesize
    if wants_paid and not a.i_accept_cost:
        print("REFUSING to make a paid call: you passed a paid flag (--generate / --use-llm-judge / "
              "--synthesize) but not --i-accept-cost. Re-run with --i-accept-cost to spend, or drop "
              "the paid flag to run the FREE heuristic eval. (Nothing was billed.)")
        # Continue on the FREE path so the user still gets the retrieval-only metrics.
        opts = EvalOptions(phoenix=a.phoenix)

    maybe_start_phoenix(opts)

    # ---- synthetic-question generation is its own (gated) mode ----
    if a.synthesize and opts.cost_accepted:
        syn = synthesize_golden_questions(a.synthesize, opts, llm_model=(a.llms[0] if a.llms else None))
        print(f"wrote {len(syn)} synthetic questions to {EVAL_DIR / 'synthetic_questions.json'} "
              f"(REVIEW before promoting — gold is David-only).")
        return

    out = run_eval(opts, sweep=a.sweep, limit=a.limit,
                   embed_only=a.embed_only, rerankers=a.rerankers, llms=a.llms)
    print_report(out)


if __name__ == "__main__":
    main()
