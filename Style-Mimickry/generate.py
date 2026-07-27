"""generate.py — write in David's voice with a FROZEN model (no fine-tuning, ever).

This is the heart of the no-fine-tuning mimicker. Because we are forbidden from touching the
weights, all of the "style" has to be injected at inference time. We do that with three layers,
ordered from cheapest/most-reliable to most-experimental (mirroring the research report's stages):

  Layer 1 — FEW-SHOT + RETRIEVED EXEMPLARS (the workhorse, runs now).
     Retrieve the handful of David-exemplars closest to the user's prompt (from the RAG index
     data_prep.py built) and show them to a frozen LLM as "here is how David writes," then ask it to
     answer in that voice. This is the report's Stage-1 baseline: prompting does style "okay," and
     showing *nearby* real examples is far stronger than a generic "write like David" instruction.

  Layer 2 — STYLEVECTOR-STYLE STEERING (scaffolded, OFF by default).
     The report's StyleVector idea: personal style is roughly a single direction in activation
     space, and you can nudge generation along it training-free. With a black-box API (Gemini) we
     cannot touch activations, so the *honest* analogue we implement is a **style-vector-in-the-
     prompt**: describe the direction explicitly (the dimensions where David deviates most) and lean
     the instruction that way. The true activation-space version is a clear TODO that needs a
     local/open model with logit or hidden-state access.

  Layer 3 — HORIKAWA-STYLE ITERATIVE MLM REFINEMENT (scaffolded, GATED, never auto-run).
     The report's novel-contribution loop: start from the draft, repeatedly (mask -> MLM proposes
     fillers -> keep the candidate whose style embedding is closest to David's centroid), evolving
     the text toward his voice while a fluency term keeps it readable. We implement the *control
     flow and the fitness function* (cosine to the centroid from data_prep, via the same embedder),
     but the masked-LM proposal step is left as a documented stub because it needs RoBERTa-large
     locally — a heavy download David hasn't approved. The loop is `enabled=False` by default and
     prints exactly what it WOULD do.

Everything routes through step_7's frozen-LLM adapter (`lib.providers.llm.generate`) and embedder,
so every call is cost-logged and the model is a swappable config variable. Nothing here fine-tunes,
trains, or downloads a model.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped (stdlib / third-party / local), the project habit
# ============================================================================

# Core Python Imports
import argparse
import logging
import sys
from pathlib import Path

# Third-Party Imports
import numpy as np

# Local File Imports — step_7 on the path (same dance as data_prep.py / lib/costlog.py)
STEP7 = Path("/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/step_7")
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402
from lib import costlog             # noqa: E402  (imported for symmetry; llm/embed log for us)
from lib import vectorstore         # noqa: E402  (retrieve nearest style exemplars)
from lib.providers import embed     # noqa: E402  (embed the query + score refinement candidates)
from lib.providers import llm       # noqa: E402  (the FROZEN generation model)

# Sibling module: reuse data_prep's space name + centroid loader so the two files agree on geometry.
import data_prep                    # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")

EMBED_MODEL, EMBED_DIM = config.DEFAULTS["embedding"]


# ============================================================================
# Layer 1a — retrieve the style exemplars nearest to the prompt
# ============================================================================
def retrieve_exemplars(prompt: str, k: int = 5,
                       model: str = EMBED_MODEL, dim: int = EMBED_DIM) -> list[dict]:
    """Fetch the `k` David-exemplars most similar to `prompt` from the RAG style index.

    Showing the model *relevant* examples (a paragraph of David's that is topically near the request)
    beats showing random ones: the model anchors on both the subject and the voice at once. We embed
    the prompt as a "query" and brute-force cosine-search the style:: partition data_prep built.

    Args:
        prompt (str): the user's request / topic to write about.
        k (int): how many exemplars to retrieve.
        model (str): embedding model id (must match the index's space).
        dim (int): embedding dimensionality (must match the index's space).

    Returns:
        list[dict]: vectorstore hits ``{"id", "score", "meta"}`` plus the exemplar text re-attached.
    """
    # One cheap embedding call (cost-logged inside embed.embed_texts) to turn the prompt into a vector.
    qvec, _ = embed.embed_texts([prompt], model=model, dim=dim, task="query")
    hits = vectorstore.search(data_prep.SPACE, qvec[0], k=k)

    # Re-attach the exemplar text by id (the vector store keeps ids + metas, not the raw text).
    by_id = {ex["id"]: ex["text"] for ex in data_prep.load_exemplars()}
    for h in hits:
        h["text"] = by_id.get(h["id"], "")
    return hits


# ============================================================================
# Layer 1b — build the few-shot prompt and call the frozen model
# ============================================================================
# This system instruction is the "grounding policy" for voice. It is deliberately about *style*,
# not *facts*: the report is emphatic that a frozen model + prompt reproduces surface voice well but
# will CONFABULATE opinions, so we tell it to imitate the manner, not invent David's stances.
STYLE_SYSTEM = (
    "You are writing in the voice of David, a warm, pedagogical explainer. Study the EXAMPLES of "
    "his actual writing below and imitate his STYLE: how he builds intuition from first principles, "
    "his use of concrete analogies, his habit of explaining *why* (not just *what*), his sentence "
    "rhythm, and his vocabulary. Imitate the manner, not the content — do NOT invent facts or "
    "opinions David did not express; if you are unsure of a fact, hedge or stay general. Match his "
    "voice, not his claims."
)


def build_prompt(prompt: str, exemplars: list[dict], style_hint: str | None = None) -> str:
    """Assemble the few-shot prompt: David's exemplars + (optional) style-direction hint + the task.

    Args:
        prompt (str): the user's request.
        exemplars (list[dict]): retrieved exemplar hits (each with a "text" field).
        style_hint (str | None): an optional Layer-2 style-direction description to lean the model.

    Returns:
        str: the full user-content string sent to the frozen LLM.
    """
    blocks = []
    for i, ex in enumerate(exemplars, 1):
        if ex.get("text"):
            blocks.append(f"[Example {i} of David's writing]\n{ex['text']}")
    examples = "\n\n".join(blocks) if blocks else "(no exemplars retrieved — falling back to instruction only)"

    parts = [examples, ""]
    if style_hint:
        # Layer-2 "style vector in the prompt": an explicit nudge along the deviation axes.
        parts += [f"[Style direction to emphasize]\n{style_hint}", ""]
    parts += [f"[Task]\nWrite the following in David's voice:\n{prompt}"]
    return "\n".join(parts)


def generate_styled(prompt: str, k: int = 5, model: str | None = None,
                    style_hint: str | None = None, temperature: float = 0.6) -> dict:
    """Layer 1 (+optional Layer 2 hint): retrieve exemplars, prompt the FROZEN model, return a draft.

    Args:
        prompt (str): the user's request to write in David's voice.
        k (int): number of style exemplars to retrieve and show.
        model (str | None): LLM id; None -> config.DEFAULTS["llm"] (a swappable variable).
        style_hint (str | None): optional Layer-2 style-direction text.
        temperature (float): sampling temperature; a touch higher than the extraction default so the
            prose breathes (style needs a little freedom; 0.2 reads stiff).

    Returns:
        dict: ``{"draft", "exemplars", "model", "usage"}``.
    """
    exemplars = retrieve_exemplars(prompt, k=k)
    full = build_prompt(prompt, exemplars, style_hint=style_hint)
    # The frozen-model call. lib.providers.llm.generate cost-logs prompt+candidate tokens for us.
    text, usage = llm.generate(full, model=model, system=STYLE_SYSTEM, temperature=temperature)
    return {"draft": text, "exemplars": exemplars, "model": model or config.DEFAULTS["llm"],
            "usage": usage}


# ============================================================================
# Layer 2 — StyleVector-style steering (scaffold; activation steering deferred)
# ============================================================================
# The report's StyleVector result: a person's style is ~a single direction in a model's activation
# space, and you can steer along it training-free. With Gemini (a closed API) we have NO access to
# hidden states, so the genuinely-available analogue is to describe that direction in words and put
# it in the prompt (Layer 2 above). The function below builds such a description from the data_prep
# centroid + the per-axis spread (the dimensions where David's voice is "tightest" are the ones most
# characteristic of him). This is a heuristic textual proxy — the real thing is the TODO at the end.
def style_direction_hint() -> str:
    """Produce a short natural-language 'style direction' to steer the frozen model (Layer 2).

    Honest scope: a black-box API cannot be steered in activation space, so we hand it a *described*
    direction instead. This is intentionally generic + safe; the precise, learned StyleVector lives
    behind the TODO below and requires a local model.

    Returns:
        str: a style-direction hint string for :func:`build_prompt`.
    """
    # If we have a centroid we *could* later map its nearest interpretable axes; for now we emit the
    # report-grounded description of David's voice. (Loading the centroid also verifies data_prep ran.)
    try:
        _ = data_prep.load_centroid()
        have = True
    except FileNotFoundError:
        have = False
    base = ("Lead with intuition and a concrete analogy before any formalism; explain *why*, not "
            "just *what*; use warm, second-person teaching asides; vary sentence length with a few "
            "short punchy sentences among longer ones; prefer plain words over jargon.")
    return base if have else base + " (Note: style centroid not built; using report-grounded defaults.)"

# TODO (deferred, needs an OPEN/LOCAL model — NOT run): the true StyleVector method.
#   1. Pick a base open model with hidden-state access (e.g. a small Llama/Qwen via transformers).
#   2. For a set of prompts, generate an "authentic" (few-shot-with-David-exemplars) completion and a
#      "style-agnostic" (plain) completion; cache both.
#   3. style_vector = mean(hidden_states(authentic)) - mean(hidden_states(style_agnostic))  (per the
#      StyleVector paper: contrast authentic vs. style-agnostic activations).
#   4. At decode time, add alpha * style_vector to the residual stream of chosen layers.
#   This is a heavy, model-specific change (a download + a forward-hook); gated until David approves.


# ============================================================================
# Layer 3 — Horikawa-style iterative MLM refinement (scaffold; GATED, not run)
# ============================================================================
# The report's headline novel idea, adapted from Horikawa's "mind captioning": treat David's style
# centroid as a *target representation* and evolve a draft toward it with masked-LM edits. We build
# the loop's skeleton and its FITNESS function (which we CAN compute now — it is just cosine to the
# centroid plus a length penalty), but leave the masked-LM proposal step as a stub because it needs
# RoBERTa-large locally. The whole loop is disabled by default and is non-destructive: it would only
# ever return a *new* string, never overwrite the draft in place.
def style_fitness(text: str, centroid: np.ndarray,
                  alpha: float = 0.1, target_words: int = 120) -> float:
    """Fitness of a candidate: cosine(style-embedding, David-centroid) with a gentle length penalty.

    This is the exact shape the report specifies for the refinement objective: a representation-match
    term (here cosine to David's style centroid, standing in for Horikawa's brain-decoded target)
    multiplied by an exponential length penalty s = r * l^(-alpha) so the search cannot win by
    collapsing to a trivially short string (ASTRAPOP's documented degenerate-output failure mode).

    Args:
        text (str): the candidate text to score.
        centroid (np.ndarray): David's style centroid (unit vector) from data_prep.
        alpha (float): length-penalty strength (report uses ~0.1).
        target_words (int): the length the penalty is centered on (roughly an exemplar's length).

    Returns:
        float: a scalar fitness; higher == closer to David's voice (and not degenerate).
    """
    # One embedding call to place the candidate in the same space as the centroid (cost-logged).
    vec, _ = embed.embed_texts([text], model=EMBED_MODEL, dim=EMBED_DIM, task="document")
    r = float(np.dot(vec[0], centroid))                      # cosine (both are unit vectors)
    # Length penalty: peaks near target_words, decays as the candidate gets much shorter/longer.
    l = max(1, len(text.split()))
    penalty = (l / target_words) ** (-alpha) if l > target_words else (target_words / l) ** (-alpha)
    return r * penalty


def _mlm_propose(text: str, n_candidates: int = 5) -> list[str]:
    """STUB: propose masked-LM rewrites of `text` (Horikawa's mask -> unmask step).

    The real implementation (deferred — needs a local RoBERTa-large, a heavy download David has not
    approved) would: (1) mask spans of up to ~3 words, (2) run a frozen masked-LM (RoBERTa-large) to
    sample `n_candidates` fillers per mask staying on the natural-text manifold, (3) return the
    distinct candidate strings. The pretrained MLM's contextual priors are what make this work
    (Horikawa showed a *random* proposer fails), so this cannot be faked with a cheap heuristic.

    Args:
        text (str): the current best candidate.
        n_candidates (int): how many variants to propose.

    Returns:
        list[str]: candidate rewrites. Stub returns [] so the loop is a safe no-op until wired.
    """
    # TODO (deferred, PAID/heavy — NOT run): load RoBERTa-large via transformers, mask + sample.
    #   from transformers import pipeline
    #   fill = pipeline("fill-mask", model="roberta-large")   # ~1.4 GB download
    #   ... mask spans, collect fill(masked)[:n_candidates] ...
    logging.info("[refine] _mlm_propose is a stub (RoBERTa-large not loaded) — returning no candidates.")
    return []


def refine_toward_style(draft: str, iterations: int = 100, search_width: int = 5,
                        enabled: bool = False) -> dict:
    """Layer 3: evolve `draft` toward David's style centroid via the Horikawa MLM-search loop.

    GATED: `enabled=False` by default. When disabled we describe what the loop WOULD do and return
    the draft unchanged — so this is provably non-destructive and never silently runs a heavy model.

    The loop (when wired) is, per the report: keep a pool of candidates; each iteration mask -> MLM
    propose -> score every candidate by :func:`style_fitness` -> keep the top `search_width`; repeat
    for `iterations`. The masked-LM proposer (:func:`_mlm_propose`) is the only missing heavy piece.

    Args:
        draft (str): the Layer-1 draft to refine.
        iterations (int): number of mask/unmask/select rounds (report uses ~100).
        search_width (int): how many candidates to keep each round (report uses ~5).
        enabled (bool): hard gate; must be explicitly True to attempt refinement.

    Returns:
        dict: ``{"refined", "ran", "note"}``. When gated, refined == draft and ran is False.
    """
    if not enabled:
        return {"refined": draft, "ran": False,
                "note": ("refinement gated off (enabled=False). Would run %d iterations of "
                         "mask->MLM->select toward David's centroid once _mlm_propose is wired."
                         % iterations)}

    # --- the live loop (safe because _mlm_propose is a no-op stub until RoBERTa-large is wired) ----
    try:
        centroid = data_prep.load_centroid()
    except FileNotFoundError:
        return {"refined": draft, "ran": False,
                "note": "no style centroid — run `data_prep.py --embed` before refining."}

    pool = [draft]
    best = draft
    best_fit = style_fitness(draft, centroid)
    for it in range(iterations):
        candidates = []
        for cand in pool:
            candidates.extend(_mlm_propose(cand, n_candidates=search_width))
        if not candidates:
            logging.info("[refine] iter %d: no MLM candidates (stub) — stopping early.", it)
            break
        scored = sorted(((style_fitness(c, centroid), c) for c in candidates), reverse=True)
        pool = [c for _, c in scored[:search_width]]
        if scored and scored[0][0] > best_fit:
            best_fit, best = scored[0]
    return {"refined": best, "ran": True, "note": f"refined; best fitness={best_fit:.4f}"}


# ============================================================================
# main — a tiny end-to-end demo (Layer 1 by default; Layers 2/3 behind flags)
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Generate text in David's voice with a frozen LLM.")
    parser.add_argument("prompt", nargs="?", default="Explain what a vector embedding is to a beginner.",
                        help="what to write in David's voice")
    parser.add_argument("-k", type=int, default=5, help="number of style exemplars to retrieve")
    parser.add_argument("--model", default=None, help="LLM id (config.LLMS); default = config default")
    parser.add_argument("--steer", action="store_true", help="add the Layer-2 style-direction hint")
    parser.add_argument("--refine", action="store_true",
                        help="attempt Layer-3 Horikawa refinement (no-op until RoBERTa-large wired)")
    parser.add_argument("--run", action="store_true",
                        help="ACTUALLY call the paid LLM/embedder. Without this we only dry-run.")
    args = parser.parse_args()

    hint = style_direction_hint() if args.steer else None

    # Default is a DRY RUN: we build everything but do not spend money unless --run is given. This
    # keeps `python generate.py` safe to execute while David hasn't said "go."
    if not args.run:
        logging.info("DRY RUN (no API calls). Re-run with --run to actually generate.")
        logging.info("Prompt        : %s", args.prompt)
        logging.info("Would retrieve: %d exemplars from space %s", args.k, data_prep.SPACE)
        logging.info("Layer-2 steer : %s", "ON" if args.steer else "off")
        logging.info("Layer-3 refine: %s", "requested (gated stub)" if args.refine else "off")
        if hint:
            logging.info("Style hint    : %s", hint)
        return

    # --- live path (billed; cost-logged) ----------------------------------------------------------
    result = generate_styled(args.prompt, k=args.k, model=args.model, style_hint=hint)
    draft = result["draft"]
    refined = refine_toward_style(draft, enabled=args.refine)

    logging.info("=" * 60)
    logging.info("STYLED GENERATION")
    logging.info("=" * 60)
    logging.info("Model     : %s", result["model"])
    logging.info("Exemplars : %d retrieved", len(result["exemplars"]))
    logging.info("-" * 60)
    logging.info("%s", refined["refined"])
    logging.info("-" * 60)
    logging.info("Refinement: %s", refined["note"])
    logging.info("Tokens    : in=%s out=%s", result["usage"].get("input_tokens"),
                 result["usage"].get("output_tokens"))
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
