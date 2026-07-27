"""evaluate.py — does the generated text actually sound like David? Measure it three ways.

A style mimicker is only trustworthy if you can *measure* the voice, not just admire the output. The
two research reports converge on a three-part evaluation, and this file implements all three (the
neural part fully; the discriminator as an honest stub, because training it needs sklearn which is
not in the venv yet):

  1. NEURAL STYLE COSINE (the primary metric, per the report).
     Embed the candidate and measure cosine similarity to David's style centroid (from data_prep) —
     a LUAR / StyleDistance-style "how close to the target author's embedding?" score. We also report
     cosine to the *nearest* individual exemplar, so a high score can't be faked by landing near the
     bland average. Higher = more David.

  2. INTERPRETABLE STYLOMETRY (the diagnostic dashboard, per the LICW report).
     A small, robust, dependency-free feature set — function-word distribution, sentence-length mean
     and variance (rhythm/burstiness), punctuation rates, type-token ratio, mean word length. We
     compute David's profile over the exemplars and report, per feature, how many standard deviations
     the candidate deviates (a z-score). This is the "too few I-words, sentences too long" audit that
     embeddings cannot give you, and it is the tripwire against a candidate that games the embedding.

  3. AUTHORSHIP-ATTRIBUTION DISCRIMINATOR (the adversarial judge — stub).
     The strongest honest test: can a classifier tell David's real writing from the clone's? We lay
     out the train/score API and a feature extractor, but the actual classifier is a documented stub
     (needs scikit-learn; it is a TODO, never trained here). When sklearn is absent we fall back to a
     transparent nearest-centroid heuristic so the file still *runs* and reports something.

Like the others, this reuses step_7's embedder (so the cosine is in the same geometry as data_prep's
centroid) and cost-logs every embedding call. The stylometry + discriminator are pure-Python and
FREE. Nothing here trains a model or makes a paid call unless you embed.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped (stdlib / third-party / local)
# ============================================================================

# Core Python Imports
import argparse
import json
import logging
import math
import re
import sys
from collections import Counter
from pathlib import Path

# Third-Party Imports
import numpy as np

# Local File Imports — step_7 on the path (same dance as the sibling modules)
STEP7 = Path("/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/step_7")
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402
from lib import costlog             # noqa: E402  (embed.* cost-logs for us; imported for symmetry)
from lib.providers import embed     # noqa: E402

import data_prep                    # noqa: E402  (centroid + exemplars + space name)

logging.basicConfig(level=logging.INFO, format="%(message)s")

EMBED_MODEL, EMBED_DIM = config.DEFAULTS["embedding"]


# ============================================================================
# Metric 1 — neural style cosine to David's centroid (LUAR/StyleDistance-style)
# ============================================================================
def neural_style_score(text: str) -> dict:
    """Cosine of `text`'s style embedding to David's centroid (primary metric) + nearest-exemplar.

    Two numbers, on purpose:
      * ``centroid_cos`` — closeness to the *center* of David's voice. Robust, but a bland output can
        drift toward the average and score deceptively well.
      * ``nearest_exemplar_cos`` — closeness to the single most-similar real exemplar. Harder to game;
        a genuinely David-like passage should sit near at least one real one.

    Args:
        text (str): the candidate (generated) text to evaluate.

    Returns:
        dict: ``{"centroid_cos", "nearest_exemplar_cos", "nearest_exemplar_id"}``.
    """
    centroid = data_prep.load_centroid()                    # raises a clear error if --embed not run
    # One embedding call for the candidate (cost-logged inside embed.embed_texts).
    vec, _ = embed.embed_texts([text], model=EMBED_MODEL, dim=EMBED_DIM, task="document")
    cand = vec[0]
    centroid_cos = float(np.dot(cand, centroid))            # both unit vectors -> dot == cosine

    # Nearest individual exemplar: load the index partition and brute-force the best match.
    ids, vecs, _metas = data_prep.vectorstore.load(data_prep.SPACE)
    sims = vecs @ cand
    j = int(np.argmax(sims))
    return {"centroid_cos": centroid_cos,
            "nearest_exemplar_cos": float(sims[j]),
            "nearest_exemplar_id": ids[j]}


# ============================================================================
# Metric 2 — interpretable stylometry (the dimension-by-dimension dashboard)
# ============================================================================
# A deliberately SMALL, robust, dependency-free feature set. The LICW report warns that piling on
# axes degrades grammaticality and that count features are unreliable on short text — so we keep it
# to a handful of topic-invariant features that survive on paragraph-sized inputs. (Richer syntactic
# features — POS/dependency n-grams via spaCy — are a TODO; spaCy is not installed in the venv.)
FUNCTION_WORDS = [
    # Pronouns (esp. first-person, the single most style-diagnostic family per Pennebaker).
    "i", "me", "my", "we", "us", "our", "you", "your", "it", "its", "they", "them", "their",
    # Articles + the most common prepositions/conjunctions/auxiliaries/negations.
    "a", "an", "the", "of", "to", "in", "on", "for", "with", "at", "by", "from", "as",
    "and", "but", "or", "so", "if", "because", "that", "which", "while",
    "is", "are", "was", "were", "be", "been", "being", "do", "does", "did", "have", "has", "had",
    "not", "no",
]


def _tokenize(text: str) -> list[str]:
    """Lowercase word tokens (letters/apostrophes only) — the unit for function-word rates."""
    return re.findall(r"[a-z']+", text.lower())


def stylometric_features(text: str) -> dict:
    """Compute the robust interpretable style features for one text.

    Features (all rates/ratios so they are comparable across lengths):
      * ``fw_<word>`` — relative frequency of each function word (the classic authorship signal).
      * ``sent_len_mean`` / ``sent_len_std`` — sentence-length rhythm and its burstiness.
      * ``comma_rate`` / ``semicolon_rate`` / ``dash_rate`` — punctuation habits (very individual).
      * ``ttr`` — type-token ratio (vocabulary richness; length-sensitive, see the report's caveat).
      * ``mean_word_len`` — average characters per word.

    Args:
        text (str): the text to profile.

    Returns:
        dict[str, float]: feature name -> value.
    """
    toks = _tokenize(text)
    n = max(1, len(toks))
    counts = Counter(toks)

    feats: dict[str, float] = {f"fw_{w}": counts.get(w, 0) / n for w in FUNCTION_WORDS}

    # Sentence-length rhythm: split on terminal punctuation, count words per sentence.
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s.split()]
    lens = [len(s.split()) for s in sentences] or [n]
    feats["sent_len_mean"] = float(np.mean(lens))
    feats["sent_len_std"]  = float(np.std(lens))

    # Punctuation rates (per token, so length-comparable).
    feats["comma_rate"]     = text.count(",") / n
    feats["semicolon_rate"] = text.count(";") / n
    feats["dash_rate"]      = (text.count("—") + text.count(" - ")) / n

    # Richness + word length.
    feats["ttr"] = len(counts) / n
    feats["mean_word_len"] = float(np.mean([len(t) for t in toks])) if toks else 0.0
    return feats


def build_style_profile(exemplars: list[dict] | None = None) -> dict:
    """David's interpretable profile: per-feature mean + std over his exemplars (the reference band).

    We compute the feature vector for every exemplar, then summarize each feature by its mean and
    standard deviation across exemplars. Those (mean, std) pairs ARE the reference David-band that
    :func:`stylometry_report` z-scores a candidate against.

    Args:
        exemplars (list[dict] | None): exemplar records; None -> load from data_prep.

    Returns:
        dict: ``{"mean": {...}, "std": {...}, "n": int}``.
    """
    exemplars = exemplars or data_prep.load_exemplars()
    per_feat: dict[str, list[float]] = {}
    for ex in exemplars:
        for k, v in stylometric_features(ex["text"]).items():
            per_feat.setdefault(k, []).append(v)
    mean = {k: float(np.mean(v)) for k, v in per_feat.items()}
    std  = {k: float(np.std(v)) for k, v in per_feat.items()}
    return {"mean": mean, "std": std, "n": len(exemplars)}


def stylometry_report(text: str, profile: dict | None = None, top: int = 8) -> dict:
    """Z-score the candidate's features against David's band; surface the biggest deviations.

    For each feature we compute z = (candidate - David_mean) / David_std (guarding std==0). A z near
    0 means "right in David's range"; a large |z| means "this dimension is off." We return the
    overall RMS z (a single 'how off overall' number) and the `top` worst offenders so the audit
    reads like "sentences too long (z=+3.1), too few 'I' (z=-2.4)".

    Args:
        text (str): the candidate text.
        profile (dict | None): David's profile from :func:`build_style_profile`; None -> build it.
        top (int): how many worst-deviating features to list.

    Returns:
        dict: ``{"rms_z", "worst": [(feature, z, candidate_value, david_mean), ...]}``.
    """
    profile = profile or build_style_profile()
    cand = stylometric_features(text)
    zs: list[tuple] = []
    for k, mu in profile["mean"].items():
        sd = profile["std"].get(k, 0.0)
        if sd <= 1e-9:                                       # near-constant feature -> skip (no signal)
            continue
        z = (cand.get(k, 0.0) - mu) / sd
        zs.append((k, z, cand.get(k, 0.0), mu))
    rms = math.sqrt(np.mean([z * z for _, z, _, _ in zs])) if zs else 0.0
    worst = sorted(zs, key=lambda t: abs(t[1]), reverse=True)[:top]
    return {"rms_z": float(rms), "worst": worst}


# ============================================================================
# Metric 3 — authorship-attribution discriminator (the adversarial judge; STUB)
# ============================================================================
# The strongest honest test from both reports: train a classifier on (David's real writing) vs.
# (clone output) and see if it can separate them — lower separability is better. The full version
# needs scikit-learn (XGBoost/logistic on stylometric+neural features, per "Catch Me If You Can?").
# sklearn is not in the venv, so we (a) define the real API, (b) leave the trained model a TODO, and
# (c) provide a transparent nearest-centroid fallback so the metric still returns a number today.
def discriminator_score(text: str) -> dict:
    """Estimate how easily a judge separates this candidate from David's real writing.

    Fallback (implemented now, free): a nearest-centroid heuristic in stylometry space — z-score
    RMS distance to David's profile, squashed to a 0..1 'looks-cloned' probability. Closer to 0 means
    "indistinguishable from David"; closer to 1 means "a discriminator would flag this."

    Args:
        text (str): the candidate text.

    Returns:
        dict: ``{"method", "clone_prob", "note"}``.
    """
    # TODO (deferred — needs scikit-learn, NOT installed/trained here): the real discriminator.
    #   1. Featurize David's exemplars (label 0) and a set of clone outputs (label 1) with
    #      stylometric_features + neural embeddings.
    #   2. Fit a logistic-regression / XGBoost classifier with cross-validation.
    #   3. Report held-out AUC: ~0.5 means the clone is indistinguishable (great); ~1.0 means
    #      trivially separable (bad — strengthen exemplars / enable the refinement loop).
    #   from sklearn.linear_model import LogisticRegression  # not in venv yet
    rep = stylometry_report(text)
    # Squash RMS z to (0,1): z=0 -> 0.0 (indistinguishable), large z -> ~1.0 (easily flagged).
    clone_prob = 1.0 - math.exp(-rep["rms_z"])
    return {"method": "nearest-centroid fallback (sklearn discriminator is a TODO)",
            "clone_prob": float(clone_prob),
            "note": "Train the real AA classifier once scikit-learn is available (see TODO)."}


# ============================================================================
# evaluate — run all three metrics on one candidate and return a report dict
# ============================================================================
def evaluate(text: str, with_neural: bool = False) -> dict:
    """Run the full style evaluation on one candidate.

    Args:
        text (str): the generated text to judge.
        with_neural (bool): if True, ALSO compute the neural cosine metric (this EMBEDS the text and
            therefore costs money + requires data_prep --embed to have run). Default False keeps a
            plain evaluate() run free: stylometry + discriminator only.

    Returns:
        dict: ``{"stylometry", "discriminator", "neural"?}``.
    """
    report: dict = {
        "stylometry":    stylometry_report(text),
        "discriminator": discriminator_score(text),
    }
    if with_neural:
        report["neural"] = neural_style_score(text)         # billed embedding call (cost-logged)
    return report


# ============================================================================
# main — evaluate a sample (stylometry/discriminator free; --neural to add cosine)
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Evaluate how much a text sounds like David.")
    parser.add_argument("text", nargs="?", default=None,
                        help="text to evaluate; if omitted, evaluates the FIRST exemplar as a sanity check")
    parser.add_argument("--neural", action="store_true",
                        help="ALSO compute neural cosine to the centroid (EMBEDS text — costs money)")
    args = parser.parse_args()

    # Default sample: a real exemplar should score very David-like — a built-in sanity check that the
    # metrics are oriented correctly (a real David passage ought to have small z and low clone_prob).
    if args.text is None:
        try:
            args.text = data_prep.load_exemplars()[0]["text"]
            logging.info("(no text given — evaluating the first real exemplar as a sanity check)")
        except (FileNotFoundError, IndexError):
            args.text = "This is a short test sentence to exercise the evaluator."

    report = evaluate(args.text, with_neural=args.neural)

    logging.info("=" * 60)
    logging.info("STYLE EVALUATION")
    logging.info("=" * 60)
    sm = report["stylometry"]
    logging.info("Stylometry RMS z (lower = more David): %.3f", sm["rms_z"])
    logging.info("Biggest deviations (feature: z | cand vs David-mean):")
    for feat, z, cv, mu in sm["worst"]:
        logging.info("   %-16s z=%+.2f   (%.4f vs %.4f)", feat, z, cv, mu)
    logging.info("-" * 60)
    disc = report["discriminator"]
    logging.info("Discriminator clone-prob (lower = harder to tell): %.3f", disc["clone_prob"])
    logging.info("   method: %s", disc["method"])
    if "neural" in report:
        nz = report["neural"]
        logging.info("-" * 60)
        logging.info("Neural cosine to centroid (higher = more David): %.4f", nz["centroid_cos"])
        logging.info("Nearest exemplar cosine: %.4f  (%s)", nz["nearest_exemplar_cos"],
                     nz["nearest_exemplar_id"])
    logging.info("=" * 60)

    # Also drop a machine-readable copy next to the data so a batch eval can collect them.
    out = data_prep.DATA_DIR / "last_eval.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    logging.info("Report written to %s", out)


if __name__ == "__main__":
    main()
