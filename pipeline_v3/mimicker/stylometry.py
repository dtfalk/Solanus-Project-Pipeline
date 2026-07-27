"""stylometry.py — interpretable, content-independent style features (free, offline).

A small, transparent feature vector for a passage: function-word rates, sentence-length mean/variance,
punctuation rates, type-token ratio, mean word length. Used to (a) build Solanus's style PROFILE and
(b) score a candidate as z-scores against it ("too few 'thee', sentences too long"). Deliberately simple
and explainable — the neural cosine (StyleDistance/LUAR centroid) is the other half in evaluate.py.
"""
from __future__ import annotations
import re
import statistics as st

# function words carry style, not topic — the backbone of classic stylometry (Mosteller-Wallace)
FUNCTION_WORDS = ("the a an and or but if while of to in for on with at by from as is are was were be been "
                  "i you he she it we they me him her them my your his our their this that these those not "
                  "no so very can could would should may might shall will do did does have has had").split()


def features(text: str) -> dict:
    text = (text or "").strip()
    words = re.findall(r"[A-Za-z']+", text.lower())
    n = max(1, len(words))
    sents = [s for s in re.split(r"[.!?]+", text) if s.strip()]
    slens = [len(re.findall(r"[A-Za-z']+", s)) for s in sents] or [0]
    feats = {f"fw_{w}": words.count(w) / n for w in FUNCTION_WORDS}
    feats.update({
        "sent_len_mean": st.fmean(slens),
        "sent_len_std": (st.pstdev(slens) if len(slens) > 1 else 0.0),
        "comma_rate": text.count(",") / n,
        "dash_rate": (text.count("-") + text.count("—")) / n,
        "exclaim_rate": text.count("!") / n,
        "ttr": len(set(words)) / n,                 # type-token ratio (lexical variety)
        "word_len_mean": sum(len(w) for w in words) / n,
    })
    return feats


def profile(texts: list[str]) -> dict:
    """Mean + std of each feature across many passages -> the author's stylometric fingerprint."""
    rows = [features(t) for t in texts if (t or "").strip()]
    keys = rows[0].keys() if rows else []
    out = {}
    for k in keys:
        vals = [r[k] for r in rows]
        out[k] = {"mean": st.fmean(vals), "std": (st.pstdev(vals) if len(vals) > 1 else 0.0) or 1e-6}
    return out


def zscores(text: str, prof: dict) -> dict:
    """How many std-devs each feature of `text` is from the author's profile (interpretable dashboard)."""
    f = features(text)
    return {k: (f.get(k, 0.0) - prof[k]["mean"]) / prof[k]["std"] for k in prof}
