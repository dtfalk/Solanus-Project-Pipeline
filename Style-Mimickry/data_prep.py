"""data_prep.py — turn David's writings into the raw materials a style mimicker needs.

NO FINE-TUNING happens here (David forbade it). Instead, this is "Stage 0" from the research
report: we take David's own corpus and produce three reusable artifacts that everything downstream
(generate.py, evaluate.py) reads:

  1. A list of clean **style exemplars** — short, self-contained passages segmented from his
     writing. These are the few-shot / retrieved examples that teach a *frozen* LLM his voice.
  2. A **RAG style-exemplar index** — each exemplar embedded into a step_7 vector-store partition,
     so generate.py can retrieve the exemplars most relevant to a new prompt (style is most
     convincing when the model is shown *nearby* examples, not random ones).
  3. A **style centroid** — the average (and spread) of those exemplar embeddings. This single
     vector is the "target" the report's Horikawa-style refinement loop steers toward, and the
     anchor evaluate.py measures cosine distance against. Think of it as the center of mass of
     David's voice in embedding space.

Why this is the whole job for a no-fine-tuning system: with a frozen model you cannot bake the
style into the weights, so you must *carry it in the prompt* (exemplars) and *measure it from
outside* (centroid + stylometry). This file builds both halves.

We reuse step_7's libraries wholesale — `config` for paths/model choices, `lib.costlog` so every
embedding call lands in costs/usage.csv, `lib.providers.embed` for the model-agnostic embedder, and
`lib.vectorstore` for the partitioned index. Reusing them means a Style-Mimickry index is the exact
same shape as a step_7 retrieval index and the same swap-a-model-in-config knob applies.

⚠️ Nothing here is billed until you actually run `main()` AND uncomment the embedding call — the
embedding step is gated behind `--embed` precisely so importing/reading this file is free.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped so it is obvious what is stdlib vs. local vs. third-party
# (same grouping habit as the rest of the project)
# ============================================================================

# Core Python Imports
import argparse
import json
import logging
import re
import sys
from pathlib import Path

# Third-Party Imports
import numpy as np

# Local File Imports — we borrow step_7's whole library by putting step_7 on the path. This is the
# *identical* sys.path dance every step_7 module does (see lib/costlog.py): resolve the step_7 dir,
# insert it at the front of sys.path, THEN import its modules. Doing it this way means a single
# source of truth for models/prices/providers — Style-Mimickry never re-implements an embedder.
STEP7 = Path("/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/step_7")
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402  (step_7 central config: paths, models, prices)
from lib import costlog             # noqa: E402  (always-on cost logging)
from lib import vectorstore         # noqa: E402  (partitioned local vector store)
from lib.providers import embed     # noqa: E402  (model-agnostic embedding adapters)


# ============================================================================
# Paths — self-contained inside the Style-Mimickry/ folder
# ============================================================================
# Everything this tool writes lives under Style-Mimickry/data so we never touch step_7's data/ or
# David's source writings. Non-destructive by construction: we only ever read the corpus.
HERE        = Path(__file__).resolve().parent
DATA_DIR    = HERE / "data"
EXEMPLARS   = DATA_DIR / "exemplars.jsonl"      # the segmented style passages (one per line)
CENTROID    = DATA_DIR / "style_centroid.npy"   # the mean style embedding (the "voice target")
PROFILE     = DATA_DIR / "style_profile.json"   # human-readable summary of what we built

# Where David's prose lives. The .tex explainers in examples/writing_examples are the cleanest
# samples of his *expository* voice; you can point CORPUS_DIRS at more of his writing later.
REPO        = config.REPO
CORPUS_DIRS = [
    REPO / "examples" / "writing_examples",     # his LaTeX explainers (diffusion, logic, intuitive)
    # TODO: add more of David's writings here as they are collected, e.g.:
    # REPO / "examples" / "code_examples" / "ipsos_code",   # docstrings/comments are also "his voice"
]

# The embedding "space" we build the index in. We default to step_7's default embedding so a
# Style-Mimickry centroid lives in the SAME geometry as the archival RAG index — handy if you ever
# want to compare a generated answer's style against retrieved corpus chunks.
EMBED_MODEL, EMBED_DIM = config.DEFAULTS["embedding"]
SPACE = f"style::{EMBED_MODEL}@{EMBED_DIM}"      # partition name under step_7 data/vectors/

logging.basicConfig(level=logging.INFO, format="%(message)s")


# ============================================================================
# Step 1 — read David's writings off disk (free, stdlib only)
# ============================================================================
def _read_corpus_files() -> list[dict]:
    """Collect David's writing files into raw {path, text} records.

    We read .tex (his explainers), .md, and .txt. For .tex we strip the LaTeX *scaffolding*
    (preamble, figure/tikz environments, listings, commands) so the embedder sees his PROSE, not
    `\\usepackage` noise — style lives in the sentences, not the markup.

    Returns:
        list[dict]: one record per file, ``{"path": str, "text": str}`` with non-empty text.
    """
    records: list[dict] = []
    for root in CORPUS_DIRS:
        if not root.exists():
            logging.warning("corpus dir missing (skipping): %s", root)
            continue
        for path in sorted(root.rglob("*")):
            if path.suffix.lower() not in {".tex", ".md", ".txt"}:
                continue
            raw = path.read_text(errors="ignore")
            text = _strip_latex(raw) if path.suffix.lower() == ".tex" else raw
            text = text.strip()
            if text:
                records.append({"path": str(path), "text": text})
    return records


def _strip_latex(src: str) -> str:
    """Pull readable prose out of a .tex file (best-effort, regex-only — no LaTeX parser needed).

    The goal is not perfect detex; it is to remove the obvious non-prose so the style signal is
    clean. We drop the preamble, math-heavy/figure environments, and the most common commands, then
    keep the natural-language remainder.

    Args:
        src (str): raw .tex file contents.

    Returns:
        str: a prose-ish approximation suitable for embedding/segmentation.
    """
    s = src
    # Drop everything before \begin{document} — that is all preamble/package noise.
    m = re.search(r"\\begin\{document\}", s)
    if m:
        s = s[m.end():]
    # Drop whole environments that are not prose (figures, tikz, code listings, tables, math blocks).
    for env in ("figure", "tikzpicture", "lstlisting", "tabular", "equation", "align", "verbatim"):
        s = re.sub(rf"\\begin\{{{env}\*?\}}.*?\\end\{{{env}\*?\}}", " ", s, flags=re.DOTALL)
    # Comment lines (start with %), then inline display math, then leftover commands.
    s = re.sub(r"(?m)^\s*%.*$", " ", s)
    s = re.sub(r"\$[^$]*\$", " ", s)                       # inline math -> space
    s = re.sub(r"\\[a-zA-Z]+\*?(\[[^\]]*\])?(\{[^{}]*\})?", " ", s)  # \cmd[..]{..} -> space
    s = re.sub(r"[{}]", " ", s)                            # stray braces
    s = re.sub(r"[ \t]+", " ", s)                          # collapse whitespace
    s = re.sub(r"\n{3,}", "\n\n", s)                       # collapse blank runs
    return s


# ============================================================================
# Step 2 — segment the prose into clean, self-contained style exemplars
# ============================================================================
# Why segment at all? Two reasons from the research:
#   (a) RAG works on *passages*: generate.py retrieves the few exemplars closest to a new prompt,
#       so we need bite-sized, standalone units, not whole documents.
#   (b) Stylometry/embeddings are unreliable on very short text (the LICW report flags ~<100 words
#       as meaningless) AND diluted on very long text. A paragraph-ish window (~40-180 words) is the
#       sweet spot the report recommends, so we target that.
MIN_WORDS = 40       # below this, count-based style features (LICW report) are noise
MAX_WORDS = 180      # above this, one "exemplar" mixes too many ideas to be a clean style sample


def _segment(text: str) -> list[str]:
    """Split one document's prose into paragraph-sized exemplars in the [MIN, MAX]-word band.

    We split on blank lines first (natural paragraph boundaries), then greedily merge tiny
    paragraphs up toward MIN_WORDS and break overly long ones near sentence boundaries so each
    exemplar is a coherent, standalone chunk of David's voice.

    Args:
        text (str): cleaned prose from one file.

    Returns:
        list[str]: exemplar strings, each roughly MIN_WORDS..MAX_WORDS long.
    """
    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    exemplars: list[str] = []
    buf: list[str] = []
    buf_words = 0

    def _flush():
        nonlocal buf, buf_words
        if buf_words >= MIN_WORDS:
            exemplars.append(" ".join(buf).strip())
        buf, buf_words = [], 0

    for para in paras:
        n = len(para.split())
        # A long paragraph is split on sentence boundaries into <= MAX_WORDS windows.
        if n > MAX_WORDS:
            _flush()
            for window in _split_long(para):
                exemplars.append(window)
            continue
        # Otherwise accumulate small paragraphs until we cross MIN_WORDS, then emit.
        buf.append(para)
        buf_words += n
        if buf_words >= MIN_WORDS:
            _flush()
    _flush()
    return exemplars


def _split_long(para: str) -> list[str]:
    """Break a too-long paragraph into <= MAX_WORDS windows at sentence boundaries.

    Args:
        para (str): a paragraph longer than MAX_WORDS words.

    Returns:
        list[str]: sentence-aligned windows, each <= MAX_WORDS words.
    """
    # Naive sentence split (good enough for windowing; a real tokenizer is a TODO if needed).
    sentences = re.split(r"(?<=[.!?])\s+", para)
    windows, cur, cur_n = [], [], 0
    for sent in sentences:
        sn = len(sent.split())
        if cur_n + sn > MAX_WORDS and cur:
            windows.append(" ".join(cur).strip())
            cur, cur_n = [], 0
        cur.append(sent)
        cur_n += sn
    if cur and cur_n >= MIN_WORDS:
        windows.append(" ".join(cur).strip())
    return windows


def build_exemplars() -> list[dict]:
    """Read the corpus and segment it into the final list of style-exemplar records.

    Each record carries provenance (which file it came from) so an exemplar shown to the model — or
    flagged by evaluate.py — can always be traced back to David's actual writing.

    Returns:
        list[dict]: ``{"id", "text", "n_words", "source"}`` exemplar records.
    """
    records = _read_corpus_files()
    exemplars: list[dict] = []
    for rec in records:
        for piece in _segment(rec["text"]):
            exemplars.append({
                "id":      f"ex{len(exemplars):05d}",
                "text":    piece,
                "n_words": len(piece.split()),
                "source":  rec["path"],
            })
    return exemplars


def save_exemplars(exemplars: list[dict]) -> Path:
    """Write exemplars to a .jsonl file (one record per line) — the canonical style-exemplar set.

    Args:
        exemplars (list[dict]): records from :func:`build_exemplars`.

    Returns:
        Path: the written exemplars.jsonl path.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(EXEMPLARS, "w", encoding="utf-8") as f:
        for ex in exemplars:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    return EXEMPLARS


def load_exemplars() -> list[dict]:
    """Read back the exemplars.jsonl produced by :func:`save_exemplars` (free, no model call)."""
    if not EXEMPLARS.exists():
        raise FileNotFoundError(f"no exemplars at {EXEMPLARS} — run data_prep.py first")
    return [json.loads(line) for line in open(EXEMPLARS, encoding="utf-8")]


# ============================================================================
# Step 3 — embed the exemplars into a RAG index + compute the style centroid
# ============================================================================
# This is the only part that costs money (one embedding call per exemplar batch). It is gated behind
# --embed so the default `data_prep.py` run is FREE: it segments and reports, and only embeds when
# you explicitly ask. Every embedding call is cost-logged by lib.providers.embed for us.
def build_index_and_centroid(exemplars: list[dict],
                             model: str = EMBED_MODEL,
                             dim: int = EMBED_DIM) -> dict:
    """Embed every exemplar, write a step_7 vector-store partition, and save the style centroid.

    The **centroid** is simply the mean of the (L2-normalized) exemplar vectors. Because the
    embedder already normalizes each vector, cosine similarity to the centroid is a clean "how close
    is this text to the center of David's voice?" score — exactly the LUAR/StyleDistance-style target
    the report (and evaluate.py) uses. We store the centroid AND the per-axis std so evaluate.py can
    report a Mahalanobis-style spread, not just a single number.

    Args:
        exemplars (list[dict]): the style exemplars to embed.
        model (str): embedding model id from config.EMBEDDINGS (a swappable variable).
        dim (int): output dimensionality (must be valid for `model`).

    Returns:
        dict: a small summary {space, n, centroid_norm, model, dim} for the profile/report.
    """
    texts = [ex["text"] for ex in exemplars]
    ids   = [ex["id"] for ex in exemplars]
    metas = [{"source": ex["source"], "n_words": ex["n_words"]} for ex in exemplars]

    # The one paid line. embed.embed_texts cost-logs each batch through lib.costlog automatically,
    # so this run shows up in step_7/costs/usage.csv just like any archival embedding run.
    logging.info("Embedding %d exemplars with %s@%d ...", len(texts), model, dim)
    vectors, _usage = embed.embed_texts(texts, model=model, dim=dim, task="document")

    # Persist the index in step_7's partitioned store, under a "style::" space so it never collides
    # with the archival RAG partitions (which are named "model@dim").
    vectorstore.write(SPACE, ids, vectors, metas)

    # The style centroid: mean of the unit vectors. (We renormalize it so downstream cosine math is
    # symmetric — comparing a unit query vector against a unit centroid.)
    vecs = np.asarray(vectors, dtype=np.float32)
    centroid = vecs.mean(axis=0)
    norm = float(np.linalg.norm(centroid)) or 1.0
    centroid_unit = centroid / norm
    spread = vecs.std(axis=0)                        # per-axis spread (a cheap "tightness" of voice)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    np.save(CENTROID, centroid_unit)
    np.save(DATA_DIR / "style_spread.npy", spread)

    return {"space": SPACE, "n": len(texts), "centroid_pre_norm": norm,
            "model": model, "dim": dim}


def load_centroid() -> np.ndarray:
    """Load the saved style centroid (the voice target). Raises if data_prep --embed hasn't run."""
    if not CENTROID.exists():
        raise FileNotFoundError(f"no centroid at {CENTROID} — run `data_prep.py --embed` first")
    return np.load(CENTROID)


# ============================================================================
# main — orchestration + a print-the-summary block (matches the project house style)
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Prepare David's corpus for style mimicry (Stage 0).")
    parser.add_argument("--embed", action="store_true",
                        help="ALSO embed exemplars + build the index/centroid (THIS COSTS MONEY).")
    parser.add_argument("--model", default=EMBED_MODEL, help="embedding model id (config.EMBEDDINGS)")
    parser.add_argument("--dim", type=int, default=EMBED_DIM, help="embedding dimensionality")
    args = parser.parse_args()

    # ---- segment (always free) -------------------------------------------------------------------
    exemplars = build_exemplars()
    save_exemplars(exemplars)
    word_total = sum(ex["n_words"] for ex in exemplars)

    profile = {
        "exemplars":        len(exemplars),
        "total_words":      word_total,
        "sources":          sorted({ex["source"] for ex in exemplars}),
        "min_words":        MIN_WORDS,
        "max_words":        MAX_WORDS,
        "embedded":         False,
    }

    # ---- embed (only with --embed; the only billed step) -----------------------------------------
    if args.embed:
        if not exemplars:
            logging.warning("No exemplars to embed — point CORPUS_DIRS at David's writings first.")
        else:
            summary = build_index_and_centroid(exemplars, model=args.model, dim=args.dim)
            profile.update({"embedded": True, **summary})
    else:
        logging.info("Skipping embeddings (free run). Re-run with --embed to build the index/centroid.")
        # TODO: this is the deferred PAID step. `--embed` calls embed.embed_texts, which bills the
        # provider and cost-logs to step_7/costs/usage.csv. David hasn't said "go", so it stays gated.

    PROFILE.write_text(json.dumps(profile, indent=2, ensure_ascii=False))

    # ---- summary block (the project's signature =*60 SUMMARY) ------------------------------------
    logging.info("=" * 60)
    logging.info("DATA PREP SUMMARY")
    logging.info("=" * 60)
    logging.info("Exemplars built : %s", len(exemplars))
    logging.info("Total words     : %s", word_total)
    logging.info("Sources         : %s file(s)", len(profile["sources"]))
    logging.info("Exemplars file  : %s", EXEMPLARS)
    if profile["embedded"]:
        logging.info("Index space     : %s", profile.get("space"))
        logging.info("Centroid        : %s", CENTROID)
    logging.info("Profile         : %s", PROFILE)
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
