"""stages/colpali_index.py — Tier-3 multimodal VISUAL retrieval (ColPali / ColQwen).

This is the "read the *page image*, not the OCR" retriever from PART D.3 of the research plan. Every
other retriever in step_7 searches *text* — the cleaned OCR of each region. That is wonderful when
the OCR is good, but on a 1930s handwritten notebook the OCR is exactly where we bleed recall:
marginalia, a struck-through line, a date squeezed into a corner, a word the transcriber guessed
wrong. **ColPali** sidesteps all of that. It embeds the *rendered page* with a vision-language model
and matches it to the query with *late interaction* — so a query token can land on the visual patch
where the answer actually sits on the paper, no transcription required. On scanned documents this is
a large recall win (the literature reports visual recall ≫ text recall on exactly this kind of
material), and it is the natural complement to our text retriever: fuse the two and you catch both
what the OCR saw and what it missed.

------------------------------------------------------------------------------------------------
WHAT "LATE INTERACTION" MEANS (the one idea you need)
------------------------------------------------------------------------------------------------
A normal dense retriever squashes a whole document into ONE vector and a whole query into ONE
vector, then compares the two. That is "early" interaction — all the matching happens after both
sides have already been compressed, so fine detail is gone. **Late interaction** (the ColBERT idea,
which ColPali brings to images) keeps a vector *per token*: the page becomes a grid of ~1030 patch
vectors (think: one vector per little square of the scan), and the query becomes ~20 token vectors.
The score is **MaxSim**: for each query token, find the single page patch it matches best, then sum
those maxima. Intuitively — "for every word in my question, is there *some* spot on this page that
answers it?" That is why it shines on messy archival pages: the match is local, so a good hit in one
corner of the page isn't washed out by the rest of the page being unrelated.

The price of keeping per-patch vectors is storage and compute: ~1030 vectors × 128 dims per page, a
full MaxSim against every page for every query. We tame that with the two-phase design below.

------------------------------------------------------------------------------------------------
TWO-PHASE RETRIEVAL (coarse mean-pool → MaxSim rerank top-100)
------------------------------------------------------------------------------------------------
Running the full MaxSim against all ~2,100 page images for every query would work at this corpus
size, but it doesn't scale and it wastes the GPU. So we do what the ColPali community does in
production (the "pooling-by-rows / hierarchical" trick):

  Phase 1 — COARSE (cheap, runs against every page):
      mean-pool each page's ~1030 patch vectors into a SINGLE page vector, and mean-pool the query's
      token vectors into a SINGLE query vector. One dot product per page → a fast shortlist. This is
      a deliberately lossy approximation (we threw away the per-patch detail), so we keep a GENEROUS
      shortlist (top-100) rather than trusting it for the final order.

  Phase 2 — FINE (expensive, runs only on the shortlist):
      load the FULL multi-vector patch tensors for just those top-100 pages and compute the real
      MaxSim. Re-sort by the exact late-interaction score. Now the per-patch detail decides the
      final ranking, but we only paid for it on 100 pages instead of 2,100.

Same answer as brute-force MaxSim in the overwhelming majority of cases, a fraction of the cost.

------------------------------------------------------------------------------------------------
HOW THIS PLUGS INTO THE REST OF step_7 (fusion hook → lib.retrieval)
------------------------------------------------------------------------------------------------
Visual retrieval returns *pages*; text retrieval returns *chunks* (letters / notebook entries). They
speak different units, so we DON'T add their scores — we fuse them with **Reciprocal Rank Fusion**,
exactly as lib.retrieval already fuses BM25 with dense (RRF only needs rank positions, so it doesn't
care that one list is pages and the other is chunks). `visual_search(...)` below returns rank-ordered
page hits whose provenance is `doc_id + section + pdf_page + masked-PNG path`, and `fuse_with_text(...)`
RRF-merges those with `lib.retrieval.retrieve(...)` text Hits into one ranked list the agent can cite.
A page hit cites the whole page (deep-zoom-able via the IIIF manifests index_sources.py emits); a
chunk hit still cites the exact region — you get both granularities in one fused list.

------------------------------------------------------------------------------------------------
⚠️ GATED — HEAVY MODEL + GPU — NOT RUN HERE
------------------------------------------------------------------------------------------------
ColPali is a multi-GB vision-language model and indexing ~2,100 page images is a GPU job. Per the
project's hard rules this stage is fully implemented but **gated behind `--run` and never executed
here**; the default `run()` only does the FREE, offline work: enumerate the page images, derive the
deterministic provenance/partition manifest, and project the storage cost. The actual embedding /
search calls are written against the real `colpali-engine` + `transformers` API and are cost-logged
(GPU compute is local/$0 in the cloud-rental sense, but we log it so the ledger shows the work).

    DEPENDENCIES (install into step_7/venv ONLY when David says go — none are present now):
        pip install colpali-engine>=0.3 transformers>=4.46 torch  pillow
        # GPU strongly recommended (a ~3B VLM). CPU "works" but is impractical for the full corpus.
    MODELS (config.VISUAL_RETRIEVERS): "vidore/colpali-v1.3" (PaliGemma-based) or
        "vidore/colqwen2-v1.0" (Qwen2-VL-based, usually stronger; bigger). Swappable, like every
        other axis in step_7.

    TODO(go): 1) add the deps above to the venv; 2) run `python stages/colpali_index.py --run
        [--limit N]` on a GPU box to build data/colpali/<model>/; 3) flip lib.retrieval to call
        `fuse_with_text` for image-bearing queries (or add a `use_visual` toggle to RetrievalConfig).

Non-destructive (writes only under data/colpali/); imports follow every other lib/stages module.
"""
from __future__ import annotations

# ============================================================================
# Imports — grouped (stdlib / third-party / local) the way the other modules do
# ============================================================================

# Core Python Imports
import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Third-Party Imports
# NOTE: numpy is the only third-party import we take at module load — it's already a dependency of
# the vector store, so importing this module stays cheap and NEVER pulls in torch/transformers. The
# heavy ML imports live *inside* the gated functions, so `import colpali_index` (e.g. for the manifest
# helpers or from lib.retrieval's fusion hook) costs nothing and needs no GPU stack installed.
import numpy as np

# Local File Imports — the same sys.path bootstrap every stage uses, so `import config` and
# `from lib import ...` resolve no matter what the caller's cwd is.
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config              # noqa: E402  (paths + model registry + prices)
from lib import costlog    # noqa: E402  (route every model/compute call through here)


# ============================================================================
# Configuration — the visual-retriever axis (mirrors config's other model tables)
# ============================================================================
# We keep these local rather than editing config.py so this scaffold is self-contained; the natural
# home once it ships is a `config.VISUAL_RETRIEVERS` table next to LLMS / EMBEDDINGS / RERANKERS, so
# the model becomes a swappable variable like everything else. (TODO: promote to config.py on `go`.)
VISUAL_RETRIEVERS = {
    # provider "local-gpu": runs on our own/rented GPU, so the $ is compute we log for the ledger,
    # not an API bill. patch_dim / approx patches drive the storage projection below.
    "vidore/colpali-v1.3":  {"provider": "local-gpu", "patch_dim": 128, "approx_patches": 1030,
                             "base": "PaliGemma-3B"},
    "vidore/colqwen2-v1.0": {"provider": "local-gpu", "patch_dim": 128, "approx_patches": 1030,
                             "base": "Qwen2-VL-2B"},
}
DEFAULT_VISUAL = "vidore/colpali-v1.3"

# A nominal GPU compute price so the cost ledger reflects the work (it is NOT an API charge). Tune to
# your actual rented-GPU hourly rate; left tiny + clearly a placeholder, like config's price tables.
GPU_USD_PER_PAGE = 0.0   # placeholder — set to (gpu_hourly / pages_per_hour) on a real run

# Page-image subfolders to look inside, SAME order/convention as stages/index_sources.py so the two
# stages resolve the identical PNG for a given (section, pdf_page). Source pages first — that's where
# the letters and notebook entries (and thus the answers) live.
_PAGE_BUCKETS = ("1_source_pages", "2_post_pages", "0_table_of_contents")


# ============================================================================
# data/colpali/ partition layout — one partition per visual-retriever model
# ============================================================================
# Mirrors lib.vectorstore's "one folder per space" idea, but a page carries MANY vectors, so a flat
# vectors.npy won't do. Layout (everything under config.DATA / "colpali" / <model-slug>/):
#
#   data/colpali/<model-slug>/
#       manifest.json        one row per indexed page: {page_uid, doc_id, section, pdf_page,
#                            kind, png (abs path), n_patches} — the join key + citation provenance.
#                            Written by the FREE path so the index is *planned* before any GPU spend.
#       coarse.npy           Phase-1 matrix [n_pages, patch_dim]: the mean-pooled page vectors,
#                            L2-normalized (dot == cosine). Row i ↔ manifest[i]. (Phase-1 shortlist.)
#       page_uids.json       [page_uid, ...] aligned to coarse.npy rows (so a shortlist index maps
#                            back to a page without re-reading the manifest).
#       patches/<page_uid>.npy   Phase-2 FULL multi-vector tensor for one page: float16
#                            [n_patches, patch_dim]. Loaded ONLY for shortlisted pages at query time.
#       meta.json            {model, patch_dim, n_pages, built_at, config_fingerprint, dtype} — lets
#                            a diff-and-rerun detect a stale index when the model/config changes.
#
# Why split coarse (one .npy) from patches (per-page .npy)? Phase 1 streams one small matrix into RAM
# and scores every page fast; Phase 2 random-reads only the ~100 page tensors it actually needs, so
# we never hold all ~2.1M patch vectors in memory at once. float16 halves patch storage at no real
# recall cost (MaxSim is robust to it). See estimate_storage() for the back-of-envelope.
def partition_dir(model: str) -> Path:
    """Folder for a visual-retriever model's index (slug-safe, like lib.vectorstore._dir)."""
    return config.DATA / "colpali" / model.replace("/", "_")


def _png_path(section: str, pdf_page: int) -> Optional[Path]:
    """Resolve the masked page PNG for a record, EXACTLY as stages/index_sources.py does.

    The stable key tying a documents.json / notebooks.json record to its scan is
    ``(section, pdf_page_number)`` → ``<ENRICHED>/<section>/<bucket>/page_<NNN>/page_<NNN>.masked.png``.
    We try each bucket (source pages first) and return the first that exists.

    Args:
        section:  the record's ``section`` (e.g. "Volume_1", "Appendix_2").
        pdf_page: 1-based PDF page number over the whole volume (the record's ``pdf_page_number``).

    Returns:
        The masked-PNG Path if present, else None (we skip pages we can't locate rather than crash —
        the corpus is large and a few strays must not fail the whole index build).
    """
    name = f"page_{pdf_page:03d}"
    base = config.ENRICHED / section
    for bucket in _PAGE_BUCKETS:
        png = base / bucket / name / f"{name}.masked.png"
        if png.is_file():
            return png
    return None


# ============================================================================
# Enumerate the page images to index (FREE — no model, just walk the corpus)
# ============================================================================
# A page can host several records (a letter spans pages; a notebook page holds many entries). For
# VISUAL retrieval the unit is the PAGE, so we deduplicate to one entry per (section, pdf_page) and
# remember which doc_ids live there (handy provenance: "this page contains letter X / entries of Y").
def enumerate_pages(limit: Optional[int] = None) -> list[dict]:
    """List the distinct page images to index, with citation provenance, from step_6 json.

    Reads documents.json + notebooks.json (same sources as lib.chunks), maps each record to its
    masked PNG, and collapses to one row per physical page. Pure/offline — no model call, no write.

    Args:
        limit: Optional cap (strided across the corpus) for a smoke run / storage projection.

    Returns:
        A list of page dicts, each:
            {"page_uid": "<section>__pdf<NNN>", "doc_id": "<first record id on the page>",
             "doc_ids": [...all record ids on the page...], "section": str, "pdf_page": int,
             "kind": "letter"|"notebook_page"|"mixed", "png": "<abs path to masked PNG>"}
    """
    pages: dict[str, dict] = {}                     # page_uid -> row (dedup across records)

    def _add(rec: dict, kind: str) -> None:
        section = rec.get("section")
        pdf_page = rec.get("pdf_page_number")
        if section is None or pdf_page is None:
            return                                  # can't locate it without the join key → skip
        png = _png_path(section, int(pdf_page))
        if png is None:
            return                                  # image missing on disk → skip (don't crash)
        uid = f"{section}__pdf{int(pdf_page):03d}"
        row = pages.get(uid)
        if row is None:
            pages[uid] = {"page_uid": uid, "doc_id": rec["id"], "doc_ids": [rec["id"]],
                          "section": section, "pdf_page": int(pdf_page), "kind": kind,
                          "png": str(png.resolve())}
        else:
            row["doc_ids"].append(rec["id"])
            if row["kind"] != kind:                 # both a letter and notebook records on one page
                row["kind"] = "mixed"

    for d in json.loads(config.DOCUMENTS.read_text()):
        _add(d, "letter")
    for p in json.loads(config.NOTEBOOKS.read_text()):
        _add(p, "notebook_page")

    rows = sorted(pages.values(), key=lambda r: (r["section"], r["pdf_page"]))
    if limit and limit < len(rows):
        step = len(rows) / limit                    # strided sample → spans all sections, like chunks.py
        rows = [rows[int(i * step)] for i in range(limit)]
    return rows


def estimate_storage(model: str = DEFAULT_VISUAL, limit: Optional[int] = None) -> dict:
    """Project the on-disk size of the multi-vector index BEFORE spending any GPU time (FREE).

    Late interaction is storage-hungry — this is the number to look at before you commit. Per page we
    store ~``approx_patches`` × ``patch_dim`` float16 patch vectors, plus a tiny coarse row. We report
    both so "is this worth it?" is answerable up front.

    Args:
        model: which entry of VISUAL_RETRIEVERS to size.
        limit: optional page cap (matches what you'd pass to run()/build).

    Returns:
        {"model", "n_pages", "patch_dim", "approx_patches", "patches_gb", "coarse_mb", "total_gb"}.
    """
    spec = VISUAL_RETRIEVERS[model]
    n_pages = len(enumerate_pages(limit=limit))
    bytes_per_patch_vec = spec["patch_dim"] * 2     # float16 = 2 bytes/value
    patches_bytes = n_pages * spec["approx_patches"] * bytes_per_patch_vec
    coarse_bytes = n_pages * spec["patch_dim"] * 4  # coarse.npy is float32
    return {"model": model, "n_pages": n_pages, "patch_dim": spec["patch_dim"],
            "approx_patches": spec["approx_patches"],
            "patches_gb": round(patches_bytes / 1e9, 3),
            "coarse_mb": round(coarse_bytes / 1e6, 3),
            "total_gb": round((patches_bytes + coarse_bytes) / 1e9, 3)}


# ============================================================================
# Model loading — the heavy ColPali / ColQwen VLM (GATED: imports torch lazily)
# ============================================================================
# Cached so a multi-call session (build then search) loads the multi-GB model once. The imports are
# INSIDE the function on purpose: nothing here touches torch/transformers until you actually opt in,
# so the manifest/fusion helpers stay importable on a machine with no GPU stack at all.
_MODEL_CACHE: dict = {}


def _load_model(model: str):
    """Load (and cache) the ColPali/ColQwen model + processor. HEAVY — only call on the --run path.

    Picks the right colpali-engine classes by model family (ColPali for the PaliGemma base, ColQwen2
    for the Qwen2-VL base). Uses bfloat16 + device_map="cuda" when a GPU is present (these models are
    far too slow on CPU for the full corpus). Returns whatever the caller needs to embed.

    Args:
        model: a key of VISUAL_RETRIEVERS.

    Returns:
        (engine, processor, device) — `engine` is the loaded model, `processor` turns PIL images /
        text into the model's inputs.
    """
    if model in _MODEL_CACHE:
        return _MODEL_CACHE[model]

    # Heavy imports, deferred to here so the module stays light until you opt in.
    import torch                                            # noqa: F401  (also picks device below)
    from colpali_engine.models import (                    # type: ignore  (installed only on `go`)
        ColPali, ColPaliProcessor, ColQwen2, ColQwen2Processor,
    )

    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.bfloat16 if device == "cuda" else torch.float32
    base = VISUAL_RETRIEVERS[model]["base"].lower()

    # ColQwen2 vs ColPali have different wrapper classes but an identical call surface downstream.
    if "qwen" in base:
        engine = ColQwen2.from_pretrained(model, torch_dtype=dtype, device_map=device).eval()
        processor = ColQwen2Processor.from_pretrained(model)
    else:
        engine = ColPali.from_pretrained(model, torch_dtype=dtype, device_map=device).eval()
        processor = ColPaliProcessor.from_pretrained(model)

    _MODEL_CACHE[model] = (engine, processor, device)
    return _MODEL_CACHE[model]


def _embed_images(pngs: list[Path], model: str, batch: int = 4) -> list[np.ndarray]:
    """Encode page images into per-page multi-vector patch tensors (HEAVY, GPU). Cost-logged.

    Each returned array is [n_patches, patch_dim] float16 — the late-interaction representation of
    one page. We batch (VLMs are memory-hungry) and log GPU compute per batch so the ledger reflects
    the indexing work even though it isn't an API bill.

    Args:
        pngs:  list of masked-PNG Paths (from enumerate_pages).
        model: a key of VISUAL_RETRIEVERS.
        batch: images per forward pass (keep small — these are big models).

    Returns:
        list[np.ndarray], one [n_patches, patch_dim] float16 tensor per input image, in input order.
    """
    import torch
    from PIL import Image
    engine, processor, device = _load_model(model)

    out: list[np.ndarray] = []
    for i in range(0, len(pngs), batch):
        chunk = pngs[i:i + batch]
        images = [Image.open(p).convert("RGB") for p in chunk]
        inputs = processor.process_images(images).to(device)
        with torch.no_grad():
            embs = engine(**inputs)                         # [B, n_patches, patch_dim]
        # Move to CPU float16 and split per image (variable n_patches across pages is fine).
        for e in embs:
            out.append(e.to(torch.float16).cpu().numpy())
        # Compute logged as GPU work (provider "local-gpu"); $ is a placeholder rate, not an API bill.
        costlog.log("local-gpu", model, "colpali_embed_image", items=len(chunk),
                    usd=GPU_USD_PER_PAGE * len(chunk), meta=f"phase=index/batch{i // batch}")
    return out


def _embed_query(query: str, model: str) -> np.ndarray:
    """Encode a text query into its per-token multi-vector tensor (HEAVY, GPU). Cost-logged.

    Returns:
        np.ndarray [n_query_tokens, patch_dim] float32 — the query side of the late interaction.
    """
    import torch
    engine, processor, device = _load_model(model)
    inputs = processor.process_queries([query]).to(device)
    with torch.no_grad():
        q = engine(**inputs)                               # [1, n_tokens, patch_dim]
    costlog.log("local-gpu", model, "colpali_embed_query", items=1,
                usd=GPU_USD_PER_PAGE, meta="phase=query")
    return q[0].to(torch.float32).cpu().numpy()


# ============================================================================
# MaxSim + mean-pool — the two scoring primitives (pure numpy; no model needed)
# ============================================================================
# These are deliberately model-free so they're unit-testable and reusable: feed them arrays and they
# return scores. _mean_pool builds the Phase-1 coarse vectors; maxsim is the Phase-2 exact score.
def _l2norm(v: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize so dot product == cosine (same convention as lib.providers.embed)."""
    n = np.linalg.norm(v, axis=-1, keepdims=True)
    n[n == 0] = 1.0
    return v / n


def _mean_pool(patches: np.ndarray) -> np.ndarray:
    """Collapse a page's [n_patches, dim] tensor to ONE [dim] vector by averaging, then normalize.

    This is the Phase-1 approximation: we throw away *where* on the page each patch was and keep only
    the page's average appearance. Lossy on purpose — it's only there to cheaply shortlist; Phase 2's
    MaxSim restores the spatial detail on the survivors.
    """
    return _l2norm(patches.astype(np.float32).mean(axis=0, keepdims=True))[0]


def maxsim(query_vecs: np.ndarray, page_patches: np.ndarray) -> float:
    """The late-interaction score: for each query token, take its BEST match over the page patches,
    then sum those maxima. This is the heart of ColPali/ColBERT.

    Mechanically: build the [n_query_tokens, n_patches] similarity matrix (query · patchesᵀ), take
    the max along the patch axis (each query token's best spot on the page), and sum. Higher = the
    page answers more of the query, somewhere on it.

    Args:
        query_vecs:   [n_query_tokens, dim] (will be L2-normalized here).
        page_patches: [n_patches, dim]      (will be L2-normalized here).

    Returns:
        The scalar MaxSim score (float).
    """
    q = _l2norm(query_vecs.astype(np.float32))
    p = _l2norm(page_patches.astype(np.float32))
    sim = q @ p.T                                          # [n_tokens, n_patches]
    return float(sim.max(axis=1).sum())                   # Σ over query tokens of best-patch sim


# ============================================================================
# BUILD — index the page images into data/colpali/<model>/ (GATED, --run only)
# ============================================================================
def build(model: str = DEFAULT_VISUAL, limit: Optional[int] = None) -> Path:
    """Encode every page image and write the two-phase index (coarse.npy + patches/ + manifest).

    HEAVY + GPU — this is the gated path. It walks enumerate_pages(), embeds each page to its patch
    tensor, mean-pools it for the coarse matrix, and persists the partition layout documented above.
    Idempotent: re-running overwrites the same partition (non-destructive to *source* data — it only
    writes under data/colpali/).

    Args:
        model: which VISUAL_RETRIEVERS entry to build.
        limit: optional page cap (smoke run).

    Returns:
        The partition directory Path it wrote.
    """
    pdir = partition_dir(model)
    (pdir / "patches").mkdir(parents=True, exist_ok=True)

    rows = enumerate_pages(limit=limit)
    pngs = [Path(r["png"]) for r in rows]
    patch_tensors = _embed_images(pngs, model)            # HEAVY (cost-logged inside)

    # ---- write per-page full tensors (Phase 2) + build the coarse matrix (Phase 1) -------------
    coarse = np.zeros((len(rows), VISUAL_RETRIEVERS[model]["patch_dim"]), dtype=np.float32)
    for i, (row, patches) in enumerate(zip(rows, patch_tensors)):
        np.save(pdir / "patches" / f"{row['page_uid']}.npy", patches)   # float16, [n_patches, dim]
        coarse[i] = _mean_pool(patches)                                  # one row per page
        row["n_patches"] = int(patches.shape[0])

    np.save(pdir / "coarse.npy", coarse)
    (pdir / "page_uids.json").write_text(json.dumps([r["page_uid"] for r in rows]))
    with open(pdir / "manifest.json", "w") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)
    (pdir / "meta.json").write_text(json.dumps({
        "model": model, "patch_dim": VISUAL_RETRIEVERS[model]["patch_dim"],
        "n_pages": len(rows), "dtype": "float16",
        "config_fingerprint": config.fingerprint(),     # stale-detect on config change (diff-and-rerun)
    }, indent=2))
    return pdir


# ============================================================================
# Page hit shape — what visual_search returns (parallel to lib.retrieval.Hit)
# ============================================================================
@dataclass
class PageHit:
    """One visually-retrieved PAGE, carrying citation provenance to the masked scan + IIIF canvas."""
    page_uid: str                                   # "<section>__pdf<NNN>"
    score: float                                    # MaxSim (or coarse score on the fast path)
    section: str = ""
    pdf_page: int = 0
    kind: str = ""                                  # "letter" | "notebook_page" | "mixed"
    doc_ids: list = field(default_factory=list)     # records living on this page (for cross-citation)
    png: str = ""                                   # absolute path to the masked PNG
    provenance: dict = field(default_factory=dict)  # citation-ready (doc_id, section, pdf_page, png)
    components: dict = field(default_factory=dict)  # debug: coarse vs maxsim scores

    def to_dict(self) -> dict:
        return {"page_uid": self.page_uid, "score": self.score, "section": self.section,
                "pdf_page": self.pdf_page, "kind": self.kind, "doc_ids": self.doc_ids,
                "png": self.png, "provenance": self.provenance, "components": self.components}


def _load_partition(model: str):
    """Read a built partition's coarse matrix + manifest (raises if the index isn't built yet)."""
    pdir = partition_dir(model)
    coarse = np.load(pdir / "coarse.npy")
    uids = json.loads((pdir / "page_uids.json").read_text())
    manifest = {r["page_uid"]: r for r in json.loads((pdir / "manifest.json").read_text())}
    return pdir, coarse, uids, manifest


# ============================================================================
# SEARCH — two-phase visual retrieval (GATED for embedding; phases are pure numpy)
# ============================================================================
def visual_search(query: str, model: str = DEFAULT_VISUAL,
                  coarse_k: int = 100, top_k: int = 10) -> list[PageHit]:
    """Two-phase visual retrieval over the page-image index → ranked PageHits with provenance.

    Phase 1 (coarse): embed the query, mean-pool it, dot-product against every page's coarse vector,
    keep the top ``coarse_k`` (default 100). Phase 2 (fine): load only those pages' FULL patch
    tensors and compute the exact MaxSim, then return the top ``top_k`` re-sorted by it.

    The only HEAVY part is embedding the query (one GPU forward pass, cost-logged in `_embed_query`);
    both phases' arithmetic is pure numpy over the prebuilt index. Requires `build(...)` to have run.

    Args:
        query:    the user's question (text; image queries are a future extension).
        model:    which built partition to search.
        coarse_k: Phase-1 shortlist size (generous — Phase 1 is a lossy approximation).
        top_k:    final number of pages to return.

    Returns:
        Ranked list[PageHit], best-first, length ≤ top_k.
    """
    pdir, coarse, uids, manifest = _load_partition(model)

    # ---- Phase 1: coarse shortlist (cheap; one dot product per page) --------------------------
    qtok = _embed_query(query, model)                     # HEAVY: [n_tokens, dim] (cost-logged)
    qmean = _mean_pool(qtok)                              # mean-pool the query side too, for Phase 1
    coarse_scores = coarse @ qmean                        # [n_pages]
    shortlist = np.argsort(-coarse_scores)[:coarse_k]     # indices of the top coarse_k pages

    # ---- Phase 2: exact MaxSim on the shortlist only (load just those tensors) ----------------
    scored: list[tuple[int, float]] = []
    for idx in shortlist:
        uid = uids[int(idx)]
        patches = np.load(pdir / "patches" / f"{uid}.npy")
        scored.append((int(idx), maxsim(qtok, patches)))
    scored.sort(key=lambda t: t[1], reverse=True)

    # ---- assemble PageHits with citation provenance ------------------------------------------
    hits: list[PageHit] = []
    for idx, ms in scored[:top_k]:
        row = manifest[uids[idx]]
        hits.append(PageHit(
            page_uid=row["page_uid"], score=ms, section=row["section"], pdf_page=row["pdf_page"],
            kind=row.get("kind", ""), doc_ids=row.get("doc_ids", []), png=row.get("png", ""),
            provenance={"doc_id": row.get("doc_id"), "section": row["section"],
                        "pdf_page": row["pdf_page"], "png": row.get("png")},
            components={"coarse": float(coarse_scores[idx]), "maxsim": ms}))
    return hits


# ============================================================================
# FUSION HOOK — RRF-merge visual page hits with lib.retrieval text Hits
# ============================================================================
# This is the seam the research plan asks for ("Run as its own tool and fuse (RRF) with text
# retrieval"). We reuse lib.retrieval's *own* RRF so fusion stays consistent with how BM25+dense are
# already combined — RRF only needs rank positions, so it happily merges a list of pages with a list
# of chunks. We don't add scores (a MaxSim sum and an RRF score aren't comparable); we fuse RANKS.
def fuse_with_text(query: str, model: str = DEFAULT_VISUAL,
                   text_cfg=None, coarse_k: int = 100, top_k: int = 10,
                   rrf_k: int = 60, weights: Optional[list[float]] = None) -> list[dict]:
    """Retrieve visually AND textually, then RRF-fuse into one provenance-carrying ranked list.

    Visual hits are keyed by ``page_uid`` and text hits by chunk ``id``; to fuse them in one RRF we
    give visual hits a synthetic key (``visual::<page_uid>``) so they never collide with chunk ids,
    then merge by rank. The result interleaves "whole pages the image model liked" with "exact
    regions the text model liked" — the agent can cite either granularity.

    HEAVY only in its visual leg (the query image-encode); the text leg is lib.retrieval's free
    hybrid path unless its own cfg toggles a paid step. Requires a built visual partition.

    Args:
        query:    the user's question.
        model:    visual partition to search.
        text_cfg: a lib.retrieval.RetrievalConfig (None → its free defaults).
        coarse_k, top_k: visual two-phase knobs.
        rrf_k:    RRF damping constant (60 = lib.retrieval's default — keep them aligned).
        weights:  optional [visual_weight, text_weight] to trust one leg more (defaults equal).

    Returns:
        A fused ranked list of dicts: {"key", "modality": "visual"|"text", "rrf_score",
        "hit": <PageHit.to_dict() | Hit.to_dict()>} — best-first, ready to cite or hand a generator.
    """
    # Import lib.retrieval lazily so this module is importable even if a future refactor makes
    # retrieval depend back on us (avoids an import cycle) — and so the FREE manifest helpers above
    # never drag in the retrieval stack. Both are step_7 modules; the cost is trivial either way.
    from lib import retrieval

    # ---- visual leg (HEAVY: query encode) ----------------------------------------------------
    page_hits = visual_search(query, model=model, coarse_k=coarse_k, top_k=top_k)
    visual_list = [{"id": f"visual::{h.page_uid}"} for h in page_hits]   # rank-ordered keys for RRF

    # ---- text leg (lib.retrieval's hybrid path; free unless its cfg opts into paid steps) -----
    text_hits = retrieval.retrieve(query, text_cfg)
    text_list = [{"id": h.id} for h in text_hits]                        # rank-ordered chunk keys

    # ---- fuse RANKS with lib.retrieval's own RRF (consistent with BM25↔dense fusion) ----------
    fused = retrieval.reciprocal_rank_fusion([visual_list, text_list], k=rrf_k, weights=weights)

    # ---- reattach the full hit object (page or chunk) behind each fused key -------------------
    by_page = {f"visual::{h.page_uid}": ("visual", h) for h in page_hits}
    by_chunk = {h.id: ("text", h) for h in text_hits}
    out: list[dict] = []
    for row in fused:
        key = row["id"]
        if key in by_page:
            modality, hit = by_page[key]
        elif key in by_chunk:
            modality, hit = by_chunk[key]
        else:
            continue
        out.append({"key": key, "modality": modality, "rrf_score": row["score"],
                    "hit": hit.to_dict()})
    return out


# ============================================================================
# run() — the stage entry point (FREE/offline by default; --run gates the GPU work)
# ============================================================================
def run(model: str = DEFAULT_VISUAL, limit: Optional[int] = None, do_build: bool = False) -> dict:
    """Default = the FREE plan: enumerate the page images, write the provenance/partition MANIFEST,
    and project storage cost — NO model load, NO GPU, NO paid call. Pass do_build=True (CLI --run) to
    actually build the index (heavy, gated). This keeps the pipeline runnable here while leaving the
    expensive work behind an explicit opt-in, per the project's hard rules.

    Args:
        model:    which VISUAL_RETRIEVERS entry this partition targets.
        limit:    optional page cap (smoke run / projection).
        do_build: True → run the heavy GPU index build; False (default) → free plan only.

    Returns:
        A small summary dict (also written to the partition as plan.json on the free path).
    """
    config.DATA.mkdir(parents=True, exist_ok=True)
    pdir = partition_dir(model)
    pdir.mkdir(parents=True, exist_ok=True)

    rows = enumerate_pages(limit=limit)
    storage = estimate_storage(model, limit=limit)

    # FREE artifact: the manifest is fully determinable WITHOUT the model, so we write it now. The
    # build step later fills in n_patches + the vectors; planning the index up front means the rest
    # of the system (and the storage projection) can reason about it before any GPU spend.
    plan = {"model": model, "n_pages": len(rows), "storage_projection": storage,
            "built": False, "config_fingerprint": config.fingerprint()}
    (pdir / "plan.json").write_text(json.dumps(plan, indent=2))
    with open(pdir / "manifest.json", "w") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)

    if do_build:
        # GATED heavy path — only reached via CLI --run (and only after deps are installed on a GPU).
        build(model, limit=limit)
        plan["built"] = True
    return plan


# ============================================================================
# CLI — FREE by default (plan + projection). `--run` gates the heavy GPU build.
# ============================================================================
# Examples:
#   python stages/colpali_index.py                         # FREE: plan + storage projection, no GPU
#   python stages/colpali_index.py --limit 50              # FREE: projection over a 50-page sample
#   python stages/colpali_index.py --run --limit 50        # HEAVY: build a 50-page smoke index (GPU)
#   python stages/colpali_index.py --run                   # HEAVY: full ~2,100-page index (GPU)
#   python stages/colpali_index.py --search "cancer cure"  # HEAVY: query a built index (GPU encode)
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tier-3 ColPali/ColQwen visual page retrieval (gated)")
    ap.add_argument("--model", default=DEFAULT_VISUAL, choices=list(VISUAL_RETRIEVERS))
    ap.add_argument("--limit", type=int, default=None, help="cap pages (strided sample)")
    ap.add_argument("--run", action="store_true", help="HEAVY: actually build the index (needs GPU + deps)")
    ap.add_argument("--search", default=None, help="HEAVY: query a built index (needs GPU + deps)")
    a = ap.parse_args()

    if a.search:
        # Heavy: requires a built partition + the GPU stack installed.
        for i, h in enumerate(visual_search(a.search, model=a.model), 1):
            print(f"[{i}] maxsim={h.score:.3f}  {h.section} pdf_pg={h.pdf_page}  {h.kind}")
            print(f"     {h.png}")
    else:
        plan = run(model=a.model, limit=a.limit, do_build=a.run)
        s = plan["storage_projection"]
        print(f"model: {plan['model']}   pages to index: {plan['n_pages']}   built: {plan['built']}")
        print(f"storage projection: patches {s['patches_gb']} GB + coarse {s['coarse_mb']} MB "
              f"= {s['total_gb']} GB  ({s['approx_patches']} patches x {s['patch_dim']}-d float16/page)")
        # Show the ledger so a FREE run visibly logged $0 / nothing paid (mirrors lib.retrieval).
        print("=" * 60)
        print("COST SUMMARY:", json.dumps(costlog.summary(), indent=2))
