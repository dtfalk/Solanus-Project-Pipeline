"""stages/raptor.py — Tier-3 RAPTOR: a recursive cluster-and-summarize tree over the corpus.

Most of our retrieval is *bottom-up*: a query meets the single chunk that happens to share its
words or its meaning. That is perfect for a pin-point question ("what favor did Grace Panyard report
in November 1933?"), but it is the wrong shape for a **global / thematic** question —

    "What kinds of conditions did people most often write to Fr. Solanus about?"
    "How did the Casey family's correspondence network change over the decades?"
    "What were the recurring spiritual themes across the notebooks?"

No single chunk answers those. The answer is *distributed* across hundreds of terse entries, and a
top-k of 8 leaf chunks can only ever see a thin slice of it. You'd have to read the whole corpus to
answer well — which is exactly the gap **RAPTOR** (Recursive Abstractive Processing for Tree-
Organized Retrieval, Sarthi et al. 2024) closes.

The idea is beautifully simple, and it mirrors how a historian actually reads an archive:

  1. Embed every leaf chunk (we already do this in embed_corpus.py).
  2. **Cluster** the leaf embeddings into groups of *semantically related* chunks — a cluster might
     gather a few dozen entries that are all about cancer cures, or all about a single family.
  3. **Summarize** each cluster with an LLM into one short abstractive node — a paragraph that says
     what the whole cluster is *about* (the theme, the recurring outcome, the span of dates).
  4. **Recurse:** treat those summaries as a new layer of "documents", embed them, cluster *them*,
     and summarize again — until the top of the tree is a handful of nodes describing the corpus at
     its coarsest, most thematic altitude.

You end up with a tree: leaves are exact entries (fine, citable), the middle layers are thematic
groupings, and the root layer is the 10,000-foot view. At query time, retrieval can search **all
levels at once** ("collapsed-tree" retrieval, which the RAPTOR paper found beats tree-walking): a
thematic question naturally pulls in the high-level summary nodes, a specific question still pulls
the exact leaf — *coarse-to-fine in a single index*. On long-document QA the paper reports ~+20%.

How this plugs into our existing machinery (no new infrastructure):
  - We reuse the **same embedding adapter** (lib.providers.embed) and the **same partitioned vector
    store** (lib.vectorstore). The summary nodes get embedded into their own partition, named
    "raptor::<model>@<dim>" — a sibling of the leaf partition "<model>@<dim>". lib.retrieval can
    then fuse the two (search leaves + search raptor summaries, RRF them) so a query gets both the
    pin-point chunk and the thematic summary. The "raptor" prefix is the **vector partition** the
    task asks for; nothing else in the store changes.
  - Every summary node carries provenance: the leaf chunk ids beneath it (transitively), so even a
    root-level summary can be expanded down to the exact regions/pages it was built from. Citations
    survive all the way up the tree.

What this stage writes (NON-DESTRUCTIVE — new artifacts only):
    data/raptor_tree.json                 # the full tree: nodes by level, children, members, text
    data/vectors/raptor::<model>@<dim>/   # the summary-node embeddings (one partition per space)

⚠️ COST — the summarization in step (3) is a PAID LLM pass (one call per cluster, at every level).
It is fully wired but **GATED OFF**: run() will build the tree's *structure* (clustering is free —
it's pure numpy over embeddings we already have) and lay out every summarization prompt, but it will
NOT call the LLM unless you pass execute=True (CLI: --execute). Until then each node's `summary` is
left empty and the prompt is saved so you can inspect/cost it first. This matches the project rule:
nothing billed until David says go.

    python stages/raptor.py --limit 200            # build tree STRUCTURE only, no API calls (free)
    python stages/raptor.py --limit 40 --execute   # tiny billed smoke test (summaries + embeds)
    python stages/raptor.py --execute              # FULL paid build (only when told)

Public entry point:
    run(limit=None, ...)  ->  Path to data/raptor_tree.json
"""
from __future__ import annotations

# ------------------------------------------------------------------ Core Python Imports
import argparse
import json
import logging
import sys
import time
from pathlib import Path

# ------------------------------------------------------------------ Third-Party Imports
import numpy as np

# ------------------------------------------------------------------ Local File Imports
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                         # noqa: E402  (paths + model registry)
from lib import chunks as chunks_lib  # noqa: E402  (the canonical chunk builder — same data shapes)
from lib import costlog               # noqa: E402  (every model call is priced)
from lib import vectorstore           # noqa: E402  (the partitioned vector store we embed summaries into)
from lib.providers import embed       # noqa: E402  (model-agnostic embedding adapter)
from lib.providers import llm         # noqa: E402  (model-agnostic generation adapter)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("raptor")

# The tree artifact. New file only — we never touch documents.json / notebooks.json / the leaves.
OUT_PATH = config.DATA / "raptor_tree.json"

# How the summary-node partition is named in the vector store. A leaf space is "<model>@<dim>";
# its RAPTOR sibling is "raptor::<model>@<dim>". Keeping the suffix identical lets retrieval pair a
# leaf partition with its summary partition just by adding/removing the "raptor::" prefix.
RAPTOR_PREFIX = "raptor::"


# ==================================================================
# Tunables — clustering + tree shape (all free to change; affect structure only)
# ==================================================================
# These are the knobs that decide the *shape* of the tree. They're deliberately collected here (and
# folded into the artifact's "params" block) so the diff-and-rerun pipeline treats a change to them
# as a reason to rebuild — and so a future eval sweep can try a few settings without code edits.
DEFAULTS = {
    # Target average cluster size at the leaf level. RAPTOR clusters are meant to be "a readable
    # handful of related passages", not one giant blob — ~12 chunks/cluster is a sane middle. The
    # number of clusters per level is derived from this and the layer's node count.
    "cluster_size": 12,
    # We stop recursing when a level has this few nodes (the root layer) — there's nothing left to
    # usefully summarize once the whole corpus fits in a few thematic nodes.
    "min_top_nodes": 5,
    # A hard ceiling on tree height, so a pathological corpus can't loop forever. In practice the
    # corpus (~9k leaves) collapses to the root in ~3-4 levels.
    "max_levels": 6,
    # k-means restarts (we use a tiny dependency-free k-means; more restarts = steadier clusters).
    "kmeans_restarts": 4,
    "kmeans_iters": 50,
    # Reproducibility: the clustering is randomized (centroid init), so we seed it. Same inputs +
    # same seed => same tree, which the hashed pipeline relies on for idempotency.
    "seed": 17,
    # Cap the characters of leaf/child text we paste into a summarization prompt, so a huge cluster
    # can't blow the context window (and the per-call bill). Members beyond the cap are sampled.
    "max_prompt_chars": 12000,
}


# ==================================================================
# The shared summarization instruction (cached once) — "summarize this cluster"
# ==================================================================
# IDENTICAL for every cluster at every level, which is exactly why it belongs in the prompt cache:
# pay to process it once, reuse it across every summarization call. (Same caching logic as
# contextualize_chunks.py — see _build_system there.) Keep it byte-stable; a one-char edit
# invalidates the cache and re-bills the prefix.
SYSTEM_INSTRUCTION = """\
You are an archival historian building a thematic index of the personal correspondence and
prayer-favor notebooks of Fr. Solanus Casey (1870-1957), a Capuchin friar known for recording
petitions and reported favors (healings, conversions, and other answered prayers).

You will be given a CLUSTER: several related passages drawn from the archive (letters and/or
notebook favor-entries), or several lower-level summaries of such passages. They were grouped
together because they are semantically similar.

Write a single faithful SUMMARY of what this cluster is about, so that a search for a broad,
thematic question can find this cluster and understand it without reading every passage. Cover:
  - the dominant theme(s) (e.g. a recurring condition, a family, a place, a kind of favor/outcome);
  - notable specifics that recur (names, conditions, outcomes like "enrolled"/"cured"/"improved");
  - the rough time span and provenance (which notebooks / decade) if discernible from the passages.

Rules:
  - Use ONLY facts present in the passages. Never invent a name, date, place, or outcome.
  - Be concise and information-dense (roughly 80-150 words). This summary is itself embedded and
    retrieved, so pack in the searchable facts rather than writing flowery prose.
  - Write plain expository sentences. No preamble ("This cluster..."), no bullet lists, no quotes."""


# ==================================================================
# Step 1 — load the leaf layer (chunks + their already-built embeddings)
# ==================================================================
def _load_leaves(space: str, limit: int | None):
    """Load the leaf chunks and their embeddings for a given embedding space.

    RAPTOR starts from the corpus chunks we already embedded in embed_corpus.py — so the *honest,
    cheap* path is to read those vectors straight out of the vector store partition (zero new
    embedding calls). If that partition doesn't exist yet (e.g. a fresh smoke test before any embed
    run), we fall back to embedding a small sample on the fly so the structure can still be built and
    inspected.

    Args:
        space: the leaf embedding space, "<model>@<dim>" (e.g. "gemini-embedding-001@1536").
        limit: optional cap on the number of leaves (a strided sample via chunks_lib.build_chunks),
            used for smoke tests. None = the whole corpus.

    Returns:
        (nodes, vectors):
            nodes   — list of leaf node dicts {id, level=0, kind, text, members=[self id], meta}.
            vectors — np.ndarray [n, dim], L2-normalized (cosine == dot), aligned row-for-row to nodes.
    """
    model, dim = _parse_space(space)

    # ----- Preferred path: reuse the embeddings already in the store (free).
    try:
        ids, vecs, metas = vectorstore.load(space)
        log.info("loaded %d leaf vectors from existing partition '%s' (dim %d)", len(ids), space, vecs.shape[1])
        nodes = []
        for cid, m in zip(ids, metas):
            nodes.append({
                "id":      cid,
                "level":   0,                                  # leaves live at level 0
                "kind":    m.get("kind", "leaf"),
                "text":    m.get("text", ""),                  # store keeps a 600-char preview per chunk
                "members": [cid],                              # a leaf "covers" only itself
                "meta":    {k: v for k, v in m.items() if k != "text"},
            })
        vectors = np.asarray(vecs, dtype=np.float32)
        if limit and limit < len(nodes):
            # Strided sample so a smoke test still spans letters + notebook entries.
            step = len(nodes) / limit
            keep = [int(i * step) for i in range(limit)]
            nodes = [nodes[i] for i in keep]
            vectors = vectors[keep]
        return nodes, _l2_normalize(vectors)
    except FileNotFoundError:
        log.warning("no leaf partition '%s' yet — embedding a fresh sample (run embed_corpus.py first "
                    "for the real build). This is the smoke-test fallback.", space)

    # ----- Fallback path: build leaves from the chunker and embed them now.
    #   embed.embed_texts cost-logs the call; with a local model (bge-*) it is $0, with a hosted
    #   model it is a real (small) charge — so this fallback is only sensible for tiny --limit runs.
    cs = chunks_lib.build_chunks(limit=limit)
    texts = [c["text"] for c in cs]
    vecs, _ = embed.embed_texts(texts, model, dim, task="document")
    nodes = [{
        "id":      c["id"],
        "level":   0,
        "kind":    c["kind"],
        "text":    c["text"][:600],
        "members": [c["id"]],
        "meta":    {**c["meta"], "kind": c["kind"]},
    } for c in cs]
    return nodes, _l2_normalize(np.asarray(vecs, dtype=np.float32))


# ==================================================================
# Step 2 — clustering (dependency-free; the FREE half of RAPTOR)
# ==================================================================
# The RAPTOR paper uses UMAP + Gaussian-Mixture soft clustering. That pulls in heavy optional deps
# (umap-learn, scikit-learn) which we're told not to download here. So we implement a small, honest
# **spherical k-means** over the (already L2-normalized) embeddings: on the unit sphere, maximizing
# cosine similarity to a centroid is exactly what k-means-with-cosine does, and it needs nothing but
# numpy. It produces *hard* clusters (each node in one cluster) rather than GMM's soft membership —
# a deliberate, documented simplification.
#
# TODO(raptor/clustering): for the full research build, swap this for the paper's UMAP→GMM with a
#   BIC-chosen k and soft (multi-parent) membership. The function contract below (vectors -> list of
#   index-lists) is all the rest of the stage depends on, so it's a drop-in replacement. Keep the
#   call cost-free (clustering touches no API), and keep it seeded for reproducibility.
def _spherical_kmeans(vectors: np.ndarray, k: int, *, restarts: int, iters: int, seed: int):
    """Cluster L2-normalized vectors into k groups by cosine similarity (spherical k-means).

    Why this works on normalized vectors: once every vector has length 1, the squared Euclidean
    distance is 2 - 2*cos, so "closest centroid in Euclidean distance" and "most-similar centroid in
    cosine" are the *same* assignment. We re-normalize centroids each iteration to keep them on the
    sphere. We run a few random restarts and keep the assignment with the highest total within-
    cluster similarity (the spherical analogue of lowest inertia) — k-means is sensitive to its
    random init, and a handful of restarts buys a lot of stability for almost no cost.

    Args:
        vectors: np.ndarray [n, d], assumed L2-normalized.
        k: number of clusters to form (caller derives this from the target cluster size).
        restarts: how many random initializations to try.
        iters: max Lloyd iterations per restart.
        seed: RNG seed (reproducible clustering => reproducible tree).

    Returns:
        clusters: list of length k, each a list of row-indices into `vectors`. Empty clusters are
            dropped, so the returned list may be shorter than k.
    """
    n = vectors.shape[0]
    k = max(1, min(k, n))                          # can't have more clusters than points
    best_labels, best_score = None, -np.inf
    rng = np.random.default_rng(seed)

    for r in range(restarts):
        # ----- Init: k distinct random points as the starting centroids (k-means++ is overkill here).
        start = rng.choice(n, size=k, replace=False)
        centroids = vectors[start].copy()
        labels = np.zeros(n, dtype=np.int64)
        for _ in range(iters):
            # Assign: each point to its most-similar centroid (cosine == dot on the unit sphere).
            sims = vectors @ centroids.T                       # [n, k] similarity matrix
            new_labels = np.argmax(sims, axis=1)
            if np.array_equal(new_labels, labels):
                break                                          # converged — assignments stopped moving
            labels = new_labels
            # Update: each centroid = normalized mean of its members (the spherical centroid).
            for c in range(k):
                members = vectors[labels == c]
                if len(members):
                    v = members.mean(axis=0)
                    nrm = np.linalg.norm(v)
                    centroids[c] = v / nrm if nrm else centroids[c]
        # Score this restart by total similarity of points to their assigned centroid (higher=tighter).
        score = float((vectors * centroids[labels]).sum())
        if score > best_score:
            best_score, best_labels = score, labels.copy()

    clusters = [list(np.where(best_labels == c)[0]) for c in range(k)]
    return [c for c in clusters if c]                          # drop any clusters that ended up empty


def _cluster_layer(vectors: np.ndarray, params: dict):
    """Pick k from the target cluster size and cluster one layer's vectors.

    Args:
        vectors: np.ndarray [n, d] for the current layer (L2-normalized).
        params: the tunables dict (we read cluster_size + kmeans settings + seed).

    Returns:
        clusters: list of index-lists (see _spherical_kmeans).
    """
    n = vectors.shape[0]
    # Aim for ~cluster_size nodes per cluster; round up so we never ask for 0 clusters.
    k = max(1, round(n / max(1, params["cluster_size"])))
    return _spherical_kmeans(vectors, k,
                             restarts = params["kmeans_restarts"],
                             iters    = params["kmeans_iters"],
                             seed     = params["seed"])


# ==================================================================
# Step 3 — summarization prompt assembly (the PAID half — gated)
# ==================================================================
def _build_system() -> str:
    """The cached system instruction for summarization.

    Returns:
        SYSTEM_INSTRUCTION (the invariant prefix — see the caching note on contextualize_chunks).
    """
    return SYSTEM_INSTRUCTION


def _build_cluster_prompt(member_nodes: list[dict], params: dict) -> str:
    """Assemble the user-turn prompt that asks the LLM to summarize one cluster.

    We paste the member passages (or lower-level summaries) under simple numbered headers, trimming
    to params["max_prompt_chars"] so an unusually large cluster can't overflow the context window or
    the budget. When we have to trim, we *sample evenly* across the cluster rather than truncating
    the tail, so the summary still reflects the whole group, not just its first few members.

    Args:
        member_nodes: the child nodes feeding this summary (leaves at level 1, summaries above).
        params: tunables (we read max_prompt_chars).

    Returns:
        The user prompt string.
    """
    # Pull each child's most informative text: a real summary if it has one, else its chunk text.
    texts = [(n.get("summary") or n.get("text") or "").strip() for n in member_nodes]
    texts = [t for t in texts if t]

    # ----- If the cluster is large, sample members evenly so the prompt represents the whole group.
    budget = params["max_prompt_chars"]
    if sum(len(t) for t in texts) > budget and len(texts) > 1:
        keep_n = max(1, budget // max(1, (sum(len(t) for t in texts) // len(texts))))
        step = len(texts) / keep_n
        texts = [texts[int(i * step)] for i in range(keep_n)]

    body, used = [], 0
    for i, t in enumerate(texts, 1):
        block = f"[{i}] {t}"
        if used + len(block) > budget:
            break
        body.append(block)
        used += len(block)

    return (
        f"CLUSTER ({len(body)} passage(s) shown):\n\n"
        + "\n\n".join(body)
        + "\n\nWrite the single faithful summary of this cluster now."
    )


# ==================================================================
# Tree building — the recursion that ties it all together
# ==================================================================
def _summarize_cluster(member_nodes: list[dict], node_id: str, level: int,
                       model: str, params: dict, execute: bool) -> dict:
    """Create ONE summary node for a cluster: assemble the prompt, (optionally) call the LLM.

    Provenance bookkeeping is the important part here: the summary node records both its *direct*
    children (`children`, the node ids one level down) and the *transitive* leaf chunk ids it covers
    (`members`, flattened from the children). That second list is what lets even a root-level summary
    be expanded back down to the exact citable regions it was built from — citations survive all the
    way up the tree.

    Args:
        member_nodes: the child nodes in this cluster.
        node_id: the id to assign this new summary node.
        level: the level of the NEW node (children are at level-1).
        model: LLM id for the (gated) summarization call.
        params: tunables (prompt sizing).
        execute: if False, build the prompt but DO NOT call the LLM (summary stays "").

    Returns:
        The new summary node dict.
    """
    prompt = _build_cluster_prompt(member_nodes, params)

    # Flatten transitive leaf coverage so any node can cite back to exact regions.
    leaf_members: list[str] = []
    for n in member_nodes:
        leaf_members.extend(n.get("members", [n["id"]]))

    node = {
        "id":       node_id,
        "level":    level,
        "kind":     "summary",
        "summary":  "",                                       # filled below iff execute=True
        "prompt":   prompt,                                   # saved so we can inspect/cost before billing
        "children": [n["id"] for n in member_nodes],          # direct children (one level down)
        "members":  leaf_members,                             # transitive leaf chunk ids (provenance)
        "n_leaves": len(leaf_members),
        "meta":     {"n_children": len(member_nodes)},
    }

    if execute:
        # ----- The one billed call for this node. Cost is logged inside llm.generate (real tokens).
        #   Low temperature: we want a faithful, stable digest, not a creative riff.
        summary, _usage = llm.generate(prompt, model=model, system=_build_system(), temperature=0.1)
        node["summary"] = summary.strip()

    return node


def _build_tree(leaf_nodes: list[dict], leaf_vectors: np.ndarray, space: str,
                model: str, params: dict, execute: bool):
    """Build the full RAPTOR tree, level by level, bottom-up.

    The loop is the heart of RAPTOR: cluster the current layer → summarize each cluster into a new
    parent node → those parents become the next layer to cluster → repeat until the layer is small
    enough to be the root (min_top_nodes) or we hit max_levels.

    Embedding the new summaries deserves a note. To cluster level L+1 we need *vectors* for the
    level-L summaries. Those vectors only exist once the summaries are written — i.e. only when
    execute=True. So:
      - execute=True  → embed each new layer's summaries (cost-logged) and keep clustering upward.
      - execute=False → we have no summary text to embed, so we STOP after laying out level 1's
        prompts. The structure of the first parent layer (clusters + provenance + prompts) is fully
        built and inspectable for free; higher levels are deferred to the real (billed) run.

    Args:
        leaf_nodes: the level-0 nodes from _load_leaves.
        leaf_vectors: the matching [n, dim] leaf embeddings.
        space: the leaf embedding space "<model>@<dim>" (its model/dim drive summary embedding too).
        model: LLM id for summarization.
        params: tunables.
        execute: whether to make paid LLM (summarize) + embedding (summary-vector) calls.

    Returns:
        (levels, summary_nodes, summary_vectors):
            levels         — list of layers; levels[0] are leaves, levels[i>0] are summary layers.
            summary_nodes  — flat list of all summary nodes (every level above 0).
            summary_vectors— np.ndarray [m, dim] aligned to summary_nodes (empty if execute=False),
                             ready to write into the "raptor::<space>" partition.
    """
    emb_model, emb_dim = _parse_space(space)
    levels = [leaf_nodes]
    layer_vectors = leaf_vectors
    layer_nodes = leaf_nodes

    all_summary_nodes: list[dict] = []
    all_summary_vecs: list[np.ndarray] = []
    node_counter = 0

    for level in range(1, params["max_levels"] + 1):
        # ----- Stop if the layer is already small enough to be the root.
        if len(layer_nodes) <= params["min_top_nodes"]:
            log.info("level %d: %d node(s) <= min_top_nodes(%d) — root reached, stopping.",
                     level - 1, len(layer_nodes), params["min_top_nodes"])
            break

        # ----- Cluster the current layer (FREE — pure numpy over existing vectors).
        clusters = _cluster_layer(layer_vectors, params)
        log.info("level %d: clustering %d node(s) -> %d cluster(s)", level, len(layer_nodes), len(clusters))

        # ----- Summarize each cluster into a parent node (PAID iff execute=True).
        new_nodes = []
        for ci, idxs in enumerate(clusters):
            member_nodes = [layer_nodes[i] for i in idxs]
            nid = f"raptor::L{level}::n{node_counter}"
            node_counter += 1
            node = _summarize_cluster(member_nodes, nid, level, model, params, execute)
            new_nodes.append(node)
            all_summary_nodes.append(node)

        levels.append(new_nodes)

        # ----- To go higher we must embed this new layer's summaries. Only possible if we ran them.
        if not execute:
            log.info("level %d: structure built; STOPPING before higher levels (execute=False — no "
                     "summary text to embed yet). Re-run with --execute for the full tree.", level)
            break

        new_texts = [n["summary"] for n in new_nodes]
        new_vecs, _ = embed.embed_texts(new_texts, emb_model, emb_dim, task="document")
        new_vecs = _l2_normalize(np.asarray(new_vecs, dtype=np.float32))
        all_summary_vecs.append(new_vecs)

        # The new summaries become the layer we cluster next time around.
        layer_nodes = new_nodes
        layer_vectors = new_vecs

    summary_vectors = (np.vstack(all_summary_vecs)
                       if all_summary_vecs else np.zeros((0, emb_dim), dtype=np.float32))
    return levels, all_summary_nodes, summary_vectors


# ==================================================================
# Persisting into the vector store — the "raptor" partition
# ==================================================================
def _write_raptor_partition(space: str, summary_nodes: list[dict], summary_vectors: np.ndarray):
    """Embed the summary nodes into the 'raptor::<space>' vector partition for coarse-to-fine search.

    This is the payoff: by writing the summaries into a sibling partition of the leaf space, anything
    in lib.retrieval can fuse a search over leaves with a search over RAPTOR summaries (just toggle
    the 'raptor::' prefix on the space name) — so a thematic query surfaces a high-level summary node
    while a specific query still surfaces the exact leaf. We mirror the leaf store's meta shape (a
    'kind' + a 'text' preview + provenance) so the retriever needs no special-casing: a summary hit
    looks like any other hit, but carries 'members' for drill-down to citable regions.

    Args:
        space: the leaf space "<model>@<dim>".
        summary_nodes: every summary node (levels > 0).
        summary_vectors: [m, dim] embeddings aligned to summary_nodes (empty if nothing was run).

    Returns:
        The partition directory Path, or None if there were no summaries to write (the gated/free case).
    """
    if not summary_nodes or summary_vectors.shape[0] == 0:
        log.info("no embedded summaries to write (execute was False) — skipping raptor partition.")
        return None

    part = RAPTOR_PREFIX + space                              # e.g. "raptor::gemini-embedding-001@1536"
    ids = [n["id"] for n in summary_nodes]
    metas = [{
        "kind":     "raptor_summary",                        # tags these hits as coarse summary nodes
        "level":    n["level"],
        "text":     n["summary"][:600],                      # preview, same convention as the leaf store
        "members":  n["members"],                            # transitive leaf ids → drill down to regions
        "children": n["children"],
        "n_leaves": n["n_leaves"],
    } for n in summary_nodes]

    d = vectorstore.write(part, ids, summary_vectors, metas)
    log.info("wrote %d summary vector(s) -> partition '%s'", len(ids), part)
    return d


# ==================================================================
# Small helpers
# ==================================================================
def _parse_space(space: str):
    """Split a "<model>@<dim>" space string into (model, dim:int)."""
    model, dim = space.rsplit("@", 1)
    return model, int(dim)


def _l2_normalize(v: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalize (so cosine == dot, matching the rest of the store). Zero rows pass through."""
    v = np.asarray(v, dtype=np.float32)
    n = np.linalg.norm(v, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return v / n


def _default_space() -> str:
    """The leaf embedding space implied by config.DEFAULTS — "<model>@<dim>"."""
    m, d = config.DEFAULTS["embedding"]
    return f"{m}@{d}"


# ==================================================================
# The stage entry point
# ==================================================================
def run(limit: int | None = None, space: str | None = None, model: str | None = None,
        execute: bool = False, params: dict | None = None) -> Path:
    """Build the RAPTOR tree and (when executed) the 'raptor' summary vector partition.

    The free path (execute=False, the default) is fully useful on its own: it clusters the existing
    leaf embeddings, lays out the level-1 summarization prompts with full provenance, and writes the
    tree skeleton + the cost-projection — all without spending a cent. Pass execute=True to actually
    summarize (and embed the summaries up the tree). This honors the project rule: gated, nothing
    billed until David says go.

    Args:
        limit: cap the number of leaf chunks (a strided sample) for smoke tests. None = full corpus.
        space: the leaf embedding space "<model>@<dim>" to build the tree over. Defaults to the
            config.DEFAULTS embedding (the same partition embed_corpus.py builds first).
        model: LLM id for cluster summarization. Defaults to config.DEFAULTS["llm"] (a cheap Flash
            tier — cluster summarization is small and well-scoped).
        execute: if True, make the gated PAID calls (LLM summaries + summary embeddings) and write
            the raptor vector partition. If False, build structure + prompts only (FREE).
        params: optional overrides for the clustering/tree tunables (merged over DEFAULTS).

    Returns:
        Path to data/raptor_tree.json.

    Side effects:
        Writes OUT_PATH (always) and data/vectors/raptor::<space>/ (only when execute=True). Every
        real model call is cost-logged inside the provider adapters, so costs/usage.csv reflects it.
    """
    space = space or _default_space()
    model = model or config.DEFAULTS["llm"]
    params = {**DEFAULTS, **(params or {})}
    config.DATA.mkdir(parents=True, exist_ok=True)

    log.info("RAPTOR build  space=%s  model=%s  limit=%s  execute=%s",
             space, model, limit, execute)

    # ----- Step 1: leaves (chunks + embeddings we already have).
    leaf_nodes, leaf_vectors = _load_leaves(space, limit)
    if not leaf_nodes:
        raise RuntimeError("no leaf chunks/embeddings found — run embed_corpus.py first.")
    log.info("leaves: %d node(s), vector dim %d", len(leaf_nodes), leaf_vectors.shape[1])

    # ----- Steps 2+3: recursively cluster + summarize into a tree (paid half gated by `execute`).
    t0 = time.time()
    levels, summary_nodes, summary_vectors = _build_tree(
        leaf_nodes, leaf_vectors, space, model, params, execute)

    # ----- Step 4: embed the summaries into the raptor partition (no-op when execute=False).
    partition = _write_raptor_partition(space, summary_nodes, summary_vectors)

    # ----- Persist the tree itself (non-destructive — a fresh artifact).
    #   We keep leaf nodes WITHOUT their (large) preview duplicated needlessly, but we do keep their
    #   ids/level/members so the tree is self-contained for provenance walks. Summary nodes keep
    #   their prompt (for inspection) + summary (when run).
    tree = {
        "schema":       "raptor_tree.v1",
        "built_at":     time.strftime("%Y-%m-%dT%H:%M:%S"),
        "executed":     execute,                               # were summaries actually generated?
        "space":        space,                                 # leaf embedding space
        "raptor_space": (RAPTOR_PREFIX + space) if partition else None,
        "llm":          model,
        "limit":        limit,
        "params":       params,
        "config_fingerprint": config.fingerprint(),            # ties the artifact to the config it was built under
        "n_levels":     len(levels),
        "level_sizes":  [len(lvl) for lvl in levels],
        "n_leaves":     len(leaf_nodes),
        "n_summaries":  len(summary_nodes),
        # Leaf nodes: minimal provenance carriers (full text/meta still live in the leaf partition).
        "leaves": [{"id": n["id"], "level": 0, "kind": n["kind"], "meta": n["meta"]} for n in leaf_nodes],
        # Summary nodes: the tree's interior + root. prompt kept so a paid run can be previewed first.
        "summaries": summary_nodes,
    }
    OUT_PATH.write_text(json.dumps(tree, ensure_ascii=False, indent=2))
    log.info("wrote tree -> %s", OUT_PATH)

    _print_summary(tree, model, execute, partition, time.time() - t0)
    return OUT_PATH


# ==================================================================
# End-of-run SUMMARY block (style-guide convention for runnable scripts)
# ==================================================================
def _print_summary(tree: dict, model: str, execute: bool, partition, secs: float) -> None:
    """Print the SUMMARY block: tree shape, cost (if billed), and the next step.

    Args:
        tree: the tree dict just written.
        model: the LLM used.
        execute: whether the paid path ran.
        partition: the raptor partition Path (or None).
        secs: wall-clock seconds for the build.
    """
    print("=" * 60)
    print("RAPTOR TREE — SUMMARY")
    print("=" * 60)
    print(f"  mode          : {'EXECUTED (billed)' if execute else 'STRUCTURE ONLY (no API calls)'}")
    print(f"  leaf space    : {tree['space']}")
    print(f"  llm           : {model}")
    print(f"  leaves        : {tree['n_leaves']}")
    print(f"  levels        : {tree['n_levels']}  sizes={tree['level_sizes']}")
    print(f"  summary nodes : {tree['n_summaries']}")
    print(f"  raptor space  : {tree['raptor_space'] or '(not written — run with --execute)'}")
    print(f"  tree artifact : {OUT_PATH}")
    print(f"  build time    : {secs:.1f}s")
    if execute:
        # Real spend pulled from the always-on cost log (LLM summaries + summary embeddings).
        agg = costlog.summary().get(model, {})
        if agg:
            print(f"  llm cost      : ${agg.get('usd', 0.0):.4f}  "
                  f"({agg.get('input_tokens', 0)} in / {agg.get('output_tokens', 0)} out tokens, "
                  f"{agg.get('calls', 0)} calls — cumulative for this model)")
    else:
        # Free run: project the bill from the prompts we built (chars/4 ≈ tokens), so David can decide.
        in_tok = sum(len(n["prompt"]) for n in tree["summaries"]) // 4
        in_tok += len(SYSTEM_INSTRUCTION) // 4 * max(1, tree["n_summaries"])   # cached prefix, billed once/call
        price = config.LLMS.get(model, {})
        # Rough: assume ~130 output tokens per summary (the target length) for the projection.
        out_tok = 130 * tree["n_summaries"]
        proj = (in_tok * price.get("in", 0) + out_tok * price.get("out", 0)) / 1_000_000
        print(f"  projected $   : ~${proj:.4f}  (level-1 only; full tree adds higher levels)")
        print( "  note          : STRUCTURE ONLY built level 1 (no summary text to embed higher).")
        print( "  next          : add --execute for a tiny billed smoke test (with --limit), then")
        print( "                  the full paid build. Prefer the provider Batch API (~-50%) for the")
        print( "                  full run — summarization is offline + embarrassingly parallel.")
    print("=" * 60)


# ==================================================================
# CLI
# ==================================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None,
                    help="cap leaf count (strided sample). Omit for the FULL corpus.")
    ap.add_argument("--space", type=str, default=None,
                    help=f"leaf embedding space '<model>@<dim>' (default: {_default_space()})")
    ap.add_argument("--model", type=str, default=None,
                    help=f"LLM id from config.LLMS for summarization (default: {config.DEFAULTS['llm']})")
    ap.add_argument("--execute", action="store_true",
                    help="make the GATED paid calls (LLM summaries + summary embeddings) and write "
                         "the raptor vector partition. Omit to build structure + prompts only (free).")
    a = ap.parse_args()
    run(limit=a.limit, space=a.space, model=a.model, execute=a.execute)
