"""app/tools/community_summary.py — GraphRAG-style thematic summaries over graph communities.

Some questions are *global*, not local: "what conditions recur across the favors?", "describe the
Casey family network", "what themes run through the correspondence?". Pointwise retrieval (find the
5 closest passages) is the wrong shape for these — the answer lives in the *structure* of the whole
graph, not in any single passage. The GraphRAG / LightRAG answer is: **detect communities** (densely
connected clusters of entities/records), summarize each, and answer the global question from the
community summaries.

This tool implements the *free, structural* half of that idea and cleanly gates the *paid* half:

  • **Free path (default).** Read ``data/graph.json``, run community detection (greedy modularity via
    networkx — a standard, deterministic, CPU-only algorithm), and return, per community: its size,
    its node-kind mix, its most-central members (by degree), the relation kinds that bind it, and a
    bag of representative labels. That structural digest already answers a lot ("the biggest cluster
    is cancer-favor entries tied to outcomes 'cured'/'grateful'") — and it's $0.

  • **Paid path (opt-in, gated, cost-logged).** If the caller passes ``summarize=True`` (and accepts
    the cost), we hand each community's digest to the LLM for a 2-3 sentence natural-language summary
    via ``lib.providers.llm.generate`` — the actual GraphRAG "community report." Off by default so the
    tool stays free; every such call is logged through lib.costlog like the rest of the pipeline.

Like the other graph tools it degrades gracefully: no graph + no networkx → an empty, well-formed
result that nudges the agent back to text search.
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (wires step_7 onto sys.path), then stdlib + step_7
# ==================================================================
from collections import Counter

from . import base                            # importing base sets up the step_7 import path
from lib import costlog                       # $0-log the free build; the LLM path logs inside llm.py

# We reuse graph_query's on-demand graph loader so both tools share one cache + build path (DRY).
from . import graph_query as _gq


# ==================================================================
# TOOL_SPEC — name / description / JSON-Schema params the LLM sees
# ==================================================================
# default_on is set on the registration call below (False): this is for GLOBAL/thematic questions,
# needs the graph artifact, and its useful (summarized) mode is paid — so the human opts in via the
# toggle rather than it firing on every query.
TOOL_SPEC = {
    "name": "community_summary",
    "description": (
        "Answer GLOBAL / thematic questions by detecting clusters (communities) in the knowledge "
        "graph and describing each — recurring conditions, family/correspondent networks, themes "
        "across the corpus. Use when the question is about the whole archive's structure rather than a "
        "specific passage. Returns each cluster's makeup, central members, and binding relations; set "
        "summarize=true for a natural-language report per cluster (this calls the LLM and costs money)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "min_size": {"type": "integer",
                         "description": "Smallest community to report (default 4)."},
            "max_communities": {"type": "integer",
                                "description": "Max communities to return (default 8)."},
            "summarize": {"type": "boolean",
                          "description": "If true, add a PAID LLM natural-language report per cluster."},
        },
        "required": [],
    },
}


# ==================================================================
# Community detection over the graph (free, deterministic, CPU-only)
# ==================================================================
def _detect_communities(min_size: int, max_communities: int):
    """Run greedy-modularity community detection on graph.json; return ranked community digests.

    We load the same node/edge list graph_query uses, rebuild a *simple undirected* networkx graph
    (community algorithms want an undirected view; multiple relation kinds between two nodes just make
    the tie stronger), and call ``networkx.algorithms.community.greedy_modularity_communities`` — the
    Clauset-Newman-Moore method, which is parameter-free and reproducible (no random seed to drift).

    Args:
        min_size: Ignore communities smaller than this (singletons/pairs are noise for a *theme*).
        max_communities: Keep at most this many of the largest communities.

    Returns:
        A list of community digest dicts (see :func:`run` for the shape), or None if the graph
        artifact is unavailable / networkx isn't installed.
    """
    graph = _gq._load_graph()                            # shared loader (builds graph.json on demand)
    if graph is None:
        return None
    try:
        import networkx as nx
        from networkx.algorithms import community as nx_comm
    except Exception:
        return None

    nodes_by_id = graph["nodes_by_id"]
    # Build an undirected, weighted projection: weight = number of edges (any kind) between two nodes.
    # We also remember, per community, which relation kinds touched it (for the "what binds it" digest).
    UG = nx.Graph()
    UG.add_nodes_from(nodes_by_id.keys())
    pair_kinds: dict = {}
    for src, edges in graph["out_adj"].items():
        for e in edges:
            dst = e["target"]
            if src == dst:
                continue
            UG.add_edge(src, dst, weight=UG.get_edge_data(src, dst, {}).get("weight", 0) + 1)
            pair_kinds.setdefault(frozenset((src, dst)), Counter())[e.get("kind")] += 1

    if UG.number_of_edges() == 0:
        return []

    communities = list(nx_comm.greedy_modularity_communities(UG, weight="weight"))
    communities.sort(key=len, reverse=True)

    digests = []
    for ci, members in enumerate(communities):
        if len(members) < min_size:
            continue
        members = list(members)
        # central members = highest-degree nodes inside this community (the cluster's "spokespeople").
        deg = sorted(members, key=lambda n: UG.degree(n, weight="weight"), reverse=True)
        kinds = Counter(nodes_by_id.get(n, {}).get("kind") for n in members)
        # which relation kinds knit this community together (only count intra-community pairs).
        member_set = set(members)
        rel_counter: Counter = Counter()
        for pair, kc in pair_kinds.items():
            if pair <= member_set:                       # both endpoints inside the community
                rel_counter.update(kc)
        labels = [nodes_by_id.get(n, {}).get("label") or n for n in deg[:8]]
        digests.append({
            "community_id":    ci,
            "size":            len(members),
            "node_kinds":      dict(kinds),
            "top_relations":   dict(rel_counter.most_common(5)),
            "central_members": [{"id": n, "kind": nodes_by_id.get(n, {}).get("kind"),
                                 "label": nodes_by_id.get(n, {}).get("label") or n}
                                for n in deg[:8]],
            "sample_labels":   labels,
        })
        if len(digests) >= max_communities:
            break

    # cost-log the (free, local) detection so the ledger reflects every model-shaped step.
    costlog.log("local", "greedy-modularity", "community_detect",
                items=len(digests), usd=0.0, meta=f"communities={len(digests)}")
    return digests


# ==================================================================
# Optional LLM community report (PAID, gated) — the GraphRAG "report"
# ==================================================================
def _summarize_community(digest: dict, llm_model) -> str:
    """Ask the LLM for a 2-3 sentence natural-language summary of one community digest (PAID).

    This is the GraphRAG "community report" step, scoped to a single cluster. It's only reached when
    the caller sets ``summarize=True``, and the call is cost-logged inside ``llm.generate``. We feed
    only the *structural* digest (kinds, central labels, relations) — small, cheap, and enough for a
    thematic gloss — not the full text of every member (which would be large and largely redundant).

    Args:
        digest: One community digest from :func:`_detect_communities`.
        llm_model: The LLM model id to use (a request "variable"); None → config default.

    Returns:
        A short summary string (empty string if the LLM call fails — we never crash the tool on it).
    """
    from lib.providers import llm             # lazy import so the free path never needs the SDK
    system = ("You summarize a community (cluster) from a knowledge graph of Father Solanus Casey's "
              "letters and notebooks. Given the cluster's node kinds, central members, and relation "
              "kinds, write 2-3 sentences describing what THEME or GROUP this cluster represents. Be "
              "concrete and grounded in the given members; do not invent facts.")
    prompt = ("Cluster digest:\n"
              f"  node kinds: {digest['node_kinds']}\n"
              f"  relations binding it: {digest['top_relations']}\n"
              f"  central members: {[m['label'] for m in digest['central_members']]}\n"
              f"  sample labels: {digest['sample_labels']}\n\n"
              "Summary (2-3 sentences):")
    try:
        text, _ = llm.generate(prompt, model=llm_model, system=system, temperature=0.3)
        return text.strip()
    except Exception:
        return ""                                        # degrade quietly to the structural digest only


# ==================================================================
# run — the single args-dict entry point (the tool's source of truth)
# ==================================================================
def run(args: dict) -> dict:
    """Detect graph communities and return per-cluster digests (optionally with LLM summaries).

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`:
            ``min_size`` (smallest community member-count to report; below this is noise, default 4),
            ``max_communities`` (max largest communities to return, default 8), ``summarize`` (if True,
            attach a PAID LLM 2-3 sentence report to each community — default False keeps it free), and
            — passed by the SERVER, not the LLM — ``llm_model`` (the per-request model "variable" used
            only when ``summarize`` is True).

    Returns:
        ``{"available": bool, "summarized": bool, "n_communities": int, "communities": [...]}`` —
        each community a digest dict; with ``"summary"`` added when ``summarize`` was True.
        ``available`` is False with empty list if no graph artifact exists / networkx is missing.
    """
    min_size        = args.get("min_size", 4)
    max_communities = args.get("max_communities", 8)
    summarize       = bool(args.get("summarize", False))
    llm_model       = args.get("llm_model")                # server-supplied per-request "variable"

    digests = _detect_communities(max(2, int(min_size)), max(1, int(max_communities)))
    if digests is None:
        return {"available": False, "summarized": False, "n_communities": 0, "communities": [],
                "note": "data/graph.json not built and networkx unavailable — run "
                        "stages/build_graph.py (after `pip install networkx`) to enable community "
                        "summaries."}

    if summarize:
        # PAID: one LLM call per community. The caller opted in; each call is cost-logged in llm.py.
        for d in digests:
            d["summary"] = _summarize_community(d, llm_model)

    return {"available": True, "summarized": summarize,
            "n_communities": len(digests), "communities": digests}


# ==================================================================
# Register the tool (runs at import time -> app.tools picks it up)
# ==================================================================
TOOL = base.tool_from_module(
    TOOL_SPEC, run,
    default_on=False,
    cost_note="free structural digests; summarize=true calls the LLM per community (paid + logged)",
)
