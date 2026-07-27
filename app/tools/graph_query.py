"""app/tools/graph_query.py — traverse the knowledge graph (neighbors of an entity/record).

The chunk/embedding tools answer "which passages look like my question?"; the **graph** answers the
*relational* questions an archivist actually asks — "whom did Solanus write to?", "which favors
reported a cure?", "what continues onto the next page?". ``stages/build_graph.py`` builds that graph
(RiC-O temporal KG) and serializes it to ``data/graph.json`` as a node/edge list; this tool reads
that file and lets the agent take a **one- or two-hop walk** from a node.

What a query does:
  1. **Find the start node(s)** by id (e.g. ``person:grace_panyard``) or by a fuzzy label match
     ("Grace Panyard" → the person node).
  2. **Walk the edges** out to ``hops`` away, optionally filtered to specific relation kinds
     (``WROTE_TO``, ``HAS_OUTCOME``, ``CONTINUES_ON``, …).
  3. Return the touched **nodes + edges**, and — crucially — surface each edge's **provenance rids**
     and **valid_time (EDTF)** so a relational answer is still *citable* back to the handwritten
     region and dateable on the timeline. (An archive graph that can't show its receipts is just a
     rumor with footnotes — the build stage's words, kept here.)

Local + free: it reads a JSON file. If ``graph.json`` doesn't exist yet, we try to build the graph
in-process *iff* networkx is installed (still free, no model); if even that isn't available we return
an empty, well-formed result telling the agent to fall back to text/entity search. We never call a
paid model here. (We cost-log a $0 row for an in-process build so the ledger stays complete.)
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (wires step_7 onto sys.path), then stdlib + step_7
# ==================================================================
import json
import re
import unicodedata
from collections import defaultdict, deque
from pathlib import Path

from . import base                            # importing base sets up the step_7 import path
import config                                 # step_7 paths (resolves via base)
from lib import costlog                       # $0-log an in-process graph build for ledger symmetry


# ==================================================================
# TOOL_SPEC — name / description / JSON-Schema params the LLM sees
# ==================================================================
TOOL_SPEC = {
    "name": "graph_query",
    "description": (
        "Traverse the knowledge graph from a person, place, organization, or record to its related "
        "nodes (1-2 hops). Use for RELATIONAL questions: who Solanus wrote to, which favors had a "
        "given outcome, what continues onto the next page, family/membership ties. Start by node id "
        "or by a name to fuzzy-match. Returns connected nodes and the edges between them, each with "
        "provenance regions and a date so the relationship stays citable."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "start": {
                "type": "string",
                "description": "Start node id (e.g. 'person:grace_panyard') or a name/label to match.",
            },
            "hops": {
                "type": "integer",
                "description": "Edges to traverse out (1 = direct neighbors, 2 = two hops). Default 1.",
            },
            "relations": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional edge kinds to follow, e.g. ['WROTE_TO','HAS_OUTCOME',"
                               "'CONTINUES_ON','FAMILY','MEMBER_OF','LOCATED_AT','HAS_CONDITION'].",
            },
            "direction": {
                "type": "string",
                "enum": ["out", "in", "both"],
                "description": "Follow outgoing, incoming, or both edge directions (default both).",
            },
        },
        "required": ["start"],
    },
}


# ==================================================================
# Loading the graph.json node/edge list (cached by mtime), or building it
# ==================================================================
# The cache keeps the parsed adjacency in-process so a multi-hop walk doesn't re-read the file each
# call; keyed on mtime so a freshly rebuilt graph.json is picked up automatically.
_CACHE: dict = {"mtime": None, "graph": None}


def _graph_path() -> Path:
    return config.DATA / "graph.json"


def _maybe_build_graph_json() -> bool:
    """If ``graph.json`` is missing, try to build it in-process (free) when networkx is installed.

    ``stages/build_graph.build_networkx_graph`` + ``export_graph_json`` do all the work and are 100%
    local (no model, no network). We only attempt it when networkx imports — if it doesn't, we leave
    graph.json absent and let the caller degrade gracefully. Returns True iff the file now exists.
    """
    path = _graph_path()
    if path.exists() and path.stat().st_size > 0:
        return True
    # graph.json is missing or truncated. Rebuilding the BASE graph from entity_store here would silently
    # DROP the in-place post-passes (kinship / descriptions / connection verdicts) — a request-time data
    # loss. The safest, zero-recompute recovery is to restore the most recent good backup (build_graph and
    # the bakers write timestamped backups to data/.backups/). Only fall back to a base rebuild if none
    # exists, and log loudly that the layers need re-applying.
    import shutil
    bdir = config.DATA / ".backups"
    good = [b for b in bdir.glob("graph.*.json") if b.stat().st_size > 0] if bdir.exists() else []
    if good:
        newest = max(good, key=lambda b: b.stat().st_mtime)
        try:
            shutil.copy2(newest, path)
            costlog.log("local", "restore-graph", "graph_build", items=0, usd=0.0,
                        meta=f"restored graph.json from backup {newest.name} (avoided lossy base rebuild)")
            return path.exists() and path.stat().st_size > 0
        except Exception:
            pass
    try:
        import networkx  # noqa: F401  — probe: build needs it; bail quietly to the degraded path
    except Exception:
        return False
    try:
        from stages import build_graph
        config.DATA.mkdir(parents=True, exist_ok=True)
        G = build_graph.build_networkx_graph()
        build_graph.export_graph_json(G, path)   # now backs up + writes atomically (see build_graph.py)
        # Building is free/local, but log it so the cost ledger reflects every artifact we produced.
        costlog.log("local", "networkx-graph", "graph_build", items=G.number_of_edges(), usd=0.0,
                    meta="on-demand BASE graph.json build — no post-passes; run "
                         "`stages/verify_connections.py --reapply` etc. to restore the layers")
        return path.exists()
    except Exception:
        # A build failure must not crash the agent — just fall through to the degraded empty result.
        return False


def _load_graph() -> dict | None:
    """Return ``{nodes_by_id, out_adj, in_adj, meta}`` for the graph, or None if unavailable.

    We pre-index the flat node/edge list into adjacency maps once per file version so the BFS walk is
    O(touched edges) rather than O(all edges) per hop.
    """
    if not _maybe_build_graph_json():
        return None
    path = _graph_path()
    mtime = path.stat().st_mtime
    if _CACHE["mtime"] == mtime and _CACHE["graph"] is not None:
        return _CACHE["graph"]

    data = json.loads(path.read_text())
    nodes_by_id = {n["id"]: n for n in data.get("nodes", [])}
    out_adj: dict = defaultdict(list)
    in_adj: dict = defaultdict(list)
    for e in data.get("edges", []):
        out_adj[e["source"]].append(e)
        in_adj[e["target"]].append(e)
    graph = {"nodes_by_id": nodes_by_id, "out_adj": out_adj, "in_adj": in_adj,
             "meta": data.get("meta", {})}
    _CACHE["mtime"] = mtime
    _CACHE["graph"] = graph
    return graph


# ==================================================================
# Finding the start node — by exact id or fuzzy label
# ==================================================================
def _norm(s: str) -> str:
    """Casefold + de-accent + word-tokens, for fuzzy label matching (mirrors entity_lookup)."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return " ".join(re.findall(r"[a-z0-9']+", s))


def _find_start_nodes(graph: dict, start: str, max_starts: int = 3) -> list[str]:
    """Resolve a user-supplied ``start`` to one or more node ids (exact id wins; else fuzzy label)."""
    nodes_by_id = graph["nodes_by_id"]
    if start in nodes_by_id:                              # exact node id (e.g. "person:grace_panyard")
        return [start]
    q = _norm(start)
    scored = []
    for nid, n in nodes_by_id.items():
        label = _norm(n.get("label") or n.get("canonical_name") or "")
        if not label:
            continue
        if label == q:
            scored.append((2.0, nid))
        elif q and (q in label or label in q):
            scored.append((1.0 * len(q) / max(len(label), len(q), 1), nid))
        else:
            shared = set(q.split()) & set(label.split())
            if shared:
                scored.append((0.5 * len(shared) / max(len(q.split()), 1), nid))
    scored.sort(reverse=True)
    return [nid for _s, nid in scored[:max_starts]]


# ==================================================================
# The implementation — start node -> N-hop neighborhood (nodes + cited edges)
# ==================================================================
def run(args: dict) -> dict:
    """Walk the knowledge graph out from a node and return the neighborhood (with edge provenance).

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`:
            ``start`` (required — a node id like ``person:grace_panyard`` / ``letter:Volume_1__p001``,
            or a label to fuzzy-match like "Grace Panyard"); ``hops`` (edges to traverse out, 1=direct
            neighbors, 2=two hops; default 1); ``relations`` (optional list of edge kinds to follow,
            e.g. ``["WROTE_TO","HAS_OUTCOME"]`` — see build_graph.EDGE_KINDS); ``direction`` ("out" |
            "in" | "both", default "both"); and ``max_nodes`` (safety cap, default 40).

    Returns:
        ``{"start": str, "available": bool, "matched_start_nodes": [...], "nodes": [...],
           "edges": [...]}`` where each edge carries ``{source, target, kind, label, valid_time,
           rids, method}`` — ``rids`` + ``valid_time`` are the citation + timeline hooks. ``available``
           is False (empty walk) if no graph artifact exists and one couldn't be built.
    """
    start     = (args.get("start") or "").strip()
    hops      = args.get("hops", 1)
    relations = args.get("relations") or None
    direction = args.get("direction", "both")
    max_nodes = args.get("max_nodes", 40)
    if not start:
        return {"start": "", "available": True, "matched_start_nodes": [], "nodes": [], "edges": [],
                "note": "empty start"}

    graph = _load_graph()
    if graph is None:
        return {"start": start, "available": False, "matched_start_nodes": [], "nodes": [], "edges": [],
                "note": "data/graph.json not built and networkx unavailable — run "
                        "stages/build_graph.py (after `pip install networkx`) to enable graph queries."}

    rel_filter = set(relations) if relations else None
    starts = _find_start_nodes(graph, start)
    if not starts:
        return {"start": start, "available": True, "matched_start_nodes": [], "nodes": [], "edges": [],
                "note": f"no node matched '{start}'."}

    # ---- breadth-first walk, collecting touched nodes + edges ----------------
    out_adj, in_adj, nodes_by_id = graph["out_adj"], graph["in_adj"], graph["nodes_by_id"]
    seen_nodes: set = set(starts)
    collected_edges: list = []
    edge_seen: set = set()                                # dedupe symmetric/duplicate edges
    frontier = deque((nid, 0) for nid in starts)

    def _edges_from(nid: str):
        """Yield (edge, neighbor) pairs honoring the direction filter."""
        if direction in ("out", "both"):
            for e in out_adj.get(nid, []):
                yield e, e["target"]
        if direction in ("in", "both"):
            for e in in_adj.get(nid, []):
                yield e, e["source"]

    while frontier and len(seen_nodes) < max_nodes:
        nid, depth = frontier.popleft()
        if depth >= max(1, int(hops)):
            continue
        for e, neighbor in _edges_from(nid):
            if rel_filter and e.get("kind") not in rel_filter:
                continue
            ekey = (e["source"], e["target"], e.get("kind"), e.get("valid_time"))
            if ekey not in edge_seen:
                edge_seen.add(ekey)
                collected_edges.append(e)
            if neighbor not in seen_nodes and len(seen_nodes) < max_nodes:
                seen_nodes.add(neighbor)
                frontier.append((neighbor, depth + 1))

    # ---- shape the touched nodes (trim heavy fields the agent doesn't need) --
    nodes_out = []
    for nid in seen_nodes:
        n = nodes_by_id.get(nid, {"id": nid})
        nodes_out.append({
            "id":             nid,
            "kind":           n.get("kind"),
            "label":          n.get("label"),
            "canonical_name": n.get("canonical_name"),
            "edtf":           n.get("edtf"),
            "doc_id":         n.get("doc_id"),
            "rid":            n.get("rid"),
            "text":           (n.get("text") or "")[:300],   # snippet; full text via vector_search
        })

    return {
        "start":               start,
        "available":           True,
        "matched_start_nodes": starts,
        "n_nodes":             len(nodes_out),
        "n_edges":             len(collected_edges),
        "nodes":               nodes_out,
        "edges":               collected_edges,
    }


# ==================================================================
# Register the tool
# ==================================================================
# default_on=False: the graph artifact (data/graph.json) needs the build_graph stage (and networkx),
# so we leave the toggle OFF until it exists — an honest UI never offers a switch that can't fire.
TOOL = base.tool_from_module(
    TOOL_SPEC, run,
    default_on=False,
    cost_note="free, local (reads data/graph.json; builds it on demand if networkx is installed)",
)
