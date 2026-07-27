"""stages/graph_analysis.py — community detection (Louvain) + a Mapper-style overview of the KG.

Two structure summaries of data/graph.json, both for EXPLORING the whole graph (David's ask):

  1. COMMUNITIES (Louvain). We build the SEMANTIC subgraph (entities + their wrote_to / appears_with /
     located_at / has_condition / enrolled / family / ... edges, excluding the structural mention/date
     edges) and run Louvain modularity clustering. Every entity gets a community id; every community
     gets a summary (size, dominant kind, top members by degree, a human label). This lets the UI colour
     the graph by community and lets the user jump straight into a thematic cluster.

  2. MAPPER (graph Mapper, the TDA construction — networkx + numpy only, no heavy deps). A "shape
     summary" of the big graph: a 1-D lens (PageRank = importance) is covered by overlapping intervals;
     within each interval we take the connected components of the induced subgraph as Mapper nodes; two
     Mapper nodes are linked when they share original nodes (the nerve). The result is a small navigable
     minimap of the entire archive's structure.

Non-destructive: writes data/communities.json + data/mapper.json (sidecars the API merges in). Re-run
after build_graph (it reads the fresh graph.json).

    python stages/graph_analysis.py
"""
from __future__ import annotations
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config              # noqa: E402

GRAPH = config.DATA / "graph.json"
COMMUNITIES_OUT = config.DATA / "communities.json"
MAPPER_OUT = config.DATA / "mapper.json"

# the interpretable relation edges (NOT structural mention/date/continuation) — the social/semantic graph
SEMANTIC = {"WROTE_TO", "APPEARS_WITH", "LOCATED_AT", "HAS_CONDITION", "ENROLLED", "HAS_OUTCOME",
            "FAMILY", "MEMBER_OF"}
ENTITY_KINDS = {"person", "place", "organization", "condition", "favor", "outcome", "role", "event"}


def _semantic_graph(gj: dict):
    """An undirected networkx graph over the SEMANTIC edges (deduped, weighted by multiplicity)."""
    import networkx as nx
    byid = {n["id"]: n for n in gj["nodes"]}
    G = nx.Graph()
    for e in gj["edges"]:
        if (e.get("kind") or "").upper() not in SEMANTIC:
            continue
        s, t = e.get("source"), e.get("target")
        if s not in byid or t not in byid or s == t:
            continue
        w = (e.get("weight") or 1)
        if G.has_edge(s, t):
            G[s][t]["weight"] += w
        else:
            G.add_edge(s, t, weight=w)
    # keep node attrs we summarize on
    for nid in G.nodes:
        n = byid[nid]
        G.nodes[nid]["kind"] = n.get("kind")
        G.nodes[nid]["label"] = n.get("label") or nid
    return G, byid


def _label_community(members, byid):
    """A readable label for a community: its highest-degree member, tagged by the dominant kind."""
    kinds = Counter(byid[m].get("kind") for m in members if m in byid)
    dominant = kinds.most_common(1)[0][0] if kinds else "mixed"
    return dominant


def detect_communities(gj: dict) -> dict:
    import networkx as nx
    G, byid = _semantic_graph(gj)
    if G.number_of_nodes() == 0:
        return {"node_community": {}, "communities": []}
    comms = nx.community.louvain_communities(G, weight="weight", seed=42)
    comms = sorted(comms, key=len, reverse=True)
    deg = dict(G.degree())
    node_community, summaries = {}, []
    for cid, members in enumerate(comms):
        members = list(members)
        for m in members:
            node_community[m] = cid
        kinds = Counter(byid[m].get("kind") for m in members if m in byid)
        top = sorted(members, key=lambda m: deg.get(m, 0), reverse=True)[:8]
        summaries.append({
            "id": cid,
            "size": len(members),
            "dominant_kind": kinds.most_common(1)[0][0] if kinds else "mixed",
            "kinds": dict(kinds),
            "top_members": [{"id": m, "label": byid[m].get("label") or m,
                             "kind": byid[m].get("kind"), "degree": deg.get(m, 0)} for m in top],
            "label": (byid[top[0]].get("label") if top else f"community {cid}"),
        })
    return {"node_community": node_community, "communities": summaries,
            "n_communities": len(summaries), "n_nodes": G.number_of_nodes()}


def mapper(gj: dict, comm: dict, min_size: int = 5, min_link: int = 2) -> dict:
    """A Mapper-style overview = the NERVE of the community cover: each sizable Louvain community becomes
    one super-node, and two super-nodes are linked by the count of semantic edges between their members.
    This is the navigable 'shape of the whole archive' minimap (a degree-lens cover is too skewed here,
    producing a disconnected nerve, so we use the community cover — a more faithful, connected summary)."""
    import networkx as nx
    G, byid = _semantic_graph(gj)
    nc = comm.get("node_community", {})
    csum = {c["id"]: c for c in comm.get("communities", [])}
    keep = {cid for cid, c in csum.items() if c["size"] >= min_size}
    mnodes = [{"id": f"c{cid}", "community": cid, "size": csum[cid]["size"],
               "dominant_kind": csum[cid]["dominant_kind"],
               "label": csum[cid]["label"],
               "top_members": csum[cid]["top_members"][:5],
               "examples": [m["label"] for m in csum[cid]["top_members"][:4]]}
              for cid in keep]
    pair = Counter()
    for u, v in G.edges():
        cu, cv = nc.get(u), nc.get(v)
        if cu is None or cv is None or cu == cv or cu not in keep or cv not in keep:
            continue
        pair[tuple(sorted((cu, cv)))] += 1
    medges = [{"source": f"c{a}", "target": f"c{b}", "shared": w}
              for (a, b), w in pair.items() if w >= min_link]
    return {"nodes": mnodes, "edges": medges, "n_nodes": len(mnodes), "n_edges": len(medges),
            "kind": "community-quotient"}


def run() -> dict:
    if not GRAPH.exists():
        raise FileNotFoundError(f"{GRAPH} not found — run stages/build_graph.py first")
    gj = json.loads(GRAPH.read_text())
    print(f"graph_analysis: {len(gj['nodes'])} nodes, {len(gj['edges'])} edges")
    comm = detect_communities(gj)
    COMMUNITIES_OUT.write_text(json.dumps(comm, ensure_ascii=False))
    print(f"  communities: {comm.get('n_communities', 0)} over {comm.get('n_nodes', 0)} entities "
          f"-> {COMMUNITIES_OUT.name}")
    mp = mapper(gj, comm)
    MAPPER_OUT.write_text(json.dumps(mp, ensure_ascii=False))
    print(f"  mapper: {mp.get('n_nodes', 0)} overview nodes, {mp.get('n_edges', 0)} links "
          f"-> {MAPPER_OUT.name}")
    # quick peek at the biggest communities
    for c in comm.get("communities", [])[:6]:
        print(f"    community {c['id']}: {c['size']} nodes, {c['dominant_kind']}, "
              f"e.g. {[m['label'] for m in c['top_members'][:3]]}")
    return {"communities": comm.get("n_communities", 0), "mapper_nodes": mp.get("n_nodes", 0)}


if __name__ == "__main__":
    run()
