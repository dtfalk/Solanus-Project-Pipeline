"""build_kinship.py — fold the verified Solanus family into the knowledge graph (non-destructive + backup).

(1) ENTITY MERGE: collapse fragmented Casey nodes (Owen ×5, Edward ×9, …) into one canonical node per real
    person, re-pointing every edge and recording the variant labels as aliases (clusters from
    data/kinship_merges.json — flagged/uncertain variants already dropped).
(2) FAMILY EDGES: add Father Solanus Casey --FAMILY[relation]--> each in-corpus relative (brother/sister/
    in-law/niece/nephew) from data/solanus_family.json, and tag the node with family_relation.

Writes data/graph.json (after backing up to graph.json.prekinship). Re-run graph_analysis afterwards.

    python stages/build_kinship.py
"""
from __future__ import annotations
import json
import re
import sys
from collections import Counter
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402
from lib.safeio import backup, atomic_write_text  # noqa: E402

GRAPH = config.DATA / "graph.json"
MERGES = config.DATA / "kinship_merges.json"
FAMILY = config.DATA / "solanus_family.json"
SOLANUS_ID = "person:solanus_capuchin:0006"
TITLES = re.compile(r"\b(mr|mrs|ms|miss|rev|revd|fr|father|msgr|monsignor|rt|very|dear|sister|sr|bro|brother|"
                    r"o\.?m\.?\s*cap|o\.?f\.?m|s\.?j|the)\b\.?", re.I)


def _norm(s: str) -> str:
    s = TITLES.sub(" ", (s or "").lower())
    return re.sub(r"[^a-z ]", " ", s).strip()


def main():
    g = json.loads(GRAPH.read_text())
    backup(GRAPH, "kinship")
    nodes = {n["id"]: n for n in g["nodes"]}
    deg = Counter()
    for e in g["edges"]:
        deg[e.get("source")] += 1; deg[e.get("target")] += 1

    # ---- (1) entity merge ----  (record each cluster's canonical node, keyed by its hint)
    remap = {}                                   # variant id -> canonical id
    cluster_canon = {}                           # canonical_hint (normalized) -> canonical node id
    merges = json.loads(MERGES.read_text()) if MERGES.exists() else []
    for cl in merges:
        present = [i for i in cl["variant_ids"] if i in nodes]
        if len(present) < 2:
            continue
        canon = max(present, key=lambda i: deg.get(i, 0))
        cluster_canon[_norm(cl["canonical_hint"])] = canon
        aliases = set(nodes[canon].get("aliases", []))
        for i in present:
            if i == canon:
                continue
            aliases.add(nodes[i].get("label") or i)
            remap[i] = canon
        nodes[canon]["aliases"] = sorted(a for a in aliases if a)
    # apply remap to edges, drop self-loops + dups; remove merged nodes
    seen, new_edges = set(), []
    for e in g["edges"]:
        s = remap.get(e.get("source"), e.get("source")); t = remap.get(e.get("target"), e.get("target"))
        if s == t:
            continue
        key = (s, t, (e.get("kind") or "").upper())
        if key in seen:
            continue
        seen.add(key); e = {**e, "source": s, "target": t}; new_edges.append(e)
    g["edges"] = new_edges
    g["nodes"] = [n for nid, n in nodes.items() if nid not in remap]
    nodes = {n["id"]: n for n in g["nodes"]}
    print(f"(1) merged {len(remap)} variant nodes into {len(merges)} canonical people")

    # ---- (2) family edges ----
    if SOLANUS_ID not in nodes:
        raise SystemExit(f"Solanus node {SOLANUS_ID} not found (post-merge)")
    fam = json.loads(FAMILY.read_text())

    def member_pairs(member):
        """(given, surname) WHOLE-TOKEN pairs from canonical_name + aliases; given!=surname (no single-name)."""
        out = set()
        for s in [member["canonical_name"], member.get("name", "")] + member.get("aliases", []):
            toks = [t for t in _norm(s).split() if len(t) > 1]
            if len(toks) >= 2 and toks[0] != toks[-1]:
                out.add((toks[0], toks[-1]))
        return out

    def cluster_for(member):
        """The merge-cluster canonical node for this member (the workflow already disambiguated it)."""
        target = " ".join(_norm(member["canonical_name"]).split()[:2])  # first two tokens
        for hint, cid in cluster_canon.items():
            if target and target in hint:
                return cid
        return None

    edge_keys = {(e["source"], e["target"], (e.get("kind") or "").upper()) for e in g["edges"]}
    added = 0
    for m in fam["members"]:
        if not m.get("in_corpus"):
            continue
        target_id = cluster_for(m)                       # prefer the disambiguated cluster node
        if target_id is None:                            # else STRICT whole-token (given AND surname) match
            pp = member_pairs(m)
            cand = []
            for n in g["nodes"]:
                if n.get("kind") != "person" or n["id"] == SOLANUS_ID:
                    continue
                toks = set(_norm(n.get("label") or "").split())
                for a in n.get("aliases", []):
                    toks |= set(_norm(a).split())
                if any(gv in toks and sn in toks for gv, sn in pp):
                    cand.append(n)
            if not cand:
                print(f"   ! no graph node matched {m['canonical_name']} — skipped")
                continue
            target_id = max(cand, key=lambda n: deg.get(n["id"], 0))["id"]
        node = nodes.get(target_id) or {"id": target_id, "label": target_id}
        rel = m["relation"]
        node["family_relation"] = rel
        key = (SOLANUS_ID, target_id, "FAMILY")
        if key not in edge_keys:
            g["edges"].append({"source": SOLANUS_ID, "target": target_id, "kind": "FAMILY",
                               "subtype": rel, "weight": 1,
                               "provenance": {"source": "kinship: biography+web+corpus, LLM-adjudicated",
                                              "confidence": m.get("confidence")}})
            edge_keys.add(key); added += 1
            print(f"   + Solanus --FAMILY[{rel}]--> {node.get('label')}  ({m['canonical_name']})")
    atomic_write_text(GRAPH, json.dumps(g, ensure_ascii=False))
    print(f"\n(2) added {added} FAMILY edges. graph -> {GRAPH.name} "
          f"({len(g['nodes'])} nodes, {len(g['edges'])} edges). timestamped backup in data/.backups/")


if __name__ == "__main__":
    main()
