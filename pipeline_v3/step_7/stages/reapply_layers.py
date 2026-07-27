"""reapply_layers.py — re-apply the FREE, deterministic post-passes onto graph.json (no model calls).

`build_graph` regenerates the BASE graph from entity_store.json and would otherwise drop everything the
later passes added. This stage restores the two PATCH-style layers whose expensive work already lives in
side-cars, so a graph rebuild is lossless and free:

  1. connection verdicts   — from data/connection_verdicts.json  (verify_connections owns the paid pass)
  2. entity descriptions    — from data/entity_descriptions.json  (enrich_descriptions owns the paid pass)

Registered in the DAG downstream of build_kinship (which does the structural merge + FAMILY edges), so the
canonical graph is exactly:  build_graph -> build_kinship -> reapply_layers -> graph_analysis.

Pairs/nodes not present in the current graph (e.g. an entity rebuild changed some ids) are skipped — this
restores what maps cleanly for free; any genuinely NEW co-occurrence pairs stay unadjudicated until an
opt-in `verify_connections.py --only-missing` top-up. This stage never calls a model and never costs money.

    python stages/reapply_layers.py
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                                            # noqa: E402
from lib.safeio import backup, atomic_write_text         # noqa: E402
from stages.verify_connections import patch_verdicts     # noqa: E402

GRAPH = config.DATA / "graph.json"
DESCRIPTIONS = config.DATA / "entity_descriptions.json"


def patch_descriptions(g: dict) -> int:
    """Patch node.description from the entity_descriptions.json side-car IN PLACE. Returns count patched."""
    if not DESCRIPTIONS.exists():
        return 0
    desc = json.loads(DESCRIPTIONS.read_text())
    n = 0
    for node in g.get("nodes", []):
        d = desc.get(node["id"])
        if d and d.get("description"):
            node["description"] = d["description"]
            n += 1
    return n


def run() -> None:
    """DAG entry point: patch verdicts + descriptions onto graph.json (backup + atomic write). Free."""
    if not GRAPH.exists():
        print(f"no {GRAPH.name} to patch (build_graph must run first)"); return
    g = json.loads(GRAPH.read_text())
    nv = patch_verdicts(g)
    nd = patch_descriptions(g)
    backup(GRAPH, "reapply_layers")
    atomic_write_text(GRAPH, json.dumps(g, ensure_ascii=False))
    print("=" * 60)
    print(f"reapply_layers: {nv} connection verdicts + {nd} descriptions re-applied to {GRAPH.name} "
          f"(no model calls). For 100% verdict coverage after an entity rebuild, run "
          f"`verify_connections.py --only-missing`.")
    print("=" * 60)


if __name__ == "__main__":
    run()
