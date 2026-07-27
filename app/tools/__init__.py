"""app/tools/ — the agent's TOGGLEABLE capabilities, one module per tool.

The dev tool's whole personality is "an agent whose retrieval powers are switches you can flip."
Each capability the agent can reach for — semantic search, full-text PDF search, an entity lookup,
a graph hop, a temporal slice, a community summary — lives in its OWN module here and registers a
single :class:`~app.tools.base.Tool`. The server collects them into a registry, shows them as toggles
in the UI, and only hands the LLM the function declarations for the tools the caller left ON.

Why one-module-per-tool? Because the README promises that *new capabilities appear as new toggles*:
to add a power you drop a file in here that calls `register(...)`, and it shows up everywhere
(UI switch, function-calling schema, trace) with zero edits to the server. That's the same
"model/stage as a variable" discipline the rest of step_7 follows — here it's "tool as a plugin."

Importing this package is SIDE-EFFECT-LIGHT: it imports each tool module, and each module registers
its Tool at import time. None of that touches the network or runs a paid model — a Tool's *callable*
only runs when the agent (or a test) actually invokes it, and every model-shaped call inside routes
through lib.costlog exactly like the rest of the pipeline.
"""
from __future__ import annotations

# ==================================================================
# Re-export the public surface so callers write `from app import tools`
# and then `tools.all_tools()`, `tools.call(name, args)`, `tools.registry_manifest()`, ...
# ==================================================================
from .base import (                                          # noqa: F401
    Tool, register, tool_from_module, all_tools, get, REGISTRY,
    call, registry_manifest, provenance,
)

# Importing the tool modules is what populates the registry (each calls register() at import). We do
# it here, once, so `import app.tools` is all the server needs. Keep this list in sync with the files
# in this folder — adding a new tool module means adding one import line (and it auto-appears as a
# toggle). We import defensively: a single broken tool module must not take down the whole agent, so
# we log and skip it rather than crash the server at startup.
import logging as _logging

_log = _logging.getLogger("app.tools")

for _name in ("vector_search", "pdf_fulltext_search", "entity_lookup",
              "graph_query", "temporal_query", "community_summary", "book_search"):
    try:
        __import__(f"{__name__}.{_name}", fromlist=["_"])
    except Exception as _e:                       # noqa: BLE001 — a bad tool shouldn't kill the rest
        _log.warning("tool module %r failed to import (skipping): %s", _name, _e)

__all__ = ["Tool", "register", "tool_from_module", "all_tools", "get", "REGISTRY",
           "call", "registry_manifest", "provenance"]
