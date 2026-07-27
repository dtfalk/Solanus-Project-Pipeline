"""app/tools/base.py — the Tool contract + a tiny registry the agent loop reads.

Everything an agent tool needs to be (a) shown as a UI toggle, (b) declared to the LLM for
function-calling, and (c) actually executed, is captured in ONE small dataclass — :class:`Tool` —
plus a process-wide :data:`REGISTRY` that the tool modules populate at import time.

The design mirrors how Gemini (and most function-calling LLMs) want tools described: a **name**, a
**human description** the model reads to decide *when* to call it, and a **JSON-Schema** of the
parameters. We keep that schema as a plain dict here (provider-agnostic) and let the server translate
it into the exact `google.genai.types.FunctionDeclaration` shape at request time — so swapping the
LLM later (Claude/OpenAI tool-use) means re-translating in one place, never rewriting the tools.

A Tool's :pyattr:`fn` is just a Python callable ``fn(**kwargs) -> dict``. It returns a JSON-able dict
that becomes the function-call *result* the model sees on the next turn AND the evidence the server
mines for citations. By convention a tool that retrieves passages returns a ``"hits"`` list whose
items carry ``provenance`` (doc_id / rid / page / vertices) — that's the contract the citation layer
and the source modal rely on. Tools that don't retrieve passages (e.g. a summary) just return their
payload and contribute no citations.

Nothing here imports a model or touches the network — this module is pure plumbing. The heavy lifting
(and any cost-logged model call) lives inside each tool's ``fn``.
"""
from __future__ import annotations

# ==================================================================
# Imports — grouped (stdlib / local) the way every step_7 module does
# ==================================================================
# Core Python Imports
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

# Local File Imports — make step_7/ importable from the app/ side of the tree.
# app/ is a SIBLING of pipeline_v3/step_7 (see config.STEP7), so unlike the lib/ modules — which sit
# *inside* step_7 — we have to walk over to step_7 explicitly before `import config` / `from lib ...`
# can resolve. We compute it once, here, and every tool module relies on this having run (importing
# this base first via `from .base import ...`). Belt-and-suspenders: each tool also imports base.
_APP = Path(__file__).resolve().parents[1]                 # .../Solanus-Project-Pipeline/app
_STEP7 = _APP.parent / "pipeline_v3" / "step_7"            # .../pipeline_v3/step_7
if str(_STEP7) not in sys.path:
    sys.path.insert(0, str(_STEP7))

# Now the step_7 foundation resolves. Importing config validates the path wiring at import time (fail
# fast + clearly if app/ is ever moved relative to step_7); costlog lets the registry surface the
# running $ ledger next to the toggles, keeping the project's cost discipline visible in the UI.
import config              # noqa: E402  (path-wiring sanity check + makes step_7 importable for tools)
from lib import costlog    # noqa: E402  (cost ledger for the registry manifest)


# ==================================================================
# The Tool dataclass — the one shape a capability must take
# ==================================================================
@dataclass
class Tool:
    """One toggleable agent capability.

    Attributes:
        name: Stable identifier — the function name the LLM will "call" and the key the UI toggles.
            Must be a valid identifier (Gemini requires function names match ``[a-zA-Z0-9_]``).
        description: Plain-language "what this does + when to use it." The model reads ONLY this to
            decide whether to call the tool, so write it for the model, not for a human skimmer.
        parameters: JSON-Schema (an OpenAPI-style dict) for the tool's arguments — ``{"type":
            "object", "properties": {...}, "required": [...]}``. Provider-agnostic; the server turns
            it into the LLM's native function-declaration format.
        fn: The implementation, ``fn(**kwargs) -> dict``. MUST return a JSON-serializable dict. If it
            retrieves passages, include a ``"hits"`` list of items each carrying ``provenance``
            (doc_id/rid/page/vertices) so the server can build citations from it.
        default_on: Whether this tool starts ENABLED in the UI (and is offered to the LLM unless the
            caller overrides ``enabled_tools``). The cheap/local/always-useful tools default on; the
            ones that can cost money or need an un-built artifact can default off.
        cost_note: A short human hint about cost/availability (shown in the UI next to the toggle),
            e.g. "free, local" or "free unless dense uses a hosted embedding model."
    """
    name: str
    description: str
    parameters: dict
    fn: Callable[..., dict]
    default_on: bool = True
    cost_note: str = "free, local"

    def to_declaration(self) -> dict:
        """Provider-neutral function declaration ``{name, description, parameters}``.

        The server consumes this to build the LLM's native tool schema (for Gemini, a
        ``types.FunctionDeclaration``). Keeping it as a plain dict means the tool authors never import
        a provider SDK — the translation lives entirely in the server's LLM adapter.
        """
        return {"name": self.name, "description": self.description, "parameters": self.parameters}

    def to_ui(self) -> dict:
        """Compact dict for the front-end toggle list (name + description + default + cost hint)."""
        return {"name": self.name, "description": self.description,
                "default_on": self.default_on, "cost_note": self.cost_note}


# ==================================================================
# The registry — a process-wide name → Tool map the modules fill in
# ==================================================================
# A plain dict is all we need: tool modules call `register(Tool(...))` at import time, the server
# reads `all_tools()` / `get(name)`. Insertion order is preserved (Python 3.7+ dicts), so the UI
# shows tools in the order their modules were imported in app/tools/__init__.py — a stable, readable
# ordering we control in one place.
REGISTRY: dict[str, Tool] = {}


def register(tool: Tool) -> Tool:
    """Add a Tool to the global registry (idempotent on name; last registration wins).

    Returns the tool so a module can write ``MY_TOOL = register(Tool(...))`` if it wants a handle.
    Last-wins (rather than raising on a duplicate name) makes hot-reload / re-import during dev
    painless — re-importing a tool module just refreshes its entry instead of exploding.
    """
    REGISTRY[tool.name] = tool
    return tool


def all_tools() -> list[Tool]:
    """Every registered tool, in registration order (the order the UI should render toggles)."""
    return list(REGISTRY.values())


def get(name: str) -> Tool | None:
    """Look up a tool by name (the name the LLM used in a function call), or None if unknown."""
    return REGISTRY.get(name)


# ==================================================================
# tool_from_module — the bridge between TOOL_SPEC/run(args) and the registry
# ==================================================================
# Every tool module in this package keeps a literal ``TOOL_SPEC`` dict (name / description / JSON
# params) and a ``run(args: dict) -> dict`` callable as its SINGLE source of truth. That's the surface
# the task asks for, it's the cleanest to unit-test, and it's exactly the JSON-Schema an LLM
# function-call wants. This helper turns that pair into a registered :class:`Tool` so the server's
# registry (which speaks `Tool.fn(**kwargs)`) and the module's `run(args)` can never drift — the
# Tool's `fn` is literally `run` with the kwargs re-packed into the single args dict it expects.
def tool_from_module(spec: dict, run: Callable[[dict], dict], *,
                     default_on: bool = True, cost_note: str = "free, local") -> Tool:
    """Build + register a Tool from a module's ``TOOL_SPEC`` + ``run(args)``.

    Args:
        spec: ``{"name": str, "description": str, "parameters": <json-schema-object>}``.
        run: the module's ``run(args: dict) -> dict`` implementation.
        default_on: whether the UI toggle starts ON (cheap/always-useful tools default on).
        cost_note: short cost/availability hint shown beside the toggle.

    Returns:
        The registered Tool (so a module can write ``TOOL = tool_from_module(TOOL_SPEC, run)``).

    Raises:
        ValueError: if the spec is missing a required key — a malformed tool should fail loudly at
            import, not silently at first call.
    """
    for key in ("name", "description", "parameters"):
        if key not in spec:
            raise ValueError(f"TOOL_SPEC missing '{key}' — got keys {sorted(spec)}")

    # The registry calls tools as fn(**kwargs); our modules think in terms of a single args dict. This
    # tiny shim re-packs one into the other so both worldviews hold at once.
    def _fn(**kwargs) -> dict:
        return run(kwargs)

    return register(Tool(name=spec["name"], description=spec["description"],
                         parameters=spec["parameters"], fn=_fn,
                         default_on=default_on, cost_note=cost_note))


def call(name: str, args: Optional[dict] = None) -> dict:
    """Invoke a registered tool by name with light validation + uniform error wrapping.

    In an agent loop a single tool raising would kill the whole turn, so we never let an exception
    escape: a failed tool returns a structured ``{"ok": False, "error": ...}`` the agent (or the
    trace panel) can read and recover from — the same forgiving spirit as ``lib.retrieval`` degrading
    to BM25 when dense isn't available. On success we return the tool's payload under ``"result"``.

    Args:
        name: the tool name the model emitted in its function call.
        args: the (already JSON-decoded) arguments dict.

    Returns:
        ``{"ok": True, "tool": name, "result": <run output>}`` or
        ``{"ok": False, "tool": name, "error": "..."}``.
    """
    import time
    import traceback
    tool = get(name)
    if tool is None:
        return {"ok": False, "tool": name, "error": f"unknown tool '{name}'"}
    args = coerce_args(tool.parameters, dict(args or {}))
    t0 = time.perf_counter()
    try:
        missing = _missing_required(tool.parameters, args)
        if missing:
            raise ValueError(f"missing required argument(s): {', '.join(missing)}")
        result = tool.fn(**args)
        return {"ok": True, "tool": name,
                "latency_ms": round((time.perf_counter() - t0) * 1000, 1), "result": result}
    except Exception as e:                            # noqa: BLE001 — keep one tool's error contained
        return {"ok": False, "tool": name,
                "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc(limit=4)}


def registry_manifest() -> dict:
    """A JSON-serializable snapshot of the registry for the server/UI + trace panel.

    One row per tool (the UI toggle data) plus the running cost ledger, so the dev harness can show
    "$ spent so far" right beside the switches. Reading the ledger here keeps cost visible at the
    surface the user actually clicks.
    """
    return {"tools": [t.to_ui() for t in all_tools()],
            "declarations": [t.to_declaration() for t in all_tools()],
            "cost_to_date": costlog.summary()}


# ==================================================================
# JSON-Schema helpers — just enough validation to catch obvious mistakes
# ==================================================================
# We deliberately DON'T pull in a full jsonschema validator (extra dependency, and the models already
# mostly produce valid args). We check `required` presence + a couple of cheap type coercions, which
# is what actually trips an agent up in practice; each tool's own run() does the deeper checks.
def _missing_required(schema: dict, args: dict) -> list:
    """Return the names of any ``required`` properties absent (or None) in args."""
    required = (schema or {}).get("required", []) or []
    return [r for r in required if args.get(r) is None]


def coerce_args(schema: dict, args: dict) -> dict:
    """Best-effort coercion of args toward the schema's declared primitive types.

    Function-calling models occasionally hand back a number as a string ("5") or a bool as "true".
    Rather than make every tool defend against that, we nudge values toward their declared type here;
    unknown/loose types pass through untouched (coercion is a convenience, not a gatekeeper).
    """
    props = (schema or {}).get("properties", {})
    out = dict(args)
    for key, val in args.items():
        want = props.get(key, {}).get("type")
        if want == "integer" and isinstance(val, str) and val.strip().lstrip("-").isdigit():
            out[key] = int(val)
        elif want == "number" and isinstance(val, str):
            try:
                out[key] = float(val)
            except ValueError:
                pass
        elif want == "boolean" and isinstance(val, str):
            out[key] = val.strip().lower() in ("true", "1", "yes", "on")
    return out


# ==================================================================
# Provenance — the citation contract every retrieval tool result speaks
# ==================================================================
# An archive answer is only trustworthy if you can click it and land on the exact handwritten region
# it came from. So every passage-returning tool emits provenance in ONE shape, which the front-end
# turns into an OpenSeadragon/IIIF deep-zoom citation. We mirror the field set lib/retrieval already
# uses (doc_id + rid + page + vertices + min_conf) so retrieval Hits and tool hits cite identically.
PROVENANCE_FIELDS = ("doc_id", "rid", "page", "pdf_page", "section", "vertices", "min_conf")


def provenance(doc_id: Optional[str] = None, rid: Optional[str] = None,
               page: Any = None, pdf_page: Any = None, section: Optional[str] = None,
               vertices: Any = None, min_conf: Any = None, **extra) -> dict:
    """Build one citation-ready provenance dict in the project-standard shape.

    Keeping this in one place means a place-name hit from `entity_lookup`, a passage from
    `vector_search`, and an edge from `graph_query` all cite the source identically — and the
    OpenSeadragon viewer needs only one resolver. Extra keys (``pdf_path``, ``edtf``, ``confidence``,
    …) are preserved alongside the standard fields.
    """
    prov = {"doc_id": doc_id, "rid": rid, "page": page, "pdf_page": pdf_page,
            "section": section, "vertices": vertices, "min_conf": min_conf}
    prov.update(extra)
    return prov
