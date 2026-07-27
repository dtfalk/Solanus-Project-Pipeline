"""app/server.py — FastAPI backend for the Solanus Casey archival DEV TOOL.

This is the brain of the web app: a small FastAPI service that exposes a **tool-using agent** whose
retrieval powers are **toggleable**, returns **cited** answers, and shows the **full trace** of what
happened "under the hood." It is the server the README describes — the place where the three model
"variables" (LLM, embedding space, reranker) and the tool switches all come together.

The endpoints (all under ``/api``):

  • ``POST /api/query``      — the agent. Takes the question + the model variables + which tools are
                              enabled, runs a Gemini function-calling loop over ONLY those tools
                              (each tool is a plugin in ``app/tools/``), and returns
                              ``{answer, citations[], trace[], grade, cost}``. Citations carry
                              ``doc_id/rid/page`` (+ vertices when known) so the UI can deep-zoom.
  • ``GET  /api/graph``      — serves ``data/graph.json`` (the KG for the Cytoscape/Sigma viz), built
                              on demand if networkx is installed.
  • ``GET  /api/search_pdf`` — direct full-text (FTS5) page search → page + source PDF + snippet.
  • ``GET  /api/source``     — region info for the citation modal: given ``doc_id`` (+ optional
                              ``rid``) return the text, the gold polygon ``vertices``, OCR
                              ``min_conf``, the page image + the page PDF to open.
  • ``GET  /api/cost``       — ``lib.costlog.summary()`` (the running $ ledger).
  • ``GET  /api/tools``      — the registry manifest (toggle list + LLM declarations + cost).
  • static — ``app/static`` is mounted so the front-end (themed by ``static/theme.css``) is served by
            the same process; ``GET /`` returns ``static/index.html`` if present.

Two cross-cutting disciplines, kept faithfully:

  * **Observability.** If OpenTelemetry + OpenInference import, we set up tracing (so spans can flow
    to Arize Phoenix as the RESEARCH_PLAN asks). If they don't, we fall back to a **lightweight JSON
    trace logger** that writes one newline-delimited record per request to ``app/traces/`` AND returns
    the trace inline in the response — so the "under the hood" panel works with zero extra deps.
  * **Cost.** Every model call goes through ``lib.providers.llm`` / ``lib.providers.embed`` /
    ``lib.providers.rerank``, which all cost-log via ``lib.costlog``. The server itself never calls a
    provider directly, so the ledger stays complete and the FREE path stays genuinely free.

RUNNING (do NOT start it as part of building — this just documents the command):

    # one-time: add the two web deps to the step_7 venv
    pipeline_v3/step_7/venv/bin/pip install "fastapi>=0.110" "uvicorn[standard]>=0.29"
    # optional observability (else the JSON trace fallback is used automatically):
    #   pip install opentelemetry-sdk opentelemetry-api openinference-instrumentation arize-phoenix
    # then, from the repo root:
    pipeline_v3/step_7/venv/bin/uvicorn app.server:app --reload --port 8000

NOTE (deps to add to the venv): ``fastapi`` + ``uvicorn`` are REQUIRED and not yet installed; the
OpenTelemetry/OpenInference/Phoenix stack is OPTIONAL (graceful fallback). We import FastAPI lazily
and raise a clear, actionable error if it's missing, so importing this module for inspection (or to
read ``app.server.agent_query`` in a test) never explodes on a machine without the web deps.
"""
from __future__ import annotations

# ==================================================================
# Imports — grouped (stdlib / local) the way every step_7 module does
# ==================================================================
# Core Python Imports
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import TypedDict

# Local File Imports — app/ is a SIBLING of pipeline_v3/step_7, so we walk to step_7 and put it on
# the path before importing the shared foundation (config + costlog + the providers + retrieval).
# This is the SAME bootstrap app/tools/base.py does; we repeat it here so server.py is importable on
# its own (e.g. `uvicorn app.server:app` with the repo root on the path).
_APP = Path(__file__).resolve().parent                          # .../Solanus-Project-Pipeline/app
_REPO = _APP.parent                                             # .../Solanus-Project-Pipeline
_STEP7 = _REPO / "pipeline_v3" / "step_7"                       # .../pipeline_v3/step_7
for _p in (str(_REPO), str(_STEP7)):                           # repo root → `import app.tools`; step_7 → `import config`
    if _p not in sys.path:
        sys.path.insert(0, _p)

import config                       # noqa: E402  step_7 paths + model registry
from lib import costlog             # noqa: E402  the running $ ledger
from lib import retrieval           # noqa: E402  (region index reuse for /api/source)
from lib.providers import llm       # noqa: E402  the model axis (function-calling lives here)
from app import tools               # noqa: E402  the toggleable tool registry (one module per tool)

log = logging.getLogger("app.server")


# ==================================================================
# Observability — OpenTelemetry/OpenInference if importable, else a JSON tracer
# ==================================================================
# The dev tool's whole pitch is "see under the hood." Ideally that's OpenTelemetry spans flowing to
# Arize Phoenix (OpenInference is the LLM-semantics layer Phoenix reads). But we must NOT make the app
# depend on a heavy optional stack — so we PROBE for it and, if it's absent, fall back to a tiny JSON
# trace logger that writes newline-delimited records to app/traces/ and also returns the trace inline.
# Either way the agent loop calls the same `Tracer` interface, so the loop code never branches on it.
TRACES_DIR = _APP / "traces"


class _JsonTracer:
    """A dependency-free trace sink: collects step records, returns them, and appends to a JSONL file.

    This is the FALLBACK when OpenTelemetry isn't installed. Each ``span(...)`` is a context manager
    that records a named step with its inputs/outputs/timing; ``records`` is the ordered list the
    ``/api/query`` response returns as ``trace`` (so the UI's "under the hood" panel works with zero
    extra deps). One JSONL line per finished request lands in ``app/traces/`` for offline inspection.
    """

    def __init__(self, request_id: str):
        self.request_id = request_id
        self.records: list[dict] = []

    def event(self, name: str, **fields) -> dict:
        """Record an instantaneous trace event (no duration) and return it (for chaining/inspection)."""
        rec = {"t": round(time.time(), 3), "name": name, **fields}
        self.records.append(rec)
        return rec

    def span(self, name: str, **attrs):
        """A timed step. Use as ``with tracer.span("tool_call", tool=...) as s: s["output"]=...``."""
        tracer = self

        class _Span:
            def __enter__(self_inner):
                self_inner.rec = {"t": round(time.time(), 3), "name": name, "kind": "span", **attrs}
                self_inner._t0 = time.perf_counter()
                tracer.records.append(self_inner.rec)
                return self_inner.rec

            def __exit__(self_inner, exc_type, exc, tb):
                self_inner.rec["latency_ms"] = round((time.perf_counter() - self_inner._t0) * 1000, 1)
                if exc is not None:
                    self_inner.rec["error"] = f"{exc_type.__name__}: {exc}"
                return False                            # never swallow — the loop decides how to handle

        return _Span()

    def flush(self) -> None:
        """Persist this request's trace as one JSONL line under app/traces/ (best-effort)."""
        try:
            TRACES_DIR.mkdir(parents=True, exist_ok=True)
            with open(TRACES_DIR / "agent_traces.jsonl", "a") as f:
                f.write(json.dumps({"request_id": self.request_id, "steps": self.records},
                                   ensure_ascii=False) + "\n")
        except OSError as e:                            # tracing must never break the request
            log.warning("trace flush failed: %s", e)


def _setup_otel():
    """Set up OpenTelemetry + OpenInference if available; return a status dict (never raises).

    We keep this OPTIONAL and SILENT-on-absence: a dev box without the stack should still run the app,
    just with the JSON-tracer fallback. When the libs ARE present we register a tracer provider (so a
    Phoenix/OTLP exporter configured via the standard ``OTEL_*`` env vars picks spans up) and report
    which pieces loaded. The per-request JSON tracer ALSO runs regardless, so the API response always
    carries an inline trace for the UI — OTel is the extra, exportable channel for Phoenix.
    """
    status = {"otel": False, "openinference": False, "note": ""}
    try:
        from opentelemetry import trace as _ot_trace
        from opentelemetry.sdk.trace import TracerProvider
        # If the host process hasn't already set a provider, install a plain one. An exporter (OTLP →
        # Phoenix) is configured by the operator via OTEL_EXPORTER_OTLP_ENDPOINT — we don't hardcode it.
        if not isinstance(_ot_trace.get_tracer_provider(), TracerProvider):
            _ot_trace.set_tracer_provider(TracerProvider())
        status["otel"] = True
    except Exception:
        status["note"] = "opentelemetry not installed — using lightweight JSON tracer"
        return status
    # OpenInference gives the LLM/agent SEMANTIC conventions Phoenix understands. Its instrumentors are
    # provider-specific; we just record that the layer is importable (auto-instrumentation of the
    # google-genai client can be wired here when the team picks the exact instrumentor package).
    try:
        import openinference.semconv.trace  # noqa: F401
        status["openinference"] = True
        # TODO(observability): when standardizing on Phoenix, add the matching auto-instrumentor, e.g.
        #   from openinference.instrumentation.google_genai import GoogleGenAIInstrumentor
        #   GoogleGenAIInstrumentor().instrument()
        # so each generate_content call becomes an OpenInference LLM span automatically. Until then the
        # JSON tracer below captures the same shape (per-call inputs/outputs/timing) for the UI.
    except Exception:
        status["note"] = "openinference not installed — OTel spans will lack LLM semantic attributes"
    return status


OTEL_STATUS = _setup_otel()


# ==================================================================
# The agent — a tool-using function-calling loop over the ENABLED tools
# ==================================================================
# The loop, in plain language:
#   1. Seed the conversation with the user's question (the system prompt carries the grounding +
#      citation policy).
#   2. Ask the model, offering ONLY the enabled tools' declarations. It either CALLS one or more tools
#      or ANSWERS.
#   3. For each tool call: run the tool (app.tools.call — error-wrapped), record a trace span, harvest
#      any provenance-bearing hits for citations, and append the tool's result to the conversation.
#   4. Repeat until the model answers or we hit max_steps (a safety cap so a confused model can't loop
#      forever and run up cost).
# Every model touch is cost-logged inside lib.providers.llm; running the loop with default tools and
# the LOCAL embedding/reranker variables makes only the LLM calls billable (retrieval stays free).

SYSTEM_PROMPT = (
    "You are an archival research assistant for the papers of Father Solanus Casey (Capuchin friar, "
    "1870–1957): ~570 letters and ~716 notebook pages of recorded favors. Answer ONLY from what the "
    "tools return — never from prior knowledge or guesswork. Call tools to gather evidence before "
    "answering; prefer vector_search for meaning, pdf_fulltext_search for exact strings, entity_lookup "
    "to resolve a name, graph_query for relationships, temporal_query for dates/timelines, and "
    "community_summary for whole-corpus themes.\n"
    "CITATIONS ARE MANDATORY. Cite every claim inline next to the claim it supports:\n"
    "  • Archive sources: give the doc_id and, when present, the region rid and page "
    "(e.g. '[Appendix_3_p042, Page 21]').\n"
    "  • The biography (book_search → Crosby, *Thank God Ahead of Time*) is SECONDARY literature: cite "
    "it as '(Crosby, p.NN)' and never present it as a primary archival fact — distinguish what the "
    "archive itself says from what the biography says.\n"
    "  • Graph relationships from graph_query are often CO-OCCURRENCES (two people recorded in the same "
    "letter or notebook entry), not stated relationships. When you assert that two people are connected, "
    "say which record links them and that the connection is a co-mention unless the text states more, so "
    "the reader can open that page and verify it is a real association rather than a same-page "
    "coincidence.\n"
    "If the evidence is weak or absent, SAY SO plainly and do not fabricate — in an archive, "
    "'I could not find this' is a correct and valuable answer."
)

# Chat PERSONAS — the system-prompt layer that makes the Ask tab conversational, not just retrieval.
# The user picks one (or edits it) in the UI; the chosen text becomes the LLM's system instruction, and
# the retrieved passages are always appended as grounding (with citation rules) regardless of persona.
PERSONAS = {
    "default": {
        "label": "Default — cited research assistant",
        "prompt": ("You are a careful archival research assistant for the papers of Father Solanus "
                   "Casey (Capuchin friar, 1870–1957). Answer the question directly and concisely, "
                   "grounded only in the passages provided."),
    },
    "archivist": {
        "label": "Archivist — conversational guide",
        "prompt": ("You are a warm, knowledgeable archivist at the Solanus Casey Center, talking with a "
                   "visitor about Father Solanus Casey (Capuchin friar, 1870–1957; porter at St. "
                   "Bonaventure Monastery in Detroit, associated with the Seraphic Mass Association, "
                   "beatified 2017). Speak naturally and conversationally, as in a guided tour — you may "
                   "add brief connective context and gentle narrative — but every FACTUAL claim must come "
                   "from the provided passages, and you never invent people, dates, or events. When the "
                   "passages don't cover something, say so warmly and move on."),
    },
    "solanus": {
        "label": "Father Solanus (first person)",
        "prompt": ("You are Father Solanus Casey, the Capuchin friar (1870–1957), speaking in the FIRST "
                   "PERSON in your own gentle, humble, faith-filled voice — plainspoken, warm, given to "
                   "gratitude ('Deo gratias', 'Thanks be to God') and to confidence in God's providence. "
                   "Draw your manner, phrasing, and substance from the provided passages of your own "
                   "letters and notebooks. Stay in character. Do NOT invent specific facts, opinions, or "
                   "events that the passages do not support — where you have not written on a matter, "
                   "demur humbly ('I could not rightly say...') rather than make something up. This is a "
                   "prompt-only impression for study; it is not the trained voice model."),
    },
}


def _enabled_declarations(enabled_tools):
    """The provider-neutral tool declarations for the tools that are turned ON for this request.

    Args:
        enabled_tools: a list of tool names to enable, or None → use each tool's ``default_on``.

    Returns:
        (declarations, names) — the ``[{name, description, parameters}]`` for the LLM and the list of
        enabled names (for the trace).
    """
    if enabled_tools is None:
        active = [t for t in tools.all_tools() if t.default_on]
    else:
        wanted = set(enabled_tools)
        active = [t for t in tools.all_tools() if t.name in wanted]
    return [t.to_declaration() for t in active], [t.name for t in active]


def _inject_request_variables(name: str, args: dict, variables: dict) -> dict:
    """Add the per-request model 'variables' to a tool's args where the tool understands them.

    The human picks the embedding space / reranker / LLM in the UI; the MODEL only chooses the tool +
    its semantic args (query, name, year…). So here — not in the LLM's schema — we inject those
    variables into the calls that consume them (vector_search reads embedding/reranker;
    community_summary's paid summary reads the llm). Tools ignore keys they don't use.
    """
    args = dict(args or {})
    if name == "vector_search":
        if variables.get("embedding_model"):
            args.setdefault("embedding_model", variables["embedding_model"])
        if variables.get("embedding_dim"):
            args.setdefault("embedding_dim", variables["embedding_dim"])
        if variables.get("reranker"):
            args.setdefault("reranker", variables["reranker"])
    if name == "community_summary" and variables.get("llm"):
        args.setdefault("llm_model", variables["llm"])
    return args


def _collect_citations(tool_name: str, result: dict, seen: dict, citations: list) -> None:
    """Mine a tool result for provenance-bearing items and append de-duplicated citations.

    Our tool contract: a retrieval tool returns a ``hits`` list (or ``matches``/``records`` for the
    entity/temporal tools) whose items each carry a ``provenance`` dict (doc_id/rid/page/…). We pull
    those into one flat, de-duplicated citation list keyed on (doc_id, rid) so the answer's footnotes
    point at exact regions. Tools without provenance (e.g. community_summary) contribute none.
    """
    if not isinstance(result, dict):
        return
    # the three list-shaped payloads our tools emit, in priority order.
    items = result.get("hits") or result.get("matches") or result.get("records") or []
    # BOOK citations are special: the biography has no archive region/scan, so they cite by
    # page in the printed book (rendered as text, never as an "open the scan" archive link).
    if tool_name == "book_search":
        for it in items:
            if not isinstance(it, dict):
                continue
            pg = it.get("page")
            key = ("book", it.get("title"), pg)
            if key in seen:
                continue
            seen[key] = True
            citations.append({
                "source": "book", "title": it.get("title") or "Thank God Ahead of Time",
                "page": pg, "from_tool": tool_name,
                "preview": (it.get("text") or "")[:200],
            })
        return
    for it in items:
        prov = (it or {}).get("provenance") if isinstance(it, dict) else None
        if not prov or not prov.get("doc_id"):
            continue
        # one source per page: a letter page collapses (even with several regions); distinct notebook
        # entries / different docs on a shared page survive. (See _source_key.)
        key = _source_key(it.get("kind"), prov)
        if key in seen:
            continue
        seen[key] = True
        citations.append({
            "doc_id":   prov.get("doc_id"),
            "rid":      prov.get("rid"),
            "page":     prov.get("page"),
            "pdf_page": prov.get("pdf_page"),
            "section":  prov.get("section"),
            "vertices": prov.get("vertices"),
            "min_conf": prov.get("min_conf"),
            "from_tool": tool_name,
            # a short preview helps the UI render a footnote without another round-trip.
            "preview":  (it.get("text") or it.get("snippet") or it.get("canonical_name") or "")[:160]
                        if isinstance(it, dict) else "",
            # full grouped-document text so the citation view can show the whole doc, not just a fragment
            "full_text": (it.get("text") or it.get("snippet") or "") if isinstance(it, dict) else "",
        })


# ==================================================================
# The agent as a LangGraph StateGraph
# ==================================================================
# The tool-using loop is expressed as an explicit LangGraph graph so the orchestration is a first-class,
# inspectable state machine — easy to grow (add a node/edge) as the agent gets more capable — while the
# MODEL axis, the tools, the tracer and cost logging stay exactly where they were: the nodes call the
# same lib.providers.llm + app.tools + _JsonTracer, so provider-neutrality and the per-query $ ledger are
# unchanged, and /api/query's response + trace shape is byte-for-byte the same. Shape:
#     agent ──answered?─────────▶ END
#       │  └──hit step cap?──────▶ finalize ─▶ END   (one plain "give your answer now" call)
#       └──wants tools?──▶ tools ─▶ agent
# Per-request values (tracer, declarations, variables, model, max_steps) travel IN the state: the graph
# is compiled ONCE and shared across requests, so a node must never close over one request's data.
from langgraph.graph import StateGraph, END        # noqa: E402


class _AgentState(TypedDict, total=False):
    contents: list           # provider-native conversation turns (the model history)
    steps: int               # llm turns taken so far
    citations: list          # harvested, de-duplicated citations
    seen_cites: dict         # dedup key -> True
    best_grade: dict         # best retrieval grade seen (drives the UI confidence flag)
    answer_text: str         # the final answer
    finish: str              # "stop" | "tool_call" | "max_steps"
    pending_calls: list      # tool calls the agent node asked for (consumed by the tools node)
    # per-request constants (set once in agent_query, never mutated by a node):
    llm_model: str
    declarations: list
    variables: dict
    max_steps: int
    tracer: object


def _ag_agent(state: _AgentState) -> dict:
    """Ask the model for its next move — a tool call, or a final answer."""
    tracer, llm_model, contents = state["tracer"], state["llm_model"], state["contents"]
    step = state["steps"] + 1
    with tracer.span("llm_turn", step=step, model=llm_model) as sp:
        turn = llm.generate_with_tools(contents, state["declarations"], model=llm_model,
                                       system=SYSTEM_PROMPT)
        sp["usage"] = turn.get("usage")
        sp["n_calls"] = len(turn.get("calls", []))
        sp["finish"] = turn.get("finish")
    # append the model's own turn so its tool-call parts are preserved for the next call.
    if turn.get("model_content") is not None:
        contents.append(turn["model_content"])
    if turn["finish"] == "stop" or not turn["calls"]:
        return {"contents": contents, "steps": step, "answer_text": turn["text"],
                "finish": "stop", "pending_calls": []}
    return {"contents": contents, "steps": step, "finish": "tool_call", "pending_calls": turn["calls"]}


def _ag_tools(state: _AgentState) -> dict:
    """Run every tool the agent asked for; trace each, harvest citations + the best retrieval grade."""
    tracer, contents = state["tracer"], state["contents"]
    citations, seen = state["citations"], state["seen_cites"]
    best_grade = state.get("best_grade")
    for call in state["pending_calls"]:
        name, raw_args = call["name"], call.get("args", {})
        args = _inject_request_variables(name, raw_args, state["variables"])
        with tracer.span("tool_call", tool=name, args=raw_args) as sp:
            outcome = tools.call(name, args)          # error-wrapped: never raises
            sp["ok"] = outcome.get("ok")
            sp["latency_ms"] = outcome.get("latency_ms")
            if not outcome.get("ok"):
                sp["error"] = outcome.get("error")
        result = outcome.get("result", {"ok": outcome.get("ok"), "error": outcome.get("error")})
        _collect_citations(name, result, seen, citations)
        if isinstance(result, dict) and result.get("grade"):
            g = result["grade"]
            if best_grade is None or (g.get("confidence", 0) > best_grade.get("confidence", 0)):
                best_grade = g
        contents.append(llm.function_result_turn(name, result))    # feed results back for the next step
    return {"contents": contents, "citations": citations, "seen_cites": seen,
            "best_grade": best_grade, "finish": "tool_call", "pending_calls": []}


def _ag_finalize(state: _AgentState) -> dict:
    """Step cap hit with the model still wanting tools: ask once, plainly, for the answer it has."""
    tracer, llm_model, contents = state["tracer"], state["llm_model"], state["contents"]
    with tracer.span("llm_final", model=llm_model) as sp:
        contents.append(llm.user_turn(
            "You have gathered enough evidence. Now give your final, grounded answer with citations, "
            "or say plainly if the archive does not contain the answer."))
        turn = llm.generate_with_tools(contents, [], model=llm_model, system=SYSTEM_PROMPT)
        sp["usage"] = turn.get("usage")
    return {"contents": contents, "answer_text": turn["text"], "finish": "max_steps"}


def _ag_route_agent(state: _AgentState) -> str:
    """After an agent turn: END if it answered, else ALWAYS run the tools it asked for.

    The step cap is checked AFTER the tool round (see _ag_route_tools), never before — the original
    loop ran the final turn's tools and THEN finalized, so routing straight to finalize here would drop
    that round (missing tool_call spans + citations) and hand the finalize call a model turn whose
    function-call has no matching function-response.
    """
    return "end" if state["finish"] == "stop" else "tools"


def _ag_route_tools(state: _AgentState) -> str:
    """After a tool round: finalize once the step budget is spent, else back to the agent for more."""
    return "finalize" if state["steps"] >= state["max_steps"] else "agent"


_AGENT_GRAPH = None


def _agent_graph():
    """Compile the agent StateGraph once and reuse it (its structure is request-independent)."""
    global _AGENT_GRAPH
    if _AGENT_GRAPH is None:
        g = StateGraph(_AgentState)
        g.add_node("agent", _ag_agent)
        g.add_node("tools", _ag_tools)
        g.add_node("finalize", _ag_finalize)
        g.set_entry_point("agent")
        g.add_conditional_edges("agent", _ag_route_agent, {"tools": "tools", "end": END})
        g.add_conditional_edges("tools", _ag_route_tools, {"finalize": "finalize", "agent": "agent"})
        g.add_edge("finalize", END)
        _AGENT_GRAPH = g.compile()
    return _AGENT_GRAPH


def agent_query(question: str, llm_model: str | None = None, embedding_space=None,
                reranker: str | None = None, enabled_tools=None, max_steps: int = 6,
                tracer: "_JsonTracer | None" = None) -> dict:
    """Run the tool-using agent for one question and return answer + citations + trace + cost.

    Args:
        question: The user's natural-language question.
        llm_model: LLM model id (a request "variable"); None → config.DEFAULTS["llm"].
        embedding_space: ``[model, dim]`` (or ``"model@dim"``) selecting the dense partition for
            vector_search; None → config default. This is the embedding "variable."
        reranker: Reranker model id (a request "variable"); None → config default.
        enabled_tools: List of tool names to allow this turn; None → each tool's ``default_on``.
        max_steps: Safety cap on tool-call rounds (prevents runaway loops / cost).
        tracer: A ``_JsonTracer`` to record into; one is created if omitted.

    Returns:
        ``{answer, citations[], trace[], grade, tools_enabled[], steps, finish, cost}``. ``grade`` is
        the Self-RAG/CRAG verdict from the best retrieval call (so the UI can flag low-confidence
        answers). ``cost`` is the running ledger AFTER this turn.

    Cost: every LLM call is billable + cost-logged; with the LOCAL embedding/reranker variables the
    retrieval tools cost $0. This function DOES call the LLM (that's the point) — callers that must
    stay free should not invoke it.
    """
    tracer = tracer or _JsonTracer(uuid.uuid4().hex[:12])
    llm_model = llm_model or config.DEFAULTS["llm"]
    # A non-positive cap would make recursion_limit (2*max_steps+5) ≤ 0 and crash .invoke(); clamp to 1
    # so a stray max_steps=0/-N gives one real turn instead of a 500 (the old range() just ran 0 turns).
    max_steps = max(1, int(max_steps))

    # normalize the embedding "variable" into a {model, dim} pair the vector_search tool understands.
    emb_model, emb_dim = None, None
    if embedding_space:
        if isinstance(embedding_space, str) and "@" in embedding_space:
            emb_model, dim = embedding_space.split("@", 1)
            emb_dim = int(dim)
        elif isinstance(embedding_space, (list, tuple)) and len(embedding_space) == 2:
            emb_model, emb_dim = embedding_space[0], int(embedding_space[1])
    variables = {"llm": llm_model, "embedding_model": emb_model,
                 "embedding_dim": emb_dim, "reranker": reranker}

    declarations, enabled_names = _enabled_declarations(enabled_tools)
    tracer.event("agent_start", question=question, llm=llm_model,
                 embedding_space=(f"{emb_model}@{emb_dim}" if emb_model else "default"),
                 reranker=reranker or "default", tools_enabled=enabled_names)

    # conversation history as provider-native turns (built via llm helpers so we don't import the SDK).
    contents = [llm.user_turn(f"{SYSTEM_PROMPT}\n\nQuestion: {question}")]

    _cost0 = costlog.snapshot()                              # ledger BEFORE this query (for the delta)

    # Run the agent ⇄ tools loop as a LangGraph state machine (see the graph above). Per-request values
    # ride in the state; recursion_limit covers the worst path (max_steps agent+tools rounds + finalize).
    init: _AgentState = {
        "contents": contents, "steps": 0, "citations": [], "seen_cites": {},
        "best_grade": None, "answer_text": "", "finish": "stop", "pending_calls": [],
        "llm_model": llm_model, "declarations": declarations, "variables": variables,
        "max_steps": max_steps, "tracer": tracer,
    }
    final = _agent_graph().invoke(init, config={"recursion_limit": 2 * max_steps + 5})
    answer_text = final.get("answer_text", "")
    citations = final.get("citations", [])
    best_grade = final.get("best_grade")
    finish = final.get("finish", "stop")
    steps = final.get("steps", 0)

    tracer.event("agent_end", finish=finish, steps=steps, n_citations=len(citations))
    tracer.flush()
    return {
        "answer":        answer_text,
        "citations":     citations,
        "grade":         best_grade or {"verdict": "unknown", "reason": "no retrieval tool was used"},
        "trace":         tracer.records,
        "tools_enabled": enabled_names,
        "steps":         steps,
        "finish":        finish,
        "request_id":    tracer.request_id,
        # THIS query's own cost (delta), plus the cumulative session total for context. The UI shows
        # the per-query number prominently so a $0.01 question never reads as the whole ledger.
        "cost":          {**costlog.delta(_cost0, costlog.snapshot()),
                          "cumulative_usd": round(sum(a["usd"] for a in costlog.summary().values()), 6)},
    }


# ==================================================================
# /api/source — resolve a citation to its region (text + polygon + images)
# ==================================================================
# The citation modal (OpenSeadragon deep-zoom) needs: the region's text, its gold polygon vertices,
# the OCR confidence, and the page image + page PDF to open. We assemble all of that from the step_6
# source records (the same ones lib.retrieval / index_sources read) plus the on-disk page folder.
def source_region(doc_id: str, rid: str | None = None, page: int | None = None) -> dict:
    """Return the citation payload for one region (or the whole page if rid is omitted).

    Args:
        doc_id: The record id (e.g. ``Volume_3__p079``) — letter or notebook page.
        page: Optional page_number_in_type of the mention — disambiguates which physical page's region
            to show when a multi-page letter reuses the same rid on every page.
        rid: Optional region id (e.g. ``doc_1.src_content.0``). If given, we return that region's text
            + polygon + confidence; if omitted, we return page-level info (every region's box).

    Returns:
        ``{doc_id, rid, found, page, pdf_page, section, kind, text, vertices, min_conf, page_png,
           page_pdf, regions}`` — paths are repo-relative when possible. ``found`` is False if the
        record/region can't be located (the modal then shows a graceful "source unavailable").
    """
    # locate the record across both source files (small linear scan; fine at this corpus size, and we
    # avoid holding the giant JSON in memory between requests).
    rec, kind = _find_record(doc_id)
    if rec is None:
        return {"doc_id": doc_id, "rid": rid, "found": False,
                "note": f"record '{doc_id}' not found in documents.json / notebooks.json"}

    section = rec.get("section")
    doc_pdf = rec.get("pdf_page_number")
    doc_pnit = rec.get("page_number_in_type")
    # A multi-page LETTER is ONE record spanning many physical pages, and EVERY page reuses the same rid
    # ("doc_1.src_content.0"). So a region's true physical page is its OWN page_number_in_type, and its
    # image is doc_pdf + (region_page - doc_first_page). Without this the modal showed page-14 text over
    # the page-1 image (the bug David reported). offset is constant across the document's page run.
    offset = (doc_pdf - doc_pnit) if (doc_pdf is not None and doc_pnit is not None) else 0

    regions, matches = [], []
    for reg in rec.get("regions", []):
        verts = reg.get("vertices")
        if isinstance(verts, str):
            try:
                verts = json.loads(verts)
            except (json.JSONDecodeError, TypeError):
                verts = None
        entry = {"rid": reg.get("rid"), "category": reg.get("category"),
                 "vertices": verts, "min_conf": _to_float(reg.get("min_conf")),
                 "page": reg.get("page_number_in_type"), "text": reg.get("text", "")}
        regions.append(entry)
        if rid and reg.get("rid") == rid:
            matches.append(entry)

    # choose the region consistently: prefer the one on the mention's page; else the first match.
    target = None
    if matches:
        target = next((m for m in matches if page is not None and m.get("page") == page), matches[0])

    # the chosen region's physical page -> its image (NOT the document's first page)
    chosen_pnit = (target or {}).get("page") if target else (page if page is not None else doc_pnit)
    region_pdf = (chosen_pnit + offset) if chosen_pnit is not None else doc_pdf
    page_png, page_pdf = _page_assets(section, region_pdf)

    # document context the citation modal surfaces: when written, where Solanus wrote FROM, to whom.
    tbl = rec.get("text_by_label", {}) or {}
    record_meta = {
        "date":      rec.get("date") or tbl.get("src_date") or tbl.get("archv_date"),
        "recipient": rec.get("recipient") or tbl.get("src_recipient"),
        "sent_from": tbl.get("src_location_sender") or tbl.get("src_origin"),
        "sent_to":   tbl.get("src_location_recipient"),
        "page_label": rec.get("page_label") or (rec.get("notebook") if kind == "notebook" else None),
        "multipage": len(rec.get("pages", []) or []) > 1,
    }

    out = {
        "doc_id":   doc_id,
        "rid":      rid,
        "found":    True if (rid is None or target is not None) else False,
        "kind":     kind,
        "section":  section,
        "page":     chosen_pnit,
        "pdf_page": region_pdf,
        "page_png": page_png,
        "page_pdf": page_pdf,
        "regions":  regions,                              # all boxes (the viewer can paint them all)
        "record":   record_meta,
    }
    if rid:
        # region-specific payload for the highlighted citation.
        out.update({
            "text":     (target or {}).get("text", ""),
            "vertices": (target or {}).get("vertices"),
            "min_conf": (target or {}).get("min_conf"),
            "category": (target or {}).get("category"),
        })
    else:
        # whole-page citation: assemble the page text from its regions so the modal isn't blank.
        out["text"] = "\n\n".join(r["text"] for r in regions if r.get("text"))
    return out


_RECORD_CACHE: dict = {"docs": None, "notes": None}


def _find_record(doc_id: str):
    """Find a record by id in documents.json (letters) or notebooks.json (pages); cache the parses."""
    if _RECORD_CACHE["docs"] is None:
        _RECORD_CACHE["docs"] = {d["id"]: d for d in json.loads(config.DOCUMENTS.read_text())}
        _RECORD_CACHE["notes"] = {p["id"]: p for p in json.loads(config.NOTEBOOKS.read_text())}
    if doc_id in _RECORD_CACHE["docs"]:
        return _RECORD_CACHE["docs"][doc_id], "letter"
    if doc_id in _RECORD_CACHE["notes"]:
        return _RECORD_CACHE["notes"][doc_id], "notebook"
    return None, None


def _to_float(x):
    """Coerce a min_conf string like '0.986' to float; None if it can't (mirrors lib.retrieval)."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _page_assets(section, pdf_page):
    """Return (page_png, page_pdf) repo-relative paths for a page, or (None, None) if not on disk.

    Mirrors index_sources' folder resolution (source pages live under ``3_enriched/<section>/
    1_source_pages/page_<NNN>/``). We hand back repo-relative paths so the front-end can request them
    via a static route without leaking absolute filesystem layout.
    """
    if section is None or pdf_page is None:
        return None, None
    name = f"page_{pdf_page:03d}"
    base = config.ENRICHED / section
    for bucket in ("1_source_pages", "2_post_pages", "0_table_of_contents"):
        folder = base / bucket / name
        if folder.is_dir():
            png = folder / f"{name}.masked.png"
            pdf = folder / f"{name}.masked.pdf"
            def _rel(p: Path):
                try:
                    return str(p.relative_to(config.REPO)) if p.exists() else None
                except ValueError:
                    return str(p) if p.exists() else None
            return _rel(png), _rel(pdf)
    return None, None


# ==================================================================
# Front-end compatibility — the single-shot, TOGGLE-based RAG the UI models
# ==================================================================
# The agent loop above is the task's headline `/api/query`. The shipped front-end (app/static/app.js),
# however, is built around the *other* shape the RESEARCH_PLAN describes: a single `RetrievalConfig`
# whose retrieval TECHNIQUES (BM25 / dense / rerank / HyDE / multi-query / router) are toggles, run
# once, then an LLM synthesizes a cited answer. We support BOTH so the UI works end-to-end: this
# `single_shot_rag` powers `/api/ask`, mapping the UI's toggles straight onto lib.retrieval (the same
# dataclass the UI's left rail mirrors) and returning the citation/trace shape app.js expects.
#
# Cost: the FREE toggles (BM25 + local dense) cost $0; the synthesis LLM call and any paid toggle
# (rerank/HyDE/multi-query/llm-router) are billable and cost-logged inside lib.providers — never here.

# The toggle keys the UI sends → fields on lib.retrieval.RetrievalConfig (one obvious mapping).
_TOGGLE_TO_CFG = {
    "use_bm25": "use_bm25", "use_dense": "use_dense", "use_rerank": "use_rerank",
    "use_hyde": "use_hyde", "use_multiquery": "use_multiquery",
    "use_llm_router": "use_llm_router", "auto_apply_route": "auto_apply_route",
}


def _cfg_from_request(embedding, reranker, toggles) -> "retrieval.RetrievalConfig":
    """Build a RetrievalConfig from the UI's embedding "model@dim", reranker, and toggle dict."""
    cfg = retrieval.RetrievalConfig()
    if embedding and isinstance(embedding, str) and "@" in embedding:
        model, dim = embedding.split("@", 1)
        cfg.embedding_model, cfg.embedding_dim = model, int(dim)
    if reranker:
        cfg.reranker = reranker
    for key, on in (toggles or {}).items():
        field = _TOGGLE_TO_CFG.get(key)
        if field:
            setattr(cfg, field, bool(on))
    # Don't let a chosen dense space silently fail: if no partition is built for it, drop to BM25-only
    # (lib.retrieval would degrade anyway, but disabling it here keeps the trace honest about cost).
    if cfg.use_dense:
        from lib import vectorstore
        if f"{cfg.embedding_model}@{cfg.embedding_dim}" not in set(vectorstore.spaces()):
            cfg.use_dense = False
    return cfg


def _synthesize_cited_answer(question: str, hits: list, llm_model: str, grade: dict,
                             system_prompt: str | None = None, history=None) -> str:
    """Ask the LLM to write a grounded answer over the retrieved hits (PAID), in a chosen PERSONA.

    The persona (system_prompt) sets the VOICE (default cited assistant / conversational archivist /
    first-person Solanus); we always append the grounding+citation contract and the numbered passages,
    so any persona stays anchored to the archive. `history` (prior turns) gives conversational memory.
    If the grade says "abstain", skip the LLM.
    """
    system, prompt, temp = _synthesis_inputs(question, hits, grade, system_prompt, history=history)
    if system is None:
        return prompt                       # the abstain message
    text, _ = llm.generate(prompt, model=llm_model, system=system, temperature=temp)
    return text.strip()


_ABSTAIN_MSG = ("I could not find support for this in the archive's letters or notebooks. "
                "(No sufficiently relevant passage was retrieved.)")


def _history_block(history, max_turns=6, max_chars=700):
    """Render the recent conversation as a short transcript the model can read for context.

    `history` is the client's bounded list of prior turns ({"q":.., "a":..}); we keep the last
    `max_turns`, trim each side to `max_chars`, and return "" when there's nothing usable. This is
    what gives the chat MEMORY: the model sees what was already said so follow-ups ("what about his
    sister?", "tell me more") resolve against the thread instead of being read cold.
    """
    if not history or not isinstance(history, list):
        return ""
    lines = []
    for turn in history[-max_turns:]:
        if not isinstance(turn, dict):
            continue
        q = (turn.get("q") or turn.get("query") or "").strip()
        a = (turn.get("a") or turn.get("answer") or turn.get("resp") or "").strip()
        # Strip [n] markers from prior answers: passages are renumbered 1..N fresh every query, so a
        # remembered [3] would point at an unrelated current source. Keep the prose, drop stale indices.
        a = re.sub(r"\s*\[\d+(?:\s*,\s*\d+)*\]", "", a).strip()
        if not q and not a:
            continue
        if q:
            lines.append(f"User: {q[:max_chars]}")
        if a:
            lines.append(f"Solanus: {a[:max_chars]}")
    if not lines:
        return ""
    return "Earlier in this conversation:\n" + "\n".join(lines) + "\n\n"


def _synthesis_inputs(question, hits, grade, system_prompt=None, history=None):
    """Build (system, prompt, temperature) for cited synthesis — shared by the one-shot and STREAMING paths.
    Returns (None, abstain_message, 0) when there's nothing to ground on."""
    persona = system_prompt or PERSONAS["default"]["prompt"]
    is_conversational = bool(system_prompt) and system_prompt != PERSONAS["default"]["prompt"]
    convo = _history_block(history)                 # recent turns → conversational memory (empty if none)
    if not hits or grade.get("verdict") == "abstain":
        if not is_conversational:
            return None, _ABSTAIN_MSG, 0        # the cited research assistant SHOULD abstain
        # Conversational personas (archivist / first-person Solanus) still answer greetings, small talk,
        # and clarifying questions when retrieval is empty — no abstain wall. No passages to cite, so drop
        # the citation contract but keep them from inventing archive facts.
        system = (persona + "\n\nNo archive passages were retrieved for this message. Respond naturally and "
                  "in character — a greeting, small talk, or a clarifying question is fine. Do NOT assert any "
                  "specific fact, date, person, or event about Solanus Casey or the archive that you cannot "
                  "support from a passage; if the user asked a factual question, gently say you have nothing "
                  "written on it and invite a more specific one. Write in FLOWING PROSE — no markdown, "
                  "asterisks, bullets, or headers.")
        return system, f"{convo}Message: {question}\n\n(No passages retrieved.)\n\nRespond:", 0.6
    numbered = "\n\n".join(f"[{i}] ({h.get('kind')}, {h.get('meta', {}).get('doc_id')}): "
                           f"{(h.get('text') or '')[:700]}" for i, h in enumerate(hits, 1))
    if is_conversational:
        # Solanus (first person) / the archivist guide are CONVERSATIONALISTS grounded in the source text:
        # draw voice + facts from the passages, but talk naturally — no inline [n] citation markers, and
        # never abstain. (The Sources panel still lists what was retrieved, so grounding stays transparent.)
        system = (persona + "\n\nGROUNDING: passages from the archive (his own letters and notebooks) are "
                  "given below. Let your manner and any specific facts — people, dates, places, events — come "
                  "from these passages or from what is well established about Solanus Casey's life; never "
                  "invent specifics you cannot support. But answer as natural, flowing CONVERSATION: NO "
                  "citation markers or [n] references, no bullet points, no headers. If a passage doesn't "
                  "bear on what was said, simply converse and don't force it in.")
        prompt = f"{convo}{question}\n\nPassages for grounding (draw on them; do not cite them):\n{numbered}\n\nRespond in conversation:"
        return system, prompt, 0.7
    # Default "cited research assistant": strict grounding with inline [n] citations.
    system = (persona + "\n\nGROUNDING: You are given numbered passages from the archive below. Base "
              "every factual claim ONLY on them and cite each with its [n] marker(s). You may speak "
              "naturally/conversationally, but do not introduce facts the passages don't support; if "
              "they don't answer the question, say so. Write in FLOWING PROSE — do not use markdown, "
              "asterisks, bullet points, or headers (the answer is rendered as typeset prose).")
    prompt = f"{convo}Question: {question}\n\nPassages:\n{numbered}\n\nAnswer (cite claims with [n]):"
    return system, prompt, 0.2


def _dedup_hits(hits: list) -> list:
    """Collapse hits that point to the SAME source so a page/region returned many times shows ONCE.

    Key = (doc_id, rid, pdf_page). Exact-region duplicates (same letter region surfaced by BM25 + dense +
    rerank, or one region retrieved repeatedly) merge into the first/best-scored hit. But two DIFFERENT
    docs/entries that happen to share a physical page keep different (doc_id, rid), so both survive as
    separate sources — each still highlights its own region in the citation viewer. Hits arrive ranked,
    so keeping the first occurrence keeps the best-scored copy.
    """
    seen, out = set(), []
    for h in hits:
        prov = h.get("provenance", {}) or {}
        if prov.get("doc_id"):
            key = _source_key(h.get("kind"), prov)
        else:                                          # no provenance → fall back to the passage text
            key = ("text", (h.get("text") or "").strip()[:160])
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def _source_key(kind: str, prov: dict):
    """The identity of a displayable source. A LETTER page is one source even if it has several regions
    (collapse by doc_id+page); a NOTEBOOK page can hold several distinct favor ENTRIES, so those stay
    separate (keyed by rid too). Different docs sharing a physical page always survive (distinct doc_id)."""
    page = prov.get("pdf_page") or prov.get("page")
    if kind == "letter":
        return (prov.get("doc_id"), page)
    return (prov.get("doc_id"), prov.get("rid"), page)


_FAMILY_CACHE = None
_KIN_RX = re.compile(r"\b(brothers?|sisters?|siblings?|family|families|kin|relatives?)\b", re.I)


def _solanus_family() -> dict:
    global _FAMILY_CACHE
    if _FAMILY_CACHE is None:
        try:
            _FAMILY_CACHE = json.loads((config.DATA / "solanus_family.json").read_text())
        except Exception:
            _FAMILY_CACHE = {"members": []}
    return _FAMILY_CACHE


def _expand_family_query(query: str):
    """Relationship-aware expansion: 'brother'/'sister' is religiously overloaded in this corpus (the friars
    are 'Brothers'), so a bare kin query retrieves lay brothers, not his siblings. When a question references
    Solanus's kin, append his ACTUAL family members' names (from data/solanus_family.json) so retrieval finds
    their letters. Returns (retrieval_query, matched_names) — query unchanged if no kin reference."""
    members = _solanus_family().get("members") or []
    if not members or not _KIN_RX.search(query):
        return query, []
    ql = query.lower()
    want, label = set(), "family"
    if re.search(r"\bbrothers?\b", ql):
        want.add("brother"); label = "brothers"
    if re.search(r"\bsisters?\b", ql):
        want.add("sister"); label = "sisters"
    if re.search(r"\bsiblings?\b", ql):
        want |= {"brother", "sister"}; label = "siblings"
    if re.search(r"\bnephews?\b", ql):
        want.add("nephew"); label = "nephews"
    if re.search(r"\bnieces?\b", ql):
        want.add("niece"); label = "nieces"
    if re.search(r"\b(parents?|mother|father|mama|papa)\b", ql):
        want |= {"father", "mother"}; label = "parents"
    if re.search(r"in-?laws?\b", ql):
        want.add("in-law")
    if re.search(r"\b(family|families|relatives?|kin|kinfolk)\b", ql) or not want:
        want = None; label = "family"          # whole family
    names = [(m["name"] if "casey" in m["name"].lower() else f"{m['name']} Casey")
             for m in members if m.get("in_corpus") and (want is None or m["relation"] in want)]
    if not names:
        return query, []
    return f"{query} — Father Solanus Casey's {label}: {', '.join(names)}", names


_ENRICH_CACHE = {"mtime": 0.0, "data": {}}


def _enriched_desc(node_id: str):
    """Detailed, book+corpus-grounded description from stages/enrich_descriptions.py (data/
    entity_descriptions.json). Read with an mtime refresh so descriptions appear LIVE as the
    background enrichment writes them — no server restart needed."""
    p = config.DATA / "entity_descriptions.json"
    try:
        mt = p.stat().st_mtime
        if mt != _ENRICH_CACHE["mtime"]:
            _ENRICH_CACHE["data"] = json.loads(p.read_text())
            _ENRICH_CACHE["mtime"] = mt
    except Exception:
        return None
    d = _ENRICH_CACHE["data"].get(node_id)
    return d.get("description") if d else None


def _citations_from_hits(hits: list) -> list:
    """Flatten ranked hits into the numbered citation rows app.js renders (shared by /api/ask + stream)."""
    out = []
    for i, h in enumerate(hits, 1):
        prov, meta = h.get("provenance", {}), h.get("meta", {})
        out.append({"n": i, "doc_id": prov.get("doc_id"), "rid": prov.get("rid"),
                    "kind": h.get("kind"), "section": prov.get("section") or meta.get("section"),
                    "page": prov.get("page"), "pdf_page": prov.get("pdf_page"),
                    "snippet": (h.get("text") or "")[:200],
                    # full grouped-document text (chunks are one-per-letter / one-per-stitched-notebook-entry,
                    # so h.text IS the whole document) — the UI shows this instead of the out-of-context snippet
                    "full_text": (h.get("text") or ""), "score": round(h.get("score", 0.0), 4),
                    "vertices": prov.get("vertices"), "min_conf": prov.get("min_conf")})
    return out


# ==================================================================
# THE SOLANUS VOICE LAYER — reverse-desanitizer (three-layer conversation)
# ==================================================================
# The plan (see pipeline_v3/step_7/docs/conversation_architecture.md): the big model owns the SUBSTANCE (a plain, cited
# answer), a stateless translator owns the VOICE, and a faithfulness check re-attaches the [n] citations
# with an EXACT-SET guarantee. Three message layers per turn:
#   user            — what the visitor typed
#   plain           — the big model's reasoned, cited reply (the reasoning layer / debug backstop)
#   styled_cited    — the SAME content in Fr. Solanus's voice, footnotes intact  ← what the user SEES
#                     and what feeds back as history (the canonical conversation).
# We reuse the mimicker's inference loop (pipeline_v3/mimicker/restyle_flow.py: restyle -> verify ->
# repair -> attach_citations) verbatim and just INJECT the app's voice backend: the Azure-hosted TRAINED
# translator when it's configured, else a frozen big-model restyler that works today. Every call routes
# through lib.providers.llm, so it's cost-logged like everything else. Until the trained endpoint is
# live the frozen backend voices answers; when David deploys it, it hot-swaps with no code change.
_RESTYLE = {"mod": None}
# 1 = restyle+verify+cite (snappy). Floored at 1: desanitize() needs >=1 iter or it raises (best unset).
_RESTYLE_MAX_ITERS = max(1, int(os.environ.get("RESTYLE_MAX_ITERS", "1")))


def _restyle_module():
    """Import the mimicker restyle loop once and inject the app's voice backend (Azure trained adapter
    if live, else frozen big model). restyle() is documented as pluggable; desanitize() calls the module
    global, so rebinding it here swaps the backend for the whole loop."""
    if _RESTYLE["mod"] is None:
        mim = str(config.REPO / "pipeline_v3" / "mimicker")
        if mim not in sys.path:
            sys.path.insert(0, mim)
        import restyle_flow                       # torch-free at import (adapter load is lazy + unused here)

        def _app_restyle(text):
            from app import translate as _translate
            if _translate.configured():           # the TRAINED reverse-desanitizer, hosted on Azure
                try:
                    return _translate.translate(text)
                except Exception:
                    log.warning("Azure translator failed; falling back to the frozen restyler")
            return restyle_flow.restyle_frozen(text)   # frozen big model + his real exemplars (works now)

        restyle_flow.restyle = _app_restyle
        _RESTYLE["mod"] = restyle_flow
    return _RESTYLE["mod"]


def _to_solanus_voice(plain_cited: str) -> dict:
    """Rewrite a plain (optionally cited) answer in Fr. Solanus's voice, footnotes preserved.

    Returns ``{final, plain, consistent, iters, sources_cited, backend, note?}``. `final` is guaranteed
    to carry the SAME [n] set as `plain`: if the styler can't transfer the citations, attach_citations
    returns the plain CITED text rather than voiced-but-uncited prose (no silent citation loss). Raises
    nothing the caller must handle — on any error we return the plain text as `final` so chat never breaks.
    (The caller also guards against an empty `final`.)"""
    plain_cited = (plain_cited or "").strip()
    if not plain_cited:
        return {"final": plain_cited, "plain": plain_cited, "consistent": True, "iters": 0,
                "sources_cited": [], "backend": "none"}
    try:
        rf = _restyle_module()
        r = rf.desanitize(plain_cited, max_iters=_RESTYLE_MAX_ITERS, verbose=False)
        from app import translate as _translate
        r["backend"] = "azure_adapter" if _translate.configured() else "frozen"
        return r
    except Exception:
        log.exception("restyle (Solanus voice) failed — returning the plain answer")
        return {"final": plain_cited, "plain": plain_cited, "consistent": False, "iters": 0,
                "sources_cited": [], "backend": "error", "note": "voice layer unavailable"}


# A short, honest label so styled prose is never mistaken for his literal words (the voice track is a
# study/creative impression; only DOCUMENTED words go in the public avatar). Surfaced by the UI.
_VOICE_NOTE = ("Rendered in Fr. Solanus Casey's voice by the style model — a faithful impression for study, "
               "not his literal words. The facts and citations come from the grounded answer.")


def single_shot_rag(query: str, llm_model: str | None = None, embedding=None,
                    reranker: str | None = None, kinds=None, toggles=None,
                    system_prompt: str | None = None, history=None, styled: bool = False) -> dict:
    """Run one toggle-driven retrieval + cited synthesis (the /api/ask path). Returns the UI's shape.

    Args:
        query: The user's question.
        llm_model: LLM model id (variable); None → config default.
        embedding: "model@dim" embedding space (variable); None → config default.
        reranker: Reranker model id (variable); None → config default.
        kinds: Optional ["letter"] / ["notebook_entry"] restriction.
        toggles: The UI's retrieval-technique switches (use_bm25, use_dense, use_rerank, …).

    Returns:
        ``{answer, citations[{n,doc_id,rid,kind,section,page,pdf_page,snippet,score,vertices,min_conf}],
           grade, trace{route, queries_used, steps[], cost}}`` — exactly what app.js renders.
    """
    llm_model = llm_model or config.DEFAULTS["llm"]
    cfg = _cfg_from_request(embedding, reranker, toggles)
    _cost0 = costlog.snapshot()                              # ledger BEFORE this query (for the delta)

    t0 = time.perf_counter()
    retr_query, fam_names = _expand_family_query(query)   # resolve 'his brothers/sisters' -> actual siblings
    result = retrieval.route_and_retrieve(retr_query, cfg, kinds=kinds)   # FREE unless a paid toggle is on
    retr_ms = round((time.perf_counter() - t0) * 1000, 1)
    hits = _dedup_hits(result.get("hits", []))     # one source per page/region (dups collapse, kept best)

    # number citations 1..N and flatten provenance into the row shape app.js expects.
    citations = _citations_from_hits(hits)

    plain = _synthesize_cited_answer(query, hits, llm_model, result.get("grade", {}),
                                     system_prompt=system_prompt, history=history)

    # VOICE LAYER: optionally restyle the plain reply into Fr. Solanus's voice (footnotes preserved).
    # `answer` is what the user sees + what feeds back as history; `answer_plain` is kept as a backstop.
    answer, answer_plain, voice = plain, None, None
    if styled:
        voice = _to_solanus_voice(plain)
        _final = (voice.get("final") or "").strip()      # never ship a blank primary answer
        answer, answer_plain = (_final or plain), plain

    # build the trace object the UI's "under the hood" panel reads (route + steps + cost).
    steps = [{"tool": "route_and_retrieve",
              "detail": f"BM25={cfg.use_bm25} dense={cfg.use_dense} rerank={cfg.use_rerank} "
                        f"hyde={cfg.use_hyde} multiquery={cfg.use_multiquery}",
              "items": len(hits), "ms": retr_ms},
             {"tool": "synthesize", "detail": f"LLM={llm_model} cited answer", "items": len(citations)}]
    if styled:
        steps.append({"tool": "restyle (Solanus voice)",
                      "detail": f"backend={voice.get('backend')} consistent={voice.get('consistent')} "
                                f"iters={voice.get('iters')}", "items": len(voice.get("sources_cited") or [])})
    # THIS query's own cost (delta) + the cumulative session total for context — so a $0.01 question
    # never displays as the whole running ledger.
    qcost = costlog.delta(_cost0, costlog.snapshot())
    qcost["cumulative_usd"] = round(sum(m.get("usd", 0.0) for m in costlog.summary().values()), 6)
    trace = {"route": result.get("route", {}), "queries_used": result.get("queries_used", [query]),
             "steps": steps, "cost": qcost}

    out = {"answer": answer, "citations": citations, "grade": result.get("grade", {}), "trace": trace}
    if styled:
        out.update({"answer_plain": answer_plain, "styled": True,
                    "voice_note": _VOICE_NOTE, "voice_consistent": voice.get("consistent")})
    return out


def region_payload(doc_id: str, rid: str | None = None, page: int | None = None) -> dict:
    """Region info in the app.js modal's shape (adds image_url/pdf_url/manifest_url + pixel size).

    Builds on :func:`source_region` (text + vertices + min_conf) and adds the deep-zoom hooks app.js
    wants: an ``image_url`` (served by /api/image), the page pixel size (so OpenSeadragon's overlay
    math is exact from the first frame), and the pdf/manifest URLs.
    """
    base = source_region(doc_id, rid, page=page)
    if not base.get("found") and base.get("regions") is None:
        return base
    section, pdf_page = base.get("section"), base.get("pdf_page")
    # Prefer the small transport image (/api/image serves it too). Its pixel size differs from the
    # OCR-grade source, so we must rescale the polygon vertices by the same factor — OpenSeadragon maps
    # the highlight using the LOADED image's real size, so unscaled source-pixel boxes would miss.
    web = _web_page(section, pdf_page)
    if web is not None:
        _, w, h, scale = web
        if scale and scale != 1.0:
            base["vertices"] = _scale_vertices(base.get("vertices"), scale)   # the highlighted region
            for r in (base.get("regions") or []):                            # and every page box
                r["vertices"] = _scale_vertices(r.get("vertices"), scale)
    else:
        w, h = _page_pixel_size(section, pdf_page)                            # fallback: masked.png dims
    base.update({
        "canvas":       f"p{pdf_page}" if pdf_page else None,
        "image_url":    f"/api/image/{section}/{pdf_page}" if (section and pdf_page) else None,
        "image_width":  w,
        "image_height": h,
        "pdf_url":      f"/api/pdf/{section}?page={pdf_page}" if (section and pdf_page) else None,
        "manifest_url": f"/api/manifest/{section}" if section else None,
    })
    return base


def _page_pixel_size(section, pdf_page):
    """Page image (width, height) in pixels — read from the page JSON, else the PNG header, else None.

    Mirrors index_sources._page_dims so the IIIF/overlay coordinate math agrees with what the indexer
    wrote. Returns (None, None) if the page can't be located (the UI then lets OSD infer the size).
    """
    if section is None or pdf_page is None:
        return None, None
    name = f"page_{pdf_page:03d}"
    base = config.ENRICHED / section
    for bucket in ("1_source_pages", "2_post_pages", "0_table_of_contents"):
        folder = base / bucket / name
        if folder.is_dir():
            pj = json.loads((folder / f"{name}.json").read_text()) if (folder / f"{name}.json").exists() else {}
            w, h = pj.get("page_width"), pj.get("page_height")
            if w and h:
                return int(w), int(h)
            try:
                from PIL import Image
                with Image.open(folder / f"{name}.masked.png") as im:
                    return im.size
            except Exception:
                return None, None
    return None, None


def _page_file(section, pdf_page, suffix):
    """Resolve an on-disk page asset path (``.masked.png`` / ``.masked.pdf``) or None."""
    if section is None or pdf_page is None:
        return None
    name = f"page_{int(pdf_page):03d}"
    base = config.ENRICHED / section
    for bucket in ("1_source_pages", "2_post_pages", "0_table_of_contents"):
        cand = base / bucket / name / f"{name}{suffix}"
        if cand.exists():
            return cand
    return None


def _web_page(section, pdf_page):
    """Transport image derived from the masked scan: (Path, w, h, scale) or None if not built.

    ``stages/build_web_pages.py`` writes ``page_NNN.web.webp`` (the scan downscaled ~20x) + a
    ``page_NNN.web.json`` sidecar {w, h, src_w, src_h, scale}. ``scale = web_w / src_w`` is what maps
    the OCR polygon ``vertices`` (source-pixel space) onto the smaller image so the highlight still
    lands. Returns None when the web set hasn't been generated → the server falls back to masked.png.
    """
    if section is None or pdf_page is None:
        return None
    name = f"page_{int(pdf_page):03d}"
    base = config.ENRICHED / section
    for bucket in ("1_source_pages", "2_post_pages", "0_table_of_contents"):
        folder = base / bucket / name
        img = folder / f"{name}.web.webp"
        if img.exists():
            meta = folder / f"{name}.web.json"
            if meta.exists():
                try:
                    m = json.loads(meta.read_text())
                    return img, int(m["w"]), int(m["h"]), float(m["scale"])
                except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                    pass
            # Sidecar missing/corrupt (e.g. a write torn between webp and json). Derive the scale from
            # the SOURCE page dims so the vertices still map correctly: scale = webp_w / src_w. Never
            # return scale=1.0 with downscaled dims — that leaves the polygon in full-res source space
            # over a small image and the highlight misses. If we can't recover the source dims, return
            # None so api_image serves masked.png and region_payload keeps source-space vertices (both
            # in source dims → still consistent).
            try:
                from PIL import Image
                with Image.open(img) as im:
                    ww, hh = im.size
                sw, _sh = _page_pixel_size(section, pdf_page)   # source (masked.png / page JSON) width
                if sw and ww:
                    return img, ww, hh, ww / float(sw)
            except Exception:
                pass
            return None
    return None


def _scale_vertices(verts, scale):
    """Multiply a polygon's coordinates by `scale` (source-pixel space -> web-image space).

    Handles both on-disk shapes: [[x, y], ...] arrays (documents.json / notebooks.json) and
    [{x, y}, ...] dicts (page JSON). Anything else is passed through untouched.
    """
    if not verts or not isinstance(verts, list):
        return verts
    out = []
    for p in verts:
        if isinstance(p, dict) and "x" in p and "y" in p:
            out.append({**p, "x": p["x"] * scale, "y": p["y"] * scale})
        elif isinstance(p, (list, tuple)) and len(p) >= 2:
            out.append([p[0] * scale, p[1] * scale])
        else:
            out.append(p)
    return out


# ==================================================================
# Request models — defined at MODULE level on purpose
# ==================================================================
# This file uses `from __future__ import annotations`, so every annotation is a *string* evaluated
# lazily. FastAPI resolves a handler's body type via typing.get_type_hints, which only sees a class if
# it lives in the MODULE globals — a Pydantic model defined *inside* create_app() is invisible to that
# lookup, so FastAPI silently demotes `req: QueryRequest` to a query parameter and every POST 422s
# ("field required: req"). Keeping the models here fixes that. Guarded so importing this module on a
# machine without pydantic still works (create_app — which needs the web deps — just won't be called).
try:
    from pydantic import BaseModel as _ReqBaseModel

    class QueryRequest(_ReqBaseModel):
        """Body for POST /api/query. All model variables + tool toggles are optional (sane defaults)."""
        question: str
        llm: str | None = None                       # LLM variable (config.LLMS key)
        embedding_space: list | str | None = None    # embedding variable: [model, dim] or "model@dim"
        reranker: str | None = None                  # reranker variable (config.RERANKERS key)
        enabled_tools: list[str] | None = None       # which tools the agent may use (None → defaults)
        max_steps: int = 6                           # safety cap on tool-call rounds

    class AskRequest(_ReqBaseModel):
        """Body for POST /api/ask (the UI's request). Mirrors the left-rail RetrievalConfig."""
        query: str
        llm: str | None = None
        embedding: str | None = None                 # "model@dim"
        reranker: str | None = None
        kinds: list[str] | None = None
        toggles: dict | None = None                  # {use_bm25:true, use_dense:true, ...}
        system_prompt: str | None = None             # persona/system prompt (default/archivist/solanus/custom)
        history: list | None = None                  # prior turns [{q, a}] for conversational memory (bounded)
        styled: bool = False                         # restyle the answer into Solanus's voice (reverse-desanitizer)

    class TtsRequest(_ReqBaseModel):
        """Body for POST /api/tts."""
        text: str
        provider: str | None = None                  # local | azure | gcp (else server default)
        voice: str | None = None                     # provider-specific voice name (optional)

    class TranslateRequest(_ReqBaseModel):
        """Body for POST /api/translate (the Solanus STYLE translator / reverse-desanitizer)."""
        text: str                                    # plain modern English to rewrite in his voice
        max_tokens: int | None = None
        temperature: float | None = None

    from starlette.requests import Request            # module-level so /api/stt's annotation resolves
except Exception:                                    # pydantic/starlette absent → web path unused anyway
    QueryRequest = AskRequest = TtsRequest = TranslateRequest = Request = None


# ==================================================================
# Graph subgraph service — bounded, cached views for the Cytoscape viz
# ==================================================================
# data/graph.json is ~47k nodes / 65k edges (42 MB). Shipping the whole thing to the browser hangs
# Cytoscape and re-parsing 42 MB per request is wasteful, so we (1) cache the parsed graph + a degree
# index keyed on the file mtime, and (2) return only a BOUNDED, meaningful subgraph: by default the
# highest-degree "backbone", optionally filtered by node kind or centered on one node's neighborhood.
_ENTITY_KINDS = {"person", "place", "organization", "condition", "favor", "outcome", "role", "event"}
_GRAPH_CACHE: dict = {"mtime": None, "data": None, "deg": None, "byid": None}


def _graph_cache() -> dict:
    """Parse data/graph.json once per file version; cache the parse + a node-degree index + id map."""
    p = config.DATA / "graph.json"
    mt = p.stat().st_mtime
    if _GRAPH_CACHE["mtime"] != mt:
        data = json.loads(p.read_text())
        deg: dict = {}
        for e in data.get("edges", []):
            deg[e["source"]] = deg.get(e["source"], 0) + 1
            deg[e["target"]] = deg.get(e["target"], 0) + 1
        _GRAPH_CACHE.update(mtime=mt, data=data, deg=deg,
                            byid={n["id"]: n for n in data.get("nodes", [])})
    return _GRAPH_CACHE


_GRAPH_IDX: dict = {"mtime": None, "idx": None}


def _graph_indexes() -> dict:
    """Projections precomputed once per graph.json version so endpoints do dict lookups / slices instead
    of re-scanning ~32k nodes + ~63k edges on every request (the pattern _people_index already proves,
    generalized). Keyed on the same mtime as _graph_cache(). Provides:
      nodes_by_kind   {kind -> [node]}          (file order preserved within a kind)
      section_by_doc  {notebook_page doc_id -> section}
      letter_order    [letter id] sorted by (edtf, doc_id)   + letter_pos {id -> index}   (prev/next)
      adj             {node id -> [incident edge]}           (each edge once per endpoint; self-loops once)
      semantic_degree {node id -> degree excluding structural MENTIONED_IN/DATED_IN edges}
    """
    from collections import defaultdict
    c = _graph_cache()
    mt = c["mtime"]
    if _GRAPH_IDX["mtime"] == mt and _GRAPH_IDX["idx"] is not None:
        return _GRAPH_IDX["idx"]
    data = c["data"]
    nodes_by_kind: dict = defaultdict(list)
    section_by_doc: dict = {}
    for n in data.get("nodes", []):
        k = n.get("kind")
        nodes_by_kind[k].append(n)
        if k == "notebook_page":
            section_by_doc[n.get("doc_id")] = n.get("section")
    letter_order = [n["id"] for n in sorted(
        nodes_by_kind.get("letter", []),
        key=lambda x: (x.get("edtf") or "zzzz", x.get("doc_id") or ""))]
    letter_pos = {lid: i for i, lid in enumerate(letter_order)}
    adj: dict = defaultdict(list)
    semantic_degree: dict = defaultdict(int)
    for e in data.get("edges", []):
        s, t = e["source"], e["target"]
        adj[s].append(e)
        if t != s:                      # a self-loop is incident to its node exactly once
            adj[t].append(e)
        if (e.get("kind") or "").upper() in ("MENTIONED_IN", "DATED_IN"):
            continue
        semantic_degree[s] += 1
        semantic_degree[t] += 1
    idx = {"nodes_by_kind": dict(nodes_by_kind), "section_by_doc": section_by_doc,
           "letter_order": letter_order, "letter_pos": letter_pos,
           "adj": dict(adj), "semantic_degree": dict(semantic_degree)}
    _GRAPH_IDX.update(mtime=mt, idx=idx)
    return idx


# Structural edges (every entity -> the record it's mentioned in, every record -> its year) make up
# ~80% of edges and turn the viz into one giant hairball. They're excluded from the default view; the
# SEMANTIC relations below are the interpretable network ("who wrote to whom", "X enrolled for Y").
_STRUCTURAL_EDGES = {"MENTIONED_IN", "DATED_IN"}

_COMM_CACHE: dict = {"mtime": None, "node_community": {}, "communities": []}


def _communities() -> dict:
    """Louvain communities from stages/graph_analysis.py (data/communities.json). Empty until that
    stage has run; cached by file mtime. Lets the graph colour by community + offer community browsing."""
    p = config.DATA / "communities.json"
    if not p.exists():
        return {"node_community": {}, "communities": []}
    mt = p.stat().st_mtime
    if _COMM_CACHE["mtime"] != mt:
        try:
            d = json.loads(p.read_text())
            _COMM_CACHE.update(mtime=mt, node_community=d.get("node_community", {}),
                               communities=d.get("communities", []))
        except Exception:
            pass
    return _COMM_CACHE


_ENT_CACHE: dict = {"mtime": None, "byid": None}
_RECTEXT_CACHE: dict = {"docs_mtime": None, "letters": None, "entries": None, "pages": None}


def _entities_index() -> dict:
    """{entity_id -> entity record} from entities.json (mtime-cached) — carries the mention list."""
    p = config.DATA / "entities.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _ENT_CACHE["mtime"] != mt:
        ents = (json.loads(p.read_text()).get("entities", []) if p.exists() else [])
        _ENT_CACHE.update(mtime=mt, byid={e.get("id"): e for e in ents})
    return _ENT_CACHE["byid"] or {}


def _entity_source_count(eid: str) -> int:
    """Number of DISTINCT source passages (unique doc_id+rid) behind an entity — i.e. how many source
    cards the dossier actually shows. The headline count uses THIS, not the raw mention tally, so the
    number a user sees (on the map, marker, and dossier header) always equals the documents they can
    open. (Raw mentions double-count a page that names the place twice — the 6-vs-5 Rochester case.)"""
    ent = _entities_index().get(eid, {})
    seen = set()
    for m in ent.get("mentions", []):
        prov = m.get("provenance", {})
        seen.add((prov.get("doc_id"), prov.get("rid")))
    return len(seen)


_CITE_RE = re.compile(r"\[\s*\d+(?:\s*[,&–-]\s*\d+)*\s*\]")


def _strip_citations(text: str) -> str:
    """Safety net for TTS: never speak citation markers ([1], [1, 2], [1-3]). The frontend already cleans
    for speech (titles, date ranges); this guarantees the numbers are gone no matter which path calls TTS."""
    t = re.sub(r"\s+([.,;:!?])", r"\1", _CITE_RE.sub("", text or ""))   # tidy " ." left by a removed citation
    return re.sub(r"\s{2,}", " ", t).strip()


_PLACE_ENR: dict = {"mtime": None, "data": {}}


def _place_enrichment() -> dict:
    """{entity_id -> enrichment} from data/place_enrichment.json (mtime-cached). Holds the building-level
    geocode (lat/lon/precise/address) and the researched dossier (what_it_was, fate, significance, summary,
    scholarly_sources, people). A non-destructive side-car the map + dossier OVERLAY — the entity store and
    geocodes.json are never touched, so the city centroid stays as the fallback."""
    p = config.DATA / "place_enrichment.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _PLACE_ENR["mtime"] != mt:
        try:
            _PLACE_ENR.update(mtime=mt, data=(json.loads(p.read_text()) if p.exists() else {}))
        except Exception:
            _PLACE_ENR.update(mtime=mt, data={})
    return _PLACE_ENR["data"] or {}


_DOCS_IDX: dict = {"mtime": None, "byid": None}


def _docs_index() -> dict:
    """{doc_id -> letter record} from documents.json (for letter from/to/date in the detail panel)."""
    p = config.DOCUMENTS
    mt = p.stat().st_mtime if p.exists() else None
    if _DOCS_IDX["mtime"] != mt:
        _DOCS_IDX.update(mtime=mt, byid={d["id"]: d for d in json.loads(p.read_text())})
    return _DOCS_IDX["byid"] or {}


# ---------------------------------------------------------------------------
# Additive read-only helpers for the archive-content pages (His Life, Reading
# Room, People, Family, "Solanus and ___"). All build on the cached graph +
# side-cars; none mutate data or change the shape of an existing endpoint.
# ---------------------------------------------------------------------------
SOLANUS_ID = "person:solanus_capuchin:0006"     # the canonical Father Solanus Casey node

_NAME_AUTH: dict = {"mtime": None, "data": {}}


def _name_authority() -> dict:
    """{entity_id -> {qid, viaf, wikidata_label, wikidata_desc, authority_name}} for the CONFIDENT matches
    in data/name_authority.json (mtime-cached). A non-destructive side-car: the graph/entity store carry
    null authority, and this overlays the reconciled Wikidata QID + VIAF id so a dossier or person card can
    link out to the authority record. ~308 confident links as of the last name_authority run."""
    p = config.DATA / "name_authority.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _NAME_AUTH["mtime"] != mt:
        out: dict = {}
        try:
            matches = (json.loads(p.read_text()).get("matches", {}) if p.exists() else {})
            for eid, m in matches.items():
                if not m.get("confident"):
                    continue
                wd = m.get("wikidata") or {}
                out[eid] = {"qid": m.get("qid"), "viaf": m.get("viaf"),
                            "wikidata_label": wd.get("label"), "wikidata_desc": wd.get("description"),
                            "authority_name": m.get("authority_name")}
        except Exception:
            out = {}
        _NAME_AUTH.update(mtime=mt, data=out)
    return _NAME_AUTH["data"] or {}


def _authority_links(eid: str) -> dict:
    """The name_authority overlay for one entity id, with ready-to-use external URLs (or {} if none)."""
    a = _name_authority().get(eid)
    if not a:
        return {}
    out = dict(a)
    if a.get("qid"):
        out["wikidata_url"] = f"https://www.wikidata.org/wiki/{a['qid']}"
    if a.get("viaf"):
        out["viaf_url"] = f"https://viaf.org/viaf/{a['viaf']}"
    return out


_PEOPLE_IDX: dict = {"mtime": None, "rows": None}


def _people_index() -> list:
    """The People directory, cached by graph mtime: every 'person' graph node ranked by prominence
    (semantic degree = edges that aren't the structural MENTIONED_IN/DATED_IN). Rows are light (no source
    texts) so 7,900+ people list fast; the dossier (/api/entity) still carries the heavy detail. 'letters'
    counts WROTE_TO edges (Solanus -> recipient). Authority (Wikidata/VIAF) is overlaid from name_authority."""
    from collections import defaultdict
    c = _graph_cache()
    mt = c["mtime"]
    if _PEOPLE_IDX["mtime"] == mt and _PEOPLE_IDX["rows"] is not None:
        return _PEOPLE_IDX["rows"]
    data = c["data"]
    deg: dict = defaultdict(int)
    letters_to: dict = defaultdict(int)
    for e in data.get("edges", []):
        k = (e.get("kind") or "").upper()
        if k in ("MENTIONED_IN", "DATED_IN"):
            continue
        deg[e["source"]] += 1
        deg[e["target"]] += 1
        if k == "WROTE_TO":
            letters_to[e["target"]] += 1
    auth = _name_authority()
    rows = []
    for n in data.get("nodes", []):
        if n.get("kind") != "person":
            continue
        nid = n["id"]
        a = auth.get(nid) or {}
        rows.append({
            "id": nid, "name": n.get("label"),
            "role": (n.get("attrs") or {}).get("role") or n.get("role"),
            "relation_to_solanus": n.get("relation_to_solanus"),
            "mentions": n.get("mention_count") or 0,
            "degree": deg.get(nid, 0),
            "letters": letters_to.get(nid, 0),
            "variants": n.get("variants") or [],
            "needs_review": bool(n.get("needs_review")),
            "description": n.get("description"),
            "qid": a.get("qid"), "viaf": a.get("viaf"),
        })
    rows.sort(key=lambda r: (r["degree"], r["mentions"]), reverse=True)
    _PEOPLE_IDX.update(mtime=mt, rows=rows)
    return rows


_LETTER_SUMM: dict = {"mtime": None, "data": {}}


def _letter_summaries() -> dict:
    """{letter_id -> {summary, recipient, edtf, date}} from the data/letter_summaries.json side-car
    (mtime-cached), or {} until generated. One grounded sentence per letter so the Reading Room index and
    the His Life timeline are scannable; a non-destructive overlay on /api/letters + /api/letter."""
    p = config.DATA / "letter_summaries.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _LETTER_SUMM["mtime"] != mt:
        try:
            _LETTER_SUMM.update(mtime=mt, data=(json.loads(p.read_text()) if p.exists() else {}))
        except Exception:
            _LETTER_SUMM.update(mtime=mt, data={})
    return _LETTER_SUMM["data"] or {}


_NB_THEMES: dict = {"mtime": None, "data": {}}


def _notebook_page_themes() -> dict:
    """{doc_id -> {theme, gloss, tags}} from the data/notebook_page_themes.json side-car (mtime-cached),
    or {} until generated. A per-page characterization overlaid onto notebook entries so a lone petition
    fragment shows the page it belongs to. Non-destructive."""
    p = config.DATA / "notebook_page_themes.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _NB_THEMES["mtime"] != mt:
        try:
            _NB_THEMES.update(mtime=mt, data=(json.loads(p.read_text()) if p.exists() else {}))
        except Exception:
            _NB_THEMES.update(mtime=mt, data={})
    return _NB_THEMES["data"] or {}


_REL_NARR: dict = {"mtime": None, "data": {}}


def _relationship_narratives() -> dict:
    """{person_id -> {title, narrative, arc[], sources[], years[], n_letters}} from the
    data/relationship_narratives.json side-car (mtime-cached), or {} until it's generated. Non-destructive:
    the LLM-written 'Solanus and ___' stories overlay the live correspondence facts, they never replace them."""
    p = config.DATA / "relationship_narratives.json"
    mt = p.stat().st_mtime if p.exists() else None
    if _REL_NARR["mtime"] != mt:
        try:
            _REL_NARR.update(mtime=mt, data=(json.loads(p.read_text()) if p.exists() else {}))
        except Exception:
            _REL_NARR.update(mtime=mt, data={})
    return _REL_NARR["data"] or {}


_ENTITY_BOOK_CACHE: dict = {}
_CONN_CACHE: dict = {}            # (frozenset{a,b}, explain) -> connection payload (avoids repeat LLM cost)
_BOOK_MIN_SCORE = 0.58            # only show biography passages that are actually about this entity


def _entity_book(name: str, key: str, top_k: int = 3) -> list:
    """Crosby-biography passages mentioning an entity, relevance-filtered + cached by entity id.

    Integrates the secondary literature into a dossier by default (David's ask), without re-embedding on
    every panel open. Each hit carries its BOOK PAGE so the UI cites it as Crosby, not the archive.
    """
    if key in _ENTITY_BOOK_CACHE:
        return _ENTITY_BOOK_CACHE[key]
    out = []
    try:
        from app.tools import book_search
        res = book_search.run({"query": name, "top_k": top_k}) or {}
        for h in (res.get("hits") or []):
            if (h.get("score") or 0) >= _BOOK_MIN_SCORE:
                out.append({"text": (h.get("text") or "")[:520], "page": h.get("page"),
                            "title": h.get("title") or "Thank God Ahead of Time",
                            "score": round(h.get("score") or 0, 3)})
    except Exception:
        out = []
    _ENTITY_BOOK_CACHE[key] = out
    return out


def _record_citation(nid: str) -> dict:
    """Resolve a RECORD node id -> an openable citation (doc/rid/page + readable label + text). Shared by
    the entity dossier and the connection endpoint so 'open the scan' works from either."""
    n = _graph_cache()["byid"].get(nid, {})
    doc_id = n.get("doc_id") or (nid.split(":", 1)[-1] if ":" in nid else nid)
    label_bits = [x for x in (n.get("notebook") or n.get("section"),
                              n.get("page_label") or (f"p.{n.get('page')}" if n.get("page") else None)) if x]
    return {"id": nid, "label": n.get("label"), "kind": n.get("kind"),
            "doc_id": doc_id, "rid": n.get("rid"), "page": n.get("page"),
            "pdf_page": n.get("pdf_page"), "section": n.get("section"),
            "notebook": n.get("notebook"), "page_label": n.get("page_label"),
            "cite_label": "  ".join(str(x) for x in label_bits) or doc_id,
            "date": n.get("date_raw") or n.get("edtf"),
            "text": (_record_text(doc_id, n.get("rid")) or n.get("text", ""))[:800]}


def _record_text(doc_id: str, rid: str | None) -> str:
    """Source text for a (doc_id, rid): a notebook entry's text, or a letter's full text. Cached."""
    p = config.DOCUMENTS
    mt = p.stat().st_mtime if p.exists() else None
    if _RECTEXT_CACHE["docs_mtime"] != mt:
        from lib import chunks as chunks_lib
        from lib import textclean
        letters = {d["id"]: textclean.clean(chunks_lib._letter_text(d))
                   for d in json.loads(config.DOCUMENTS.read_text())}
        entries, pages = {}, {}
        for page in json.loads(config.NOTEBOOKS.read_text()):
            for e in page.get("entries", []):
                entries[(page["id"], e.get("rid"))] = textclean.clean((e.get("text") or "").strip())
            # a notebook PAGE has no text field of its own — its OCR lives on the entries (preferred,
            # logical units) or, failing that, the raw regions. Assemble page-level text so a page node
            # (rid=None) still resolves to its transcription instead of showing "no transcribed text".
            ptxt = "\n\n".join(t for t in (textclean.clean((e.get("text") or "").strip())
                                           for e in page.get("entries", [])) if t)
            if not ptxt:
                ptxt = "\n\n".join(t for t in (textclean.clean((r.get("text") or "").strip())
                                               for r in page.get("regions", [])) if t)
            pages[page["id"]] = ptxt
        _RECTEXT_CACHE.update(docs_mtime=mt, letters=letters, entries=entries, pages=pages)
    return (_RECTEXT_CACHE["entries"].get((doc_id, rid))
            or _RECTEXT_CACHE["letters"].get(doc_id)
            or (_RECTEXT_CACHE.get("pages", {}).get(doc_id) if not rid else None) or "")


def _subgraph(limit: int = 250, kinds: str | None = None, center: str | None = None,
              hops: int = 1, entities_only: bool = False, q: str | None = None,
              rels: str | None = None, structural: bool = False) -> dict:
    """Return a bounded, LEGIBLE subgraph for the viz.

    By default we show only the SEMANTIC relation edges (wrote_to, enrolled, has_condition, located_at,
    family, …) and rank nodes by their degree IN THAT relation graph — so the view is the interpretable
    network, not a mention hairball. ``structural=True`` re-includes mention/date edges; ``rels`` (CSV)
    restricts to specific edge kinds. ``q`` resolves a free-text label to a center (prefers entities).
    """
    c = _graph_cache()
    data, byid = c["data"], c["byid"]
    nodes, all_edges = data.get("nodes", []), data.get("edges", [])
    kindset = {k.strip() for k in kinds.split(",")} if kinds else None
    relset = {r.strip().upper() for r in rels.split(",")} if rels else None

    # choose which edges count for THIS view (degree, adjacency, and the returned edges all use these)
    def edge_ok(e) -> bool:
        k = (e.get("kind") or "").upper()
        if relset is not None:
            return k in relset
        return structural or k not in _STRUCTURAL_EDGES
    edges = [e for e in all_edges if edge_ok(e)]

    deg: dict = {}
    for e in edges:
        deg[e["source"]] = deg.get(e["source"], 0) + 1
        deg[e["target"]] = deg.get(e["target"], 0) + 1

    if q and not center:
        ql = q.lower().strip()
        matches = [n for n in nodes
                   if ql in str(n.get("label", "")).lower() or ql in str(n.get("canonical_name", "")).lower()]
        if matches:
            # prefer an ENTITY node over a record/year that merely mentions the term, then highest-degree
            center = max(matches, key=lambda n: (n.get("kind") in _ENTITY_KINDS,
                                                 deg.get(n["id"], 0)))["id"]

    def ok(n) -> bool:
        k = n.get("kind")
        if entities_only and k not in _ENTITY_KINDS:
            return False
        return not (kindset and k not in kindset)

    if center and center in byid:
        adj: dict = {}
        for e in edges:
            adj.setdefault(e["source"], set()).add(e["target"])
            adj.setdefault(e["target"], set()).add(e["source"])
        seen, frontier = {center}, {center}
        for _ in range(max(1, hops)):
            nxt = set()
            for u in frontier:
                nxt |= adj.get(u, set())
            seen |= nxt
            frontier = nxt
            if len(seen) > limit:
                break
        pool = [byid[i] for i in seen if i in byid and ok(byid[i])]
    else:
        # default backbone: only nodes that actually participate in the semantic graph (degree > 0),
        # so isolated mention-only nodes don't clutter the view.
        pool = [n for n in nodes if ok(n) and deg.get(n["id"], 0) > 0]
    pool.sort(key=lambda n: deg.get(n["id"], 0), reverse=True)
    chosen = pool[:limit]
    keep = {n["id"] for n in chosen}
    # attach Louvain community id to each node (if graph_analysis has run) so the UI can colour by it
    ncom = _communities().get("node_community", {})
    if ncom:
        for n in chosen:
            cid = ncom.get(n["id"])
            if cid is not None:
                n["community"] = cid
    sub_edges = [e for e in edges if e["source"] in keep and e["target"] in keep]
    eligible = sum(1 for n in nodes if ok(n) and deg.get(n["id"], 0) > 0)
    return {"nodes": chosen, "edges": sub_edges,
            "meta": {**data.get("meta", {}), "shown_nodes": len(chosen), "shown_edges": len(sub_edges),
                     "total_nodes": len(nodes), "total_edges": len(all_edges),
                     "eligible_nodes": eligible, "truncated": len(chosen) < eligible,
                     "center": center if (center and center in keep) else None,
                     "view": "neighborhood" if center else ("relations" if not structural else "all")}}


# ==================================================================
# FastAPI app — lazily built so importing this module never needs the web deps
# ==================================================================
# We construct the app inside `create_app()` and only call it at import-time IF FastAPI is installed.
# That keeps `import app.server` usable for tests/inspection on a machine without fastapi, while
# `uvicorn app.server:app` still finds a ready `app` object when the deps ARE present. If they're not,
# `app` is a tiny stub whose any-route handler explains how to install them — a clear, actionable
# failure instead of an ImportError at startup.
def create_app():
    """Build and return the FastAPI application (requires fastapi + pydantic in the venv)."""
    from fastapi import FastAPI, Query, HTTPException
    from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel

    api = FastAPI(title="Solanus Casey Archival Dev Tool",
                  description="Tool-using RAG agent over Father Solanus Casey's letters & notebooks.",
                  version="0.1.0")

    # ---- POST /api/query : the agent (QueryRequest is module-level — see note there) ----
    @api.post("/api/query")
    def api_query(req: QueryRequest):
        """Run the tool-using agent and return answer + citations + full trace + cost.

        ⚠️ This endpoint MAKES PAID LLM CALLS (the agent reasons with the model). Retrieval tools are
        free with the local embedding/reranker variables; the LLM turns are billable and cost-logged.
        """
        tracer = _JsonTracer(uuid.uuid4().hex[:12])
        try:
            result = agent_query(
                question=req.question, llm_model=req.llm, embedding_space=req.embedding_space,
                reranker=req.reranker, enabled_tools=req.enabled_tools,
                max_steps=req.max_steps, tracer=tracer,
            )
        except Exception as e:                          # surface a clean 500 + the partial trace
            log.exception("agent_query failed")
            raise HTTPException(status_code=500,
                                detail={"error": f"{type(e).__name__}: {e}",
                                        "trace": tracer.records})
        return JSONResponse(result)

    # ---- GET /api/graph : the KG for the visualization ------------------
    @api.get("/api/graph")
    def api_graph(limit: int = Query(250, ge=10, le=3000, description="max nodes to return"),
                  kinds: str | None = Query(None, description="CSV of node kinds to include"),
                  center: str | None = Query(None, description="node id to center an N-hop neighborhood on"),
                  q: str | None = Query(None, description="free-text label search -> center on best match"),
                  hops: int = Query(1, ge=1, le=3, description="neighborhood radius when center is set"),
                  entities_only: bool = Query(False, description="drop record/year nodes, keep entities"),
                  rels: str | None = Query(None, description="CSV of edge kinds to include"),
                  structural: bool = Query(False, description="include mention/date edges (default: relations only)")):
        """Serve a BOUNDED, cached subgraph of data/graph.json for the viz (full graph is ~47k nodes).

        Default = the highest-degree backbone (limit nodes) + the edges among them. Pass ``kinds`` to
        filter by node kind, or ``center``+``hops`` for one entity's neighborhood. ``meta`` reports
        shown-vs-total so the UI can say "showing 300 of 46,931".
        """
        from app.tools import graph_query as gq
        if not gq._maybe_build_graph_json():
            raise HTTPException(status_code=404,
                                detail="data/graph.json not built and networkx unavailable — run "
                                       "stages/build_graph.py (after `pip install networkx`).")
        return JSONResponse(_subgraph(limit=limit, kinds=kinds, center=center, hops=hops,
                                      entities_only=entities_only, q=q, rels=rels, structural=structural))

    # ---- GET /api/entities : browse/search entities (top-connected first) — the explorer's index ----
    @api.get("/api/entities")
    def api_entities(q: str | None = Query(None), kind: str | None = Query(None),
                     limit: int = Query(40, ge=1, le=200)):
        """A searchable, kind-filterable list of entities ranked by how connected they are — so the user
        can DISCOVER where to start exploring the full graph (not just guess a name)."""
        from app.tools import graph_query as gq
        gq._maybe_build_graph_json()
        data = _graph_cache()["data"]
        deg = _graph_indexes()["semantic_degree"]   # mtime-keyed, so it can't go stale after a rebuild
        ent_kinds = {"person", "place", "organization", "condition", "favor", "outcome", "role", "event"}
        ql = (q or "").lower().strip()
        out = []
        for n in data.get("nodes", []):
            k = n.get("kind")
            if k not in ent_kinds or (kind and k != kind):
                continue
            lbl = n.get("label") or ""
            if ql and ql not in lbl.lower():
                continue
            out.append({"id": n["id"], "label": lbl, "kind": k,
                        "degree": deg.get(n["id"], 0),
                        "relation_to_solanus": n.get("relation_to_solanus")})
        out.sort(key=lambda x: x["degree"], reverse=True)
        return JSONResponse({"entities": out[:limit], "total": len(out)})

    # ---- GET /api/communities : Louvain clusters + the Mapper overview (from graph_analysis.py) ----
    @api.get("/api/communities")
    def api_communities():
        """Thematic clusters of the archive (Louvain) + a Mapper-style 'shape' minimap. Empty until
        stages/graph_analysis.py has run. Each community lists its top members so the UI can offer
        'jump into this cluster' as a whole-graph entry point."""
        c = _communities()
        mp = {}
        mpath = config.DATA / "mapper.json"
        if mpath.exists():
            try:
                mp = json.loads(mpath.read_text())
            except Exception:
                mp = {}
        return JSONResponse({"communities": c.get("communities", []),
                             "n_communities": len(c.get("communities", [])),
                             "mapper": {"nodes": mp.get("nodes", []), "edges": mp.get("edges", [])}})

    # ---- GET /api/timeline : records-per-year histogram (Solanus's career trajectory) ----
    @api.get("/api/timeline")
    def api_timeline():
        """Year histogram from the graph's DATED_IN edges — records per year, split by kind. Powers the
        timeline strip; reflects the temporal spine so you can see the arc of Solanus's career. Reads
        the cached graph, so it updates whenever the graph is rebuilt."""
        from app.tools import graph_query as gq
        if not gq._maybe_build_graph_json():
            return JSONResponse({"years": []})
        c = _graph_cache()
        byid = c["byid"]
        years: dict = {}
        for e in c["data"].get("edges", []):
            if (e.get("kind") or "").upper() != "DATED_IN":
                continue
            yr = (byid.get(e["target"]) or {}).get("year")
            if yr is None:
                continue
            kind = (byid.get(e["source"]) or {}).get("kind") or "other"
            d = years.setdefault(int(yr), {"year": int(yr), "letter": 0, "notebook_entry": 0, "total": 0})
            d[kind] = d.get(kind, 0) + 1
            d["total"] += 1
        return JSONResponse({"years": sorted(years.values(), key=lambda x: x["year"])})

    # ---- GET /api/entity : full dossier for one node — summary + relations + EVERY source text ----
    @api.get("/api/entity")
    def api_entity(id: str = Query(..., description="graph node id")):
        """Everything behind a node: its enriched summary, its semantic relations (who/what it's
        connected to + when), and the FULL text of every record that mentions it — each resolvable to
        the scanned region. This is the 'review all the texts relating to it' view."""
        from app.tools import graph_query as gq
        gq._maybe_build_graph_json()
        node = _graph_cache()["byid"].get(id)
        if not node:
            raise HTTPException(status_code=404, detail=f"no node {id!r}")
        byid = _graph_cache()["byid"]
        # source texts from the entity's mentions (dedup by record)
        ent = _entities_index().get(id, {})
        texts, seen = [], set()
        for m in ent.get("mentions", []):
            prov = m.get("provenance", {})
            doc_id, rid = prov.get("doc_id"), prov.get("rid")
            key = (doc_id, rid)
            if key in seen:
                continue
            seen.add(key)
            texts.append({"doc_id": doc_id, "rid": rid, "page": prov.get("page"),
                          "pdf_page": prov.get("pdf_page"), "section": prov.get("section"),
                          "vertices": prov.get("vertices"), "min_conf": prov.get("min_conf"),
                          "surface": m.get("surface"), "date": prov.get("date"),
                          "text": _record_text(doc_id, rid)[:1200]})
        # Record nodes (letters/entries) aren't in entities.json — show their own text + from/to/date.
        record_meta = {}
        if not texts and node.get("kind") in ("letter", "notebook_entry", "notebook_page"):
            doc_id = node.get("doc_id") or id.split(":", 1)[-1]
            rid = node.get("rid")
            txt = _record_text(doc_id, rid) or node.get("text", "")
            if txt:
                texts.append({"doc_id": doc_id, "rid": rid, "page": node.get("page"),
                              "pdf_page": node.get("pdf_page"), "section": node.get("section"),
                              "date": node.get("date_raw") or node.get("edtf"), "text": txt[:2000]})
            d = _docs_index().get(doc_id)
            if d:
                tbl = d.get("text_by_label", {})
                record_meta = {"date": d.get("date") or node.get("date_raw"),
                               "recipient": d.get("recipient") or tbl.get("src_recipient"),
                               "sent_from": tbl.get("src_location_sender"),
                               "sent_to": tbl.get("src_location_recipient")}

        _rec_cite = _record_citation         # shared module-level resolver (doc/rid/page + text)

        # ROBUSTNESS: an entity with connections but no entity-mentions (e.g. a recipient reconciled from
        # a letter field) would otherwise show zero passages. Gather its records from its MENTIONED_IN
        # edges so "connections but no source passages" can't happen.
        if not texts:
            for e in _graph_indexes()["adj"].get(id, ()):
                if (e.get("kind") or "").upper() != "MENTIONED_IN":
                    continue
                rec_id = e["target"] if e.get("source") == id else (e["source"] if e.get("target") == id else None)
                if not rec_id or not str(rec_id).startswith(("letter:", "notebook_entry:", "notebook_page:")):
                    continue
                c = _record_citation(rec_id)
                if c.get("text"):
                    texts.append({"doc_id": c["doc_id"], "rid": c.get("rid"), "page": c.get("page"),
                                  "pdf_page": c.get("pdf_page"), "section": c.get("section"),
                                  "date": c.get("date"), "text": c["text"]})
                if len(texts) >= 12:
                    break

        # semantic relations (skip the structural mention/date edges). Each relation carries its OWN
        # provenance: the region rids that assert it, and — for co-occurrence (APPEARS_WITH) — the
        # actual shared records, so a reader can open the page and confirm a real co-mention vs. a
        # same-page coincidence (David's ask). 'evidence' resolves those records to openable citations.
        rels = []
        for e in _graph_indexes()["adj"].get(id, ()):
            k = (e.get("kind") or "").upper()
            if k in ("MENTIONED_IN", "DATED_IN"):
                continue
            if e.get("source") == id:
                other_id, direction = e["target"], "->"
            elif e.get("target") == id:
                other_id, direction = e["source"], "<-"
            else:
                continue
            o = byid.get(other_id, {})
            evidence = [_rec_cite(r) for r in (e.get("shared_records") or [])]
            rels.append({"rel": e.get("label"), "rel_kind": k, "dir": direction, "other_id": other_id,
                         "other": o.get("label"), "other_kind": o.get("kind"),
                         "when": e.get("valid_time"), "weight": e.get("weight"),
                         "rids": e.get("rids") or [], "method": e.get("method"),
                         # co-occurrence adjudication (verify_connections): real tie vs same-page coincidence
                         "verified": e.get("verified"), "relationship": e.get("relationship"),
                         "subtype": e.get("subtype"), "evidence": evidence})
        # sort: real/kinship ties first, coincidences last; then by strength
        _vrank = {"real": 0, None: 1, "unknown": 1, "coincidental": 2}
        rels.sort(key=lambda r: (_vrank.get(r.get("verified"), 1) if r["rel_kind"] == "APPEARS_WITH" else 0,
                                 -(r.get("weight") or 0)))
        # Biography (Crosby) passages mentioning this entity — integrated by default, relevance-filtered
        # and cached so re-opening a dossier costs nothing. Only for named entities with a real label.
        kind, label = node.get("kind"), (node.get("label") or "")
        book = _entity_book(label, id) if (kind in ("person", "place", "organization") and len(label) > 3) else []

        return JSONResponse({
            "id": id, "label": node.get("label"), "kind": node.get("kind"),
            "description": _enriched_desc(id) or node.get("description"), "role": node.get("role"),
            "what_it_is": node.get("what_it_is"),
            "relation_to_solanus": node.get("relation_to_solanus"), "location": node.get("location"),
            "lat": node.get("lat"), "lon": node.get("lon"),
            "mention_count": node.get("mention_count"),
            "source_count": len(texts),   # distinct source passages actually shown — the headline number
            "variants": ent.get("variants", []),
            "relations": rels[:60], "texts": texts, "record": record_meta, "book": book,
            # reconciled authority overlay (name_authority side-car): Wikidata QID + VIAF id + external
            # URLs, so a person/place dossier can link out to the authority record. Empty {} if unmatched.
            "authority": _authority_links(id),
            # researched place dossier (side-car): precise address, what it was + its fate, significance,
            # a synthesis of who Solanus engaged there, and scholarly sources. Empty {} for non-places.
            "place": _place_enrichment().get(id, {}),
        })

    # ---- GET /api/connection : the document(s) behind an edge + an LLM explanation of the link ----
    @api.get("/api/connection")
    def api_connection(a: str = Query(..., description="one entity node id"),
                       b: str = Query(..., description="the other entity node id"),
                       explain: bool = Query(True, description="include an LLM explanation (paid)")):
        """Why are A and B connected? Returns the actual connecting document(s) (openable scans) and,
        when asked, a grounded LLM explanation — so the user can both READ the source and get a plain-
        language account, distinct from just jumping to the other entity."""
        ckey = (frozenset((a, b)), explain)
        if ckey in _CONN_CACHE:
            return JSONResponse(_CONN_CACHE[ckey])
        gq = _graph_cache()
        byid = gq["byid"]
        na, nb = byid.get(a, {}), byid.get(b, {})
        if not na or not nb:
            raise HTTPException(status_code=404, detail="unknown entity id(s)")
        # the semantic edge(s) directly between a and b
        edges = [e for e in gq["data"].get("edges", [])
                 if {e.get("source"), e.get("target")} == {a, b}
                 and (e.get("kind") or "").upper() not in ("MENTIONED_IN", "DATED_IN")]
        rel = edges[0].get("label") if edges else "connected"
        # connecting record nodes: shared_records (co-occurrence) first; else the records that mention BOTH
        rec_ids: list = []
        for e in edges:
            rec_ids += e.get("shared_records") or []
        if not rec_ids:
            ea, eb = _entities_index().get(a, {}), _entities_index().get(b, {})
            da = {(m.get("provenance", {}).get("doc_id")) for m in ea.get("mentions", [])}
            db = {(m.get("provenance", {}).get("doc_id")) for m in eb.get("mentions", [])}
            shared_docs = [d for d in (da & db) if d]
            # map a shared doc_id to its record node (letter or notebook page) for an openable citation
            for d in shared_docs:
                for cand in (f"letter:{d}", f"notebook_page:{d}"):
                    if cand in byid:
                        rec_ids.append(cand)
                        break
        # also fall back to the edge's own region provenance (rids carry a doc via the record nodes)
        docs = []
        seen = set()
        for r in rec_ids:
            if r in seen:
                continue
            seen.add(r)
            docs.append(_record_citation(r))
            if len(docs) >= 8:
                break
        explanation = None
        if explain and docs:
            from lib.providers import llm
            passages = "\n\n".join(f"[{d.get('cite_label')}] {d.get('text','')[:600]}" for d in docs[:4])
            prompt = (
                f'In the Father Solanus Casey archive, "{na.get("label")}" and "{nb.get("label")}" are '
                f'linked ({rel}). Here are the archival passage(s) that link them:\n\n{passages}\n\n'
                f'In 1-2 sentences, explain how they are connected, grounded ONLY in these passages and '
                f'citing the page label(s). If the passages merely place both names on the same page with '
                f'no stated relationship, say that plainly (a co-mention, not a confirmed relationship).')
            try:
                txt, _ = llm.generate(prompt, json_mode=False, temperature=0.1, system=SYSTEM_PROMPT)
                explanation = (txt or "").strip()
            except Exception as e:
                explanation = None
                log.warning("connection explain failed: %s", e)
        result = {"a": a, "b": b, "a_label": na.get("label"), "b_label": nb.get("label"),
                  "relation": rel, "documents": docs, "explanation": explanation}
        _CONN_CACHE[ckey] = result
        return JSONResponse(result)

    # ---- GET /api/map : geocoded PLACE entities for the map view ----
    @api.get("/api/map")
    def api_map():
        """Place entities that carry coordinates (set by geocode_places -> build_graph). Powers the Map
        tab: where Solanus's correspondents and petitioners were, sized by how often they appear."""
        from app.tools import graph_query as gq
        if not gq._maybe_build_graph_json():
            return JSONResponse({"places": []})
        # generic single-word place names ("Hospital", "Monastery") geocode to arbitrary spots — skip
        # them so the map shows only meaningful, specific locations.
        _generic = {"hospital", "monastery", "church", "friary", "convent", "parish", "home", "city",
                    "rectory", "chapel", "shrine", "clinic", "asylum", "sanitarium", "school", "here"}
        enr = _place_enrichment()
        places = []
        for n in _graph_indexes()["nodes_by_kind"].get("place", []):
            if not (n.get("lat") and n.get("lon")):
                continue
            if n.get("label", "").strip().lower() in _generic:
                continue
            pe = enr.get(n["id"]) or {}
            precise = bool(pe.get("precise"))
            places.append({
                "id": n["id"], "name": n.get("label"),
                # building-level coords when we resolved them, else the city centroid
                "lat": pe.get("lat") if precise else n.get("lat"),
                "lon": pe.get("lon") if precise else n.get("lon"),
                "precise": precise, "address": pe.get("address"),
                # headline count = distinct source passages (what the dossier shows), not raw mentions
                "sources": (_entity_source_count(n["id"]) or n.get("mention_count") or 0),
                "mentions": n.get("mention_count"), "role": n.get("role"),
                "what_it_was": pe.get("what_it_was"), "fate": pe.get("fate"),
                "description": pe.get("summary") or n.get("description")})
        return JSONResponse({"places": places, "n": len(places)})

    # ==================================================================
    # ARCHIVE-CONTENT PAGES (additive, read-only): His Life timeline, the
    # Letters + Notebooks reading rooms, the People directory, the Casey
    # family tree, and the "Solanus and ___" relationship view. Each is a
    # thin projection of the cached graph + side-cars.
    # ==================================================================

    # ---- GET /api/life : the scrollytelling spine (milestones + per-year corpus activity) ----
    @api.get("/api/life")
    def api_life():
        """The 'His Life' story spine: authored milestones (app/content/life_milestones.json, read live) +
        per-year corpus activity (letters / notebook entries) from the graph, and a few real letters from
        each milestone's year so the narrative stays anchored to the archive's own documents."""
        import re as _re
        mpath = _APP / "content" / "life_milestones.json"
        content = json.loads(mpath.read_text()) if mpath.exists() else {"milestones": []}
        nbk = _graph_indexes()["nodes_by_kind"]
        years: dict = {}
        letters_by_year: dict = {}
        for n in nbk.get("letter", []) + nbk.get("notebook_entry", []):
            k = n.get("kind")
            if k not in ("letter", "notebook_entry"):
                continue
            m = _re.match(r"(\d{4})", n.get("edtf") or "")
            if not m:
                continue
            yr = int(m.group(1))
            d = years.setdefault(yr, {"year": yr, "letters": 0, "notebook_entries": 0})
            if k == "letter":
                d["letters"] += 1
                letters_by_year.setdefault(yr, []).append(n)
            else:
                d["notebook_entries"] += 1
        docs = _docs_index()
        out_ms = []
        for ms in content.get("milestones", []):
            yr = ms.get("year")
            sample = []
            for n in sorted(letters_by_year.get(yr, []), key=lambda x: x.get("edtf") or "")[:6]:
                rec = docs.get(n.get("doc_id"), {})
                sample.append({"id": n["id"], "recipient": rec.get("recipient") or n.get("label"),
                               "date": n.get("date_raw") or n.get("edtf"), "doc_id": n.get("doc_id")})
            row = dict(ms)
            row["activity"] = years.get(yr, {"year": yr, "letters": 0, "notebook_entries": 0})
            row["letters"] = sample
            out_ms.append(row)
        return JSONResponse({"subject": content.get("subject"), "milestones": out_ms,
                             "years": sorted(years.values(), key=lambda x: x["year"])})

    # ---- GET /api/letters : the correspondence index (570 letters) ----
    @api.get("/api/letters")
    def api_letters(q: str | None = Query(None), year: int | None = Query(None),
                    sort: str = Query("date", description="date | recipient")):
        """The Letters reading-room index: every correspondence record with recipient, date, where it was
        written from/to, and a snippet — sorted by date. Filter by free text (q) or year."""
        docs = _docs_index()
        summ = _letter_summaries()
        ql = (q or "").lower().strip()
        rows = []
        for n in _graph_indexes()["nodes_by_kind"].get("letter", []):
            rec = docs.get(n.get("doc_id"), {})
            tbl = rec.get("text_by_label", {})
            edtf = n.get("edtf") or ""
            yr = int(edtf[:4]) if edtf[:4].isdigit() else None
            if year and yr != year:
                continue
            text = n.get("text") or ""
            recipient = rec.get("recipient") or (n.get("label") or "").replace("Letter to ", "")
            s = (summ.get(n["id"]) or {}).get("summary")
            if ql and ql not in text.lower() and ql not in (recipient or "").lower() \
                    and ql not in (s or "").lower():
                continue
            rows.append({
                "id": n["id"], "doc_id": n.get("doc_id"), "recipient": recipient,
                "date": n.get("date_raw"), "edtf": edtf, "year": yr,
                "section": n.get("section"), "page": n.get("page"), "pdf_page": n.get("pdf_page"),
                "from": tbl.get("src_location_sender"), "to": tbl.get("src_location_recipient"),
                "summary": s, "snippet": text.strip()[:220],
            })
        if sort == "recipient":
            rows.sort(key=lambda r: (r["recipient"] or "").lower())
        else:
            rows.sort(key=lambda r: r["edtf"] or "zzzz")
        return JSONResponse({"letters": rows, "n": len(rows),
                             "years": sorted({r["year"] for r in rows if r["year"]})})

    # ---- GET /api/letter : one letter in full (transcription + scan + named entities + prev/next) ----
    @api.get("/api/letter")
    def api_letter(id: str = Query(..., description="letter node id, e.g. letter:Volume_1__p001")):
        """One letter: transcription (greeting/body/farewell/signature), to whom and from/to where, the
        scan (image + PDF), the entities named in it (each linkable to its dossier), and prev/next by date."""
        c = _graph_cache()
        byid = c["byid"]
        node = byid.get(id)
        if not node or node.get("kind") != "letter":
            raise HTTPException(status_code=404, detail=f"no letter {id!r}")
        doc_id = node.get("doc_id")
        rec = _docs_index().get(doc_id, {})
        tbl = rec.get("text_by_label", {})
        idx = _graph_indexes()
        ents, seen = [], set()
        for e in idx["adj"].get(id, ()):
            if (e.get("kind") or "").upper() != "MENTIONED_IN":
                continue
            ent_id = (e["source"] if e.get("target") == id else
                      (e["target"] if e.get("source") == id else None))
            if not ent_id or ent_id in seen:
                continue
            en = byid.get(ent_id, {})
            if en.get("kind") not in ("person", "place", "organization", "condition", "favor", "event"):
                continue
            seen.add(ent_id)
            ents.append({"id": ent_id, "label": en.get("label"), "kind": en.get("kind"),
                         "variants": en.get("variants") or []})
        letter_ids = idx["letter_order"]
        i = idx["letter_pos"].get(id, -1)
        return JSONResponse({
            "id": id, "doc_id": doc_id, "section": node.get("section"),
            "page": node.get("page"), "pdf_page": node.get("pdf_page"),
            "recipient": rec.get("recipient") or (node.get("label") or "").replace("Letter to ", ""),
            "date": node.get("date_raw"), "edtf": node.get("edtf"),
            "summary": (_letter_summaries().get(id) or {}).get("summary"),
            "from": tbl.get("src_location_sender"), "to": tbl.get("src_location_recipient"),
            "greeting": tbl.get("src_greeting"), "body": tbl.get("src_content") or node.get("text"),
            "farewell": tbl.get("src_farewell"), "signature": tbl.get("src_signature"),
            "commentary": tbl.get("archv_commentary"),
            "image_url": f"/api/image/{node.get('section')}/{node.get('pdf_page')}",
            "pdf_url": f"/api/pdf/{node.get('section')}?page={node.get('pdf_page')}",
            "entities": ents,
            "prev": letter_ids[i - 1] if i > 0 else None,
            "next": letter_ids[i + 1] if 0 <= i < len(letter_ids) - 1 else None,
        })

    # ---- GET /api/notebooks : the notebook volumes (selector for the reading room) ----
    @api.get("/api/notebooks")
    def api_notebooks():
        """The notebook volumes (grouped by section — the clean grouping; the OCR'd notebook titles vary
        page to page), each with page and entry counts. The picker for the notebook reading room."""
        from collections import Counter
        idx = _graph_indexes()
        nbk = idx["nodes_by_kind"]
        vols: dict = {}
        names: dict = {}
        for n in nbk.get("notebook_page", []):
            sec = n.get("section")
            vols.setdefault(sec, {"section": sec, "pages": 0, "entries": 0})
            vols[sec]["pages"] += 1
            names.setdefault(sec, Counter())[n.get("notebook") or ""] += 1
        page_section = idx["section_by_doc"]
        for n in nbk.get("notebook_entry", []):
            sec = page_section.get(n.get("doc_id"))
            if sec in vols:
                vols[sec]["entries"] += 1
        for sec, d in vols.items():
            top = names.get(sec, Counter()).most_common(1)
            d["title"] = (top[0][0] if top and top[0][0] else sec).title().replace("Volume", "Volume ").strip()
        return JSONResponse({"notebooks": sorted(vols.values(), key=lambda x: x["section"] or "")})

    # ---- GET /api/notebook_entries : the petition entries, paginated ----
    @api.get("/api/notebook_entries")
    def api_notebook_entries(q: str | None = Query(None), section: str | None = Query(None),
                             year: int | None = Query(None),
                             offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
        """The notebook petition entries (~8,773), paginated. Each carries its text, date, notebook + page
        label, and the scan of the page it sits on. Filter by free text, volume (section), or year."""
        idx = _graph_indexes()
        byid = _graph_cache()["byid"]
        page_section = idx["section_by_doc"]
        ql = (q or "").lower().strip()
        matched = []
        for n in idx["nodes_by_kind"].get("notebook_entry", []):
            if section and page_section.get(n.get("doc_id")) != section:
                continue
            edtf = n.get("edtf") or ""
            yr = int(edtf[:4]) if edtf[:4].isdigit() else None
            if year and yr != year:
                continue
            if ql and ql not in (n.get("text") or "").lower():
                continue
            matched.append(n)
        matched.sort(key=lambda x: (x.get("edtf") or "zzzz", x.get("doc_id") or ""))
        total = len(matched)
        themes = _notebook_page_themes()
        out = []
        for n in matched[offset:offset + limit]:
            page = byid.get(f"notebook_page:{n.get('doc_id')}", {})
            th = themes.get(n.get("doc_id")) or {}
            out.append({
                "id": n["id"], "doc_id": n.get("doc_id"), "notebook": n.get("notebook"),
                "page_label": n.get("page_label"), "date": n.get("date_raw"), "edtf": n.get("edtf"),
                "text": n.get("text"), "section": page.get("section"), "pdf_page": page.get("pdf_page"),
                "page_theme": th.get("theme"), "page_gloss": th.get("gloss"),
                "image_url": (f"/api/image/{page.get('section')}/{page.get('pdf_page')}"
                              if page.get("pdf_page") else None),
            })
        return JSONResponse({"entries": out, "total": total, "offset": offset, "limit": limit})

    # ---- GET /api/people : the people directory (ranked, filterable, paginated) ----
    @api.get("/api/people")
    def api_people(q: str | None = Query(None), role: str | None = Query(None),
                   has_authority: bool = Query(False), letters_only: bool = Query(False),
                   sort: str = Query("prominence", description="prominence | name | letters"),
                   offset: int = Query(0, ge=0), limit: int = Query(60, ge=1, le=200)):
        """The People directory: every person, ranked by prominence (default), searchable by name and
        filterable by role / has-authority / received-a-letter. Paginated; each row links to the dossier."""
        from collections import Counter
        rows = _people_index()
        ql = (q or "").lower().strip()
        rl = (role or "").lower().strip()

        def keep(r):
            if ql and ql not in (r["name"] or "").lower() and \
                    not any(ql in (v or "").lower() for v in r["variants"]):
                return False
            if rl and rl not in (r["role"] or "").lower():
                return False
            if has_authority and not (r.get("qid") or r.get("viaf")):
                return False
            if letters_only and not r.get("letters"):
                return False
            return True

        filt = [r for r in rows if keep(r)]
        if sort == "name":
            filt = sorted(filt, key=lambda r: (r["name"] or "").lower())
        elif sort == "letters":
            filt = sorted(filt, key=lambda r: r.get("letters", 0), reverse=True)
        total = len(filt)
        page = []
        for r in filt[offset:offset + limit]:
            r = dict(r)
            if r.get("qid"):
                r["wikidata_url"] = f"https://www.wikidata.org/wiki/{r['qid']}"
            if r.get("viaf"):
                r["viaf_url"] = f"https://viaf.org/viaf/{r['viaf']}"
            page.append(r)
        roles = Counter((r["role"] or "—").lower() for r in rows)
        return JSONResponse({"people": page, "total": total, "offset": offset, "limit": limit,
                             "n_people": len(rows),
                             "roles": [{"role": k, "n": v} for k, v in roles.most_common(24)]})

    # ---- GET /api/family : the Casey family roster as a linkable tree ----
    @api.get("/api/family")
    def api_family():
        """The Casey family roster (solanus_family.json) as a linkable tree: parents, siblings (Solanus
        flagged), in-laws, nieces/nephews — each resolved where possible to a graph entity id so a card
        opens the dossier + that person's letters."""
        import re as _re
        fam = _solanus_family()
        _titles = _re.compile(r"\b(mr|mrs|miss|ms|rev|fr|father|sr|sister|br|brother|mother|very|rt|"
                              r"reverend|monsignor|mgr|msgr|dr|st|saint|o\.?f\.?m|cap|s\.?j)\b\.?", _re.I)

        def _norm(s):
            s = _titles.sub(" ", (s or "").lower())
            s = _re.sub(r"[^a-z\s]", " ", s)
            return _re.sub(r"\s+", " ", s).strip()

        name2id: dict = {}          # normalized full name -> person node id (first, highest-degree seen)
        for n in _graph_indexes()["nodes_by_kind"].get("person", []):
            for nm in [n.get("label"), n.get("canonical_name"), *(n.get("variants") or [])]:
                k = _norm(nm)
                if k and len(k) >= 5:   # need a real name (surname present), not "mary" alone
                    name2id.setdefault(k, n["id"])

        def resolve(mem):
            for cand in [mem.get("canonical_name"), mem.get("name"), *(mem.get("aliases") or [])]:
                k = _norm(cand)
                if k and k in name2id:
                    return name2id[k]
            return None

        members = []
        for mem in fam.get("members", []):
            members.append({
                "name": mem.get("name"), "canonical_name": mem.get("canonical_name"),
                "relation": mem.get("relation"), "aliases": mem.get("aliases") or [],
                "birth": mem.get("birth"), "death": mem.get("death"),
                "in_corpus": mem.get("in_corpus"), "n_letters": mem.get("n_letters"),
                "notes": mem.get("notes"), "became_religious": mem.get("became_religious"),
                "entity_id": resolve(mem),
            })
        return JSONResponse({"subject": fam.get("subject"), "note": fam.get("note"),
                             "solanus_id": SOLANUS_ID, "members": members})

    # ---- GET /api/relationship : "Solanus and ___" — the correspondence + optional micro-narrative ----
    @api.get("/api/relationship")
    def api_relationship(person: str = Query(..., description="person node id")):
        """'Solanus and ___': the records that tie Fr. Solanus to one person — the letters he addressed to
        them and the documents that name them — sorted into a year arc, plus (when generated) the LLM
        micro-narrative from the relationship_narratives side-car. Facts are live; the narrative overlays."""
        c = _graph_cache()
        byid = c["byid"]
        pn = byid.get(person)
        if not pn or pn.get("kind") != "person":
            raise HTTPException(status_code=404, detail=f"no person {person!r}")
        incident = _graph_indexes()["adj"].get(person, ())
        direct, n_letters = set(), 0
        for e in incident:
            k = (e.get("kind") or "").upper()
            if k == "WROTE_TO" and e.get("source") == SOLANUS_ID and e.get("target") == person:
                n_letters += 1
            if k in ("MENTIONED_IN", "DATED_IN"):
                continue
            if {e.get("source"), e.get("target")} == {SOLANUS_ID, person}:
                direct.add(e.get("label") or k.lower())
        recs, seen = [], set()
        for e in incident:
            if (e.get("kind") or "").upper() != "MENTIONED_IN":
                continue
            rid = (e["target"] if e.get("source") == person else
                   (e["source"] if e.get("target") == person else None))
            if not rid or rid in seen or not str(rid).startswith(("letter:", "notebook_entry:")):
                continue
            seen.add(rid)
            rn = byid.get(rid, {})
            recs.append({"id": rid, "kind": rn.get("kind"), "doc_id": rn.get("doc_id"),
                         "date": rn.get("date_raw"), "edtf": rn.get("edtf") or "",
                         "label": rn.get("label"), "snippet": (rn.get("text") or "")[:240]})
        recs.sort(key=lambda r: r["edtf"] or "zzzz")
        arc: dict = {}
        for r in recs:
            y = r["edtf"][:4]
            if y.isdigit():
                arc[y] = arc.get(y, 0) + 1
        # honest counts over ALL tying records (not the WROTE_TO summary edge, which is one-per-recipient)
        n_in_letters = sum(1 for r in recs if r["kind"] == "letter")
        n_in_notebooks = sum(1 for r in recs if r["kind"] == "notebook_entry")
        narr = _relationship_narratives().get(person, {})
        return JSONResponse({
            "person": person, "label": pn.get("label"),
            "relation_to_solanus": pn.get("relation_to_solanus"),
            "direct_relations": sorted(direct), "n_letters": n_letters,
            "letters": n_in_letters, "petitions": n_in_notebooks,
            "records": recs[:60], "n_records": len(recs),
            "arc": [{"year": int(y), "n": n} for y, n in sorted(arc.items())],
            "narrative": narr or None,
        })

    # ---- GET /api/his_life : the archive-derived narrative of Solanus's life (editable markdown) ----
    @api.get("/api/his_life")
    def api_his_life():
        p = _APP / "content" / "his_life.md"
        return JSONResponse({"markdown": p.read_text() if p.exists() else ""})

    # ---- GET /api/walkthrough : editable step-by-step content (read LIVE so edits show w/o restart) --
    @api.get("/api/walkthrough")
    def api_walkthrough():
        """Serve app/content/walkthrough.json verbatim — David edits the file, the tab updates."""
        p = _APP / "content" / "walkthrough.json"
        if not p.exists():
            return JSONResponse({"title": "Walkthrough", "steps": [], "todo": [],
                                 "intro": "app/content/walkthrough.json not found."})
        try:
            return JSONResponse(json.loads(p.read_text()))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"walkthrough.json invalid: {e}")

    # ---- GET /api/explain : the thorough explanations (rendered from the docs/ markdown) ----
    @api.get("/api/explain")
    def api_explain():
        """Return the long-form explanation docs (docs/*.md) as {name, title, markdown} for the
        Explanations tab — so the full architecture/retrieval/graph write-ups live in-app."""
        docs_dir = config.STEP7 / "docs"
        wanted = ["architecture", "retrieval", "ner_and_resolution", "graph_and_dates",
                  "dev_tool", "ENRICHMENT_AND_DEDUP_PLAN"]
        out = []
        for name in wanted:
            f = docs_dir / f"{name}.md"
            if f.exists():
                md = f.read_text()
                title = next((ln.lstrip("# ").strip() for ln in md.splitlines() if ln.startswith("#")), name)
                out.append({"name": name, "title": title, "markdown": md})
        return JSONResponse({"docs": out})

    # ---- GET /api/search_pdf : direct full-text page search ------------
    @api.get("/api/search_pdf")
    def api_search_pdf(q: str = Query(..., description="FTS5 query string"),
                       k: int = Query(10, ge=1, le=50)):
        """Full-text (FTS5) search over page OCR text → pages + source PDF + snippet (free, local)."""
        return JSONResponse(tools.call("pdf_fulltext_search", {"query": q, "k": k}).get("result", {}))

    # ---- GET /api/source : region info for the citation modal ----------
    @api.get("/api/source")
    def api_source(doc_id: str = Query(..., description="record id, e.g. Volume_3__p079"),
                   rid: str | None = Query(None, description="optional region id")):
        """Resolve a citation to its region: text + gold polygon + OCR confidence + page image/PDF."""
        return JSONResponse(source_region(doc_id, rid))

    # ---- GET /api/cost : the running ledger ----------------------------
    @api.get("/api/cost")
    def api_cost():
        """Return lib.costlog.summary() — the running $ ledger across every model call to date."""
        return JSONResponse({"summary": costlog.summary(), "csv": str(costlog.CSV_PATH)})

    # ---- GET /api/tools : the registry manifest (toggles + declarations) -
    @api.get("/api/tools")
    def api_tools():
        """The tool registry: UI toggle data + the LLM function declarations + the cost ledger."""
        manifest = tools.registry_manifest()
        manifest["observability"] = OTEL_STATUS
        manifest["models"] = {"llms": list(config.LLMS), "embeddings": config.EMBEDDING_MATRIX,
                              "rerankers": list(config.RERANKERS), "defaults": config.DEFAULTS}
        return JSONResponse(manifest)

    # ==================================================================
    # FRONT-END COMPATIBILITY ROUTES
    # ------------------------------------------------------------------
    # The shipped UI (app/static/app.js) speaks a slightly different /api dialect than the task's
    # canonical names above — it's modeled on the single-shot, toggle-based RetrievalConfig (its left
    # rail IS a RetrievalConfig). We add those routes here so the page works out of the box, each one a
    # thin translation onto the SAME backend the canonical endpoints use. (Canonical routes are
    # declared first, so nothing below shadows /api/query, /api/source, etc.)
    # ==================================================================

    # ---- GET /api/health : is the backend awake? -----------------------
    @api.get("/api/health")
    def api_health():
        """Liveness check the UI pings on boot before building itself from /api/config."""
        return JSONResponse({"ok": True})

    # ---- GET /api/config : describe the swappable axes + technique toggles
    @api.get("/api/config")
    def api_config():
        """Self-describing config the UI builds its rail from: LLMs, built embedding spaces, rerankers,
        defaults, and the retrieval-technique toggles (the RetrievalConfig knobs)."""
        import os as _os
        from lib import vectorstore
        spaces = vectorstore.spaces()                  # only spaces with a built partition are real
        default_emb = f"{config.DEFAULTS['embedding'][0]}@{config.DEFAULTS['embedding'][1]}"
        # provider of each selectable model + which providers actually have a key (drives the UI's
        # availability marking + provider-match ordering of embeddings/rerankers by chosen LLM).
        _prov_env = {"gemini": "GEMINI_API_KEY", "openai": "OPENAI_API_KEY", "voyage": "VOYAGE_API_KEY",
                     "anthropic": "ANTHROPIC_API_KEY", "cohere": "COHERE_API_KEY", "local": None}
        availability = {p: (e is None or bool(_os.environ.get(e))) for p, e in _prov_env.items()}
        emb_prov = lambda sp: config.EMBEDDINGS.get(sp.split("@")[0], {}).get("provider", "local")
        providers = {
            "llms": {m: d.get("provider") for m, d in config.LLMS.items()},
            "embeddings": {sp: emb_prov(sp) for sp in (spaces or [default_emb])},
            "rerankers": {r: d.get("provider") for r, d in config.RERANKERS.items()},
        }
        return JSONResponse({
            "llms":       list(config.LLMS),
            "embeddings": spaces or [default_emb],
            "rerankers":  list(config.RERANKERS),
            "providers":  providers,
            "availability": availability,
            "embed_compat": getattr(config, "EMBED_COMPAT", {}),   # chat-LLM provider -> allowed embedding providers
            "defaults":   {"llm": config.DEFAULTS["llm"], "embedding": default_emb,
                           "reranker": config.DEFAULTS["reranker"]},
            # retrieval techniques as toggles (RESEARCH_PLAN part D); `paid` flags the ones that spend.
            "tools": [
                {"key": "use_bm25",        "label": "BM25 (lexical)",          "default": True,  "paid": False,
                 "hint": "sparse keyword search — exact names/dates"},
                {"key": "use_dense",       "label": "Dense (semantic)",        "default": True,  "paid": False,
                 "hint": "embedding similarity — paraphrase/meaning"},
                {"key": "use_rerank",      "label": "Rerank",                  "default": False, "paid": True,
                 "hint": "cross-encoder re-order of the top hits"},
                {"key": "use_hyde",        "label": "HyDE",                    "default": False, "paid": True,
                 "hint": "embed a hypothetical answer for better recall"},
                {"key": "use_multiquery",  "label": "Multi-query (RAG-Fusion)", "default": False, "paid": True,
                 "hint": "retrieve for several phrasings, fuse"},
                {"key": "use_llm_router",  "label": "LLM router",              "default": False, "paid": True,
                 "hint": "let an LLM classify query complexity"},
                {"key": "auto_apply_route", "label": "Auto-escalate route",    "default": False, "paid": True,
                 "hint": "let the router turn on paid transforms for complex asks"},
            ],
            "observability": OTEL_STATUS,
        })

    # ---- POST /api/ask : single-shot toggle RAG (AskRequest is module-level — see note there) ----
    @api.post("/api/ask")
    def api_ask(req: AskRequest):
        """Single-shot toggle-driven retrieval + cited synthesis (the front-end's primary endpoint).

        ⚠️ MAKES A PAID LLM CALL for the answer synthesis (unless the retrieval abstains). The retrieval
        itself is free with BM25 + local dense; paid toggles (rerank/HyDE/multi-query/router) spend
        only when switched on. Everything is cost-logged.
        """
        try:
            return JSONResponse(single_shot_rag(
                query=req.query, llm_model=req.llm, embedding=req.embedding,
                reranker=req.reranker, kinds=req.kinds, toggles=req.toggles,
                system_prompt=req.system_prompt, history=req.history, styled=req.styled))
        except Exception as e:
            log.exception("single_shot_rag failed")
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    # ---- POST /api/ask_stream : same as /api/ask but STREAMS the answer (SSE) ----
    @api.post("/api/ask_stream")
    def api_ask_stream(req: AskRequest):
        """Streaming sibling of /api/ask. Retrieval runs first (citations known up front), then the cited
        answer streams token-by-token over Server-Sent Events: a `meta` event (citations+grade+trace), then
        `token` events, then `done` (full answer). Same grounding/persona/family-expansion as /api/ask."""
        def _sse(obj):
            return "data: " + json.dumps(obj, ensure_ascii=False) + "\n\n"

        def gen():
            try:
                llm_model = req.llm or config.DEFAULTS["llm"]
                cfg = _cfg_from_request(req.embedding, req.reranker, req.toggles)
                # LIVE PIPELINE PROGRESS: emit `stage` events at each real milestone so the popup shows
                # where the query actually IS right now (retrieving → synthesizing → answering), not a guess.
                retr_nodes = ["question"]
                if getattr(cfg, "use_bm25", True): retr_nodes.append("bm25")
                if getattr(cfg, "use_dense", True): retr_nodes += ["dense", "embed"]
                if getattr(cfg, "use_hyde", False): retr_nodes.append("hyde")
                if getattr(cfg, "use_multiquery", False): retr_nodes.append("multiquery")
                if getattr(cfg, "use_rerank", False): retr_nodes += ["rerank", "reranker_model"]
                if getattr(cfg, "use_llm_router", False): retr_nodes.append("router")
                # 1) retrieval is starting now → light the retrieval cluster "running"
                yield _sse({"type": "stage", "active": ["question"], "running": [n for n in retr_nodes if n != "question"]})
                retr_query, _ = _expand_family_query(req.query)
                result = retrieval.route_and_retrieve(retr_query, cfg, kinds=req.kinds)
                hits = _dedup_hits(result.get("hits", []))
                citations = _citations_from_hits(hits)
                grade = result.get("grade", {})
                trace = {"route": result.get("route", {}),
                         "queries_used": result.get("queries_used", [req.query]),
                         "steps": [{"tool": "route_and_retrieve", "items": len(hits)},
                                   {"tool": "synthesize (stream)", "items": len(citations)}], "cost": {}}
                # 2) retrieval done → those nodes go "active"; synthesis is now "running"
                yield _sse({"type": "stage", "active": retr_nodes, "running": ["synthesize", "llm"]})
                yield _sse({"type": "meta", "citations": citations, "grade": grade, "trace": trace})
                system, prompt, temp = _synthesis_inputs(req.query, hits, grade, req.system_prompt,
                                                         history=req.history)
                if system is None:
                    yield _sse({"type": "stage", "active": retr_nodes + ["synthesize", "llm", "answer"], "running": []})
                    yield _sse({"type": "token", "text": prompt})
                    yield _sse({"type": "done", "answer": prompt})
                    return
                acc = []
                for chunk in llm.generate_stream(prompt, model=llm_model, system=system, temperature=temp):
                    if not acc:   # first token → the answer is now being written
                        yield _sse({"type": "stage", "active": retr_nodes + ["synthesize", "llm"], "running": ["answer"]})
                    acc.append(chunk)
                    yield _sse({"type": "token", "text": chunk})
                plain = "".join(acc).strip()
                # 3) done → the whole fired path settles "active"
                yield _sse({"type": "stage", "active": retr_nodes + ["synthesize", "llm", "answer"], "running": []})
                # VOICE LAYER: the plain reply streamed live above; if styling is on, restyle it into
                # Solanus's voice (a post-step — can't stream) and swap it in as the final answer. `done`
                # carries the styled text as `answer` (what the user keeps + what feeds back as history)
                # plus the plain backstop, so the rich re-render + chat memory match the non-stream path.
                if getattr(req, "styled", False) and plain:
                    yield _sse({"type": "stage", "active": retr_nodes + ["synthesize", "llm", "answer"],
                                "running": ["restyle"]})
                    voice = _to_solanus_voice(plain)
                    _final = (voice.get("final") or "").strip() or plain    # never blank the answer
                    yield _sse({"type": "done", "answer": _final, "answer_plain": plain,
                                "styled": True, "voice_note": _VOICE_NOTE,
                                "voice_consistent": voice.get("consistent")})
                else:
                    yield _sse({"type": "done", "answer": plain})
            except Exception as e:
                log.exception("ask_stream failed")
                yield _sse({"type": "error", "detail": f"{type(e).__name__}: {e}"})
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # ---- GET /api/personas : the chat persona presets (default / archivist / Solanus) ----
    @api.get("/api/personas")
    def api_personas():
        """The selectable chat personas + their editable system prompts (drives the Ask persona picker)."""
        return JSONResponse({"personas": [{"key": k, "label": v["label"], "prompt": v["prompt"]}
                                          for k, v in PERSONAS.items()]})

    # ---- GET /api/agent_graph : the pipeline topology for the flow-node view ----
    @api.get("/api/agent_graph")
    def api_agent_graph():
        """Static topology of the retrieval/agent pipeline, for the Drawflow flow view. The FRONT END lights
        each node active/inactive/running per query from the trace + toggles. Our authored analog of
        LangGraph's get_graph() — the custom loop already emits the runtime path (tracer.records), which is
        what we actually visualize."""
        C = 210  # column width
        nodes = [
            {"id": "question",   "label": "Question",          "kind": "io",        "x": 20,       "y": 200},
            {"id": "router",     "label": "Router",            "kind": "control",   "x": 20 + C,   "y": 200},
            {"id": "bm25",       "label": "BM25 · lexical",    "kind": "retrieval", "x": 20 + 2*C, "y": 60},
            {"id": "dense",      "label": "Dense · semantic",  "kind": "retrieval", "x": 20 + 2*C, "y": 160},
            {"id": "multiquery", "label": "Multi-query",       "kind": "retrieval", "x": 20 + 2*C, "y": 260},
            {"id": "hyde",       "label": "HyDE",              "kind": "retrieval", "x": 20 + 2*C, "y": 360},
            {"id": "rerank",     "label": "Rerank",            "kind": "rerank",    "x": 20 + 3*C, "y": 200},
            {"id": "synthesize", "label": "Synthesize · cited", "kind": "llm",      "x": 20 + 4*C, "y": 200},
            {"id": "answer",     "label": "Answer",            "kind": "io",        "x": 20 + 5*C, "y": 200},
            {"id": "tts",        "label": "Voice · TTS",       "kind": "output",    "x": 20 + 6*C, "y": 130},
            {"id": "avatar",     "label": "Avatar",            "kind": "output",    "x": 20 + 6*C, "y": 270},
            # model axes (feed the stages that consume them)
            {"id": "llm",        "label": "LLM model",         "kind": "model",     "x": 20 + 4*C, "y": 360},
            {"id": "embed",      "label": "Embedding model",   "kind": "model",     "x": 20 + 2*C, "y": 470},
            {"id": "reranker_model", "label": "Reranker model", "kind": "model",    "x": 20 + 3*C, "y": 360},
        ]
        edges = [("question", "router"),
                 ("router", "bm25"), ("router", "dense"), ("router", "multiquery"), ("router", "hyde"),
                 ("bm25", "rerank"), ("dense", "rerank"), ("multiquery", "rerank"), ("hyde", "rerank"),
                 ("rerank", "synthesize"), ("synthesize", "answer"), ("answer", "tts"), ("tts", "avatar"),
                 ("embed", "dense"), ("reranker_model", "rerank"), ("llm", "synthesize")]
        return JSONResponse({"nodes": nodes, "edges": [{"from": a, "to": b} for a, b in edges]})

    # ---- GET /api/voices : which TTS/STT providers (local|azure|gcp) are available + configured ----
    @api.get("/api/voices")
    def api_voices():
        """Lets the site's voice picker show local / Azure / Google and whether each has credentials."""
        from app import voice as _voice
        return JSONResponse(_voice.providers())

    # ---- GET /api/voice_catalog : curated Azure voices (+ active default/prosody) for the picker ----
    @api.get("/api/voice_catalog")
    def api_voice_catalog():
        """Powers the Settings voice picker; any custom Azure voice name can also be typed in."""
        from app import voice as _voice
        return JSONResponse(_voice.azure_voice_catalog())

    # ---- POST /api/tts : server text-to-speech, provider-selectable (local Piper | Azure | GCP) ----
    @api.post("/api/tts")
    def api_tts(req: TtsRequest):
        """Synthesize speech server-side and return audio/wav. provider = local|azure|gcp (the clone becomes
        another provider in app.voice later). The site picker sends the chosen provider/voice."""
        from app import voice as _voice
        from fastapi.responses import Response
        text = _strip_citations((req.text or "").strip())
        if not text:
            raise HTTPException(status_code=400, detail="empty text")
        try:
            wav = _voice.synthesize(text[:4000], provider=req.provider, voice=req.voice)
        except Exception as e:
            log.exception("tts failed")
            raise HTTPException(status_code=500, detail=f"tts_failed: {type(e).__name__}: {e}")
        return Response(content=wav, media_type="audio/wav")

    # ---- POST /api/tts_visemes : Azure TTS + viseme timings (JSON) for in-browser avatar lip-sync ----
    @api.post("/api/tts_visemes")
    def api_tts_visemes(req: TtsRequest):
        """Synthesize via the Azure Speech SDK and return {audio: base64 wav, visemes: [{t,id}]} so the
        avatar lip-syncs to the REAL Solanus voice. Azure-only; the client falls back to /api/tts on error."""
        from app import voice as _voice
        import base64 as _b64
        text = _strip_citations((req.text or "").strip())
        if not text:
            raise HTTPException(status_code=400, detail="empty text")
        try:
            wav, visemes = _voice.synthesize_with_visemes(text[:4000], voice=req.voice)
        except Exception as e:
            log.exception("tts_visemes failed")
            raise HTTPException(status_code=500, detail=f"visemes_failed: {type(e).__name__}: {e}")
        return {"audio": _b64.b64encode(wav).decode("ascii"), "visemes": visemes}

    # ---- POST /api/stt : server speech-to-text, provider-selectable (?provider=local|azure|gcp) ----
    @api.post("/api/stt")
    async def api_stt(request: Request):
        """Transcribe a recorded audio clip (wav/webm/ogg) posted as the raw body. provider via ?provider=."""
        from app import voice as _voice
        audio = await request.body()
        if not audio:
            raise HTTPException(status_code=400, detail="empty audio body")
        try:
            text = _voice.transcribe(audio, provider=request.query_params.get("provider"))
        except Exception as e:
            log.exception("stt failed")
            raise HTTPException(status_code=500, detail=f"stt_failed: {type(e).__name__}: {e}")
        return JSONResponse({"text": text})

    # ---- GET /api/translate : is the Solanus STYLE translator wired? (UI gating; never leaks the key) ----
    @api.get("/api/translate")
    def api_translate_status():
        """Capability check for the reverse-desanitizer endpoint. configured=False until the Azure
        endpoint env vars exist, so the front-end can hide/disable the feature until then."""
        from app import translate as _translate
        return JSONResponse(_translate.status())

    # ---- POST /api/translate : rewrite plain modern English in Father Solanus Casey's voice ----
    @api.post("/api/translate")
    def api_translate(req: TranslateRequest):
        """Calls the Azure-hosted translator LoRA (Qwen2.5-7B + adapters/translator-qwen_fast). GATED:
        returns 503 (no-op) until AZURE_TRANSLATOR_ENDPOINT + AZURE_TRANSLATOR_KEY are set. Input is plain
        modern English; output is the SAME meaning in his gentle, humble voice (no facts added/changed)."""
        from app import translate as _translate
        text = (req.text or "").strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty text")
        if not _translate.configured():
            raise HTTPException(status_code=503, detail="translator_unconfigured: deploy the Azure endpoint "
                                "and set AZURE_TRANSLATOR_ENDPOINT + AZURE_TRANSLATOR_KEY (see app/translate.py)")
        try:
            styled = _translate.translate(text[:8000], max_tokens=req.max_tokens, temperature=req.temperature)
        except Exception as e:
            log.exception("translate failed")
            raise HTTPException(status_code=502, detail=f"translate_failed: {type(e).__name__}: {e}")
        return JSONResponse({"input": text, "styled": styled,
                             "direction": "modern_english -> solanus_voice"})

    # ---- GET /api/region : citation modal payload (deep-zoom hooks) ----
    @api.get("/api/region")
    def api_region(doc_id: str = Query(...), rid: str | None = Query(None),
                   page: int | None = Query(None)):
        """Region info for the OpenSeadragon modal — text + polygon + image/pdf/manifest URLs."""
        return JSONResponse(region_payload(doc_id, rid, page=page))

    # ---- GET /api/image/<section>/<pdf_page> : the page scan for deep-zoom -----
    @api.get("/api/image/{section}/{pdf_page}")
    def api_image(section: str, pdf_page: int):
        """Serve the page scan OpenSeadragon tiles. Prefer the small transport WebP (~20x lighter,
        built by stages/build_web_pages.py); fall back to the OCR-grade masked.png where it's absent.
        region_payload rescales the polygon vertices to whichever image is served, so the highlight
        lands either way."""
        web = _web_page(section, pdf_page)
        if web is not None:
            return FileResponse(str(web[0]), media_type="image/webp")
        png = _page_file(section, pdf_page, ".masked.png")
        if png is None:
            raise HTTPException(status_code=404, detail=f"page image not found: {section}/{pdf_page}")
        return FileResponse(str(png), media_type="image/png")

    # ---- GET /api/pdf/<section>?page= : the page (searchable) PDF ------
    @api.get("/api/pdf/{section}")
    def api_pdf(section: str, page: int = Query(...)):
        """Serve a page's source PDF (prefer the searchable text-layer copy if it was generated)."""
        # prefer the searchable PDF (index_sources output) over the raw masked page PDF when present.
        searchable = config.SEARCHABLE_PDFS / section / f"page_{page:03d}.searchable.pdf"
        pdf = searchable if searchable.exists() else _page_file(section, page, ".masked.pdf")
        if not pdf or not Path(pdf).exists():
            raise HTTPException(status_code=404, detail=f"page PDF not found: {section}/{page}")
        return FileResponse(str(pdf), media_type="application/pdf")

    # ---- GET /api/manifest/<section> : the IIIF manifest (if built) ----
    @api.get("/api/manifest/{section}")
    def api_manifest(section: str):
        """Serve the IIIF v3 manifest for a volume (from index_sources), if it's been generated."""
        manifest = config.SEARCHABLE_PDFS / "iiif" / section / "manifest.json"
        if not manifest.exists():
            raise HTTPException(status_code=404,
                                detail=f"IIIF manifest not built for {section} — run stages/index_sources.py")
        return JSONResponse(json.loads(manifest.read_text()))

    # ---- static front-end ----------------------------------------------
    # Mount app/static at /static (theme.css + the SPA assets). Also serve index.html at / if present,
    # so visiting the root loads the UI. We mount LAST so the /api routes above always win.
    # SOLANUS_STATIC_DIR lets a checkpoint instance serve a frozen snapshot of the front-end (same
    # backend, different /static) on its own port — used by the overnight checkpoint runner.
    static_dir = Path(os.environ["SOLANUS_STATIC_DIR"]) if os.environ.get("SOLANUS_STATIC_DIR") else (_APP / "static")
    if static_dir.is_dir():
        api.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @api.get("/")
        def index():
            """Serve the SPA entry point (static/index.html) if it exists, else a friendly stub."""
            idx = static_dir / "index.html"
            if idx.exists():
                return FileResponse(str(idx))
            return JSONResponse({"app": "Solanus Casey Archival Dev Tool",
                                 "note": "static/index.html not built yet; the API is live under /api",
                                 "endpoints": ["/api/query", "/api/graph", "/api/search_pdf",
                                               "/api/source", "/api/cost", "/api/tools"]})

    # NOTE: a "/sources" StaticFiles mount over config.REPO (the repo ROOT) was removed here — it served
    # the whole tree read-only, including pipeline_v3/step_7/.env (live API keys), .git/, and the 3 GB
    # data/. The frontend never used it (citations go through /api/image, /api/pdf, /api/manifest), so it
    # was pure attack surface. Do NOT reintroduce a repo-root mount; serve only what a route needs.

    return api


class _MissingDepsApp:
    """Stand-in 'app' used when FastAPI isn't installed, so `uvicorn app.server:app` fails CLEARLY.

    ASGI apps are callables; this one responds to any request with a 500 whose body says exactly what
    to install. It means a missing dependency yields an actionable message at request time rather than
    an opaque ImportError at import time (and `import app.server` still works for unit tests).
    """

    def __init__(self, reason: str):
        self.reason = reason

    async def __call__(self, scope, receive, send):
        msg = (f"FastAPI/uvicorn not installed ({self.reason}). Install the web deps into the step_7 "
               f"venv:\n    {config.STEP7}/venv/bin/pip install 'fastapi>=0.110' 'uvicorn[standard]>=0.29'")
        if scope["type"] != "http":
            return
        await send({"type": "http.response.start", "status": 500,
                    "headers": [(b"content-type", b"text/plain; charset=utf-8")]})
        await send({"type": "http.response.body", "body": msg.encode("utf-8")})


# Build `app` at import time iff FastAPI is present; otherwise expose the actionable stub. Either way
# `app.server:app` resolves for uvicorn.
try:
    import fastapi  # noqa: F401  — presence probe only
    app = create_app()
    log.info("FastAPI app ready (observability: %s)", OTEL_STATUS)
except ImportError as _e:
    app = _MissingDepsApp(str(_e))
    log.warning("FastAPI not installed — serving the missing-deps stub. %s", _e)


# ==================================================================
# CLI — a FREE, offline self-check (does NOT start the server, no paid calls)
# ==================================================================
# `python app/server.py` prints what's wired: the registered tools, the model variables, the
# observability status, and whether the web deps are present. It deliberately does NOT call the LLM
# (that would cost money) and does NOT start uvicorn (per the build rules) — it's a wiring sanity check.
def _selfcheck() -> dict:
    """Return a JSON-able snapshot of the server wiring without making any paid call or starting it."""
    return {
        "tools":          [t.name for t in tools.all_tools()],
        "tools_default_on": [t.name for t in tools.all_tools() if t.default_on],
        "llms":           list(config.LLMS),
        "embedding_matrix": config.EMBEDDING_MATRIX,
        "rerankers":      list(config.RERANKERS),
        "defaults":       config.DEFAULTS,
        "observability":  OTEL_STATUS,
        "fastapi_ready":  not isinstance(app, _MissingDepsApp),
        "cost_to_date":   costlog.summary(),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    print("=" * 60)
    print("app/server.py SELF-CHECK (no server started, no paid call)")
    print("=" * 60)
    print(json.dumps(_selfcheck(), indent=2, default=str))
    print("=" * 60)
    print("To run the API (after `pip install fastapi 'uvicorn[standard]'`):")
    print(f"  {config.STEP7}/venv/bin/uvicorn app.server:app --reload --port 8000")
