"""lib/providers/llm.py — model-agnostic generation adapter (the LLM axis).

generate(prompt, model=None, system=None, json_mode=False) -> (text, usage). Routes by
config.LLMS[model].provider; Gemini wired now (Claude/OpenAI swappable later — on GCP both are in
Model Garden). Every call is cost-logged with real prompt/candidate token counts.
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

from tenacity import retry, retry_if_exception, wait_exponential, stop_after_attempt

STEP7 = Path(__file__).resolve().parents[2]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config            # noqa: E402
from lib import costlog  # noqa: E402

try:
    from dotenv import load_dotenv
    load_dotenv(config.STEP7 / ".env")
    load_dotenv(config.REPO / "pipeline_v3" / "step_4" / ".env")
except Exception:
    pass


# Rate limits (429) and transient 5xx are normal under bulk load; retry with backoff so a long
# extraction self-throttles to the provider's pace rather than dying partway.
def _is_transient(exc: Exception) -> bool:
    s = str(exc)
    # include client-side timeouts/deadlines: with an HTTP timeout set (see _client), a stalled call
    # RAISES instead of hanging the whole thread pool forever — and we want those retried, not fatal.
    return any(tok in s for tok in ("429", "RESOURCE_EXHAUSTED", "503", "UNAVAILABLE", "500",
                                    "timeout", "Timeout", "DEADLINE", "deadline", "ReadTimeout",
                                    "ConnectTimeout", "WriteTimeout"))


# Two timeouts matter, and they are DIFFERENT things:
#  • `timeout` (ms) is a SERVER-SIDE deadline (the SDK forwards it as the request deadline). It does NOT
#    abort a client-side network READ stall — which is the intermittent hang (~1-2% of calls) that
#    wedged the bulk entity-engine run.
#  • `client_args={"timeout": <seconds>}` is passed straight to httpx.Client, giving a real CLIENT-SIDE
#    connect/read/write timeout. A stalled read then raises httpx.ReadTimeout, which _is_transient
#    catches → _RETRY reconnects (usually succeeds immediately on a fresh connection).
_HTTP_TIMEOUT_MS = 90_000
_CLIENT_TIMEOUT_S = 45.0


import threading as _threading
_CLIENT_CACHE: dict = {}
_CLIENT_LOCK = _threading.Lock()


def _client(key: str):
    """ONE shared genai client per key (thread-safe, reused across calls). Creating a client per call
    leaks httpx sockets and, under a thread pool, eventually blocks on connection exhaustion — the hang
    that stalled the bulk entity-engine run. The client's httpx layer is safe for concurrent use."""
    with _CLIENT_LOCK:
        c = _CLIENT_CACHE.get(key)
        if c is None:
            from google import genai
            from google.genai import types
            try:
                c = genai.Client(api_key=key, http_options=types.HttpOptions(
                    timeout=_HTTP_TIMEOUT_MS, client_args={"timeout": _CLIENT_TIMEOUT_S}))
            except Exception:
                try:
                    c = genai.Client(api_key=key, http_options=types.HttpOptions(timeout=_HTTP_TIMEOUT_MS))
                except Exception:
                    c = genai.Client(api_key=key)   # very old SDK → degrade gracefully
            _CLIENT_CACHE[key] = c
        return c


_RETRY = retry(retry=retry_if_exception(_is_transient),
               wait=wait_exponential(multiplier=2, min=5, max=90),
               stop=stop_after_attempt(10), reraise=True)


def generate(prompt: str, model: str | None = None, system: str | None = None,
             json_mode: bool = False, temperature: float = 0.2):
    model = model or config.DEFAULTS["llm"]
    prov = config.LLMS.get(model, {}).get("provider", "gemini")
    if prov == "gemini":
        return _gemini(prompt, model, system, json_mode, temperature)
    if prov == "openai":
        return _openai_chat(prompt, model, system, json_mode, temperature)
    if prov == "anthropic":
        return _anthropic_chat(prompt, model, system, json_mode, temperature)
    raise NotImplementedError(f"LLM provider '{prov}' not wired yet (model {model})")


def _openai_chat(prompt, model, system, json_mode, temperature):
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set — add it to step_7/.env to use OpenAI models")
    from openai import OpenAI
    client = OpenAI(api_key=key)
    msgs = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": prompt}]
    kw = {"model": model, "messages": msgs}
    if json_mode:
        kw["response_format"] = {"type": "json_object"}
    # GPT-5.x reasoning models can reject a non-default temperature; send it, retry without on that 400.
    try:
        r = _RETRY(lambda: client.chat.completions.create(temperature=temperature, **kw))()
    except Exception as e:
        if "temperature" in str(e).lower():
            r = _RETRY(lambda: client.chat.completions.create(**kw))()
        else:
            raise
    txt = (r.choices[0].message.content or "")
    u = getattr(r, "usage", None)
    it = getattr(u, "prompt_tokens", 0) or 0
    ot = getattr(u, "completion_tokens", 0) or 0
    costlog.log("openai", model, "generate", input_tokens=it, output_tokens=ot,
                meta="json" if json_mode else "text")
    return txt, {"input_tokens": it, "output_tokens": ot}


def _anthropic_chat(prompt, model, system, json_mode, temperature):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — Claude is unavailable here (add a direct key, "
                           "or run via Vertex AI Model Garden)")
    import anthropic
    client = anthropic.Anthropic(api_key=key)
    r = _RETRY(lambda: client.messages.create(
        model=model, max_tokens=2048, system=system or "",
        messages=[{"role": "user", "content": prompt}], temperature=temperature))()
    txt = "".join(b.text for b in r.content if getattr(b, "type", None) == "text")
    u = getattr(r, "usage", None)
    it = getattr(u, "input_tokens", 0) or 0
    ot = getattr(u, "output_tokens", 0) or 0
    costlog.log("anthropic", model, "generate", input_tokens=it, output_tokens=ot,
                meta="json" if json_mode else "text")
    return txt, {"input_tokens": it, "output_tokens": ot}


def _gemini(prompt, model, system, json_mode, temperature):
    from google import genai
    from google.genai import types
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set (step_4/.env or step_7/.env)")
    client = _client(key)
    cfg = types.GenerateContentConfig(
        temperature=temperature,
        system_instruction=system,
        response_mime_type="application/json" if json_mode else "text/plain",
    )
    r = _RETRY(lambda: client.models.generate_content(model=model, contents=prompt, config=cfg))()
    u = getattr(r, "usage_metadata", None)
    it = getattr(u, "prompt_token_count", 0) or 0
    ot = getattr(u, "candidates_token_count", 0) or 0
    costlog.log("gemini", model, "generate", input_tokens=it, output_tokens=ot,
                meta="json" if json_mode else "text")
    return (r.text or ""), {"input_tokens": it, "output_tokens": ot}


def generate_stream(prompt: str, model: str | None = None, system: str | None = None,
                    temperature: float = 0.2):
    """Yield answer text INCREMENTALLY (for the streaming chat). Gemini streams natively; other providers
    aren't wired for token streaming, so they produce the full text and yield it once (still works, just not
    progressive). Plain-text only (no json_mode)."""
    model = model or config.DEFAULTS["llm"]
    prov = config.LLMS.get(model, {}).get("provider", "gemini")
    if prov == "gemini":
        yield from _gemini_stream(prompt, model, system, temperature)
    else:
        txt, _ = generate(prompt, model=model, system=system, temperature=temperature)
        yield txt


def _gemini_stream(prompt, model, system, temperature):
    from google import genai  # noqa: F401
    from google.genai import types
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set (step_4/.env or step_7/.env)")
    client = _client(key)
    cfg = types.GenerateContentConfig(temperature=temperature, system_instruction=system,
                                      response_mime_type="text/plain")
    out_chars = 0
    for chunk in client.models.generate_content_stream(model=model, contents=prompt, config=cfg):
        t = getattr(chunk, "text", None)
        if t:
            out_chars += len(t)
            yield t
    costlog.log("gemini", model, "generate", input_tokens=len(prompt) // 4,
                output_tokens=out_chars // 4, meta="stream")


# ============================================================================
# generate_with_tools — ONE turn of function-calling (drives the agent loop)
# ============================================================================
# The dev tool's agent is a tool-USING loop: the model sees the question + a set of tool
# declarations, decides to either CALL a tool or ANSWER, we run any requested tools, feed the results
# back, and repeat until it answers. That loop's orchestration (which tools are enabled, running them,
# building the trace) lives in the app server; what belongs HERE — the model axis — is the single
# provider call that, given the running conversation + the tool schemas, returns the model's next move
# (function calls and/or text). Keeping it in lib/providers/llm.py means the agent stays model-
# agnostic: a Claude/OpenAI tool-use adapter would add its own branch right beside the Gemini one,
# and every call is cost-logged identically to `generate`.
def generate_with_tools(contents, tools: list, model: str | None = None,
                        system: str | None = None, temperature: float = 0.2):
    """Run ONE function-calling turn and return the model's reply (text + any tool calls).

    Args:
        contents: The running conversation as the provider's native turn list (for Gemini, a list of
            ``types.Content`` — user/model/function-response turns). The caller owns the history; this
            function just sends it and appends nothing.
        tools: Provider-NEUTRAL tool declarations — a list of ``{name, description, parameters}``
            dicts (exactly ``app.tools.Tool.to_declaration()``). We translate them into the provider's
            tool schema here, so tool authors never import a model SDK.
        model: LLM model id (a request "variable"); None → config.DEFAULTS["llm"].
        system: System instruction (the agent's grounding/citation policy).
        temperature: Sampling temperature (low for tool-use reliability).

    Returns:
        A dict the agent loop reads::

            {
              "text":  str,                              # any natural-language text the model emitted
              "calls": [ {"name": str, "args": dict} ],  # tool calls it wants run (possibly empty)
              "model_content": <provider turn>,          # the model's raw turn, to append to history
              "usage": {"input_tokens": int, "output_tokens": int},
              "finish": "tool_call" | "stop",            # did it ask for tools, or is it done?
            }

    Raises:
        NotImplementedError: if `model`'s provider isn't wired for tool-use yet.
    """
    model = model or config.DEFAULTS["llm"]
    prov = config.LLMS.get(model, {}).get("provider", "gemini")
    if prov == "gemini":
        return _gemini_tools(contents, tools, model, system, temperature)
    # The tool-using AGENT manages provider-native conversation turns (Gemini Content objects), so other
    # providers aren't wired for the multi-turn tool loop yet. Rather than break /api/query, orchestrate
    # the tools on the default Gemini model. (The single-shot Ask path, /api/ask, fully honours the
    # chosen OpenAI/etc. model — that's where model choice matters most.)
    return _gemini_tools(contents, tools, config.DEFAULTS["llm"], system, temperature)


def _gemini_tools(contents, tool_decls, model, system, temperature):
    """Gemini implementation of one function-calling turn (cost-logged like `generate`)."""
    from google import genai
    from google.genai import types
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set (step_4/.env or step_7/.env)")
    client = _client(key)

    # Translate our neutral {name, description, parameters} dicts into Gemini's tool schema. The SDK
    # accepts a plain dict for `parameters` and converts it to a types.Schema for us.
    declarations = [types.FunctionDeclaration(name=d["name"], description=d.get("description", ""),
                                              parameters=d.get("parameters") or {"type": "object"})
                    for d in tool_decls]
    gem_tools = [types.Tool(function_declarations=declarations)] if declarations else None

    cfg = types.GenerateContentConfig(temperature=temperature, system_instruction=system,
                                      tools=gem_tools)
    r = _RETRY(lambda: client.models.generate_content(model=model, contents=contents, config=cfg))()

    # Pull the model's turn (its Content) so the caller can append it verbatim to history, and harvest
    # any function calls + any plain text it emitted alongside them.
    calls, text_bits = [], []
    cand = (getattr(r, "candidates", None) or [None])[0]
    model_content = getattr(cand, "content", None) if cand else None
    for part in (getattr(model_content, "parts", None) or []):
        fc = getattr(part, "function_call", None)
        if fc is not None:
            # fc.args is a proto-ish Map; dict() gives a plain JSON-able dict for our tool runner.
            calls.append({"name": fc.name, "args": dict(fc.args or {})})
        if getattr(part, "text", None):
            text_bits.append(part.text)

    u = getattr(r, "usage_metadata", None)
    it = getattr(u, "prompt_token_count", 0) or 0
    ot = getattr(u, "candidates_token_count", 0) or 0
    costlog.log("gemini", model, "generate", input_tokens=it, output_tokens=ot,
                meta=f"toolcall x{len(calls)}" if calls else "tooluse-answer")

    return {
        "text":          "".join(text_bits).strip(),
        "calls":         calls,
        "model_content": model_content,
        "usage":         {"input_tokens": it, "output_tokens": ot},
        "finish":        "tool_call" if calls else "stop",
    }


# ============================================================================
# Small helpers the agent loop uses to build Gemini turns without importing the SDK itself
# ============================================================================
# The server keeps conversation history as provider-native turns but should not have to import
# google.genai to build them — that would leak the provider into the app layer. These thin factories
# live beside the adapter so the loop stays model-agnostic: a different provider would supply its own.
def user_turn(text: str):
    """Build a 'user' turn for the conversation history (a Gemini types.Content)."""
    from google.genai import types
    return types.Content(role="user", parts=[types.Part(text=text)])


def function_result_turn(name: str, result: dict):
    """Build a 'function'/'tool' turn carrying one tool's result back to the model.

    Gemini expects the tool output as a ``function_response`` part whose ``response`` is a dict. We
    wrap the tool payload under a ``"result"`` key (a stable shape the model can read regardless of
    which tool produced it).
    """
    from google.genai import types
    return types.Content(role="user",
                         parts=[types.Part.from_function_response(name=name, response={"result": result})])
