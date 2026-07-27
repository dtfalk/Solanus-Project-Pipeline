"""translate.py — backend adapter for the Solanus STYLE TRANSLATOR (the reverse-desanitizer).

WHAT THIS IS. pipeline_v3/mimicker trained a LoRA adapter (adapters/translator-qwen_fast, base
Qwen2.5-7B-Instruct) whose ONLY job is to rewrite plain, modern English into Father Solanus Casey's
gentle, humble writing voice WITHOUT changing any fact. It is NOT a language translator and NOT an
archaic->modern modernizer — it goes modern English -> Solanus's voice. (See
pipeline_v3/mimicker/prompts.py and restyle_flow.py.)

This module is the app-side seam that calls that adapter once it is hosted behind an Azure endpoint
(Azure ML managed online endpoint or Azure AI Foundry, exposing an OpenAI-compatible /chat/completions
shape — which is what vLLM / MaaS deployments give you). It is GATED: until the endpoint env vars are
set it reports configured=False and translate() raises a clear, actionable error, so the /api/translate
route no-ops (503) and nothing else in the app is affected.

Credentials (env — add to pipeline_v3/step_7/.env, the same file voice.py loads):
  AZURE_TRANSLATOR_ENDPOINT   full URL of the deployed scoring endpoint, e.g.
                              https://<resource>.<region>.inference.ml.azure.com/score   (Azure ML), or
                              https://<resource>.services.ai.azure.com/models/chat/completions?... (Foundry)
  AZURE_TRANSLATOR_KEY        the endpoint's primary key (sent as `Authorization: Bearer <key>`)
Optional:
  AZURE_TRANSLATOR_DEPLOYMENT name of the specific deployment under the endpoint
                              (sent as the `azureml-model-deployment` header; Azure ML only)
  AZURE_TRANSLATOR_API_STYLE  "openai" (default) | "azureml"   — request/response envelope (see _call)
  TRANSLATOR_ENABLED          "1" to allow calls (default: auto — enabled iff endpoint+key are present)
  AZURE_TRANSLATOR_MAX_TOKENS (default 1024)  AZURE_TRANSLATOR_TEMPERATURE (default 0.6)

The route stays a no-op until BOTH endpoint and key exist, so it is safe to ship today.
"""
from __future__ import annotations
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

_APP = Path(__file__).resolve().parent
_REPO = _APP.parent

# Load the project's keys (the SAME .env voice.py / the providers use), regardless of import order.
try:
    from dotenv import load_dotenv
    load_dotenv(_REPO / "pipeline_v3" / "step_7" / ".env")
except Exception:
    pass

# ---------------------------------------------------------------------------------------------------------
# THE SYSTEM PROMPT. This MUST byte-match the one the adapter was trained with, or the LoRA is run
# off-distribution (see pipeline_v3/mimicker/prompts.py for the policy). We import it from that single
# source of truth when reachable; otherwise we fall back to a verbatim copy (keep the two identical).
# ---------------------------------------------------------------------------------------------------------
_MIMICKER = _REPO / "pipeline_v3" / "mimicker"
TRANSLATOR_SYS = None
try:
    if str(_MIMICKER) not in sys.path:
        sys.path.insert(0, str(_MIMICKER))
    from prompts import TRANSLATOR_SYS as _TS   # pure-stdlib module, safe to import
    TRANSLATOR_SYS = _TS
except Exception:
    TRANSLATOR_SYS = (
        "You are a faithful style translator. Rewrite the user's passage of plain, modern English into the "
        "writing voice of Father Solanus Casey, the Capuchin friar (1870-1957) — his gentle, humble, grateful "
        "idiom ('Deo gratias', 'Thanks be to God', confidence in Providence). Preserve the meaning and every "
        "fact EXACTLY; change only the voice. Output ONLY the rewritten passage, nothing else.")

ENDPOINT = (os.environ.get("AZURE_TRANSLATOR_ENDPOINT") or "").strip().rstrip("/")
KEY = (os.environ.get("AZURE_TRANSLATOR_KEY") or "").strip()
DEPLOYMENT = (os.environ.get("AZURE_TRANSLATOR_DEPLOYMENT") or "").strip()
API_STYLE = (os.environ.get("AZURE_TRANSLATOR_API_STYLE") or "openai").strip().lower()
MAX_TOKENS = int(os.environ.get("AZURE_TRANSLATOR_MAX_TOKENS", "1024"))
TEMPERATURE = float(os.environ.get("AZURE_TRANSLATOR_TEMPERATURE", "0.6"))
TOP_P = float(os.environ.get("AZURE_TRANSLATOR_TOP_P", "0.92"))
_ENABLED_ENV = os.environ.get("TRANSLATOR_ENABLED")


def configured() -> bool:
    """True iff the endpoint is wired AND not explicitly disabled. The route checks this and no-ops if False."""
    if _ENABLED_ENV is not None and _ENABLED_ENV.strip() not in ("1", "true", "True", "yes"):
        return False
    return bool(ENDPOINT and KEY)


def status() -> dict:
    """Capability payload for the UI / GET /api/translate (never leaks the key)."""
    return {
        "configured": configured(),
        "endpoint_set": bool(ENDPOINT),
        "key_set": bool(KEY),
        "deployment": DEPLOYMENT or None,
        "api_style": API_STYLE,
        "direction": "modern_english -> solanus_voice",
        "model": "Qwen2.5-7B-Instruct + translator LoRA (adapters/translator-qwen_fast)",
    }


def _post(url: str, body: bytes, headers: dict, timeout: int = 90) -> bytes:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        detail = e.read()[:400].decode("utf-8", "ignore")
        hint = ""
        if e.code in (401, 403):
            hint = " — check AZURE_TRANSLATOR_KEY and that key auth is enabled on the endpoint"
        elif e.code == 404:
            hint = " — check AZURE_TRANSLATOR_ENDPOINT path (Azure ML uses /score; OpenAI-style uses /chat/completions)"
        raise RuntimeError(f"http_{e.code}: {detail}{hint}")


def _messages(text: str) -> list[dict]:
    # EXACTLY the shape the adapter saw at train time and restyle_flow uses at inference: system + user.
    return [{"role": "system", "content": TRANSLATOR_SYS}, {"role": "user", "content": text}]


def _call(text: str, max_tokens: int, temperature: float) -> str:
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + KEY}
    if DEPLOYMENT:
        headers["azureml-model-deployment"] = DEPLOYMENT
    msgs = _messages(text)

    if API_STYLE == "azureml":
        # Generic Azure ML online-endpoint scoring envelope. Match this to your scoring script's contract;
        # this default assumes a script that accepts {"input_data": {...}} and returns {"output": "..."}.
        body = json.dumps({"input_data": {"messages": msgs, "max_new_tokens": max_tokens,
                                          "temperature": temperature, "top_p": TOP_P}}).encode("utf-8")
        out = json.loads(_post(ENDPOINT, body, headers))
        if isinstance(out, str):
            return out.strip()
        for k in ("output", "result", "generated_text", "text"):
            if isinstance(out, dict) and k in out:
                v = out[k]
                return (v[0] if isinstance(v, list) else v).strip()
        # fall through to OpenAI-shape parsing in case the script mirrors it
    else:
        # OpenAI-compatible /chat/completions (vLLM / MaaS / Foundry serverless). The default + recommended.
        body = json.dumps({"messages": msgs, "max_tokens": max_tokens, "temperature": temperature,
                           "top_p": TOP_P, "stream": False}).encode("utf-8")
        out = json.loads(_post(ENDPOINT, body, headers))

    try:
        return out["choices"][0]["message"]["content"].strip()
    except Exception:
        raise RuntimeError(f"unrecognized_response_shape: {str(out)[:300]}")


def translate(text: str, max_tokens: int | None = None, temperature: float | None = None) -> str:
    """Rewrite plain modern English `text` in Father Solanus Casey's voice via the Azure-hosted adapter.

    Raises RuntimeError (caught by the route -> 503) when the endpoint is not configured yet, so this is a
    safe no-op until the user deploys the endpoint and sets AZURE_TRANSLATOR_ENDPOINT + AZURE_TRANSLATOR_KEY.
    """
    text = (text or "").strip()
    if not text:
        raise RuntimeError("empty_text")
    if not configured():
        raise RuntimeError("translator_unconfigured: set AZURE_TRANSLATOR_ENDPOINT + AZURE_TRANSLATOR_KEY "
                           "in pipeline_v3/step_7/.env (and deploy the adapter — see the morning steps)")
    return _call(text, max_tokens or MAX_TOKENS, temperature if temperature is not None else TEMPERATURE)
