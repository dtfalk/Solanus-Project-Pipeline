"""lib/providers/embed.py — model-agnostic embedding adapters.

embed_texts(texts, model, dim, task) -> (np.ndarray [n, dim], usage dict). Routes to the provider
named in config.EMBEDDINGS[model]. Every call logs cost via lib.costlog. Vectors are L2-normalized
(cosine == dot). Adapters: gemini + local(fastembed) are usable now; openai + voyage are real but
require their API keys (raise a clear error otherwise).
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

import numpy as np
from tenacity import retry, retry_if_exception, wait_exponential, stop_after_attempt

STEP7 = Path(__file__).resolve().parents[2]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config            # noqa: E402
from lib import costlog  # noqa: E402

# load keys: step_7/.env first, then step_4/.env (GEMINI_API_KEY lives there)
try:
    from dotenv import load_dotenv
    load_dotenv(config.STEP7 / ".env")
    load_dotenv(config.REPO / "pipeline_v3" / "step_4" / ".env")
except Exception:
    pass

FASTEMBED_NAMES = {"bge-large-en-v1.5": "BAAI/bge-large-en-v1.5",
                   "bge-small-en-v1.5": "BAAI/bge-small-en-v1.5"}
_LOCAL_CACHE: dict = {}


def _est_tokens(texts) -> int:
    return max(1, sum(len(t) for t in texts) // 4)   # ~4 chars/token (for cost logging/projection)


# Providers throttle by requests-per-minute and occasionally hiccup with 5xx. We treat those as
# *transient* and let tenacity wait-and-retry with exponential backoff. This makes a bulk run
# self-throttle to the provider's pace instead of crashing partway (same lesson as the OCR step).
def _is_transient(exc: Exception) -> bool:
    s = str(exc)
    return any(tok in s for tok in ("429", "RESOURCE_EXHAUSTED", "503", "UNAVAILABLE", "500", "ServiceUnavailable"))


_RETRY = retry(retry=retry_if_exception(_is_transient),
               wait=wait_exponential(multiplier=2, min=5, max=90),
               stop=stop_after_attempt(10), reraise=True)


def _norm(vecs) -> np.ndarray:
    v = np.asarray(vecs, dtype=np.float32)
    n = np.linalg.norm(v, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return v / n


def embed_texts(texts, model: str, dim: int, task: str = "document", batch: int = 64):
    if model not in config.EMBEDDINGS:
        raise ValueError(f"unknown embedding model '{model}' (add to config.EMBEDDINGS)")
    prov = config.EMBEDDINGS[model]["provider"]
    return {"gemini": _gemini, "local": _local, "openai": _openai, "voyage": _voyage}[prov](
        texts, model, dim, task, batch)


def _gemini(texts, model, dim, task, batch):
    from google import genai
    from google.genai import types
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set (step_4/.env or step_7/.env)")
    client = genai.Client(api_key=key)
    tt = "RETRIEVAL_QUERY" if task == "query" else "RETRIEVAL_DOCUMENT"
    cfg = types.EmbedContentConfig(output_dimensionality=dim, task_type=tt)
    # The config KEY is the partition/space name (what costs + the vector store key on), which may be
    # an ALIAS that differs from the provider's real model name — e.g. a "…-ctx" space that stores
    # contextualized embeddings but is still produced by the same underlying API model. Honor an
    # optional "api" override so one real model can back several named spaces.
    api_model = config.EMBEDDINGS[model].get("api", model)

    # One batch -> the API, retried with backoff on transient rate-limit / 5xx errors.
    @_RETRY
    def _call(chunk):
        return client.models.embed_content(model=api_model, contents=chunk, config=cfg)

    out = []
    for i in range(0, len(texts), batch):
        chunk = texts[i:i + batch]
        r = _call(chunk)
        out.extend([e.values for e in r.embeddings])
        costlog.log("gemini", model, "embed", input_tokens=_est_tokens(chunk), items=len(chunk), meta=f"dim={dim}")
    return _norm(out), {"n": len(texts)}


def _local(texts, model, dim, task, batch):
    from fastembed import TextEmbedding
    name = FASTEMBED_NAMES.get(model, model)
    m = _LOCAL_CACHE.get(name)
    if m is None:
        m = _LOCAL_CACHE[name] = TextEmbedding(model_name=name)
    vecs = list(m.embed(texts))
    costlog.log("local", model, "embed", items=len(texts), usd=0.0, meta=f"dim={vecs[0].shape[0] if vecs else dim}")
    return _norm(vecs), {"n": len(texts)}


def _openai(texts, model, dim, task, batch):
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set — add to step_7/.env to use OpenAI embeddings")
    from openai import OpenAI
    client = OpenAI(api_key=key)
    out = []
    # OpenAI embeddings cap at 8192 tokens/input; a few stitched multi-page letters exceed that. Cap to
    # ~6k tokens worth of chars so the request never 400s (the tail of a giant chunk is rarely decisive).
    for i in range(0, len(texts), batch):
        chunk = [(t or "")[:24000] for t in texts[i:i + batch]]
        r = client.embeddings.create(model=model, input=chunk, dimensions=dim)
        out.extend([d.embedding for d in r.data])
        costlog.log("openai", model, "embed", input_tokens=r.usage.total_tokens, items=len(chunk), meta=f"dim={dim}")
    return _norm(out), {"n": len(texts)}


def _voyage(texts, model, dim, task, batch):
    key = os.environ.get("VOYAGE_API_KEY")
    if not key:
        raise RuntimeError("VOYAGE_API_KEY not set — add to step_7/.env to use Voyage embeddings")
    import voyageai
    vo = voyageai.Client(api_key=key)
    out, toks = [], 0
    for i in range(0, len(texts), batch):
        chunk = [(t or "")[:100000] for t in texts[i:i + batch]]   # guard against an outlier giant chunk
        r = vo.embed(chunk, model=model, input_type=("query" if task == "query" else "document"), output_dimension=dim)
        out.extend(r.embeddings)
        toks += getattr(r, "total_tokens", _est_tokens(chunk))
    costlog.log("voyage", model, "embed", input_tokens=toks, items=len(texts), meta=f"dim={dim}")
    return _norm(out), {"n": len(texts)}
