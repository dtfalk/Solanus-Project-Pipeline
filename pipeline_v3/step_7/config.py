"""step_7/config.py — central, swappable configuration (the "model as a variable" layer).

Edit THIS file to add a model, an embedding space, or repoint paths. Everything else reads from
here. Model `price` entries are used only for cost logging (lib/costlog.py).

⚠️ PRICES ARE PLACEHOLDERS — confirm against each provider's current pricing page before any
billed run. Nothing is billed until a stage is actually executed (and David hasn't said go yet).
"""
from __future__ import annotations
import json
from pathlib import Path

# ---------------------------------------------------------------- paths
STEP7 = Path(__file__).resolve().parent
REPO = STEP7.parent.parent                      # Solanus-Project-Pipeline/
STEP6 = REPO / "pipeline_v3" / "step_6"
ENRICHED = STEP6 / "3_enriched"                 # source pages (gold json + masked png/pdf + extracts)
DOCUMENTS = STEP6 / "documents.json"            # correspondence (input)
NOTEBOOKS = STEP6 / "notebooks.json"            # notebooks (input)

DATA = STEP7 / "data"                           # step_7 generated outputs
COSTS = STEP7 / "costs"                         # cost logs
SEARCHABLE_PDFS = REPO / "searchable_pdfs"      # text-layer PDFs + IIIF manifests (per David)

# ---------------------------------------------------------------- generation LLMs (price USD / 1M tokens)
LLMS = {
    "gemini-2.5-flash":      {"provider": "gemini", "in": 0.30, "out": 2.50},
    "gemini-2.5-flash-lite": {"provider": "gemini", "in": 0.10, "out": 0.40},
    "gemini-2.5-pro":        {"provider": "gemini", "in": 1.25, "out": 10.00},
    # OpenAI (direct API — NOT in Google Model Garden). Wired in lib/providers/llm.py.
    "gpt-5.5":               {"provider": "openai", "in": 5.00, "out": 30.00},
    "gpt-5.4":               {"provider": "openai", "in": 2.50, "out": 15.00},
    "gpt-5.4-mini":          {"provider": "openai", "in": 0.75, "out": 4.50},
    # Anthropic Claude is available via Vertex Model Garden OR a direct ANTHROPIC_API_KEY; neither is
    # configured here, so it is registered as a provider but shown UNAVAILABLE until a key is added.
    "claude-opus-4-8":       {"provider": "anthropic", "in": 5.00, "out": 25.00},
    "claude-sonnet-4-6":     {"provider": "anthropic", "in": 3.00, "out": 15.00},
}

# ---------------------------------------------------------------- embedding models (price USD / 1M tokens)
EMBEDDINGS = {
    "gemini-embedding-001":   {"provider": "gemini", "price": 0.15, "dims": [768, 1536, 3072]},
    # ALIAS space: contextualized chunks (Anthropic Contextual Retrieval — an LLM-written situating
    # blurb prepended before embedding) embedded by the SAME real model. The "api" override points at
    # the real model name; the distinct KEY gives it its own vector-store partition + cost line, so it
    # is selectable side-by-side with the base space for A/B retrieval.
    "gemini-embedding-001-ctx": {"provider": "gemini", "api": "gemini-embedding-001",
                                 "price": 0.15, "dims": [768, 1536, 3072]},
    # OpenAI embedding suite (NOT the GPT chat models — those don't produce embeddings).
    "text-embedding-3-small": {"provider": "openai", "price": 0.02, "dims": [1536]},
    "text-embedding-3-large": {"provider": "openai", "price": 0.13, "dims": [3072]},
    # Voyage 4 family (Jan 2026): shared embedding space, dims 2048/1024(default)/512/256. First 200M
    # tokens free per account. This is the provider Anthropic recommends for embeddings (no Claude API).
    "voyage-4-large":         {"provider": "voyage", "price": 0.12, "dims": [2048, 1024, 512, 256]},
    "voyage-4":               {"provider": "voyage", "price": 0.06, "dims": [2048, 1024, 512, 256]},
    "voyage-4-lite":          {"provider": "voyage", "price": 0.02, "dims": [2048, 1024, 512, 256]},
    "bge-large-en-v1.5":      {"provider": "local",  "price": 0.00, "dims": [1024]},   # open-source, local
    "bge-small-en-v1.5":      {"provider": "local",  "price": 0.00, "dims": [384]},    # small/fast local (smoke tests)
}

# ---------------------------------------------------------------- rerankers
RERANKERS = {
    "rerank-2.5":          {"provider": "voyage", "price": 0.05},
    "rerank-2.5-lite":     {"provider": "voyage", "price": 0.02},
    "rerank-4-pro":        {"provider": "cohere", "price": 2.00},
    "bge-reranker-v2-m3":  {"provider": "local",  "price": 0.00},
}

# Embedding spaces to precompute (model, dim). RAG selects one of these as a variable; the vector
# store keeps one partition per space. Add a tuple here -> only that space (re)builds.
EMBEDDING_MATRIX = [
    ("gemini-embedding-001", 768),
    ("gemini-embedding-001", 1536),
    ("gemini-embedding-001", 3072),
    ("text-embedding-3-small", 1536),
    ("text-embedding-3-large", 3072),
    ("voyage-4-large", 1024),
    ("voyage-4", 1024),
    ("voyage-4-lite", 1024),
    ("bge-large-en-v1.5", 1024),
]

# Current default selections (the runtime "variables")
DEFAULTS = {
    "llm": "gemini-2.5-flash",
    "embedding": ["gemini-embedding-001", 1536],
    "reranker": "rerank-2.5",
}

# Which EMBEDDING providers are compatible with each chat-LLM provider. The chosen chat model drives
# the embedding choices the UI offers (e.g. an OpenAI GPT hides Gemini/Voyage embeddings). `local`
# (free, provider-agnostic) is ALWAYS allowed and added by the UI, so the list is never empty.
# Anthropic has no first-party embedding → Voyage is its recommended pairing.
EMBED_COMPAT = {
    "gemini":    ["gemini"],
    "openai":    ["openai"],
    "anthropic": ["voyage"],
}

# Env var name per provider (keys live in step_7/.env, never committed)
API_KEY_ENV = {
    "gemini": "GEMINI_API_KEY",      # already present in the project
    "openai": "OPENAI_API_KEY",
    "voyage": "VOYAGE_API_KEY",
    "cohere": "COHERE_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
}


def fingerprint() -> str:
    """A stable hash of the config that affects pipeline outputs — so editing a model/matrix/path
    marks dependent stages stale (diff-and-rerun)."""
    import hashlib
    payload = json.dumps({"LLMS": LLMS, "EMBEDDINGS": EMBEDDINGS, "RERANKERS": RERANKERS,
                          "EMBEDDING_MATRIX": EMBEDDING_MATRIX, "DEFAULTS": DEFAULTS},
                         sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()
