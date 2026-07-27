"""step_7/lib/costlog.py — always-on cost logging for every model/API call.

Call `log(...)` around every LLM / embedding / reranker request. Appends a timestamped row to
step_7/costs/usage.csv (provider, model, op, tokens, items, $). If `usd` is omitted it's estimated
from config prices (placeholders — verify before billing). `summary()` aggregates by model.
Thread-safe append. Importing this module never makes a network call or runs anything.
"""
from __future__ import annotations
import csv
import sys
import threading
import time
from pathlib import Path

_STEP7 = Path(__file__).resolve().parent.parent
if str(_STEP7) not in sys.path:
    sys.path.insert(0, str(_STEP7))
import config  # noqa: E402

CSV_PATH = config.COSTS / "usage.csv"
HEADER = ["ts", "provider", "model", "op", "input_tokens", "output_tokens", "items", "usd", "meta"]
_LOCK = threading.Lock()


def estimate(provider: str, model: str, op: str,
             input_tokens: int = 0, output_tokens: int = 0, items: int = 0) -> float:
    """Best-effort $ estimate from config price tables (per 1M tokens). Returns 0.0 if unknown."""
    if model in config.LLMS:
        m = config.LLMS[model]
        return (input_tokens * m.get("in", 0) + output_tokens * m.get("out", 0)) / 1_000_000
    if model in config.EMBEDDINGS:
        return (input_tokens * config.EMBEDDINGS[model].get("price", 0)) / 1_000_000
    if model in config.RERANKERS:
        # reranker pricing varies (per-search / per-token); record items, leave $ to explicit usd
        return 0.0
    return 0.0


def log(provider: str, model: str, op: str,
        input_tokens: int = 0, output_tokens: int = 0, items: int = 0,
        usd: float | None = None, meta: str = "") -> float:
    """Record one billed call. Returns the $ logged."""
    if usd is None:
        usd = estimate(provider, model, op, input_tokens, output_tokens, items)
    config.COSTS.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        new = not CSV_PATH.exists()
        with open(CSV_PATH, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(HEADER)
            w.writerow([time.strftime("%Y-%m-%dT%H:%M:%S"), provider, model, op,
                        input_tokens, output_tokens, items, round(usd, 6), meta])
    return usd


def snapshot() -> dict:
    """Running TOTALS across all models — {usd, input_tokens, output_tokens, calls}. Take one before
    and after an operation; the difference is THAT operation's cost (what a per-query UI should show,
    not the cumulative ledger from summary())."""
    agg = summary()
    return {"usd": round(sum(a["usd"] for a in agg.values()), 6),
            "input_tokens": sum(a["input_tokens"] for a in agg.values()),
            "output_tokens": sum(a["output_tokens"] for a in agg.values()),
            "calls": sum(a["calls"] for a in agg.values())}


def delta(before: dict, after: dict) -> dict:
    """after - before per field — one operation's OWN cost."""
    return {"usd": round(after.get("usd", 0) - before.get("usd", 0), 6),
            "input_tokens": after.get("input_tokens", 0) - before.get("input_tokens", 0),
            "output_tokens": after.get("output_tokens", 0) - before.get("output_tokens", 0),
            "calls": after.get("calls", 0) - before.get("calls", 0)}


def summary() -> dict:
    """Aggregate usage.csv by model -> {calls, input_tokens, output_tokens, items, usd}."""
    agg: dict = {}
    if not CSV_PATH.exists():
        return agg
    with open(CSV_PATH, newline="") as f:
        for r in csv.DictReader(f):
            a = agg.setdefault(r["model"], {"calls": 0, "input_tokens": 0, "output_tokens": 0, "items": 0, "usd": 0.0})
            a["calls"] += 1
            a["input_tokens"] += int(r["input_tokens"] or 0)
            a["output_tokens"] += int(r["output_tokens"] or 0)
            a["items"] += int(r["items"] or 0)
            a["usd"] += float(r["usd"] or 0)
    return agg


if __name__ == "__main__":
    import json
    print(json.dumps(summary(), indent=2))
