"""Pricing per model + helper to append a usage row to a CSV.

Add new models to PRICING by their exact model name string as used in
auto_labeler (e.g. "gemini-2.0-flash"). Values are (input_per_million,
output_per_million) in USD.
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path


# Model name (as passed to client.models.generate_content) -> (input $/1M tok, output $/1M tok)
PRICING: dict[str, tuple[float, float]] = {
    "gemini-3.1-flash-lite":  (0.25, 1.50),
    "gemini-3-flash-preview": (0.50, 3.00),
    "gemini-3.5-flash":       (1.50, 9.00),
    "gemini-3.1-pro-preview": (2.00, 12.00)
}


def calculate_cost(
    model: str,
    input_tokens:  int,
    output_tokens: int,
) -> tuple[float, float, float]:
    """Return (input_cost, output_cost, total_cost) in USD.

    Falls back to (0, 0, 0) for unknown models so missing pricing never crashes
    the labeling run — but prints a one-line warning to stderr.
    """
    if model not in PRICING:
        print(f"[pricing] WARNING: no entry for model {model!r}; logging cost as 0")
        return 0.0, 0.0, 0.0

    in_per_m, out_per_m = PRICING[model]
    in_cost  = input_tokens  / 1_000_000 * in_per_m
    out_cost = output_tokens / 1_000_000 * out_per_m
    return in_cost, out_cost, in_cost + out_cost


def append_run_summary(
    csv_path: Path,
    model:    str,
    pages:    int,
    input_tokens:  int,
    output_tokens: int,
) -> None:
    """Append ONE row summarizing a full labeling run.

    Tokens are the sum across both passes (polygons + connections) and across
    all pages that succeeded during this invocation.
    """
    in_cost, out_cost, total_cost = calculate_cost(model, input_tokens, output_tokens)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "model", "pages",
                "input_tokens", "output_tokens",
                "input_cost_usd", "output_cost_usd", "total_cost_usd",
            ])
        writer.writerow([
            datetime.now().isoformat(timespec="seconds"),
            model,
            pages,
            input_tokens,
            output_tokens,
            f"{in_cost:.6f}",
            f"{out_cost:.6f}",
            f"{total_cost:.6f}",
        ])
