"""
Cost Tracker — tracks API usage, tokens, and estimated costs.

Every time we call Azure OpenAI, we record:
  - Which endpoint/model was used
  - How many input and output tokens were consumed
  - The estimated cost in USD

This module provides a single global `tracker` instance that all
pipeline scripts share. Call tracker.record_api_call() after each
successful API call, and tracker.log_summary() at the end.
"""

from collections import defaultdict
import logging

# ── Azure OpenAI pricing (USD per 1 million tokens) ───────────────────────────
# Update these if your pricing changes. Format: "model_name": (input_cost, output_cost)
PRICE_PER_MILLION_TOKENS = {
    "gpt-5-nano": (0.05, 0.40),
    "gpt-5-mini": (0.25, 2.00),
}


class CostTracker:
    """
    Accumulates API usage statistics across all calls in a pipeline run.

    Usage:
        tracker.record_api_call(endpoint, model, input_tokens, output_tokens)
        tracker.record_failure()
        tracker.log_summary()
        summary_dict = tracker.get_summary()
    """

    def __init__(self):
        # Aggregate stats keyed by (endpoint_url, model_name)
        self.usage_by_endpoint_and_model = defaultdict(
            lambda: {"calls": 0, "input_tokens": 0, "output_tokens": 0}
        )
        # Per-call records for detailed analysis
        self.individual_call_records = []
        # Counters
        self.total_errors_processed = 0
        self.total_failed = 0

    def _extract_region_from_endpoint(self, endpoint_url):
        """
        Pull the Azure region name from an endpoint URL.

        Example: "https://my-resource-eastus.openai.azure.com/" → "eastus"
        """
        # Split on "://" to get the hostname, then grab the last part before the first "."
        hostname = endpoint_url.split("://")[1].split(".")[0]
        return hostname.split("-")[-1]

    def record_api_call(self, endpoint_url, model_name, input_tokens, output_tokens, error_id=None):
        """
        Record a successful API call and its token usage.

        This updates both the aggregate totals and the per-call record list.
        """
        # Update aggregate stats
        key = (endpoint_url, model_name)
        self.usage_by_endpoint_and_model[key]["calls"] += 1
        self.usage_by_endpoint_and_model[key]["input_tokens"] += input_tokens
        self.usage_by_endpoint_and_model[key]["output_tokens"] += output_tokens

        # Calculate cost for this individual call
        input_price, output_price = PRICE_PER_MILLION_TOKENS[model_name]
        call_cost = (input_tokens / 1_000_000) * input_price + (output_tokens / 1_000_000) * output_price

        # Store individual record
        self.individual_call_records.append({
            "error_id": error_id,
            "endpoint": endpoint_url,
            "region": self._extract_region_from_endpoint(endpoint_url),
            "model": model_name,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": call_cost,
        })

        self.total_errors_processed += 1

    def record_failure(self):
        """Record that an API call failed (for summary reporting)."""
        self.total_failed += 1

    def get_total_cost(self):
        """Sum up the cost of every recorded call."""
        return sum(record["cost"] for record in self.individual_call_records)

    def get_summary(self):
        """
        Build a summary dict with totals, per-region breakdowns, and per-model breakdowns.

        This dict is saved to progress files and logged at the end of a run.
        """
        total_input = sum(stats["input_tokens"] for stats in self.usage_by_endpoint_and_model.values())
        total_output = sum(stats["output_tokens"] for stats in self.usage_by_endpoint_and_model.values())
        total_cost = self.get_total_cost()
        cost_per_error = total_cost / self.total_errors_processed if self.total_errors_processed else 0.0

        # Group by region
        by_region = {}
        for (endpoint, model), stats in self.usage_by_endpoint_and_model.items():
            region = self._extract_region_from_endpoint(endpoint)
            if region not in by_region:
                by_region[region] = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
            by_region[region]["calls"] += stats["calls"]
            by_region[region]["input_tokens"] += stats["input_tokens"]
            by_region[region]["output_tokens"] += stats["output_tokens"]
            # Calculate cost for this endpoint
            input_price, output_price = PRICE_PER_MILLION_TOKENS[model]
            by_region[region]["cost_usd"] += (
                (stats["input_tokens"] / 1_000_000) * input_price
                + (stats["output_tokens"] / 1_000_000) * output_price
            )

        # Group by model
        by_model = {}
        for (endpoint, model), stats in self.usage_by_endpoint_and_model.items():
            if model not in by_model:
                by_model[model] = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
            by_model[model]["calls"] += stats["calls"]
            by_model[model]["input_tokens"] += stats["input_tokens"]
            by_model[model]["output_tokens"] += stats["output_tokens"]
            input_price, output_price = PRICE_PER_MILLION_TOKENS[model]
            by_model[model]["cost_usd"] += (
                (stats["input_tokens"] / 1_000_000) * input_price
                + (stats["output_tokens"] / 1_000_000) * output_price
            )

        return {
            "total_errors_processed": self.total_errors_processed,
            "total_failed": self.total_failed,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_cost_usd": total_cost,
            "cost_per_error_usd": cost_per_error,
            "usage_by_endpoint": by_region,
            "usage_by_model": by_model,
        }

    def log_summary(self):
        """Print a human-readable usage and cost report to the console."""
        summary = self.get_summary()

        logging.info(f"\nUsage by Region:")
        for region, stats in summary["usage_by_endpoint"].items():
            logging.info(f"  {region}:")
            logging.info(f"    Calls:           {stats['calls']}")
            logging.info(f"    Input Tokens:    {stats['input_tokens']:,}")
            logging.info(f"    Output Tokens:   {stats['output_tokens']:,}")
            logging.info(f"    Cost:            ${stats['cost_usd']:.4f}")

        logging.info(f"\nUsage by Model:")
        for model, stats in summary["usage_by_model"].items():
            logging.info(f"  {model}:")
            logging.info(f"    Calls:           {stats['calls']}")
            logging.info(f"    Input Tokens:    {stats['input_tokens']:,}")
            logging.info(f"    Output Tokens:   {stats['output_tokens']:,}")
            logging.info(f"    Cost:            ${stats['cost_usd']:.4f}")

        logging.info("\n" + "=" * 70)
        logging.info("USAGE AND COST SUMMARY")
        logging.info("=" * 70)
        logging.info(f"  Errors Processed:  {summary['total_errors_processed']}")
        logging.info(f"  Failures:          {summary['total_failed']}")
        logging.info(f"  Input Tokens:      {summary['total_input_tokens']:,}")
        logging.info(f"  Output Tokens:     {summary['total_output_tokens']:,}")
        logging.info(f"  Total Tokens:      {summary['total_input_tokens'] + summary['total_output_tokens']:,}")
        logging.info(f"  Total Cost:        ${summary['total_cost_usd']:.4f}")
        logging.info(f"  Cost per Error:    ${summary['cost_per_error_usd']:.6f}")
        logging.info("=" * 70 + "\n")


# ── Global tracker instance (imported by pipeline scripts) ─────────────────────
tracker = CostTracker()
