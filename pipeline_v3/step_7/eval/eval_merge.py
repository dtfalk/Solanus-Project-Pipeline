#!/usr/bin/env python3
"""eval/eval_merge.py — measure cautious-merge PRECISION/RECALL on the adversarial calibration set.

Runs the resolver's ``decide_pair`` (+ optional LLM gate) over ``eval/merge_calibration.json`` (119
pairs, each adversarially labeled ``gold_same`` by 2 skeptics) and reports precision / recall / F1 on
the MERGE decision — plus the dangerous FALSE MERGES and the missed FALSE SPLITS. This is the tight
loop for tuning the rules to "strong numbers" before a full (slow) resolve run.

    python eval/eval_merge.py              # deterministic rules only (free, instant)
    python eval/eval_merge.py --use-llm    # also run the LLM gate on grey pairs (paid; ~grey count)
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                              # noqa: E402
from stages import resolve_entities as R   # noqa: E402

CALIB = STEP7 / "eval" / "merge_calibration.json"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--use-llm", action="store_true", help="run the LLM gate on grey pairs (paid)")
    a = ap.parse_args()

    pairs = json.loads(CALIB.read_text())
    mentions, _ = R._load_raw_mentions()
    common = R.build_common_tokens(mentions)                # per-type anchor table from the real corpus

    tp = fp = fn = tn = n_llm = 0
    false_merges, false_splits = [], []
    for p in pairs:
        typ = (p.get("a_id") or "person").split(":")[0].upper()
        m1 = {"surface": p["a_name"], "type": typ, "attrs": {}, "mention_id": p.get("a_id", "a")}
        m2 = {"surface": p["b_name"], "type": typ, "attrs": {}, "mention_id": p.get("b_id", "b")}
        decision, score, reason = R.decide_pair(m1, m2, common)
        merged = decision == "merge"
        if decision == "llm":
            if a.use_llm:
                v = R.llm_same_pair(m1, m2)
                n_llm += 1
                merged = bool(v.get("same")) and float(v.get("confidence", 0)) >= R.LLM_CONFIDENCE_MIN
            else:
                merged = False                              # no LLM -> grey stays separate (abstain)
        gold = bool(p.get("gold_same"))
        if merged and gold:
            tp += 1
        elif merged and not gold:
            fp += 1
            false_merges.append((p, decision, reason))
        elif (not merged) and gold:
            fn += 1
            false_splits.append((p, decision, reason))
        else:
            tn += 1

    prec = tp / (tp + fp) if (tp + fp) else 1.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    print("=" * 70)
    print(f"CAUTIOUS-MERGE on calibration ({len(pairs)} pairs)  | LLM gate: {a.use_llm}")
    print("=" * 70)
    print(f"  precision={prec:.3f}  recall={rec:.3f}  F1={f1:.3f}   "
          f"(tp={tp} fp={fp} fn={fn} tn={tn}; llm_calls={n_llm})")
    print(f"\n  FALSE MERGES ({len(false_merges)}) — predicted SAME but gold DIFFERENT (precision errors, the dangerous ones):")
    for p, d, r in false_merges:
        print(f"    [{d}/{r.get('rule', '?')}] {p['a_name']!r} == {p['b_name']!r}  anchor={r.get('anchor')}")
    print(f"\n  FALSE SPLITS ({len(false_splits)}) — gold SAME but not merged (recall misses):")
    for p, d, r in false_splits[:30]:
        print(f"    [{d}] {p['a_name']!r} != {p['b_name']!r}")


if __name__ == "__main__":
    main()
