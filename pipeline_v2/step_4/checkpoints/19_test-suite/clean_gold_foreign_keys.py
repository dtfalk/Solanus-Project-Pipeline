#!/usr/bin/env python3
"""OPT-IN: strip EMPTY schema-foreign category stamps from reviewed/ gold.

The editor (normalized_editor.py) writes empty `src_margin_note` / `src_insertion`
lists (categories not in the model's 20-class schema) into every gold JSON it saves
(PLAYBOOK §3.11). They're inert (empty, downstream-stripped, the few-shot pool is
clean) but cosmetically pollute the gold. This scrubs ONLY empty foreign-category
keys — it never touches a real box, a real category, or geometry.

SAFE BY DEFAULT: dry-run unless you pass --apply. This is the ONLY tool that writes
reviewed/, and it is YOURS to run consciously — the agent will not run it on gold.

    ./venv/bin/python clean_gold_foreign_keys.py            # dry-run (report only)
    ./venv/bin/python clean_gold_foreign_keys.py --apply    # actually scrub
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import auto_labeler as AL

REVIEW = Path(__file__).resolve().parent / "reviewed"
SCHEMA = set(AL.CATEGORIES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()
    n_files = n_keys = n_changed = 0
    refused = []
    for jp in sorted(REVIEW.glob("*/page_*/*.json")):
        d = json.load(open(jp))
        changed = False
        for doc in d.get("documents", {}).values():
            if not isinstance(doc, dict):
                continue
            for c in [k for k in doc if k not in SCHEMA]:
                polys = doc[c]
                if isinstance(polys, list) and len(polys) == 0:
                    del doc[c]; n_keys += 1; changed = True
                else:
                    refused.append(f"{jp.parent.parent.name}/{jp.parent.name}:{c}({len(polys)})")  # REAL box — never touch
        if changed:
            n_changed += 1
            if args.apply:
                json.dump(d, open(jp, "w"), indent=2)
        n_files += 1
    mode = "APPLIED" if args.apply else "DRY-RUN (no writes)"
    print(f"[{mode}] scanned {n_files} gold files; "
          f"{'removed' if args.apply else 'would remove'} {n_keys} empty foreign keys "
          f"across {n_changed} files.")
    if refused:
        print(f"REFUSED to touch {len(refused)} NON-empty foreign entries (real boxes — curator must "
              f"reassign a valid category): {refused[:5]}")
    if not args.apply and n_keys:
        print("Re-run with --apply to scrub. (Agent will not run this on your gold without you.)")


if __name__ == "__main__":
    main()
