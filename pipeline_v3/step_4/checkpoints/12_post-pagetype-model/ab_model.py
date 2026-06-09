#!/usr/bin/env python3
"""A/B harness: relabel a handful of HARD pages with different models (and/or
few-shot strategies), write to throwaway dirs, and diff each vs the human gold.

Isolates the MODEL lever: every model sees the EXACT few-shot set that the
production run gave that page (seed-42 replay over the full volume, per-page
holdout), the same snap+backstop+pass2 pipeline, identical everything else.

Usage:
    ./venv/bin/python rerun_compare/ab_model.py \
        --volume Appendix_3 --pages 23 24 25 31 33 34 37 \
        --models gemini-3.1-flash-lite gemini-3.5-flash gemini-3.1-pro-preview

Output: rerun_compare/ab/<model>/<volume>/page_XXX/page_XXX.json
Then prints a per-page, per-model diff-vs-gold table.
"""
from __future__ import annotations

import argparse
import random
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import auto_labeler as AL
from review_diff import diff_page

GOLD = ROOT / "reviewed"
AB_ROOT = HERE / "ab"


def build_production_fewshot(volume: str, seed: int, num_fewshot: int,
                             min_multi: int, num_fewshot_pass2: int,
                             page_type: bool = False, client=None,
                             pt_model: str = "gemini-3.1-flash-lite"):
    """Replay main()'s sequential few-shot selection to reproduce, for every page
    of `volume`, the EXACT few-shot set production used. Returns
    {page_num: (fewshot_dirs, pass2_dirs, target_type)}.

    With page_type=True, types the pool offline and each target via a cheap VLM
    call (cached), and routes same-type demos — exactly like production's
    --page-type-fewshot path."""
    all_examples = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(all_examples)
    pass2_pool = AL.discover_pass2_pool(all_examples)
    type_of = AL.classify_examples_by_type(all_examples) if page_type else None
    rng = random.Random(seed)
    pages = AL.discover_target_pages(volume)
    page_pdf = {pn: pdf for (dn, pn, pdf) in pages}
    out: dict[int, tuple] = {}
    for doc_name, page_num, _pdf in pages:
        page_name = f"page_{page_num:03d}"
        target_type = None
        if page_type:
            target_type, _i, _o = AL.classify_page_type(
                client, pt_model, page_pdf[page_num],
                cache_dir=AL.PAGE_TYPE_CACHE_DIR / doc_name)
        fs = AL.select_few_shot(all_examples, multi, doc_name, num_fewshot,
                                min_multi, rng, target_page=page_name,
                                target_type=target_type, type_of=type_of)
        p2 = (AL.select_pass2_fewshot(pass2_pool, doc_name, num_fewshot_pass2, rng,
                                      target_page=page_name)
              if pass2_pool else None)
        out[page_num] = (fs, p2, target_type)
    return out


def diff_counts(src_json: Path, gold_json: Path) -> dict:
    d = diff_page(src_json, gold_json)
    return {
        "recat": len(d["recat"]),
        "add": len(d["added"]),
        "rem": len(d["removed"]),
        "resize": len(d["resized"]),
        "doc": 1 if d["doc_count"] else 0,
        # "hard" = real category/recall/spurious errors (NOT cosmetic resizes)
        "hard": len(d["recat"]) + len(d["added"]) + len(d["removed"]) + (1 if d["doc_count"] else 0),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--volume", required=True)
    ap.add_argument("--pages", type=int, nargs="+", required=True)
    ap.add_argument("--models", nargs="+", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--num-fewshot", type=int, default=12)
    ap.add_argument("--min-multi", type=int, default=4)
    ap.add_argument("--num-fewshot-pass2", type=int, default=6)
    ap.add_argument("--image-width", default="1024")
    ap.add_argument("--no-backstop", action="store_true")
    ap.add_argument("--page-type", action="store_true",
                    help="Route same-PAGE-TYPE few-shot demos (vs same-volume).")
    args = ap.parse_args()

    AL._setup_logging()
    import os
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    client = AL.genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    AL._client = client
    AL._file_uri_map = AL._load_file_uri_map(args.image_width)
    image_width = AL.resolve_image_width(args.image_width)

    fewshot_map = build_production_fewshot(
        args.volume, args.seed, args.num_fewshot, args.min_multi, args.num_fewshot_pass2,
        page_type=args.page_type, client=client, pt_model="gemini-3.1-flash-lite")

    # locate the pdf for each requested page
    page_pdf = {pn: pdf for (dn, pn, pdf) in AL.discover_target_pages(args.volume)}
    tag = "pt" if args.page_type else "vol"   # few-shot strategy tag in output path

    results: dict[str, dict[int, dict]] = {}
    for model in args.models:
        results[model] = {}
        for pn in args.pages:
            page_name = f"page_{pn:03d}"
            fs, p2, ttype = fewshot_map[pn]
            out_path = AB_ROOT / f"{model}__{tag}" / args.volume / page_name / f"{page_name}.json"
            print(f"[{model}|{tag}] labeling {args.volume}/{page_name} type={ttype} "
                  f"({len(fs)} p1 + {len(p2) if p2 else 0} p2 examples) ...", flush=True)
            try:
                AL.process_page(
                    pdf_path=page_pdf[pn], doc_name=args.volume, page_number=pn,
                    client=client, model_name=model, fewshot_dirs=fs,
                    image_width=image_width, output_path=out_path,
                    pass2_fewshot_dirs=p2, snap=True, backstop=not args.no_backstop,
                )
                gold_json = GOLD / args.volume / page_name / f"{page_name}.json"
                results[model][pn] = diff_counts(out_path, gold_json)
            except Exception as exc:
                print(f"   FAILED: {exc}", flush=True)
                results[model][pn] = {"recat": -1, "add": -1, "rem": -1,
                                      "resize": -1, "doc": -1, "hard": -1}

    # ── Report ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 78)
    print(f"A/B vs GOLD — {args.volume} — 'hard' = recat+add+rem+doc (cosmetic resizes excluded)")
    print("=" * 78)
    header = "page    " + "".join(f"{m.replace('gemini-','g-'):>26}" for m in args.models)
    print(header)
    totals = {m: Counter() for m in args.models}
    for pn in args.pages:
        row = f"p{pn:03d}   "
        for m in args.models:
            r = results[m][pn]
            row += f"{('hard=%d (a%d r%d c%d d%d)' % (r['hard'], r['add'], r['rem'], r['recat'], r['doc'])):>26}"
            for k in ("recat", "add", "rem", "resize", "doc", "hard"):
                if r[k] > 0:
                    totals[m][k] += r[k]
        print(row)
    print("-" * 78)
    print("TOTALS:")
    for m in args.models:
        t = totals[m]
        print(f"  {m:28} hard={t['hard']:>3}  add={t['add']:>3} rem={t['rem']:>3} "
              f"recat={t['recat']:>3} doc={t['doc']:>2}  (resize={t['resize']})")
    print("\nOutputs under:", AB_ROOT)


if __name__ == "__main__":
    main()
