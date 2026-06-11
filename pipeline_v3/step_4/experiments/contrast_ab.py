#!/usr/bin/env python3
"""Contrastive-pairs A/B (HITL §6.1, David's diff idea, 2026-06-11).

Both arms relabel David's 25 hand-reviewed Volume_2 sample pages (per-page
holdout keeps each page's own gold out of its demos) with the production
config (his 25 demos in pool + the new V2 volume note). The treatment arm adds
2 contrastive pairs — the volume's most-corrected pages shown as the model's
INCORRECT attempt vs David's CORRECT gold. Scored vs his gold with
panoptic_eval; decision rule: ship --contrast-pairs for --run-rest only if
strict-PQ improves. (Priors both ways: C-ICL literature says it helps; our
c20<c12 result says extra prompt mass can hurt.)

Usage: ./venv/bin/python experiments/contrast_ab.py
"""
from __future__ import annotations
import json, os, random, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
from dotenv import load_dotenv; load_dotenv(HERE / ".env")
import auto_labeler as AL
import panoptic_eval as PE
from google import genai

VOL = "Volume_2"
OUT = HERE / "rerun_compare" / "contrast_ab"


def main():
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    AL._client = client
    AL._file_uri_map = AL._load_file_uri_map("1024")
    state = json.loads((HERE / "qa_output" / VOL / "bootstrap_state.json").read_text())
    targets = state["sampled"]
    print(f"targets: {len(targets)} David-reviewed {VOL} sample pages")

    ex = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex)
    type_of = AL.classify_examples_by_type(ex)
    print(f"pool {len(ex)}; fingerprinting for similarity selection...")
    desc = {p: AL._layout_descriptor(AL._example_pdf(p)) for p in ex}

    contrast = AL.build_contrast_pairs(VOL, 2, 1024)
    arms = {"control": [], "contrast2": contrast}

    for arm, cps in arms.items():
        outdir = OUT / arm
        for name in targets:
            pdf = AL.POLYGON_PDFS_DIR / VOL / "pages" / f"{name}.pdf"
            ptype, _, _ = AL.classify_page_type(client, "gemini-3.1-flash-lite", pdf,
                                                cache_dir=AL.PAGE_TYPE_CACHE_DIR / VOL)
            tdesc = AL._layout_descriptor(pdf)
            fs = AL.select_few_shot(ex, multi, VOL, 12, 2, random.Random(42),
                                    target_page=name, target_type=ptype, type_of=type_of,
                                    target_desc=tdesc, desc_cache=desc)
            outp = outdir / VOL / name / f"{name}.json"
            try:
                AL.process_page(pdf_path=pdf, doc_name=VOL,
                                page_number=int(name.split("_")[1]),
                                client=client, model_name="gemini-3.5-flash",
                                fewshot_dirs=fs, image_width=1024, output_path=outp,
                                pass2_fewshot_dirs=None, snap=True, backstop=True,
                                contrast_pairs=cps)
                print(f"  [{arm}] {name} ok")
            except Exception as e:
                print(f"  [{arm}] {name} FAIL {type(e).__name__}: {str(e)[:70]}")

    print(f"\n{'arm':10} {'PQ':>7} {'PQ_strict':>10} {'RQ':>7} {'SQ':>7}  TP/FP/FN")
    for arm in arms:
        r = PE.eval_volume(VOL, OUT / arm, HERE / "reviewed", 1000, False)
        rq = r["TP"] / (r["TP"] + 0.5 * r["FP"] + 0.5 * r["FN"]) if r["TP"] else 0
        srq = r["sTP"] / (r["sTP"] + 0.5 * r["sFP"] + 0.5 * r["sFN"]) if r["sTP"] else 0
        print(f"{arm:10} {r['SQ']*rq:7.3f} {r['SQ']*srq:10.3f} {rq:7.3f} {r['SQ']:7.3f}"
              f"  {r['TP']}/{r['FP']}/{r['FN']}")
    print("\n(scored only on the sampled pages present in both pred dirs; "
          "ship contrast only on a strict-PQ win)")


if __name__ == "__main__":
    main()
