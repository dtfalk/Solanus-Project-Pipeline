#!/usr/bin/env python3
"""Gold-free review triage via model disagreement (Query-by-Committee).

After a volume is labeled (auto_labeled/<vol>, the primary model), this labels the
same pages with a cheap SHADOW model into shadow_labels/<vol>/ and ranks every page
by how much the two models disagree (boxes in one with no same-category IoU>=0.5
match in the other). Measured on Appendix_3: reviewing the top-disagreement half
catches ~79% of the real errors; zero-disagreement pages are auto-accept candidates
(spot-check a sample, don't walk them all).

Writes qa_output/<vol>/triage.txt (ranked worklist) and prints the summary.

Usage:
    ./venv/bin/python triage.py Appendix_2
    ./venv/bin/python triage.py Appendix_2 --shadow-model gemini-3.1-flash-lite
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import auto_labeler as AL  # noqa: E402

SHADOW_ROOT = SCRIPT_DIR / "shadow_labels"


def _load_ab():
    """Reuse the A/B harness's production-faithful few-shot replay."""
    spec = importlib.util.spec_from_file_location(
        "ab_model", SCRIPT_DIR / "rerun_compare" / "ab_model.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _boxes(path: Path):
    d = json.load(open(path))
    out = []
    for doc in d.get("documents", {}).values():
        if isinstance(doc, dict):
            for cat, polys in doc.items():
                if isinstance(polys, list):
                    for b in polys:
                        if b.get("vertices"):
                            xs = [v["x"] for v in b["vertices"]]
                            ys = [v["y"] for v in b["vertices"]]
                            out.append((cat, (min(xs), min(ys), max(xs), max(ys))))
    return out


def _iou(a, b):
    ix = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    ua = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def _disagreement(primary: Path, shadow: Path) -> int:
    """Boxes in either set with no same-category IoU>=0.5 partner in the other."""
    A, B = _boxes(primary), _boxes(shadow)
    n = 0
    for src, other in ((A, B), (B, A)):
        for cat, bb in src:
            if not any(c == cat and _iou(bb, ob) >= 0.5 for c, ob in other):
                n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volume")
    ap.add_argument("--shadow-model", default="gemini-3.1-flash-lite")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    AL._setup_logging()
    primary_dir = AL.AUTO_LABELED_DIR / args.volume
    if not primary_dir.exists():
        raise SystemExit(f"label {args.volume} first (no {primary_dir})")

    ab = _load_ab()
    AL.load_dotenv(AL.ENV_PATH)
    client = AL.genai.Client(api_key=AL.os.getenv("GEMINI_API_KEY"))
    fewshot_map = ab.build_production_fewshot(
        args.volume, args.seed, 12, 4, 6, page_type=True, client=client)
    page_pdf = {pn: pdf for (_d, pn, pdf) in AL.discover_target_pages(args.volume)}

    # shadow-label any page not already shadowed (resumable)
    for pn, pdf in sorted(page_pdf.items()):
        name = f"page_{pn:03d}"
        out = SHADOW_ROOT / args.volume / name / f"{name}.json"
        if out.exists() or pn not in fewshot_map:
            continue
        fs, p2, _t = fewshot_map[pn]
        print(f"[shadow {args.shadow_model}] {args.volume}/{name}", flush=True)
        try:
            AL.process_page(pdf_path=pdf, doc_name=args.volume, page_number=pn,
                            client=client, model_name=args.shadow_model,
                            fewshot_dirs=fs, image_width=1024, output_path=out,
                            pass2_fewshot_dirs=p2, snap=True, backstop=True)
        except Exception as exc:
            print(f"  shadow failed {name}: {exc}")

    # rank by disagreement
    rows = []
    for pn in sorted(page_pdf):
        name = f"page_{pn:03d}"
        pj = primary_dir / name / f"{name}.json"
        sj = SHADOW_ROOT / args.volume / name / f"{name}.json"
        if pj.exists() and sj.exists():
            rows.append((_disagreement(pj, sj), name))
    rows.sort(reverse=True)

    out_dir = SCRIPT_DIR / "qa_output" / args.volume
    out_dir.mkdir(parents=True, exist_ok=True)
    zero = [n for d, n in rows if d == 0]
    with open(out_dir / "triage.txt", "w") as f:
        f.write(f"# Review triage for {args.volume} — walk top-down; "
                f"high disagreement = review first\n")
        for d, n in rows:
            f.write(f"{d:4d}  {n}\n")
        f.write(f"\n# zero-disagreement auto-accept candidates ({len(zero)}): "
                f"spot-check a few, don't walk all\n")
    print(f"\ntriage written: {out_dir/'triage.txt'}")
    print(f"pages: {len(rows)} | zero-disagreement: {len(zero)} | "
          f"top-5: {[(d, n) for d, n in rows[:5]]}")


if __name__ == "__main__":
    main()
