#!/usr/bin/env python3
"""Self-consistency A/B: 3-sample flash-lite ensemble vs single 3.5-flash.

Diversity comes from different few-shot draws (seeds 42/43/44) since the pipeline
is temperature-0. LEAKAGE CONTROL: selection uses the PRE-promotion pool
(checkpoints/15_pre-integrations/labeled_examples) so the 7 test pages' newly
promoted hard-page siblings are NOT available as demos — identical conditions to
the existing seed-42 and 3.5-flash baselines.

Fusion: same-category box clusters across samples (IoU>=0.5 greedy); keep clusters
with >=2/3 votes; average coordinates; num_documents = majority. Connections are
dropped in the fused output (not scored by panoptic_eval).

Run: ./venv/bin/python rerun_compare/sc_ab.py          # generate + fuse + score
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from pathlib import Path
from uuid import uuid4

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import auto_labeler as AL  # noqa: E402

PAGES = [23, 24, 25, 31, 33, 34, 37]
SEEDS = [42, 43, 44]
VOL = "Appendix_3"
AB = HERE / "ab"
FUSED = AB / "ensemble3-lite__pt"
PRE_PROMOTION_POOL = ROOT / "checkpoints" / "15_pre-integrations" / "labeled_examples"


def _load_ab_module():
    spec = importlib.util.spec_from_file_location("ab_model", HERE / "ab_model.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def generate():
    assert PRE_PROMOTION_POOL.exists(), "checkpoint 15 pool missing"
    AL.LABELED_EXAMPLES_DIR = PRE_PROMOTION_POOL          # leakage control
    ab = _load_ab_module()
    AL.load_dotenv(AL.ENV_PATH)
    client = AL.genai.Client(api_key=AL.os.getenv("GEMINI_API_KEY"))
    page_pdf = {pn: pdf for (_d, pn, pdf) in AL.discover_target_pages(VOL)}
    for seed in SEEDS:
        out_root = AB / f"sclite_s{seed}" / VOL
        if seed == 42 and (AB / "gemini-3.1-flash-lite__pt" / VOL).exists():
            if not out_root.exists():                      # reuse the existing seed-42 run
                shutil.copytree(AB / "gemini-3.1-flash-lite__pt" / VOL, out_root)
            print(f"seed 42: reusing existing gemini-3.1-flash-lite__pt outputs")
            continue
        fmap = ab.build_production_fewshot(VOL, seed, 12, 4, 6, page_type=True, client=client)
        for pn in PAGES:
            name = f"page_{pn:03d}"
            out = out_root / name / f"{name}.json"
            if out.exists():
                continue
            fs, p2, _t = fmap[pn]
            print(f"[lite seed={seed}] {name}", flush=True)
            AL.process_page(pdf_path=page_pdf[pn], doc_name=VOL, page_number=pn,
                            client=client, model_name="gemini-3.1-flash-lite",
                            fewshot_dirs=fs, image_width=1024, output_path=out,
                            pass2_fewshot_dirs=p2, snap=True, backstop=True)


def _boxes(doc_json):
    out = []
    for did, doc in doc_json.get("documents", {}).items():
        if isinstance(doc, dict):
            for cat, polys in doc.items():
                if isinstance(polys, list):
                    for b in polys:
                        if b.get("vertices"):
                            xs = [v["x"] for v in b["vertices"]]
                            ys = [v["y"] for v in b["vertices"]]
                            out.append([did, cat, min(xs), min(ys), max(xs), max(ys)])
    return out


def _iou(a, b):
    ix = max(0, min(a[4], b[4]) - max(a[2], b[2]))
    iy = max(0, min(a[5], b[5]) - max(a[3], b[3]))
    inter = ix * iy
    ua = (a[4]-a[2])*(a[5]-a[3]) + (b[4]-b[2])*(b[5]-b[3]) - inter
    return inter / ua if ua > 0 else 0.0


def fuse():
    for pn in PAGES:
        name = f"page_{pn:03d}"
        samples = []
        for seed in SEEDS:
            p = AB / f"sclite_s{seed}" / VOL / name / f"{name}.json"
            samples.append(json.load(open(p)))
        # majority doc count
        from collections import Counter
        nd = Counter(s.get("num_documents", 1) for s in samples).most_common(1)[0][0]
        # cluster boxes across samples (same category, IoU>=0.5, greedy)
        clusters = []   # each: list of (sample_idx, box)
        for si, s in enumerate(samples):
            for b in _boxes(s):
                for cl in clusters:
                    c0 = cl[0][1]
                    if c0[1] == b[1] and _iou(c0, b) >= 0.5 and all(x[0] != si for x in cl):
                        cl.append((si, b)); break
                else:
                    clusters.append([(si, b)])
        docs = {f"doc_{i}": {} for i in range(1, nd + 1)}
        kept = 0
        for cl in clusters:
            if len(cl) < 2:                                # < 2/3 votes -> drop
                continue
            kept += 1
            cat = cl[0][1][1]
            did = cl[0][1][0] if cl[0][1][0] in docs else "doc_1"
            x0 = sum(b[2] for _s, b in cl) / len(cl); y0 = sum(b[3] for _s, b in cl) / len(cl)
            x1 = sum(b[4] for _s, b in cl) / len(cl); y1 = sum(b[5] for _s, b in cl) / len(cl)
            docs[did].setdefault(cat, []).append({
                "id": str(uuid4()), "connections": [],
                "vertices": [{"x": int(x0), "y": int(y0)}, {"x": int(x1), "y": int(y0)},
                             {"x": int(x1), "y": int(y1)}, {"x": int(x0), "y": int(y1)}]})
        base = samples[0]
        fused = {k: base[k] for k in ("page_number", "source_file", "page_width",
                                      "page_height", "render_dpi") if k in base}
        fused["num_documents"] = nd
        fused["documents"] = docs
        outd = FUSED / VOL / name
        outd.mkdir(parents=True, exist_ok=True)
        json.dump(fused, open(outd / f"{name}.json", "w"), indent=2)
        src_pdf = AB / "sclite_s42" / VOL / name / f"{name}.pdf"
        if src_pdf.exists() and not (outd / f"{name}.pdf").exists():
            shutil.copy2(src_pdf, outd / f"{name}.pdf")
        print(f"fused {name}: {kept} boxes kept, docs={nd}")


def score():
    spec = importlib.util.spec_from_file_location("panoptic_eval", ROOT / "panoptic_eval.py")
    PE = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(PE)
    print(f"\n{'config':28} {'PQ':>6} {'RQ':>6} {'SQ':>6} {'TP':>4} {'FP':>4} {'FN':>4}")
    for label, pred in (("single flash-lite (s42)", AB / "sclite_s42"),
                        ("ENSEMBLE 3x flash-lite", FUSED),
                        ("single 3.5-flash", AB / "gemini-3.5-flash__pt")):
        r = PE.eval_volume(VOL, pred, ROOT / "reviewed", 1000, False)
        print(f"{label:28} {r['PQ']:6.3f} {r['RQ']:6.3f} {r['SQ']:6.3f} "
              f"{r['TP']:4d} {r['FP']:4d} {r['FN']:4d}")


if __name__ == "__main__":
    generate()
    fuse()
    score()
