#!/usr/bin/env python3
"""Few-shot A/B: does upping the demo count or smarter (layout-similarity) selection
improve labeling on a held-out set? Scores PQ / strict-PQ / RQ(recall) / SQ vs gold.

Configs (production path, snap+backstop ON, connections off — geometry/PQ unaffected):
  c12       12 demos, current selection (type-routed + random-within-type)  [= production]
  c20       20 demos, current selection
  c12_sim   12 demos, type-routed but nearest-by-LAYOUT within type (deterministic, no API)

Held-out pages stay in the pool but per-page holdout prevents self-leakage (production-like).
Usage: ./venv/bin/python experiments/fewshot_ab.py
"""
from __future__ import annotations
import sys, os, json, random, tempfile
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
from dotenv import load_dotenv; load_dotenv(HERE / ".env")
import auto_labeler as AL
import panoptic_eval as PE
from google import genai

# Held-out test set: stratified, weighted to the hard cases (notebook) + recall-prone mixed pages.
TEST = [("Volume_1","page_010"),("Volume_1","page_092"),("Volume_1","page_120"),
        ("Volume_1","page_200"),("Volume_1","page_237"),("Volume_1","page_050"),
        ("Appendix_1","page_007"),("Appendix_1","page_011"),
        ("Appendix_2","page_006"),("Appendix_2","page_010"),
        ("Appendix_3","page_021"),("Appendix_3","page_030")]


def layout_desc(pdf):
    """192-dim ink-density grid (12 rows x 16 cols) of the page — cheap layout fingerprint."""
    img = AL.render_page(pdf, 256)[0].convert("L")
    thr = AL._otsu_threshold(img.histogram()[:256])
    a = (np.asarray(img) < thr).astype(np.float32)
    h, w = a.shape
    rows, cols = 12, 16
    g = np.zeros((rows, cols), np.float32)
    for r in range(rows):
        for c in range(cols):
            g[r, c] = a[r*h//rows:(r+1)*h//rows, c*w//cols:(c+1)*w//cols].mean()
    v = g.flatten()
    n = np.linalg.norm(v)
    return v / n if n > 0 else v


def main():
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    AL._client = client
    AL._file_uri_map = AL._load_file_uri_map("1024")     # V1 not uploaded -> inline fallback (fine)
    ex = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex)
    type_of = AL.classify_examples_by_type(ex)

    # precompute layout descriptors for the pool (cached) for the similarity selector
    print(f"pool {len(ex)}; computing layout descriptors...")
    desc = {}
    for p in ex:
        pdf = next(p.glob("*.pdf"), None)
        if pdf: desc[p] = layout_desc(pdf)

    def sel_current(doc, page, ptype, n):
        return AL.select_few_shot(ex, multi, doc, n, 2, random.Random(42),
                                  target_page=page, target_type=ptype, type_of=type_of)

    def sel_sim(doc, page, ptype, n, tgt_pdf):
        same = [p for p in ex if type_of.get(p) == ptype
                and not (p.parent.name == doc and p.name == page) and p in desc]
        if len(same) < n:  # fall back to all (minus self) if a type is thin
            same = [p for p in ex if not (p.parent.name == doc and p.name == page) and p in desc]
        td = layout_desc(tgt_pdf)
        same.sort(key=lambda p: float(np.dot(desc[p], td)), reverse=True)   # cosine sim desc
        return same[:n]

    CONFIGS = {
        "c12":     lambda d,pg,t,pdf: sel_current(d,pg,t,12),
        "c20":     lambda d,pg,t,pdf: sel_current(d,pg,t,20),
        "c12_sim": lambda d,pg,t,pdf: sel_sim(d,pg,t,12,pdf),
    }
    preddirs = {k: Path(tempfile.mkdtemp(prefix=f"fsab_{k}_")) for k in CONFIGS}
    for vol, name in TEST:
        pdf = AL.POLYGON_PDFS_DIR / vol / "pages" / f"{name}.pdf"
        if not pdf.exists():
            print(f"  SKIP {vol}/{name} (no pdf)"); continue
        ptype, _, _ = AL.classify_page_type(client, "gemini-3.1-flash-lite", pdf,
                                            cache_dir=AL.PAGE_TYPE_CACHE_DIR / vol)
        for cfg, selfn in CONFIGS.items():
            fs = selfn(vol, name, ptype, pdf)
            outp = preddirs[cfg] / vol / name / f"{name}.json"
            try:
                AL.process_page(pdf_path=pdf, doc_name=vol, page_number=int(name.split("_")[1]),
                                client=client, model_name="gemini-3.5-flash", fewshot_dirs=fs,
                                image_width=1024, output_path=outp, pass2_fewshot_dirs=None,
                                snap=True, backstop=True)
            except Exception as e:
                print(f"  {cfg} {vol}/{name} FAIL {type(e).__name__}: {str(e)[:70]}")
        print(f"  labeled {vol}/{name} ({ptype})")

    # score each config vs gold, aggregated across the test volumes
    print(f"\n{'config':10} {'PQ':>7} {'PQ_strict':>10} {'RQ(recall)':>11} {'SQ':>7}  TP/FP/FN")
    vols = sorted({v for v, _ in TEST})
    for cfg in CONFIGS:
        TP=FP=FN=0; sqs=[]; sTP=sFP=sFN=0
        for vol in vols:
            r = PE.eval_volume(vol, preddirs[cfg], HERE/"reviewed", 1000, False)
            TP+=r["TP"]; FP+=r["FP"]; FN+=r["FN"]; sTP+=r["sTP"]; sFP+=r["sFP"]; sFN+=r["sFN"]
            if r["TP"]: sqs.append((r["SQ"], r["TP"]))
        rq = TP/(TP+0.5*FP+0.5*FN) if (TP+FP+FN) else 0
        sq = sum(s*w for s,w in sqs)/sum(w for _,w in sqs) if sqs else 0
        srq = sTP/(sTP+0.5*sFP+0.5*sFN) if (sTP+sFP+sFN) else 0
        print(f"{cfg:10} {sq*rq:7.3f} {sq*srq:10.3f} {rq:11.3f} {sq:7.3f}  {TP}/{FP}/{FN}")
    print("\n(eval pages restricted to the 12 test pages that exist in each pred dir; "
          "RQ=recall/recognition F1, the metric more demos COULD move; SQ=box tightness.)")


if __name__ == "__main__":
    main()
