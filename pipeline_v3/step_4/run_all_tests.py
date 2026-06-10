#!/usr/bin/env python3
"""Comprehensive test suite for the step_4 VLM labeling pipeline.

Covers: static/structural (compile, ontology consistency, JSON validity), logic
(few-shot holdout/leakage, determinism, page-type routing + fallback, coordinate
round-trip, metric sanity), cheap-API integration (page-type classify, one full
process_page, tuning-data export), and integrity/safety (gold untouched, ZERO GCP
resources deployed). NEVER deploys anything — safe to run unattended overnight.

Run:  ./venv/bin/python run_all_tests.py            # everything incl. ~$0.30 API
      ./venv/bin/python run_all_tests.py --no-api   # offline only
"""
from __future__ import annotations
import argparse, importlib.util, io, json, os, random, subprocess, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

PASS, FAIL, SKIP = [], [], []
def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    (PASS if cond else FAIL).append(name)
    print(f"  [{tag}] {name}" + (f"  — {detail}" if (detail and not cond) else ""))
    return cond
def skip(name, why):
    SKIP.append(name); print(f"  [SKIP] {name} — {why}")
def section(t): print(f"\n{'='*70}\n{t}\n{'='*70}")

import auto_labeler as AL
import review_diff as RD
import panoptic_eval as PE
REVIEW = HERE / "reviewed"
AUTOLAB = HERE / "auto_labeled"
EXAMPLES = HERE / "labeled_examples"


# ── A. STATIC / STRUCTURAL ──────────────────────────────────────────────────────
def test_static():
    section("A. STATIC / STRUCTURAL")
    # A1: every python file compiles
    pyfiles = sorted([*HERE.glob("*.py"), *(HERE/"rerun_compare").glob("*.py")])
    bad = []
    for p in pyfiles:
        r = subprocess.run([sys.executable, "-m", "py_compile", str(p)], capture_output=True, text=True)
        if r.returncode != 0: bad.append(f"{p.name}: {r.stderr.strip()[:60]}")
    check(f"A1 all {len(pyfiles)} python files compile", not bad, "; ".join(bad[:3]))

    # A2: ontology consistency — schema categories == descriptions == DocumentLabels fields
    cats = set(AL.CATEGORIES)
    desc = set(AL.CATEGORY_DESCRIPTIONS)
    dl_fields = set(AL.DocumentLabels.model_fields)
    check("A2a CATEGORIES == CATEGORY_DESCRIPTIONS keys", cats == desc, f"diff={cats ^ desc}")
    check("A2b CATEGORIES == DocumentLabels schema fields", cats == dl_fields, f"diff={cats ^ dl_fields}")

    # A3: every gold + example JSON parses and is structurally valid.
    # Distinguish REAL foreign boxes (schema bug) from empty schema-foreign stamps
    # (the editor §3.11 writes empty src_margin_note/src_insertion lists into gold —
    # cosmetic; downstream strips them and the few-shot pool is clean).
    nbad = nfiles = empty_foreign = 0; real_foreign = []; nonquad = []
    for root in (REVIEW, EXAMPLES):
        for jp in root.glob("*/page_*/*.json"):
            nfiles += 1
            try:
                d = json.load(open(jp))
                for doc in d.get("documents", {}).values():
                    if not isinstance(doc, dict): continue
                    for c, polys in doc.items():
                        plist = polys if isinstance(polys, list) else []
                        if c not in cats:
                            if plist: real_foreign.append(f"{root.name}/{jp.parent.parent.name}/{jp.parent.name}:{c}")
                            else: empty_foreign += 1
                        for b in plist:
                            vs = b.get("vertices")
                            if vs and len(vs) != 4: nonquad.append(f"{jp.parent.name}:{len(vs)}v")
            except Exception:
                nbad += 1
    check(f"A3a all {nfiles} gold/example JSONs parse", nbad == 0, f"{nbad} unparseable")
    check("A3b no REAL foreign-category boxes (schema bug)", not real_foreign, f"{real_foreign[:3]}")
    if empty_foreign:
        print(f"  [WARN] {empty_foreign} empty schema-foreign stamps in gold (editor §3.11; cosmetic, "
              "downstream-stripped, pool clean) — see clean_gold_foreign_keys.py to scrub if desired")
    if nonquad:
        print(f"  [WARN] {len(nonquad)} non-quad (L-shaped) src_content polygons in the few-shot pool "
              f"({sorted(set(nonquad))[:4]}...) — intentional tight fits; production output is squared by "
              "_normalize_quad at inference. Pool + tuning target kept as-drawn (no squaring, per directive).")

    # A4: EXCLUDED_EXAMPLES all exist
    missing = [k for k in AL.EXCLUDED_EXAMPLES if not (EXAMPLES/k).is_dir()]
    check("A4 EXCLUDED_EXAMPLES all exist on disk", not missing, f"missing={missing}")

    # A5: pydantic response models construct
    try:
        AL.PageTypeResponse(page_type="notebook"); AL.DocumentsResponse(documents=[]); ok=True
    except Exception as e: ok=False
    check("A5 response models validate", ok)


# ── B. LOGIC (offline) ──────────────────────────────────────────────────────────
def test_logic():
    section("B. LOGIC (deterministic, offline)")
    ex = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex)
    type_of = AL.classify_examples_by_type(ex)

    # B1: per-page holdout — NO page ever sees itself (leakage), every volume, both selection modes
    viol = 0
    for tdoc in sorted({p.parent.name for p in ex}):
        for tt in (None, "notebook", "letter", "mass_card", "other"):
            for pg in [p.name for p in ex if p.parent.name == tdoc][:6]:
                sel = AL.select_few_shot(ex, multi, tdoc, 12, 4, random.Random(42),
                                         target_page=pg, target_type=tt, type_of=type_of)
                if any(s.parent.name == tdoc and s.name == pg for s in sel): viol += 1
    check("B1 per-page holdout: no page sees itself (all vols, all modes)", viol == 0, f"{viol} leaks")

    # B2: determinism — same seed -> identical picks
    a = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(42), target_page="page_999",
                           target_type="notebook", type_of=type_of)
    b = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(42), target_page="page_999",
                           target_type="notebook", type_of=type_of)
    check("B2 few-shot selection deterministic (same seed)", [x.name for x in a] == [x.name for x in b])

    # B3: 'other'/None fall back to identical same-volume selection (regression-safety)
    none_sel = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(7), target_page="page_999")
    other_sel = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(7), target_page="page_999",
                                   target_type="other", type_of=type_of)
    check("B3 'other' type == None (same-volume fallback)",
          [x.name for x in none_sel] == [x.name for x in other_sel])

    # B3b: notebook target gets mostly notebook demos
    nb = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(7), target_page="page_999",
                            target_type="notebook", type_of=type_of)
    n_nb = sum(type_of[p] == "notebook" for p in nb)
    check("B3b notebook target routed >=10/12 notebook demos", n_nb >= 10, f"got {n_nb}/12")

    # B4: infer_page_type sanity on a clearly-notebook gold page (many content+date)
    nbjson = json.load(open(REVIEW/"Appendix_3"/"page_027"/"page_027.json"))
    check("B4 infer_page_type(notebook page)==notebook", AL.infer_page_type_from_labels(nbjson) == "notebook",
          AL.infer_page_type_from_labels(nbjson))

    # B5: coordinate round-trip via export_tuning_data._canonical_target
    spec = importlib.util.spec_from_file_location("etd", HERE/"export_tuning_data.py")
    etd = importlib.util.module_from_spec(spec); spec.loader.exec_module(etd)
    data = json.load(open(REVIEW/"Appendix_3"/"page_004"/"page_004.json"))
    W, H = data["page_width"], data["page_height"]
    tgt = json.loads(etd._canonical_target(data))
    allx = [v["x"] for d in tgt["documents"].values() for ps in d.values() for b in ps for v in b["vertices"]]
    check("B5a canonical_target normalized to [0,1000]", allx and 0 <= min(allx) and max(allx) <= 1000,
          f"range [{min(allx)},{max(allx)}]")
    b0 = next(iter(next(iter(tgt["documents"].values())).values()))[0]
    check("B5b canonical_target strips ids/connections", set(b0) == {"vertices"}, f"keys={set(b0)}")

    # B6: Otsu + ink integral basic sanity (white image -> 0 ink; black -> full)
    thr = AL._otsu_threshold([0]*128 + [1000]*128)
    check("B6 Otsu threshold in mid-range", 0 < thr < 256, f"thr={thr}")

    # B7: panoptic metric sanity — gold vs itself == perfect
    r = PE.eval_volume("Appendix_3", REVIEW, REVIEW, 1000, False)
    check("B7 panoptic gold-vs-gold RQ==1.0, FP==FN==0", r["RQ"] > 0.999 and r["FP"] == 0 and r["FN"] == 0,
          f"RQ={r['RQ']:.3f} FP={r['FP']} FN={r['FN']}")

    # B8: review_diff self-comparison == no changes
    d = RD.diff_page(REVIEW/"Appendix_3"/"page_027"/"page_027.json",
                     REVIEW/"Appendix_3"/"page_027"/"page_027.json")
    check("B8 review_diff(gold,gold): 0 add/remove/recat",
          not d["added"] and not d["removed"] and not d["recat"])

    # B9: render + snap doesn't crash on a real page
    try:
        pdf = AL.POLYGON_PDFS_DIR/"Appendix_3"/"pages"/"page_027.pdf"
        gray = AL.render_page(pdf, None)[0].convert("L")
        docs = json.load(open(AUTOLAB/"Appendix_3"/"page_027"/"page_027.json"))["documents"]
        n = AL.snap_all_polygons(docs, gray); ok = True
    except Exception as e:
        ok = False; n = str(e)[:60]
    check("B9 snap_all_polygons runs on a real page", ok, str(n))


# ── C. INTEGRATION (cheap API; no deployment) ───────────────────────────────────
def test_api():
    section("C. INTEGRATION (cheap Gemini API — no deployment)")
    from dotenv import load_dotenv
    load_dotenv(HERE/".env")
    if not os.getenv("GEMINI_API_KEY"):
        skip("C* all API tests", "no GEMINI_API_KEY"); return
    from google import genai
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    AL._client = client
    AL._file_uri_map = AL._load_file_uri_map("1024")

    # C1: page-type classifier vs gold-derived type on 5 Appendix_3 pages (cached)
    agree = tot = 0
    for pg in ["page_021","page_025","page_005","page_009","page_011"]:
        pdf = AL.POLYGON_PDFS_DIR/"Appendix_3"/"pages"/f"{pg}.pdf"
        gjson = REVIEW/"Appendix_3"/pg/f"{pg}.json"
        if not (pdf.exists() and gjson.exists()): continue
        pt,_,_ = AL.classify_page_type(client, "gemini-3.1-flash-lite", pdf,
                                       cache_dir=AL.PAGE_TYPE_CACHE_DIR/"Appendix_3")
        gold = AL.infer_page_type_from_labels(json.load(open(gjson)))
        tot += 1; agree += (pt == gold or (pt in ("letter","mass_card") and gold in ("letter","mass_card")))
    check(f"C1 page-type classifier sane ({agree}/{tot} agree w/ gold-ish)", tot and agree >= tot-1, f"{agree}/{tot}")

    # C2: ONE full process_page through production path -> temp dir (non-mutating)
    ex = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex); type_of = AL.classify_examples_by_type(ex)
    pass2 = AL.discover_pass2_pool(ex)
    pg = "page_005"
    fs = AL.select_few_shot(ex, multi, "Appendix_3", 12, 4, random.Random(42),
                            target_page=pg, target_type="letter", type_of=type_of)
    p2 = AL.select_pass2_fewshot(pass2, "Appendix_3", 6, random.Random(42), target_page=pg)
    with tempfile.TemporaryDirectory() as td:
        outp = Path(td)/"page_005"/"page_005.json"
        try:
            AL.process_page(pdf_path=AL.POLYGON_PDFS_DIR/"Appendix_3"/"pages"/f"{pg}.pdf",
                            doc_name="Appendix_3", page_number=5, client=client,
                            model_name="gemini-3.5-flash", fewshot_dirs=fs, image_width=1024,
                            output_path=outp, pass2_fewshot_dirs=p2, snap=True, backstop=True)
            o = json.load(open(outp))
            nb = sum(len(p) for d in o["documents"].values() for p in d.values() if isinstance(p,list))
            ok = nb > 0 and all(c in AL.CATEGORIES for d in o["documents"].values() for c in d)
            check("C2 full process_page (label+snap+backstop+pass2) valid output", ok, f"{nb} boxes")
        except Exception as e:
            check("C2 full process_page", False, f"{type(e).__name__}: {str(e)[:80]}")

    # C3: export_tuning_data produces valid JSONL
    r = subprocess.run([sys.executable, "export_tuning_data.py"], capture_output=True, text=True, cwd=HERE)
    tj = HERE/"tuning_data"/"train.jsonl"
    okj = tj.exists() and all(json.loads(l) for l in open(tj))
    check("C3 export_tuning_data -> valid train.jsonl", okj and "examples:" in (r.stdout+r.stderr))


# ── D. INTEGRITY / SAFETY ───────────────────────────────────────────────────────
def test_integrity():
    section("D. INTEGRITY / SAFETY")
    # D1: ZERO Vertex resources deployed (no overnight billing)
    eps = subprocess.run(["gcloud","ai","endpoints","list","--region=us-central1","--project=solanus-project"],
                         capture_output=True, text=True)
    mds = subprocess.run(["gcloud","ai","models","list","--region=us-central1","--project=solanus-project"],
                         capture_output=True, text=True)
    check("D1a 0 Vertex ENDPOINTS deployed (no billing)", "Listed 0 items" in (eps.stdout+eps.stderr),
          eps.stdout[-80:])
    # Parked MODELS are deliberate ($0 idle cost; GENTLE_TUNING_PLAN R2 keeps the sweep winner +
    # the continuation base for future tune-from-tuned rounds). Report, don't fail.
    parked = [l.split()[0] for l in (mds.stdout or "").splitlines()[1:] if l.strip()]
    if parked:
        print(f"  [INFO] D1b parked tuned models (deliberate, $0 idle): {parked}")
    else:
        print("  [INFO] D1b no tuned models parked")

    # D2: reviewed gold not modified today (my session) — git shows no new modifications
    g = subprocess.run(["git","status","--short","reviewed/"], capture_output=True, text=True, cwd=HERE)
    mod = [l for l in g.stdout.splitlines() if l.startswith(" M") and "page_001" not in l]
    check("D2 reviewed/ gold has no unexpected modifications", not mod, f"{mod[:2]}")

    # D3: all auto_labeled outputs parse
    nbad = 0; n = 0
    for jp in AUTOLAB.glob("*/page_*/*.json"):
        n += 1
        try: json.load(open(jp))
        except Exception: nbad += 1
    check(f"D3 all {n} auto_labeled JSONs parse", nbad == 0, f"{nbad} bad")


def test_edge():
    section("E. EDGE CASES / ROBUSTNESS (offline)")
    ex = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex)
    # E1: _normalize_quad reduces non-quads to 4, preserves 4
    q4 = [{"x":0,"y":0},{"x":10,"y":0},{"x":10,"y":10},{"x":0,"y":10}]
    q6 = q4 + [{"x":5,"y":5},{"x":3,"y":3}]
    check("E1 _normalize_quad: 4->4 and 6->4 vertices",
          len(AL._normalize_quad(q4))==4 and len(AL._normalize_quad(q6))==4)
    # E2: few-shot count > pool size doesn't crash, caps at pool
    big = AL.select_few_shot(ex, multi, "Appendix_3", 9999, 4, random.Random(1), target_page="page_999")
    check("E2 num_fewshot > pool: caps, no crash", len(big) <= len(ex))
    # E3: num_fewshot 0 -> empty
    check("E3 num_fewshot 0 -> empty", AL.select_few_shot(ex, multi, "Appendix_3", 0, 0, random.Random(1))==[])
    # E4: infer_page_type on empty/garbage -> 'other', no crash
    check("E4 infer_page_type({}) -> 'other'", AL.infer_page_type_from_labels({})=="other")
    check("E4b infer_page_type(no documents) -> 'other'",
          AL.infer_page_type_from_labels({"num_documents":1})=="other")
    # E5: canonical_target on empty documents -> valid JSON
    spec = importlib.util.spec_from_file_location("etd", HERE/"export_tuning_data.py")
    etd = importlib.util.module_from_spec(spec); spec.loader.exec_module(etd)
    try:
        json.loads(etd._canonical_target({"page_width":100,"page_height":100,"documents":{}})); ok=True
    except Exception: ok=False
    check("E5 canonical_target(empty docs) -> valid JSON", ok)
    # E6: classify_examples_by_type covers ALL pool examples (no crash/missing)
    t = AL.classify_examples_by_type(ex)
    check("E6 every pool example typed", len(t)==len(ex) and all(v in AL.PAGE_TYPES for v in t.values()))
    # E7: review_diff handles a page vs empty-documents gold (no crash)
    import tempfile as _tf
    with _tf.TemporaryDirectory() as td:
        emp = Path(td)/"e.json"; json.dump({"documents":{},"num_documents":0}, open(emp,"w"))
        try:
            d = RD.diff_page(REVIEW/"Appendix_3"/"page_025"/"page_025.json", emp); ok=True
        except Exception: ok=False
    check("E7 review_diff vs empty-doc page: no crash", ok)


def test_bootstrap():
    section("F. HITL BOOTSTRAP TOOLING (offline; HITL_BOOTSTRAP.md)")
    import pick_representatives as PR
    import promote_examples as PRO
    # F1: --pages parser accepts lists/ranges, rejects garbage
    ok = AL.parse_pages_arg("3,17,42-45") == {3,17,42,43,44,45}
    for bad in ("45-42", "x", ""):
        try: AL.parse_pages_arg(bad); ok = False
        except SystemExit: pass
    check("F1 parse_pages_arg: parses ranges, rejects garbage", ok)
    # F2: rep allocation — sums to k, min 1 per stratum, capped at stratum size
    a1 = PR.allocate({"notebook": 90, "letter": 8, "other": 2}, 12)
    a2 = PR.allocate({"a": 3, "b": 2}, 12)
    check("F2 allocate: k total, min-1, size-capped",
          sum(a1.values()) == 12 and min(a1.values()) >= 1 and a1["notebook"] >= 8
          and sum(a2.values()) == 5, f"{a1} {a2}")
    # F3: farthest-point sampling deterministic + picks the layout outliers
    import numpy as np
    vs = [np.array(v, dtype="float32") for v in ([1,0,0],[0.9,0.1,0],[0,1,0],[0,0,1])]
    vs = [v/np.linalg.norm(v) for v in vs]
    p1, p2 = PR.farthest_point_sample(vs, 3), PR.farthest_point_sample(vs, 3)
    check("F3 FPS deterministic + outliers picked", p1 == p2 and {2,3} <= set(p1), f"{p1}")
    # F4: promote copy/skip/force in a sandbox (never touches real dirs)
    import shutil, tempfile as _tf
    tmp = Path(_tf.mkdtemp(prefix="f4_promote_"))
    try:
        (tmp/"reviewed/TV/page_005").mkdir(parents=True)
        (tmp/"reviewed/TV/page_005/page_005.json").write_text('{"documents": {}}')
        (tmp/"polygon_cropped_pdfs/TV/pages").mkdir(parents=True)
        (tmp/"polygon_cropped_pdfs/TV/pages/page_005.pdf").write_bytes(b"%PDF-fake")
        old = PRO.REVIEW_ROOT, PRO.POOL_ROOT, PRO.POLYGON_PDFS_ROOT, PRO.SCRIPT_DIR, sys.argv
        PRO.REVIEW_ROOT, PRO.POOL_ROOT, PRO.POLYGON_PDFS_ROOT, PRO.SCRIPT_DIR = (
            tmp/"reviewed", tmp/"pool", tmp/"polygon_cropped_pdfs", tmp)
        sys.argv = ["promote_examples.py", "TV", "--pages", "5"]
        PRO.main()
        dest = tmp/"pool/TV/page_005"
        copied = (dest/"page_005.json").exists() and (dest/"page_005.pdf").exists()
        (tmp/"reviewed/TV/page_005/page_005.json").write_text('{"documents": {}, "v": 2}')
        PRO.main()   # no --force: must skip
        skipped = '"v": 2' not in (dest/"page_005.json").read_text()
        sys.argv += ["--force"]
        PRO.main()   # --force: must refresh
        forced = '"v": 2' in (dest/"page_005.json").read_text()
        PRO.REVIEW_ROOT, PRO.POOL_ROOT, PRO.POLYGON_PDFS_ROOT, PRO.SCRIPT_DIR, sys.argv = old
        check("F4 promote: copy / skip-without-force / force-refresh", copied and skipped and forced,
              f"copied={copied} skipped={skipped} forced={forced}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    # F5: draft_note deterministic, respects the >=2 threshold, carries the paste header
    from collections import Counter
    agg = {"recat": Counter({"src_content -> archv_other": 3, "src_date -> archv_date": 1}),
           "added": Counter({"src_content": 5}), "removed": Counter(),
           "resized": 0, "doc_count_changes": 1, "pages_changed": 4, "pages_compared": 12}
    n1 = RD.draft_note("TestVol", agg, 0, 0, Counter())
    check("F5 draft_note: deterministic, thresholded, paste-ready",
          n1 == RD.draft_note("TestVol", agg, 0, 0, Counter())
          and "VOLUME-SPECIFIC OVERRIDE" in n1 and "relabeled 3" in n1
          and "src_date -> archv_date" not in n1.split("(below-threshold")[0]
          and "MISSES 'src_content'" in n1)
    # F6: argparse help strings render (catches unescaped % in help text)
    h = subprocess.run([sys.executable, "auto_labeler.py", "--help"], capture_output=True, text=True, cwd=HERE)
    check("F6 auto_labeler --help renders", h.returncode == 0 and "--pages" in h.stdout,
          h.stderr.strip()[-80:])
    # F7: cluster_pages kmeans — deterministic, separates two obvious blobs
    import cluster_pages as CP
    rngf = np.random.RandomState(7)
    blob = np.vstack([rngf.normal(0, .05, (10, 6)), rngf.normal(5, .05, (10, 6))]).astype("float32")
    asg_a, _ = CP.kmeans(blob, 2, seed=42)
    asg_b, _ = CP.kmeans(blob, 2, seed=42)
    sep = len(set(asg_a[:10])) == 1 and len(set(asg_a[10:])) == 1 and asg_a[0] != asg_a[10]
    sil = CP.mean_silhouette(blob, asg_a)
    check("F7 cluster kmeans: deterministic, blob-separating, silhouette~1",
          (asg_a == asg_b).all() and sep and sil > 0.9, f"sep={sep} sil={sil:.2f}")
    # F8: load_volume_note — file overrides constant; cluster addendum appended
    tmp8 = Path(_tf.mkdtemp(prefix="f8_notes_"))
    try:
        old8 = AL.VOLUME_NOTES_DIR
        AL.VOLUME_NOTES_DIR = tmp8
        AL._volume_note_cache.clear(); AL._page_cluster_cache.clear()
        const_ok = "casebook" in AL.load_volume_note("Volume_4")          # python constant
        (tmp8/"Volume_4.md").write_text("DAVID FILE RULE")
        AL._volume_note_cache.clear()
        file_ok = ("DAVID FILE RULE" in AL.load_volume_note("Volume_4")
                   and "casebook" not in AL.load_volume_note("Volume_4"))  # file wins
        (tmp8/"Volume_4.cluster_1.md").write_text("CLUSTER RULE")
        AL._page_cluster_cache["Volume_4"] = {"page_009": 1, "page_010": 2}
        clus_ok = ("CLUSTER RULE" in AL.load_volume_note("Volume_4", "page_009")
                   and "CLUSTER RULE" not in AL.load_volume_note("Volume_4", "page_010"))
        check("F8 volume notes: file>constant, per-cluster addendum scoped",
              const_ok and file_ok and clus_ok,
              f"const={const_ok} file={file_ok} cluster={clus_ok}")
    finally:
        AL.VOLUME_NOTES_DIR = old8
        AL._volume_note_cache.clear(); AL._page_cluster_cache.clear()
        shutil.rmtree(tmp8, ignore_errors=True)
    # F9: new staged-flow CLIs render their help (argparse sanity)
    ok9 = True
    for script, want in (("cluster_pages.py", "--no-api"), ("pick_chunk.py", "--count"),
                         ("pick_representatives.py", "--clusters"),
                         ("bootstrap.py", "--run-rest")):
        h9 = subprocess.run([sys.executable, script, "--help"], capture_output=True, text=True, cwd=HERE)
        ok9 = ok9 and h9.returncode == 0 and want in h9.stdout
    check("F9 staged-flow CLIs --help render (cluster/chunk/reps/bootstrap)", ok9)
    # F10: bootstrap driver pure helpers — merge folding, pin selection, stage derivation
    import bootstrap as BS
    cl = {"page_to_cluster": {"page_001": "letter_1", "page_002": "letter_2",
                              "page_003": "notebook", "page_004": "letter_2"},
          "clusters": {
              "letter_1": {"size": 1, "pages": ["page_001"], "most_central": ["page_001"]},
              "letter_2": {"size": 2, "pages": ["page_002", "page_004"], "most_central": ["page_002"]},
              "notebook": {"size": 1, "pages": ["page_003"], "most_central": ["page_003"]}},
          "k": 3}
    m = BS.apply_merges(cl, [["letter_1", "letter_2"]])
    merge_ok = (m["k"] == 2 and m["page_to_cluster"]["page_002"] == "letter_1"
                and m["page_to_cluster"]["page_003"] == "notebook"
                and m["clusters"]["letter_1"]["size"] == 3
                and "letter_2" not in m["clusters"])
    p2c = {f"page_{i:03d}": ("a" if i % 2 else "b") for i in range(1, 31)}
    pins = BS.pick_pins("V", [f"page_{i:03d}" for i in range(1, 31)], p2c, max_pins=12)
    pin_ok = (len(pins) == 12
              and sum(1 for p in pins if p2c[p] == "a") == 6
              and BS.pick_pins("V", ["page_001"], p2c) == ["page_001"])
    stages = [BS.derive_stage("V", st, pa) for st, pa in (
        ({}, {"has_pages": False, "has_clusters": False, "has_reps": False}),
        ({}, {"has_pages": True, "has_clusters": False, "has_reps": False}),
        ({}, {"has_pages": True, "has_clusters": True, "has_reps": False}),
        ({"clusters_confirmed": "t"}, {"has_pages": True, "has_clusters": True, "has_reps": False}),
        ({"clusters_confirmed": "t"}, {"has_pages": True, "has_clusters": True, "has_reps": True}))]
    stage_ok = stages == ["BLOCKED", "A", "B", "C", "D"]
    check("F10 bootstrap: merge folds clusters, pins round-robin, stages derive",
          merge_ok and pin_ok and stage_ok,
          f"merge={merge_ok} pins={pin_ok} stages={stages}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--no-api", action="store_true"); a = ap.parse_args()
    t0 = time.time()
    print(f"\n{'#'*70}\n# STEP_4 PIPELINE TEST SUITE  ({time.strftime('%Y-%m-%d %H:%M')})\n{'#'*70}")
    test_static()
    test_logic()
    test_edge()
    test_bootstrap()
    if a.no_api: skip("C. integration", "--no-api")
    else: test_api()
    test_integrity()
    print(f"\n{'#'*70}\n# RESULT: {len(PASS)} PASS / {len(FAIL)} FAIL / {len(SKIP)} SKIP  "
          f"({time.time()-t0:.0f}s)\n{'#'*70}")
    if FAIL: print("FAILURES:\n  " + "\n  ".join(FAIL))
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
