#!/usr/bin/env python3
"""GENTLE_TUNING_PLAN Phase-1 sweep helpers.

  infer   --arm sweep-G1 --endpoint <ep-or-state>   bare-prompt tuned inference on the
          fixed cold-20 stratified A2 subset -> rerun_compare/ab/<arm>/Appendix_2/
  valpick --label G1                                 score EVERY checkpoint endpoint of the
          current job on a fixed 8-page val subset (A1/A3 gold) -> prints PQ_strict per
          checkpoint so the best pre-overfit checkpoint can be chosen when Vertex
          doesn't expose eval_total_loss via the SDK.

Score an arm afterwards:
  ./venv/bin/python panoptic_eval.py Appendix_2 --pred rerun_compare/ab/<arm> \
      --csv experiments/gate1_results.csv --arm <arm> --notes "..."
"""
from __future__ import annotations
import argparse, importlib.util, io, json, sys, tempfile, shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
import auto_labeler as AL                                     # noqa: E402

AB = ROOT / "rerun_compare" / "ab"
COLD20 = HERE / "cold20_pages.json"
STATE = ROOT / "tuning_data" / "tuning_job.json"


def _etd():
    spec = importlib.util.spec_from_file_location("etd", ROOT / "export_tuning_data.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


def _client():
    from google import genai
    return genai.Client(vertexai=True, project="solanus-project", location="us-central1")


def _label_one(c, endpoint, sysp, vol, name, outroot):
    """Bare-prompt tuned inference; [0,1000] -> source-pixel scaling (run_arms convention)."""
    from google.genai import types
    pdf = AL.POLYGON_PDFS_DIR / vol / "pages" / f"{name}.pdf"
    img, _rw, _rh, sw, sh = AL.render_page(pdf, 768)
    buf = io.BytesIO(); img.convert("RGB").save(buf, format="PNG")
    r = c.models.generate_content(model=endpoint, contents=[
        types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"),
        "Label this page in the schema."],
        config=types.GenerateContentConfig(system_instruction=sysp, temperature=0.0,
                                           response_mime_type="application/json"))
    parsed = json.loads((r.text or "").strip())
    docs = {}
    for dname, cats in (parsed.get("documents") or {}).items():
        dd = {}
        for cat, boxes in (cats or {}).items():
            bs = []
            for b in boxes or []:
                vs = [{"x": round(v["x"] / 1000 * sw), "y": round(v["y"] / 1000 * sh)}
                      for v in b.get("vertices", [])]
                if vs:
                    bs.append({"id": "", "vertices": vs, "connections": []})
            if bs:
                dd[cat] = bs
        if dd:
            docs[dname] = dd
    out = {"page_number": int(name.split("_")[1]), "page_width": sw, "page_height": sh,
           "num_documents": parsed.get("num_documents", len(docs)), "documents": docs}
    outp = outroot / name / f"{name}.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, open(outp, "w"), indent=2)
    return sum(len(v) for d in docs.values() for v in d.values())


def cmd_infer(args):
    sysp = _etd()._tuning_system_prompt()
    c = _client()
    ep = args.endpoint
    if ep in (None, "state"):
        ep = json.loads(STATE.read_text()).get("endpoint")
    pages = json.load(open(COLD20))
    outroot = AB / args.arm / "Appendix_2"
    print(f"[{args.arm}] {len(pages)} cold A2 pages, bare prompt, endpoint={ep}")
    ok = fail = 0
    for name in pages:
        if (outroot / name / f"{name}.json").exists():
            ok += 1; continue
        try:
            nb = _label_one(c, ep, sysp, "Appendix_2", name, outroot)
            ok += 1; print(f"  {name}: ok ({nb} boxes) [{ok}/{len(pages)}]", flush=True)
        except Exception as e:
            fail += 1; print(f"  {name}: FAIL {type(e).__name__} {str(e)[:90]}", flush=True)
    print(f"[{args.arm}] done: {ok} ok, {fail} fail")


# fixed 8-page val subset for checkpoint picking (stratified-ish over the 17-val list)
def _val_subset(n=8):
    rows = []
    for line in open(ROOT / "tuning_data" / "vertex_val.jsonl"):
        d = json.loads(line)
        for part in d["contents"][0]["parts"]:
            if "fileData" in part:
                stem = part["fileData"]["fileUri"].split("/")[-1].removesuffix(".png")
                vol, page = stem.split("__")
                rows.append((vol, page))
    step = max(1, len(rows) // n)
    return rows[::step][:n]


def cmd_valpick(args):
    """Score every checkpoint endpoint of the CURRENT job on the fixed val subset.
    Prints combined PQ_strict per checkpoint (relative comparison -> pick best)."""
    spec = importlib.util.spec_from_file_location("pe", ROOT / "panoptic_eval.py")
    pe = importlib.util.module_from_spec(spec); spec.loader.exec_module(pe)
    sysp = _etd()._tuning_system_prompt()
    c = _client()
    st = json.loads(STATE.read_text())
    job = c.tunings.get(name=st["job"])
    cps = (getattr(job.tuned_model, "checkpoints", None) or []) if job.tuned_model else []
    if not cps:   # single endpoint, no per-epoch checkpoints
        cps = [type("X", (), {"checkpoint_id": "final", "endpoint": st.get("endpoint")})]
    subset = _val_subset()
    print(f"valpick[{args.label}]: {len(cps)} checkpoint(s) x {len(subset)} val pages: {subset}")
    results = []
    for cp in cps:
        ep = getattr(cp, "endpoint", None)
        cid = getattr(cp, "checkpoint_id", "?")
        if not ep:
            continue
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            okp = 0
            for vol, name in subset:
                try:
                    _label_one(c, ep, sysp, vol, name, tdp / "pred" / vol)
                    g = tdp / "gold" / vol / name
                    g.mkdir(parents=True, exist_ok=True)
                    shutil.copy(ROOT / "reviewed" / vol / name / f"{name}.json", g / f"{name}.json")
                    okp += 1
                except Exception as e:
                    print(f"   ckpt {cid} {vol}/{name}: {type(e).__name__} {str(e)[:70]}", flush=True)
            agg = {"TP": 0, "FP": 0, "FN": 0, "sTP": 0, "sFP": 0, "sFN": 0, "ssq": 0.0}
            for vol in {v for v, _ in subset}:
                if not (tdp / "pred" / vol).exists():
                    continue
                r = pe.eval_volume(vol, tdp / "pred" / vol, tdp / "gold", 800, True)
                for k in ("TP", "FP", "FN", "sTP", "sFP", "sFN"):
                    agg[k] += r[k]
                agg["ssq"] += r["SQ_strict"] * r["sTP"]
            srq = agg["sTP"] / (agg["sTP"] + 0.5 * agg["sFP"] + 0.5 * agg["sFN"]) if (agg["sTP"] + agg["sFP"] + agg["sFN"]) else 0
            ssq = agg["ssq"] / agg["sTP"] if agg["sTP"] else 0
            print(f"  ckpt {cid}: val PQ_strict={srq*ssq:.3f} (RQs={srq:.3f} SQs={ssq:.3f}) "
                  f"pages_ok={okp}/{len(subset)} endpoint={ep.split('/')[-1]}", flush=True)
            results.append((srq * ssq, cid, ep))
    if results:
        best = max(results)
        print(f"BEST checkpoint: id={best[1]} PQ_strict={best[0]:.3f}\n  endpoint={best[2]}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("infer"); p.add_argument("--arm", required=True); p.add_argument("--endpoint", default="state")
    p = sub.add_parser("valpick"); p.add_argument("--label", default="?")
    a = ap.parse_args()
    {"infer": cmd_infer, "valpick": cmd_valpick}[a.cmd](a)


if __name__ == "__main__":
    main()
