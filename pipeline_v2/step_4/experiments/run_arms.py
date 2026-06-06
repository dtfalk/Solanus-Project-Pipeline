#!/usr/bin/env python3
"""GATE-1 experiment arms not covered by existing tools.

Arms:
  base-bare   gemini-3.5-flash, ZERO few-shot demos, full production scaffolding
              (snap + backstop), on Appendix_2 -> rerun_compare/ab/base-bare/Appendix_2/
  tuned-fs    tuned endpoint + N inline same-type demos (training format preserved:
              system prompt + parts), on a page subset -> rerun_compare/ab/tuned-fs/Appendix_2/

Score any arm afterwards with:
  ./venv/bin/python panoptic_eval.py Appendix_2 --pred rerun_compare/ab/<arm> --csv experiments/gate1_results.csv --arm <arm>
"""
from __future__ import annotations
import argparse, concurrent.futures, io, json, random, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
import auto_labeler as AL                                    # noqa: E402

AB = ROOT / "rerun_compare" / "ab"


def _gold_a2_pages(max_n=None):
    pages = sorted((ROOT / "reviewed" / "Appendix_2").glob("page_*"))
    if max_n:
        # stratified-ish: spread evenly across the volume instead of taking the head
        step = max(1, len(pages) // max_n)
        pages = pages[::step][:max_n]
    return [p.name for p in pages]


def cmd_base_bare(args):
    from dotenv import load_dotenv
    import os
    load_dotenv(ROOT / ".env")
    client = __import__("google.genai", fromlist=["genai"]).Client(api_key=os.getenv("GEMINI_API_KEY"))
    pages = _gold_a2_pages(args.max)
    outroot = AB / "base-bare" / "Appendix_2"
    print(f"base-bare: {len(pages)} Appendix_2 pages, 0 demos, snap+backstop ON")

    def one(name):
        num = int(name.split("_")[1])
        outp = outroot / name / f"{name}.json"
        if outp.exists():
            return name, "skip"
        AL.process_page(
            pdf_path=AL.POLYGON_PDFS_DIR / "Appendix_2" / "pages" / f"{name}.pdf",
            doc_name="Appendix_2", page_number=num, client=client,
            model_name="gemini-3.5-flash", fewshot_dirs=[], image_width=1024,
            output_path=outp, pass2_fewshot_dirs=None, snap=True, backstop=True)
        return name, "ok"

    done = fail = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {}
        for n in pages:
            futs[ex.submit(one, n)] = n
            time.sleep(0.5)
        for f in concurrent.futures.as_completed(futs):
            try:
                name, st = f.result(); done += 1
                print(f"  {name}: {st} ({done}/{len(pages)})")
            except Exception as e:
                fail += 1
                print(f"  {futs[f]}: FAIL {type(e).__name__}: {str(e)[:100]}")
    print(f"base-bare done: {done} ok, {fail} failed")


def cmd_tuned_fs(args):
    """Tuned endpoint + inline same-type demos (image + normalized-target text pairs)."""
    import importlib.util
    from google import genai
    from google.genai import types
    spec = importlib.util.spec_from_file_location("etd", ROOT / "export_tuning_data.py")
    etd = importlib.util.module_from_spec(spec); spec.loader.exec_module(etd)
    sysp = etd._tuning_system_prompt()
    state = json.loads((ROOT / "tuning_data" / "tuning_job.json").read_text())
    model = state.get("endpoint") or state.get("model")
    if not model:
        raise SystemExit("no tuned endpoint in tuning_data/tuning_job.json")
    c = genai.Client(vertexai=True, project="solanus-project", location="us-central1")

    ex_all = AL._discover_labeled_examples()
    multi = AL._classify_examples_by_num_docs(ex_all)
    type_of = AL.classify_examples_by_type(ex_all)
    rng = random.Random(42)
    pages = _gold_a2_pages(args.max)
    outroot = AB / "tuned-fs" / "Appendix_2"
    print(f"tuned-fs: {len(pages)} pages, {args.demos} inline demos each, endpoint={model}")

    def demo_parts(dirs):
        parts = []
        for d in dirs:
            data = json.load(open(d / f"{d.name}.json"))
            pdf = next(d.glob("*.pdf"), None)
            if pdf is None:
                continue
            img = AL.render_page(pdf, 768)[0].convert("RGB")
            buf = io.BytesIO(); img.save(buf, format="PNG")
            parts.append(types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"))
            parts.append(etd._canonical_target(data))
        return parts

    ok = fail = 0
    for name in pages:
        outp = outroot / name / f"{name}.json"
        if outp.exists():
            ok += 1; continue
        try:
            pdf = AL.POLYGON_PDFS_DIR / "Appendix_2" / "pages" / f"{name}.pdf"
            ptype = AL.infer_page_type_from_labels(
                json.load(open(ROOT / "reviewed" / "Appendix_2" / name / f"{name}.json")))
            fs = AL.select_few_shot(ex_all, multi, "Appendix_2", args.demos, 2, rng,
                                    target_page=name, target_type=ptype, type_of=type_of)
            img, _rw, _rh, sw, sh = AL.render_page(pdf, 768)
            buf = io.BytesIO(); img.convert("RGB").save(buf, format="PNG")
            contents = demo_parts(fs) + [
                types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"),
                "Label this page in the schema."]
            r = c.models.generate_content(model=model, contents=contents,
                config=types.GenerateContentConfig(system_instruction=sysp, temperature=0.0,
                                                   response_mime_type="application/json"))
            parsed = json.loads((r.text or "").strip())
            # scale [0,1000] -> source pixels (same convention as tuned_eval)
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
            outp.parent.mkdir(parents=True, exist_ok=True)
            json.dump(out, open(outp, "w"), indent=2)
            ok += 1
            print(f"  {name}: ok ({ok}/{len(pages)})")
        except Exception as e:
            fail += 1
            print(f"  {name}: FAIL {type(e).__name__}: {str(e)[:100]}")
    print(f"tuned-fs done: {ok} ok, {fail} failed")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("base-bare"); p.add_argument("--max", type=int, default=None)
    p = sub.add_parser("tuned-fs"); p.add_argument("--max", type=int, default=20)
    p.add_argument("--demos", type=int, default=12)
    a = ap.parse_args()
    {"base-bare": cmd_base_bare, "tuned-fs": cmd_tuned_fs}[a.cmd](a)


if __name__ == "__main__":
    main()
