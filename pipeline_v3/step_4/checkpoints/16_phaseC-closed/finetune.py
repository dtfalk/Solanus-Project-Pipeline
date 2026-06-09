#!/usr/bin/env python3
"""Vertex AI supervised fine-tuning PLUMBING for the document labeler.

End-to-end: prepare (render gold pages -> GCS, build JSONL) -> tune (submit Vertex
SFT job) -> status (poll) -> eval (held-out, scored) -> teardown (delete tuned model
so NOTHING is left deployed). Built to be run once GCP/Vertex is set up; fails loudly
with setup steps otherwise.

Goal here is to PROVE THE PIPELINE on a small/cheap subset, not to ship a model.

Usage (from step_4/, with ADC + Vertex enabled):
  ./venv/bin/python finetune.py prepare --subset 40 --val 8 --width 768
  ./venv/bin/python finetune.py tune    --base gemini-2.5-flash-lite --epochs 1
  ./venv/bin/python finetune.py status
  ./venv/bin/python finetune.py eval    --volume Appendix_3 --max 6
  ./venv/bin/python finetune.py teardown          # delete the tuned model (don't leave deployed)
"""
from __future__ import annotations
import argparse, json, random, subprocess, sys, tempfile, time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import auto_labeler as AL
from export_tuning_data import _tuning_system_prompt, _canonical_target, _find_pdf

PROJECT  = "solanus-project"
LOCATION = "us-central1"
BUCKET   = "gs://solanus-project-vertex-tuning-389262253193"
STATE    = SCRIPT_DIR / "tuning_data" / "tuning_job.json"   # remembers the job/model name
REVIEW   = SCRIPT_DIR / "reviewed"


def _client():
    from google import genai
    return genai.Client(vertexai=True, project=PROJECT, location=LOCATION)


# HOLDOUT PIN (2026-06-04 directive: "completely held out, no training leakage"):
# Appendix_2 is the held-out TEST volume. TRAINING data (cmd_prepare) is pinned to
# these volumes; eval intentionally still sees everything (it scores against the
# held-out gold once the user has reviewed it).
TRAIN_VOLUMES = ("Appendix_1", "Appendix_3")


def _gold_pages(volumes=None):
    items = []
    for vol in sorted(p.name for p in REVIEW.iterdir() if p.is_dir()):
        if volumes is not None and vol not in volumes:
            continue
        for pd in sorted((REVIEW / vol).glob("page_*")):
            pdf = _find_pdf(vol, pd.name)
            jp = pd / f"{pd.name}.json"
            if pdf and jp.exists():
                data = json.load(open(jp))
                items.append((vol, pd.name, pdf, jp, data,
                              AL.infer_page_type_from_labels(data)))
    return items


# ── prepare ────────────────────────────────────────────────────────────────────
def cmd_prepare(args):
    from PIL import Image
    items = _gold_pages(volumes=TRAIN_VOLUMES)   # holdout pin: never train on the test volume
    # stratify-ish split by page type, then optional subset
    rng = random.Random(42); rng.shuffle(items)
    if args.subset:
        # keep type diversity in the subset
        items = items[: args.subset + args.val]
    n_val = args.val
    val, train = items[:n_val], items[n_val:]
    sysp = _tuning_system_prompt()

    stage = Path(tempfile.mkdtemp(prefix="ft_imgs_"))
    print(f"rendering {len(items)} page images @ {args.width}px to {stage} ...")
    def line_for(vol, name, pdf, data):
        img = AL.render_page(pdf, args.width)[0].convert("RGB")
        local = stage / f"{vol}__{name}.png"
        img.save(local, format="PNG")
        uri = f"{BUCKET}/img/{vol}__{name}.png"
        return uri, {
            "systemInstruction": {"role": "system", "parts": [{"text": sysp}]},
            "contents": [
                {"role": "user", "parts": [
                    {"fileData": {"fileUri": uri, "mimeType": "image/png"}},
                    {"text": "Label this page in the schema."}]},
                {"role": "model", "parts": [{"text": _canonical_target(data)}]},
            ]}
    out = {"train": [], "val": []}
    for split, rows in (("train", train), ("val", val)):
        for vol, name, pdf, jp, data, t in rows:
            _uri, line = line_for(vol, name, pdf, data)
            out[split].append(json.dumps(line))
    (SCRIPT_DIR/"tuning_data").mkdir(exist_ok=True)
    for split in ("train", "val"):
        (SCRIPT_DIR/"tuning_data"/f"vertex_{split}.jsonl").write_text("\n".join(out[split])+"\n")
    print(f"  train={len(out['train'])}  val={len(out['val'])}")
    print("uploading images + jsonl to GCS ...")
    pngs = [str(p) for p in sorted(stage.glob("*.png"))]   # upload FLAT into img/ (no nested temp dir)
    _sh(["gcloud","storage","cp",*pngs,f"{BUCKET}/img/","--project",PROJECT])
    for split in ("train","val"):
        _sh(["gcloud","storage","cp",str(SCRIPT_DIR/'tuning_data'/f'vertex_{split}.jsonl'),
             f"{BUCKET}/{split}.jsonl","--project",PROJECT])
    print(f"DONE. train: {BUCKET}/train.jsonl  val: {BUCKET}/val.jsonl")


# ── tune ───────────────────────────────────────────────────────────────────────
def cmd_tune(args):
    from google.genai import types
    c = _client()
    print(f"submitting SFT job: base={args.base} epochs={args.epochs} "
          f"lr_mult={args.lr_mult} adapter={args.adapter}")
    # v1 lesson: default LoRA on 86 examples learned the ontology but could NOT
    # override the base model's native box_2d detection format — underfit. Stronger
    # adapter + LR are needed for format override on small datasets.
    cfg = dict(
        epoch_count=args.epochs,
        validation_dataset=types.TuningValidationDataset(gcs_uri=f"{BUCKET}/val.jsonl"),
        tuned_model_display_name=args.name,
        export_last_checkpoint_only=args.last_ckpt_only,
    )
    if args.lr_mult:
        cfg["learning_rate_multiplier"] = args.lr_mult
    if args.adapter:
        words = {1: "ONE", 2: "TWO", 4: "FOUR", 8: "EIGHT", 16: "SIXTEEN", 32: "THIRTY_TWO"}
        cfg["adapter_size"] = getattr(types.AdapterSize, f"ADAPTER_SIZE_{words[args.adapter]}")
    job = c.tunings.tune(
        base_model=args.base,
        training_dataset=types.TuningDataset(gcs_uri=f"{BUCKET}/train.jsonl"),
        config=types.CreateTuningJobConfig(**cfg),
    )
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps({"job": job.name, "base": args.base}))
    print("submitted. job:", job.name, "state:", getattr(job,'state',None))
    print("poll with:  ./venv/bin/python finetune.py status")


def cmd_status(args):
    c = _client()
    st = json.loads(STATE.read_text())
    job = c.tunings.get(name=st["job"])
    print("job:", job.name, "| state:", getattr(job,"state",None))
    tm = getattr(job,"tuned_model",None)
    if tm:
        print("tuned_model.model:", getattr(tm,"model",None))
        print("tuned_model.endpoint:", getattr(tm,"endpoint",None))
        st["model"] = getattr(tm,"model",None); st["endpoint"]=getattr(tm,"endpoint",None)
        STATE.write_text(json.dumps(st))
    err = getattr(job,"error",None)
    if err: print("error:", err)


# ── eval ───────────────────────────────────────────────────────────────────────
def cmd_eval(args):
    c = _client()
    st = json.loads(STATE.read_text())
    model = st.get("endpoint") or st.get("model")
    if not model:
        raise SystemExit("no tuned model yet; run status until tuned_model appears")
    sysp = _tuning_system_prompt()
    from google.genai import types
    import review_diff as RD, uuid
    items = [(v,n,pdf,jp,d,t) for (v,n,pdf,jp,d,t) in _gold_pages() if v==args.volume][:args.max]
    print(f"eval tuned model {model} on {len(items)} {args.volume} pages (NO few-shot)")
    ok=0
    for vol,name,pdf,jp,data,t in items:
        img = AL.render_page(pdf, 1024)[0].convert("RGB")
        import io; buf=io.BytesIO(); img.save(buf,format="PNG")
        try:
            r = c.models.generate_content(model=model, contents=[
                types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"),
                "Label this page in the schema."],
                config=types.GenerateContentConfig(system_instruction=sysp, temperature=0.0,
                    response_mime_type="application/json"))
            txt=(r.text or "").strip()
            try:
                parsed=json.loads(txt); nd=parsed.get("num_documents"); ndocs=len(parsed.get("documents",{}))
                print(f"  {name} (type={t}): VALID JSON, num_documents={nd}, docs={ndocs}, {len(txt)} chars"); ok+=1
            except Exception:
                print(f"  {name} (type={t}): NON-JSON output ({len(txt)} chars): {txt[:80]}")
        except Exception as e:
            print(f"  {name}: CALL FAILED {type(e).__name__}: {str(e)[:120]}")
    print(f"tuned-model inference: {ok}/{len(items)} pages returned valid schema JSON "
          f"-> {'PLUMBING OK' if ok else 'NEEDS ENDPOINT (see status)'}")


# ── teardown ─────────────────────────────────────────────────────────────────--
def cmd_teardown(args):
    """Delete the tuned model + serving endpoint so NOTHING is left deployed.
    IMPORTANT: tuned checkpoint endpoints are DEDICATED deployments
    (minReplicaCount=1) that BILL per hour while up — NOT serverless. Correct,
    validated order: undeploy model from endpoint -> delete endpoint -> delete
    model. The genai SDK can't undeploy, so use gcloud ai (authed via ADC)."""
    st = json.loads(STATE.read_text()) if STATE.exists() else {}
    ep, model = st.get("endpoint"), st.get("model")
    ep_id = ep.split("/")[-1] if ep else None
    model_id = model.split("/")[-1].split("@")[0] if model else None
    base = ["--region", LOCATION, "--project", PROJECT]
    if ep_id:
        dmid = subprocess.run(["gcloud", "ai", "endpoints", "describe", ep_id, *base,
                               "--format=value(deployedModels[0].id)"],
                              capture_output=True, text=True).stdout.strip()
        if dmid:
            _sh(["gcloud", "ai", "endpoints", "undeploy-model", ep_id,
                 f"--deployed-model-id={dmid}", *base, "--quiet"]); print("undeployed", dmid)
        _sh(["gcloud", "ai", "endpoints", "delete", ep_id, *base, "--quiet"]); print("deleted endpoint", ep_id)
    if model_id:
        _sh(["gcloud", "ai", "models", "delete", model_id, *base, "--quiet"]); print("deleted model", model_id)
    if not (ep_id or model_id):
        print("nothing recorded to delete")


def _sh(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print("  CMD FAILED:", " ".join(cmd)); print("  ", r.stderr[:300])
    return r.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare"); p.add_argument("--subset",type=int,default=0); p.add_argument("--val",type=int,default=9); p.add_argument("--width",type=int,default=768)
    p = sub.add_parser("tune"); p.add_argument("--base",default="gemini-2.5-flash-lite"); p.add_argument("--epochs",type=int,default=1); p.add_argument("--name",default="solanus-labeler-test")
    p.add_argument("--lr-mult",type=float,default=None); p.add_argument("--adapter",type=int,default=None,choices=[1,2,4,8,16,32]); p.add_argument("--last-ckpt-only",action="store_true")
    sub.add_parser("status")
    p = sub.add_parser("eval"); p.add_argument("--volume",default="Appendix_3"); p.add_argument("--max",type=int,default=6)
    sub.add_parser("teardown")
    args = ap.parse_args()
    {"prepare":cmd_prepare,"tune":cmd_tune,"status":cmd_status,"eval":cmd_eval,"teardown":cmd_teardown}[args.cmd](args)


if __name__ == "__main__":
    main()
