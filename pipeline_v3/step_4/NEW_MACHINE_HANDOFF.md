# New-Machine Handoff — resume the Solanus labeler (2026-06-05)

Everything is committed + pushed (`origin/master = 689fa03`). `git pull` on the new machine gets all
code, docs, gold, examples, checkpoints, and the tuning JSONL. Two things do NOT travel via git and
must be recreated locally: **`venv/`** and **`.env`**.

## 1. Set up the new machine (from `pipeline_v2/step_4/`)
```bash
# a) Python env (41 deps). Needs system 'poppler' for pdf2image (apt install poppler-utils).
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
# b) API key for labeling (AI Studio):
echo 'GEMINI_API_KEY=<your key>' > .env
# c) ONLY for fine-tuning (Vertex): auth + project (Vertex API already enabled cloud-side):
gcloud auth application-default login           # project: solanus-project
# d) Verify the whole pipeline is healthy:
./venv/bin/python run_all_tests.py --no-api      # expect 30 PASS / 0 FAIL (~75s)
```
Cloud state (shared across machines, checked 2026-06-05): **0 Vertex endpoints/models (nothing
billing)**; GCS bucket `gs://solanus-project-vertex-tuning-389262253193/` holds the tuning dataset.

## 2. What changed on this device's session (all in commit 689fa03)
Closed the interrupted Phase-C fine-tune, ran a teardown-lifecycle test, and built the test suite:
- **Fine-tune v2 evaluated + torn down** — v2 (8ep/LR×5/adapter8) FIXED the output format (emits our
  schema natively) but labels were POOR at 95 gold pages (**held-out panoptic PQ 0.159** vs
  few-shot-3.5-flash **0.940**) → confirms GATE-1 needs ~150–250+ pages. Caught a live billing endpoint
  and killed it. **0 deployed now.**
- **Cost-lifecycle mapped** — only deployed ENDPOINTS bill (dedicated minReplica=1, NOT serverless);
  managed Gemini tuned models can't be manually redeployed → the "restore" path is RE-TUNE. Added
  `finetune.py park`/`redeploy` + fixed `teardown` (gcloud undeploy order).
- **Test suite built** (`run_all_tests.py`, 33 checks, all green): leakage/holdout gate, determinism,
  page-type routing, panoptic metric sanity, full `process_page`, integrity (0 GCP / gold pristine).
  Caught 2 real data issues → `clean_gold_foreign_keys.py` (opt-in, gold untouched) + tuning-target
  coordinate normalization (squaring reverted per your call — L-shapes preserved).
- `reviewed/` gold NEVER modified this session.

## 3. Files to review (priority order)
**Docs (read first):**
1. `MASTER_GUIDE.md` — the big-picture data-flywheel framework (3-gear model, measurement, fine-tune loop, cost, reproducibility).
2. `RESUME_HERE.md` — current state + the GATE-1 command sequence + next steps.
3. `RESEARCH_AND_PLAN.md` — error-ID redesign, clustering verdict, fine-tune roadmap, error-reduction menu.
4. `DEEP_REVIEW.md` — the model/page-type A/B evidence (−77% hard pages; PQ 0.874→0.940).
5. `TEST_REPORT.md` — the test pass + 2 issues handled.
6. `LABEL_REVIEW.md` (Iter 7–11) — the per-iteration log.
**Code:**
- `auto_labeler.py` — page-type classifier + routing; defaults `--model gemini-3.5-flash --page-type-fewshot`.
- `panoptic_eval.py` — the correct metric (ink-IoU + Panoptic Quality).
- `finetune.py` — Vertex SFT pipeline (prepare/tune/status/eval/park/redeploy/teardown).
- `run_all_tests.py` — the test suite · `triage.py` — gold-free review triage · `tuned_eval.py` — tuned-model eval.
- `clean_gold_foreign_keys.py` — opt-in gold scrubber (dry-run default).

## 4. Where we left off / next steps
1. **⭐ Review Appendix_2** (your call — the critical path). 76 pages auto-labeled with the new defaults;
   triage worklist at `qa_output/Appendix_2/triage.txt` (high-disagreement first).
   `EDITOR_DOCUMENT=Appendix_2 ./venv/bin/python normalized_editor.py`
   Key question: does it take far LESS than the Appendix_3 "painful hour"? That's the success measure.
   It also takes the corpus to ~171 gold pages → unblocks the fine-tune gate.
2. **GATE-1 fine-tune A/B** (after #1, agent can run): re-`prepare` → `tune --epochs 8 --lr-mult 5
   --adapter 8 --last-ckpt-only` → `tuned_eval` → panoptic A/B (tuned-flash-lite no-few-shot vs
   few-shot-3.5-flash) → `teardown`.
3. **Promote** corrected A2 pages into the few-shot pool (esp. mass_card — only 4).
4. **Scale** to the big notebook volumes (Volume_1/3/4) with the proven defaults.
5. Optional cost levers: fixed per-type few-shot (implicit caching ~90% off) + Batch API (50% off).

Current labeler defaults are already correct — labeling is just `./venv/bin/python auto_labeler.py --volume <V>`.
