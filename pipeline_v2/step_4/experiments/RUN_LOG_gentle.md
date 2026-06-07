# GENTLE_TUNING_PLAN run log — started 2026-06-06 18:11
- Machine: david-desktop at /Workspace (NOT the desktop): B7 gold-vs-gold FAILS on Appendix_3
  (RQ 0.720, 226 unscoreable boxes — stale A3 crop vintage, anticipated by Phase 0).
  => A3-side val scoring carries a uniform negative bias on this machine (relative
  comparisons still valid). Checking A2 gold-vs-gold below; A2 is the scoring volume.
- Endpoints at start: 0. Budget cap $40 ($15/job abort). Spend ledger below.
- ANOMALY + REPAIR (Phase 0): this machine's local polygon_cropped_pdfs were a stale crop
  vintage — A2 gold-vs-gold RQ 0.709 (275 dead boxes; 57/76 pages affected), A3 likewise (B7 fail).
  Diagnosis: PDFs are untracked/per-machine; dead regions blank under both pdftoppm+pdftocairo;
  no annotations — the local file CONTENT differs. REPAIR: swapped in the git-tracked
  desktop-vintage page PDFs from auto_labeled/<vol>/<page>/ for ALL A1/A2/A3 pages (stale copies
  backed up to polygon_cropped_pdfs/<vol>/pages_stale_local_backup/). RESULT: A2 gold-vs-gold
  RQ=1.000 FP=0 FN=0 -> eval/prepare/inference on this machine now desktop-comparable.
- G1 (job 8071995878660898816) SUCCEEDED. Google DEFAULT epochs resolved to **40** (ckpts at
  epochs 5,9,...,40; 138 steps). billable tokens/pass=820,347 -> ~32.8M training tokens.
  COST ESTIMATE: ~$8-12 by this project's prior-job empirical rate; ~$33 if 2.5-lite bills at the
  published 2.0-lite $1/1M (2.5 tuning prices unpublished, R3). DECISION: proceed, but ALL
  remaining jobs get explicit epoch caps (G2 ->6 epochs to stay in budget; no default-epoch jobs).
  G1's 10-ckpt ladder doubles as the epoch/over-tuning sweep at default-LR/adapter-4.
- 10 checkpoint endpoints live post-job (inherent to keeping checkpoints); valpick now, then
  delete ALL (R1 window minimized).
- G1 RESULTS: val curve over epoch ladder (5,9,...,40): 0.000/0.506/0.572/0.573/0.604/0.748/
  0.789/0.792/0.793/0.783 -> rises to epoch ~37, slight dip at 40. BEST ckpt9(epoch37) val
  PQ_strict 0.793. Cold-20 scored as arm sweep-G1 (19/20 pages; 1 inference fail). All 10
  endpoints deleted post-eval. G1 MODEL parked pending end-of-phase keep-one rule.
- DEVIATION (budget, recorded per plan): default-epochs G2 would re-run ~40 epochs (~32.8M tok).
  G1's ladder already covers the epoch axis at LR=1/adapter4. G2 REDEFINED as the adapter ablation
  at the affordable mid-pole: epochs 6, LR 2, ADAPTER 8 (pairs exactly with G3 = 6/2/4). G4
  unchanged (8/5/8 control).
- PHASE 1 RESULTS (cold-20 A2, PQ_strict): sweep-G1 0.669 (PQ 0.782) | sweep-G4 0.362 |
  sweep-G2 0.329 | sweep-G3 0.313. Val ladders: G1 peaks epoch37 (0.793, dip at 40);
  G4 best=final (0.655); G2/G3 ~0.44 (6 epochs insufficient; epochs 1-4 can't even emit format).
  DECISION RULE: G1 >= G4 -> OVER-TUNING HYPOTHESIS CONFIRMED. Nuance: "gentle" wins via default
  LR + adapter4 + MANY epochs (Google default chose 40) + val-selected checkpoint (37) — not via
  less training. Adapter ablation (G2 vs G3 at 6/2): negligible (0.435 vs 0.453).
  No 0.55-plateau -> headroom exists. Kept ONLY G1 model (902444509462265856); G2/G3/G4 models
  deleted; 0 endpoints.
- PHASE 2: SKIPPED per R3. G1's curve shows flash would need ~30-40 epochs to be useful;
  at published 2.0-flash tuning rate that's ~$98 (even empirical-rate ~$35) >> $15/job abort line.
  Recorded, not run.
- Budget ledger (empirical-rate estimates; 2.5 prices unpublished): G1 ~$11.5 (32.8M tok),
  G2 ~$1.7 (4.9M), G3 ~$1.7 (4.9M), G4 ~$2.3 (6.6M) -> ~ $17-20 phase total. Worst-case at
  published 2.0-lite $1/1M: ~$50 (flagged; actuals unverifiable from CLI — David: check console).
- PHASE 3 (continuous tuning, tuned-cont-r1): from G1@ckpt9(ep37), +epochs on an 85-line targeted
  top-up (43 struct_doc/archv_commentary pages + David's 21 consistency-fix pages x3; val pages
  excluded; one bad-data submission cancelled+resubmitted after an oversample-parsing bug).
  Val ladder: +1ep 0.838 / +2ep 0.812 / +3ep 0.796 -> overfit onset right after +1; val-selection
  took ckpt1. COLD-20: PQ 0.807 / PQ_strict 0.692 vs base sweep-G1 0.782/0.669 -> +0.023 strict:
  REAL improvement, narrowly below the +0.03 success bar (PARTIAL). Loop mechanism proven.
  Distillation booster SKIPPED (budget conservatism). Cost ~$1-2.
- PHASE 4 (adoption rung 1): Spearman vs David's ACTUAL per-page edits on cold-20:
  tuned shadow 0.739 > flash-lite shadow 0.644 -> GATE PASSED. Shipped gated triage.py routes
  (--shadow-pred / --shadow-tuned-endpoint / --out-name; defaults unchanged); demo worklist
  qa_output/Appendix_2/triage_tuned20.txt. Production labeling untouched (R5).
- CLEANUP: orphan model from the cancelled job deleted. End state: 0 endpoints;
  parked models = solanus-gentle-G1 (902444509462265856; sweep winner / reproducible baseline)
  and solanus-cont-r1 (899066809741737984; CURRENT BEST, future continuation base, ckpt1).
- POST-PLAN (David directive): promoted ALL Appendix_2 gold into the few-shot pool (65 new; pool
  73 -> 138; notebook 41 -> 55). Copies stripped of empty editor stamps; reviewed/ untouched;
  TUNING holdout unaffected (prepare reads reviewed/ with the A1+A3 pin). EVAL-HYGIENE NOTE:
  future FEW-SHOT-arm evals on A2 are no longer cold (A2 siblings now in the pool; per-page
  holdout still prevents self-leakage) — use the checkpointed pre-promotion pool
  (checkpoints/15_pre-integrations/labeled_examples) for any future few-shot-on-A2 comparison.
  Tuned-model A2 evals unaffected (bare prompt, no demos). Re-uploaded 138 URIs.

## 2026-06-07 — Iter 14 session (geometry + continuation r2)
- GEOMETRY (primary win, no API cost): geom_diagnose.py over 2774 boxes -> over-coverage 6.5% >>
  right-clip 0.8%. snap_polygon_to_ink rewritten to per-line horizontal extent (margin_frac_h 0.45)
  + vertical unchanged. snap_lab.py harness (976 boxes): clip 13.0->8.4px (-35%), over 28.7->27.5,
  under 6.4->8.0 (safe). Real re-snap: 10/22 clips recovered (-45%). Shipped to production snap.
- Research (deep-research, verified): per-line/RLSA fixes clipping; whitespace-rect fencing + adaptive
  gaps for over-coverage; MANY-SHOT ICL helps classification but NOT bbox -> demo-count scaling won't
  fix geometry. Saved label_review/snap_geometry_research_2026-06-07.json.
- V1 (273pg) promoted to pool -> 396 examples. page_148 excluded everywhere. A3 vintage repaired.
- CONTINUATION r2: prepare 408 train / 20 val (A1+A2+A3+V1, cold-20 A2 + page_148 held out);
  tune --from-model cont-r1 --from-ckpt 1 --epochs 2. SUCCEEDED, 3 ckpt endpoints.
  Cold-20 A2: ckpt1 PQ 0.630/strict 0.543 > ckpt3 0.578/0.461. BOTH < cont-r1 0.807/0.692 -> REGRESS
  (distribution shift to V1 + step too large). r2 model + all 3 endpoints deleted. cont-r1 stays best.
- END STATE: 0 endpoints; parked solanus-cont-r1 (best) + solanus-gentle-G1. No labeling API spent
  (geometry all local). Tune+eval cost ~$8-12 est (2.5 rates unpublished).
