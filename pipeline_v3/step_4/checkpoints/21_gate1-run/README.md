# experiments/ — GATE-1 results and arm definitions

All rows in `gate1_results.csv` are produced by `panoptic_eval.py --csv` (ink-IoU Panoptic
Quality; PQ + category-strict PQ per row). The held-out test set is **Appendix_2 (76 pages of
fresh human gold)** — pinned out of all training (finetune.py TRAIN_VOLUMES) and now scored
against every arm. Training data = Appendix_1 + Appendix_3 (98 gold pages, 89 train / 9 val).

## Arms

| arm | weights | demos | scaffolding | produced by |
|---|---|---|---|---|
| `fewshot-3.5-flash+pt (production)` | vanilla 3.5-flash | 12, page-type routed | snap+backstop+pass2 | `auto_labeler.py` (the production run) |
| `base-bare` | vanilla 3.5-flash | 0 | snap+backstop | `experiments/run_arms.py base-bare` |
| `tuned-bare` | SFT flash-lite (8ep/LR×5/adapter8 on 89 pages) | 0 | RAW (no snap/backstop) | `rerun_compare/tuned_eval.py val` + `a2` |
| `tuned-fs` | same SFT | 12 inline, page-type routed | RAW | `experiments/run_arms.py tuned-fs` (subset) |
| `promptfix-archv` | vanilla 3.5-flash | 12 (OLD drifted pool) | full | prompt amendment only — 6-page ablation |
| `promptfix2-pool` | vanilla 3.5-flash | 12 (gold-synced pool) | full | prompt amendment + pool sync — 6-page ablation |

## 2026-06-06 prompt/pool fix (the src_content→archv_commentary confusion)

Appendix_2 review surfaced 53 recategorizations, 39 of them archivist catalog/index
descriptions mislabeled `src_content`. Root causes found:
1. The OLD prompt **explicitly instructed** the wrong convention — `archv_other`'s scope-limit
   rule said contents/index page descriptions are `src_content`. Fixed to `archv_commentary`
   (+ connections to `struct_doc`, matching the fresh gold).
2. All 10 Appendix_2 few-shot pool entries had **drifted from the new gold** (they predate the
   review) — the demos were teaching the old convention. Pool synced to gold.
The 6-page ablation (worst recat pages) isolates: production → prompt-only → prompt+pool.
Note: prompt-only round used inline demo fallback (uploads expired) — same demo content, minor
condition difference vs production (machine-A uploads).
