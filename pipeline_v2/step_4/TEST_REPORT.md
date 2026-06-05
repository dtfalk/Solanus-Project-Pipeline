# Full Pipeline Test Report — 2026-06-05 (overnight autonomous pass)

Built `run_all_tests.py` (the project had no tests) — 33 checks across static, logic, edge,
integration, and integrity. **Result: 33 PASS / 0 FAIL.** No Vertex tuning run (nothing deployed,
nothing billing). Two real data issues caught and handled; gold never modified.

## Coverage
- **A. Static/structural (7):** all 28 py files compile · ontology consistent across schema/descriptions/
  DocumentLabels · 176 gold+example JSONs parse · no REAL foreign-category boxes · EXCLUDED all exist ·
  response models validate.
- **B. Logic (11):** **per-page holdout — NO page sees itself across ALL volumes & all selection modes
  (leakage gate)** · few-shot determinism · `other`/None same-volume fallback identical · notebook target
  →≥10/12 notebook demos · page-type inference · canonical_target normalized to [0,1000] + ids/connections
  stripped · Otsu sane · **panoptic gold-vs-gold = RQ 1.0 / FP 0 / FN 0 (metric sanity)** · review_diff
  self-diff = 0 changes · snap runs on a real page.
- **E. Edge cases (8):** _normalize_quad 6→4 · num_fewshot>pool caps · num_fewshot 0 → empty ·
  infer_page_type({})→other · canonical_target(empty)→valid · all pool examples typed · review_diff vs
  empty page no crash.
- **C. Integration — cheap API, NO deployment (3):** page-type classifier 5/5 agree w/ gold · **one full
  `process_page` (label+snap+backstop+pass2) → valid schema output** · export_tuning_data → valid JSONL.
- **D. Integrity/safety (4):** **0 Vertex endpoints / 0 models (no billing)** · reviewed/ gold unmodified ·
  171 auto_labeled JSONs parse.
- **CLI smoke (manual):** review_diff, fair_diff, triage (shadow-model disagreement), qa_report
  (Appendix_3: 6 overlap flags, 0 coverage misses) all run clean.

## Two real issues found + handled
1. **206 empty schema-foreign stamps** (`src_margin_note`/`src_insertion`) in reviewed/ gold — the editor
   (§3.11) writes them on every save. Inert (downstream-stripped, pool clean, model never sees them).
   **Did NOT touch gold** (your rule); shipped opt-in `clean_gold_foreign_keys.py` (dry-run default) for
   you to scrub consciously. Test now flags only REAL foreign boxes.
2. **10 non-quad (L-shaped) `src_content` polygons** in the few-shot pool — violate the prompt's "exactly
   4 vertices" rule. Low impact (production output is always squared by `_normalize_quad`), but they'd
   teach a tuned model to emit non-quads. **Fixed the tuning target** (`_canonical_target` now squares
   non-quads to 4-corner bbox); left the pool as the curator drew it (squaring could create overlaps).

## Not changed (deliberately)
- `reviewed/` gold (your hard rule). The editor's 22-vs-20 category ontology decision is yours.
- No Vertex tune (would deploy a billing endpoint overnight). Fine-tune pipeline already proven (Iter 8-10).

Run it any time: `./venv/bin/python run_all_tests.py` (full) or `--no-api` (offline, ~75s).
