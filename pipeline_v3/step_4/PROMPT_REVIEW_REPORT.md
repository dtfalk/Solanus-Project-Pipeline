# auto_labeler.py Prompt Review & Verification Report

_Generated 2026-05-29. Reviewed all 71 labeled examples (PDF-image + JSON), rewrote both
system prompts + category descriptions, then verified via held-out reproduction testing
over 5 iterative rounds. Nothing committed — review with `git diff auto_labeler.py`._

## Result: alignment 83 → 91 / 100 (locked)

A model following the **final** prompts (image-only, no answer key) reproduces the human
labels at **mean 91/100** across all 71 pages.

| Metric | R1 | R2 | R3 | R4 | **R5 final** |
|---|---|---|---|---|---|
| Mean alignment | 83 | 86 | 89 | 89 | **91** |
| Doc-count match | 68/71 | 68/71 | 68/71 | 68/71 | **69/71** |
| Edge exact-match pages | 13 | 16 | 25 | 19 | **26** |
| Edge mismatch pages | 7 | 3 | 0 | 1 | **0** |
| Spurious edges (total) | 38 | 15 | 13 | 14 | **11** |
| Missing edges (total) | 10 | 30 | 10 | 12 | **13** |

## What changed in `auto_labeler.py` (158 insertions, 71 deletions)

- **`CATEGORY_DESCRIPTIONS`** — all 20 rewritten with disambiguation for every confusable
  pair (archv_date vs src_date, struct_id vs struct_doc, origin vs signature, sender vs
  recipient location, src_other vs other), plus role rules for Mass-cards, datelines,
  closing blessings, ditto marks.
- **`SYSTEM_PROMPT` (Pass 1)** — added: dateline segmentation ("Yonkers, N.Y., Christmas
  Day 1913" → src_location_sender + src_date; holidays/feasts/slash-dates count as dates);
  refined doc-counting (own-signature ⇒ own doc); notebook-vs-prose granularity (one
  src_content per margin-date/Page-N span, not per entry); far-left date-column test;
  multi-line institution-name bundling; archivist-date form rules.
- **`PASS2_SYSTEM_PROMPT` (Pass 2)** — reframed around **page type**: connections occur on
  notebook/journal/ledger pages; formal letters & Mass-cards usually have **zero** edges.
  The real 5-edge scheme, page-marker fan-out (every block in a "Page N" span), ditto =
  no extra edges, and the formal-letterhead/Mass-card carve-outs.
- **`ALLOWED_EDGE_PAIRS` + guard in `apply_edges()`** — code-level safety net that drops any
  edge whose category-pair isn't one of the 5 observed in the corpus.

## The real connection scheme (mined from all 71 JSONs, 305 edges)

Only 5 category-pairs ever occur: `src_content↔struct_doc` (164), `src_content↔src_date`
(131), `archv_other↔struct_doc` (8), `archv_format_note↔src_content` (1), `other↔struct_doc`
(1). Connections are governed by **page type** — notebook/journal/ledger pages carry nearly
all edges; formal letters and Mass-cards usually have none.

## YOUR action item: relabel candidates (not prompt-fixable)

~1/3 of remaining divergences are ground-truth inconsistencies the prompts cannot (and
should not) reproduce. On these pages the model's prediction was rule-correct:

1. **Mass-card greeting/recipient labels are inconsistent** — e.g. `Mr. and Mrs. M.H.
   Bennett and Family` labeled `src_greeting` on some cards (it's the canonical
   `src_recipient` example), oversized greeting boxes overlapping date/sig/recipient.
   (Appendix_1/028, Appendix_1/042, Appendix_3/019, Volume_2/075)
2. **`Blessed be God in all His designs!`** labeled `src_other` (Appendix_1/042) — the
   opposite of the same phrase used as `src_greeting` elsewhere.
3. **Letterhead idiosyncrasies** — `St. Michaels Monastery`→src_content (Volume_2/002);
   `Shonnard Place` kept in src_origin (Appendix_1/001); `225 Jerome Street.` dropped
   entirely (Appendix_1/004).
4. **`archv_possessor` block labeled `archv_other`** (Volume_2/002).
5. **Notebook merges across DISTINCT explicit margin dates** (Volume_4/001 Oct.7+7th,
   Volume_4/007 17+18, Volume_4/165 Dec.17+17) — violates your own one-date-one-block rule.
6. **Continuation-page header merges** — `Page 2`+title merged as archv_other (Volume_1/274);
   recipient running header as `other` (Appendix_3/006); `WANTED` heading as `other`
   producing a degenerate edge (Volume_3/218).
7. **`Feast of the Presentation.`** labeled `struct_doc` and connected (Volume_3/001) — your
   own prompt lists it as a `src_date` example; topology right, edge type wrong.
8. **Single letter split into two docs** at an in-body time-stamp, leaving a signature-less
   doc_1 (Appendix_1/006).
9. **Same-page contradictions** — `-- sinner.` folded into signature while `Praised be
   Jesus!` peeled to src_other on the same page (Appendix_2/020).
10. **Stray role-neutral `other` fragments** (Appendix_3/019, Volume_1/244, Volume_1/245).

Lowest-alignment residual pages: Volume_1/245 (48), Appendix_2/006 (52), Appendix_1/006 (58),
Appendix_2/070 (58), Appendix_1/028 (62), Volume_2/165 (62).

## Remaining genuinely-fixable (deferred — diminishing returns, deferred to avoid over-fit)

The final synthesis noted a few low-frequency prompt-fixable clusters left on the table:
consolidating the Mass-card role-split into one coherent block; retreat/outline pages needing
"both edges" (section-marker AND per-day date fan-out); centered piece-title (struct_id) vs
folded-in heading. These were deferred because (a) gains are marginal at 91/100, (b) Mass-card
GT is itself internally contradictory, and (c) two earlier medium-confidence fixes in this
class overcorrected and had to be reverted.

## Not yet done: live end-to-end test

This whole report measures **prompt fidelity** via a Claude stand-in (zero-shot, image-only) —
a strong proxy. The only untested layer is the **actual Gemini pipeline** (few-shot + real
coordinate handling). Run a small batch to confirm end-to-end:

```
python auto_labeler.py --volume Volume_1 --overwrite --dry-run 5
```

(needs `GEMINI_API_KEY` in `.env`; costs a small amount). Then diff outputs vs labeled_examples.
