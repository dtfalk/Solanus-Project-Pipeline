# pipeline_v3 — HITL bootstrap development copy

Created 2026-06-09 as a full `cp -a` of `pipeline_v2` so that new human-in-the-loop bootstrap
tooling can be developed WITHOUT interfering with ongoing pipeline_v2 processes.

**Provenance caveats of this snapshot:**
- At copy time, a live `auto_labeler.py --volume Volume_4 --start 6 --end 272 --overwrite`
  run was writing into `pipeline_v2/step_4/auto_labeled/Volume_4/` (and appending `usage.csv`).
  This copy's `auto_labeled/Volume_4/` is therefore a MID-RUN snapshot of mixed vintage.
  **CUTOVER 2026-06-09 (evening): pipeline_v3 is now canonical** — all data + tool fixes synced; v2 frozen. (Historical text below predates cutover:) pipeline_v2 was canonical for data artifacts (gold `reviewed/`, `auto_labeled/`,
  `usage.csv`, `file_uris.json`) until an explicit cutover decision.
- `reviewed/` gold here matches git commit `01cbcc54` (save-state push, 2026-06-09).
- The venv was copied, not rebuilt: `./venv/bin/python <script>` works, but the venv's own
  entry-point shebangs (e.g. `venv/bin/pip`) still point at pipeline_v2 — use
  `./venv/bin/python -m pip` inside v3 if you ever need pip here.

**What's NEW in v3 (vs v2):** the per-volume HITL bootstrap tooling — see
`step_4/HITL_BOOTSTRAP.md` (design + protocol), `step_4/pick_representatives.py`,
`step_4/promote_examples.py`, `auto_labeler.py --pages`, `review_diff.py --draft-note`.
