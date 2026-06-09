# Checkpoint 01 — pre-label-review (2026-06-02)

Snapshot taken **before** the label-agreement review & coverage/generalization work began.

## What's here
- `auto_labeler.py`, `qa_report.py`, `review_diff.py`, `normalized_editor.py`, `compare_overlay.py`
  — the mutable code as of this checkpoint.
- `labeled_examples/` — the full few-shot pool (the thing the review may add/remove/relabel).
- `reviewed/` — copy of the gold (it is *never* written by the pipeline; this is belt-and-suspenders).
- `GIT_HEAD.txt` — commit the working tree was based on (`cf78cba`).

## To revert
Copy any file/dir back over the working copy, e.g.:
```bash
cd step_4
cp checkpoints/01_pre-label-review/auto_labeler.py .
cp -r checkpoints/01_pre-label-review/labeled_examples .
```
Or restore tracked files from git: `git checkout cf78cba -- <path>`.

Later checkpoints are `checkpoints/02_*`, `03_*`, … created before each subsequent mutation.
