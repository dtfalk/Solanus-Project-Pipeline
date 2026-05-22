# pipeline_v2

This folder contains a non-destructive, structured pipeline for:
1. Preparing OCR-oriented cleaned page assets.
2. Building a reliable few-shot annotation package.
3. Running final cleanup exports after manual annotation.

The legacy `step_1`, `step_2`, `step_3`, and `step_x` folders are not overwritten by this pipeline.

## What Is Implemented

- Structured few-shot manifest workflow.
- Annotation packaging with edit-preserving source precedence:
  - `step_3/final` first,
  - then `step_3/normalized`,
  - then `step_x/label_page_data`.
- Clean export stage that mirrors the old `count_usages.py` behavior for excluded keys.
- Launch helper for using `step_3/normalized_editor.py` against `pipeline_v2/workspace` paths.

## Commands

Run from repository root.

```bash
python -m pipeline_v2.run init-manifest
python -m pipeline_v2.run validate-manifest
python -m pipeline_v2.run package-annotation
python pipeline_v2/launch_normalized_editor_v2.py
python -m pipeline_v2.run export-cleaned
```

## Manifest Location

Default manifest path:

`pipeline_v2/manifests/few_shot_manifest.json`

You can override with `--manifest /path/to/manifest.json`.

## Manifest Status Values

Each page entry supports:

- `approved`: Included by default.
- `candidate`: Included only with `--include-candidates`.
- `excluded`: Not packaged.

## Workspace Outputs

- `pipeline_v2/workspace/sample_pages`: per-page PDFs for selected few-shot pages.
- `pipeline_v2/workspace/annotation_seed`: source JSON seed pages for editor discovery.
- `pipeline_v2/workspace/final`: editable JSON and PDFs used by normalized editor.
- `pipeline_v2/workspace/CLEANED_FINAL`: cleaned export output.

## Current Scope

This first implementation focuses on the few-shot and annotation flow. A one-time migration script is intentionally not implemented yet so you can review before data transfer steps are added.
