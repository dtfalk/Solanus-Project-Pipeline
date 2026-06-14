---
name: verify-visual-labels
description: >-
  Use BEFORE claiming any labeled/annotated page is correct — bounding boxes,
  polygons, region labels, OCR boxes, segmentation, connections drawn on a
  scanned page or image. Invoke whenever judging "is this box/label right?",
  "does it bleed/clip?", "did the run come out well?", comparing label variants,
  or verifying a labeling/relabel result against rules or gold. It exists because
  judging label correctness from a whole-page render or from aggregate metrics
  FAILS — this skill forces the crop-zoom-overlay-checklist method that actually
  sees the errors.
---

# Verifying visual labels accurately

## Why naive checking fails (the root cause — read this once)

You cannot perceive box-level errors in a full-page render, for a concrete technical reason:
multimodal vision encoders **downscale every image to a fixed low resolution** (Claude's effective
sweet spot ≈ 1568px; encoders tile at 448–512px). A scanned page is ~5000–7000px wide. Downscaled
to ~1568px, a box edge that clips a character or overruns by 30–60px collapses to **sub-pixel** —
it is physically invisible. So a full-page overlay *always* "looks fine," and a 2px outline at that
scale is a hairline. This — not carelessness alone — is why label errors survived every "I looked
at it" check on this project. (Refs: Claude image-resolution guidance; InternVL/Monkey dynamic
tiling for high-res perception; VLMs have weak spatial reasoning + no self-correction, so method
must replace eyeballing.)

Two compounding habits made it worse:
1. **Measurement substituted for perception.** A median pixel-offset or box count can be perfect
   while individual boxes are wrong. *Measuring is not verifying.*
2. **Gestalt instead of box-by-box.** The page's overall structure looks right at a glance; the
   errors are local (one box too wide, one clip) and only appear when you inspect each box.

## The method (do ALL of it — do not skip to a conclusion)

**1. Never judge from a whole-page render.** Crop to a SMALL region (a few rows / one box) and
render THAT at high DPI so each glyph and box edge spans many display pixels. Use the helper:

```
./venv/bin/python ~/.claude/skills/verify-visual-labels/overlay_inspect.py \
    <page.pdf> <labels.json> --tiles 8        # 8 stacked strips, boxes overlaid, /tmp/inspect_*.png
# single box, max zoom, with neighbours + connections:
    ... --box <doc>:<category>:<index>
```
It draws thick colour-coded outlines, the category name on each box, a margin OUTSIDE each box (so
bleed into neighbours is visible), and connection lines. Then **Read every output crop** — not the
whole page.

**2. Inspect box-by-box against an explicit checklist.** For each box ask, looking at the pixels:
- Does it contain ALL of its own ink (no clipped first/last letter, top/bottom line)?
- Does it exclude everything NOT its own — neighbour text, the next column, **leader dots /
  ellipses**, connectors? (The #1 error class on this corpus is rightward/downward bleed.)
- Is the category right for what the box is actually on?
- (If connections) does each line go to the correct partner row, including under skew?

**3. State specific findings, never a verdict from stats.** Report "box X (archv_date row 4) clips
the final '8'" or "description box bleeds ~600px across the dots into the date column" — with the
crop you saw it in. Banned: "looks good / median offset 0 / 95% match, so it's fine." A metric may
accompany a finding; it may never replace looking.

**4. Compare against a reference when one exists.** Overlay gold (or another variant) on the SAME
crops and diff visually. For variants, view each at the same zoom on the same region.

**5. If you cannot see it, you have not verified it.** If a crop is still too small to resolve an
edge, zoom further (fewer rows, one column, `--box`). Silence/“looks fine” at low resolution is the
failure mode this skill exists to kill.

## Output contract

End with a per-region findings list (region → specific issue → which box), then the overall
judgement, then any fix. If you wrote "verified"/"correct"/"tight", you must have Read a high-zoom
crop showing it. Otherwise say "not yet verified" and crop further.
