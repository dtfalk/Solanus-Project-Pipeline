# Digital avatar — the best-version-first build plan

Grounded in deep research (AS_OF 2026-06-22; ~48 cited sources in `SOURCES.md`). Compute/storage no object —
build at full fidelity, downsample later. Authorized via the vice-postulator, Fr. Ed Foley. Concrete compute
target: the RCC **Midway3** node (4× L40S = 192 GB; see `../mimicker/SYSTEM.md`) for what fits there, with
heavier inference (e.g. HunyuanVideo-Avatar) noted where it exceeds that.

## Headline
Face geometry + a photoreal frontal head are achievable from the photo corpus; **profile/back/hair/color are
generative**, **motion is borrowed** (no footage of him exists), and **relighting is effectively unsolved**
from old photos with unknown lighting. The deliverable is an *historically reconstructed, motion-synthesized*
avatar — labeled as such — not a documentary capture. It is an integration of ~8 research tools on the shared
**FLAME** (head) / **SMPL-X** (body) rig, not one product.

## Stage 0 — Acquire (do first)
High-res TIFF scans of original prints/**negatives** from the **Capuchin Province of St. Joseph Archives**
(Junia Yasenov; 1820 Mt. Elliott St., Detroit; (313) 579-2100), prioritizing: every distinct frontal/3-quarter
portrait across his life, any side-profile/angled candids (rare — request explicitly), full-length standing
shots (for body), and **the single color photo** (palette anchor; eyes documented as blue). Cross-reference
captions via *Blessed Solanus Casey* (Images of America, ISBN 9781467102544). → `data/raw/`.

## Stage 1 — Restoration, upscale, colorization (per photo)
Blind-restore (**CodeFormer**) + diffusion old-photo restore (**Selective-Guided Diffusion**); for the best
frontals, **Time-Travel Rephotography** to lift into a modern high-res face space. Colorize driven by the one
color photo as palette anchor — **treat all other color as a documented guess**. `restore_images.py`.

## Stage 2 — Head geometry + identity fusion
**Pixel3DMM** (single-image normals/UV → FLAME fit; current SOTA, >15% better geometry than DECA/EMOCA) on each
cleaned frontal/profile; **fuse the per-photo FLAME identity (β)** across shots into one identity mesh. This is
the geometry anchor, kept separate from appearance. `reconstruct_head.py`.

## Stage 3 — Photoreal head appearance (3DGS)
Best version: feed the ~4 best restored views into **Avat3r** (sparse-image animatable Gaussian head; animates
on one RTX-3090-class GPU) or **FaceLift** from the single best frontal (multi-view diffusion → GS-LRM);
optionally personalize a **HeadGAP/GAGAvatar** prior. Bind the Gaussians to the Stage-2 FLAME rig for expression
control. Accept that ±90°/back/ear/hair detail is diffusion-invented. `build_head_appearance.py`.

## Stage 4 — Body + habit
**LHM** (1 image → animatable full-body 3DGS, feed-forward ~2–7 s) or **SiTH** (watertight clothed mesh) from a
full-figure still, registered to **SMPL-X**. Author the **Capuchin habit separately**: image-to-sewing-pattern
(**Dress-1-to-3 / DressWild**) → physics sim of hood/cowl/cincture with **ContourCraft** or Marvelous Designer
(hand-tuned coarse-wool params — no learned religious-habit prior exists). Body + cloth as separate layers so the
robe drapes over articulation. `build_body.py`.

## Stage 5 — Motion & speech (SMPL-X is the single driving currency)
- **Locomotion/blessing gestures:** retarget **AMASS** clips or a mocap stand-in actor.
- **Co-speech gesture from audio:** **EMAGE** (face+body+hand co-speech gesture directly in SMPL-X+FLAME),
  driven by the `../voice` render of his documented words.
- **Facial/lip motion:** drive the FLAME-rigged head; or for 2D video output choose by quality/latency:
  **OmniHuman-1.5** (closed/API, max realism) · **HunyuanVideo-Avatar** (open, photoreal, heavy: ~60 min/10 s on
  a 96 GB GPU — exceeds one L40S, shard or batch offline) · **Hallo2** (open, 4K/long) · **Ditto + LivePortrait**
  (open, real-time ~78 FPS on a 4090-class GPU). `drive.py`.

## Stage 6 — Composite, relight, runtime
Target architecture: **Relightable Full-Body Gaussian Codec Avatars** (head+body+hands, intrinsic decomposition)
— but its light-stage data is unavailable, so do **approximate relighting** (GaussianShader-style shading /
PHORHUM albedo) and accept baked-lighting limits. Real-time render the unified 3DGS in **Unreal Engine 5.6/5.7**
(XScene-UEPlugin); voice-drive via **NVIDIA ACE Audio2Face-3D**. Production-hardened alternative: the
**MetaHuman** path (Mesh-to-MetaHuman + MetaHuman Animator), which trades identity fidelity for robustness
(it "rounds" geometry toward Epic's scan DB). `composite.py`.

## Compute / storage (best-version; downsample later)
- Feed-forward recon (FaceLift/Avat3r/LHM) runs on a single high-VRAM GPU — fits Midway3's L40S.
- Open photoreal talking-head inference is heavy (HunyuanVideo ~60 min/10 s @96 GB) — run offline/batched.
- True relightable **codec avatars need A100/H100 + a 110-cam/460-LED light stage** → out of reach; use the
  approximate path.
- 3DGS avatars are ~hundreds of MB–low GB; keep all intermediates (raw scans, restored, per-photo FLAME fits,
  head/body Gaussians, garment sims, motion clips, renders).

## Honest limits (state in any output)
Off-captured-angle geometry, hair, ears, back-of-head and **all color are generative**; identity metrics
(ArcFace cosine) certify only the seen frontal view; **relighting is ill-posed** from unknown-lighting photos;
**motion is borrowed, never his**; the habit is bespoke garment engineering. Label outputs *"historically
reconstructed; motion synthesized."*

## Open questions
Exact count/angle coverage of archive photos (request an inventory); whether any profile shots exist; closed
(OmniHuman) vs open (Hunyuan/Ditto) talking-head given licensing + the L40S budget; MetaHuman robustness vs
3DGS identity fidelity; how much profile/back to sculpt by hand vs diffusion-invent.
