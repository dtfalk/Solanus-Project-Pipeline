# Task D — Compositing a Reconstructed Head + Body + Cloth into One Drivable, Relightable Photoreal Digital Human (from sparse historical stills)

AS_OF: 2026-06-22

## Sources
[d1] Meta Reality Labs (Saito, Schwartz, Simon, Li, Nam) — Relightable Gaussian Codec Avatars (CVPR 2024) | https://arxiv.org/html/2312.03704 (proj: https://shunsukesaito.github.io/rgca/) | academic | 2023-12-06 | 10
[d2] Meta Codec Avatars Lab (Junxuan Li et al.) — URAvatar: Universal Relightable Gaussian Codec Avatars (SIGGRAPH Asia 2024) | https://arxiv.org/html/2410.24223v1 | academic | 2024-10-31 | 10
[d3] Meta + ETH (neuralbodies) — Relightable Full-Body Gaussian Codec Avatars (CVPR 2025) | https://arxiv.org/abs/2501.14726 (proj: https://neuralbodies.github.io/RFGCA/) | academic | 2025-01-24 | 10
[d4] Jiang et al. — GaussianShader: 3D Gaussian Splatting with Shading Functions for Reflective Surfaces (CVPR 2024) | https://asparagus15.github.io/GaussianShader.github.io/ | academic | 2023-11-29 | 9
[d5] Kirschstein et al. (TUM/Meta) — Avat3r: Large Animatable Gaussian Reconstruction Model for High-fidelity 3D Head Avatars | https://tobias-kirschstein.github.io/avat3r/ | academic | 2025-02-27 | 9
[d6] Qiu et al. — LHM / generalizable Body-Head 3DGS avatars (Human reconstruction using 3DGS: a brief survey, Frontiers AI) | https://www.frontiersin.org/journals/artificial-intelligence/articles/10.3389/frai.2025.1709229/full | academic | 2025 | 8
[d7] Epic Games — MetaHuman Animator / Mono Video Capture documentation (UE 5.6/5.7) | https://dev.epicgames.com/documentation/metahuman/metahuman-animation-from-mono-video-capture-in-unreal-engine | official | 2025 | 9
[d8] NVIDIA — ACE Audio2Face-3D Unreal Engine Plugin (Character Animation docs, v2.5) | https://docs.nvidia.com/ace/ace-unreal-plugin/2.5/ace-unreal-plugin-animation.html | official | 2025 | 9
[d9] XVERSE Technology — XScene-UEPlugin: real-time 3DGS rendering/editing in UE5 | https://github.com/xverse-engine/XScene-UEPlugin | secondary-industry | 2025 | 7
[d10] Anon. — Capture, Canonicalize, Splat: Zero-Shot 3D Gaussian Avatars from Unstructured Phone Images | https://arxiv.org/html/2510.14081 | academic | 2025-10 | 8
[d11] Saito/Schwartz et al. — Relightable Gaussian Codec Avatars project page (capture rig + VR demo) | https://shunsukesaito.github.io/rgca/ | official | 2024 | 9
[d12] Alldieck et al. (Google) — PHORHUM: Photorealistic Monocular 3D Reconstruction of Humans Wearing Clothing (CVPR 2022) | https://phorhum.github.io/ | academic | 2022-04-19 | 8

## Findings
1. The strongest relightable head pipeline (RGCA) is trained from a dome of 110 cameras at 4096x2668 + 460 white LEDs at 90 Hz, ~144,000 frames/subject on 4x NVIDIA A100 for 200k iters — the opposite of "a handful of stills" [d1].
2. RGCA relights in real time on a tethered consumer VR headset by learning radiance transfer with diffuse spherical harmonics + specular spherical Gaussians (all-frequency reflections, sub-mm hair/pore detail), not classic albedo/normal/BRDF inverse rendering [d1][d11].
3. URAvatar builds a universal prior from 342 multi-view scans (same 110-cam/460-LED rig) on 64x A100 for 400k iters (~5 days), then personalizes from a 5-10 min phone scan (100 sampled frames) via inverse rendering on 8x A100 (~3 hours), and is still "not instant" [d2].
4. Relightable Full-Body Gaussian Codec Avatars unifies face + body + hands and splits light transport into local diffuse (learnable zonal harmonics, cheap to rotate under articulation), non-local shadows (a shadow network over precomputed irradiance), and specular (deferred shading) — but all from multi-view light-stage data [d3].
5. Feed-forward sparse-input head methods (Avat3r) need ~4 face images, build an animatable avatar in "minutes" with no test-time optimization, and animate at ~8 FPS on a single RTX 3090 — the realistic compute floor vs. light-stage dome avatars [d5].
6. Single-view reconstruction provably hallucinates: models conditioned on one frontal view "struggle to maintain identity in side and back views," so the back of an unseen head is invented, not measured [d10].
7. Head-body unification at scale uses a Multimodal Body-Head Transformer (LHM) to fuse head and body 3DGS into one animatable rig, and 3DGS avatars render real-time (reported up to 361 FPS) once built [d6].
8. Baked-in lighting is the core blocker for historical stills: shadows get baked into diffuse albedo and methods often assume lights at infinity, so a single B&W photo with unknown illumination makes albedo/illumination separation ill-posed [d12].
9. Production runtime: UE 5.6/5.7 MetaHuman Animator now drives photoreal heads from mono webcam/smartphone via LiveLink, but there is no direct single-photo import — you must go through Mesh-to-MetaHuman, which "rounds" geometry back toward its photoreal scan database [d7].
10. 3DGS avatars are engine-renderable in real time via plugins (XScene-UEPlugin, SplatRenderer ~2M Gaussians @ 100+ FPS) and can be voice-driven by NVIDIA ACE Audio2Face-3D streaming blendshapes/animation into UE5 [d8][d9].

## Deep Read Notes

### [d1] Relightable Gaussian Codec Avatars (Saito et al., CVPR 2024) — the quality ceiling
- Capture: 110 cameras @ 4096x2668, 460 white LED lights @ 90 Hz, ~144,000 frames per subject. This is a Meta light-stage dome; there is no path to reproduce it from archival photos.
- Training: 4x NVIDIA A100, batch 16, 200k iterations, lr 5e-4. Paper does NOT state per-avatar Gaussian count, VRAM, total wall-clock, or FPS (only "real-time" and "tethered consumer VR headset").
- Relighting model: instead of estimating explicit BRDF/albedo/normals via inverse rendering, it learns radiance transfer — diffuse via global-illumination-aware spherical harmonics, specular via spherical Gaussians — giving all-frequency reflections, eye glints, and explicit gaze control while staying real-time.
- Stated limits: needs a coarse mesh + gaze tracking as preprocessing (sensitive to tracking failure); "extending to in-the-wild inputs remains a challenge due to the lack of precisely known illumination information." That last sentence is the direct verdict on the Solanus sparse-stills use case: the method assumes known, controlled illumination it cannot get from old photos.

### [d2] URAvatar (Li et al., SIGGRAPH Asia 2024) — the closest thing to "sparse consumer input," and how far it still is
- A universal relightable prior is learned from 342 multi-view subjects (110-cam/460-LED rig) on 64x A100, batch 128, 400k iters, ~5 days.
- Personalization input is a 5-10 minute phone scan (not stills): 100 frames sampled "to cover sufficient variation," fit on 8x A100 (3k iters light fitting + 10k iters encoder/decoder = 13k), ~3 hours total.
- Key relevance: even the "phone scan" path needs a multi-minute moving capture under varied pose/lighting AND a giant studio-trained prior; it explicitly is "not instant." Limits the authors name: out-of-corpus variation generalizes poorly, clothing relights worse than the head, and high-frequency illumination estimation is unsolved. For a few frontal B&W stills the personalization signal (illumination cues, multi-view coverage) simply isn't present.

### [d3] Relightable Full-Body Gaussian Codec Avatars (CVPR 2025) — the head+body+hands unification answer
- This is the most direct "one consistent full-body relightable avatar" paper: it models face + hands + body together and is the architecture to study for head-body stitching with relighting.
- Decomposition: local diffuse = learnable zonal harmonics (chosen because they rotate cheaply under articulation, disentangling appearance from pose); non-local = a shadow network predicting inter-part shadowing from precomputed incoming irradiance on a base mesh; specular = deferred shading for highlights/reflections.
- Claims convincing intrinsic decomposition + generalization to novel point/environment light and unseen poses. Data is multi-view light-stage; the abstract does not publish camera count, GPU, VRAM, training time, or Gaussian count (consistent with the Meta codec-avatar line requiring dome capture). Failure modes not enumerated in the abstract.

### [d5] Avat3r — realistic sparse-input floor
- Feed-forward Large Reconstruction Model: 4 input face images -> animatable 3D Gaussian head in a single forward pass, full creation "within minutes," no per-subject optimization.
- Built on DUSt3R (geometry) + Sapiens (features) + dense self-attention; the forward pass is "expensive." Animation ~8 FPS on one RTX 3090 via activation caching.
- Robust to inconsistent inputs (different expressions across the 4 views) and renders arbitrary viewpoints; adapts Gaussian count to identity silhouette. This is the realistic compute/quality bracket if the Solanus head must come from a small number of photos — but note it still expects ~4 reasonably-posed views, and identity in side/back is interpolated, not observed.

## Gaps
- No source gives exact per-avatar Gaussian count, peak training VRAM, or rendered FPS for the Meta codec-avatar line ([d1][d3]) — papers report GPU counts and iterations but omit VRAM and wall-clock; could not confirm whether a single A100 (40/80 GB) suffices.
- No on-disk storage figure found for a finished relightable codec avatar / splat model (MB-GB per avatar); searches did not surface a committed number.
- Could not find any published method that produces a relightable, drivable full-body avatar specifically from a few frontal B&W historical stills; every relightable pipeline assumes either a light-stage dome or a multi-minute moving phone scan — the sparse-historical-stills regime appears to be an open/unaddressed gap.
- Did not find a quantitative study of identity drift / temporal flicker as a function of number of input stills (e.g. PSNR/identity-cos vs. 1 vs 4 vs 8 views); evidence is qualitative ("struggles in side/back views," "hallucination").
- NVIDIA ACE Audio2Face-3D ([d8]) drives MetaHuman-style rigs/blendshapes; could not confirm a documented turnkey path to drive a raw 3DGS/codec-avatar splat head directly (vs. a meshed MetaHuman) inside UE.
- B&W-specific colorization-then-reconstruction error propagation (how much hallucinated color corrupts albedo) was not found in any quantified source.
