# Task A — Photoreal 3D Head/Face Reconstruction of a Specific Person from Sparse Historical Stills

AS_OF: 2026-06-22. Scope: reconstruct a PHOTOREAL 3D head of a specific historical person from a handful of unposed B&W portraits (mostly frontal, thin profile coverage, ~1 color photo, NO video, NO synchronized multi-view rig). "Best version first."

## Sources
[a1] MPI-IS / T. Bolkart — FLAME-Universe (FLAME 3D head model + method index: DECA, EMOCA, MICA, SPECTRE, GAGAvatar) | https://github.com/TimoBolkart/FLAME-Universe | official | As-Of(maintained 2026) | 9
[a2] TU Munich (S. Giebenhain et al.) — Pixel3DMM: Versatile Screen-Space Priors for Single-Image 3D Face Reconstruction | https://simongiebenhain.github.io/pixel3dmm/ (arXiv:2505.00615) | academic | As-Of(2025-05) | 8
[a3] Yannick Hold-Geoffroy / EPFL+MPI (EMOCA) — EMOCA: Emotion Driven Monocular Face Capture and Animation | https://arxiv.org/abs/2204.11312 | academic (CVPR 2022) | As-Of(2022-04) | 8
[a4] UC Merced + Adobe Research — FaceLift: Single Image to 3D Head with View Generation and GS-LRM | https://arxiv.org/html/2412.17812v1 | academic | As-Of(2024-12) | 8
[a5] University of Tokyo + RIKEN AIP (X. Chu et al.) — GAGAvatar: Generalizable and Animatable Gaussian Head Avatar | https://arxiv.org/abs/2410.07971 (NeurIPS 2024) | academic | As-Of(2024-10) | 8
[a6] (HeadGAP authors) — HeadGAP: Few-Shot 3D Head Avatar via Generalizable Gaussian Priors | https://arxiv.org/abs/2408.06019 (3DV 2025) | academic | As-Of(2024-08, rev 2025-01) | 8
[a7] (anon/TU Darmstadt-style) — GaussianAvatars: Photorealistic Head Avatars with Rigged 3D Gaussians | https://arxiv.org/abs/2312.02069 (CVPR 2024) | academic | As-Of(2023-12) | 8
[a8] (Yang et al.) — High-Quality 3D Head Reconstruction from Any Single Portrait Image | https://arxiv.org/abs/2503.08516 | academic | As-Of(2025-03, rev 2026-01) | 7
[a9] (Wang et al.) — Self-Supervised Selective-Guided Diffusion Model for Old-Photo Face Restoration | https://arxiv.org/abs/2510.12114 | academic | As-Of(2025-10) | 7
[a10] UW + Google (Luo et al.) — Time-Travel Rephotography (project antique photos into modern high-res face space) | https://arxiv.org/abs/2012.12261 | academic (SIGGRAPH Asia 2021) | As-Of(2020-12) | 8
[a11] S. Zhou et al. — CodeFormer: Towards Robust Blind Face Restoration with Codebook Lookup Transformer | https://arxiv.org/abs/2206.11253 (NeurIPS 2022) | academic | As-Of(2022-06) | 8
[a12] (review) — From Shades to Vibrance: A Comprehensive Review of Modern Image Colorization Techniques (2015–2025) | https://www.researchgate.net/publication/395598503 | academic (review) | As-Of(2025) | 6

## Findings
1. FLAME is the dominant identity/geometry backbone: a linear identity shape space trained on head scans of 3800 subjects plus articulated neck/jaw/eyeballs, pose blendshapes, and expression blendshapes, and is the shared topology for DECA/EMOCA/MICA/SPECTRE/GAGAvatar [a1].
2. DECA and EMOCA both take a SINGLE RGB image and regress FLAME shape+expression(+detail displacement), but are self-supervised on 2D data only, giving coarse identity geometry rather than photoreal skin/appearance [a1][a3].
3. Pixel3DMM (2025) is the current single-image geometry SOTA: it fine-tunes a DINO ViT to predict per-pixel surface normals + UV, then fits FLAME, beating DECA/EMOCA baselines by >15% geometric accuracy on posed expressions [a2].
4. Pixel3DMM was trained on FLAME-registered scan datasets (NPHM, FaceScape, Ava256) totaling >1,000 identities and 976K images, and ships a new benchmark evaluating both posed AND neutral geometry [a2].
5. FaceLift turns one image into a photoreal 3D head via Stage-1 multi-view latent diffusion (6 views: front/±45/±90/back, SD V2-1-unCLIP) then Stage-2 GS-LRM Gaussian-splat reconstruction in <1s, trained on 8×A100-80GB, using ArcFace distance for identity (0.157 on Cafca, beating LGM 0.256 / Era3D 0.298) [a4].
6. GAGAvatar (NeurIPS 2024) does one-shot, single-photo 3DGS head reconstruction with real-time reenactment via a dual-lifting method (forward+backward lift distances) and a FLAME/3DMM expression handle, rendering 10–100x faster than NeRF [a5].
7. GaussianAvatars and most high-fidelity 3DGS/NeRF head avatars (GaussianAvatars, MonoGaussianAvatar, SHARP) require MONOCULAR VIDEO or multi-view sequences to fit a person — they assume many posed frames, not a handful of stills [a7].
8. Few-shot 3DGS avatars exist (HeadGAP, 3DV 2025): a generalizable Gaussian prior is pre-trained on a large multi-view DYNAMIC dataset, then a NEW identity is personalized from "few-shot in-the-wild" images via inversion + fine-tuning — but the strong prior must be built from video/rig data even if the target person is given only stills [a6].
9. "High-Quality 3D Head Reconstruction from Any Single Portrait" (2025) handles side-face angles and accessories by running identity- and expression-aware multi-view diffusion to hallucinate 96 orbital frames, then reconstructing 3D — identity is enforced via guidance losses but unseen geometry is generative (hallucinated) [a8].
10. Identity fidelity is universally measured by face-recognition embedding distance (ArcFace / VGG-Face cosine) between input and rendered novel views, NOT by ground-truth 3D — so a method can score "high identity" while still hallucinating the back of the head, hair, ears, and all color [a4][a8].

## Deep Read Notes

### Pixel3DMM [a2] — single-image GEOMETRY SOTA (use for the metric face shape)
- Input: one RGB face image. Pipeline: fine-tuned DINO ViT backbone + lightweight heads predict per-pixel surface normals and UV coordinates; 2D vertex locations are obtained by nearest-neighbor lookup on the predicted UV map; FLAME is then fit by optimization against the normal + UV constraints.
- Training data: NPHM + FaceScape + Ava256, all registered to FLAME topology → >1,000 identities / 976K images.
- Result: >15% better geometric accuracy than the most competitive baselines on POSED expressions; first benchmark to score both posed and neutral geometry across diverse expressions/angles/ethnicities. Baselines include DECA and FlowFace.
- Relevance to our case: produces a clean, metric, animatable FLAME mesh from a single frontal B&W portrait — but it gives GEOMETRY/topology, not photoreal skin or color. This is the geometry anchor, not the final render. No published failure-mode list, but as a fitting method it cannot invent profile/occluded geometry the prior doesn't supply.

### FaceLift [a4] — single-image to photoreal 3DGS head (highest "best-version" candidate)
- Two stages: (1) multi-view latent diffusion (Stable Diffusion V2-1-unCLIP, CLIP image conditioning) hallucinates 6 consistent views (front, ±45°, ±90°, back); (2) GS-LRM produces 3D Gaussian splats, full reconstruction <1s at inference.
- Trained ENTIRELY on synthetic data: 200 identities × 50 variations, 512×512, ambient + random HDR lighting. Diffusion fine-tunes on synthetic heads; GS-LRM pre-trains on Objaverse then fine-tunes on synthetic heads. Compute: 8×A100-80GB (diffusion, 20k steps) + 8×A100-40GB (GS-LRM, 20k steps).
- Identity (ArcFace distance, lower=better): 0.157 Cafca / 0.187 Ava-256, beating LGM 0.256, Era3D 0.298. Render metrics on Cafca: PSNR 16.61, SSIM 0.797, LPIPS 0.269, DreamSim 0.110.
- Stated limitations / failure modes: no training data for hats/glasses → it approximates them as hair; produces extraneous hair on out-of-distribution inputs; temporal inconsistency in hair across video. The ±90° and back views are DIFFUSION-HALLUCINATED, so anything not in the single input (back of head, far profile, hair detail) is invented, not recovered — directly the risk for a sparse-historical subject.

### HeadGAP [a6] — few-shot personalization (the closest fit to "a handful of stills")
- Two phases: (1) Prior learning on a large-scale multi-view DYNAMIC dataset → a Gaussian-Splatting auto-decoder with part-based dynamic modeling; (2) Avatar creation = fast personalization from few-shot in-the-wild images via inversion + fine-tuning. Accepted 3DV 2025.
- Key caveat for us: the few-shot magic comes from the strong learned prior, which itself is built from multi-view/video capture of OTHER people; the target can be given few images, but the system's realism for unseen views is borrowed from the prior population, i.e. plausible rather than verified for the specific historical individual. Abstract gives no exact shot count or quantitative numbers.

### Old-photo face restoration / colorization feeding the pipeline [a9][a10][a11][a12]
- Self-Supervised Selective-Guided Diffusion (2025) [a9]: casts old-photo face repair as JOINT restoration + colorization + inpainting in one diffusion model with selective guidance, explicitly to avoid cascading errors of sequential pipelines; explicitly names the identity-vs-hallucination tension (aggressive restore = plausible-but-wrong features; conservative = under-recovery). Compares to DifFace, DiffBIR.
- Time-Travel Rephotography [a10] (SIGGRAPH Asia 2021): projects antique portraits into the StyleGAN2 latent space of modern high-res faces to "rephotograph" historical subjects — the canonical method for cleaning B&W historical portraits before 3D, and a documented case where output identity is constrained by, but partly synthesized from, a modern face prior.
- CodeFormer [a11]: transformer codebook-lookup blind face restoration with an explicit fidelity-vs-quality knob (w parameter); robust to heavy degradation; the standard GFPGAN successor for the restoration step.
- Colorization is fundamentally ill-posed for history: reviews flag semantic ambiguity and that original colors are UNKNOWN/unrecoverable for B&W historical images — color of eyes/skin/hair/clothing is a guess unless documented externally [a12]. (~1 color photo in our set is therefore disproportionately valuable as a color anchor.)

### Synthesis for our exact constraints
- Recommended "best-version" stack: (1) restore + (selective) colorize each still with a diffusion restorer (CodeFormer/selective-guided diffusion) anchored to the single color photo for palette [a9][a11]; (2) recover metric FLAME geometry per usable still with Pixel3DMM and fuse identity shape across the few frontal/profile shots [a2]; (3) build the photoreal appearance either feed-forward via FaceLift / GAGAvatar from the best restored frontal, or via HeadGAP-style few-shot personalization on top of a population prior [a4][a5][a6].
- Hard truth: with NO video and thin profile coverage, EVERYTHING off the captured angles (full profile, back of head, ears, hair volume, all color absent the one color photo) is HALLUCINATED by generative priors; identity scores (ArcFace) only certify the seen frontal view, not the invented geometry [a4][a8][a10].

## Gaps
- No source reports identity-fidelity numbers for the EXACT scenario here (multiple unposed B&W stills of ONE real person, fused, no video) — published metrics are single-image or video, on synthetic/modern color datasets (Cafca, Ava-256, NoW), so true fidelity to a sparse historical subject is unquantified.
- HeadGAP's actual few-shot input count and quantitative identity/geometry numbers were not in the abstract (full PDF exceeded fetch size); the precise "minimum N stills" for acceptable few-shot personalization is unconfirmed.
- Found no 2026 method specifically designed to FUSE a handful of unposed historical stills (vs single-image or video) into one identity-consistent 3DGS head without a multi-view rig — closest are HeadGAP (needs a video-built prior) and classic Internet-photos head reconstruction (older, non-photoreal); this is a genuine open gap.
- Could not extract concrete PSNR/identity tables from the old-photo restoration paper [a9] or the single-portrait head paper [a8] (numbers were in PDF tables / not in metadata); colorization fidelity for historical subjects has no agreed ground-truth metric since the true colors are unknowable.
- VRAM/compute for INFERENCE (vs training) is rarely stated; FaceLift training is 8×A100 but per-image inference cost on a single GPU is not given [a4]; GAGAvatar is feed-forward/real-time but exact VRAM unlisted [a5].
