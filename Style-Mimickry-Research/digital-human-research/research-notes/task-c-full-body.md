# Task C — Full-Body Human Reconstruction & Animation from Sparse Stills (2026 SOTA)

Scope: rebuild + ANIMATE a photoreal full body of one person (Solanus Casey) from sparse still photos — no video of him, no mocap of him — wearing a Capuchin Franciscan habit (brown hooded wool robe + rope cincture) needing physics cloth. "Best version first."

## Sources
[c1] Max Planck IS / vchoutas — SMPL-X: Expressive Body Capture (3D Hands, Face, Body from Single Image) | https://github.com/vchoutas/smplx | official | As-Of(2019, maintained 2024) | Authority(10)
[c2] Max Planck IS (Osman, Bolkart, Black) — STAR: Sparse Trained Articulated Human Body Regressor | https://arxiv.org/abs/2008.08535 | academic | As-Of(2020-08, ECCV2020) | Authority(9)
[c3] DGIST + Meta Codec Avatars (Moon, Shiratori, Saito) — ExAvatar: Expressive Whole-Body 3D Gaussian Avatar | https://mks0601.github.io/ExAvatar/ | official(project) | As-Of(2024-07, ECCV2024) | Authority(9)
[c4] Alibaba/Tongyi (Qiu et al.) — LHM: Large Animatable Human Reconstruction Model from a Single Image in Seconds | https://arxiv.org/abs/2503.10625 | academic | As-Of(2025-03) | Authority(8)
[c5] ETH Zurich (Ho et al.) — SiTH: Single-view Textured Human Reconstruction with Image-Conditioned Diffusion | https://arxiv.org/abs/2311.15855 | academic | As-Of(2023-11, CVPR2024) | Authority(8)
[c6] Mahmood, Ghorbani, Black et al. — AMASS: Archive of Motion Capture as Surface Shapes | https://arxiv.org/abs/1904.03278 | academic | As-Of(2019, ICCV2019) | Authority(10)
[c7] Liu et al. — EMAGE: Unified Holistic Co-Speech Gesture Generation (BEAT2, SMPL-X+FLAME) | https://arxiv.org/abs/2401.00374 | academic | As-Of(2024-03, CVPR2024) | Authority(8)
[c8] Grigorev et al. (Meta/MPI) — ContourCraft: Learning to Resolve Intersections in Neural Multi-Garment Simulations | https://dl.acm.org/doi/10.1145/3641519.3657408 | academic | As-Of(2024-05, SIGGRAPH2024) | Authority(8)
[c9] Xu et al. — Dress-1-to-3: Single Image to Simulation-Ready 3D Outfit (Diffusion Prior + Differentiable Physics) | https://arxiv.org/abs/2502.03449 | academic | As-Of(2025-02, TOG/SIGGRAPH2025) | Authority(8)
[c10] DressWild — Feed-Forward Pose-Agnostic Garment Sewing Pattern Generation from In-the-Wild Images | https://arxiv.org/html/2602.16502v1 | academic | As-Of(2026-02) | Authority(7)
[c11] Saric/Bharadwaj et al. (MPI/VCAI) — GIGA: Generalizable Sparse Image-driven Gaussian Avatars | https://arxiv.org/abs/2504.07144 | academic | As-Of(2025-04) | Authority(7)
[c12] Frontiers in AI — Human reconstruction using 3D Gaussian Splatting: a brief survey | https://www.frontiersin.org/journals/artificial-intelligence/articles/10.3389/frai.2025.1709229/full | academic(survey) | As-Of(2025) | Authority(6)

## Findings
- SMPL-X is the canonical expressive full-body rig: M(theta, beta, psi) with 10,475 vertices and 54 joints (incl. neck, jaw, eyeballs, fingers), unifying SMPL body + MANO hands + FLAME face via linear blend skinning, and is the de-facto driving skeleton for nearly every method below [c1].
- STAR is a drop-in SMPL replacement that learns sparse per-joint, BMI-dependent pose correctives, cutting parameters to ~20% of SMPL with better generalization, but it is body-only (no hands/face) so it is inferior to SMPL-X for an expressive talking/gesturing avatar [c2].
- ExAvatar is the strongest "best-quality" animatable photoreal path but needs ~10 s of casual MONOCULAR VIDEO (neutral pose) of the subject — it binds 3D Gaussians as vertices on the SMPL-X mesh and is then drivable by full-body pose theta + facial expression psi — which is the blocker for stills-only Solanus [c3].
- LHM reconstructs an animatable full-body Gaussian-splat avatar from a SINGLE image feed-forward in ~2–6.6 s (0.5B/0.7B/1B variants), preserving clothing geometry/texture via a multimodal transformer + head feature-pyramid for face identity — the most direct stills-to-animatable route [c4].
- For high-fidelity geometry from one photo, diffusion-prior implicit methods (SiTH hallucinates the back view then fits an SDF mesh; relatives ECON/SIFU/PSHuman/SyncHuman) give a watertight clothed mesh but are not inherently animation-rigged without SMPL-X registration [c5].
- AMASS is the motion bank to drive a subject with no mocap: ~40 h of optical mocap unified onto SMPL/SMPL+H (extensible to SMPL-X), and it is the training corpus for text-to-motion generators (MDM, MotionDiffuse, MoMask via HumanML3D's 14,616 motions / 44,970 captions) [c6].
- Motion onto a Solanus SMPL-X rig comes from (a) retargeting an actor/AMASS clip in the shared SMPL-X parameter space or (b) text-to-motion / audio-to-gesture generation, since both produce SMPL(-X) theta sequences that the avatar consumes directly [c6].
- EMAGE generates holistic co-speech gesture (face + body + hands + global translation) directly in SMPL-X+FLAME from audio + masked seed gestures using 4 compositional VQ-VAEs, trained on BEAT2 — ideal for making a sermon-audio-driven, gesturing Solanus [c7].
- ContourCraft (SIGGRAPH 2024) is a learned graph-neural cloth simulator that explicitly resolves garment–body and self-intersections (extending HOOD/SNUG), giving fast garment-agnostic dynamics suitable for a flowing hooded wool robe [c8].
- A simulation-ready habit can be authored from photos: Dress-1-to-3 turns one image into separated body + garment via image-to-sewing-pattern + multi-view diffusion refined by a differentiable simulator, and DressWild (2026) does feed-forward pose-agnostic sewing-pattern generation — both yield patterns importable to Marvelous Designer/Blender physics for a true draped, hooded robe [c9][c10].

## Deep Read Notes

### ExAvatar (c3) — best-quality animatable avatar, but the input gate
- Authors: Gyeongsik Moon (DGIST), Takaaki Shiratori, Shunsuke Saito (Meta Codec Avatars Lab); ECCV 2024.
- Input: a casually captured short MONOCULAR phone scan, ~10 s of neutral pose. Pre-step: co-register the video frames to SMPL-X.
- Representation: hybrid mesh + 3DGS. Each 3D Gaussian is treated as a vertex on the SMPL-X surface with predefined connectivity (the SMPL-X triangle topology). This gets 3DGS photorealism + SMPL-X whole-body drivability.
- Driving: after training, drivable by SMPL-X whole-body pose theta AND facial expression code psi — supports body motion, hand gestures, AND facial expressions (most casual-video avatars do body only).
- Contributions: connectivity-based regularizers + Laplacian + custom face loss to suppress artifacts under novel poses/expressions; joint and per-face offset optimization to compensate SMPL-X's limited expressiveness; generalizes to arbitrary SMPL-X expression codes despite low training diversity.
- Implication for Solanus: ExAvatar is the gold standard for an expressive, drivable photoreal avatar, but requires video. With only stills, the realistic plan is to SYNTHESIZE the needed video/views first (single-image-to-3D such as LHM/SiTH, or sparse multi-view + multi-view diffusion) and feed that into an ExAvatar-style rig, OR use a single-image feed-forward model directly.

### LHM (c4) — single-image to animatable Gaussian avatar
- Authors: Lingteng Qiu + 10 (Alibaba/Tongyi lab); arXiv:2503.10625, submitted 2025-03-13; code released.
- Input: a SINGLE image. Output: an animatable human as 3D Gaussian splatting for real-time photoreal rendering.
- Architecture: scalable feed-forward multimodal transformer encoding body positional features + image features via attention; a head feature-pyramid aggregates multi-scale head features to preserve face identity and fine detail. Preserves clothing geometry and texture; no post-processing for face/hands.
- Speed: LHM-0.5B ~2.01 s, LHM-0.7B ~4.13 s, LHM-1B ~6.57 s per reconstruction.
- Implication for Solanus: most direct stills-only route to an animatable body; quality will be lower than a video-trained ExAvatar and back/side detail is hallucinated, but it is rig-ready (Gaussian avatar drivable by pose). Good first pass; upgrade by aggregating MULTIPLE Solanus photos rather than one.

### SMPL-X (c1) — the rig everything shares
- M(theta, beta, psi): theta = pose (body + hands + jaw + eyes), beta = shape, psi = facial expression. 10,475 verts, 54 joints. Vertex-based LBS + learned corrective blendshapes.
- Family: extends SMPL (body), absorbs MANO (hands) and FLAME (face); SMPL+H = SMPL+hands (what AMASS natively stores). Repo ships SMPL-X<->MANO/FLAME vertex correspondences so data transfers across the family; gender variants (male/female/neutral).
- Why it matters here: it is the COMMON CURRENCY — reconstruction (LHM/SiTH/ExAvatar), motion (AMASS/HumanML3D), and gesture (EMAGE) all express output as SMPL-X parameters, so a single Solanus SMPL-X identity (beta from his stills) can be driven by any of those theta/psi streams.

### EMAGE (c7) — audio-driven gesture for a person with no captured motion
- Authors: Haiyang Liu + 9; CVPR 2024 (arXiv:2401.00374).
- Generates full-body holistic gesture (face, local body, hands, global movement) in SMPL-X body + FLAME head from audio + masked gesture seeds; uses a Masked Audio Gesture Transformer and 4 compositional VQ-VAEs; trained on BEAT2 (MoShed SMPL-X + FLAME, mesh-level).
- Implication for Solanus: if there is audio (or TTS of his writings — see voice task), EMAGE can produce period-plausible co-speech gesture directly in his SMPL-X rig; combine with AMASS locomotion/blessing gestures retargeted for non-speech motion.

## Gaps
- No method found that natively trains a video-quality ExAvatar-grade avatar from SPARSE STILLS alone — every top photoreal animatable result (ExAvatar c3, GIGA c11) assumes monocular video or sparse multi-VIEW images captured simultaneously of a LIVING subject; for a deceased person with only archival single-pose photos, the honest pipeline is stills -> single/few-image 3D (LHM/SiTH/multi-view diffusion) -> rig as SMPL-X Gaussian avatar, accepting hallucinated back/sides.
- Could not find a published method specializing in a HOODED RELIGIOUS HABIT (cowl/hood draping, rope cincture, heavy wool) — generic neural/physics cloth (ContourCraft c8, Marvelous Designer, Dress-1-to-3 c9) can do robes, but hood-on-vs-off and the cincture/knot are bespoke garment-authoring problems with no off-the-shelf "habit" asset reported.
- Wool-specific material parameters (bending stiffness, weight/density, friction for coarse Capuchin serge) are not given by any paper read; these must be hand-tuned in Marvelous Designer / Blender or a sim like ContourCraft, with no learned prior for this fabric.
- Verifying IDENTITY accuracy of a stills-reconstructed face under the SMPL-X/FLAME expression space is unmeasured for historical low-res B&W photos; no source quantified reconstruction fidelity from degraded archival imagery (all benchmarks use modern color in-the-wild or studio data).
- Did not find a single end-to-end published system that jointly reconstructs body + simulation-ready habit + drives it from generated motion; this remains an integration/engineering task stitching c4/c5 (body), c9/c10 + c8 (cloth), and c6/c7 (motion) on the shared SMPL-X rig.
