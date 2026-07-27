# Task B — Audio-Driven Talking Head / Facial Reenactment State of the Art (AS_OF 2026-06-22)

Scope: animating a digital human of a specific person where the source identity is a STILL PHOTO (no driving video of the person exists). "Best version first" (max quality, compute no object), with honest realism limits noted.

## Sources
[b1] ByteDance — OmniHuman-1: Rethinking the Scaling-Up of One-Stage Conditioned Human Animation Models | https://arxiv.org/html/2502.01061v1 | academic | As-Of 2025-02-03 (v1; v3 2025-06-30) | Authority 9
[b2] ByteDance Intelligent Creation Lab — OmniHuman-1.5: Instilling an Active Mind in Avatars via Cognitive Simulation | https://arxiv.org/html/2508.19209v1 | academic | As-Of 2025-08-26 | Authority 9
[b3] Alibaba HumanAIGC — EMO2: End-Effector Guided Audio-Driven Avatar Video Generation | https://arxiv.org/html/2501.10687v1 | academic | As-Of 2025-01-18 | Authority 9
[b4] Alibaba Institute for Intelligent Computing — EMO: Emote Portrait Alive (Audio2Video Diffusion under Weak Conditions) | https://arxiv.org/abs/2402.17485 | academic | As-Of 2024-02-27 | Authority 9
[b5] Fudan Generative Vision — Hallo3: Highly Dynamic and Realistic Portrait Image Animation with Video Diffusion Transformer (CVPR 2025) | https://arxiv.org/html/2412.00733v3 | academic | As-Of 2024-12-01 (CVPR 2025) | Authority 9
[b6] Fudan Generative Vision — Hallo2: Long-Duration and High-Resolution Audio-Driven Portrait Image Animation (ICLR 2025) | https://arxiv.org/abs/2410.07718 | academic | As-Of 2024-10-10 | Authority 9
[b7] Xi'an Jiaotong / Tencent AI Lab / Ant — SadTalker: Learning Realistic 3D Motion Coefficients for Stylized Audio-Driven Single Image Talking Face (CVPR 2023) | https://openaccess.thecvf.com/content/CVPR2023/html/Zhang_SadTalker_..._CVPR_2023_paper.html | academic | As-Of 2023-06 (arXiv 2211.12194) | Authority 9
[b8] Kuaishou KwaiVGI / Kling — LivePortrait: Efficient Portrait Animation with Stitching and Retargeting Control | https://liveportrait.github.io/ (arXiv 2407.03168) | official | As-Of 2024-07-03 | Authority 9
[b9] Tencent AI Lab — V-Express: Conditional Dropout for Progressive Training of Portrait Video Generation | https://arxiv.org/abs/2406.02511 | academic | As-Of 2024-06-04 | Authority 8
[b10] Tencent — Sonic: Shifting Focus to Global Audio Perception in Portrait Animation | https://arxiv.org/abs/2411.16331 | academic | As-Of 2024-11-25 | Authority 8
[b11] Ant Group — Ditto: Motion-Space Diffusion for Controllable Realtime Talking Head Synthesis (ACM MM 2025) | https://arxiv.org/abs/2411.19509 | academic | As-Of 2024-11-29 | Authority 8
[b12] Tencent Hunyuan — HunyuanVideo-Avatar: High-Fidelity Audio-Driven Human Animation for Multiple Characters | https://arxiv.org/html/2505.20156v2 | academic | As-Of 2025-05-26 | Authority 9
[b13] Ant Group — EchoMimic: Lifelike Audio-Driven Portrait Animations through Editable Landmark Conditions (AAAI 2025) | https://arxiv.org/pdf/2407.08136 | academic | As-Of 2024-07-11 | Authority 8
[b14] Various (survey of 2026 systems) — StreamAvatar / Live Avatar / DiTalker / EditYourself (real-time + DiT audio-to-portrait) | https://arxiv.org/html/2512.22065 ; https://arxiv.org/pdf/2512.04677 ; https://arxiv.org/pdf/2508.06511 | academic | As-Of 2025-12 to 2026-01 | Authority 7

## Findings
- OmniHuman-1 (ByteDance) is a Diffusion-Transformer (MMDiT, Seaweed backbone + 3D causal VAE, flow matching) trained on ~18.7K hrs of video that animates a single reference image from audio/pose/text, supports face-to-full-body and arbitrary length, and beats Loopy on Sync-C (5.199), FID (31.4) and FVD (46.4) — but it is closed (API-only on Replicate) and not real-time [b1].
- OmniHuman-1.5 adds an MLLM "System-1/System-2 cognitive" planner so gestures/emotion follow audio *semantics* (not just rhythm), generates >1-minute clips with camera motion and multi-character interaction across photoreal/anime/non-human styles, but remains closed API-only [b2].
- EMO2 (Alibaba) takes one image + audio and outputs 704×512 2D video at 24 fps via a two-stage diffusion pipeline (Stage-1 DiT predicts MANO hand poses, Stage-2 ReferenceNet U-Net renders body), scoring FVD 129.4 / FID 27.3 / Sync-C 4.58 / identity CSIM 0.650 — closed source, not real-time [b3].
- EMO (original, Alibaba 2024) pioneered direct audio→video diffusion with NO intermediate 3D/landmarks for expressive talking-and-singing heads from one photo, but is closed and was never released [b4].
- Hallo3 (Fudan, CVPR 2025) is an open-source video-DiT (built on CogVideoX-style backbone) using a 3D-VAE identity reference network; it gives highly dynamic motion from image+WAV audio but is English-only, needs 1:1 or 3:2 input, was evaluated at 512×512, and is limited to a few tens of frames per clip (extended by motion-frame extrapolation) [b5].
- Hallo2 (Fudan, ICLR 2025) is the first open audio-driven portrait system to reach 4K resolution and hour-long duration via patch-drop+Gaussian-noise augmentation, latent vector-quantization and a high-quality decoder, plus optional text expression labels [b6].
- SadTalker (CVPR 2023, open source) generates 3DMM head-pose+expression coefficients from audio (ExpNet + PoseVAE) to drive a 3D-aware renderer from a single image, but is fundamentally low-res (~256–512), shows blurry/coarse lips and teeth/eye artifacts because 3DMMs don't model teeth or eyes (usually patched with GFPGAN) [b7].
- LivePortrait (Kuaishou/Kling, open source) is implicit-keypoint based (NOT diffusion, NOT natively audio) and is VIDEO-driven, hitting 12.8 ms/frame (~78 fps) on an RTX 4090 with strong eye/lip stitching+retargeting — so for a still photo it needs an audio→motion front-end (e.g. it is the renderer used by Ditto/JoyVASA-style pipelines) [b8].
- V-Express (Tencent, open source) solves the "audio is a weak condition" problem via progressive training + conditional dropout, driving a talking head from reference image + audio + a V-Kps landmark sequence (so it still wants a pose/keypoint track, not pure audio) [b9].
- Sonic (Tencent, open) targets long-audio stability with context-enhanced audio learning + time-aware position-shift fusion of shifted windows for global audio perception, improving naturalness and lip-sync over prior SOTA from a single portrait + audio [b10].
- Ditto (Ant, ACM MM 2025, open) is a motion-space DiT that explicitly decouples motion from identity for controllable, streaming, real-time talking heads with low first-frame latency — the strongest open real-time option, at the cost of less full-body/photoreal richness than OmniHuman [b11].
- HunyuanVideo-Avatar (Tencent, open, MM-DiT 13B) drives one image + audio at up to 720×1216 with an Audio-Emotion Module and Face-Aware Audio Adapter (multi-character), scoring identity 4.84/5 and lip-sync 4.65/5 in user study — but a 10 s clip needs ~60 min on a 96 GB GPU (not real-time) [b12].
- EchoMimic (Ant, AAAI 2025, open) is a Stable-Diffusion+ReferenceNet system that combines audio with EDITABLE facial landmarks for controllable, lifelike single-image portrait animation; EchoMimicV2 (2024-11) extended to half-body [b13].
- The 2025-2026 frontier is splitting into (a) closed photoreal full-body avatars (OmniHuman-1/1.5) and (b) open real-time streaming DiT heads — StreamAvatar, Live Avatar (infinite-length streaming), and DiTalker/EditYourself add streaming, style control and one-step distillation (e.g. TurboTalk) to cut diffusion latency [b14].

## Deep Read Notes

### OmniHuman-1 (read in full) [b1]
- Inputs: single reference image + any of {audio, pose, text}; multimodal "omni-conditions mixed training" lets the model reuse data normally discarded for weak-signal training. Audio path = wav2vec → MLP → cross-attention; pose path = pose guider concatenated/channel-stacked; reference reuses the DiT backbone itself (parameter-efficient, no separate ReferenceNet) with modified 3D RoPE to separate reference vs video tokens.
- Output: 2D video; arbitrary aspect ratio (face close-up → full body) and "arbitrary length within memory constraints." Backbone = MMDiT on pretrained Seaweed + 3D causal VAE, flow-matching objective, 3-stage progressive training.
- Quality (CelebV-HQ / RAVDESS): IQA 3.875, Sync-C 5.199, FID 31.435, FVD 46.393 — all best vs baselines incl. Loopy, SadTalker, Hallo, CyberHost.
- Training data: 18.7K hrs human video; only ~13% passed strict lip-sync/pose filtering — the key contribution is using the mixed-condition trick to still exploit the other 87%.
- Compute: inference speed and VRAM NOT disclosed; explicitly not positioned as real-time. Closed source; access is via ByteDance API / Replicate `bytedance/omni-human`, not weights.
- Failure modes from ablations: too much weak-condition data → poorer audio correlation; high pose ratio → less gesture diversity; low reference ratio → error accumulation, noise/color shift. Relevant to a single-photo deployment: identity drift and color shift over long clips are the main risks.

### HunyuanVideo-Avatar (read in full) [b12]
- Inputs: single character image + audio (required); optional emotion reference image; optional face mask for multi-character. Audio = Whisper features; face detection = InsightFace.
- Output: trained 704×704 → 704×1216, tested 720×1216; base 129 frames (~5 s at ~25 fps), extended via Time-aware Position Shift Fusion (same trick as Sonic). Base model = HunyuanVideo-I2V 13B params, MM-DiT.
- Quality (HDTF): FID 38.01, FVD 358.71, Sync-C 5.30, IQA 3.99; user study identity 4.84/5, lip-sync 4.65/5.
- Capture/compute: TRAINING used 160× 96 GB GPUs, batch 40. INFERENCE: a 10 s 720×1216 clip at 50 steps ≈ 60 minutes — explicitly not real-time; this is the practical cost ceiling of the open photoreal route.
- Limitations: needs an emotion reference image (can't infer emotion from audio alone); cannot model dynamic emotional transitions within one audio segment; slow.
- Open: weights + code on HuggingFace (`tencent/HunyuanVideo-Avatar`) and GitHub; license not stated in paper.

### EMO2 (read in full) [b3]
- Inputs: single reference image + audio. Output: 704×512 2D video, 24 fps, 24-frame clips. Two-stage: Stage-1 = 24 DiT blocks (hidden 512) predict 134 MANO hand params/frame from wav2vec+style/speed embeddings; Stage-2 = ReferenceNet latent-diffusion U-Net renders pixels from hand poses + audio + keypoint maps, with a pose discriminator. ReferenceNet does identity preservation via cross-attention.
- Quality (EMTD): FVD 129.41, FID 27.28, Sync-C 4.58, hand-motion diversity HKV 0.198, identity CSIM 0.650.
- Compute: Stage-1 1×A100, Stage-2 4×A100 training; inference not stated as real-time. ~275 hrs training data (MOSEI, AVSPEECH + collected). Closed source.
- Design insight ("pixels prior IK"): only generate hand end-effector poses and let the pretrained video model implicitly recover full-body kinematics; intentional temporal misalignment of keypoint guidance gives the model creative freedom.

### LivePortrait (read in full) [b8]
- IMPORTANT for a still-photo+audio use case: LivePortrait is VIDEO-DRIVEN and implicit-keypoint based (NOT diffusion, NOT audio). It takes an appearance reference image and derives motion from a driving video; stitching + eye/lip retargeting MLPs add negligible overhead and let you scalar-control eye/lip openness.
- Speed: 12.8 ms/frame on RTX 4090 PyTorch (~78 fps) — by far the fastest renderer here; real-time. Trained on ~69M frames, mixed image-video. Open source (KwaiVGI GitHub).
- Implication: to use it for an audio-only avatar you bolt an audio→motion/keypoint model in front (this is exactly what Ditto/V-Express-style pipelines do), so LivePortrait is best viewed as the real-time renderer, not the audio engine.

## Gaps
- Hallo3 exact VRAM/GPU and per-clip inference time were NOT found in the paper text I could fetch; community reports suggest a heavy DiT (CogVideoX-class, tens of GB VRAM, minutes per clip) but I could not confirm a primary number — verify on the GitHub README before planning.
- Licenses are inconsistently documented: HunyuanVideo-Avatar and Hallo2/3 publish weights but the papers don't state license terms; OmniHuman-1/1.5 and EMO/EMO2 are closed with no weights at all (API-only or demo-only) — confirm commercial-use terms before any deployment of a real person's likeness.
- I did not get standalone primary quality numbers (FID/FVD/Sync-C) for Sonic, V-Express, EchoMimic, Loopy, or Ditto from their own pages in this pass — only OmniHuman's cross-comparison table where Loopy is the strongest baseline; a second pass on each project page is needed for apples-to-apples.
- None of these systems is a true single-photo→rigged 3D avatar; they output 2D video (or implicit-keypoint warps). I found no mature open audio-driven system that drives an actual 3D mesh/Gaussian-splat head of a specific person from one photo at comparable realism — the 3D-Gaussian-avatar line generally still needs multi-view or video capture, which is OUT of the stated "still photo only" constraint and a known gap for this project.
- "Compute no object, best quality first" honest verdict: the realism ceiling from a single still is OmniHuman-1.5 (closed, best) > HunyuanVideo-Avatar / Hallo2 (open, photoreal but ~minutes-to-an-hour per short clip) > Ditto/LivePortrait (open, real-time but lower fidelity / video-driven). All still exhibit identity drift, lip/teeth artifacts on hard phonemes, and reduced realism on large head turns — there is no artifact-free option from a single photo as of 2026-06.
