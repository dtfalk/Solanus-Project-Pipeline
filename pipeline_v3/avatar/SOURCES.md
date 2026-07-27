# Sources — digital avatar (deep research, AS_OF 2026-06-22)

## Source material (visual)
- Capuchin Province of St. Joseph archives (Detroit; Archivist Junia Yasenov; glass-plate negatives, AV):
  https://ischool.sjsu.edu/webcast/capuchin-archives-detroit-michigan-usa
- *Blessed Solanus Casey* (Images of America), ~200 photos from the archives, ISBN 9781467102544:
  https://www.arcadiapublishing.com/products/9781467102544
- Wikimedia Commons set (top portrait 2361×3500): https://commons.wikimedia.org/wiki/Category:Solanus_Casey
- No authentic moving film of him alive ("rare footage" = stills + 1987 exhumation):
  https://ignatius.com/solanus-casey-scpp2m/

## Face/head reconstruction
- FLAME (identity/geometry backbone): https://github.com/TimoBolkart/FLAME-Universe
- Pixel3DMM (single-image → FLAME, SOTA geometry): https://simongiebenhain.github.io/pixel3dmm/ (arXiv:2505.00615)
- EMOCA: https://arxiv.org/abs/2204.11312
- FaceLift (1 image → photoreal 3DGS head): https://arxiv.org/html/2412.17812v1
- Avat3r (~4 images → animatable Gaussian head): https://tobias-kirschstein.github.io/avat3r/
- GAGAvatar: https://arxiv.org/abs/2410.07971 · HeadGAP: https://arxiv.org/abs/2408.06019 ·
  GaussianAvatars: https://arxiv.org/abs/2312.02069
- Old-photo restore: CodeFormer https://arxiv.org/abs/2206.11253 · Selective-Guided Diffusion
  https://arxiv.org/abs/2510.12114 · Time-Travel Rephotography https://arxiv.org/abs/2012.12261

## Talking head (audio-driven)
- OmniHuman-1 / 1.5 (closed, max quality): https://arxiv.org/html/2502.01061v1 · https://arxiv.org/html/2508.19209v1
- EMO / EMO2 (Alibaba): https://arxiv.org/abs/2402.17485 · https://arxiv.org/html/2501.10687v1
- Hallo2 (4K/long) https://arxiv.org/abs/2410.07718 · Hallo3 https://arxiv.org/html/2412.00733v3
- HunyuanVideo-Avatar (open photoreal): https://arxiv.org/html/2505.20156v2
- SadTalker / LivePortrait / V-Express / Sonic / Ditto:
  https://liveportrait.github.io/ · https://arxiv.org/abs/2411.19509 · https://arxiv.org/abs/2411.16331

## Full body, habit, motion, relight, runtime
- SMPL-X: https://github.com/vchoutas/smplx · LHM (1 image → animatable body): https://arxiv.org/abs/2503.10625 ·
  SiTH: https://arxiv.org/abs/2311.15855 · ExAvatar: https://mks0601.github.io/ExAvatar/
- AMASS (mocap): https://arxiv.org/abs/1904.03278 · EMAGE (co-speech gesture, SMPL-X): https://arxiv.org/abs/2401.00374
- Cloth: ContourCraft https://dl.acm.org/doi/10.1145/3641519.3657408 · Dress-1-to-3 https://arxiv.org/abs/2502.03449
- Relightable codec avatars (need a light stage — out of reach): RGCA https://shunsukesaito.github.io/rgca/ ·
  URAvatar https://arxiv.org/html/2410.24223v1 · Full-Body GCA https://arxiv.org/abs/2501.14726
- Approx relight: GaussianShader https://asparagus15.github.io/GaussianShader.github.io/ · PHORHUM https://phorhum.github.io/
- Runtime: MetaHuman Animator https://dev.epicgames.com/documentation/metahuman · NVIDIA ACE Audio2Face-3D
  https://docs.nvidia.com/ace/ace-unreal-plugin/2.5/ · XScene-UEPlugin https://github.com/xverse-engine/XScene-UEPlugin

(Full cited research notes: `../../Style-Mimickry-Research/digital-human-research/`.)
