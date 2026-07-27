# Task B — Open / Open-Weight TTS Voice-Cloning Systems (mid-2026)

AS_OF: 2026-06-22
Scope: cloning a specific person from VERY limited archival audio (seconds-to-minutes of usable connected speech). Emphasis on min reference duration, fine-tune support, quality numbers, license/openness, fidelity.

## Sources
[1] arXiv (Chen et al.) — F5-TTS: A Fairytaler that Fakes Fluent and Faithful Speech with Flow Matching (HTML v1) | https://arxiv.org/html/2410.06885v1 | academic | As-Of 2025-05-20 (v3) | Authority 9
[2] arXiv — F5-TTS abstract page (2410.06885) | https://arxiv.org/abs/2410.06885 | academic | As-Of 2025-05-20 | Authority 9
[3] Hugging Face — SWivid/F5-TTS license discussion #7 + model card (CC-BY-NC-4.0 weights, MIT code) | https://huggingface.co/SWivid/F5-TTS/discussions/7 | official | As-Of 2026 | Authority 8
[4] Hugging Face — FunAudioLLM/CosyVoice2-0.5B model card | https://huggingface.co/FunAudioLLM/CosyVoice2-0.5B | official | As-Of 2026 | Authority 9
[5] arXiv (Du et al.) — CosyVoice 3: Towards In-the-wild Speech Generation via Scaling-up and Post-training (HTML v1) | https://arxiv.org/html/2505.17589v1 | academic | As-Of 2025-05-23 | Authority 9
[6] GitHub — FunAudioLLM/CosyVoice LICENSE (Apache-2.0) | https://github.com/FunAudioLLM/CosyVoice/blob/main/LICENSE | official | As-Of 2026 | Authority 9
[7] Fish Audio Blog — Launching Fish Audio S1: A Frontier TTS Audio Foundation Model | https://fish.audio/blog/introducing-s1/ | official | As-Of 2025 | Authority 7
[8] Hugging Face — fishaudio/openaudio-s1-mini model card (CC-BY-NC-SA-4.0, 0.5B distill) | https://huggingface.co/fishaudio/openaudio-s1-mini | official | As-Of 2026 | Authority 8
[9] arXiv (Wang et al.) — MaskGCT: Zero-Shot TTS with Masked Generative Codec Transformer (abstract) | https://arxiv.org/abs/2409.00750 | academic | As-Of 2024-10-20 (v3) | Authority 9
[10] GitHub — open-mmlab/Amphion MaskGCT README (WER/SIM-O tables) | https://github.com/open-mmlab/Amphion/blob/main/models/tts/maskgct/README.md | official | As-Of 2026 | Authority 8
[11] arXiv (Ye et al.) — LLaSA: Scaling Train-Time and Test-Time Compute for LLaMA-based Speech Synthesis (HTML) | https://arxiv.org/html/2502.04128 | academic | As-Of 2025-02-22 | Authority 9
[12] Hugging Face — coqui/XTTS-v2 model card (6s ref, 24kHz, CPML) | https://huggingface.co/coqui/XTTS-v2 | official | As-Of 2026 | Authority 8
[13] arXiv (Li et al.) — StyleTTS 2: Towards Human-Level TTS via Style Diffusion & Adversarial Training | https://arxiv.org/abs/2306.07691 | academic | As-Of 2023-11 (NeurIPS'23) | Authority 9
[14] GitHub — neonbjb/tortoise-tts README (Apache-2.0, ~3 clips ~10s, 22.05kHz) | https://github.com/neonbjb/tortoise-tts | official | As-Of 2026 | Authority 8
[15] CodeSOTA — Best TTS Models 2026: Elo, Vendors, and Open-Weight Voice AI | https://www.codesota.com/guides/tts-models | secondary-industry | As-Of 2026 | Authority 5

## Findings
- F5-TTS clones zero-shot from a short prompt (LibriSpeech-PC eval used 4–10s utterances; community guidance ~5–15s), outputs 24kHz mel, and scores WER 2.42 / SIM-o 0.66 on LibriSpeech-PC and WER 1.83 / SIM-o 0.67 on Seed-TTS test-en [1][15].
- F5-TTS weights are CC-BY-NC-4.0 (NON-commercial, because trained on Emilia) while the code is MIT; commercial use requires retraining from scratch on a permissive dataset [3].
- CosyVoice 2 (0.5B) clones from ~3s of prompt speech, hits WER 2.57 (en) / CER 1.45 (zh) with SIM 0.659 (en) / 0.757 (zh), runs at 25Hz token rate with 150ms streaming latency, and is Apache-2.0 (commercial OK) [4][6].
- CosyVoice 3 scales the LM to 1.5B (CFM 300M) on ~1M hours and reaches WER 1.45 (en) / CER 0.71 (zh) with SIM 0.695–0.775 (WavLM) on Seed-TTS, with MOS ~4.5+, and ships under the same Apache-2.0 repo [5][6].
- Fish-Speech / OpenAudio S1 needs ~10–30s of reference audio, reports WER 0.8% / CER 0.4% on Seed-TTS Eval (S1-mini ~0.011 WER / 0.005 CER en), but the open weights (S1-mini 0.5B distill) are CC-BY-NC-SA-4.0 — commercial use needs a separate license from Fish Audio [7][8].
- MaskGCT (Amphion, ~100K hrs Emilia, masked generative codec transformer) scores WER 2.634 / SIM-O 0.687 on LibriSpeech test-clean and WER 2.623 / SIM-O 0.717 on Seed-TTS test-en, generating tokens in parallel (non-autoregressive) [9][10].
- Llasa is a LLaMA-based codec LM in 1B/3B/8B sizes at 16kHz; the 8B with test-time search reaches Seed-TTS test-en WER 1.39 / SIM-O 0.783 and test-zh CER 0.47 / SIM-O 0.825, and clones from a "couple seconds" prompt [11].
- XTTS-v2 (Coqui) clones from a 6s clip, outputs 24kHz, supports fine-tuning (GPT encoder training recipe + Gradio UI), but is licensed CPML (commercial restricted), 1388 Elo on the CodeSOTA arena [12][15].
- StyleTTS 2 uses ~3s reference for zero-shot (15–30s recommended), outputs 24kHz, and reports MOS 4.55 (> ground-truth 4.23 on LJSpeech) with ~3.2% WER / 0.8% CER; inference depends on a GPL pkg but an MIT (gruut) path exists [13].
- Tortoise-TTS needs at least ~3 clips of ~10s each (~22.05kHz), is Apache-2.0 (commercial OK), but is autoregressive+diffusion and extremely slow (minutes per sentence on older GPUs) [14].
- Least-reference cloners: CosyVoice 2/3 and StyleTTS 2 (~3s), Llasa (~couple seconds), then F5-TTS (~5–15s); Fish/OpenAudio S1 and Tortoise want more (10–30s / multiple 10s clips) [4][5][13][11][1][7][14].
- Permissive-license open weights for commercial cloning: CosyVoice 2/3 (Apache-2.0), Tortoise (Apache-2.0), StyleTTS 2 (MIT path); restricted: F5-TTS, OpenAudio S1 (NC), XTTS-v2 (CPML) [6][14][13][3][8][12].

## Deep Read Notes

### F5-TTS [1][2][3]
- Architecture: fully non-autoregressive, conditional flow-matching over a Diffusion Transformer (DiT) on mel-spectrograms; no explicit duration model; uses ConvNeXt text embedding, filler-padding + masking, and "Sway Sampling" at inference.
- Reference audio: no hard minimum stated in the paper; LibriSpeech-PC eval prompts were 4–10s utterances, and the released app/community guidance points to ~5–15s of clean reference for best clone fidelity.
- Benchmarks (32 NFE + Sway): LibriSpeech-PC test-clean WER 2.42 / SIM-o 0.66; Seed-TTS test-en WER 1.83 / SIM-o 0.67 / CMOS +0.31 / SMOS 3.89; Seed-TTS test-zh WER 1.56 / SIM-o 0.76. RTF 0.15–0.31; trained on a public 100K-hour multilingual (Emilia) set.
- Output: 100-dim log-mel at 24kHz sample rate.
- Failure modes: word-skipping on stacks of repeated words; the reproduced E2-TTS baseline showed catastrophic hard-case failures (~7% of samples WER>>50%) — F5-TTS is more robust but not immune on hard sentences.
- Fine-tune: training/finetune code is open (MIT) and the community fine-tunes it heavily; weights are CC-BY-NC-4.0 (non-commercial) because of the Emilia training data.

### CosyVoice 2 / 3 [4][5][6]
- CosyVoice 2 = 0.5B LM with FSQ speech tokens + chunk-aware causal flow matching; supports streaming (text-in/audio-out, ~150ms latency) and zero-shot in-context cloning from ~3s of prompt speech; WER 2.57 (en) / CER 1.45 (zh), SIM 0.659 (en) / 0.757 (zh).
- CosyVoice 3 scales LM 0.5B→1.5B and CFM 100M→300M, training 10K→1M hours across 9 languages + 18 Chinese dialects, adds RL/DPO post-training and the new CV3-Eval multilingual benchmark; Seed-TTS WER 1.45 (en) / CER 0.71 (zh), SIM 0.695–0.775 (WavLM) / 0.784–0.836 (ERes2Net), MOS ~4.5+ (matches/exceeds human in English).
- Fine-tune: repo provides full inference + training + deployment; SFT and instruct fine-tuning supported; many community finetunes exist on HF.
- License: Apache-2.0 (code AND weights) — commercial use allowed, the most permissive of the high-similarity systems. Output ~24kHz (token rate 25Hz/50Hz); exact output Hz not nailed in the paper.
- Best fit for very-limited-audio cloning: lowest stated reference need (~3s) plus commercial-friendly license.

### MaskGCT [9][10]
- Two-stage masked generative codec transformer (Amphion): stage 1 text→semantic SSL tokens, stage 2 semantic→acoustic tokens; fully parallel (non-AR), no text-speech alignment or phone duration needed; trained on 100K-hour Emilia.
- Benchmarks: LibriSpeech test-clean WER 2.634 / SIM-O 0.687 (2.012 / 0.723 with ground-truth length); Seed-TTS test-en WER 2.623 / SIM-O 0.717 (1.283 / 0.760 with GT length) — competitive similarity, slightly higher WER than F5/CosyVoice.
- Reference duration and output sample rate are not stated in the README/abstract (gap). Four downloadable checkpoints (semantic codec, acoustic codec, T2S, S2A); inference-focused, no documented first-class finetune recipe.

## Gaps
- Min reference duration: NOT explicitly stated for F5-TTS (inferred 4–10s from eval / ~5–15s community), MaskGCT, CosyVoice 3 (CosyVoice 2 ~3s assumed to carry over), or Llasa (only "a couple seconds" qualitatively).
- Output sample rate unconfirmed for MaskGCT and CosyVoice 3 (CosyVoice family commonly 24kHz; Llasa codec is 16kHz, lower fidelity than 24kHz peers — relevant for archival voice quality).
- Llasa license: checkpoints + training code released openly, but the precise license string (e.g. CC-BY vs Apache) was not confirmed in the paper text — verify on the HKUST-Audio HF repos before any commercial use.
- Fish/OpenAudio S1: full 4B S1 weights appear gated/commercial; only the 0.5B S1-mini distill is openly downloadable (CC-BY-NC-SA-4.0). Few-shot fine-tune support for S1 is community-only, not officially documented.
- Few-shot fine-tuning data requirements (how many minutes to meaningfully improve a clone) are not quantified by any vendor — all headline numbers are zero-shot; for our seconds-to-minutes archival case, zero-shot is the realistic path and fine-tuning gains are unproven from published sources.
- One secondary source [15] lists F5-TTS as "CC-BY-NC 4.0" and Fish as "Apache 2.0," but primary sources [3][8] show F5-TTS weights CC-BY-NC-4.0 (code MIT) and OpenAudio S1-mini CC-BY-NC-SA-4.0 — trust the primary model cards over the aggregator.
