# Sources — voice mimicker (deep research, AS_OF 2026-06-22)

## Source material (audio)
- Detroit Catholic — great-niece (Sr. Herkenrath) on the June 1945 home recording, "the only recording":
  https://detroitcatholic.com/news/elizabeth-wong-barnstead/great-niece-of-ven-solanus-casey-shares-memories-hopes-for-beatification
- "Echoes of Solanus Casey" CD (Father Solanus Guild, 2000; OCLC 187781405) — embeds two tracks of his real
  voice/violin: https://giftshop.solanuscenter.org/product/echoes-of-solanus-casey/ ·
  https://search.worldcat.org/title/echoes-of-solanus-casey/oclc/187781405
- Aleteia — 1945 audio in "The Violinist" (2024): https://aleteia.org/2024/10/26/hear-blessed-solanus-casey-play-violin-in-new-documentary/
- Capuchin archive (Provincial Archivist Junia Yasenov; AV section): https://ischool.sjsu.edu/webcast/capuchin-archives-detroit-michigan-usa
- "Priest, Porter, Prophet" (Ignatius) — "rare footage" = 1987 exhumation, NOT motion of him alive:
  https://ignatius.com/solanus-casey-scpp2m/
- No authentic moving film footage of him alive was found (high confidence).

## Restoration (run before cloning)
- iZotope — order of audio repair (declick→denoise; deepest damage first): https://www.izotope.com/en/learn/order-of-audio-repair-operations.html
- CEDAR Audio — declick/decrackle/dethump: https://cedaraudio.com/plugins/declick
- DeepFilterNet (MIT/Apache, 48kHz denoise): https://github.com/Rikorose/DeepFilterNet
- NVIDIA CleanUNet: https://research.nvidia.com/labs/adlr/projects/cleanunet/
- Resemble Enhance (MIT): https://github.com/resemble-ai/resemble-enhance
- AudioSR (super-res; OPTIONAL, non-canonical — invents highs): https://github.com/haoheliu/versatile_audio_super_resolution
- VoiceFixer: https://arxiv.org/abs/2204.05841 · Audio-SR survey 2026: https://arxiv.org/html/2605.16681v1
- Adobe Enhance "hallucinates" on respeak (avoid as canonical): https://thepodcastconsultant.com/blog/adobe-podcast-enhance

## Cloning (zero-shot; best-version-first)
- CosyVoice 2 (Apache-2.0, ~3s ref, tags): https://huggingface.co/FunAudioLLM/CosyVoice2-0.5B ·
  CosyVoice 3: https://arxiv.org/html/2505.17589v1 · license: https://github.com/FunAudioLLM/CosyVoice/blob/main/LICENSE
- F5-TTS (weights CC-BY-NC): https://arxiv.org/abs/2410.06885
- Llasa: https://arxiv.org/html/2502.04128 · MaskGCT: https://arxiv.org/abs/2409.00750
- StyleTTS2 (timbre/prosody sliders): https://arxiv.org/abs/2306.07691 · XTTS-v2: https://huggingface.co/coqui/XTTS-v2
- OpenAudio S1 (whisper/soft-tone/breath markers): https://huggingface.co/fishaudio/openaudio-s1-mini
- Seed-VC (timbre conversion of an actor take, GPL-3.0): https://github.com/Plachtaa/seed-vc
- ElevenLabs PVC (needs ≥30 min clean — NOT viable here): https://elevenlabs.io/docs/eleven-creative/voices/voice-cloning/professional-voice-cloning
- Precedents: Warhol (~3 min, actor-blended) https://voicebot.ai/2022/02/24/ai-powered-andy-warhol-voice-reads-his-diary-in-new-netflix-documentary/ ·
  Bourdain https://www.npr.org/2021/07/16/1016838440/ · JFK Unsilenced (831 recordings) https://www.accenture.com/us-en/case-studies/interactive/jfk-unsilenced-ai-audio-speech

(Full cited research notes: `../../Style-Mimickry-Research/voice-research-notes/`.)
