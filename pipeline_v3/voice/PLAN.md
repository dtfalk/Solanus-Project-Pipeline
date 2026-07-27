# Voice mimicker — the best-version-first build plan

Grounded in deep research (AS_OF 2026-06-22; ~47 cited sources in `SOURCES.md`). Compute/storage is no
object — build at full fidelity, downsample later. The real constraint is the **source reality**: one short,
noisy, soft 1945 recording. (Project authorized via the vice-postulator, Fr. Ed Foley.)

## Headline
Exactly one authentic recording exists; there is **no corpus to fine-tune**. So: **restore conservatively,
then zero-shot clone from the cleaned reference**, keep a fully-disclosed period-correct **human-actor
stand-in** as the fallback, and never promise an "indistinguishable" clone. Honest ceiling = a recognizable
approximation that will likely blend AI output with a human actor (as Netflix's Warhol voice did from
*more* and cleaner audio).

## Stage 0 — Acquire (do first)
- Buy the **"Echoes of Solanus Casey" CD** (Solanus Casey Center gift shop, SKU 103336) for the two released
  tracks of his real voice/violin — immediate analysis material.
- Request the **higher-fidelity master** (original 7-inch disc / first-gen cassette transfer) from the
  **Capuchin Province of St. Joseph Archives**, Provincial Archivist **Junia Yasenov**, 1820 Mt. Elliott St.,
  Detroit MI 48207, (313) 579-2100. Ask for the "June 1945 McCluskey first-Mass recording." (Fr. Ed Foley,
  vice-postulator, is the project contact.)
- Place raw transfers in `data/raw/` (work only on copies; keep every intermediate).

## Stage 1 — Restoration (run before any cloning)
Documented archival order — **deepest damage first**, two *light* denoise passes (a single aggressive pass
above ~18 dB produces hollow/phasey artifacts):
1. digitize/transfer at max bit-depth/sample-rate;
2. **de-click → de-crackle → de-hum** (iZotope RX or CEDAR = archival standard for disc damage);
3. **gentle broadband denoise** ×2 light passes — open-source: DeepFilterNet / NVIDIA CleanUNet / Resemble Enhance.
4. **Keep the conservative, non-respeak'd version as the canonical clone reference.** Treat bandwidth
   extension / super-resolution (AudioSR, VoiceFixer) and any generative "re-speak" enhancer (Adobe Podcast)
   as a **separate, optional, clearly-labeled output only** — they *invent* high-frequency timbre the disc
   never captured, which a cloner would then learn as if real.

`restore_audio.py` wires this order with TODO hooks for the commercial tools.

## Stage 2 — Reference curation
Hand-pick the cleanest **seconds-to-minutes of connected speech** (prayer/greeting/poem, not singing/violin)
into several **5–15 s** clips (CosyVoice wants ~3 s, F5 ~5–15 s). `make_references.py`.

## Stage 3 — Cloning (zero-shot; this much audio does not justify fine-tuning)
- **Primary: CosyVoice 3** (fallback CosyVoice 2) — best similarity + **Apache-2.0** (commercial-safe) +
  smallest reference need (~3 s) + richest control (natural-language instruction + `[breath]`/`<strong>` tags
  to layer "soft/gentle/slow/breathy" onto the reference).
- **A/B comparators: Llasa-8B** (top similarity; 16 kHz codec = lower fidelity) and **F5-TTS** (strong, simple;
  CC-BY-NC weights → non-commercial).
- **For the wispy/breathy character: OpenAudio S1** `(whisper)`/`(soft tone)`/`(breath)` markers (NC license;
  use at least as a style reference). **StyleTTS 2** if you want continuous timbre/prosody sliders.
- **Accent/era control (no model has an "era" knob), in priority:** (1) the reference clip itself carries the
  Irish-American accent — curate the most accent-rich clean clip; (2) era via the *text* (period lexicon, no
  anachronisms); (3) soft/breathy via style markers; (4) if zero-shot over-normalizes the accent, route through
  an accent-control front-end (SpeechAccentLLM-style) or record a modern actor and convert timbre with
  **Seed-VC** to keep natural 1940s prosody.
- **ElevenLabs Professional Voice Clone is NOT viable** (needs ≥30 min, ideally 2–3 h of clean audio).
`clone_voice.py` is a CosyVoice-first stub.

## Stage 4 — Speak (integrate with the text mimicker)
`speak.py`: text from `../mimicker` → **normalize** (expand numbers/dates/abbrev.; guard "Mr."/"Ave." false
breaks) → **sentence-chunk** (≤~20 s) → synth each chunk against **one frozen reference embedding** with
identical settings → **stitch**. Pace with punctuation; expressivity via inline tags (CosyVoice/S1) or SSML
(`<break>`/`<prosody>`) on SSML engines. Freeze a single reference embedding for cross-utterance consistency.

## Stage 5 — Evaluation
- **Speaker similarity** (WavLM/ECAPA cosine to the restored reference) — but remember it certifies similarity
  to a *degraded* target; **intelligibility** (ASR WER); **human review** by people who know the recording.
- Watch the central failure mode: enhancement + zero-shot cloners **average away** the soft/wispy/damaged
  quality toward a generic healthy studio voice — i.e. erase the very thing that makes him recognizable.
`evaluate_voice.py`.

## Compute / storage
Inference-only zero-shot runs on a single ~24 GB GPU; restoration models are light. Keep ALL intermediates
(raw transfer, declicked, denoised, optional super-res, curated clips, every render). No training corpus.

## Honest limits (state these in any output)
Seconds-to-minutes of noisy, band-limited, soft/damaged speech is far below every documented minimum.
Deliverable = an evocative, **estate-disclosed, likely actor-assisted reconstruction**, not a faithful
reproduction. Precedents (Warhol, Bourdain, JFK Unsilenced) used more/cleaner audio and/or an actor and/or
estate sanction.

## Open questions
Exact runtime/content of the 1945 recording; whether to preserve-and-intensify the accent (unbenchmarked)
vs neutralize; actor-assist vs pure-synthetic.
