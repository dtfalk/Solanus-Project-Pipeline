# Task C — Hosted Voice Cloning & The Reality of Cloning a Voice From ~1945 Archival Audio

AS_OF: 2026-06-22
Context: cloning one specific deceased person's voice from a single ~1945 home disc; under 5 minutes total; only seconds-to-minutes of usable connected speech; soft/wispy diphtheria-damaged voice; noisy, band-limited.

## Sources
[1] ElevenLabs — Professional Voice Cloning (docs) | https://elevenlabs.io/docs/eleven-creative/voices/voice-cloning/professional-voice-cloning | official | 2026-06-22 | 10
[2] ElevenLabs — Instant Voice Cloning (docs) | https://elevenlabs.io/docs/eleven-creative/voices/voice-cloning/instant-voice-cloning | official | 2026-06-22 | 10
[3] ElevenLabs — 7 Tips for a Professional-Grade Voice Clone (blog) | https://elevenlabs.io/blog/7-tips-for-creating-a-professional-grade-voice-clone-in-elevenlabs | official | 2026-06-22 | 8
[4] ElevenLabs — Pricing | https://elevenlabs.io/pricing | official | 2026-06-22 | 9
[5] Cartesia — Sonic / Voice Cloning (product + docs) | https://www.cartesia.ai/product/voice-cloning/ | official | 2026-06-22 | 8
[6] OpenAI — Navigating the challenges and opportunities of synthetic voices (Voice Engine) | https://openai.com/index/navigating-the-challenges-and-opportunities-of-synthetic-voices/ | official | 2024-03-29 | 9
[7] Microsoft — Custom Neural Voice overview / VALL-E project | https://learn.microsoft.com/en-us/azure/ai-services/speech-service/custom-neural-voice | official | 2026-06-22 | 9
[8] Resemble AI — Andy Warhol voice (case study / PRWeb) | https://www.prweb.com/releases/resemble-ai-creates-andy-warhol-docu-series-narration-using-3-minutes-of-original-voice-recordings-800189343.html | secondary-industry | 2022-02-24 | 7
[9] Voicebot.ai — AI-Powered Andy Warhol Voice Reads His Diary | https://voicebot.ai/2022/02/24/ai-powered-andy-warhol-voice-reads-his-diary-in-new-netflix-documentary/ | journalism | 2022-02-24 | 7
[10] NPR — AI brought Anthony Bourdain's voice back to life. Should it have? (Roadrunner) | https://www.npr.org/2021/07/16/1016838440/ai-brought-anthony-bourdains-voice-back-to-life-should-it-have | journalism | 2021-07-16 | 8
[11] NPR — Edith Piaf biopic to use AI to recreate her voice | https://www.npr.org/2023/11/15/1213130056/with-the-help-of-ai-iconic-french-singer-edith-piaf-will-narrate-her-own-story | journalism | 2023-11-15 | 8
[12] Accenture/Rothco — "JFK Unsilenced" case study (CereProc) | https://www.accenture.com/us-en/case-studies/interactive/jfk-unsilenced-ai-audio-speech | secondary-industry | 2026-06-22 | 6
[13] arXiv 2110.03347 — Cloning one's voice using very limited data in the wild | https://arxiv.org/pdf/2110.03347 | academic | 2021-10-07 | 8
[14] ResearchGate — Data-Efficient Voice Cloning from Noisy Samples w/ Domain Adversarial Training | https://www.researchgate.net/publication/343568692 | academic | 2020-07-29 | 7
[15] Wikipedia — ELVIS Act (Ensuring Likeness Voice and Image Security Act) | https://en.wikipedia.org/wiki/ELVIS_Act | secondary-industry | 2026-06-22 | 7
[16] Descript — Voice Cloning / Overdub | https://www.descript.com/tools/voice-cloning | official | 2026-06-22 | 7

## Findings
1. ElevenLabs PVC documents a hard floor of "the bare minimum we recommend is 30 minutes" and a recommended "closer to 2-3 hours" / "as close to three hours as possible" of clean, single-speaker, noise-free audio [1].
2. ElevenLabs Instant Voice Clone (IVC) works from "at least 1 minute," is best at 1-2 minutes, and explicitly warns that more than ~3 minutes "will yield little improvement and can... even be detrimental" [2].
3. ElevenLabs PVC requires the Creator plan ($22/mo, 1 PVC + 3 IVC) or above, and at IVC/PVC setup the user must affirm "you have the right and consent to clone the voice"; PVC adds voice verification [1][2][4].
4. ElevenLabs' own guidance is blunt: "garbage in, garbage out is doubly important," the "model learns background noise as part of the voice," and "the AI can only effectively recreate what it has been shown" — i.e., there is no recovery of timbre that the recording never captured [3].
5. Competitor instant-clone minimums are tiny on paper but assume clean input: Cartesia Sonic from ~3 seconds (and ~15s for higher fidelity, Pro cloning from 30 min), Resemble AI ~3-5 seconds, Descript Overdub ~60 seconds (formerly 10+ min) [5][16].
6. OpenAI Voice Engine can clone from a 15-second sample but as of 2026 is still NOT publicly released — limited to a small set of vetted partners on safety grounds [6].
7. Microsoft restricts its highest-fidelity cloning: Azure Custom Neural Voice (Pro) is limited/gated-access and consent-gated, and VALL-E (which reached "human parity" zero-shot in 2024) was never publicly released over deepfake-misuse concerns [7].
8. The closest real precedent — Netflix's "The Andy Warhol Diaries" — cloned Warhol from only ~3 minutes 12 seconds of 1970s/80s archival audio, BUT the broadcast voice was the AI output blended with a human actor's (Bill Irwin) performance, not raw clone output [8][9].
9. Roadrunner (Bourdain) used an unnamed TTS vendor for under ~60 seconds of synthesized lines and triggered major backlash over non-disclosure and unclear consent — the cautionary case for undisclosed deceased-voice cloning [10].
10. By contrast, the convincing historical reconstructions used enormous clean corpora: "JFK Unsilenced" drew on 831 analog recordings / 116,777 voice samples, showing the data scale behind a truly faithful result [12].
11. Academic limited-data/noisy work clones from ~10-30 seconds "in the wild," but reports prosody instability and quality degradation, and finds denoising/dereverb pre-processing or domain-adversarial training are needed just to make noisy data usable — not to make it sound original [13][14].
12. Legally, the Tennessee ELVIS Act (effective 2024) and the EU AI Act's Article 50 transparency/labeling duties cover voice; deceased-person voice rights persist post-mortem (ELVIS Act: estate consent for ~10 years of commercial exploitation), so consent/estate authorization and disclosure are the governing requirements in 2026 [15].

## Deep Read Notes

### [1] ElevenLabs — Professional Voice Cloning docs (read in full)
- Audio duration: minimum recommended = 30 minutes; recommended/optimal = 2-3 hours ("as close to three hours as possible"). The model is trained on the actual uploaded audio, so its characteristics become part of the model weights.
- Quality spec: -23 dB to -18 dB RMS, true peak -3 dB; single speaking voice throughout; "noise-free," "clean, uncluttered audio," no background noise / room reverb / echo; spoken voice only (no singing).
- Consent/verification: a voice-verification step is required; "for now, we only allow you to clone your own voice" — meaning cloning a third party (let alone a deceased person) is outside the documented/sanctioned PVC path and requires their separate legal/ToS clearance.
- Archival/low-quality audio: NOT addressed. There is no documented "restore from old recording" mode. The implication of [3] is decisive: a ~1945 noisy, band-limited home disc with seconds-to-minutes of speech is FAR below even the IVC floor (1 min clean) and orders of magnitude below the PVC floor (30 min clean) — the platform is built for the opposite of this input.

### [9] Voicebot.ai — Andy Warhol AI voice (read in full) — the most relevant precedent
- Source audio: only ~3 min 12 sec of usable Warhol audio existed (1970s/80s, already cleaner and more modern than a 1945 home disc).
- Critically, the documentary's final narration was the Resemble clone BLENDED with actor Bill Irwin's recorded performance — i.e., the human actor supplied prosody/intonation/emotion the limited clone could not, and the clone supplied timbre. Resemble's framing ("capture the essence of how someone speaks") is deliberately soft; this was an artistic, estate-approved, disclosed-on-screen reconstruction, not a faithful "this is exactly how he sounded reading new text" replica.
- Takeaway for this task: even with cleaner/longer source than ours and a top vendor, the credible result needed (a) estate consent, (b) on-screen disclosure, and (c) a human actor in the loop. Pure clone output from ~3 min was not deemed broadcast-sufficient on its own.

### [13] arXiv 2110.03347 — Cloning one's voice using very limited data in the wild (read)
- Operates from ~10-30 seconds of "in-the-wild" (noisy, non-studio) audio per speaker, which superficially matches our seconds-to-minutes budget.
- Reports the expected limitations: prosody/intonation is unstable and effectively sampled from the limited reference; quality degrades as data shrinks; speaker similarity is variable across voices. Paired with [14], the consistent message is that noisy found-data cloning leans on aggressive speech-enhancement (denoise + dereverb) and adversarial training, and even then the OUTPUT tends toward a clean, generic-leaning rendition rather than a faithful reproduction of the specific degraded original. The very thing that makes our target distinctive — a soft, wispy, diphtheria-affected timbre on a noisy 1945 disc — is exactly what enhancement and zero-shot models tend to "average away" toward a normal, healthy, studio-clean voice.

## Gaps
- No vendor publishes a benchmark for "clone fidelity vs. seconds of NOISY, band-limited, pre-1950 source"; all stated minimums (3s-30min) implicitly assume modern, clean, full-bandwidth audio, so they are not predictive of this case.
- It is genuinely unknown whether the target's distinctive pathology (soft/wispy, diphtheria-damaged) can be preserved at all; the literature suggests enhancement pipelines actively suppress such "defects," and there is no published case of faithfully reproducing a damaged/atypical voice from minimal archival audio.
- The Warhol/Bourdain/Piaf/JFK precedents either used much more or cleaner audio, or relied on a human actor and/or estate-sanctioned artistic license — none demonstrate a faithful pure-clone result from a single short noisy 1940s disc.
- Consent/legal status of this specific subject is undetermined here: post-mortem voice rights, estate authorization, and EU AI Act / ELVIS-style disclosure obligations would all need clearing before any public use, regardless of technical feasibility.
- Whether a custom (non-hosted) restoration + fine-tune pipeline (e.g., heavy disc restoration → bandwidth extension → fine-tune on the target) could beat hosted tools is unverified; hosted commercial products are not designed for, and will likely underperform on, this input.
