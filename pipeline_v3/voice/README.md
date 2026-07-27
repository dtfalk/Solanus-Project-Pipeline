# Solanus voice mimicker (TTS)

Reconstruct Father Solanus Casey's **speaking voice** so the text mimicker (`../mimicker`) can be *heard*,
not just read. A **separate track** from the text mimicker and the archival tool. Best-version-first
(max quality; downsample later).

> Project authorized directly by the **vice-postulator of the cause, Fr. Ed Foley** — so this proceeds with
> the steward's blessing. As a design choice (not a gate), the voice reads his **documented words** from the
> archive rather than free-form generation, and renders carry a provenance credit.

## The source reality (this drives the whole design)
There is **exactly one** authentic recording of his voice: a **June 1945 home disc** cut at a family
gathering after his nephew Fr. John McCluskey's first Mass — him speaking, reciting, singing, and playing
violin. His great-niece confirms it is "the only recording." Publicly it survives as two short tracks on
the **"Echoes of Solanus Casey" CD** (Father Solanus Guild, 2000; OCLC 187781405); the master sits in the
**Capuchin Province of St. Joseph archives** (Detroit). Realistically that is **well under ~5 minutes**,
with only **seconds-to-minutes of clean connected speech**, on a noisy disc, in a soft voice permanently
weakened by childhood diphtheria. See `SOURCES.md`.

**Consequence:** there is **no corpus to fine-tune** a faithful clone on. The honest ceiling is a
*recognizable approximation*, very likely needing a human-actor assist — not "indistinguishable." We
design around that, transparently. This is a **Case B/C** build (restoration-first + zero-shot cloning
from the cleaned reference + a disclosed period-correct stand-in fallback).

## Pipeline (see `PLAN.md` for the full, cited version)
| Stage | Script | What |
|---|---|---|
| 0 | `acquire.md` | obtain the 1945 master / Echoes CD from the Capuchin archive (contacts in `SOURCES.md`) |
| 1 | `restore_audio.py` | de-click → de-crackle → gentle 2-pass denoise; keep a conservative master (super-res is a *separate*, non-canonical output) |
| 2 | `make_references.py` | hand-curate the cleanest 5–15s reference clips |
| 3 | `clone_voice.py` | zero-shot clone (CosyVoice 3 primary; Llasa-8B / F5-TTS comparators) |
| 4 | `speak.py` | text (from `../mimicker`) → normalize → sentence-chunk → synth against one frozen reference → stitch |
| 5 | `evaluate_voice.py` | speaker-similarity + intelligibility + human review |

## Status
Plans + scaffold complete. **Next step is Stage 0** — obtain the actual audio from the Capuchin archive
(authorized via Fr. Ed Foley). The scaffold runs once real reference audio is in `data/`.
