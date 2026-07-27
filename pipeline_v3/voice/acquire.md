# Stage 0 — acquire the audio

Goal: get the **June 1945 recording** of Solanus's voice in the **highest-fidelity, UN-processed** form
available, into `data/raw/`. Authorized via the vice-postulator, **Fr. Ed Foley** — name him as the project
sponsor in every request.

## Two parallel paths (do both)

### A. Fast path — the "Echoes of Solanus Casey" CD (today, ~$10.50)
The CD embeds two short tracks of his real voice/violin. Good enough to start restoration experiments while
the archive request is in flight.
1. Buy it from the **Solanus Casey Center gift shop** (SKU 103336): https://giftshop.solanuscenter.org/product/echoes-of-solanus-casey/ (Amazon ASIN B000LL2RQ8 as backup).
2. Rip the disc **losslessly** — `cdparanoia` or EAC to **WAV/FLAC, 16-bit/44.1 kHz, no normalization**.
3. Identify the **two Solanus tracks** (the other 14 are the modern choir) and copy them to `data/raw/echoes_track_*.wav`.
   This is a CD master (already mastered/compressed once) — treat it as a *fallback* reference, not the best source.

### B. Best path — the archive master (the real Stage 0)
The original **7-inch disc** and the first-generation **cassette transfer** live in the **Capuchin Province of
St. Joseph archives**. Request a fresh, flat transfer — this is what we actually want.

**Contacts**
- Provincial Archivist: **Junia Yasenov (Papas)**, Capuchin Province of St. Joseph Archives, 1820 Mt. Elliott St., Detroit, MI 48207 · (313) 579-2100
- Project sponsor / authorization: **Fr. Ed Foley, vice-postulator of the cause**
- (Solanus Casey Center / Father Solanus Guild, same campus, for coordination)

**Exactly what to request (put in the email):**
- The **June 1945 recording** made at the first Mass of his nephew Fr. John (Neil) McCluskey, SJ — "the only
  recording of his voice and violin."
- A **flat, archival transfer** of the *original disc* (and the cassette, if the disc is unplayable):
  **24-bit / 96 kHz WAV**, **both sides, full length, uncut**.
- **NO processing applied** — no noise reduction, de-click, EQ, normalization, or "enhancement." We do
  restoration ourselves and need the raw capture. (If they only have a previously cleaned copy, ask for *both*
  the cleaned and the rawest available.)
- The **preservation master** if one already exists (ask what digitization they hold).
- **Provenance metadata**: accession number, recording date, who/what is on each segment (the EWTN/CNA account
  says it includes a prayer, a family greeting, a recited poem, singing, and violin — ask them to confirm/timestamp).

**Draft email**
> Subject: Stage-0 audio request — Fr. Solanus 1945 recording (project authorized by Fr. Ed Foley)
>
> Dear Ms. Yasenov,
>
> With the blessing of Fr. Ed Foley, vice-postulator of Fr. Solanus Casey's cause, we are building a faithful,
> permission-based digital preservation of Fr. Solanus's voice. For that we need the best possible transfer of
> the **June 1945 recording** (his nephew Fr. John McCluskey's first Mass).
>
> Could you provide a **flat, unprocessed archival transfer** — 24-bit/96 kHz WAV, both sides, full length,
> with **no noise reduction or enhancement applied** — of the original disc (and the cassette transfer if the
> disc is unplayable)? If a cleaned copy is all that exists, we'd be grateful for both the cleaned and the
> rawest available versions, plus the accession number and any notes on what is on the recording.
>
> Fr. Foley can confirm the project and its scope. Thank you for your stewardship of these materials.

## When the files arrive
- Put raw transfers in `data/raw/` and **work only on copies**.
- Verify: open in a spectrogram (e.g. Audacity / Sonic Visualiser) — confirm real speech content, note the true
  high-frequency cutoff (a 1945 home disc is band-limited; anything above ~mid-single-kHz will be invented by
  super-res later, so this tells you what's recoverable).
- Then run `restore_audio.py` (Stage 1).

## What NOT to do
- Don't rely on YouTube rips (e.g. "Father Solanus and His Violin") as a source — lower fidelity and rights-murky;
  fine only for scouting which segments exist.
- Don't accept a noise-reduced file as the *only* source — NR bakes in artifacts a cloner will learn.
