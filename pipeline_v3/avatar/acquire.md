# Stage 0 — acquire the photographs

Goal: get the **highest-resolution, UN-retouched scans** of every usable photograph of Solanus into
`data/raw/`, with angle coverage as wide as the archive allows. Authorized via the vice-postulator,
**Fr. Ed Foley** — name him as sponsor in every request.

## Step 1 — get the finding aid first
Ask the archive for an **inventory / finding aid of the Solanus photo holdings** before requesting scans, so we
know what angles and life-stages actually exist (the public set is overwhelmingly frontal). This also tells us
whether any **profile / three-quarter / full-length** shots exist — the scarce views we most need.

## Step 2 — request high-res scans
**Contacts**
- Provincial Archivist: **Junia Yasenov (Papas)**, Capuchin Province of St. Joseph Archives, 1820 Mt. Elliott St., Detroit, MI 48207 · (313) 579-2100
- Project sponsor: **Fr. Ed Foley, vice-postulator of the cause**
- Cross-reference captions in *Blessed Solanus Casey* (Images of America, ISBN 9781467102544; ~200 photos from
  these archives) and request specific items by caption/page.

**Priority list (state in the request):**
1. **Every distinct frontal / three-quarter portrait** across his life (Wisconsin youth → ordination →
   St. Bonaventure/Detroit porter years → Yonkers → late life) — more identity samples = better FLAME fusion.
2. **Any side-profile or angled candids** — explicitly ask; these are scarce and disproportionately valuable
   for 3D head shape (without them the profile/back is diffusion-invented).
3. **Full-length standing shots** (for body proportions + the habit).
4. **The single color photograph** (the palette anchor; eyes documented as blue) — request as-is.
5. Any **candids in motion** (playing violin, at the monastery door) — useful for expression/pose reference.

**Scan specs (so scans are reconstruction-grade, not pre-enhanced):**
- **Scan the original negatives/glass plates where they exist** (more detail than prints); prints otherwise.
- **TIFF, 16-bit, maximum optical resolution** (≥1200 dpi for prints; native for negatives).
- **No auto-enhance, no sharpening, no denoise, no cropping** — flat scans; we restore ourselves.
- Include a **grey/colour target or ruler** in-frame when feasible (helps tonal calibration).
- Provenance per file: **accession number, date, photographer/source, life-stage**.

**Draft email**
> Subject: Stage-0 photo request — Fr. Solanus images (project authorized by Fr. Ed Foley)
>
> Dear Ms. Yasenov,
>
> With Fr. Ed Foley's blessing, we are building a faithful, permission-based digital reconstruction of
> Fr. Solanus. Could we start with a **finding aid of the Solanus photograph holdings**, and then request
> **flat, high-resolution scans** (TIFF, 16-bit, max optical resolution, **no enhancement/sharpening/cropping**,
> scanning negatives where available) of: all distinct frontal/three-quarter portraits across his life; **any
> profile or angled shots**; full-length standing photos; the color photograph; and any candids of him in
> motion? Accession numbers and dates with each file would be ideal. Fr. Foley can confirm scope. Thank you.

## When the scans arrive
- Put them in `data/raw/`; name the color photo `solanus_color.tif` (config expects it as the palette anchor).
- Quick QA: confirm resolution/bit-depth, that no sharpening halos are present, and tag each with its angle
  (frontal / 3-quarter / profile / full-length) — Stage 2 fuses by angle.
- Then run `restore_images.py` (Stage 1) → `reconstruct_head.py` (Stage 2).

## What NOT to do
- Don't build from web/Wikimedia JPEGs as the primary source (compressed, low-res) — fine only for scouting.
- Don't accept auto-"enhanced" scans as the only copy — enhancement bakes in features the 3D fit will inherit.
- Don't mistake documentary **reenactment actors** or devotional **artwork/holy cards** for photographs of him.
