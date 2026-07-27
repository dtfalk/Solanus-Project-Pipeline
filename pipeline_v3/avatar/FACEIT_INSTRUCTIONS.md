# Your own Faceit viseme pass (optional — the auto version already works)

**What's already live (mine):** the photoreal ChatAvatar Solanus is the app avatar, and I baked a **jawOpen** blendshape onto it and wired it to the Azure voice — so in **avatar mode** he speaks and his **jaw/mouth opens and closes in sync** with the audio. It's amplitude-driven (open on loud syllables, closed on silence); the beard hides the lack of a teeth cavity, so it reads as talking. Good enough to ship.

**Why you might still do Faceit:** to get *phoneme-accurate lip shapes* (rounded "O", spread "E", pressed "M/B", etc.) instead of just an open/close jaw. That's the full ARKit + viseme set, and Faceit bakes it.

## Mesh to use
`pipeline_v3/avatar/faceit_ready/solanus_faceit.glb`
— 90k faces, photoreal texture, **clean topology with a real mouth** (already decimated from ChatAvatar's 1M faces so Faceit won't choke). This is the one to rig. Do NOT use the raw 745k ChatAvatar export — too heavy.

## Steps
1. **Import:** Blender → `File → Import → glTF 2.0` → `solanus_faceit.glb`. Press **`Z` → Material Preview** to see his face.
2. **Open Faceit:** press **`N`** → **Faceit** tab.
3. **Setup → Register:** select the head mesh → **Register Face Objects** (one mesh is fine — it has no separate eyes/teeth, that's OK).
4. **Landmarks:** **Generate Landmarks** → choose the human/full-face type → drag the markers onto the **eye corners, nose, mouth corners, chin**. This is the fiddly step; accuracy here = quality. (Beard hides small errors, so approximate on the chin.) Confirm.
5. **Rig + Bind:** **Generate Rig**, then **Bind**.
6. **Expressions:** load **BOTH** the **ARKit** set *and* the **Visemes / Phonemes** set (not just one).
7. **Bake → Bake Shape Keys.** The mesh now has the viseme blendshapes (check Object Data Properties → Shape Keys).
8. **Export:** `File → Export → glTF 2.0` → tick **"Shape Keys"** → save as `pipeline_v3/avatar/solanus_visemes.glb`.

## Then hand it to me
Tell me the path and I'll **replace the jaw-only driver with full viseme lip-sync** — the app already has `/api/tts_visemes` (Azure Speech-SDK viseme events) wired; I just map those viseme IDs onto your baked blendshape names and drive `morphTargetInfluences`. Result: phoneme-accurate mouth on the photoreal Solanus.

## Notes
- The viseme names Faceit bakes may differ from what the app expects (`viseme_aa`, etc.) — I'll handle the rename/mapping on my side, you don't need to.
- Want teeth visible on jaw-open? Add a teeth mesh before baking — but the beard covers the mouth, so it's optional.
