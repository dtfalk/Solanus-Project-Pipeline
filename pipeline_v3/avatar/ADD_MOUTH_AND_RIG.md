# Add a working mouth to Solanus + rig it (Blender + Faceit) — you drive it, I wire it

You can see the model; I can't (I was rigging the wrong spot blind). So you do the modeling/landmarks in Blender,
export, and I map the result into the app. This is the reliable path.

**File to open:** `pipeline_v3/avatar/faceit_ready/solanus_faceit.glb`
(the ChatAvatar head, decimated to ~90k faces so Faceit won't choke; photoreal texture intact)

---

## Step 1 — Import & look at the mouth
1. `File → Import → glTF 2.0` → `solanus_faceit.glb`. Press **`Z` → Material Preview** (see his face).
2. **`Tab`** into Edit Mode, zoom into the mouth (between mustache and beard).
3. Find out if the lips can open: hover the upper lip, press **`L`** (select-linked). If the whole head selects as ONE surface and the lip line has no gap, the lips are **sealed** → do Step 2a. If the upper and lower lip select separately, they're already open-able → skip to Step 2b.

## Step 2a — Separate the lips (only if sealed)
The mouth can't open if upper and lower lip share the same edge.
1. In Edit Mode (Edge select, `2`), **`Alt`+click** along the lip seam to select the mouth edge loop.
2. **`Mesh → Split → Faces by Edges`** (or press **`V`** to Rip) — this splits the upper lip from the lower so they can part.
3. Check: select just the lower lip + move it down (`G Z`) — there should now be a gap. Undo (`Ctrl+Z`) after checking.

## Step 2b — Give the open mouth something to show (interior)
Otherwise opening reveals a hole through the head.
1. **Teeth:** `Add → Mesh → Cube`, scale it thin and wide (`S`), place it **just behind the lips** as the upper teeth; duplicate (`Shift+D`) for the lower row. Give them an off-white material. (Or import Ready-Player-Me teeth if you have them.)
2. **Dark cavity:** `Add → Mesh → UV Sphere`, scale small, place **behind the teeth**, give it a near-black material — so the inside of the mouth reads dark.
3. Because you can see the mouth, position these precisely behind the lip line. Keep them as **separate objects** for now.

## Step 3 — Rig with Faceit
1. **`N` → Faceit** tab.
2. **Setup → Register:** select the head → **Register Face Objects** (Main). If you made separate teeth/eyes, register them as **Teeth / Eyes** so they move with the rig.
3. **Landmarks → Generate** → place markers on eye corners, nose, and especially the **mouth corners + upper/lower lip** — you can see the mouth, so nail these. Confirm.
4. **Generate Rig**, then **Bind**.
5. **Expressions:** load **BOTH** the **ARKit** set *and* the **Visemes / Phonemes** set.
6. **Bake → Bake Shape Keys.**

## Step 4 — TEST it before exporting
1. Object Data Properties (green triangle) → **Shape Keys**.
2. Drag **`jawOpen`** (and a couple visemes like `viseme_aa`) to **1.0** — the **mouth should open and you should see the teeth/cavity.**
3. If it tears or stretches instead of opening → the lips weren't separated; redo Step 2a, then re-bake.
4. Set the values back to 0.

## Step 5 — Export & hand to me
1. `File → Export → glTF 2.0` → in the export panel tick **"Shape Keys"** (under Data → Mesh) and **include all objects**.
2. Save as **`pipeline_v3/avatar/solanus_visemes.glb`**.
3. Tell me it's there. I'll map the ARKit/viseme blendshape names onto the Azure **`/api/tts_visemes`** stream (already wired) and drive `morphTargetInfluences` — full phoneme lip-sync on the photoreal head, in avatar mode.

---

### Notes
- The viseme names Faceit produces may differ from what the app wants (`viseme_aa`, etc.) — **I handle the rename/mapping**, you don't need to.
- Keep the texture intact (don't change mesh topology after baking, or the shape keys break).
- If Faceit's landmark step fights the bearded face, approximate — you can fine-tune the baked shapes afterward by editing the shape keys.
