# Solanus digital avatar

A photoreal, drivable digital human of **Father Solanus Casey** — face/head reconstructed from historical
photographs, a full body in the Capuchin habit, driven by the voice model (`../voice`) and the text mimicker
(`../mimicker`). A **separate track**. Best-version-first (max quality + storage/compute; downsample later).

> Project authorized via the vice-postulator of the cause, **Fr. Ed Foley**. As a design choice, the avatar
> speaks his **documented words** (from `../mimicker`) and outputs carry a provenance credit + an
> "historically reconstructed, motion synthesized" note (there is no footage of him in motion).

## The source reality (this drives the whole design)
- **Photos: yes, a real corpus.** Hundreds in the **Capuchin Province of St. Joseph archives** (Detroit;
  Provincial Archivist Junia Yasenov), ~200 reproduced in *Blessed Solanus Casey* (Images of America), plus a
  small public set on Wikimedia (top portrait 2361×3500). **Overwhelmingly frontal B&W; thin profile/back
  coverage; ~one color photo.**
- **Motion: none.** No verified moving film of him alive exists — the "rare footage" in documentaries is
  **stills + 1987 exhumation footage**. So **all body motion and most expression must be synthesized** from a
  mocap stand-in / motion library, never recovered from him.
- **Consequence:** face geometry from photos is feasible; **profile, back-of-head, hair, and all color are
  generative**; **relighting is effectively unsolved** from old photos with unknown lighting. See `SOURCES.md`.

## Pipeline (see `PLAN.md` for the full, cited version)
| Stage | Script | What |
|---|---|---|
| 0 | `acquire.md` | high-res scans of original prints/negatives from the Capuchin archive |
| 1 | `restore_images.py` | blind-restore + upscale + colorize (anchored on the one color photo) |
| 2 | `reconstruct_head.py` | Pixel3DMM → FLAME identity (geometry), fused across photos |
| 3 | `build_head_appearance.py` | photoreal 3DGS head (Avat3r / FaceLift), bound to the FLAME rig |
| 4 | `build_body.py` | LHM/SMPL-X body + simulated Capuchin habit (Dress-1-to-3 + ContourCraft) |
| 5 | `drive.py` | motion (AMASS/EMAGE) + audio-driven face (OmniHuman-1.5 / HunyuanVideo / Ditto) |
| 6 | `composite.py` | unify + (approx) relight; real-time runtime in UE5 + NVIDIA ACE |

## Status
Plans + scaffold complete. **Next step is Stage 0** — high-res scans from the Capuchin archive. Most of the
pipeline is integration of ~8 separate research-grade tools on the shared FLAME/SMPL-X rig; there is no
turnkey product for "fuse sparse historical B&W stills into one relightable avatar."
