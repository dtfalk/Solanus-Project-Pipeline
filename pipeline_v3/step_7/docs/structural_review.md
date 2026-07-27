<!-- Generated 2026-07-02 by a 4-dimension structural review (backend / frontend / data-pipeline /
deploy) + synthesis.
     APPLIED SO FAR:
       - C1a (delete /sources mount) — done; GET /sources/.env now 404. KEY ROTATION is still the user's action.
       - A1 (_graph_indexes() projection cache + fold stale _ENT_DEGREE_CACHE) — done; verified byte-identical
         across 39 endpoint responses; /api/relationship & /api/letter now ~0.6ms (were full 63k-edge scans /
         full letter-sort per request). See app/server.py::_graph_indexes().
       - C2a (graph.json data-loss) — LARGELY DONE. A Jun-27 build_graph had already wiped the PAID connection
         verdicts + FAMILY edges; recovered the verdicts (89%) from the .preenrich backup into a new
         data/connection_verdicts.json side-car + a no-model `verify_connections.py --reapply`. Added
         lib/safeio.py (timestamped backups + atomic writes) across build_graph/the 3 bakers/solanus_family;
         made app-time _maybe_build_graph_json restore-from-backup instead of base-rebuild; added
         `run.py seed` and seeded _pipeline_state.json (a bare `run` no longer rebuilds/wipes). STILL OPEN =
         F4: register a deterministic reapply_layers DAG stage deps=[build_graph] (BREAKING — needs David).
         See memory graph-coherence-safety.md. -->

# Solanus Archive App — Consolidated Structural Improvement Plan

Four reviews (backend / frontend / data-pipeline / deploy), 35 findings, collapse to ~24 distinct actions across three buckets. The strongest signal is convergent: **the same "one giant module + copy-pasted boilerplate" pattern recurs in all three code layers, and the same "read-time overlay vs. baked-in artifact" ambiguity recurs across the data layer.** Fixing the split/DRY structure is what makes everything else (testability, pop-out reuse, deploy) cheap.

## Overlaps collapsed (so you don't do the same work three times)
- **"Split the monolith"** is *one* program in three surfaces: `server.py` → APIRouters (B2), `app.js` → ES modules (F1), inline `index.html` CSS → `components.css` (F2). The pop-out duplication (F3) and the scattered per-view state (F6) are **consequences** of the app.js monolith and get fixed for free by F1 — don't schedule them separately.
- **"One mtime-cache primitive"** covers backend loaders (B3), the triple-parsed documents store (B4), and — same idea, different layer — the copy-pasted ThreadPool/resume/costlog loop in the LLM side-cars (D5). Two helpers, applied in two layers.
- **CSP (G3) is coupled** to external runtime assets (G5), the inline importmap/`<style>` (F2), and the `el({html})` path (F10). Tune them together or the policy is either self-defeating or bypassed.
- **Cache-busting `?v=mtime` (F7)** is a frontend finding but a *deploy* correctness fix (stale app.js vs. fresh HTML after redeploy). Do it as part of the deploy pass.
- **B7 (source/region, connection/relationship)** and **B5 (list envelopes)** are the only backend items that actually change the client contract.

---

## (C) PRE-DEPLOY / PRE-RERUN MUST-FIXES — blocking

These gate exposure. C1 = security/hygiene before *any* public Azure surface. C2 = coherence before the next "populate all the way up" run (a graph rebuild currently corrupts derived state silently).

### C1 — Security & deploy hygiene

**C1a — Delete the `/sources` StaticFiles mount** · Effort: **S** · Breaking: **no**
- **Change:** Remove `api.mount("/sources", StaticFiles(directory=config.REPO))` (server.py ~2491). Rotate GEMINI/OPENAI/VOYAGE/COHERE/Azure keys afterward.
- **Why:** `config.REPO` is the repo root; Starlette StaticFiles serves dotfiles, so `GET /sources/pipeline_v3/step_7/.env` returns the live keys in cleartext, plus `.git/` and the 3GB `data/`. The frontend never calls `/sources` (citations go through gated `/api/image|pdf|manifest`), so this is pure attack surface. On a public Container App FQDN it's directly exploitable. **This is the single highest-severity item in the entire review.**

**C1b — `.dockerignore` + runtime secret injection** · Effort: **M** · Breaking: **no**
- **Change:** Add a `.dockerignore` that whitelists only `app/` + the specific data the backend needs; exclude `.env`, `.git`, `checkpoints/`, `*.backup.*`, `app/static/.backups/`, `*.blend`, screenshots. Inject keys as Container App secrets / Key Vault refs, never `COPY .env`. Keep env-var names identical so `config.py`/`server.py` are untouched.
- **Why:** A naive `COPY . .` re-leaks exactly what C1a leaks, baked into a public image layer.

**C1c — Keep backend ingress internal + gate the paid endpoints** · Effort: **M** · Breaking: **yes** (deployment topology / adds an auth header)
- **Change:** Set the Container App ingress to **internal**, reachable only via the SWA linked backend (same-origin `/api`). Add a lightweight shared-token header the SWA injects (or SWA Easy Auth if the archive should be sign-in-gated) + a `slowapi` rate limit on `/api/query`. Set a hard monthly spend cap / budget alert on each provider key.
- **Why:** `/api/query` bills real LLM/embed/rerank per call with zero gating today. A public URL + a loop = drained keys. The spend cap is the backstop if anything else leaks.
- **Note (G9):** The *absence* of CORSMiddleware is **correct** for this proxy model — do not "fix" cross-origin errors with `allow_origins=['*']`; that re-opens the paid endpoints to any site. If ever needed, allowlist the exact SWA hostname. Document this so a future dev doesn't undo it.

**C1d — CSP + baseline security headers in `staticwebapp.config.json`** · Effort: **M** · Breaking: **no**
- **Change:** Add `X-Content-Type-Options: nosniff`, `Referrer-Policy: strict-origin-when-cross-origin`, HSTS, and a CSP. CSP must fit reality: `script-src`/`style-src` need `'unsafe-inline'` (inline `<script type=importmap>` and `<style>` blocks — no nonce possible on static SWA files), `connect-src 'self'`, and `img-src` must allowlist `*.basemaps.cartocdn.com` (map tiles) and — until vendored — `cdn.jsdelivr.net`, or the map and deep-zoom controls go blank. Tune `img-src` jointly with C1e.

**C1e — Vendor the last external runtime assets** · Effort: **S** · Breaking: **no**
- **Change:** Vendor OpenSeadragon's `images/` into `app/static/vendor/openseadragon/images/` and point `prefixUrl` there (kills the jsdelivr dependency). The Carto world basemap can't practically be vendored — make an explicit decision: keep Carto + allowlist it in CSP, or self-host tiles. Add `rel="noopener"` to the `google.com/maps` click-out links (they're navigations, not subresources, so no CSP allowance needed).

**C1f — Make the backend container-buildable** · Effort: **M** · Breaking: **no**
- **Change:** Add a `Dockerfile` whose entrypoint is `uvicorn app.server:app --host 0.0.0.0 --port 8000` (serve.py defaults to `127.0.0.1`, unreachable from ingress). Freeze the local venv to a pinned `requirements.txt` (fastapi, uvicorn[standard], networkx, pydantic, provider SDKs) checked into `app/`. Health probe → existing `GET /api/health`.

**C1g — Deploy the real files, not the symlinks** · Effort: **S** · Breaking: **no**
- **Change:** Point the SWA GitHub Action's `app_location` at `app/static` (the real files) and move `staticwebapp.config.json` next to them. Oryx doesn't reliably resolve the `swa/frontend` → `../../app/static` symlinks; keep `swa/` as the local-emulator harness only. Decide the canonical home of `dossier.html`/`pipeline.html` so clean-URL routing matches prod.

**C1h — Static-asset cache-busting** (= F7 second half) · Effort: **S** · Breaking: **no**
- **Change:** Template an mtime/build query onto `app.js`, `theme.css`, `pages.css` (`/static/app.js?v={mtime}`). **Why:** without it, a redeploy serves stale cached `app.js` against fresh HTML — split-brain cache bug.

*Deferred, not blocking:* **G7 hosting shape** (Container App, `minReplicas=1` to keep the 47k-node graph warm, ~2–4GB RAM, data baked-in vs. premium Azure Files) is **L** effort and an infra decision, not a code fix — plan it, but it doesn't gate the code-hygiene fixes above.

### C2 — Data reproducibility & coherence (before the next graph rebuild)

**C2a — `build_graph` re-run silently erases three post-passes** · Effort: **M** · Breaking: **yes** (changes when/how graph.json is finalized)
- **Change:** `enrich_descriptions` (node.description), `build_kinship` (FAMILY edges), and `verify_connections` all rewrite `graph.json` in place *after* `build_graph`, which deterministically regenerates it from `entity_store.json`. Any DAG re-run of `build_graph` wipes all three. Either **fold them into `build_graph` as ordered post-passes it always applies**, or register them as DAG stages `deps=[build_graph]` so staleness re-applies them. Standardize the ad-hoc `.preenrich`/`.prekinship`/`.preconnverify` backups onto one naming scheme.
- **Why:** This is the single biggest coherence hazard — canonical `graph.json` is only correct if a human re-runs three unwritten-down steps in the right order every time. The "read-time overlay (safe) vs. bakes-into-graph.json (fragile)" distinction is real but documented nowhere.

**C2b — The place dossiers have no committed producer** · Effort: **M** · Breaking: **no**
- **Change:** The richest/most expensive enrichment — the ~60 researched place dossiers with scholarly sources in `place_enrichment.json` — has **no committed `.py` that writes** `what_it_was/fate/significance/scholarly_sources` (`geocode_precise` writes only lat/lon; the app only reads). Recover/port it into `stages/research_places.py` on the `name_authority` template (run(), .backups, resume, cost-cap) and register it. At minimum, document where the generator lives.
- **Why:** If that file is lost or needs a refresh on cleaner data, the dossiers are unregenerable. MEMORY calls it "re-runnable" but the re-runner isn't in the tree.

---

## (A) SAFE, high-value refactors — no behavior change

Ranked by impact/effort.

**A1 — `_graph_indexes()` projection cache** *(B1 + B6)* · Effort: **M** · Breaking: **no** — *best ratio in the whole review*
- **Change:** Add a sibling to `_graph_cache()`, keyed on the same graph mtime, that precomputes once per graph version: `nodes_by_kind`, `doc_id→section`, the sorted letter-id list (prev/next), and a partitioned incident-edge adjacency. Endpoints become dict lookups/slices instead of scans. Fold the stale `_ENT_DEGREE_CACHE` (filled once at 1464, never re-keyed → silently stale after rebuild) into it; give `_CONN_CACHE`/`_ENTITY_BOOK_CACHE` an mtime key or LRU cap.
- **Why:** Nearly every archive endpoint re-derives the same projections as O(nodes+edges) scans *per request* — `/api/letter` sorts the entire letter-id list to compute one prev/next; `/api/relationship` does two ~65k-edge scans per call. The correct pattern already exists (`_people_index` memoizes on mtime) but is applied to exactly one projection. Tens of thousands of iterations per page view that never change between rebuilds.

**A2 — One `mtime_cached()` loader primitive** *(B3 + B4)* · Effort: **S–M** · Breaking: **no**
- **Change:** Replace the 9 hand-rolled mtime loaders + 13 module-level cache dicts with one `mtime_cached(path, build=None)` helper. Consolidate the three independent parses of `documents.json`/`notebooks.json` (`_find_record`, `_docs_index`, `_record_text`) into one store keyed by mtime exposing `.record(id)/.letter_text(id)/.entry_text(id,rid)`; the three current caches become thin views.
- **Why:** ~120 lines expressing one idea; triple memory and three invalidation paths that can silently drift. Four of the loaders are byte-identical except the filename.

**A3 — `lib/enrich.py::run_map()` — one LLM side-car orchestrator** *(D5 + D6)* · Effort: **M** · Breaking: **no**
- **Change:** Extract the copy-pasted `load-done → filter-todo → ThreadPool → as_completed → periodic checkpoint → costlog cost-cap → .backups + write` loop (repeated ~40 lines × 4 in summarize_letters / theme_notebook_pages / relationship_narratives / enrich_descriptions) into `run_map(items, key_fn, work_fn, out_path, *, max_cost, workers, checkpoint_every, backup=True)`. Point `_template.py` at it so new stages inherit the convention.
- **Why:** Drift already crept in (compact vs. `indent=1`, checkpoint cadence 20/40/50, one stage skips its own backup). Every future side-car copies whichever file the author opened. Also fixes D6's convention inconsistency for free.

**A4 — Split the three monoliths along their existing seams** — the big structural payoff, do incrementally
- **A4-server** *(B2)* · Effort: **L** · Breaking: **no** — Split `create_app()`'s 41 routes into `app/api/` routers (query / graph / archive / content / sources / voice / meta), move loaders to `app/loaders.py` and pure logic (`agent_query`, `single_shot_rag`, `source_region`, `_subgraph`) to `app/services/`. Keep the pydantic request models module-level (the line-921 note explains why). *Why:* one 2,560-line file mixes 8 concerns; endpoints are closures so **none are importable/testable** without building the whole app. The repeated in-function local imports are a symptom.
- **A4-app.js** *(F1, unlocks F3 + F6)* · Effort: **L** · Breaking: **no** — Switch `<script src=app.js>` to `type="module"` (the infra already exists — `avatar.mjs` + importmap ship today, no bundler needed) and carve along the comment-banner seams: `core/dom`, `state`, `chat`, `graph` (the ~800-line Cytoscape block), `dossier`, `map`, `reading`, `people`, `family`, `life`, `voice`, `pipeline-flow`, `nav`. **One region per commit**, verify after each — shared closure state and DOM-ready timing make big-bang risky. Extract `dossier-render.mjs` + `pipeline-flow.mjs` that the pop-out pages (`dossier.html`, `pipeline.html`) *also import*, killing the forked logic/CSS (F3). Group per-view state into its owning module and formalize the app↔avatar seam as one `window.Solanus = {avatar, ttsVoice, avatarGlb, exitFullscreen}` object (F6).
- **A4-CSS** *(F2)* · Effort: **M** · Breaking: **no** — Cut `index.html:63–917` (~59% of the file, the project's largest stylesheet, buried in the HTML shell) into a linked `components.css` loaded *between* theme.css and pages.css so cascade order is preserved (the one regressable thing — verify visually). Follow up by folding the two source-order "override" blocks back into their base rules so correctness stops depending on physical position. `pipeline.html` links the same file, removing its mirrored node CSS.

**A5 — Register the enrichment side-cars in the DAG** *(D1)* · Effort: **M** (S for the first one) · Breaking: **no**
- **Change:** `run.py` registers 14 stages but not `enrich_descriptions`, `summarize_letters`, `theme_notebook_pages`, `relationship_narratives`, or `geocode_precise`. Register each with correct deps/inputs/outputs. **Cheapest immediate win:** `geocode_precise` already has `run()` and free `deps=[geocode_places]` — add it to imports now. Wire the paid ones on their SAFE default path (DAG previews scope, real pass opt-in) so a blanket `run` stays free but staleness is finally tracked.
- **Why:** The reproducibility promise ("populate all the way up") silently excludes the entire LLM enrichment layer — after a graph/entity rebuild, summaries/themes/descriptions/narratives are quietly stale. `name_authority` is already in the DAG, proving the pattern was intended.
- *Prereq for the paid stages:* the four LLM side-cars are `main()`-only (no `run()`), which is exactly why they can't drop in — A3's helper gives them a `run(**kwargs)` for free.

**A6 — Consolidate init to one entry point** *(F8)* · Effort: **S** · Breaking: **no** — Fold the second mid-file `DOMContentLoaded` (2662, wires graph toolbar/settings/help/`?rel`) into `boot()` (or each view's `init()` post-split). One handler, each view owns its wiring.

**A7 — Single deep-link module** *(F7 first half)* · Effort: **S** · Breaking: **no** — Add `deeplink.mjs` that reads/writes hash + `?rel/?letter/?rr` + `#flow` in one place. Today routing is parsed inline at ~6 sites with no enumeration of shareable URLs, so it's easy to collide a param or miss one when refactoring.

**A8 — `makeDraggable`/`makeResizable` helper** *(F4)* · Effort: **M** · Breaking: **no** — Route all 7 hand-rolled pointer/userSelect/localStorage/clamp implementations (~150 dup lines) through one helper in `interactions.mjs`. Standardizes keyboard arrow-resize + `role=separator`, which today exist on some handles and not others.

**A9 — Accessibility pass** *(F5)* · Effort: **M** · Breaking: **no** — Either implement the WAI-ARIA menu keyboard model (Arrow/Home/End/Escape-returns-focus) for the nav **or** honestly drop `role=menu/menuitem` and treat it as disclosure nav (`aria-expanded` suffices). Add a shared `openDialog()/closeDialog()` that traps Tab and restores focus; give entity-panel + rel-overlay `role=dialog`+`aria-modal`; set the intentionally-non-dimming citation modal to `aria-modal="false"` (it currently claims `true` while keeping the page interactive — a direct contradiction). Add `aria-live="polite"` to `#chat-log`.

**A10 — `response_model` types + drop redundant `JSONResponse`** *(B8)* · Effort: **M** · Breaking: **no** — Declare pydantic/TypedDict response models for at least the list + dossier endpoints and return plain dicts. Gives the frontend a real contract and populates the (currently empty-on-the-response-side) OpenAPI docs. Do it during the router split. Lower priority.

**A11 — `el({html})` hardening** *(F10)* · Effort: **S** · Breaking: **no** — Rename the attr to `unsafeHtml` and keep it only for known-static trusted HTML (the HELP blob); route any corpus/model text through `textContent`/the DOM builder. Currently safe (inputs are regex-escaped/same-origin) but the convenient door invites a future unsafe caller.

**A12 — Data-dir + registry hygiene** *(D4, D7, D8)* · Effort: **S–M** · Breaking: **no**
- Add **one authoritative side-car registry** — a table (README/CHECKPOINT) or, better, a `SIDE_CARS` dict in `config.py` both the app and a driver import — with columns *file | producing stage | consuming endpoint | regen command | graph-mutating?*. Add a `make enrich` / `enrich_all.py` driver that runs overlays in dependency order. Today MEMORY.md is the de-facto registry.
- Document `place_enrichment.json`'s implicit key-ownership contract (geocode_precise owns lat/lon via `rec.update()`; the dossier pass owns `what_it_was` etc.; merge-not-overwrite) *or* split into `place_geocode.json` + `place_dossiers.json` so each file has one producer.
- Move the timestamped `entity_engine.py.bak.*` files out of `stages/` into an attic dir (import-adjacent noise), and mark each `data/` artifact **canonical | derived | audit | backup** in the README so `entity_store.json` is distinguishable from `entities_preconsolidation.json` etc. Confirm the 812MB audit + `checkpoints/` are gitignored.

**A13 — Inline style strings → classes** *(F9)* · Effort: **M** · Breaking: **no** — Promote recurring `style:"…"` strings in JS `el()` calls and `index.html` structural nodes to utility classes in `components.css`. Do it *opportunistically* while touching each area, not as a separate sweep.

---

## (B) BEHAVIOR/CONTRACT CHANGES — need care, roll out additively

**B-1 — Standardize the list envelope + paginate the unbounded endpoints** *(B5)* · Effort: **M** · Breaking: **yes**
- **Change:** Adopt one envelope `{items, total, offset, limit}` across all list endpoints; the count key is currently `n`/`total`/`n_people`/`n_records`/`n_communities` depending on the endpoint. Add offset/limit to `/api/letters` (returns **all** matching rows today, unbounded) and `/api/entities` (limit-only). **Roll out additively** — add the new keys, keep the old ones through a deprecation window — so `app.js` keeps working; migrate the client, then remove.
- **Why:** Every call-site special-cases each endpoint's shape, and unbounded `/api/letters` won't scale.

**B-2 — Collapse the overlapping endpoint pairs** *(B7)* · Effort: **S** · Breaking: **yes**
- **Change:** Fold `/api/source` into `/api/region` (always return the deep-zoom image/pdf/manifest URLs, or gate them behind a `?deep` flag) — `region_payload()` already just calls `source_region()` and appends URLs. Make `/api/relationship` the Solanus-specialization of `/api/connection` (call the connection resolver with `a=SOLANUS_ID`) so the shared record-resolution logic lives once.
- **Why:** Two endpoints + two frontend paths for one concept = drift risk. Low severity — sequence it *after* the router split (A4-server) so you refactor once. Requires a coordinated frontend change.

---

## If you only do five things

1. **Delete the `/sources` mount and rotate the four+ API keys** (C1a). One line removes a 200-OK cleartext leak of every provider key, `.git`, and the 3GB data tree. Nothing in the app uses it.
2. **`.dockerignore` + inject secrets at runtime + set per-key monthly spend caps** (C1b). Stops the image from re-baking the same leak, and the budget cap is the backstop that bounds *any* future leak or abuse.
3. **Keep the backend ingress internal (SWA linked backend) and token-gate + rate-limit `/api/query`** (C1c). A public URL on a per-call-billed LLM endpoint is an open budget drain; internal-only is the cheapest, strongest guardrail.
4. **Add `_graph_indexes()` and fold the stale degree cache into it** (A1). Best impact/effort non-deploy change: turns tens-of-thousands-of-iterations-per-page into dict lookups, reusing the pattern `_people_index` already proves, with zero behavior change.
5. **Fix the graph.json coherence hazard: fold/register the three post-passes and register the enrichment side-cars in the DAG** (C2a + A5). Right now a routine `build_graph` re-run silently erases descriptions, kinship, and connection verifications and leaves all the LLM enrichment stale — the reproducibility promise is quietly false until this is closed.

Everything else is real but sequenced behind these: the three monolith splits (A4) are the highest-leverage *structural* work and unlock testability + pop-out reuse, but they're large and non-blocking — do them one region per commit after the must-fixes land.