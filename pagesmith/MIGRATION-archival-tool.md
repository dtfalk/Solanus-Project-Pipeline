# Migrating the archival research tool onto Pagesmith

Goal (David's): rebuild the whole dynamic app as a Pagesmith **instance**, adding only **GENERAL**,
non-project-named components to the engine catalogue. The engine must never learn anything about this
project; the project is only `site-config` + `data/*.json` + `css` + the generic blocks it composes.

This is a genuine multi-session migration (the app embeds Cytoscape, Leaflet, Drawflow, TalkingHead,
OpenSeadragon, and a streaming RAG chat). This doc specifies the port so it can proceed in passes.

## What's reusable vs new
Pagesmith already ships generic blocks: `text, heading, gallery, video, carousel, embed, links, quotes,
button, chart, stat-cards, spotlight, ranking, group`. The dynamic views become NEW generic catalogue
parts (in `app/javascript/blocks.js`, generically named — never `solanus-*`):

| App view (today) | New GENERAL block | Block config (all instance-supplied, engine stays neutral) |
|---|---|---|
| streaming cited chat | **`conversation`** | `{ endpoint, streamEndpoint, personasEndpoint, ttsEndpoint, placeholder }` |
| Cytoscape KG viz | **`graph-explorer`** | `{ dataEndpoint, entityEndpoint, layouts[], colorBy }` |
| Leaflet markers map | **`marker-map`** | `{ markersEndpoint, center:[lat,lon], zoom, tileUrl, attribution }` |
| Drawflow live pipeline | **`flow-diagram`** | `{ graphEndpoint, stateEndpoint }` (nodes light from a trace) |
| TalkingHead avatar | **`avatar-stage`** | `{ glbUrl, ttsEndpoint }` |
| OpenSeadragon citation viewer | **`deep-zoom-viewer`** | `{ regionEndpoint }` (opened by a `conversation` citation click) |
| collapsible sidebar + nav | **`app-sidebar`** + **`view-tabs`** | `{ items[], collapsible }` |

Each block follows the part contract (render(block, ctx) → DOM, optional init(), CSS namespaced
`catalog-`, `fields[]`/`blank()` for the editor). They load their library from a CDN once (mirroring how
the current app loads Cytoscape/Leaflet/Drawflow), and they talk ONLY to endpoints named in the block —
so the same `graph-explorer` works for any dataset, not just this archive.

## The instance (this project only)
- `app/javascript/site-config.js` — siteName, wordmark, theme accent = Capuchin brown `#3C1605`, pages.
- `app/css/*` — the Province brand (Raleway ExtraBold / Open Sans, the palette already in the live app's
  theme.css; copy it in as instance CSS).
- `app/data/page-*.json` — pages composed of the blocks above, each pointing at the FastAPI endpoints the
  current app already serves (`/api/ask_stream`, `/api/graph`, `/api/agent_graph`, `/api/region`, …). The
  Python backend (app/server.py) is reused unchanged — Pagesmith is the front-end shell.

## Render / verify locally
The stack is available here (`azurite` + `func` installed): `./start-local.sh` → http://localhost:4280.
Seed `app/data` + `app/media-content` into Azurite (the script does this). Then drive headless Chromium
(rev 1208 binary at `~/.cache/ms-playwright/chromium-1208/chrome-linux64/chrome`) to screenshot + verify,
exactly as the live app was verified this session.

## Build order (suggested passes)
1. **Instance shell**: site-config + brand CSS + a `page-home.json` from existing generic blocks; render.
2. **`conversation` block** (highest value): port the streaming cited chat as a general block; wire to
   `/api/ask_stream`; citations open a `deep-zoom-viewer`. Render + screenshot.
3. **`graph-explorer` + `marker-map`**: port Cytoscape + Leaflet as general blocks.
4. **`flow-diagram` + `avatar-stage`**: port Drawflow + TalkingHead.
5. **`app-sidebar` + `view-tabs`**: the collapsible shell (from Job 1) as general layout blocks.
6. Replace the hand-rolled `app/static/*` with the Pagesmith instance once parity is reached.

## Rules (David's, non-negotiable)
- General first, specialize via config. **No component may be named after this project.** The engine must
  not reference the archive, Solanus, Capuchins, etc. Only the instance (config/data/css) is specific.
- Never make the framework *less* general; only add capability.
