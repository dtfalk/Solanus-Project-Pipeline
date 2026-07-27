# Pagesmith

A tiny, dependency-free **page-builder engine** for JavaScript web apps: a minimal
*skeleton*, a *catalogue* of drop-in content parts, an inline visual *editor*, and a
small blob-storage *backend*. You build pages by editing JSON (or clicking around in
the editor) — there is no per-page code.

Pagesmith is deliberately written in **plain, old, boring JavaScript** — no build
step, no framework, no npm front-end dependencies. The goal is longevity: it should
keep working for as long as browsers have `for` loops and `if` statements. You grow
it by adding small, self-contained parts to the catalogue, not by upgrading a tower
of tooling.

> This folder is a **clean starter** — a "virgin" instance. Open it, run it, and
> start building your first site.

---

## The one idea

Pagesmith is split into two halves, and keeping them apart is the whole design:

```
            ENGINE  (generic — you never edit it)              INSTANCE  (you edit this)
  ┌───────────────────────────────────────────────┐   ┌──────────────────────────────────────┐
  │  skeleton   page.js · sections.js · theme.js   │   │  site-config.js   (the manifest)       │
  │             style.js                            │   │  data/*.json      (your content)       │
  │  catalogue  blocks.js   (the parts)             │◄──┤  css/*.css        (your styling)       │
  │  editor     edit-mode.js                        │   │  html-pages/*     (page shells/chrome) │
  │  chrome     global.js · header.js · footer.js   │   │  site-blocks.js   (your custom parts)  │
  │  backend    api/*                               │   └──────────────────────────────────────┘
  └───────────────────────────────────────────────┘
```

The engine knows nothing about *your* site. It reads everything it needs from the
instance files at runtime. So **a new site = new config + new content; the engine is
untouched.** See `guides/` for the full story.

A page is just a JSON file — a flat list of typed **blocks**:

```jsonc
// data/page-home.json
{
  "title": "Home",
  "theme": { "accent": "#4f7cff" },
  "sections": [
    { "id": "h", "type": "heading", "text": "Hello" },
    { "id": "t", "type": "text", "title": "About", "body": "…" }
  ]
}
```

The engine looks up each block's `type` in the catalogue and asks it to render. (Same
model as Gutenberg / Editor.js / Puck — just much smaller.)

---

## Run it locally (no cloud)

```bash
./start-local.sh
```

This starts Azurite (a local blob emulator), seeds it from `app/data` +
`app/media-content`, and launches the Static Web Apps emulator at
**http://localhost:4280**.

To **edit**: open the site → you'll be taken to a login screen → sign in as user
`dev` and **add the role `editor`** → the Edit toolbar appears. Toggle Edit, change
text/photos, **Save** (writes the draft), then **Publish** (promotes draft → live).

**Requirements:** Node 20, plus these installed globally
(`npm i -g`): `azurite`, `@azure/static-web-apps-cli`, `azure-functions-core-tools@4`.

---

## Project layout

```
app/
  index.html              home page shell        (instance)
  login.html              private-draft sign-in  (instance)
  staticwebapp.config.json  routes/auth          (instance)
  css/
    layout.css            global reset + sticky footer   (ENGINE — keep)
    header.css footer.css chrome styling                 (instance)
  data/
    global.json           nav / social / footer / survey / theme   (instance)
    page-home.json        the home page content                    (instance)
    page-showcase.json    a demo page exercising the catalogue      (instance)
  html-pages/
    page.html             the universal page shell  (ENGINE — keep)
    header.html footer.html  chrome markup           (instance)
  javascript/
    page.js sections.js theme.js style.js   skeleton   (ENGINE)
    blocks.js                               catalogue  (ENGINE)
    edit-mode.js                            editor     (ENGINE)
    global.js header.js footer.js
      load-fragments.js lightbox.js media-base.js  chrome/plumbing  (ENGINE)
    site-config.js          the manifest            (instance)
    site-blocks.js          your custom parts       (instance — starts empty)
  media-content/            images, logo            (instance)
api/                        Azure Functions backend (ENGINE — needs zero site config)
infra/ scripts/ .github/    deploy automation examples (adapt per deployment)
guides/                     the documentation (Markdown + compiled PDFs)
```

---

## Make it your own

1. **Rename the site.** Edit `app/javascript/site-config.js` — `siteName`, `wordmark`,
   `pages`, `socialIcons`, and (when you go live) `prodHosts`.
2. **Replace the content.** Edit `app/data/global.json` (nav/social/footer/theme) and
   the `page-*.json` files — or just build pages in the editor and Save.
3. **Restyle.** Most colour comes from `theme.accent` in `global.json` (via the
   `--site-*` CSS variables). Tweak `app/css/header.css` / `footer.css` for chrome.
4. **Add custom parts.** Drop site-specific blocks into `app/javascript/site-blocks.js`
   (it ships empty, with a worked template inside).
5. **New pages need no code.** Any `/p/<slug>` URL renders `data/page-<slug>.json`
   through the universal shell. Add a clean URL with one rewrite in
   `staticwebapp.config.json` if you want (`/showcase` is the worked example).

---

## Documentation (`guides/`)

| Guide | Audience | What it covers |
|---|---|---|
| **Skeleton & Catalogue** (`guide-skeleton-catalogue`) | developers | how the engine works end to end, the data model, the part contract, and the rules every part must follow |
| **Editor — User Guide** (`guide-editor-user`) | site owners | what the editor is, how to run it, and how to build & edit every kind of content |
| **Editor — Developer Guide** (`guide-editor-dev`) | developers | how the editor is built: panels, the data-edit-* protocol, save/publish, and how to extend it |

Each guide is provided as **Markdown** (in `guides/`) and as a compiled **PDF** (also
in `guides/`); the LaTeX sources and build artifacts live in `guides/latex/`.

---

## Deploy

The app is a standard Azure **Static Web App** (the `./app` folder) linked to an
**Azure Functions** backend (`./api`). "Dev vs prod" is just **draft vs published
blobs**: you edit on a draft host (editor on), then **Publish** copies the draft
containers to the live ones. The hostname switch lives in `site-config.js`
(`prodHosts`). The scripts in `infra/`, `scripts/`, and `.github/` are working
examples to adapt to your own Azure resources — see the Editor Developer Guide.
