# Pagesmith — The Skeleton & Catalogue (Developer Guide)

**Author:** David Falk

> **Who this is for.** You are a developer who wants to build on, extend, or
> simply understand the Pagesmith engine. By the end you will know exactly what
> happens between a URL being typed and pixels appearing on screen, how a "part"
> is written, and what rules a part must obey so it composes with everything
> else and stays portable across sites. You will be able to add a new part in
> twenty lines and stand up a brand-new site without touching the engine.

---

## Table of contents

1. [Philosophy: why it is built this way](#1-philosophy)
2. [The data model: a page is a JSON file](#2-the-data-model)
3. [Boot and load order](#3-boot-and-load-order)
4. [The render data-flow](#4-the-render-data-flow)
5. [The catalogue and the part contract](#5-the-catalogue-and-the-part-contract)
6. [The rules every part must follow](#6-the-rules-every-part-must-follow)
7. [The editing protocol](#7-the-editing-protocol)
8. [The two namespaces: `engine-` vs `catalog-`](#8-the-two-namespaces)
9. [Theming with CSS variables](#9-theming)
10. [The backend in one section](#10-the-backend-in-one-section)
11. [Standing up a new site](#11-standing-up-a-new-site)

---

## 1. Philosophy

Before any code, it is worth understanding *why* Pagesmith looks the way it
does, because every design decision downstream falls out of three commitments.

### Plain, dependency-free JavaScript

There is no framework here. No React, no Vue, no build step, no `npm install`
on the front end, no bundler, no transpiler. Every file under
`app/javascript/` is an IIFE — an old-fashioned `(function () { ... })()` — that
runs directly in the browser exactly as written. The catalogue builds DOM with
`document.createElement`. State lives in a JSON file. Events are plain
`CustomEvent`s.

This is a deliberate, almost stubborn choice, and the reason is **longevity**.
A framework is a bet that a particular toolchain will still be installable,
patchable, and well-understood in five or ten years. That bet frequently loses:
the build breaks, a transitive dependency is abandoned, a major version forces a
rewrite. Plain ES5-ish JavaScript served as static files has no such failure
mode. If a browser can run it today, it will run it in a decade. The cost is
that you write a little more by hand; the payoff is that the thing keeps working
with zero maintenance. For a tool whose whole job is to outlive the attention
span of whoever set it up, that trade is the entire point.

> **Aside.** You will notice the code is written in a careful, conservative
> dialect: `var` not `let`, `function` expressions not arrows in the hot paths,
> `[].forEach.call(...)` for NodeLists. That is not nostalgia — it is the same
> longevity instinct. The fewer language features you lean on, the fewer ways
> the floor can move under you.

### The engine-vs-instance split

Pagesmith is two halves that never blur into each other:

```
  ENGINE (generic, brand-neutral, never edited per site)
  ┌──────────────────────────────────────────────────────┐
  │  skeleton    page.js · sections.js · theme.js · style.js
  │  catalogue   blocks.js  (the generic parts)
  │  editor      edit-mode.js
  │  chrome      global.js (hydrates header/footer)
  │  backend     api/  (Azure Functions)
  └──────────────────────────────────────────────────────┘
                          ▲  reads at runtime
                          │
  INSTANCE (everything that makes THIS deployment specific)
  ┌──────────────────────────────────────────────────────┐
  │  site-config.js   the manifest (name, hosts, tabs…)
  │  site-blocks.js   your bespoke parts (ships empty)
  │  data/*.json      the content (pages + global chrome)
  │  css/             header/footer/layout styling
  │  html-pages/      page shells
  └──────────────────────────────────────────────────────┘
```

The engine knows *nothing* about your site — not its name, colours, pages, or
social links. It reads all of that from the instance at runtime. The contract is
strict and one-directional: the instance depends on the engine; the engine never
depends on the instance. The single bridge is `site-config.js`, a plain manifest
of values, plus the optional `site-blocks.js`, where you register parts that only
make sense for one site.

The reason this matters is **reuse without forking**. Because the engine carries
no site-specific strings, the same battle-tested engine files can be dropped into
any number of sites unchanged. Bug fixes and new parts flow to every site by
copying engine files; nothing site-specific ever has to be untangled first. To
stand up a new site you change values, not code.

> **The litmus test for "does this belong in the engine?"** If a line of code
> mentions a brand, a colour by name, a specific page, or a specific URL, it is
> *instance*, and it belongs in `site-config.js` / `site-blocks.js` / `data/`.
> The engine deals only in *roles* and *shapes*.

---

## 2. The data model

This is the keystone, so go slowly.

**A page is a JSON file.** That is the whole model. A page lives at
`data/page-<slug>.json` and has this shape:

```json
{
  "title": "Showcase",
  "theme": { "accent": "#ff3b8e" },
  "sections": [
    { "id": "sc-h", "type": "heading", "visible": true, "text": "Component showcase" },
    { "id": "sc-intro", "type": "text", "title": "The catalogue",
      "body": "These are the building blocks Pagesmith ships with." }
  ]
}
```

Three top-level keys, and only `sections` is required:

- **`title`** *(optional)* — sets the browser tab title (suffixed with the site
  name from `site-config.js`).
- **`theme`** *(optional)* — a per-page colour/background override (see
  [§9](#9-theming)). Absent → the page inherits the site-wide theme from
  `global.json`.
- **`sections`** — an **ordered, flat array of typed blocks**. This is the page.

### Blocks are flat and typed

Every entry in `sections[]` is a **block**: a plain object whose `type` names a
part in the catalogue, plus whatever data that part needs. The engine guarantees
each block has an *envelope* of three fields, filling them in if absent
(`normalize()` in `blocks.js`):

| Field | Meaning |
|---|---|
| `id` | A stable identifier (the editor keys reorder/hide/remove on it). |
| `type` | Which catalogue part renders it (`text`, `gallery`, `chart`, …). Unknown type → falls back to `text`. |
| `visible` | `false` hides the block on the published page; anything else shows it. |
| `style` | *(optional)* per-block inline styling, see [`style.js`](#9-theming). |

Everything *else* on the block is part-specific payload. A `gallery` carries
`images: [...]`; a `chart` carries `kind` and `series: [...]`; a `spotlight`
carries `value`, `label`, `caption`. The engine does not care — it hands the
whole block to the part and lets the part read what it needs.

> **Why flat, not a tree?** A flat array is trivial to reason about, trivial to
> reorder (it is just array index manipulation), and trivial to diff and store.
> Nesting *is* supported — the `group` part holds a `children: [...]` array of
> blocks and renders them recursively — but nesting is opt-in, owned by one
> part, rather than baked into the model. The model stays a list; complexity
> lives in the parts that want it.

Here is a fuller, real example showing several part types and a nested group:

```json
{
  "title": "Showcase",
  "sections": [
    { "id": "sc-h",   "type": "heading", "text": "Component showcase" },
    { "id": "sc-gal", "type": "gallery", "title": "Photo gallery",
      "images": [
        { "src": "/media-content/sample-1.svg", "alt": "Sample one" },
        { "src": "/media-content/sample-2.svg", "alt": "Sample two" }
      ] },
    { "id": "sc-chart", "type": "chart", "title": "Monthly visitors", "kind": "bar",
      "series": [ { "label": "Jan", "value": 1200 }, { "label": "Feb", "value": 1800 } ] },
    { "id": "sc-group", "type": "group", "title": "Parts inside parts",
      "children": [
        { "id": "g-h", "type": "heading", "text": "Grouped heading" },
        { "id": "g-b", "type": "button", "label": "A nested button", "url": "#" }
      ] }
  ]
}
```

The shared site chrome (header, footer, nav, social links, theme default) lives
in one more file of the same family, `data/global.json`. It is not a page — it
has no `sections[]` — but it flows through the very same machinery (see
`global.js` and [§4](#4-the-render-data-flow)).

---

## 3. Boot and load order

A page is an almost-empty HTML shell (`html-pages/page.html`). It contains a
`<main></main>` and almost nothing else — there is *no per-page CSS or JS*. The
body is built entirely from the page's `sections[]` at runtime. So the
interesting question is: what scripts load, and in what order, and why does the
order matter?

The `<head>` loads exactly two scripts synchronously:

```html
<script src="/javascript/site-config.js"></script>
<script src="/javascript/media-base.js"></script>
```

**`site-config.js` is first, always, on every page.** It defines
`window.SITE_CONFIG` and the read-only accessor `window.SITE`. Because it is
synchronous and first, `window.SITE` is guaranteed to exist before any other
engine code runs. This matters because the very next thing to run —
`media-base.js` — already needs to ask `SITE.isProd(host)` to decide whether to
load the editor and which storage container to read. If you add a new HTML shell
and forget this script, prod-gating, the wordmark, media tabs, and theming all
silently fall back to harmless defaults — so it must come first.

**`media-base.js` is the bootstrapper.** It does two jobs, then injects the rest
of the engine. Its first job is URL plumbing: it patches `window.fetch` so that
`/data/X.json` is redirected to the correct blob-storage container (draft vs.
published, chosen by hostname), and it rewrites `/media-content/...` references
to the real storage URL everywhere they appear — in fetched JSON bodies, in DOM
attributes, even in CSS rules — via a `MutationObserver` that keeps doing it for
dynamically-rendered nodes. This is what lets the same HTML/JSON work whether
served from local files, a local Azurite emulator, or cloud blob storage,
without per-file edits.

Its second job is to **inject the remaining engine scripts in a deliberate
order** by appending `<script>` tags. The order is the whole ballgame:

```
        page.html <head>
        ┌─────────────────────────────┐
        │ 1. site-config.js  (sync)    │   window.SITE ready
        │ 2. media-base.js   (sync)    │   patches fetch; injects ↓
        └─────────────────────────────┘
                      │ appends, in this order:
                      ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  global.js     (defer)     hydrate header/footer from global.json
   │  lightbox.js   (defer)     site-wide image lightbox
   │  style.js      (async=false)  EngineStyle — must exist before parts render
   │  blocks.js     (async=false)  THE CATALOGUE — window.Catalog registry
   │  site-blocks.js(async=false)  your bespoke parts — EXTEND the registry
   │  sections.js   (async=false, defer)  the renderer — CONSUMES the registry
   │  theme.js      (defer)     EngineTheme — event-driven, order-independent
   │  edit-mode.js  (defer)     editor runtime — DRAFT hosts only
   └──────────────────────────────────────────────────────────────┘
```

Two kinds of ordering are at work, and they are doing different things:

1. **`async = false` ordered execution** for `style.js → blocks.js →
   site-blocks.js → sections.js`. Dynamically-injected scripts default to
   `async`, which means "run whenever you finish downloading" — a race. Setting
   `async = false` forces them to execute *in insertion order*, regardless of
   download timing. This gives a strict dependency chain:
   - `style.js` publishes `window.EngineStyle` first, because the catalogue
     calls `EngineStyle.apply()` while rendering each block.
   - `blocks.js` defines `window.Catalog` / `window.CatalogBlocks` — the
     registry — next.
   - `site-blocks.js` runs *after* `blocks.js` so the registry already exists
     and it can simply add to it.
   - `sections.js` runs *last* of the four, so by the time it renders, the
     registry (generic + bespoke parts) is complete.

2. **`defer` + event-driven** for everything else. `global.js` and `theme.js`
   do *not* care when they load relative to `page.js`, because they listen for
   the `engine-data-ready` event (and have a fallback for the case where the
   event already fired). More on that next.

> **Why two mechanisms?** The catalogue chain has a real *code* dependency
> (`sections.js` literally calls `window.Catalog.render`), so it needs hard
> ordering. Theming and chrome only have a *data* dependency (they need the
> page's JSON, whenever it arrives), so they use events and become
> order-independent — which is more robust than trying to order everything.

Finally, in the `<body>`, the shell loads `load-fragments.js`, `header.js`,
`footer.js`, and `page.js`. `page.js` is the trigger that actually fetches the
page's content and kicks off rendering.

---

## 4. The render data-flow

Now the heart of it: how data becomes DOM. The flow is **event-driven and
order-independent**, which is the design that makes the whole thing robust to the
load-order races above.

```
   URL  /p/showcase
     │
     ▼
  ┌──────────────┐   pageKey()        derive "page-showcase"
  │   page.js    │ ──────────────►  fetch('/data/page-showcase.json')
  └──────────────┘                       │ (media-base.js redirected this
        │                                │  to the right blob container)
        │  normalise to {sections:[]}    ▼
        │  set document.title        JSON data
        │
        ▼
   dispatch  CustomEvent('engine-data-ready', {detail:{file, data}})
   (also stashed at window.__ENGINE_DATA__ for the late-listener race)
        │
        ├───────────────────────────────┬───────────────────────────┐
        ▼                                ▼                           ▼
 ┌──────────────┐                ┌──────────────┐            ┌──────────────┐
 │ sections.js  │                │   theme.js   │            │ edit-mode.js │
 │ render each  │                │ data.theme → │            │ (draft only) │
 │ block via    │                │ --site-* /   │            │ binds editor │
 │ window.Catalog│               │ --cat-* vars │            │ to the DOM   │
 └──────┬───────┘                └──────────────┘            └──────────────┘
        │ for each block:
        │   Catalog.normalize(block, i)   → ensure id/type/visible
        │   Catalog.isVisible(block)      → skip hidden on the page
        │   Catalog.render(block, ctx)    → DOM node  (style.js applies styling)
        ▼
   append nodes into  #engine-sections  inside <main>
        │
        ▼
   dispatch  CustomEvent('engine-rebind')   → tells the editor to bind new nodes
```

Walk through it once concretely. `page.js` runs on `DOMContentLoaded`. It calls
`pageKey()`, which derives a content-file key from the URL in priority order: an
explicit `<meta name="engine-page">` (used by hand-built shells like the home
page), then a `/p/<slug>` route, then a clean top-level slug, defaulting to
`home`. The slug is sanitised to `[a-z0-9-]` so the resulting filename can never
traverse out of `/data/`. It then `fetch`es `/data/<key>.json`.

A missing file is *not* an error — a brand-new page simply has no JSON yet, so a
404 becomes an empty page (`{sections: []}`), which the editor can start filling.
`page.js` normalises whatever it got to a valid shape, sets the tab title,
stashes the data on `window.__ENGINE_DATA__`, and fires `engine-data-ready`.

That event is the linchpin. `page.js` does not *call* the renderer or the
themer; it *announces* the data and lets anyone interested react. `sections.js`
listens and renders. `theme.js` listens and re-colours. `edit-mode.js` listens
and wires up editing. None of them needs to have loaded before `page.js` ran —
each also checks `window.__ENGINE_DATA__` on startup to catch the case where the
event fired *before* the listener attached. This is the order-independence in
action: the load order can shuffle and the page still composes correctly.

`sections.js` is intentionally ignorant. Look at how little it knows: it never
hard-codes a block type. For each entry in `sections[]` it builds a small `ctx`
object and calls `window.Catalog.render(block, ctx)`, appending the returned
node into a `#engine-sections` host inside `<main>`. New part types appear with
zero changes to `sections.js` — that is the catalogue pattern paying off. When it
is done it fires `engine-rebind` so the editor (if present) can bind the freshly
created nodes.

> **Note on `global.json`.** The shared chrome flows through the *same* event.
> `global.js` fetches `global.json`, hydrates the header/footer, and dispatches
> `engine-data-ready` with `file: 'global'`. `sections.js` deliberately ignores
> that one (`if (e.detail.file === 'global') return;` — global is chrome, not a
> page), while `theme.js` happily applies `global.json`'s theme as the site-wide
> default *before* a page's own theme arrives and overrides it.

---

## 5. The catalogue and the part contract

This is the centrepiece. The catalogue (`blocks.js`) is a **registry of part
types**. Each entry is a fully self-contained component bundling four things that
in a framework would be scattered across files:

- its **markup** — a `render()` function returning a DOM node;
- its **styling** — CSS, injected once, namespaced under `catalog-`;
- its **edit schema** — `fields[]` / `lists{}` the editor reads to draw controls;
- its **behaviour** — an optional `init()` run after the node is in the DOM.

Adding a capability to the *entire system* — a new kind of section anyone can add
from the picker, that the editor can edit, that themes correctly, that the
backend persists — is **adding one object to this registry**. Nothing leaks into
the skeleton, the renderer, or the editor. That containment is what makes the
engine portable.

### Anatomy of a part

```
  window.CatalogBlocks['mytype'] = {
  ┌─────────────────────────────────────────────────────────────────┐
  │  PICKER METADATA                                                  │
  │    label     "My Thing"      shown in the + Add picker            │
  │    category  "Media"         which picker group                   │
  │    icon      "★"             a glyph for the tile                 │
  │    hidden    true            omit from picker (site packs)        │
  ├─────────────────────────────────────────────────────────────────┤
  │  blank()  →  { type:'mytype', ... }                              │
  │     the default block created when someone adds one.             │
  │     MUST set `type`. Keep placeholder copy generic.             │
  ├─────────────────────────────────────────────────────────────────┤
  │  EDIT SCHEMA  (consumed by the editor's inspector)              │
  │    fields[]   scalar inputs:  {key,type,label,options?}        │
  │               type ∈ text|textarea|url|number|select|          │
  │                      color|bool|image|embed                     │
  │    lists{ key: { label, fields[] } }                           │
  │               array editors (a gallery's images, a chart's     │
  │               series): each item edited with the given fields. │
  ├─────────────────────────────────────────────────────────────────┤
  │  render(block, ctx)   REQUIRED   → DOM node                     │
  │     build markup from `block`, using ctx.el(...).              │
  │     emit data-edit-* attributes for editability.              │
  │     stamp data-item-index on list children.                   │
  │     namespace any CSS classes under catalog-.                 │
  ├─────────────────────────────────────────────────────────────────┤
  │  init(node, block, ctx)   optional                             │
  │     wire behaviour after the node is in the DOM               │
  │     (e.g. carousel arrows). Errors here are non-fatal.        │
  └─────────────────────────────────────────────────────────────────┘
  };
```

### The `ctx` object

Every `render(block, ctx)` (and `init`) receives a small context object built by
the renderer. It is the part's entire window onto the engine — a part never
reaches for globals it does not need:

| `ctx` key | What it is | Why you need it |
|---|---|---|
| `ctx.el` | A tiny DOM builder: `el(tag, attrs, kids)`. `text`/`html` keys are special-cased; everything else becomes an attribute. | Build markup without verbose `createElement` boilerplate, and consistently with the rest of the catalogue. |
| `ctx.mediaUrl` | `mediaUrl(path)` — resolves a `/media-content/...` path to the real (blob/Azurite/local) URL. | So your `<img src>` works in every environment. |
| `ctx.file` | The content-file key (e.g. `page-showcase`). | Goes into `data-edit-file` so the editor knows which JSON to write back. |
| `ctx.index` | This block's index in `sections[]`. | Useful for unique ids (e.g. a chart gradient id) and lightbox grouping. |
| `ctx.path` | The data-path to this block, e.g. `sections[2]`. | The base for every `data-edit-*` path you emit. **This is the most important field.** |

The `block` argument is just the block's data — the same object that sat in the
JSON `sections[]`.

### Two real parts, read closely

The smallest useful part — `text`:

```js
text: {
  label: 'Text', category: 'Basic', icon: '¶',
  blank: function () { return { type: 'text', title: 'New section', body: 'Add your text here.' }; },
  render: function (block, ctx) {
    var sec = ctx.el('section', { 'class': 'engine-section' });
    sec.appendChild(ctx.el('h2', {
      'class': 'engine-section-title',
      text: block.title || '',
      'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.title'
    }));
    sec.appendChild(ctx.el('div', {
      'class': 'engine-section-body', html: block.body || '',
      'data-edit-file': ctx.file, 'data-edit-html': ctx.path + '.body'
    }));
    return sec;
  }
}
```

Note what makes the title and body *editable*: nothing more than the
`data-edit-file` + `data-edit-text` / `data-edit-html` attribute pairs, with the
path built off `ctx.path`. The part author writes no editor code at all.

A list-bearing part — `gallery` — adds a `lists{}` schema and stamps
`data-item-index` on each child:

```js
gallery: {
  label: 'Photo gallery', category: 'Media', icon: '▦',
  blank: function () { return { type: 'gallery', title: 'New gallery', images: [] }; },
  lists: { images: { label: 'Image', fields: [
    { key: 'src', type: 'image', label: 'Image' },
    { key: 'alt', type: 'text',  label: 'Alt text' }
  ] } },
  render: function (block, ctx) {
    var sec = ctx.el('section', { 'class': 'engine-section' });
    sec.appendChild(/* titleEl, as in text */);
    var grid = ctx.el('div', { 'class': 'engine-gallery',
      'data-edit-file': ctx.file,
      'data-edit-list': ctx.path + '.images',
      'data-edit-list-label': 'images' });
    (Array.isArray(block.images) ? block.images : []).forEach(function (im, j) {
      var src = (im && im.src) ? im.src : '';
      grid.appendChild(ctx.el('img', {
        src: ctx.mediaUrl(src), alt: (im && im.alt) || '', loading: 'lazy',
        'data-item-index': j,                              // TRUE array index
        'data-edit-file': ctx.file,
        'data-edit-image': ctx.path + '.images[' + j + '].src'
      }));
    });
    sec.appendChild(grid);
    return sec;
  }
}
```

The container carries `data-edit-list` (so the editor's list machinery binds to
it), and each child carries both an editable path
(`...images[j].src`) and its true `data-item-index`. The `Array.isArray(...)`
guard means a malformed or missing `images` degrades to an empty grid instead of
throwing.

### How the renderer drives a part

`renderOne(block, ctx)` in `blocks.js` is the only thing that calls your part,
and it does so in a fixed sequence — worth knowing because it tells you what is
guaranteed by the time your code runs:

1. `ensureCSS()` — inject the catalogue stylesheet once (lazy, idempotent).
2. refresh `STATPAL` from the live `--cat-*` palette (so theming is current).
3. look up `BLOCKS[block.type]`, falling back to `text` for unknown types.
4. call your `render(block, ctx)` to get a node.
5. stamp engine bookkeeping attributes on the node: `data-engine-block`
   (the id), `data-engine-index`, `data-engine-path`.
6. apply block-level `style` via `EngineStyle.apply()` and per-text
   `textStyles` via `EngineStyle.applyTextStyles()`.
7. call your `init(node, block, ctx)` inside a `try/catch` — a throw here is
   non-fatal and never takes the page down.

### Worked example: adding a part to `site-blocks.js`

`site-blocks.js` ships empty on purpose, with a commented template. Here is the
full procedure to add a bespoke `hero` part. Everything below goes inside the
file's IIFE.

**Step 1 — guard, then register on the existing registry.** `site-blocks.js`
runs after `blocks.js`, so `window.CatalogBlocks` already exists. Add to it; do
not redefine it.

```js
(function () {
  'use strict';
  if (!window.CatalogBlocks) return;   // engine not loaded → no-op, never throw

  window.CatalogBlocks['hero'] = {
    // Step 2 — picker metadata so it shows up in "+ Add section".
    label: 'Hero', category: 'Layout', icon: '★',

    // Step 3 — blank(): the default block created from the picker. MUST set type.
    blank: function () {
      return { type: 'hero', title: 'Big headline', subtitle: 'Supporting line' };
    },

    // Step 4 — edit schema for scalar fields (the editor draws these inputs).
    fields: [
      { key: 'title',    type: 'text', label: 'Headline' },
      { key: 'subtitle', type: 'text', label: 'Subtitle' }
    ],

    // Step 5 — render(): build the DOM, emit data-edit-* for editability,
    // namespace CSS under catalog-.
    render: function (block, ctx) {
      ensureHeroCSS();   // see step 6
      var sec = ctx.el('section', { 'class': 'engine-section catalog-hero' });
      sec.appendChild(ctx.el('h1', {
        'class': 'catalog-hero-title',
        text: block.title || '',
        'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.title'
      }));
      sec.appendChild(ctx.el('p', {
        'class': 'catalog-hero-sub',
        text: block.subtitle || '',
        'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.subtitle'
      }));
      return sec;
    }
  };

  // Step 6 — inject CSS once, lazily, namespaced, theme-aware.
  function ensureHeroCSS() {
    if (document.getElementById('catalog-hero-styles')) return;
    var s = document.createElement('style');
    s.id = 'catalog-hero-styles';
    s.textContent = [
      '.catalog-hero{text-align:center;padding:3rem 1rem}',
      '.catalog-hero-title{font-size:clamp(2rem,6vw,4rem);margin:0;color:var(--site-ink,#fff)}',
      '.catalog-hero-sub{opacity:.8;margin:.6rem 0 0;color:var(--site-ink,#fff)}'
    ].join('\n');
    document.head.appendChild(s);
  }
})();
```

**Step 7 — that's it.** Reload a draft page. The `hero` part now appears in the
"+ Add section" picker, can be added, its `title`/`subtitle` are click-to-edit on
the page *and* editable from the inspector's fields, its styling is namespaced
and theme-aware, and Save persists it into the page JSON like any other block.
You wrote one object and one CSS helper — no changes to `sections.js`,
`page.js`, the editor, or the backend.

If your part needs runtime behaviour (say, an auto-rotating hero), add an
`init(node, block, ctx)`; if it has an array (say, multiple slides), add a
`lists{}` schema and stamp `data-item-index` on each child, exactly like
`gallery`.

---

## 6. The rules every part must follow

A part is portable and composable only if it obeys a handful of rules. Treat
this as a checklist when you write or review one. Each rule exists because
violating it breaks composition with *other* parts, or breaks portability across
*other* sites.

- [ ] **Namespace every CSS class under `catalog-`.** A part's CSS must not
      collide with the skeleton (`engine-`), the editor, or another part. Class
      names like `.catalog-hero-title`, not `.title`. *(Why: two parts on one
      page, or one part on a foreign site's CSS, must not leak styles into each
      other.)*

- [ ] **Inject CSS once, lazily, on first render.** Guard with a unique
      `id` (`if (document.getElementById('catalog-hero-styles')) return;`) and
      append a single `<style>`. *(Why: a part must look right on *any* page
      without depending on the page's stylesheet, and must not re-inject N
      times for N instances.)*

- [ ] **Emit `data-edit-*` attributes for everything editable.** Text →
      `data-edit-text`; rich text → `data-edit-html`; images →
      `data-edit-image`; arrays → `data-edit-list`; an attribute like a link
      URL → `data-edit-attr`. Always pair with `data-edit-file: ctx.file`.
      *(Why: this is the only way the generic editor can edit your part — you
      write no editor code.)*

- [ ] **Build paths off `ctx.path`.** Never hard-code `sections[2]`; always
      `ctx.path + '.title'` or `ctx.path + '.images[' + j + ']'`. *(Why: the
      same part renders at different indices, and inside groups at deep paths
      like `sections[1].children[0]`. `ctx.path` is correct everywhere.)*

- [ ] **Stamp `data-item-index` on every list child.** When you render an
      array, put the item's *true* array index on each child node. *(Why: the
      on-page drag-to-reorder uses it to move the right item even when some
      items are filtered out of the render — e.g. a chart skips unlabelled
      points but each chip still carries its real index.)*

- [ ] **Guard with `Array.isArray` / falsy checks — degrade, never throw.**
      `(Array.isArray(block.images) ? block.images : []).forEach(...)`, and
      skip `if (!item) return;`. *(Why: content can be partial or malformed,
      especially mid-edit. A part that throws takes the whole render down; a
      part that degrades shows an empty-but-valid section. The renderer wraps
      `init()` in try/catch for the same reason.)*

- [ ] **Be theme-aware via CSS variables.** Read `var(--site-accent, ...)`,
      `var(--site-ink, ...)`, `var(--cat-1..N, ...)`, `var(--cat-spot, ...)` —
      *always with a sensible fallback*. Never hard-code a brand colour.
      *(Why: a single theme change in JSON must re-colour your part with no
      code edit, and the fallback keeps the part looking good with no theme
      configured at all.)*

- [ ] **No site-specific strings in `blocks.js`.** Anything that names a brand,
      a real page, or a real URL belongs in `site-blocks.js` (or content),
      never in the generic catalogue. *(Why: `blocks.js` is engine — it must
      drop into any site unchanged. Placeholder copy in `blank()` must stay
      generic for the same reason.)*

> **The unifying idea.** Every rule is a corollary of one principle: *a part is
> a black box that talks to the engine only through the documented contract
> (the registry keys, `ctx`, `data-edit-*`, the CSS variables) and otherwise
> keeps entirely to itself.* Stay inside that boundary and your part will
> compose with every other part and travel to every other site.

---

## 7. The editing protocol

You make a part editable purely by tagging its DOM. The editor (`edit-mode.js`,
documented fully in the separate *Editor — Developer Guide*) scans for these
attributes and wires the interactions. As a part author you only need the
producer side, which is this:

| Attribute (paired with `data-edit-file`) | Makes editable | Example value |
|---|---|---|
| `data-edit-text` | plain text of the node | `sections[2].title` |
| `data-edit-html` | rich text (innerHTML) | `sections[2].body` |
| `data-edit-image` | an image (opens the picker) | `sections[2].images[0].src` |
| `data-edit-attr` | a single attribute, `path\|attr` | `sections[2].url\|href` |
| `data-edit-bg` | a background image | `sections[2].style.bgImage` |
| `data-edit-list` | a reorderable/editable array | `sections[2].images` |
| `data-item-index` | (on each list child) its true array index | `0`, `1`, `2`, … |

Every value is a **data-path**: a dotted/bracketed address into the page JSON,
such as `sections[2].items[0].value`. The editor resolves the path, edits that
spot in the in-memory data, re-renders, and on Save writes the JSON back. You
never write that resolution code — you only emit the right path, and you always
build it from `ctx.path` so it is correct at any depth.

A complete, minimal editable triple looks like this (from the `button` part —
note `data-edit-attr` used to edit the link URL, and the path-pipe-attr syntax):

```js
ctx.el('a', {
  'class': 'catalog-cta', href: block.url || '#',
  'data-edit-file': ctx.file,
  'data-edit-text': ctx.path + '.label',           // edit the visible label
  text: block.label || ''
});
// ...elsewhere, an edit-only affordance to set the link target:
ctx.el('a', {
  'data-edit-file': ctx.file,
  'data-edit-attr': ctx.path + '.url|text',         // edit url; reflect into text
  text: block.url ? ('Link: ' + block.url) : 'Set link'
});
```

That is the whole producer surface. The deep editor internals — how paths
resolve, how the inspector renders your `fields[]`/`lists{}`, how Save and
Publish work — are covered in the **Editor — Developer Guide**. Cross-reference
it when you need the consumer side.

---

## 8. The two namespaces

Pagesmith uses exactly two prefixes, and the split is meaningful, not cosmetic:

```
  engine-   →  the SKELETON, EDITOR, and page STRUCTURE
              CSS:    .engine-section, .engine-sections, .engine-gallery,
                      .engine-section-title, .engine-addsection, .engine-edit
              DOM id: #engine-sections
              JS:     window.EngineStyle, window.EngineTheme, window.EngineEdit
              event:  engine-data-ready, engine-rebind
              attrs:  data-engine-block, data-engine-index, data-engine-path

  catalog-  →  the PARTS (the catalogue and your bespoke parts)
              CSS:    .catalog-carousel, .catalog-statcard, .catalog-hero, …
              JS:     window.Catalog, window.CatalogBlocks
```

The reason for two namespaces is **the engine-vs-instance / skeleton-vs-parts
boundary made visible**. Anything `engine-` is structural plumbing owned by the
skeleton and editor; it is stable and you rarely touch it. Anything `catalog-`
is a part's own surface; parts may be added, removed, or replaced freely. When
you read a stylesheet or a stack trace, the prefix instantly tells you which
layer you are in and which rules apply: `engine-` classes are shared and must not
be reused by parts; `catalog-` classes belong to one part and must not collide
with another. The boundary that §1 drew philosophically is the boundary these
two prefixes enforce in the code.

---

## 9. Theming

Colour and brand are kept *out of the code* and expressed as CSS custom
properties. The engine and every part reference variables like
`var(--site-accent)`; `theme.js` is the one place that sets them, from data.

`theme.js` reads a `theme` object — from `global.json` (the site-wide default)
and/or from a single page's own JSON (a per-page override) — and writes the
matching variables onto `<html>`. It is event-driven: `global.json`'s theme
arrives first and sets the defaults; a page's theme arrives after and overrides.

The variable map:

| `theme` JSON key | CSS variable | Role |
|---|---|---|
| `accent` | `--site-accent` | highlight colour: links, buttons, chart bars |
| `text` | `--site-ink` | main foreground / ink colour |
| `bg` | `--site-bg` | page background colour |
| `palette: [..]` | `--cat-1` … `--cat-N` | data-viz palette (chart, stat-cards, ranking, spotlight) |
| `spotlight` | `--cat-spot` | the spotlight part's gradient |
| `spotlightGlow` | `--cat-spot-glow` | the spotlight glow |
| `bgImage` / `bgImageMobile` | `--site-bg-image(-mobile)` | full-bleed page background photo |

Two details worth internalising:

- **Clearing reverts live.** When a `theme` key is *absent*, `theme.js` actively
  `removeProperty()`s the variable rather than skipping it. So if the editor
  deletes a colour, the page immediately falls back to the stylesheet default,
  not a stale value. Roles are set-or-cleared, never left dangling.

- **`--cat-*` is overridable but optional.** Parts read the palette through
  `STATPAL = palette()`, which pulls `--cat-1..8` from the live computed style,
  falling back to a built-in neon default if none are set. So the catalogue
  looks good out of the box with no theme, *and* a site can recolour all
  data-viz with one `theme.palette` array. The renderer refreshes `STATPAL` on
  every block render so palette edits preview live.

The practical upshot for a part author: **never write a hex colour without a
variable in front of it.** `var(--site-accent, #4f7cff)` — variable first,
sensible fallback second. That single habit is what makes a part both
themeable and safe with no theme.

---

## 10. The backend in one section

The backend is a small set of Azure Functions under `api/`. Its defining
property mirrors the front end: **it is generic by *shape*, so it needs zero
site configuration.** Nothing in the API names your site, your pages, or your
folders.

The genericity comes from two allow-lists that match *patterns*, not names:

- **Content files** (`_lib/content.js`): a content file is any flat lowercase
  JSON filename matching `^[a-z0-9][a-z0-9-]{0,39}\.json$`. That covers
  `global.json` and every user-created `page-<slug>.json`, with no per-site list
  and no path traversal (no slashes or dots in the slug). The same engine hosts
  any site because "what is a valid page file" is a regex, not a registry.

- **Media paths** (`_lib/media.js`): a media path is allow-listed by shape — a
  root-level file (`logo.png`) or one folder deep (`<folder>/<file>`), safe
  charsets, no `..`, no leading slash. Again a pattern, not a folder list.

The endpoints, all gated on the SWA `editor` role server-side
(`_lib/auth.js`, checking the `x-ms-client-principal` header — defence in depth
on top of route-level role config):

| Endpoint | What it does |
|---|---|
| `POST /api/save-content/{file}` | Validate the filename against the content shape, then write that one JSON file to the **draft** container. This is what the editor calls on Save. |
| `POST /api/promote` | **Publish.** Copy every content file draft → live, skipping any page marked `visibility:'dev'` in `global.json`'s nav. Then sync media draft → live (server-side blob copy, only changed/missing blobs). |
| `POST /api/revert` | The inverse of promote: copy every content file live → draft, discarding all unpublished edits. |
| `POST /api/media-upload` | Accept a base64 file, validate its path/extension, write it to the draft media container; re-encode rasters to WebP and generate `-mobile`/`-thumb` variants (and transcode large videos), unless the prefix is configured full-quality. |
| `POST /api/submit-questionnaire` | Append a visitor questionnaire submission (the footer email-signup form) to storage. |

The "draft vs published" model is just **two sets of blob containers**. You edit
against draft; Publish copies draft → live; the live site reads live containers.
The hostname switch that decides which set you are looking at lives entirely in
`site-config.js` (`prodHosts`) on the front end — the backend just exposes the
draft and live containers and copies between them. There is genuinely no
site-specific configuration in the API: a new site reuses these Functions
verbatim and only points new storage containers at them via environment
settings.

---

## 11. Standing up a new site

Because the engine is generic, a new site is *new values + new content*, not new
code. The full checklist, expanded from the README:

1. **Copy the project.** You keep the entire engine (`app/javascript/*` except
   `site-config.js`/`site-blocks.js`, `api/`, `css/layout.css`, the shells).
   Everything you change is instance.

2. **Rename the site — edit `app/javascript/site-config.js`.** This one file is
   the manifest:
   - `siteName` — used as the browser-tab title suffix.
   - `branding.wordmark` — the little label in the editor toolbar.
   - `pages` — friendly names (and optional background-image slots) for the
     editor's Pages panel. *Not required for a page to exist* — any `/p/<slug>`
     works without an entry; this just gives a nicer name.
   - `mediaTabs` — the media picker's folder tabs (one per logical asset group).
   - `socialIcons` — map each platform key to its SVG `<symbol>` id (defined in
     `header.html`), a label, and a tracking key. Delete the ones you don't use.
   - `prodHosts` — **leave empty while building.** With no live host, every host
     is treated as a draft (editor on, nothing tracked) — exactly what you want.
     Add your real domain(s) only when you go live.
   - `fullQualityMedia` — media prefixes that should skip WebP/transcode
     (e.g. a press kit). If you set this, mirror it in the API's
     `MEDIA_FULL_QUALITY_PREFIXES` app setting so server and client agree.

3. **Replace the content — edit `app/data/`.** Edit `global.json` (nav, social,
   footer text, questionnaire, site-wide `theme`) and the `page-*.json` files —
   or just build pages in the editor and Save, which writes these files for you.

4. **Restyle.** Most colour flows from `theme.accent` (and the rest of the
   `theme` object) in `global.json` via the `--site-*` variables. Tweak
   `app/css/header.css` and `footer.css` for chrome specifics;
   `app/css/layout.css` is generic and rarely touched.

5. **Add custom parts (optional) — `app/javascript/site-blocks.js`.** It ships
   empty with a worked template. Add bespoke parts here following the contract in
   [§5](#5-the-catalogue-and-the-part-contract) and the rules in
   [§6](#6-the-rules-every-part-must-follow). The generic catalogue
   (`blocks.js`) already gives you text, headings, galleries, video, carousels,
   embeds, links, quotes, buttons, charts, stat-cards, spotlights, rankings, and
   groups — so most sites need *no* custom parts at all.

6. **New pages need no code.** Any `/p/<slug>` URL renders
   `data/page-<slug>.json` through the universal shell. If you want a clean
   top-level URL (e.g. `/showcase` instead of `/p/showcase`), add one rewrite in
   `staticwebapp.config.json` — `/showcase` is the worked example.

7. **Deploy & go live.** Deploy the `./app` Static Web App linked to the `./api`
   Functions. Edit on a draft host (editor on); when ready, **Publish** to copy
   draft containers to live; finally set `prodHosts` to your real domain so the
   live host turns the editor off and tracking on.

You never edit the engine. That is the promise the whole architecture exists to
keep: a constant, battle-tested core, and a thin shell of values and content
that is all that ever differs between one site and the next.
