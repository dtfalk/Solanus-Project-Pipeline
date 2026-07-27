# Pagesmith — The Editor Internals (Developer Guide)

**Author:** David Falk

> Audience: developers who maintain or extend `app/javascript/edit-mode.js`. This
> guide explains how the editor is *built* — its boot sequence, its state model,
> the `data-edit-*` binding protocol, list reordering, the Sections staging
> editor, save/publish/revert, and the media pipeline — and then shows you how to
> extend it without breaking the things that already work.

---

## 0. Orientation: what edit-mode.js actually is

`edit-mode.js` is a single ~2250-line IIFE that turns an ordinary live web page
into an editable surface. It is part of a larger engine/instance split:

- The **engine** (skeleton + catalogue + editor) is brand-neutral. It renders
  whatever typed blocks are in a page's JSON and knows nothing about any specific
  site.
- The **instance** is a single config file (`site-config.js`, exposed as
  `window.SITE`) plus content JSON, CSS, and HTML shells.

The editor never ships any site-specific strings. Everything it needs about *this*
deployment — the prod hostnames, the wordmark, the media-picker tabs, the
full-quality folders, the friendly page names — comes through `window.SITE`. Keep
it that way: if you find yourself about to hardcode a page name or a brand, route
it through config instead.

The file opens with a **SURFACE MAP** comment. That comment is the canonical
table of contents for the file, and this guide follows the same spine:

```
boot()            fetch /.auth/me, gate on the 'editor' role, wire events,
                  expose window.EngineEdit.                 (bottom of file)
state             { models, file, data, dirtyFiles, editing, principal }.
                  fileKeyOf(node) routes a node to its model.
buildToolbar()    the .engine-bar.
bindAll()         binds the data-edit-* nodes after each (re)render.
Panels            Sections (staging), Pages, Theme, Background, section editor,
                  inspector, list panel, media picker.
save(reloadAfter) POST the dirty models; if reloadAfter, stash scroll and reload.
```

Two gotchas are called out at the top of the file, and they explain a surprising
amount of the code. Internalize them now:

1. **The Sections panel is a STAGING editor.** Add / hide / delete / reorder are
   staged inside the panel and only written to the live page on *Apply & Save*.
   The on-page section count does **not** change as you click.
2. **Many "Done" buttons call `save(true)`, which reloads the page.** After most
   edits the page reloads and re-renders from the saved model. That is *why*
   unsaved inline edits live in `state.models`, not in the DOM — the DOM is
   disposable.

---

## 1. Boot & gating

### 1.1 How the file even gets onto the page

`edit-mode.js` is not in the page's static `<script>` tags. It is **injected at
runtime by `media-base.js`, and only on non-prod hosts**:

```js
// media-base.js
var isProdHost = !!(window.SITE && window.SITE.isProd(host));
...
// Load the inline editor runtime on dev/local hosts only (never on prod).
// edit-mode.js self-gates further: it no-ops unless /.auth/me has the editor role.
if (!isProdHost) {
  var __es = document.createElement('script');
  __es.src = '/javascript/edit-mode.js';
  __es.defer = true;
  (document.head || document.documentElement).appendChild(__es);
}
```

So there are **two independent gates**, by design (defence in depth):

- **Host gate (media-base.js):** on a prod host the editor script is *never even
  downloaded*. Production users pay zero bytes for an editor they can't use.
- **Role gate (edit-mode.js):** even when the script *is* loaded, it does nothing
  unless the signed-in SWA principal carries the `editor` role.

There is also a belt-and-braces third check: the very first line inside the IIFE
re-checks the host and bails:

```js
var host = (location.hostname || '').toLowerCase();
if (window.SITE && window.SITE.isProd(host)) return; // never on the live site
```

This guards the case where someone wires the script onto a prod page by mistake.
It costs one comparison and removes a whole class of "why is the toolbar showing
on production" incidents.

### 1.2 The role gate in `boot()`

```js
function boot() {
  fetch('/.auth/me').then(function (r) { return r.ok ? r.json() : null; }).then(function (me) {
    var p = me && me.clientPrincipal;
    state.principal = p;
    var roles = (p && p.userRoles) || [];
    if (roles.indexOf('editor') === -1) {
      console.info('[engine-edit] not an editor — editor disabled.');
      return;                          // <-- everything below never runs
    }
    ...build toolbar, wire events, expose window.EngineEdit...
  });
}
```

`/.auth/me` is the Azure Static Web Apps endpoint that returns the current
`clientPrincipal` (identity provider, user id, and roles). If the user is
anonymous or lacks the `editor` role, `boot()` returns early and the page is
indistinguishable from the public site. No toolbar, no outlines, no
`window.EngineEdit`.

> **Why fetch roles client-side at all, if the server enforces them?** Because the
> client gate is purely cosmetic — it decides whether to *show* editing UI. Every
> mutating API call (`save-content`, `promote`, `revert`, `media-upload`) is
> independently enforced server-side via `requireEditor` (see §8). A user who
> forged the client check would still be rejected by the Functions. The client
> check just avoids dangling a broken toolbar in front of someone who can't use
> it.

### 1.3 The rest of boot

Once past the gate, `boot()` does the housekeeping that lets the editor survive
the constant `save → reload` cycle:

```
boot()
 ├─ restore state.editing from sessionStorage('engine-editing')
 ├─ read & clear sessionStorage('engine-scroll') into pendingScroll
 ├─ buildToolbar()              ── the .engine-bar
 ├─ maybeWelcome()              ── first-run modal (localStorage flag)
 ├─ dragHandleEl()             ── create the drag grip + shared dragover/drop
 ├─ window 'scroll' x4         ── hide floating UI while scrolling
 ├─ document 'scroll' (capture) ─ reposition the right-edge "Edit section" tab
 ├─ onData(__ENGINE_DATA__)    ── if the page already announced its data
 ├─ onData('global', __ENGINE_GLOBAL__)  ── replay global if it raced ahead
 ├─ listen 'engine-data-ready' → onData(file, data)
 ├─ listen 'engine-rebind'      → bindAll()
 ├─ window 'beforeunload'       → warn if isDirty()
 └─ window.EngineEdit = { state, save, publish, addSection, addInside, addItem, moveChild }
```

The `engine-editing` and `engine-scroll` session-storage keys are how the editor
feels continuous across a reload: when `save(true)` reloads the page, the editor
re-enters edit mode and scrolls back to where you were. The `engine-data-ready`
listener is the heart of the data flow — see §2.

---

## 2. The state model

Everything the editor knows lives in one object:

```js
var state = {
  file: null,        // primary page file key, e.g. 'page-music'  (for the toolbar pill)
  data: null,        // primary page model — a POINTER into models[file]
  models: {},        // fileKey -> data   (the page file AND 'global')
  dirtyFiles: {},    // fileKey -> true   (which models have unsaved edits)
  editing: false,    // Edit vs Preview
  principal: null    // the SWA clientPrincipal from /.auth/me
};
```

### 2.1 Why two models, and the `fileKeyOf` router

A rendered page is assembled from **two JSON files**:

- the **page file** (`index.json`, `page-music.json`, …) — the page's own blocks;
- **`global.json`** — the chrome shared by every page: nav, social links, footer,
  questionnaire, tracking, theme.

The header and footer are editable on every page, but they belong to `global`,
not to the page you happen to be on. So each editable node must declare *which
model it edits*. It does so with `data-edit-file`:

```js
function fileKeyOf(node) {
  var a = node && node.closest && node.closest('[data-edit-file]');
  return (a && a.getAttribute('data-edit-file')) || state.file;
}
function modelOf(node) { return state.models[fileKeyOf(node)]; }
```

The rule is simple: a node inside a `[data-edit-file="global"]` ancestor edits the
global model; everything else defaults to the current page file. The header/footer
markup carries `data-edit-file="global"`; `sections.js` stamps each rendered block
with `data-edit-file="<pageFile>"`.

This is the single most important routing decision in the editor. Get it wrong and
a header edit silently lands in the page model (where it does nothing, and is lost
on the next page).

```
                         ┌──────────────────────────────────────────┐
                         │                state                     │
                         │  file: 'page-music'                      │
                         │  data: ───────────────┐                  │
                         │  models: {            │                  │
                         │    'page-music': ◄────┘  (page blocks)   │
                         │    'global':     ◄──────  (nav/footer/…) │
                         │  }                                       │
                         │  dirtyFiles: { 'page-music': true }      │
                         │  editing: true   principal: {roles:[…]}  │
                         └──────────────────────────────────────────┘
                              ▲                         ▲
        node in <main>        │                         │   node under
        (a section block)     │                         │   [data-edit-file="global"]
                              │                         │   (header / footer)
                    fileKeyOf(node) = state.file   fileKeyOf(node) = "global"
```

`state.data` and `state.file` are conveniences that point at the *current page's*
model so the toolbar pill and page-level operations don't have to look it up.
`state.models` is the source of truth.

### 2.2 How models get populated: `onData`

The skeleton (`page.js`, `global.js`) loads each JSON file and announces it:

```js
window.__ENGINE_DATA__ = { file, data };               // for the page file
document.dispatchEvent(new CustomEvent('engine-data-ready', { detail: { file, data } }));
```

The editor listens and folds it into state:

```js
function onData(file, data) {
  state.models[file] = data;
  if (file !== 'global') { state.file = file; state.data = data; }
  bindAll(); refreshToolbar();
  if (pendingScroll != null) { /* restore scroll after a save→reload */ }
}
```

Note the `file !== 'global'` guard: receiving the global model must *not* clobber
`state.file`/`state.data`, which always track the page. Both models can arrive in
either order (a header-less standalone page may announce `global` after the editor
booted), which is why `boot()` replays `__ENGINE_GLOBAL__` if it raced ahead.

### 2.3 Dirty tracking

```js
function isDirty() { return Object.keys(state.dirtyFiles).length > 0; }
function markDirty(fileKey) { if (fileKey) state.dirtyFiles[fileKey] = true; refreshToolbar(); }
```

Every mutation calls `markDirty(fileKeyOf(node))`. The toolbar reflects it (the
page pill turns amber and reads "unsaved"; the Save button enables). `save()`
clears `dirtyFiles` only after the POSTs succeed.

---

## 3. The `data-edit-*` binding protocol, in depth

The editor does **not** know your blocks' data shapes. Parts (in `blocks.js`)
*tag* their DOM with `data-edit-*` attributes, and the editor wires behaviour onto
whatever it finds. This is the entire contract between a part and the editor:

| Attribute | Behaviour | Reads/writes |
|---|---|---|
| `data-edit-text="<path>"` | inline plain-text editing (`contentEditable`) | `node.textContent` ↔ path |
| `data-edit-html="<path>"` | inline rich-text editing | `node.innerHTML` ↔ path |
| `data-edit-image="<path>"` | click → media picker / section editor | path = the stored media path |
| `data-edit-attr="<path>\|<attr>"` | click → `prompt()` to edit one attribute | path ↔ `node[attr]` |
| `data-edit-bg="<blobName>"` | click → media picker, replace a CSS background | the fixed blob |
| `data-edit-list="<path>"` | drag-reorder + the list panel | path = an array |
| `data-edit-file="<key>"` | routes all of the above to a model | (see §2) |
| `data-item-index="<i>"` | on each LIST CHILD, its true array index | (see §4) |

### 3.1 `bindAll()` — idempotent, runs after every render

```js
function bindAll() {
  if (!Object.keys(state.models).length) return;
  document.querySelectorAll('[data-edit-text]').forEach(function (n) { bindText(n, false); });
  document.querySelectorAll('[data-edit-html]').forEach(function (n) { bindText(n, true); });
  document.querySelectorAll('[data-edit-image]').forEach(bindImage);
  document.querySelectorAll('[data-edit-attr]').forEach(bindAttr);
  document.querySelectorAll('[data-edit-bg]').forEach(bindBg);
  document.querySelectorAll('[data-edit-list]').forEach(bindList);
  setEditing(state.editing);
}
```

`bindAll()` is called after each (re)render — from `onData`, and from the
`engine-rebind` event a part fires after it re-renders itself. It must therefore
be **idempotent**: every binder guards with a per-node flag so re-binding the same
node is a no-op:

```js
function bindText(n, isHtml) {
  if (n.__engineBound) return; n.__engineBound = true;   // bind once
  ...
}
```

> This is why you can call `bindAll()` freely after any partial re-render without
> stacking duplicate listeners. When you add a new binder (see §9), keep the
> `__engineBound` guard — it's load-bearing.

### 3.2 Paths: tokenize → resolve

The glue between a DOM node and the model is a **path string** like
`sections[2].items[0].value`. Two tiny functions do all the addressing:

```js
function tokenize(path) {
  return String(path)
    .replace(/\[(\d+)\]/g, '.$1')   // sections[2] -> sections.2
    .split('.')
    .filter(function (s) { return s !== ''; });
}
function getByPath(obj, path) {
  var t = tokenize(path), o = obj;
  for (var i = 0; i < t.length; i++) { if (o == null) return undefined; o = o[t[i]]; }
  return o;
}
function setByPath(obj, path, val) {
  var t = tokenize(path), o = obj;
  for (var i = 0; i < t.length - 1; i++) {
    var k = t[i];
    // create the missing container, array if the NEXT token is numeric, else object
    if (o[k] == null || typeof o[k] !== 'object') o[k] = /^\d+$/.test(t[i + 1]) ? [] : {};
    o = o[k];
  }
  o[t[t.length - 1]] = val;
}
```

Worked example. The path `sections[2].items[0].value` tokenizes to
`['sections','2','items','0','value']`. Against this model:

```jsonc
{
  "sections": [
    { "type": "heading", "text": "Hi" },
    { "type": "text", "body": "…" },
    { "type": "links", "items": [ { "value": "https://a", "label": "A" } ] }
  ]
}
```

`getByPath` walks `model → sections → [2] → items → [0] → value` and returns
`"https://a"`. `setByPath` walks the same route but stops one short, then assigns
the final key. The `setByPath` auto-vivify rule (create an array when the next
token is a number, otherwise an object) means a part can bind to a path that
doesn't exist yet and the first edit will materialise the container correctly.

### 3.3 Text/HTML binding

```js
n.addEventListener('input', function () {
  var m = modelOf(n); if (!m) return;
  setByPath(m, n.getAttribute(isHtml ? 'data-edit-html' : 'data-edit-text'),
            isHtml ? n.innerHTML : n.textContent);
  markDirty(fileKeyOf(n));
});
```

Plain text writes `textContent`; rich text writes `innerHTML`. Plain-text fields
also intercept Enter to blur instead of inserting a newline. (Known sharp edge:
`data-edit-html` stores raw `contentEditable` innerHTML with no sanitisation —
browsers inject `<div>`/`<br>` and pasted markup is persisted verbatim. Fine for
trusted editors; see §10.)

### 3.4 Image, attr, bg

- **image** — clicking a media node opens its *section's* one Edit panel (the
  unified editor), or, for legacy standalone media with no `[data-engine-path]`
  ancestor, the small floating Replace/Remove toolbar. `replaceMedia` opens the
  media picker, writes the chosen path back with `setByPath`, repaints the node
  in place, and marks dirty.
- **attr** — `data-edit-attr="path|attr"` splits on `|`; a `window.prompt()`
  collects the new value, which is written to the model *and* reflected onto the
  live attribute. Used for things like a link's `href`.
- **bg** — `data-edit-bg` names a fixed CSS background blob. Replacing it uploads
  to that exact filename and reloads (the page CSS references the name, so the new
  bytes appear after a cache-bust/refresh).

---

## 4. List reordering: the `data-item-index` contract

On-page drag-to-reorder is where a subtle, expensive bug class hides, and the
editor avoids it deliberately. Read this section before you touch list rendering.

### 4.1 The bug class

A list block renders DOM children from a backing array. But **a part may render
fewer DOM children than the array has items** — it might skip an item with a blank
URL, or an invalid image. Now the array index and the DOM position have
*desynced*.

If reordering used DOM position to splice the array — "the user dropped child #3
onto child #1, so `splice` index 3 to index 1" — it would move the *wrong array
element* whenever any earlier item was filtered out. Silent data corruption: the
visible order changes, but a different, invisible item actually moved.

### 4.2 The fix: parts stamp the true index

Each list part stamps every rendered child with its **true array index** via
`data-item-index`. This is part of the documented editing protocol in
`blocks.js`:

```
//   data-item-index="<i>"   → on each LIST CHILD, its true array index
//                             (so on-page drag reorders the right item
//                             even when some items aren't rendered)
```

```js
// blocks.js — a gallery part, stamping the real array index j on each child
var img = ctx.el('img', { src: ctx.mediaUrl(src), alt: (im && im.alt) || '',
                          'data-item-index': j, ... });
```

The editor reads the stamp instead of trusting DOM position:

```js
function itemIndexOf(container, child) {
  if (child && child.getAttribute) {
    var raw = child.getAttribute('data-item-index');
    if (raw != null && raw !== '') { var n = parseInt(raw, 10); if (!isNaN(n)) return n; }
  }
  return Array.prototype.indexOf.call(container.children, child);   // fallback
}
```

The fallback to DOM position exists for simple lists that render one child per item
and don't bother stamping — but **any part that filters items MUST stamp**, or it
re-introduces the desync. That's the rule to remember when authoring a part.

### 4.3 The drag itself

The whole item is draggable from anywhere on it (no grip), except interactive
descendants (inputs, links, contenteditable) so clicking those still works:

```js
container.addEventListener('dragstart', function (e) {
  if (e.target.closest('[data-edit-list]') !== container) return;  // innermost list owns it
  var child = directChildOf(container, e.target);
  if (!child || !child.draggable) return;
  startReorder(container, itemIndexOf(container, child), e);       // <-- TRUE index
});
```

`dragover` tracks the drop target's true index; `endReorder` performs the splice
against the **model**, then saves+reloads:

```js
function endReorder() {
  var d = dragNow; dragNow = null;
  if (d && d.to != null && d.to !== d.from) {
    var fk = fileKeyOf(d.container), model = state.models[fk];
    var arr = getByPath(model, d.path);
    if (Array.isArray(arr) && d.from < arr.length) {
      arr.splice(d.to, 0, arr.splice(d.from, 1)[0]);   // move from -> to
      markDirty(fk);
      save(true);                                       // persist + reload to re-render
    }
  }
}
```

Because `from` and `to` are *true array indices*, the splice is correct even when
the DOM and the array disagree on length.

---

## 5. The Sections panel as a staging editor

`openSectionsNav()` is the page-structure editor: reorder sections, hide/show
them, delete added blocks, jump to one, edit one. The crucial design choice: **it
edits a working copy, not the live page.** Nothing you click changes the on-page
section count until you press *Apply & Save*.

### 5.1 The working list

It assembles one flat `working[]` array from two sources, in render order:

```js
var working = [];
// 1) built-in [data-section] nodes from the shell (hand-built static sections)
builtinEls.forEach(function (s) {
  working.push({ kind: 'builtin', key, label, el: s, hidden: !!hiddenBuiltin[key] });
});
// 2) catalogue blocks from state.data.sections[]
blocks.forEach(function (b) {
  working.push({ kind: 'block', block: b, label, hidden: b.visible === false, deleted: false });
});
```

Hide, Show, Delete, Undo, and drag-reorder all mutate `working[]` and re-`render()`
the panel rows. The live page is untouched.

### 5.2 `applySave` — splitting built-ins from blocks

On *Apply & Save*, the working list is split back into the two mechanisms the
renderer actually honours:

```js
function applySave() {
  var builtinOrder = working.filter(function (w) { return w.kind === 'builtin'; });
  // built-in ordering -> state.data.order (an array of [data-section] keys)
  if (builtinOrder.length) state.data.order = builtinOrder.map(function (w) { return w.key; });
  else delete state.data.order;
  // built-in visibility -> state.data.hiddenSections
  var hk = builtinOrder.filter(function (w) { return w.hidden; }).map(function (w) { return w.key; });
  if (hk.length) state.data.hiddenSections = hk; else delete state.data.hiddenSections;
  // catalogue blocks -> state.data.sections (minus deleted; visible:false for hidden)
  var newBlocks = working.filter(function (w) { return w.kind === 'block' && !w.deleted; })
    .map(function (w) {
      if (w.hidden) w.block.visible = false; else if ('visible' in w.block) delete w.block.visible;
      return w.block;
    });
  if (newBlocks.length || hadBlocks) state.data.sections = newBlocks;
  markDirty(state.file);
  overlay.remove();
  save(true);     // persist + reload; the page now re-renders from the saved model
}
```

So three different structures carry the result:

- `state.data.order` — the order of *built-in* `[data-section]` nodes (applied by
  `sections.js applyOrder`);
- `state.data.hiddenSections` — built-ins to hide (applied by `sections.js`);
- `state.data.sections` — the catalogue blocks, in order, with `visible:false` on
  hidden ones and deleted ones dropped.

### 5.3 Why the count doesn't change live, and a real limitation

Because the change only lands in the model on Apply, and the page only re-renders
on the subsequent reload, the live `#engine-sections` count stays put while you
fiddle. This is the staging contract — and it is why automated tests that assert
against the live DOM mid-edit get false failures. Assert after the save→reload, or
assert against `state.data.sections`.

There is a genuine limitation worth knowing: `sections.js` appends the
`#engine-sections` host (all catalogue blocks) to `<main>` **after** every
built-in `[data-section]`. The panel lets you drag a block above a built-in, but
`applySave` splits them into separate structures, so the *relative* order of
built-ins vs blocks is not representable. Dragging a block above a built-in has no
on-page effect. If you extend the panel, either constrain drag within each group
or persist a single unified order the renderer honours.

---

## 6. Save, and why edits live in the model

```js
function save(reloadAfter) {
  var files = Object.keys(state.dirtyFiles);
  if (!files.length) {
    if (reloadAfter) { stashScroll(); setTimeout(location.reload, 300); }   // nothing to save, still reload
    return;
  }
  toast('Saving…');
  freezeQuestionnaireNames();
  Promise.all(files.map(function (fk) {
    return fetchJson('/api/save-content/' + fk, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(state.models[fk])
    });
  })).then(function () {
    state.dirtyFiles = {}; refreshToolbar();
    toast('Saved (' + files.join(', ') + ')');
    if (reloadAfter) { stashScroll(); setTimeout(location.reload, 600); }
  }).catch(function () { toast('Couldn’t save your draft — check your connection.'); });
}
```

`save()` POSTs **only the dirty models** — each to `/api/save-content/<fileKey>`,
which writes that one JSON file into the dev (draft) container. Multiple dirty
files (page + global) go up in parallel.

### 6.1 Why edits live in `state.models`, not the DOM

This is the central architectural fact of the editor. After almost any structural
edit the page is reloaded and **re-rendered from the saved JSON**. The DOM you
were just editing is thrown away. Therefore:

- Every inline edit writes through to `state.models` *immediately* (on `input`,
  on pick, on prompt). The DOM change is just a live preview.
- A reorder/add/delete mutates the model array, then `save(true)`.
- When the model is the source of truth, a re-render can never lose an edit that
  was already written to it.

If you ever add a feature that mutates only the DOM and forgets the model, your
change will vanish on the next reload. Don't.

### 6.2 The reload gotcha (especially for tests)

`save(true)` reloads after ~300–600 ms, stashing `scrollY` in
`sessionStorage('engine-scroll')` so `onData` can restore it. The reload is what
makes the page re-render from the saved model.

For tests this is a classic trap: an assertion that fires immediately after an
action that triggers `save(true)` reads the *pre-reload* DOM and sees stale
content. Wait for the reload to settle before asserting (the heavy pages need a
generous wait), or assert against `window.EngineEdit.state.models`.

---

## 7. Media picker + upload pipeline

### 7.1 The picker

`openMediaPicker(prefix, onChoose, opts)` shows a tabbed browser. The tabs come
straight from config:

```js
var MEDIA_TABS = (window.SITE && window.SITE.mediaTabs) || [];
```

Each tab is one media folder. Selecting a tab calls `/api/media?prefix=…` and
lays the blobs out as clickable tiles. Picking an existing tile calls
`onChoose(path)` (the caller writes it back to the model). Uploading routes
through `doUpload`.

`opts.replaceName` switches the picker into "replace a fixed blob" mode (used for
CSS backgrounds): instead of returning a path, it copies the chosen/uploaded bytes
to a *known* filename via `copyExistingTo` and fires `opts.onReplaced`.

### 7.2 Client-side optimisation, then upload

Before upload, `prepareMedia` optimises in the browser:

- **Images** (`jpg/png/webp`) are downscaled to a max edge of 2560 px and
  re-encoded to WebP at quality 0.82 via a canvas (`toWebpBlob`). If the WebP
  isn't smaller, it keeps the original.
- **Videos** are size-capped (50 MB) — big ones are steered to YouTube.
- **Everything** is bounded by a ~90 MB transport ceiling (the Function request
  limit, with headroom).
- **Full-quality folders are exempt.** If the target prefix matches
  `SITE.isFullQuality(prefix)`, the file is passed through untouched (up to the
  transport ceiling). This is for press kits whose recipients need originals.

The optimised blob is base64-encoded and POSTed as JSON to `/api/media-upload`
(base64-JSON keeps the Function dependency-free — no multipart parser).

### 7.3 The server side: WebP + responsive variants

`/api/media-upload` re-runs the same policy as a backstop (defence in depth) and
generates responsive variants:

```js
// api/media-upload — the full-quality rule MUST mirror the client's SITE.isFullQuality
const FULL_QUALITY_PREFIXES = (process.env.MEDIA_FULL_QUALITY_PREFIXES || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const isFull = isFullQuality(blobName);
if (!isFull && WEBP_FROM.includes(ext)) { /* toWebp(buffer) */ }
else if (!isFull && VIDEO_EXT.includes(ext) && big) { /* transcodeToWebMp4 */ }
// then generateResponsiveVariants(...) for non-full-quality images
```

> **The full-quality bypass must match on both sides.** The client reads
> `SITE.fullQualityMedia` (via `SITE.isFullQuality`); the server reads the
> `MEDIA_FULL_QUALITY_PREFIXES` app setting. If they drift, you get the worst of
> both worlds — the client passes a 30 MB original through, and the server
> WebP-crushes it anyway (or vice versa). Keep them in sync; changing one without
> the other is a bug.

```
   browser                                         Functions (api)
 ┌──────────────────────────┐                    ┌────────────────────────────┐
 │ openMediaPicker          │                    │ media-upload               │
 │  ▼ choose / upload       │   POST JSON        │  requireEditor             │
 │ prepareMedia             │  (base64 body)     │  isFullQuality? ──┐        │
 │  • downscale 2560px      │ ─────────────────► │  no → toWebp /     │ yes    │
 │  • WebP q0.82            │                    │       transcode    │  ▼     │
 │  • caps (50MB / 90MB)    │                    │     + variants     │ store  │
 │  • full-quality bypass ──┼── (SITE.isFullQuality)  (mirror rule) ◄─┘ original│
 │ base64 → POST            │ ◄───────────────── │  → blob in media-dev       │
 └──────────────────────────┘   { path, url }    └────────────────────────────┘
```

---

## 8. Publish / Revert and the backend

### 8.1 The draft-vs-published model

There are two sets of blob containers:

- **draft (dev):** `content-dev`, `media-dev` — what the editor reads and writes.
- **published (prod):** `content-prod`, `media-prod` — what the live site serves.

The editor only ever writes to **draft**. Publishing and reverting are
server-side copies between the two container sets.

### 8.2 Publish → `/api/promote`

`publish()` warns if there are unsaved edits (they won't be included — Publish
ships the last *saved* draft, not in-memory edits), confirms, then POSTs
`/api/promote`:

```js
function publish() {
  if (isDirty() && !confirm('You have unsaved edits that won\'t be included. …')) return;
  if (!confirm('Publish your draft to the LIVE website? …')) return;
  fetchJson('/api/promote', { method: 'POST' }).then(...).catch(...);
}
```

`/api/promote` copies every content JSON **dev → prod**, *skipping pages marked
`visibility:'dev'`* in `global.json` nav (Dev-only pages never go live). It then
syncs media dev → prod (server-side blob copy from the public-read draft URL — no
bytes flow through the Function), only when a prod media container is configured.

### 8.3 Revert → `/api/revert`

`/api/revert` is the inverse: copy every content file **prod → dev**, restoring
the draft to exactly the last published version. `revert()` confirms, POSTs, clears
`dirtyFiles`, and reloads.

### 8.4 Auth behind the SWA gate

Every mutating endpoint calls `requireEditor`, which reads the
`x-ms-client-principal` header that Static Web Apps injects (base64 JSON with the
user's roles) and requires `editor`:

```js
// api/_lib/auth.js
function requireEditor(context, req) {
  if (isEditor(req)) return true;     // checks x-ms-client-principal roles
  context.res = { status: 403, body: { error: 'Not authorized — editor role required' } };
  return false;
}
// EDITOR_AUTH_DISABLED=true bypasses this locally (never set in cloud).
```

The browser never holds a storage key. The Functions reach blob storage via
scoped per-container SAS tokens in app settings.

```
  browser                 Static Web Apps               Azure Functions            Blob Storage
 ┌─────────┐   POST       ┌──────────────────┐  injects   ┌────────────────┐  SAS  ┌──────────────┐
 │ publish │ ───────────► │ SWA gate         │ ─────────► │ promote        │ ────► │ content-dev  │
 │ revert  │  (cookie)    │ + x-ms-client-   │  header    │  requireEditor │ copy  │   → -prod    │
 │ save    │              │   principal      │            │  (role: editor)│       │ media-dev    │
 └─────────┘ ◄─────────── └──────────────────┘ ◄───────── └────────────────┘ ◄──── │   → -prod    │
                                                                                    └──────────────┘
```

---

## 9. How to extend the editor

### 9.1 Make a part editable

You write **zero editor code**. In your part's render (`blocks.js` /
`site-blocks.js`), tag the DOM:

```js
// inside a part's render(block, ctx)
ctx.el('h2', { 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.title', text: block.title });
ctx.el('img', { 'data-edit-file': ctx.file, 'data-edit-image': ctx.path + '.src', src: ctx.mediaUrl(block.src) });
var grid = ctx.el('div', { 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.items',
                           'data-edit-list-label': 'links' });
block.items.forEach(function (it, j) {
  grid.appendChild(ctx.el('a', { 'data-item-index': j, href: it.url, text: it.label }));  // stamp j!
});
```

`bindAll()` discovers and wires these on the next render. Remember: stamp
`data-item-index` on filtered lists (§4), and set `data-edit-file` (§2).

### 9.2 Add a new inspector field type

Inspector and list-panel fields are dispatched by a `type` string
(`text | textarea | url | image | bool | select | csv | color | embed`). To add a
type, extend the field-renderer switch (in `inspectorField` / the list-panel
field builder) with a new `case`. Each case builds an input, reads the current
value via `getByPath`, and on change calls the supplied `onChange(value)` callback
— it must not write the model directly; the callback owns `setByPath` +
`markDirty` + the live re-render. Follow the existing `color` case as a template;
it was the most recently added type and shows the full pattern (build control,
seed from current value, call back on change).

### 9.3 Add a panel to the More menu

Toolbar buttons are created in `buildToolbar()`. Secondary tools live in the
`#engine-more` dropdown:

```js
var btnThing = el('button', { text: 'My Thing', title: '…', on: { click: openThingPanel } });
[btnTheme, btnBg, /* … */, btnThing].forEach(function (x) { moreMenu.appendChild(x); });
```

Write `openThingPanel()` following the slide-in panel convention: build a
`.engine-panel` with an `.engine-phead` (title + `helpBtn('thing')` + Close), an
`.engine-pbody`, then `panelize()` it into an `.engine-overlay`. Persist with
`markDirty(state.file)` + `save()`; if your panel's Done should re-render the page,
call `save(true)`.

### 9.4 The per-tool HELP registry

Every panel shows a round `?` next to its title via `helpBtn(key)`, which opens
`openHelpTopic(key)` against the `HELP` registry:

```js
var HELP = {
  thing: {
    title: 'My Thing',
    intro: 'One sentence on what this does.',
    body: [
      ['A subheading', ['A bullet.', 'Another bullet — note the empty-field behaviour.']]
    ]
  },
  /* sections, pages, theme, style, editsection, lists, questionnaire,
     tracking, background, media, addsection … */
};
```

When you add a panel, add a matching `HELP` entry and wire `helpBtn('thing')` into
its header. The existing entries are deliberately plain-language and call out the
"what if I leave this empty?" edge cases — match that tone.

---

## 10. Known sharp edges (respect these)

These are real, current behaviours — not bugs to fix in passing, but things to
keep in mind so you don't trip over them or "fix" them into a regression.

1. **Unsaved page edits vs Pages-panel navigation.** Inline text/media edits write
   to `state.models[pageFile]` and set `dirtyFiles`, but are only POSTed by
   `save()`. Several navigation flows (Pages panel "Open", create-page, "Add to
   menu") run `saveGlobal` (which persists *only* `global`) then `location.href`
   away — the dirty *page* model is never saved. The `beforeunload` guard fires
   the browser's generic prompt, but it's easy to dismiss. If you add a
   navigation flow, save the page model first (or warn).

2. **The staging / live split.** The Sections panel stages changes; the page only
   reflects them after Apply & Save → reload. Don't assert against the live count
   mid-edit, and don't add a "live as you click" Sections feature without
   reconciling it with `applySave`'s built-in/block split.

3. **The save → reload timing.** `save(true)` reloads ~300–600 ms later. Anything
   that reads the DOM right after triggering it sees stale content. Wait for the
   reload (or read `state.models`).

4. **`data-edit-html` stores raw contentEditable HTML.** No sanitisation;
   browser-injected `<div>`/`<br>` and pasted markup are persisted. Acceptable for
   trusted editors; be aware if you ever widen who can edit.

5. **Built-in vs block ordering is not unified.** See §5.3 — dragging a block above
   a built-in in the Sections panel has no on-page effect.

6. **Background replace can show a cached old image.** Fixed-blob backgrounds reuse
   the same filename; `applyLiveBackground` cache-busts the element currently
   showing it, but if the bg comes from a stylesheet rule (not a computed match) it
   can't repaint and tells the user to hard-refresh. Expected, not a bug.

7. **The full-quality rule lives in two places.** Client `SITE.fullQualityMedia`
   and server `MEDIA_FULL_QUALITY_PREFIXES` must agree (§7.3).

---

*End of guide.*
```
