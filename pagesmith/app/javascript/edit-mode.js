/* edit-mode.js — the engine's inline content editor.
 *
 * Loaded only on dev/local hosts (injected by media-base.js). Self-gates: does
 * nothing unless the logged-in SWA user has the "editor" role. Turns the live
 * site into an editable surface:
 *
 *   - [data-edit-text]   inline plain-text editing  (writes to a JSON path)
 *   - [data-edit-html]   inline rich-text editing
 *   - [data-edit-image]  click → media picker (choose existing / upload new)
 *   - [data-edit-attr]   click → edit an attribute value (e.g. a link URL)
 *   - [data-edit-bg]     click → replace a CSS background image in place
 *   - [data-edit-list]   "Edit list" panel → add / remove / reorder items
 *
 * Each page renders from exactly one JSON file and announces it via:
 *   window.__ENGINE_DATA__ = { file, data }; + event 'engine-data-ready'.
 * Edits mutate that working copy; Save POSTs it to /api/save-content/<file>;
 * Publish POSTs /api/promote (dev → prod).
 *
 * ---------------------------------------------------------------------------
 * SURFACE MAP (so you can find your way around this large file)
 * ---------------------------------------------------------------------------
 *   boot()                 fetch /.auth/me, gate on the 'editor' role, wire
 *                          events, expose window.EngineEdit. (bottom of file)
 *   state                  { models:{<file>:data, global:data}, file, data,
 *                            dirtyFiles, editing, principal }. fileKeyOf(node)
 *                          routes a node to its model via [data-edit-file].
 *   buildToolbar()         the .engine-bar: Edit/Preview · page pill · Sections ·
 *                          Pages · Save · Publish · More(Theme/Background/
 *                          Questionnaire/Tracking/Responses/Revert/Help/Log out).
 *   bindAll()              binds the data-edit-* nodes after each (re)render.
 *   Panels                 openSectionsNav (the Sections STAGING editor),
 *                          openPagesPanel, openThemePanel, editBackground,
 *                          openSectionEditor / openInspector (per-block fields),
 *                          openListPanel (array editing), openMediaPicker.
 *   save(reloadAfter)      POST the dirty models; if reloadAfter, stash scroll
 *                          and location.reload() (~300ms later).
 *
 * ---------------------------------------------------------------------------
 * TWO GOTCHAS WORTH KNOWING (they explain a lot of the code):
 *   1. The Sections panel is a STAGING editor: add / hide / delete / reorder are
 *      staged in the panel and only applied to the live page on "Apply & Save".
 *      The on-page section count does NOT change as you click.
 *   2. Many panels' "Done" buttons call save(true), which RELOADS the page. So
 *      after most edits the page reloads and re-renders from the saved model —
 *      which is why unsaved inline edits live in state.models, not the DOM.
 * ---------------------------------------------------------------------------
 */
(function () {
  'use strict';

  var host = (location.hostname || '').toLowerCase();
  if (window.SITE && window.SITE.isProd(host)) return; // never on the live site

  var state = {
    file: null,          // primary page file (for the toolbar pill)
    data: null,          // primary page model (pointer into models)
    models: {},          // fileKey -> data  (the page file + 'global')
    dirtyFiles: {},      // fileKey -> true
    editing: false,
    principal: null
  };
  function isDirty() { return Object.keys(state.dirtyFiles).length > 0; }
  // Which JSON file does this node edit? Header/footer nodes carry
  // data-edit-file="global"; everything else defaults to the page file.
  function fileKeyOf(node) {
    var a = node && node.closest && node.closest('[data-edit-file]');
    return (a && a.getAttribute('data-edit-file')) || state.file;
  }
  function modelOf(node) { return state.models[fileKeyOf(node)]; }
  var pendingScroll = null;
  var mtools = null, mtoolsTimer = null;
  var listFloat = null, listFloatTimer = null;
  var styleFloat = null, styleFloatTimer = null;
  var dragHandle = null, dragHandleTimer = null, dragNow = null;

  // Per-page CSS background image comes from the site config (window.SITE.pageBg);
  // uploading a replacement also regenerates the -mobile.webp variant the mobile
  // CSS already points at. Tabs for the media picker (one folder each) likewise.
  var MEDIA_TABS = (window.SITE && window.SITE.mediaTabs) || [];

  // ---------------------------------------------------------------- utilities
  function tokenize(path) {
    return String(path).replace(/\[(\d+)\]/g, '.$1').split('.').filter(function (s) { return s !== ''; });
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
      if (o[k] == null || typeof o[k] !== 'object') o[k] = /^\d+$/.test(t[i + 1]) ? [] : {};
      o = o[k];
    }
    o[t[t.length - 1]] = val;
  }
  function mediaUrl(p) { return (window.mediaUrl ? window.mediaUrl(p) : p); }
  function prefixOf(logicalPath) {
    var m = String(logicalPath || '').match(/\/media-content\/([^/]+)\//);
    return m ? m[1] + '/' : '';
  }
  function el(tag, attrs, kids) {
    var n = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (k === 'text') n.textContent = attrs[k];
      else if (k === 'html') n.innerHTML = attrs[k];
      else if (k === 'on') Object.keys(attrs.on).forEach(function (ev) { n.addEventListener(ev, attrs.on[ev]); });
      else if (attrs[k] != null) n.setAttribute(k, attrs[k]);
    });
    [].concat(kids || []).forEach(function (c) { if (c) n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return n;
  }

  function markDirty(fileKey) { if (fileKey) state.dirtyFiles[fileKey] = true; refreshToolbar(); }

  // ----------------------------------------------------------------- list schemas
  // Field types: text | textarea | url | image | bool | stringItem (whole item is a string)
  // Edit schema for the signup questionnaire's questions — a generic feature (any
  // site can have a fan signup survey). Section/list schemas for everything else
  // come from the zoo (a block's `lists`), so nothing site-specific lives here.
  // (The old per-page schemas were pre-harvest: every page is now sections[].)
  var SCHEMAS = {
    'global:questionnaire.fields': { label: 'Question', fields: [
      { key: 'label', type: 'text', label: 'Question' },
      { key: 'type', type: 'select', label: 'Answer type', options: ['text', 'select'] },
      { key: 'options', type: 'csv', label: 'Dropdown choices (comma-separated; for "select")' },
      { key: 'placeholder', type: 'text', label: 'Placeholder (for "text")' }
    ]}
  };
  // Generic schemas for custom page sections (apply on any page).
  var SECTION_SCHEMA = { label: 'Section', fields: [
    { key: 'type', type: 'select', label: 'Type', options: ['text', 'gallery'] },
    { key: 'title', type: 'text', label: 'Title' },
    { key: 'body', type: 'textarea', label: 'Body (text sections)' }
  ] };
  var GALLERY_IMG_SCHEMA = { label: 'Image', fields: [
    { key: 'src', type: 'image', label: 'Image' },
    { key: 'alt', type: 'text', label: 'Alt text' }
  ] };
  function schemaFor(listPath, fileKey) {
    if (/(^|\.)sections$/.test(listPath)) return SECTION_SCHEMA;
    if (/^sections\[\d+\]\.images$/.test(listPath)) return GALLERY_IMG_SCHEMA;
    // Generic: a zoo block's list field (e.g. carousel.slides, links.links),
    // including nested group children — resolved from the parent block's type.
    if (window.Catalog && window.Catalog.listSchema) {
      var __pm = listPath.match(/^(.*)\.([a-zA-Z_]\w*)$/);
      if (__pm) {
        var __parent = getByPath(state.models[fileKey || state.file], __pm[1]);
        if (__parent && __parent.type) { var __sc = window.Catalog.listSchema(__parent.type, __pm[2]); if (__sc) return __sc; }
      }
    }
    var key = (fileKey || state.file) + ':' + listPath;
    if (SCHEMAS[key]) return SCHEMAS[key];
    // generic fallback: infer text fields from first item's string keys
    var arr = getByPath(state.models[fileKey || state.file], listPath);
    if (Array.isArray(arr) && arr.length && typeof arr[0] === 'object') {
      return { label: 'Item', fields: Object.keys(arr[0]).map(function (k) {
        return { key: k, type: /url/i.test(k) ? 'url' : (typeof arr[0][k] === 'boolean' ? 'bool' : 'text'), label: k };
      })};
    }
    if (Array.isArray(arr)) return { stringArray: true, label: 'Item' };
    return { fields: [] };
  }

  // ------------------------------------------------------------------- styles
  function injectStyles() {
    if (document.getElementById('engine-edit-styles')) return;
    var fonts = document.createElement('link');
    fonts.rel = 'stylesheet';
    fonts.href = 'https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,500;0,9..144,600;1,9..144,500&family=Bricolage+Grotesque:opsz,wght@12..96,400;12..96,600;12..96,700&family=DM+Mono:wght@500&display=swap';
    document.head.appendChild(fonts);
    var grain = "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='140' height='140'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.05'/%3E%3C/svg%3E\")";
    var css = [
      ':root{--engine-ink:#18150f;--engine-cream:#f4ead4;--engine-dim:rgba(244,234,212,.55);--engine-amber:#e3a23c;--engine-amber-hi:#f6bd54;--engine-line:rgba(244,234,212,.14);--engine-ui:"Bricolage Grotesque",ui-sans-serif,system-ui,sans-serif;--engine-display:"Fraunces",Georgia,serif;--engine-mono:"DM Mono",ui-monospace,monospace}',
      '@keyframes engine-drop{from{transform:translateY(-100%)}to{transform:translateY(0)}}',
      '@keyframes engine-rise{from{opacity:0;transform:translateY(14px) scale(.98)}to{opacity:1;transform:none}}',
      '@keyframes engine-slidein{from{transform:translateX(100%)}to{transform:translateX(0)}}',
      '@keyframes engine-pulse{0%,100%{box-shadow:0 0 0 0 rgba(227,162,60,.55)}50%{box-shadow:0 0 0 5px rgba(227,162,60,0)}}',
      '@keyframes engine-editpulse{0%,100%{box-shadow:0 0 0 0 rgba(227,162,60,0)}45%{box-shadow:0 0 0 3px rgba(227,162,60,.55)}}',
      'body.engine-pulse-edit [data-edit-text],body.engine-pulse-edit [data-edit-html],body.engine-pulse-edit [data-edit-image],body.engine-pulse-edit [data-edit-list]{animation:engine-editpulse 1.3s ease-in-out 2;border-radius:6px}',
      'body.engine-has-bar{padding-top:var(--engine-barh,54px)!important}',
      '#engine-bar{position:fixed;top:0;left:0;right:0;min-height:54px;z-index:2147483000;display:flex;flex-wrap:wrap;align-items:center;gap:10px;row-gap:6px;padding:7px 12px;',
      'color:var(--engine-cream);font-family:var(--engine-ui);font-size:13px;letter-spacing:.2px;',
      'background:linear-gradient(180deg,#221d14,#16130d),' + grain + ';background-blend-mode:overlay;',
      'border-bottom:1px solid var(--engine-line);box-shadow:0 8px 26px -10px rgba(0,0,0,.75),inset 0 2px 0 rgba(246,189,84,.12),inset 0 -1px 0 rgba(0,0,0,.45);animation:engine-drop .5s cubic-bezier(.2,.8,.2,1)}',
      '#engine-bar::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--engine-amber),#c2582f 55%,var(--engine-amber-hi))}',
      '#engine-bar .sp{flex:1}',
      '.engine-brand{display:flex;align-items:baseline;gap:8px;padding-right:4px}',
      '.engine-rec{align-self:center;width:9px;height:9px;border-radius:50%;background:#6b5f47;border:1px solid rgba(244,234,212,.25);transition:.25s}',
      'body.engine-edit .engine-rec{background:var(--engine-amber);border-color:var(--engine-amber-hi);animation:engine-pulse 1.6s infinite}',
      '.engine-wordmark{font-family:var(--engine-display);font-weight:600;font-size:21px;letter-spacing:.5px;line-height:1}',
      '.engine-sub{font-family:var(--engine-mono);font-size:10px;text-transform:uppercase;letter-spacing:2.5px;color:var(--engine-amber)}',
      '#engine-bar .pill{font-family:var(--engine-mono);font-size:10.5px;text-transform:uppercase;letter-spacing:1.4px;color:var(--engine-dim);padding:4px 10px;border:1px solid var(--engine-line);border-radius:6px;background:rgba(0,0,0,.25)}',
      '#engine-bar .pill.engine-pill-dirty{color:var(--engine-amber);border-color:rgba(246,189,84,.5);background:rgba(246,189,84,.12)}',
      '#engine-bar button{font-family:var(--engine-ui);font-weight:600;font-size:12.5px;border:1px solid var(--engine-line);border-radius:9px;padding:8px 13px;cursor:pointer;color:var(--engine-cream);background:rgba(244,234,212,.06);transition:transform .08s,background .15s,border-color .15s}',
      '#engine-bar button:hover{background:rgba(244,234,212,.12);border-color:rgba(244,234,212,.28)}',
      '#engine-bar button:active{transform:translateY(1px)}',
      '#engine-bar button:focus-visible{outline:2px solid var(--engine-amber);outline-offset:2px}',
      '#engine-bar button.primary{background:rgba(244,234,212,.10);border-color:rgba(244,234,212,.3)}',
      '#engine-bar button.go{background:linear-gradient(180deg,var(--engine-amber-hi),var(--engine-amber));color:#241a08;border-color:#a9742a;font-weight:700;box-shadow:0 2px 0 #8f5f1f}',
      '#engine-bar button.go:hover{filter:brightness(1.06)}',
      '#engine-bar button.go:active{box-shadow:0 1px 0 #8f5f1f}',
      '#engine-bar button[disabled]{opacity:.4;cursor:default;transform:none}',
      '#engine-bar .toggle{display:flex;border:1px solid var(--engine-line);border-radius:9px;overflow:hidden;background:rgba(0,0,0,.28)}',
      '#engine-bar .toggle button{border:0;border-radius:0;background:transparent;color:var(--engine-dim)}',
      '#engine-bar .toggle button.on{background:linear-gradient(180deg,#2a2418,#211c13);color:var(--engine-amber);box-shadow:inset 0 1px 0 rgba(246,189,84,.18)}',
      '#engine-more{position:fixed;top:50px;right:12px;z-index:2147483001;display:none;flex-direction:column;gap:4px;padding:8px;min-width:172px;background:linear-gradient(180deg,#221d14,#16130d);border:1px solid var(--engine-line);border-radius:10px;box-shadow:0 16px 40px -12px rgba(0,0,0,.7)}',
      '#engine-more.show{display:flex}',
      '#engine-more button{width:100%;justify-content:flex-start;text-align:left}',
      // Push the site\'s own fixed header/nav below the editor bar so it stays usable.
      'body.engine-has-bar .site-header{top:var(--engine-barh,54px)!important}',
      'body.engine-has-bar .menu-toggle{top:calc(0.5rem + var(--engine-barh,54px))!important}',
      'body.engine-has-bar .mobile-menu-overlay{top:var(--engine-barh,54px)!important}',
      'body.engine-edit [data-edit-text],body.engine-edit [data-edit-html]{outline:2px dashed rgba(227,162,60,.7);outline-offset:3px;border-radius:2px;cursor:text;transition:.15s}',
      'body.engine-edit [data-edit-text]:hover,body.engine-edit [data-edit-html]:hover{outline-color:var(--engine-amber);background:rgba(227,162,60,.10);box-shadow:0 0 0 6px rgba(227,162,60,.06)}',
      'body.engine-edit [data-edit-text]:focus,body.engine-edit [data-edit-html]:focus{outline:2px solid var(--engine-amber);background:rgba(227,162,60,.08)}',
      'body.engine-edit [data-edit-image],body.engine-edit [data-edit-bg]{cursor:pointer;position:relative;outline:3px solid rgba(227,162,60,.65);outline-offset:3px;transition:.15s}',
      'body.engine-edit [data-edit-image]:hover,body.engine-edit [data-edit-bg]:hover{outline-color:var(--engine-amber);box-shadow:0 0 0 6px rgba(227,162,60,.1)}',
      'body.engine-edit [data-edit-attr]{cursor:pointer;outline:2px dashed rgba(194,88,47,.85);outline-offset:2px}',
      'body.engine-edit [data-edit-list]{outline:3px dashed rgba(227,162,60,.55);outline-offset:7px;border-radius:3px}',
      // Whole list items are drag-to-reorder from anywhere → show a grab cursor.
      'body.engine-edit [data-edit-list] > *{cursor:grab}',
      'body.engine-edit [data-edit-list] iframe{pointer-events:none}',
      'body.engine-reordering [data-edit-list] > *{cursor:grabbing}',
      // Header outlines stay THIN (only the body editable outlines were thickened).
      'body.engine-edit .site-header [data-edit-text],body.engine-edit .site-header [data-edit-html],body.engine-edit .site-header [data-edit-attr]{outline-width:1px}',
      'body.engine-edit .site-header [data-edit-list]{outline-width:1px;outline-offset:4px}',
      '.engine-listbtn{position:relative;z-index:5;display:inline-flex;align-items:center;gap:7px;margin:10px auto;font-family:var(--engine-mono);font-size:11px;text-transform:uppercase;letter-spacing:1.4px;',
      'background:var(--engine-ink);color:var(--engine-amber);border:1px solid var(--engine-amber);border-radius:8px;padding:8px 14px;cursor:pointer;transition:.15s}',
      '.engine-listbtn:hover{background:var(--engine-amber);color:var(--engine-ink)}',
      'body:not(.engine-edit) .engine-listbtn{display:none}',
      '.engine-overlay{position:fixed;inset:0;background:rgba(20,16,9,.62);backdrop-filter:blur(3px);z-index:2147483200;display:flex}',
      '.engine-modal{margin:auto;background:#fbf4e6;color:#2a2317;border:1px solid #e6d9bd;border-radius:16px;max-width:880px;width:92%;max-height:86vh;overflow:auto;padding:22px 24px;box-shadow:0 30px 80px -20px rgba(0,0,0,.6);animation:engine-rise .35s cubic-bezier(.2,.8,.2,1)}',
      '.engine-panel{position:absolute;top:0;right:0;bottom:0;background:#fbf4e6;color:#2a2317;width:min(520px,96%);display:flex;flex-direction:column;overflow:hidden;box-sizing:border-box;box-shadow:-12px 0 50px -12px rgba(0,0,0,.5);animation:engine-slidein .3s cubic-bezier(.2,.8,.2,1)}',
      '.engine-panel h3,.engine-modal h3{margin:0 0 4px;font-family:var(--engine-display);font-weight:600;font-size:22px;letter-spacing:.3px}',
      '.engine-modal h3::after,.engine-panel h3::after{content:"";display:block;width:42px;height:3px;background:var(--engine-amber);border-radius:3px;margin:8px 0 16px}',
      '.engine-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(118px,1fr));gap:12px}',
      '.engine-tile{border:1px solid #e6d9bd;border-radius:10px;padding:7px;cursor:pointer;text-align:center;font:500 10.5px/1.3 var(--engine-mono);color:#6a5c40;word-break:break-all;background:#fff;transition:.15s}',
      '.engine-tile:hover{border-color:var(--engine-amber);box-shadow:0 6px 16px -6px rgba(227,162,60,.6);transform:translateY(-2px)}',
      '.engine-tile img{width:100%;height:84px;object-fit:contain;border-radius:6px;display:block;margin-bottom:6px;background:repeating-conic-gradient(#cdbf9e 0% 25%,#f3ead4 0% 50%) 0/14px 14px}',
      '.engine-item .row img{background:repeating-conic-gradient(#cdbf9e 0% 25%,#f3ead4 0% 50%) 0/12px 12px;object-fit:contain;padding:2px}',
      '.engine-item{border:1px solid #e6d9bd;border-radius:12px;padding:14px;margin-bottom:12px;background:#fff}',
      '.engine-item .row{display:flex;gap:8px;align-items:center;margin-bottom:6px}',
      '.engine-item b{font-family:var(--engine-display);font-weight:600;font-size:15px}',
      '.engine-item label{font:600 10.5px/1 var(--engine-ui);text-transform:uppercase;letter-spacing:.8px;color:#9a8a66;display:block;margin:9px 0 3px}',
      '.engine-item input[type=text],.engine-item input[type=url],.engine-item textarea{width:100%;box-sizing:border-box;padding:9px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317;transition:.15s}',
      '.engine-item select{width:100%;box-sizing:border-box;padding:9px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317}',
      '.engine-item input:focus,.engine-item textarea:focus{outline:0;border-color:var(--engine-amber);box-shadow:0 0 0 3px rgba(227,162,60,.22)}',
      '.engine-item textarea{min-height:70px;resize:vertical;line-height:1.45}',
      '.engine-btnrow{display:flex;gap:10px;justify-content:flex-end;margin-top:16px;position:sticky;bottom:-22px;background:linear-gradient(180deg,rgba(251,244,230,0),#fbf4e6 22%);padding:14px 0 2px}',
      '.engine-btnrow button{border:1px solid #a9742a;border-radius:9px;padding:10px 18px;font:700 13px var(--engine-ui);cursor:pointer;color:#241a08;background:linear-gradient(180deg,var(--engine-amber-hi),var(--engine-amber));box-shadow:0 2px 0 #8f5f1f;transition:.1s}',
      '.engine-btnrow button:active{transform:translateY(1px);box-shadow:0 1px 0 #8f5f1f}',
      '.engine-btnrow button.ghost{background:#efe7d4;color:#5d5036;border-color:#ddcfb1;box-shadow:none;font-weight:600}',
      '.engine-mini{border:1px solid #ddcfb1;background:#fff;color:#5d5036;border-radius:7px;padding:5px 10px;cursor:pointer;font:600 12px var(--engine-ui);transition:.12s}',
      '.engine-mini:hover{border-color:var(--engine-amber);color:#2a2317}',
      '.engine-phead{flex:0 0 auto;display:flex;align-items:center;gap:8px;background:#fbf4e6;padding:18px 22px 12px;border-bottom:1px solid #e6d9bd}',
      '.engine-pbody{flex:1 1 auto;min-height:0;overflow-y:auto;overscroll-behavior:contain;padding:14px 22px 22px}',
      '.engine-phead h3{margin:0}.engine-phead h3::after{display:none}',
      '.engine-phead button{border:1px solid #a9742a;border-radius:8px;padding:8px 14px;font:700 12.5px var(--engine-ui);cursor:pointer;color:#241a08;background:linear-gradient(180deg,var(--engine-amber-hi),var(--engine-amber))}',
      '.engine-phead button.ghost{background:#efe7d4;color:#5d5036;border-color:#ddcfb1;font-weight:600}',
      // Round "?" help button shown beside a tool\'s title. Equal specificity to
      // ".engine-phead button" + later in source, so it overrides the pill styling.
      'button.engine-help{flex:0 0 auto;width:24px;height:24px;min-width:0;padding:0;border-radius:50%;border:1.5px solid var(--engine-amber);background:#fff;color:#a9742a;font:800 13px/1 var(--engine-ui);cursor:help;display:inline-flex;align-items:center;justify-content:center;box-shadow:none}',
      'button.engine-help:hover{background:var(--engine-amber);color:#241a08}',
      '.engine-mhead{display:flex;align-items:center;gap:8px;margin-bottom:4px}.engine-mhead h3{margin:0}.engine-mhead h3::after{display:none}',
      '#engine-mtools{position:fixed;z-index:2147483100;display:none;gap:6px;padding:5px;background:var(--engine-ink);border:1px solid var(--engine-amber);border-radius:9px;box-shadow:0 10px 26px -8px rgba(0,0,0,.7)}',
      '#engine-mtools.show{display:flex}',
      '#engine-mtools button{font:600 13.5px var(--engine-ui);border:0;border-radius:7px;padding:10px 15px;cursor:pointer;color:#241a08;background:var(--engine-amber)}',
      '#engine-mtools button.rm{background:#7a2d18;color:#ffd9c9}',
      '.engine-tabs{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 14px;border-bottom:1px solid #e6d9bd;padding-bottom:12px}',
      '.engine-tab{border:1px solid #e6d9bd;background:#fff;color:#6a5c40;border-radius:7px;padding:6px 12px;cursor:pointer;font:600 12px var(--engine-ui)}',
      '.engine-tab:hover{border-color:var(--engine-amber)}',
      '.engine-tab.on{background:var(--engine-amber);color:#241a08;border-color:#a9742a}',
      '.engine-uphint{font:600 11px var(--engine-ui);color:#9a8a66;margin-right:auto;align-self:center}',
      '#engine-listfloat,#engine-stylefloat{position:fixed;z-index:2147483100;display:none;font:700 13px var(--engine-ui);text-transform:uppercase;letter-spacing:.6px;background:var(--engine-ink);color:var(--engine-amber);border:1px solid var(--engine-amber);border-radius:9px;padding:11px 16px;cursor:pointer;box-shadow:0 8px 22px -8px rgba(0,0,0,.7)}',
      '#engine-editsection{position:fixed;right:0;top:50%;transform:translateY(-50%);z-index:2147483090;display:none;font:700 12.5px var(--engine-ui);background:var(--engine-ink);color:var(--engine-amber);border:1px solid var(--engine-amber);border-right:0;border-radius:10px 0 0 10px;padding:13px 15px;cursor:pointer;white-space:nowrap;box-shadow:-6px 8px 24px -8px rgba(0,0,0,.6)}',
      '#engine-editsection.show{display:block}',
      '#engine-editsection:hover{background:var(--engine-amber);color:var(--engine-ink)}',
      '#engine-listfloat.show,#engine-stylefloat.show{display:block}',
      '#engine-listfloat:hover,#engine-stylefloat:hover{background:var(--engine-amber);color:var(--engine-ink)}',
      '.engine-drag{cursor:grab;color:#9a8a66;font:700 10px var(--engine-mono,monospace);text-transform:uppercase;letter-spacing:1px;padding:3px 6px;border:1px solid #e6d9bd;border-radius:5px;user-select:none}',
      '.engine-drag:active{cursor:grabbing}',
      '.engine-item.engine-dragover{border-color:var(--engine-amber);box-shadow:0 0 0 3px rgba(227,162,60,.25)}',
      '.engine-item.engine-dragging{opacity:.5}',
      '#engine-draghandle{position:fixed;z-index:2147483100;display:none;cursor:grab;background:var(--engine-ink);color:var(--engine-amber);border:1px solid var(--engine-amber);border-radius:6px;padding:3px 6px;font-size:13px;line-height:1;box-shadow:0 4px 14px -4px rgba(0,0,0,.7)}',
      '#engine-draghandle.show{display:block}',
      '#engine-draghandle:active{cursor:grabbing}',
      '.engine-drop-target{outline:2px dashed var(--engine-amber)!important;outline-offset:3px}',
      'body.engine-reordering{cursor:grabbing}',
      '.engine-navrow{display:flex;flex-wrap:wrap;align-items:center;gap:7px;row-gap:7px;padding:11px 2px;border-bottom:1px solid #eee2c8}',
      '.engine-navrow > b{flex:1 1 100%}',
      '.engine-navrow b{font-family:var(--engine-display);font-weight:600;font-size:15px}',
      '.engine-navfile{font:600 10px var(--engine-mono);color:#b9a87f;text-transform:uppercase;letter-spacing:1px}',
      '.engine-navhint{font:12px var(--engine-ui);color:#9a8a66;margin:0 0 12px;line-height:1.4}',
      '.engine-navrow.engine-dragover{outline:2px dashed var(--engine-amber);outline-offset:-2px;border-radius:4px}',
      '.engine-navrow.engine-rowhidden b{color:#b9a87f;text-decoration:line-through}',
      '.engine-navrow.engine-dragging{opacity:.5}',
      '@keyframes engine-flash{0%,100%{box-shadow:none}30%{box-shadow:0 0 0 4px var(--engine-amber)}}',
      '.engine-flash{animation:engine-flash 1.7s;border-radius:4px}',
      '.engine-modal input[type=file]{font:12px var(--engine-ui);color:#5d5036}',
      '.engine-toast{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(8px);background:var(--engine-ink);color:var(--engine-cream);padding:12px 18px 12px 16px;border-left:3px solid var(--engine-amber);border-radius:8px;z-index:2147483300;font:500 13px var(--engine-mono);letter-spacing:.3px;box-shadow:0 16px 40px -12px rgba(0,0,0,.7);display:flex;align-items:center;max-width:calc(100vw - 32px);opacity:0;transition:opacity .25s,transform .25s}',
      '.engine-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}',
      '.engine-toast-act{margin-left:14px;flex:none;border:0;border-radius:6px;padding:5px 12px;cursor:pointer;background:var(--engine-amber);color:#241a08;font:700 12px var(--engine-ui)}',
      '#engine-mtools{max-width:calc(100vw - 16px);flex-wrap:wrap}',
      '@media (max-width:760px){#engine-bar{gap:6px;padding:6px 8px}#engine-bar .engine-sub{display:none}#engine-bar button{padding:7px 9px;font-size:12px;white-space:nowrap}#engine-bar .engine-wordmark{font-size:18px}}'
    ].join('\n');
    document.head.appendChild(el('style', { id: 'engine-edit-styles', text: css }));
  }

  // ------------------------------------------------------------------- toast
  var toastEl, toastTimer;
  function toast(msg, ms, action) {
    if (!toastEl) { toastEl = el('div', { 'class': 'engine-toast' }); document.body.appendChild(toastEl); }
    toastEl.innerHTML = '';
    toastEl.appendChild(document.createTextNode(msg));
    if (action && action.label) {
      toastEl.appendChild(el('button', { 'class': 'engine-toast-act', text: action.label, on: { click: function () { toastEl.classList.remove('show'); action.fn(); } } }));
    }
    toastEl.classList.add('show');
    clearTimeout(toastTimer); toastTimer = setTimeout(function () { toastEl.classList.remove('show'); }, ms || 2200);
  }

  // ----------------------------------------------------------------- toolbar
  var bar, btnSave, modeEdit, modePrev, fileLabel;
  // Human page names for the toolbar/panels (never show raw filenames like
  // "index"), from the site config (window.SITE.pages[*].name).
  function friendlyPageName(fk) {
    if (!fk) return '';
    var cfgName = window.SITE && window.SITE.pageName(fk);
    if (cfgName) return cfgName;
    var m = String(fk).match(/^page-(.+)$/);
    if (m) {
      var g = state.models.global;
      if (g && Array.isArray(g.nav)) { var hit = g.nav.filter(function (n) { return n.url === '/p/' + m[1]; })[0]; if (hit && hit.label) return hit.label; }
      return m[1].replace(/-/g, ' ').replace(/\b\w/g, function (c) { return c.toUpperCase(); });
    }
    return fk;
  }
  function buildToolbar() {
    injectStyles();
    document.body.classList.add('engine-has-bar');
    bar = el('div', { id: 'engine-bar' });
    var toggle = el('div', { 'class': 'toggle' });
    modeEdit = el('button', { text: 'Edit', on: { click: function () { setEditing(true); } } });
    modePrev = el('button', { text: 'Preview', on: { click: function () { setEditing(false); } } });
    toggle.appendChild(modeEdit); toggle.appendChild(modePrev);
    fileLabel = el('span', { 'class': 'pill', text: 'loading…' });
    btnSave = el('button', { 'class': 'primary', text: 'Save draft', title: 'Save your edits as a private draft — only you see them until you Publish.', on: { click: save } });
    var btnPub = el('button', { 'class': 'go', text: 'Publish', title: 'Publish your saved draft to the live website — visitors will see these changes.', on: { click: publish } });
    var btnOut = el('button', { text: 'Log out', on: { click: function () { location.href = '/.auth/logout'; } } });
    var btnBg = el('button', { text: 'Background', on: { click: editBackground } });
    var btnTheme = el('button', { text: 'Theme', on: { click: openThemePanel } });
    var btnNav = el('button', { text: 'Sections', on: { click: openSectionsNav } });
    var btnPages = el('button', { text: 'Pages', on: { click: openPagesPanel } });
    var btnResp = el('button', { text: 'Responses', title: 'Download questionnaire responses (CSV)', on: { click: function () { window.location.href = '/api/questionnaire-export?format=csv'; } } });
    var btnStats = el('button', { text: 'Refresh stats', title: 'Pull live numbers (YouTube/Instagram) into the Stats page draft', on: { click: function () {
      toast('Refreshing stats…', 8000);
      fetchJson('/api/stats-refresh', { method: 'POST' }).then(function (r) {
        toast((r && r.message) || 'Stats refreshed', 5000, (r && r.updated && r.updated.length) ? { label: 'Reload', fn: function () { location.reload(); } } : null);
      }).catch(function (e) { toast('Stats refresh failed: ' + e.message, 6000); });
    } } });
    var btnQuest = el('button', { text: 'Questionnaire', title: 'Edit the signup survey questions', on: { click: openQuestionnairePanel } });
    var btnTrack = el('button', { text: 'Tracking', title: 'Analytics & pixel IDs (Meta, Google)', on: { click: openTrackingPanel } });
    var btnHelp = el('button', { text: 'Help', on: { click: openHelp } });
    var btnRevert = el('button', { text: 'Revert', title: 'Discard all unpublished changes and restore the published version', on: { click: revert } });
    bar.appendChild(el('div', { 'class': 'engine-brand' }, [
      el('span', { 'class': 'engine-rec', title: 'You are editing a private draft. Use Publish to make changes live.' }),
      el('span', { 'class': 'engine-wordmark', text: (window.SITE && window.SITE.wordmark) || '' }),
      el('span', { 'class': 'engine-sub', text: 'editor' })
    ]));
    bar.appendChild(toggle);
    bar.appendChild(fileLabel);
    bar.appendChild(el('span', { 'class': 'sp' }));
    // Primary actions stay on the bar; secondary ones go in a "More" dropdown so
    // the toolbar doesn't overflow on laptops / narrow windows.
    bar.appendChild(btnNav); bar.appendChild(btnPages); bar.appendChild(btnSave); bar.appendChild(btnPub);
    var moreMenu = el('div', { id: 'engine-more' });
    [btnTheme, btnBg, btnQuest, btnTrack, btnResp, btnStats, btnRevert, btnHelp, btnOut].forEach(function (x) { moreMenu.appendChild(x); });
    var moreBtn = el('button', { 'class': 'engine-morebtn', text: 'More', title: 'More options', on: { click: function (e) { e.stopPropagation(); moreMenu.classList.toggle('show'); } } });
    document.addEventListener('click', function (e) { if (e.target !== moreBtn && moreMenu && !moreMenu.contains(e.target)) moreMenu.classList.remove('show'); });
    bar.appendChild(moreBtn); bar.appendChild(moreMenu);
    document.body.appendChild(bar);
    refreshToolbar();
    syncBarPad();
    window.addEventListener('resize', syncBarPad);
    // The bar's height changes when it wraps (and after web fonts load). A
    // ResizeObserver catches every change, not just window resizes, so the header
    // offset always matches — even on the first paint before the bar has wrapped.
    if (window.ResizeObserver) { try { new ResizeObserver(syncBarPad).observe(bar); } catch (_) {} }
  }
  // The toolbar wraps when narrow, so its height varies — keep the page content
  // clear of it by matching the body's top padding to the bar's actual height.
  function syncBarPad() {
    // Publish the real bar height as a CSS var so the body padding AND the fixed
    // site header / menu-toggle / mobile overlay all track it (they used to be
    // hardcoded to 54px, which the wrapping bar overran on narrow screens).
    if (bar) { try { document.documentElement.style.setProperty('--engine-barh', bar.offsetHeight + 'px'); } catch (_) {} }
  }
  function refreshToolbar() {
    if (!bar) return;
    if (fileLabel) {
      var nm = friendlyPageName(state.file) || 'page';
      fileLabel.textContent = state.file ? (nm + (isDirty() ? ' — unsaved' : ' — draft')) : 'loading…';
      fileLabel.classList.toggle('engine-pill-dirty', isDirty());
    }
    if (btnSave) btnSave.disabled = !isDirty();
    if (modeEdit) modeEdit.classList.toggle('on', state.editing);
    if (modePrev) modePrev.classList.toggle('on', !state.editing);
  }
  function setEditing(on) {
    state.editing = on;
    document.body.classList.toggle('engine-edit', on);
    var nodes = document.querySelectorAll('[data-edit-text],[data-edit-html]');
    for (var i = 0; i < nodes.length; i++) nodes[i].contentEditable = on ? 'true' : 'false';
    if (!on) { hideMediaTools(); hideListFloat(); hideDragHandle(); hideStyleFloat(); }
    if (on) maybePulseEditable();
    try { sessionStorage.setItem('engine-editing', on ? '1' : '0'); } catch (_) {}
    refreshToolbar();
    updateEditTab();
  }

  // ------------------------------------------------------------- bind the DOM
  function bindAll() {
    if (!Object.keys(state.models).length) return;
    document.querySelectorAll('[data-edit-text]').forEach(function (n) { bindText(n, false); });
    document.querySelectorAll('[data-edit-html]').forEach(function (n) { bindText(n, true); });
    document.querySelectorAll('[data-edit-image]').forEach(bindImage);
    document.querySelectorAll('[data-edit-attr]').forEach(bindAttr);
    document.querySelectorAll('[data-edit-bg]').forEach(bindBg);
    document.querySelectorAll('[data-edit-list]').forEach(bindList);
    // (No per-block "Style" hover button — the persistent right-side "Edit
    // <section>" tab covers that, so they don't double up.)
    setEditing(state.editing);
  }
  function bindText(n, isHtml) {
    if (n.__engineBound) return; n.__engineBound = true;
    n.addEventListener('input', function () {
      var m = modelOf(n); if (!m) return;
      setByPath(m, n.getAttribute(isHtml ? 'data-edit-html' : 'data-edit-text'), isHtml ? n.innerHTML : n.textContent);
      markDirty(fileKeyOf(n));
    });
    n.addEventListener('keydown', function (e) { if (!isHtml && e.key === 'Enter') { e.preventDefault(); n.blur(); } });
  }
  function bindImage(n) {
    if (n.__engineBound) return; n.__engineBound = true;
    // Clicking media opens its section's one Edit panel (no per-image hover bar).
    n.addEventListener('click', function (e) {
      if (!state.editing) return;
      e.preventDefault(); e.stopPropagation();
      var sec = n.closest && n.closest('[data-engine-path]');
      if (sec) openSectionEditor(sec.getAttribute('data-engine-path'));
      else showMediaTools(n); // legacy standalone media with no section block
    }, true);
  }
  // Swap the media a node points at, writing the chosen path back to the model.
  function replaceMedia(n) {
    var path = n.getAttribute('data-edit-image');
    if (!path) return;
    var m = modelOf(n); if (!m) return;
    openMediaPicker(n.getAttribute('data-edit-prefix') || prefixOf(getByPath(m, path)), function (chosen) {
      setByPath(m, path, chosen);
      applyMediaSrc(n, chosen);
      markDirty(fileKeyOf(n));
    });
  }
  function applyMediaSrc(n, chosen) {
    n.style.display = '';
    if (n.tagName === 'IMG') n.src = mediaUrl(chosen);
    else if (n.tagName === 'VIDEO') { var s = n.querySelector('source'); if (!s) { s = document.createElement('source'); n.appendChild(s); } s.src = mediaUrl(chosen); n.load(); }
    else n.style.backgroundImage = 'url("' + mediaUrl(chosen) + '")';
  }
  // Remove a standalone media field (e.g. the tour poster). List-item media are
  // removed from the list panel instead, so Remove is hidden for those.
  function removeMedia(n) {
    var path = n.getAttribute('data-edit-image');
    if (!path) return;
    var m = modelOf(n); if (!m) return;
    var fk = fileKeyOf(n);
    var noun = n.getAttribute('data-edit-label') || (n.tagName === 'VIDEO' ? 'video' : 'image');
    var prev = getByPath(m, path);
    setByPath(m, path, '');
    if (n.tagName === 'IMG' || n.tagName === 'VIDEO') n.style.display = 'none';
    else n.style.backgroundImage = '';
    markDirty(fk);
    toast('Removed ' + noun + ' — not saved yet', 7000, { label: 'Undo', fn: function () {
      setByPath(m, path, prev);
      if (prev) applyMediaSrc(n, prev); else n.style.display = '';
      markDirty(fk);
      toast(noun + ' restored');
    } });
  }
  // ---- floating media toolbar (Replace / Remove), shown on hover in Edit mode ----
  function mtoolsEl() {
    if (mtools) return mtools;
    mtools = el('div', { id: 'engine-mtools' });
    mtools.__rep = el('button', { text: 'Replace', title: 'Pick a different file or upload a new one', on: { click: function () { hideMediaTools(); if (mtools.__n) replaceMedia(mtools.__n); } } });
    mtools.__rm = el('button', { 'class': 'rm', text: 'Remove', title: 'Hide it from the page (you can undo)', on: { click: function () { hideMediaTools(); if (mtools.__n) removeMedia(mtools.__n); } } });
    mtools.appendChild(mtools.__rep); mtools.appendChild(mtools.__rm);
    mtools.addEventListener('mouseenter', function () { clearTimeout(mtoolsTimer); });
    mtools.addEventListener('mouseleave', hideMediaToolsSoon);
    document.addEventListener('pointerdown', function (e) {
      if (mtools && mtools.classList.contains('show') && !mtools.contains(e.target) && !(e.target.closest && e.target.closest('[data-edit-image]'))) hideMediaTools();
    }, true);
    document.body.appendChild(mtools);
    return mtools;
  }
  function showMediaTools(n) {
    var m = mtoolsEl();
    m.__n = n;
    var noun = n.getAttribute('data-edit-label') || (n.tagName === 'VIDEO' ? 'video' : 'image');
    m.__rep.textContent = 'Replace ' + noun;
    m.__rm.textContent = 'Remove ' + noun;
    m.__rm.style.display = /\[/.test(n.getAttribute('data-edit-image') || '') ? 'none' : '';
    clearTimeout(mtoolsTimer);
    m.classList.add('show');
    var r = n.getBoundingClientRect();
    // Sit ABOVE the element (in the gap) so we never cover what you're editing;
    // drop below only if there's no room above.
    var top = r.top - m.offsetHeight - 6;
    if (top < 60) top = Math.min(r.bottom + 6, window.innerHeight - m.offsetHeight - 8);
    m.style.top = top + 'px';
    m.style.left = Math.min(Math.max(8, r.left), window.innerWidth - 12 - m.offsetWidth) + 'px';
  }
  function hideMediaTools() { if (mtools) mtools.classList.remove('show'); }
  function hideMediaToolsSoon() { clearTimeout(mtoolsTimer); mtoolsTimer = setTimeout(hideMediaTools, 280); }
  function bindAttr(n) {
    if (n.__engineBound) return; n.__engineBound = true;
    n.addEventListener('click', function (e) {
      if (!state.editing) return;
      e.preventDefault(); e.stopPropagation();
      var spec = n.getAttribute('data-edit-attr'); // "path|attr"
      var parts = spec.split('|'), path = parts[0], attr = parts[1] || 'href';
      var m = modelOf(n); if (!m) return;
      var cur = getByPath(m, path);
      var val = window.prompt('Edit ' + (n.getAttribute('data-edit-label') || 'value') + ':', cur != null ? cur : '');
      if (val == null) return;
      setByPath(m, path, val);
      if (attr === 'text') n.textContent = val; else n.setAttribute(attr, val);
      markDirty(fileKeyOf(n));
    }, true);
  }
  function bindBg(n) {
    if (n.__engineBound) return; n.__engineBound = true;
    n.addEventListener('click', function (e) {
      if (!state.editing) return;
      e.preventDefault(); e.stopPropagation();
      var blobName = n.getAttribute('data-edit-bg'); // e.g. <folder>/background.jpg
      openMediaPicker(prefixOf('/media-content/' + blobName), function () {}, {
        replaceName: blobName,
        onReplaced: function () { toast('Background replaced — reloading…'); setTimeout(function () { location.reload(); }, 700); }
      });
    }, true);
  }
  function bindList(container) {
    if (container.__engineListBound) return; container.__engineListBound = true;
    // (No hover "Edit list" float — lists are edited from the section's Edit panel.)
    // Drag the whole item from ANYWHERE on it (no grip) — except editable text /
    // inputs / links, so clicking those still works. Grabbing an image or video
    // drags the tile (we disable the media's own native drag below).
    container.addEventListener('mousedown', function (e) {
      if (!state.editing) return;
      var child = directChildOf(container, e.target);
      if (!child) return;
      var interactive = e.target.closest && e.target.closest('input,textarea,select,button,[contenteditable="true"]');
      child.draggable = !interactive;
      if (!interactive) {
        var media = child.querySelectorAll && child.querySelectorAll('img,video,a');
        if (media) for (var mi = 0; mi < media.length; mi++) media[mi].draggable = false;
      }
    });
    container.addEventListener('dragstart', function (e) {
      if (!state.editing) return;
      if (e.target.closest && e.target.closest('[data-edit-list]') !== container) return; // innermost list owns the drag
      var child = directChildOf(container, e.target);
      if (!child || !child.draggable) return;
      startReorder(container, itemIndexOf(container, child), e);
    });
    container.addEventListener('dragend', function () { endReorder(); });
  }
  // ---- on-screen drag-to-reorder: a floating grip on the hovered list item ----
  function directChildOf(container, node) {
    while (node && node.parentNode !== container) node = node.parentNode;
    return (node && node.parentNode === container) ? node : null;
  }
  // The TRUE model-array index of a rendered list child. Parts stamp each child
  // with data-item-index (its real index in the data array), because some parts
  // render fewer DOM children than the array has items (they skip empty/invalid
  // ones). Reordering by DOM position would then splice the wrong item, so we
  // prefer the stamped index and only fall back to DOM position when it's absent.
  function itemIndexOf(container, child) {
    if (child && child.getAttribute) {
      var raw = child.getAttribute('data-item-index');
      if (raw != null && raw !== '') { var n = parseInt(raw, 10); if (!isNaN(n)) return n; }
    }
    return Array.prototype.indexOf.call(container.children, child);
  }
  function startReorder(container, fromIndex, e) {
    dragNow = { container: container, path: container.getAttribute('data-edit-list'), from: fromIndex, to: fromIndex };
    try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', 'engine'); } catch (_) {}
    document.body.classList.add('engine-reordering');
  }
  function dragHandleEl() {
    if (dragHandle) return dragHandle;
    dragHandle = el('div', { id: 'engine-draghandle', title: 'Drag to reorder', text: '' });
    dragHandle.setAttribute('draggable', 'true');
    dragHandle.addEventListener('mouseenter', function () { clearTimeout(dragHandleTimer); });
    dragHandle.addEventListener('mouseleave', hideDragHandleSoon);
    dragHandle.addEventListener('dragstart', function (e) {
      if (!dragHandle.__c || !dragHandle.__item) return;
      startReorder(dragHandle.__c, itemIndexOf(dragHandle.__c, dragHandle.__item), e);
    });
    dragHandle.addEventListener('dragend', endReorder);
    document.addEventListener('dragover', function (e) {
      if (!dragNow) return;
      e.preventDefault();
      var child = directChildOf(dragNow.container, e.target);
      var prev = document.querySelectorAll('.engine-drop-target');
      for (var i = 0; i < prev.length; i++) if (prev[i] !== child) prev[i].classList.remove('engine-drop-target');
      if (child) { child.classList.add('engine-drop-target'); dragNow.to = itemIndexOf(dragNow.container, child); }
    });
    document.addEventListener('drop', function (e) { if (dragNow) { e.preventDefault(); endReorder(); } });
    document.body.appendChild(dragHandle);
    return dragHandle;
  }
  function hideDragHandle() { if (dragHandle && !dragNow) dragHandle.classList.remove('show'); }
  function hideDragHandleSoon() { clearTimeout(dragHandleTimer); dragHandleTimer = setTimeout(hideDragHandle, 280); }
  function endReorder() {
    document.body.classList.remove('engine-reordering');
    var t = document.querySelectorAll('.engine-drop-target');
    for (var i = 0; i < t.length; i++) t[i].classList.remove('engine-drop-target');
    var d = dragNow; dragNow = null;
    if (dragHandle) dragHandle.classList.remove('show');
    if (d && d.to != null && d.to !== d.from) {
      var fk = fileKeyOf(d.container), model = state.models[fk];
      var arr = getByPath(model, d.path);
      if (Array.isArray(arr) && d.from < arr.length) {
        arr.splice(d.to, 0, arr.splice(d.from, 1)[0]);
        markDirty(fk);
        save(true);
      }
    }
  }
  // Floating "Edit <list>" button at the container's top-right — never inserted
  // into the flow, so it can't disturb grid/flex layouts (e.g. the header).
  function listFloatEl() {
    if (listFloat) return listFloat;
    listFloat = el('button', { id: 'engine-listfloat', on: { click: function () {
      var c = listFloat.__c;
      if (c) openListPanel(c, c.getAttribute('data-edit-list'), c.getAttribute('data-edit-list-label') || 'items');
    } } });
    listFloat.addEventListener('mouseenter', function () { clearTimeout(listFloatTimer); });
    listFloat.addEventListener('mouseleave', hideListFloatSoon);
    document.body.appendChild(listFloat);
    return listFloat;
  }
  function showListFloat(container) {
    var b = listFloatEl();
    b.__c = container;
    b.textContent = 'Edit ' + (container.getAttribute('data-edit-list-label') || 'list');
    clearTimeout(listFloatTimer);
    b.classList.add('show');
    var r = container.getBoundingClientRect();
    var top = r.top - b.offsetHeight - 6;        // sit above the list, not over it
    if (top < 60) top = Math.min(r.bottom + 6, window.innerHeight - b.offsetHeight - 8); // no room above → below (so it can't cover the nav)
    b.style.top = top + 'px';
    b.style.left = Math.min(Math.max(8, r.right - b.offsetWidth), window.innerWidth - 12 - b.offsetWidth) + 'px';
  }
  function hideListFloat() { if (listFloat) listFloat.classList.remove('show'); }
  function hideListFloatSoon() { clearTimeout(listFloatTimer); listFloatTimer = setTimeout(hideListFloat, 300); }

  // ------------------------------------------------------------- media picker
  // Copy an existing blob to a fixed name (used when picking an existing image
  // as a page background, which must live at a known filename).
  function copyExistingTo(name, sourceUrl, overlay, opts) {
    toast('Updating…', 4000);
    fetch(sourceUrl).then(function (r) { return r.blob(); }).then(function (b) {
      return new Promise(function (res, rej) { var fr = new FileReader(); fr.onload = function () { res(fr.result); }; fr.onerror = rej; fr.readAsDataURL(b); });
    }).then(function (dataUrl) {
      return fetchJson('/api/media-upload', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ prefix: name.replace(/\/[^/]+$/, ''), filename: name.split('/').pop(), contentBase64: dataUrl }) });
    }).then(function (r) { overlay.remove(); if (opts.onReplaced) opts.onReplaced(r); toast('Updated'); })
      .catch(function (e) { toast('Couldn’t update that — please try again.', 5000); });
  }
  // ---- upload optimisation: compress + WebP + hard caps ----
  // Images are downscaled and converted to WebP in the browser before upload;
  // videos are size-capped (steer big ones to YouTube). Folders the site config
  // marks as full-quality (SITE.fullQualityMedia, e.g. a press kit) keep the
  // original file up to the transport ceiling. The server has its own backstop.
  var IMG_MAX_EDGE = 2560, IMG_WEBP_Q = 0.82;
  var CAP_VIDEO = 50 * 1048576;     // video (full-quality folders exempt)
  var CAP_IMG_SRC = 40 * 1048576;   // sanity cap on a source image
  var CAP_TRANSPORT = 90 * 1048576; // absolute (Function request limit, with headroom)
  function mbStr(b) { return (Math.round((b || 0) / 1048576 * 10) / 10) + ' MB'; }
  function toWebpBlob(file) {
    function fromBitmap(bmp) {
      var scale = Math.min(1, IMG_MAX_EDGE / Math.max(bmp.width, bmp.height));
      var cw = Math.max(1, Math.round(bmp.width * scale)), ch = Math.max(1, Math.round(bmp.height * scale));
      var c = document.createElement('canvas'); c.width = cw; c.height = ch;
      c.getContext('2d').drawImage(bmp, 0, 0, cw, ch);
      if (bmp.close) bmp.close();
      return new Promise(function (res, rej) {
        c.toBlob(function (b) { b ? res(b) : rej(new Error('encode failed')); }, 'image/webp', IMG_WEBP_Q);
      });
    }
    if (typeof createImageBitmap === 'function') {
      return createImageBitmap(file, { imageOrientation: 'from-image' }).then(fromBitmap);
    }
    return new Promise(function (res, rej) {
      var url = URL.createObjectURL(file), img = new Image();
      img.onload = function () { URL.revokeObjectURL(url); fromBitmap(img).then(res, rej); };
      img.onerror = function () { URL.revokeObjectURL(url); rej(new Error('decode failed')); };
      img.src = url;
    });
  }
  // Returns Promise<{blob, name, note}>; throws a friendly Error to reject.
  function prepareMedia(file, name, isFullQuality) {
    var isImg = /\.(jpe?g|png|webp|gif|bmp)$/i.test(name);
    var isVid = /\.(mp4|webm|mov|m4v)$/i.test(name);
    if (isFullQuality) {
      if (file.size > CAP_TRANSPORT) throw new Error('This file is ' + mbStr(file.size) + '. The editor can upload up to ~90 MB — please use a smaller export.');
      return Promise.resolve({ blob: file, name: name, note: '' });
    }
    if (isVid) {
      if (file.size > CAP_VIDEO) throw new Error('This video is ' + mbStr(file.size) + '. Videos are capped at ' + mbStr(CAP_VIDEO) + ' for fast loading — upload it to YouTube and paste the link, or use a shorter/smaller clip.');
      return Promise.resolve({ blob: file, name: name, note: '' });
    }
    if (isImg && /\.(jpe?g|png|webp)$/i.test(name)) {
      if (file.size > CAP_IMG_SRC) throw new Error('This image is ' + mbStr(file.size) + ' — please use one under ' + mbStr(CAP_IMG_SRC) + '.');
      return toWebpBlob(file).then(function (blob) {
        if (blob.size < file.size) return { blob: blob, name: name.replace(/\.[^.]+$/, '.webp'), note: 'compressed ' + mbStr(file.size) + ' → ' + mbStr(blob.size) + ', WebP' };
        return { blob: file, name: name, note: '' };
      }).catch(function () { return { blob: file, name: name, note: '' }; }); // server has a sharp backstop
    }
    // gif / svg / ico / other: pass through (don't rasterise vectors or animations).
    if (file.size > CAP_TRANSPORT) throw new Error('This file is ' + mbStr(file.size) + ' — the editor can upload up to ~90 MB.');
    return Promise.resolve({ blob: file, name: name, note: '' });
  }
  function doUpload(file, targetPrefix, overlay, onChoose, opts) {
    var src = opts.replaceName ? opts.replaceName.split('/').pop() : file.name;
    var name = src.toLowerCase().replace(/[^a-z0-9._-]+/g, '-');
    var mb = function (b) { return Math.round((b || 0) / 1048576 * 10) / 10; };
    var isFull = !!(window.SITE && window.SITE.isFullQuality && window.SITE.isFullQuality(targetPrefix || ''));
    toast('Preparing ' + name + '…', 9000);
    Promise.resolve().then(function () { return prepareMedia(file, name, isFull); }).then(function (prep) {
      var reader = new FileReader();
      reader.onload = function () {
        toast('Uploading ' + prep.name + (prep.note ? ' (' + prep.note + ')' : '') + '…', 9000);
        fetchJson('/api/media-upload', { method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ prefix: targetPrefix, filename: prep.name, contentBase64: reader.result })
        }).then(function (r) {
          overlay.remove();
          if (opts.onReplaced) opts.onReplaced(r); else onChoose(r.path);
          var msg = 'Uploaded ' + r.name;
          if (prep.note) msg += ' — ' + prep.note;
          else if (r.optimized === 'video') msg += ' (optimized ' + mb(r.originalBytes) + ' → ' + mb(r.storedBytes) + ' MB)';
          else if (r.optimized === 'webp') msg += ' (converted to WebP)';
          toast(msg, 4000);
        }).catch(function (e) { toast('Couldn’t upload that file — it may be too large or an unsupported type. Try a smaller image, or paste a YouTube link for a video.', 7000); });
      };
      reader.readAsDataURL(prep.blob);
    }).catch(function (e) {
      // Cap/validation failure: keep the picker open so they can choose another file.
      toast(e.message || 'Could not prepare that file', 8000);
    });
  }
  // Tabbed media browser: pick any image from any section, or upload to the
  // current tab. Solves "I can't find the original logo" — switch to its tab.
  function openHelp() {
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) close(); } } });
    var modal = el('div', { 'class': 'engine-modal', style: 'max-width:640px' });
    overlay.appendChild(modal); document.body.appendChild(overlay);
    function close() { overlay.remove(); document.removeEventListener('keydown', esc); }
    function esc(e) { if (e.key === 'Escape') close(); }
    document.addEventListener('keydown', esc);

    modal.appendChild(el('h3', { text: 'How to use the editor' }));
    modal.appendChild(el('p', {
      style: 'margin:0 0 4px;color:#6a5c40;font:14.5px/1.6 var(--engine-ui,system-ui)',
      text: 'You are editing a private draft of the site. Nothing is public until you choose to publish, so take your time and click around.'
    }));

    var sections = [
      ['Edit and Preview', [
        'Use the Edit / Preview toggle at the top left.',
        'Edit shows a dashed outline around everything you can change. Preview hides all the editing controls so you see exactly what visitors see.']],
      ['Text', [
        'In Edit mode, click any outlined text (taglines, bios, descriptions, show details) and just type.',
        'Click somewhere else when you are done.']],
      ['Photos and videos', [
        'Click any outlined image, or the hero video, to open the media browser.',
        'Pick an existing file from any tab, or upload a new one from your computer.',
        'When you hover over an image or video, small Replace and Remove buttons appear just above it.']],
      ['Lists (people, events, quotes, links)', [
        'Each list has an amber button labelled "Edit" followed by the list name, near its corner.',
        'Open it to add, remove, or reorder items.',
        'You can also drag an item directly on the page to reorder it.']],
      ['Page sections', [
        'The Sections button lists every block on the page.',
        'Drag a row to set the order, then choose Apply and save.',
        'Use Hide to remove a section (such as the tour poster) from the page. It stays in the list so you can Show it again later.']],
      ['Background', [
        'The Background button changes the large background image behind the page.']],
      ['Saving and publishing', [
        'Save Changes stores your edits on the private draft. Refresh the page to confirm they stuck.',
        'Publish Changes copies the draft to the live public website for everyone to see.']],
      ['Undo and revert', [
        'If you only typed something and have not saved, just reload the page to undo it.',
        'Revert discards every change you have saved but not yet published and restores the last published version. Use it if something went wrong.']]
    ];
    sections.forEach(function (s) {
      modal.appendChild(el('div', {
        style: 'margin-top:14px;font:700 11px var(--engine-mono,monospace);text-transform:uppercase;letter-spacing:1.3px;color:#a9742a',
        text: s[0]
      }));
      var ul = el('ul', { style: 'margin:5px 0 0;padding-left:20px;color:#2a2317;font:14.5px/1.6 var(--engine-ui,system-ui)' });
      s[1].forEach(function (t) { ul.appendChild(el('li', { style: 'margin:2px 0', text: t })); });
      modal.appendChild(ul);
    });

    modal.appendChild(el('div', { 'class': 'engine-btnrow' }, [
      el('button', { text: 'Got it', on: { click: close } })
    ]));
  }

  // ----------------------------------------------------------- per-tool help
  // A small "?" beside each tool's title opens a focused, plain-language
  // explainer — including the "what if I leave this empty?" edge cases that
  // trip up non-technical editors. Content is intentionally short and concrete.
  var HELP = {
    sections: {
      title: 'Sections — your page builder',
      intro: 'Every page is a stack of "sections". This panel lists them all and gives each the same controls, so nothing is a special case.',
      body: [
        ['The controls on each row', [
          'Drag the handle to reorder — the new order previews instantly.',
          'Hide takes a section off the page but keeps it in the list — click Show to bring it back. Nothing is lost (great for a tour poster between tours).',
          'View jumps to that section on the page. Edit opens its content.',
          'Delete removes an added section. An Undo appears — use it before you save if it was a mistake.']],
        ['Adding & saving', [
          '"+ Add section" opens the parts library (Text, Photo gallery, Video, Carousel, and more). Whatever you pick drops in pre-filled, so you never face a blank box.',
          'Changes apply to your private draft when you click "Apply & Save". Nothing is public until you Publish.']],
        ['Hide vs Delete — which?', [
          'Hide = "not now, maybe later" (reversible any time).',
          'Delete = "gone" (only undoable until you save).']]
      ]
    },
    pages: {
      title: 'Pages & visibility',
      intro: 'Create, rename, and remove the pages in your menu — and control who can see each one.',
      body: [
        ['The three visibility levels', [
          'Public — in the live menu, visible to everyone (most pages).',
          'Unlisted — not in the live menu, but anyone with the link can open it. Perfect for a press kit or a page you share privately.',
          'Dev only — stays on your private draft and is NEVER published. Use it for works-in-progress or admin pages (the Stats page is set this way).']],
        ['Creating a page', [
          'Type a name and Create — you get a blank page you build from the parts library, exactly like any other.',
          'New pages start in the menu; set them Unlisted or Dev only if they are not ready.']],
        ['Publishing', [
          'Publish copies your draft to the live site. Pages marked "Dev only" are skipped automatically — they never go live.']]
      ]
    },
    theme: {
      title: 'Theme — site-wide colors',
      intro: 'Set the colors used across every page in one place. Changes preview live; Save Changes to keep them.',
      body: [
        ['What each color does', [
          'Accent — buttons, links, and highlights.',
          'Text color — the default text color.',
          'Page background — the base color behind pages.']],
        ['Leaving a color unset', [
          'Clear a color and that element falls back to the built-in default — nothing breaks.']],
        ['Theme vs one section', [
          'Theme changes everything. To restyle just one section, hover it and use its "Style" button instead.']]
      ]
    },
    style: {
      title: 'Style — this section\'s look',
      intro: 'Fine-tune the appearance of just this one section. It overrides the site Theme for this section only.',
      body: [
        ['What you can change', [
          'Text color, background, and alignment.',
          'Font size — type a number for exact pixels (e.g. 20) or pick a preset.',
          'Borders — choose a thickness and color (handy around grids and cards).']],
        ['Inheriting', [
          'Leave a field empty and this section inherits the site Theme. So "empty" means "use the default", not "blank".']]
      ]
    },
    editsection: {
      title: 'Editing a section',
      intro: 'This is the content of one section — its text, items, images, and embeds.',
      body: [
        ['Tips', [
          'You can also edit most text right on the page — click the outlined text and type.',
          'For lists (photos, links, videos) use the list editor to add, remove, and reorder items.',
          'Paste a YouTube or Spotify link into an embed field and it becomes a player automatically.']]
      ]
    },
    lists: {
      title: 'Editing a list',
      intro: 'Add, remove, and reorder the items in this list — people, events, photos, links, questions, and so on.',
      body: [
        ['How it works', [
          'Drag an item to reorder. Use Add to create one, and an item\'s Remove to delete it.',
          'Edits are held until you click "Apply & Save". Cancel throws away changes made in this panel.']],
        ['Empty fields', [
          'Leaving an optional field (like a URL) empty just omits it — e.g. a show with no ticket link simply shows no button.']]
      ]
    },
    questionnaire: {
      title: 'Questionnaire (the signup survey)',
      intro: 'After a fan enters their email, they see this short survey. Here you edit its intro and its questions.',
      body: [
        ['What happens if you leave a box empty', [
          'A question with an empty label is skipped — it simply will not appear to fans. (So an empty question box = no question, not a blank one.)',
          'An empty intro heading or intro text shows that part blank, so it is best to fill them in (or keep them short).',
          'The email box is always there — it is how fans start the survey.']],
        ['Editing questions', [
          'Use "Edit questions" to add, remove, and reorder them. Each answer becomes a column in your downloaded responses (the Responses button).']],
        ['Renaming & removing — your data stays safe', [
          'Renaming a question keeps its responses column stable, so past answers still line up.',
          'Removing a question does not touch responses people already submitted.']]
      ]
    },
    tracking: {
      title: 'Tracking & analytics',
      intro: 'Connect analytics by pasting in your IDs. This is optional — the site works fine without it.',
      body: [
        ['How it works', [
          'Paste a Meta (Facebook) Pixel ID and/or a Google Analytics measurement ID.',
          'These run ONLY on the live, published site — never here in the editor — so your own clicks while editing are not counted.']],
        ['Leaving a box empty', [
          'Leave a box blank to keep that service off. Turn it off later by clearing the box and saving.']],
        ['Where the data shows up', [
          'In that service\'s own dashboard (Meta Events Manager, Google Analytics). It can take a little while to appear.']]
      ]
    },
    background: {
      title: 'Page background',
      intro: 'Each page has two background images — a wide one for large screens and a taller one phones swap in.',
      body: [
        ['Which image am I changing?', [
          'Large screens — the wide image behind the page on desktops and tablets.',
          'Phones — the taller image small screens swap in, so the subject stays visible on a narrow crop.']],
        ['After replacing', [
          'Built-in pages reuse the same file name, so if the new image doesn’t show, hard-refresh (Ctrl/Cmd-Shift-R).',
          'Pages you created store their background in the page itself — pick any image, then Save Changes.']]
      ]
    },
    media: {
      title: 'Photos, videos & backgrounds',
      intro: 'Pick an existing file or upload a new one from your computer.',
      body: [
        ['Uploading', [
          'Drop in a JPG, PNG, or video. Images are converted to fast-loading WebP automatically.',
          'Large images and videos are compressed and capped so pages stay quick on phones.']],
        ['Full-quality folders', [
          'Some folders (set in the site config — e.g. a press kit whose recipients need the originals) keep files at full quality and skip the size cap.']],
        ['Backgrounds', [
          'The Background tool opens this same browser to swap a page\'s big background image. If you reuse the same file name, hard-refresh (Ctrl/Cmd-Shift-R) to see the new one.']]
      ]
    },
    addsection: {
      title: 'Adding a section',
      intro: 'Pick a part from the library and it drops into the page, pre-filled, ready to edit in place.',
      body: [
        ['The library', [
          'Parts are grouped (Basic, Media, and more). Each tile is one part — click to add it.',
          'Everything you add gets the same controls in the Sections panel — reorder, Hide/Show, Edit, Delete.']]
      ]
    }
  };
  function helpBtn(key) {
    return el('button', { 'class': 'engine-help', type: 'button', text: '?', title: 'What is this? Click for help',
      'aria-label': 'Help', on: { click: function (e) { e.stopPropagation(); openHelpTopic(key); } } });
  }
  function openHelpTopic(key) {
    var t = HELP[key];
    if (!t) { openHelp(); return; }
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) close(); } } });
    var modal = el('div', { 'class': 'engine-modal', style: 'max-width:560px' });
    overlay.appendChild(modal); document.body.appendChild(overlay);
    function close() { overlay.remove(); document.removeEventListener('keydown', esc); }
    function esc(e) { if (e.key === 'Escape') close(); }
    document.addEventListener('keydown', esc);
    modal.appendChild(el('h3', { text: t.title }));
    if (t.intro) modal.appendChild(el('p', { style: 'margin:0 0 4px;color:#6a5c40;font:14.5px/1.6 var(--engine-ui,system-ui)', text: t.intro }));
    (t.body || []).forEach(function (s) {
      modal.appendChild(el('div', { style: 'margin-top:14px;font:700 11px var(--engine-mono,monospace);text-transform:uppercase;letter-spacing:1.3px;color:#a9742a', text: s[0] }));
      var ul = el('ul', { style: 'margin:5px 0 0;padding-left:20px;color:#2a2317;font:14.5px/1.6 var(--engine-ui,system-ui)' });
      s[1].forEach(function (line) { ul.appendChild(el('li', { style: 'margin:2px 0', text: line })); });
      modal.appendChild(ul);
    });
    modal.appendChild(el('div', { 'class': 'engine-btnrow' }, [ el('button', { text: 'Got it', on: { click: close } }) ]));
  }
  function openMediaPicker(prefix, onChoose, opts) {
    opts = opts || {};
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var modal = el('div', { 'class': 'engine-modal' });
    overlay.appendChild(modal); document.body.appendChild(overlay);

    modal.appendChild(el('div', { 'class': 'engine-mhead' }, [
      el('h3', { text: opts.replaceName ? ('Replace ' + opts.replaceName) : 'Choose or upload media' }), helpBtn('media')
    ]));
    function pick(it) {
      if (opts.replaceName) copyExistingTo(opts.replaceName, it.url, overlay, opts);
      else { overlay.remove(); onChoose(it.path); }
    }
    var tabs = el('div', { 'class': 'engine-tabs' });
    var grid = el('div', { 'class': 'engine-grid' });
    var up = el('input', { type: 'file', accept: 'image/*,video/*' });
    var current = '';
    modal.appendChild(tabs); modal.appendChild(grid);
    modal.appendChild(el('div', { 'class': 'engine-btnrow' }, [
      el('span', { 'class': 'engine-uphint', text: opts.replaceName ? 'Upload a new one' : 'Upload to this tab' }), up,
      el('button', { 'class': 'ghost', text: 'Cancel', on: { click: function () { overlay.remove(); } } })
    ]));

    function loadTab(tp) {
      current = tp;
      Array.prototype.forEach.call(tabs.children, function (b) { b.classList.toggle('on', b.__p === tp); });
      grid.textContent = 'Loading…';
      fetchJson('/api/media?prefix=' + encodeURIComponent(tp)).then(function (r) {
        grid.innerHTML = '';
        var items = (r.items || []);
        if (tp === '') items = items.filter(function (it) { return it.name.indexOf('/') === -1; }); // root files only
        if (!items.length) { grid.appendChild(el('div', { text: 'Nothing in this tab yet — upload below.' })); return; }
        items.forEach(function (it) {
          grid.appendChild(el('div', { 'class': 'engine-tile', on: { click: function () { pick(it); } } }, [
            el('img', { src: it.url, loading: 'lazy' }),
            el('div', { text: it.name.split('/').pop() })
          ]));
        });
      }).catch(function (e) { grid.innerHTML = ''; grid.appendChild(el('div', { text: 'List failed: ' + e.message })); });
    }

    MEDIA_TABS.forEach(function (t) {
      var b = el('button', { 'class': 'engine-tab', text: t.label, on: { click: function () { loadTab(t.prefix); } } });
      b.__p = t.prefix; tabs.appendChild(b);
    });
    up.addEventListener('change', function () {
      if (!up.files[0]) return;
      var target = opts.replaceName ? opts.replaceName.replace(/\/[^/]+$/, '') : (current || '').replace(/\/$/, '');
      doUpload(up.files[0], target, overlay, onChoose, opts);
    });

    var startMatch = MEDIA_TABS.filter(function (t) { return t.prefix === (prefix || ''); })[0];
    loadTab(startMatch ? startMatch.prefix : (prefix || ''));
  }

  // -------------------------------------------------------------- list panel
  // Turn a pasted share link (or full <iframe> embed code) into an embeddable URL,
  // so a non-technical user just pastes the normal Spotify/YouTube/Vimeo link. The
  // type is inferred from the URL (track/album/playlist; video/playlist) — nothing
  // to choose. Returns the input unchanged if it isn't a recognised link.
  function toEmbedUrl(input) {
    var s = String(input == null ? '' : input).trim();
    if (!s) return '';
    var iframe = s.match(/<iframe[^>]*\ssrc=["']([^"']+)["']/i);
    if (iframe) s = iframe[1].trim();
    if (/(open\.spotify\.com\/embed\/|youtube(?:-nocookie)?\.com\/embed\/|player\.vimeo\.com\/video\/)/i.test(s)) return s;
    var m;
    if ((m = s.match(/open\.spotify\.com\/(?:intl-[a-z]+\/)?(track|album|playlist|artist|episode|show)\/([A-Za-z0-9]+)/i))) return 'https://open.spotify.com/embed/' + m[1].toLowerCase() + '/' + m[2];
    if ((m = s.match(/spotify:(track|album|playlist|artist|episode|show):([A-Za-z0-9]+)/i))) return 'https://open.spotify.com/embed/' + m[1].toLowerCase() + '/' + m[2];
    if ((m = s.match(/(?:youtube(?:-nocookie)?\.com\/(?:watch\?(?:[^#]*&)?v=|shorts\/|live\/|embed\/)|youtu\.be\/)([A-Za-z0-9_-]{6,})/i))) {
      var list = s.match(/[?&]list=([A-Za-z0-9_-]+)/);
      return 'https://www.youtube.com/embed/' + m[1] + (list ? '?list=' + list[1] : '');
    }
    if ((m = s.match(/youtube\.com\/playlist\?(?:[^#]*&)?list=([A-Za-z0-9_-]+)/i))) return 'https://www.youtube.com/embed/videoseries?list=' + m[1];
    if ((m = s.match(/vimeo\.com\/(?:video\/)?(\d+)/i))) return 'https://player.vimeo.com/video/' + m[1];
    return s;
  }
  function openListPanel(container, path, label) {
    var fk = fileKeyOf(container);
    var model = state.models[fk];
    var schema = schemaFor(path, fk);
    var arr = getByPath(model, path);
    if (!Array.isArray(arr)) { toast('Not a list: ' + path); return; }
    var working = JSON.parse(JSON.stringify(arr)); // edit a copy; apply on save
    var dragIdx = null;
    // Parallel "original index" tracker so per-item text styles (block.textStyles,
    // keyed by index) follow their paragraph through reorder/delete on Apply. It
    // only feeds remapTextStyles() below — it never touches `working`, so a slip
    // here can only misalign styles (today's behaviour), never corrupt the list.
    var origIdx = working.map(function (_, i) { return i; });
    function remapTextStyles() {
      if (!schema.stringArray) return;
      var bn = container.closest && container.closest('[data-engine-path]');
      var bp = bn && bn.getAttribute('data-engine-path');
      if (!bp || path.indexOf(bp) !== 0) return;
      var blk = getByPath(model, bp);
      if (!blk || !blk.textStyles) return;
      var prefix = path.slice(bp.length) + '['; // e.g. ".description["
      var next = {};
      Object.keys(blk.textStyles).forEach(function (k) { if (k.indexOf(prefix) !== 0) next[k] = blk.textStyles[k]; });
      origIdx.forEach(function (o, i) {
        if (o < 0) return;
        var oldKey = prefix + o + ']';
        if (blk.textStyles[oldKey] != null) next[prefix + i + ']'] = blk.textStyles[oldKey];
      });
      blk.textStyles = next;
    }

    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    var applyBtn = el('button', { text: 'Apply & Save', on: { click: function () {
      remapTextStyles();
      setByPath(model, path, working);
      markDirty(fk);
      overlay.remove();
      save(true);
    } } });
    var cancelBtn = el('button', { 'class': 'ghost', text: 'Cancel', on: { click: function () { overlay.remove(); } } });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Edit ' + label }), helpBtn('lists'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      cancelBtn, applyBtn
    ]));
    var listWrap = el('div');
    panel.appendChild(listWrap);

    function blankItem() {
      if (schema.stringArray) return '';
      var o = {}; (schema.fields || []).forEach(function (f) { o[f.key] = f.type === 'bool' ? false : (f.type === 'csv' ? [] : (f.type === 'select' ? ((f.options && f.options[0]) || '') : '')); }); return o;
    }
    function render() {
      listWrap.innerHTML = '';
      working.forEach(function (item, idx) {
        var box = el('div', { 'class': 'engine-item' });
        box.addEventListener('dragover', function (e) { e.preventDefault(); box.classList.add('engine-dragover'); });
        box.addEventListener('dragleave', function () { box.classList.remove('engine-dragover'); });
        box.addEventListener('drop', function (e) {
          e.preventDefault(); box.classList.remove('engine-dragover');
          if (dragIdx != null && dragIdx !== idx) { working.splice(idx, 0, working.splice(dragIdx, 1)[0]); origIdx.splice(idx, 0, origIdx.splice(dragIdx, 1)[0]); dragIdx = null; render(); }
        });
        var handle = el('span', { 'class': 'engine-drag', text: 'Drag', title: 'Drag this item to reorder' });
        // The whole box drags from any non-editable spot (not just the handle).
        box.addEventListener('mousedown', function (e) {
          box.draggable = !(e.target.closest && e.target.closest('input,textarea,select,button'));
        });
        box.addEventListener('dragstart', function (e) { dragIdx = idx; try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', String(idx)); } catch (_) {} box.classList.add('engine-dragging'); });
        box.addEventListener('dragend', function () { box.classList.remove('engine-dragging'); });
        var row = el('div', { 'class': 'row' }, [
          handle,
          el('b', { text: (schema.label || 'Item') + ' ' + (idx + 1) }),
          el('span', { 'class': 'sp', style: 'flex:1' }),
          el('button', { 'class': 'engine-mini', text: 'Up', on: { click: function () { if (idx > 0) { working.splice(idx - 1, 0, working.splice(idx, 1)[0]); origIdx.splice(idx - 1, 0, origIdx.splice(idx, 1)[0]); render(); } } } }),
          el('button', { 'class': 'engine-mini', text: 'Down', on: { click: function () { if (idx < working.length - 1) { working.splice(idx + 1, 0, working.splice(idx, 1)[0]); origIdx.splice(idx + 1, 0, origIdx.splice(idx, 1)[0]); render(); } } } }),
          el('button', { 'class': 'engine-mini', text: 'Delete', on: { click: function () { working.splice(idx, 1); origIdx.splice(idx, 1); render(); } } })
        ]);
        box.appendChild(row);
        if (schema.stringArray) {
          var ta = el('textarea'); ta.value = item;
          ta.addEventListener('input', function () { working[idx] = ta.value; });
          box.appendChild(ta);
          // Per-paragraph styling (size/color/bold/italic/align) — same control as
          // standalone text. Only shown when the live text node exists to target.
          var _bn = container.closest && container.closest('[data-engine-path]');
          if (_bn) {
            var _bp = _bn.getAttribute('data-engine-path');
            var _ip = path + '[' + idx + ']';
            var _suf = (_bp && _ip.indexOf(_bp) === 0) ? _ip.slice(_bp.length) : null;
            var _le = _suf ? findEditEl(_bn, _ip) : null;
            if (_le) box.appendChild(textStyleRow(fk, _bp, _suf, _le));
          }
        } else {
          (schema.fields || []).forEach(function (f) {
            box.appendChild(el('label', { text: f.label || f.key }));
            if (f.type === 'textarea') {
              var ta2 = el('textarea'); ta2.value = item[f.key] || '';
              ta2.addEventListener('input', function () { item[f.key] = ta2.value; });
              box.appendChild(ta2);
            } else if (f.type === 'bool') {
              var cb = el('input', { type: 'checkbox' }); cb.checked = !!item[f.key];
              cb.addEventListener('change', function () { item[f.key] = cb.checked; });
              box.appendChild(cb);
            } else if (f.type === 'image') {
              var prev = el('img', { src: item[f.key] ? mediaUrl(item[f.key]) : '', style: 'height:48px;border-radius:5px;vertical-align:middle' });
              var pick = el('button', { 'class': 'engine-mini', text: 'Choose / upload', on: { click: function () {
                openMediaPicker(f.prefix || prefixOf(item[f.key]), function (chosen) { item[f.key] = chosen; prev.src = mediaUrl(chosen); });
              } } });
              box.appendChild(el('div', { 'class': 'row' }, [prev, pick]));
            } else if (f.type === 'csv') {
              var tac = el('textarea'); tac.value = Array.isArray(item[f.key]) ? item[f.key].join(', ') : (item[f.key] || '');
              tac.addEventListener('input', function () { item[f.key] = tac.value.split(/\s*,\s*/).map(function (s) { return s.trim(); }).filter(Boolean); });
              box.appendChild(tac);
            } else if (f.type === 'select') {
              var sel = el('select');
              (f.options || []).forEach(function (o) { var op = el('option', { value: o, text: o }); if (item[f.key] === o) op.selected = true; sel.appendChild(op); });
              sel.addEventListener('change', function () { item[f.key] = sel.value; });
              box.appendChild(sel);
            } else if (f.type === 'embed') {
              box.appendChild(el('div', { style: 'font:12px var(--engine-ui,system-ui);color:#6a5c40;margin:2px 0 4px', text: 'Paste the normal Spotify, YouTube, or Vimeo share link — it becomes an embed automatically.' }));
              var einp = el('input', { type: 'text', placeholder: 'https://open.spotify.com/…   or   https://youtu.be/…' });
              einp.value = item[f.key] || '';
              var estatus = el('div', { style: 'font:11px var(--engine-mono,monospace);margin:3px 0 4px;min-height:14px' });
              var ekey = f.key;
              var refreshEmbed = function (rewrite) {
                var conv = toEmbedUrl(einp.value);
                item[ekey] = conv;
                if (rewrite && conv) einp.value = conv;
                if (conv && /\/embed\/|player\.vimeo\.com\/video\//.test(conv)) { estatus.textContent = '✓ embed ready'; estatus.style.color = '#2e7d32'; }
                else if (conv) { estatus.textContent = 'kept as typed (not a recognised link)'; estatus.style.color = '#a9742a'; }
                else { estatus.textContent = ''; }
              };
              einp.addEventListener('input', function () { refreshEmbed(false); });
              einp.addEventListener('blur', function () { refreshEmbed(true); });
              refreshEmbed(false);
              box.appendChild(einp); box.appendChild(estatus);
            } else if (f.type === 'color') {
              var ckey = f.key;
              // Show the item's CURRENT colour in the swatch (its live --c, e.g. the
              // auto palette colour) instead of a confusing black, so it's obvious
              // what you're changing.
              var liveC = function () { try { var l = container.children[idx]; var v = (getComputedStyle(l).getPropertyValue('--c') || '').trim(); return /^#[0-9a-f]{6}$/i.test(v) ? v : ''; } catch (_) { return ''; } };
              var crow = el('div', { style: 'display:flex;align-items:center;gap:8px' });
              var cin = el('input', { type: 'color', style: 'width:46px;height:32px;padding:0;border:1px solid #ddcfb1;border-radius:6px;background:#fff;cursor:pointer' });
              var cv = item[ckey] || liveC(); if (cv) cin.value = cv;
              cin.addEventListener('input', function () { item[ckey] = cin.value; });
              crow.appendChild(cin);
              crow.appendChild(el('button', { 'class': 'engine-mini', type: 'button', text: 'Auto', title: 'Use the automatic colour', on: { click: function () { delete item[ckey]; cin.value = liveC() || '#000000'; } } }));
              box.appendChild(crow);
            } else {
              var inp = el('input', { type: f.type === 'url' ? 'url' : 'text' }); inp.value = item[f.key] || '';
              inp.addEventListener('input', function () { item[f.key] = inp.value; });
              box.appendChild(inp);
            }
          });
        }
        listWrap.appendChild(box);
      });
    }
    render();

    panel.appendChild(el('button', { 'class': 'engine-mini', text: '+ Add ' + (schema.label || 'item'), style: 'margin-top:6px', on: { click: function () { working.push(blankItem()); origIdx.push(-1); render(); } } }));
    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }

  // --------------------------------------------------------------- save / publish
  function fetchJson(url, opts) {
    return fetch(url, opts).then(function (r) {
      return r.text().then(function (t) {
        var j; try { j = t ? JSON.parse(t) : {}; } catch (_) { j = { raw: t }; }
        if (!r.ok) throw new Error((j && j.error) || ('HTTP ' + r.status));
        return j;
      });
    });
  }
  // Give every questionnaire question a stable `name` (its data key) the first time
  // it's saved, so renaming a question's label later doesn't split its CSV column
  // across response versions. Existing questions keep their names; only new ones
  // (added in the editor without a name) get one frozen from their first label.
  function freezeQuestionnaireNames() {
    var gm = state.models.global;
    if (!gm || !gm.questionnaire || !Array.isArray(gm.questionnaire.fields)) return;
    var used = {};
    gm.questionnaire.fields.forEach(function (f) {
      if (!f) return;
      if (!f.name) { var base = slug(f.label || 'q') || 'q', nm = base, i = 2; while (used[nm]) { nm = base + '-' + (i++); } f.name = nm; }
      used[f.name] = true;
    });
  }
  function save(reloadAfter) {
    var files = Object.keys(state.dirtyFiles);
    if (!files.length) {
      if (reloadAfter) { try { sessionStorage.setItem('engine-scroll', String(window.scrollY)); } catch (_) {} setTimeout(function () { location.reload(); }, 300); }
      return;
    }
    toast('Saving…', 4000);
    freezeQuestionnaireNames();
    Promise.all(files.map(function (fk) {
      return fetchJson('/api/save-content/' + fk, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(state.models[fk]) });
    })).then(function () {
      state.dirtyFiles = {}; refreshToolbar();
      toast('Saved (' + files.join(', ') + ')');
      if (reloadAfter) {
        try { sessionStorage.setItem('engine-scroll', String(window.scrollY)); } catch (_) {}
        setTimeout(function () { location.reload(); }, 600);
      }
    }).catch(function (e) { toast('Couldn’t save your draft — check your internet connection and try again.', 6000); });
  }
  // Replace the page's CSS background image in place (the upload pipeline also
  // regenerates the -mobile.webp variant the mobile stylesheet already uses).
  function editBackground() {
    var desktop = (window.SITE && window.SITE.pageBg(state.file)) || null;
    var mobile = (window.SITE && window.SITE.pageBgMobile && window.SITE.pageBgMobile(state.file)) || null;
    // Engine (/p/*) pages keep their background in their own theme instead of a
    // fixed CSS blob — editable the same way, stored as theme.bgImage(/Mobile).
    var themed = !desktop && state.data && Array.isArray(state.data.sections);
    if (!desktop && !themed) { toast('This page has no editable background.'); return; }

    // Replace-in-place for the fixed CSS blobs (the page CSS references the name).
    function replaceFixed(blob) {
      openMediaPicker(prefixOf('/media-content/' + blob), function () {}, {
        replaceName: blob,
        onReplaced: function (r) {
          // Same blob name → the browser would keep showing the cached old image.
          // Repaint whatever element shows it now, with a cache-busting token.
          var url = (r && r.url) ? r.url : mediaUrl('/media-content/' + blob);
          var busted = url + (url.indexOf('?') === -1 ? '?' : '&') + 'v=' + Date.now();
          var hit = applyLiveBackground(blob, busted);
          toast(hit ? 'Background updated' : 'Background saved — hard-refresh (Ctrl/Cmd-Shift-R) to see it', 4000);
        }
      });
    }
    // Engine pages: point the page theme at any picked/uploaded image.
    function chooseThemed(key) {
      if (!state.data.theme) state.data.theme = {};
      var cur = String(state.data.theme[key] || state.data.theme.bgImage || '');
      openMediaPicker(prefixOf(cur) || '', function (chosen) {
        state.data.theme[key] = chosen;
        if (window.EngineTheme) window.EngineTheme.apply(state.data.theme);
        markDirty(state.file);
        toast('Background updated — Save Changes to keep it', 4000);
      });
    }

    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var modal = el('div', { 'class': 'engine-modal', style: 'max-width:430px' });
    modal.appendChild(el('div', { 'class': 'engine-mhead' }, [ el('h3', { text: 'Page background' }), helpBtn('background') ]));
    modal.appendChild(el('p', { style: 'margin:0 0 10px;color:#6a5c40;font:14px/1.55 var(--engine-ui,system-ui)', text: 'Each page has two background images — a wide one for large screens and a taller one phones swap in.' }));
    modal.appendChild(el('button', { 'class': 'engine-listbtn', style: 'display:block;width:100%;margin:0 0 8px', text: 'Large screens (desktop / tablet)', on: { click: function () { overlay.remove(); desktop ? replaceFixed(desktop) : chooseThemed('bgImage'); } } }));
    if (mobile || themed) modal.appendChild(el('button', { 'class': 'engine-listbtn', style: 'display:block;width:100%;margin:0', text: 'Phones (small screens)', on: { click: function () { overlay.remove(); mobile ? replaceFixed(mobile) : chooseThemed('bgImageMobile'); } } }));
    overlay.appendChild(modal); document.body.appendChild(overlay);
  }
  // Repaint the element currently showing this background blob with a cache-busted
  // URL, so a replace shows immediately instead of waiting out the blob cache.
  function applyLiveBackground(blob, url) {
    var key = blob.replace(/\.[^.]+$/, '');
    var candidates = [document.body, document.documentElement];
    var main = document.querySelector('main'); if (main) candidates.push(main);
    [].push.apply(candidates, document.querySelectorAll('section, .platforms, [style*="background"]'));
    var hit = false;
    candidates.forEach(function (n) {
      if (!n) return;
      var bg = ''; try { bg = getComputedStyle(n).backgroundImage || ''; } catch (_) {}
      if (bg && bg.indexOf(key) !== -1) { n.style.backgroundImage = 'url("' + url + '")'; hit = true; }
    });
    return hit;
  }
  // Insert a new block of a chosen type into the page's sections[] at `atIndex`
  // (or the end). The type list + defaults come from the zoo registry, so the
  // picker shows whatever parts are registered — the editor hardcodes nothing.
  function openAddPicker(atIndex, listPath) {
    if (!state.data) { toast('Open a page first'); return; }
    var zoo = window.Catalog;
    if (!zoo || !zoo.list) { toast('Components are still loading — try again.'); return; }
    listPath = listPath || 'sections';
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var modal = el('div', { 'class': 'engine-modal', style: 'max-width:640px' });
    overlay.appendChild(modal); document.body.appendChild(overlay);
    modal.appendChild(el('div', { 'class': 'engine-mhead' }, [
      el('h3', { text: 'Add a section' }), helpBtn('addsection')
    ]));
    modal.appendChild(el('p', { style: 'margin:0 0 4px;color:#6a5c40;font:14px/1.55 var(--engine-ui,system-ui)', text: 'Pick a part from the library. It drops in pre-filled — edit it right on the page.' }));

    function choose(type) {
      var arr = getByPath(state.data, listPath);
      if (!Array.isArray(arr)) { arr = []; setByPath(state.data, listPath, arr); }
      var b = zoo.make(type);
      var at = (atIndex == null || atIndex > arr.length) ? arr.length : atIndex;
      arr.splice(at, 0, b);
      markDirty(state.file);
      overlay.remove();
      toast('Section added — saving…');
      save(true);
    }

    var cats = [], byCat = {};
    zoo.list().forEach(function (it) { if (!byCat[it.category]) { byCat[it.category] = []; cats.push(it.category); } byCat[it.category].push(it); });
    cats.forEach(function (cat) {
      modal.appendChild(el('div', { style: 'margin:14px 0 6px;font:700 11px var(--engine-mono,monospace);text-transform:uppercase;letter-spacing:1.3px;color:#a9742a', text: cat }));
      var grid = el('div', { 'class': 'engine-grid' });
      byCat[cat].forEach(function (it) {
        grid.appendChild(el('div', { 'class': 'engine-tile', title: it.label, on: { click: function () { choose(it.type); } } }, [
          el('div', { style: 'padding:6px 2px', text: it.label })
        ]));
      });
      modal.appendChild(grid);
    });
    modal.appendChild(el('div', { 'class': 'engine-btnrow' }, [
      el('button', { 'class': 'ghost', text: 'Cancel', on: { click: function () { overlay.remove(); } } })
    ]));
  }

  // Add a new section via the library picker (used by the page's "+ Add section"
  // button and the Sections panel).
  function addSection() { openAddPicker(null); }
  // Add a block INSIDE a group (its children list). Wired from the group part.
  function addInside(parentPath) { if (parentPath) openAddPicker(null, parentPath + '.children'); }
  // Reorder a child within a group's children[] (the Up/Down controls on the page).
  function moveChild(groupPath, idx, dir) {
    var group = getByPath(state.data, groupPath);
    if (!group || !Array.isArray(group.children)) return;
    var to = idx + dir;
    if (to < 0 || to >= group.children.length) return;
    group.children.splice(to, 0, group.children.splice(idx, 1)[0]);
    markDirty(state.file);
    rerenderBlock(groupPath);
    toast('Moved — Save to keep', 2500);
  }
  // Generic list-append: push a blank item onto a block's named list and save.
  // Wired from parts (window.EngineEdit.addItem) so a part can offer its own
  // "+ add" button without the engine knowing the part's data shape.
  function addItem(blockPath, key, blankItem, label) {
    if (!blockPath || !key) return;
    var arr = getByPath(state.data, blockPath + '.' + key);
    if (!Array.isArray(arr)) { arr = []; setByPath(state.data, blockPath + '.' + key, arr); }
    arr.push(blankItem != null ? blankItem : {});
    markDirty(state.file); toast((label || 'Item') + ' added — saving…'); save(true);
  }

  function slugify(s) { return String(s || '').toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40); }

  // Pages panel: the menu's pages, plus create/remove. A new page is just a
  // content file (page-<slug>.json) rendered by the generic page.html via the
  // zoo — created on first Save. Creating one also adds it to the site menu
  // (the editable global.json nav), so it's reachable immediately.
  function openPagesPanel() {
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    var g = state.models.global;
    var nav = (g && Array.isArray(g.nav)) ? g.nav : [];

    function saveGlobal(then) {
      fetchJson('/api/save-content/global', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(g) })
        .then(then).catch(function (e) { toast('Menu update failed: ' + e.message, 4000); });
    }
    function createPage(name) {
      var s = slugify(name);
      if (!s) { toast('Enter a page name first'); return; }
      var label = (name || s).trim();
      var file = 'page-' + s;
      var go = function () { location.href = '/p/' + s; };
      toast('Creating page…');
      // Create the (empty) page content file first so the page exists right away,
      // then add it to the menu, then open it.
      fetchJson('/api/save-content/' + file, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: label, sections: [] })
      }).then(function () {
        if (!g) { go(); return; }
        if (!Array.isArray(g.nav)) g.nav = [];
        if (!g.nav.some(function (it) { return it.url === '/p/' + s; })) {
          g.nav.push({ label: label, url: '/p/' + s });
        }
        saveGlobal(go);
      }).catch(function (e) { toast('Create failed: ' + e.message, 5000); });
    }
    // A "link" menu item: no page content, just redirects to another URL (like Merch).
    function createLink(name, url) {
      name = (name || '').trim(); url = (url || '').trim();
      if (!name || !url) { toast('Enter a name and a URL for the link'); return; }
      if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
      if (!g) { toast('Menu not loaded yet — try again in a moment'); return; }
      if (!Array.isArray(g.nav)) g.nav = [];
      g.nav.push({ label: name, url: url, external: true });
      toast('Adding link…');
      saveGlobal(function () { overlay.remove(); toast('Added link “' + name + '”'); openPagesPanel(); });
    }
    function removeNav(idx) {
      if (!g || !Array.isArray(g.nav) || !g.nav[idx]) return;
      if (!window.confirm('Remove "' + (g.nav[idx].label || g.nav[idx].url) + '" from the menu?')) return;
      g.nav.splice(idx, 1);
      toast('Removing…');
      saveGlobal(function () { overlay.remove(); toast('Removed from menu'); });
    }

    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Pages' }), helpBtn('pages'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Close', on: { click: function () { overlay.remove(); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Your menu pages, each with a visibility: Public (in the live menu) · Unlisted (reachable by link, hidden from the live menu) · Dev only (stays on your draft, never published). Create a blank page and build it from the parts library.' }));
    var input = el('input', { type: 'text', placeholder: 'New page name (e.g. Press)', style: 'width:100%;box-sizing:border-box;padding:9px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317;margin-bottom:8px' });
    input.addEventListener('keydown', function (e) { if (e.key === 'Enter') createPage(input.value); });
    panel.appendChild(input);
    panel.appendChild(el('button', { 'class': 'engine-listbtn', style: 'margin:0 0 6px', text: '+ Create page', on: { click: function () { createPage(input.value); } } }));

    // Or add a "link" page that just opens another website (like the Merch menu item).
    var linkBoxStyle = 'width:100%;box-sizing:border-box;padding:9px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317;margin-bottom:8px';
    panel.appendChild(el('div', { 'class': 'engine-navhint', style: 'margin:12px 0 6px', text: 'Or add a link that opens another website (a redirect, like Merch):' }));
    var linkName = el('input', { type: 'text', placeholder: 'Link name (e.g. Merch)', style: linkBoxStyle });
    var linkUrl = el('input', { type: 'text', placeholder: 'https://… (where it goes)', style: linkBoxStyle });
    linkUrl.addEventListener('keydown', function (e) { if (e.key === 'Enter') createLink(linkName.value, linkUrl.value); });
    panel.appendChild(linkName); panel.appendChild(linkUrl);
    panel.appendChild(el('button', { 'class': 'engine-listbtn', style: 'margin:0 0 6px', text: '+ Add link', on: { click: function () { createLink(linkName.value, linkUrl.value); } } }));

    var listWrap = el('div', { style: 'margin-top:14px' }); panel.appendChild(listWrap);
    nav.forEach(function (item, idx) {
      var visSel = el('select', { title: 'Who can see this page', style: 'padding:5px 6px;border:1px solid #ddcfb1;border-radius:7px;font:12px var(--engine-ui);background:#fffdf8;color:#2a2317' });
      [['public', 'Public'], ['unlisted', 'Unlisted'], ['dev', 'Dev only']].forEach(function (o) { var op = el('option', { value: o[0], text: o[1] }); if ((item.visibility || 'public') === o[0]) op.selected = true; visSel.appendChild(op); });
      visSel.addEventListener('change', function () { if (visSel.value === 'public') delete item.visibility; else item.visibility = visSel.value; saveGlobal(function () { toast('“' + (item.label || 'page') + '” → ' + visSel.value); }); });
      listWrap.appendChild(el('div', { 'class': 'engine-navrow' }, [
        el('b', { text: item.label || item.url || 'page' }),
        el('span', { 'class': 'engine-navfile', text: item.url || '' }),
        el('span', { 'class': 'sp', style: 'flex:1' }),
        visSel,
        el('button', { 'class': 'engine-mini', text: 'Open', on: { click: function () { if (item.url) location.href = item.url; } } }),
        el('button', { 'class': 'engine-mini', text: 'Remove', on: { click: function () { removeNav(idx); } } })
      ]));
    });
    if (!nav.length) listWrap.appendChild(el('div', { 'class': 'engine-navhint', text: 'No menu items yet.' }));

    // Built-in standalone pages (from SITE_CONFIG.pages) that aren't in the menu —
    // e.g. the EPK and the standalone Merch page. They have their own routes/shells
    // and are always reachable by link, so list them here too. This comes from the
    // shipped site config, NOT the saved menu, so it shows even if the menu (blob)
    // never referenced them.
    function normUrl(u) { u = String(u || '').toLowerCase().replace(/[#?].*$/, ''); if (u.length > 1) u = u.replace(/\/+$/, ''); return u; }
    var navUrls = {}; nav.forEach(function (it) { if (it && it.url) navUrls[normUrl(it.url)] = true; });
    var cfgPages = (window.SITE && window.SITE.cfg && window.SITE.cfg.pages) || {};
    var others = Object.keys(cfgPages).filter(function (k) {
      if (k === 'global') return false; // header/footer, edited via Theme — not a page
      var url = (k === 'index') ? '/' : '/' + k;
      return !navUrls[normUrl(url)];
    });
    if (others.length) {
      listWrap.appendChild(el('div', { 'class': 'engine-navhint', style: 'margin-top:18px;font-weight:700;color:#a9742a', text: 'Other pages (reachable by link, not in the menu)' }));
      others.forEach(function (k) {
        var url = (k === 'index') ? '/' : '/' + k;
        var name = (window.SITE && window.SITE.pageName(k)) || k;
        listWrap.appendChild(el('div', { 'class': 'engine-navrow' }, [
          el('b', { text: name }),
          el('span', { 'class': 'engine-navfile', text: url }),
          el('span', { 'class': 'sp', style: 'flex:1' }),
          el('button', { 'class': 'engine-mini', text: 'Open', on: { click: function () { location.href = url; } } }),
          el('button', { 'class': 'engine-mini', text: 'Add to menu', title: 'Add this page to the site menu', on: { click: function () {
            if (!g) { toast('Menu not loaded yet — try again in a moment'); return; }
            if (!Array.isArray(g.nav)) g.nav = [];
            if (g.nav.some(function (it) { return normUrl(it.url) === normUrl(url); })) { toast('Already in the menu'); return; }
            g.nav.push({ label: name, url: url });
            toast('Adding to menu…');
            saveGlobal(function () { overlay.remove(); toast('Added “' + name + '” to the menu'); openPagesPanel(); });
          } } })
        ]));
      });
    }
    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }

  // ---- per-object inspector (the "attached UI" for styling + simple content) ----
  function sectionLabel(t) { return el('div', { style: 'margin:16px 0 4px;font:700 11px var(--engine-mono);text-transform:uppercase;letter-spacing:1.3px;color:#a9742a', text: t }); }
  function rgbToHex(rgb) { var m = String(rgb || '').match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/); if (!m) return ''; function h(n) { n = parseInt(n, 10).toString(16); return n.length < 2 ? '0' + n : n; } return '#' + h(m[1]) + h(m[2]) + h(m[3]); }
  // Lay every side panel out as a fixed header + scrolling body, so the header
  // never overlaps the content and the body scroll doesn't chain to the page.
  function panelize(panel) {
    var head = panel.querySelector('.engine-phead');
    if (!head || panel.querySelector('.engine-pbody')) return;
    var body = el('div', { 'class': 'engine-pbody' });
    [].slice.call(panel.childNodes).forEach(function (c) { if (c !== head) body.appendChild(c); });
    panel.appendChild(body);
  }
  // A persistent right-edge "Edit section" control that always targets whichever
  // block is centred in the viewport as you scroll — so you don't have to hunt
  // for a small hover button (e.g. on the full-screen hero). Per-element hover
  // tools (Replace/Remove, Style) remain for fine-grained edits.
  var editTab = null, editTabRaf = null;
  function editTabEl() {
    if (editTab) return editTab;
    editTab = el('button', { id: 'engine-editsection', text: 'Edit section', title: 'Edit the section in view', on: { click: function () { if (editTab.__path) openSectionEditor(editTab.__path); } } });
    document.body.appendChild(editTab);
    return editTab;
  }
  function updateEditTab() {
    if (!state.editing) { if (editTab) editTab.classList.remove('show'); return; }
    // Anchor point slides top→bottom with scroll progress (instead of a fixed
    // viewport-center), so at the very top the FIRST section wins and at the
    // very bottom the LAST one does — otherwise short top/bottom sections could
    // never become "the section in view".
    var nodes = document.querySelectorAll('[data-engine-block][data-engine-path]'), cy = window.innerHeight / 2, best = null, bestD = Infinity;
    var maxScroll = document.documentElement.scrollHeight - window.innerHeight;
    if (maxScroll > 40) {
      var sp = Math.min(1, Math.max(0, window.scrollY / maxScroll));
      cy = 90 + (window.innerHeight - 140) * sp;
    }
    for (var i = 0; i < nodes.length; i++) {
      var r = nodes[i].getBoundingClientRect();
      if (r.bottom < 80 || r.top > window.innerHeight - 10) continue;
      var mid = Math.max(r.top, 0) + (Math.min(r.bottom, window.innerHeight) - Math.max(r.top, 0)) / 2;
      var d = Math.abs(mid - cy);
      if (d < bestD) { bestD = d; best = nodes[i]; }
    }
    var t = editTabEl();
    if (best) {
      var bpath = best.getAttribute('data-engine-path');
      t.__path = bpath;
      var blk = getByPath(state.data, bpath) || {};
      var def = window.CatalogBlocks && window.CatalogBlocks[blk.type];
      var nm = (blk.title && String(blk.title).trim()) || (blk.text && String(blk.text).trim()) || (def && def.label) || blk.type || 'section';
      if (nm.length > 24) nm = nm.slice(0, 23) + '…';
      t.textContent = 'Edit ' + nm;
      t.classList.add('show');
    } else t.classList.remove('show');
  }
  function scheduleEditTab() { if (editTabRaf) return; editTabRaf = requestAnimationFrame(function () { editTabRaf = null; updateEditTab(); }); }
  function inspectorField(f, obj, onChange) {
    var wrap = el('div', { style: 'margin:8px 0' });
    wrap.appendChild(el('label', { text: f.label || f.key, style: 'display:block;font:600 10.5px/1 var(--engine-ui);text-transform:uppercase;letter-spacing:.8px;color:#9a8a66;margin:0 0 4px' }));
    var cur = obj ? obj[f.key] : '';
    if (f.type === 'color') {
      var row = el('div', { style: 'display:flex;gap:8px;align-items:center' });
      var ci = el('input', { type: 'color', value: cur || f.computed || '#000000', style: 'width:42px;height:30px;padding:0;border:1px solid #ddcfb1;border-radius:6px;background:#fff' });
      var tx = el('input', { type: 'text', value: cur || '', placeholder: '(default)', style: 'flex:1;min-width:0;padding:7px 9px;border:1px solid #ddcfb1;border-radius:8px;font:13px var(--engine-ui);background:#fffdf8;color:#2a2317' });
      ci.addEventListener('input', function () { tx.value = ci.value; onChange(ci.value); });
      tx.addEventListener('input', function () { onChange(tx.value); if (/^#[0-9a-fA-F]{3,8}$/.test(tx.value)) ci.value = tx.value; });
      var clr = el('button', { 'class': 'engine-mini', text: 'Clear', on: { click: function () { tx.value = ''; onChange(''); } } });
      row.appendChild(ci); row.appendChild(tx); row.appendChild(clr); wrap.appendChild(row);
    } else if (f.type === 'select') {
      var sel = el('select', { style: 'width:100%;padding:8px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317' });
      (f.options || []).forEach(function (o) { var val = Array.isArray(o) ? o[0] : o; var lab = Array.isArray(o) ? o[1] : o; var op = el('option', { value: val, text: lab }); if ((cur || '') === val) op.selected = true; sel.appendChild(op); });
      sel.addEventListener('change', function () { onChange(sel.value); });
      wrap.appendChild(sel);
    } else if (f.type === 'textarea') {
      var ta2 = el('textarea', { style: 'width:100%;box-sizing:border-box;min-height:90px;padding:8px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317;resize:vertical;line-height:1.45' });
      ta2.value = cur || '';
      ta2.addEventListener('change', function () { onChange(ta2.value); });
      wrap.appendChild(ta2);
    } else if (f.type === 'image') {
      var prow = el('div', { style: 'display:flex;gap:8px;align-items:center' });
      var pv = el('img', { src: cur ? mediaUrl(cur) : '', style: 'width:48px;height:34px;object-fit:cover;border-radius:6px;border:1px solid #ddcfb1;background:repeating-conic-gradient(#cdbf9e 0 25%,#f3ead4 0 50%) 0/10px 10px' });
      var pickB = el('button', { 'class': 'engine-mini', text: cur ? 'Change' : 'Choose / upload', on: { click: function () { openMediaPicker(f.prefix || prefixOf(cur) || '', function (chosen) { onChange(chosen); pv.src = mediaUrl(chosen); pickB.textContent = 'Change'; }); } } });
      var clrB = el('button', { 'class': 'engine-mini', text: 'Clear', on: { click: function () { onChange(''); pv.removeAttribute('src'); pickB.textContent = 'Choose / upload'; } } });
      prow.appendChild(pv); prow.appendChild(pickB); prow.appendChild(clrB); wrap.appendChild(prow);
    } else if (f.type === 'number') {
      var ni = el('input', { type: 'number', value: cur || '', placeholder: f.placeholder || '', style: 'width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317' });
      ni.addEventListener('input', function () { onChange(ni.value); });
      wrap.appendChild(ni);
    } else {
      var inp = el('input', { type: f.type === 'url' ? 'url' : 'text', value: cur || '', style: 'width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317' });
      inp.addEventListener('change', function () { onChange(inp.value); });
      wrap.appendChild(inp);
    }
    return wrap;
  }
  function camelToWords(s) { return String(s || '').replace(/([A-Z])/g, ' $1').replace(/^./, function (c) { return c.toUpperCase(); }).trim(); }
  function lastSeg(path) { var m = String(path).match(/([a-zA-Z_]\w*)\s*$/); return m ? m[1] : path; }
  // Re-render one block in place (after a content edit) so changes show live.
  function rerenderBlock(path) {
    var n = document.querySelector('[data-engine-path="' + path + '"]');
    if (!n || !n.parentNode || !window.Catalog) return;
    var block = getByPath(state.data, path);
    if (!block) return;
    var im = path.match(/\[(\d+)\][^\[]*$/); var idx = im ? parseInt(im[1], 10) : 0;
    var fresh = window.Catalog.render(block, { el: el, mediaUrl: mediaUrl, file: fileKeyOf(n) || state.file, index: idx, path: path });
    n.parentNode.replaceChild(fresh, n);
    bindAll();
  }
  // Find the live editable element for a given data-path inside a block node
  // (matches data-edit-text or data-edit-html exactly).
  function findEditEl(root, dataPath) {
    if (!root) return null;
    var all = root.querySelectorAll('[data-edit-text],[data-edit-html]');
    for (var i = 0; i < all.length; i++) {
      if ((all[i].getAttribute('data-edit-text') || all[i].getAttribute('data-edit-html')) === dataPath) return all[i];
    }
    return null;
  }
  // A compact, per-ELEMENT text-style control strip (size / color / bold / italic /
  // align). Stored on the owning block as block.textStyles[suffix] (suffix = the
  // text node's data-path relative to the block) and applied live + on every render
  // (style.js) so it persists on the published site. Used by the section editor and
  // the list panel — so every heading, paragraph, label, etc. is individually styleable.
  function textStyleRow(fk, blockPath, suffix, liveEl) {
    var model = state.models[fk];
    var block = (blockPath != null) ? getByPath(model, blockPath) : null;
    if (!block) return el('span', { style: 'display:none' });
    // Don't create block.textStyles until a value is actually set, so merely
    // opening the editor doesn't litter saved JSON with empty {} objects.
    var ts = (block.textStyles && block.textStyles[suffix]) || {};
    function commit() {
      var empty = (ts.font == null || ts.font === '') && !ts.color && !ts.bold && !ts.italic && !ts.align;
      if (empty) { if (block.textStyles) delete block.textStyles[suffix]; }
      else { if (!block.textStyles) block.textStyles = {}; block.textStyles[suffix] = ts; }
      if (liveEl && window.EngineStyle && window.EngineStyle.applyText) window.EngineStyle.applyText(liveEl, ts);
      markDirty(fk);
    }
    function lbl(t) { return el('span', { text: t, style: 'font:600 10px var(--engine-ui);text-transform:uppercase;letter-spacing:.6px;color:#9a8a66' }); }
    var size = el('input', { type: 'number', min: '8', max: '200', placeholder: 'auto', style: 'width:64px;padding:5px 6px;border:1px solid #ddcfb1;border-radius:6px;font:13px var(--engine-ui);background:#fffdf8;color:#2a2317' });
    if (ts.font != null && ts.font !== '') size.value = ts.font;
    size.addEventListener('input', function () { var v = String(size.value).trim(); if (v === '') delete ts.font; else ts.font = parseFloat(v); commit(); });
    var color = el('input', { type: 'color', title: 'Text color', style: 'width:34px;height:30px;padding:0;border:1px solid #ddcfb1;border-radius:6px;background:#fff;cursor:pointer' });
    if (ts.color) color.value = ts.color;
    color.addEventListener('input', function () { ts.color = color.value; commit(); });
    var colorAuto = el('button', { 'class': 'engine-mini', type: 'button', text: 'Auto', title: 'Use the default color', on: { click: function () { delete ts.color; commit(); } } });
    function toggle(label, key, css) {
      var b = el('button', { 'class': 'engine-mini', type: 'button', text: label, title: label, style: css });
      function paint() { b.style.background = ts[key] ? 'var(--engine-amber)' : ''; b.style.color = ts[key] ? '#241a08' : ''; }
      paint();
      b.addEventListener('click', function () { if (ts[key]) delete ts[key]; else ts[key] = true; paint(); commit(); });
      return b;
    }
    var align = el('select', { title: 'Align', style: 'padding:5px 6px;border:1px solid #ddcfb1;border-radius:6px;font:12px var(--engine-ui);background:#fffdf8;color:#2a2317' });
    [['', 'Align'], ['left', 'Left'], ['center', 'Center'], ['right', 'Right']].forEach(function (o) { var op = el('option', { value: o[0], text: o[1] }); if ((ts.align || '') === o[0]) op.selected = true; align.appendChild(op); });
    align.addEventListener('change', function () { if (align.value) ts.align = align.value; else delete ts.align; commit(); });
    return el('div', { style: 'display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin:4px 0 2px' }, [
      lbl('Size'), size, lbl('px'), el('span', { style: 'width:6px' }),
      color, colorAuto, toggle('B', 'bold', 'font-weight:800;min-width:30px'), toggle('I', 'italic', 'font-style:italic;min-width:30px'), align
    ]);
  }
  // Attached settings for one block: its content fields (if the part declares
  // any) plus its style. Edits apply live; the toolbar Save persists them.
  function openInspector(path) {
    var block = getByPath(state.data, path);
    if (!block) { toast('Could not find that section.'); return; }
    var def = window.CatalogBlocks && window.CatalogBlocks[block.type];
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: (def && def.label ? def.label : 'Section') + ' settings' }), helpBtn('style'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Done', on: { click: function () { overlay.remove(); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Edits apply live. Use Save Changes in the toolbar to keep them.' }));
    if (def && def.fields && def.fields.length) {
      panel.appendChild(sectionLabel('Content'));
      def.fields.forEach(function (f) {
        panel.appendChild(inspectorField(f, block, function (v) {
          if (v === '' || v == null) delete block[f.key]; else block[f.key] = v;
          markDirty(state.file); rerenderBlock(path);
        }));
      });
    }
    panel.appendChild(sectionLabel('Style'));
    if (!block.style) block.style = {};
    // Pre-fill the colour swatches with the block's ACTUAL current colours (read
    // from the live page) so the inspector reflects reality, not a blank #000.
    var node0 = document.querySelector('[data-engine-path="' + path + '"]');
    var cs = node0 ? getComputedStyle(node0) : null;
    var comp = cs ? { color: rgbToHex(cs.color), bg: rgbToHex(cs.backgroundColor) } : {};
    (window.EngineStyle ? window.EngineStyle.fields : []).forEach(function (f) {
      var ff = (f.type === 'color' && comp[f.key]) ? Object.assign({}, f, { computed: comp[f.key] }) : f;
      panel.appendChild(inspectorField(ff, block.style, function (v) {
        if (v === '' || v == null) delete block.style[f.key]; else block.style[f.key] = v;
        var nn = document.querySelector('[data-engine-path="' + path + '"]');
        if (nn && window.EngineStyle) window.EngineStyle.apply(nn, block.style);
        markDirty(state.file);
      }));
    });
    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }
  // THE one editor per section: edits its heading, all its text, its media, its
  // lists, and its style — in a single panel. This is what every "Edit" entry
  // (the right-side tab, the Sections panel, clicking a section's media) opens,
  // so there isn't a different little button for every element.
  function openSectionEditor(path) {
    var node = document.querySelector('[data-engine-path="' + path + '"]');
    var block = getByPath(state.data, path);
    if (!block || !node) { toast('Show this section first to edit it.'); return; }
    var def = window.CatalogBlocks && window.CatalogBlocks[block.type];
    var fk = fileKeyOf(node) || state.file;
    var model = state.models[fk];
    var name = (block.title && String(block.title).trim()) || (def && def.label) || block.type;
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Edit ' + name }), helpBtn('editsection'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Done', on: { click: function () { overlay.remove(); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Everything in this section, in one place. Edits apply live; use Save Changes to keep them.' }));

    function inList(n) { var l = n.closest('[data-edit-list]'); return l && node.contains(l) && l !== n; }

    // ---- text (headings, body) not belonging to a list item ----
    var texts = [].slice.call(node.querySelectorAll('[data-edit-text],[data-edit-html]')).filter(function (n) { return !inList(n); });
    if (texts.length) {
      panel.appendChild(sectionLabel('Text'));
      texts.forEach(function (tn) {
        var isHtml = tn.hasAttribute('data-edit-html');
        var fpath = tn.getAttribute(isHtml ? 'data-edit-html' : 'data-edit-text');
        panel.appendChild(inspectorField({ key: 'v', label: camelToWords(lastSeg(fpath)), type: isHtml ? 'textarea' : 'text' }, { v: isHtml ? tn.innerHTML : tn.textContent }, function (val) {
          setByPath(model, fpath, val);
          if (isHtml) tn.innerHTML = val; else tn.textContent = val;
          markDirty(fk);
        }));
        var suffix = (path && fpath.indexOf(path) === 0) ? fpath.slice(path.length) : null;
        if (suffix) panel.appendChild(textStyleRow(fk, path, suffix, tn));
      });
    }

    // ---- standalone media (not list items) ----
    var media = [].slice.call(node.querySelectorAll('[data-edit-image]')).filter(function (n) { return !inList(n); });
    if (media.length) {
      panel.appendChild(sectionLabel('Media'));
      media.forEach(function (mn) {
        var fpath = mn.getAttribute('data-edit-image');
        var noun = mn.getAttribute('data-edit-label') || (mn.tagName === 'VIDEO' ? 'video' : 'image');
        var prev = el('img', { src: getByPath(model, fpath) ? mediaUrl(getByPath(model, fpath)) : '', style: 'height:54px;border-radius:6px;background:repeating-conic-gradient(#cdbf9e 0% 25%,#f3ead4 0% 50%) 0/12px 12px' });
        var pick = el('button', { 'class': 'engine-mini', text: 'Choose / upload', on: { click: function () {
          openMediaPicker(mn.getAttribute('data-edit-prefix') || prefixOf(getByPath(model, fpath)), function (chosen) {
            setByPath(model, fpath, chosen); applyMediaSrc(mn, chosen); prev.src = mediaUrl(chosen); markDirty(fk);
          });
        } } });
        var row = el('div', { style: 'margin:8px 0' }, [
          el('label', { text: camelToWords(noun), style: 'display:block;font:600 10.5px/1 var(--engine-ui);text-transform:uppercase;letter-spacing:.8px;color:#9a8a66;margin:0 0 4px' }),
          el('div', { style: 'display:flex;gap:8px;align-items:center' }, [prev, pick])
        ]);
        panel.appendChild(row);
      });
    }

    // ---- lists (galleries, videos, links, quotes, slides, shows…) ----
    var lists = [].slice.call(node.querySelectorAll('[data-edit-list]'));
    // The list can live on the section ROOT (a list-only part) — querySelectorAll
    // only finds descendants, so add the node itself when it carries the attribute.
    if (node.matches && node.matches('[data-edit-list]')) lists.unshift(node);
    lists = lists.filter(function (n) {
      var p = n.parentElement && n.parentElement.closest('[data-edit-list]');
      return !(p && node.contains(p));
    });
    if (lists.length) {
      panel.appendChild(sectionLabel('Lists'));
      lists.forEach(function (ln) {
        var lpath = ln.getAttribute('data-edit-list');
        var llabel = ln.getAttribute('data-edit-list-label') || 'items';
        var arr = getByPath(model, lpath); var n = Array.isArray(arr) ? arr.length : 0;
        panel.appendChild(el('button', { 'class': 'engine-listbtn', style: 'margin:6px 8px 6px 0', text: 'Edit ' + llabel + ' (' + n + ')', on: { click: function () { openListPanel(ln, lpath, llabel); } } }));
      });
    }

    // ---- the part's own settings (colors, selects, urls…) that aren't already
    // shown as on-page text/media above — e.g. the spotlight's number colors.
    if (def && def.fields && def.fields.length) {
      var covered = {};
      texts.forEach(function (tn) { covered[lastSeg(tn.getAttribute('data-edit-text') || tn.getAttribute('data-edit-html') || '')] = 1; });
      media.forEach(function (mn) { covered[lastSeg(mn.getAttribute('data-edit-image') || '')] = 1; });
      var extras = def.fields.filter(function (f) { return !covered[f.key]; });
      if (extras.length) {
        panel.appendChild(sectionLabel('Settings'));
        extras.forEach(function (f) {
          panel.appendChild(inspectorField(f, block, function (v) {
            if (v === '' || v == null) delete block[f.key]; else block[f.key] = v;
            markDirty(fk); rerenderBlock(path);
          }));
        });
      }
    }

    // ---- style ----
    panel.appendChild(sectionLabel('Style'));
    if (!block.style) block.style = {};
    var cs = getComputedStyle(node);
    var comp = { color: rgbToHex(cs.color), bg: rgbToHex(cs.backgroundColor) };
    var isGridish = /grid|flex/.test(cs.display);
    var BASIC = { font: 1, color: 1, bg: 1, bgImage: 1 };
    function styleField(f) {
      if (f.key === 'gap' && !isGridish) return null; // grid spacing is inert on non-grid blocks
      var ff = (f.type === 'color' && comp[f.key]) ? Object.assign({}, f, { computed: comp[f.key] }) : f;
      return inspectorField(ff, block.style, function (v) {
        if (v === '' || v == null) delete block.style[f.key]; else block.style[f.key] = v;
        var nn = document.querySelector('[data-engine-path="' + path + '"]');
        if (nn && window.EngineStyle) window.EngineStyle.apply(nn, block.style);
        markDirty(fk);
      });
    }
    var sfields = (window.EngineStyle ? window.EngineStyle.fields : []);
    sfields.filter(function (f) { return BASIC[f.key]; }).forEach(function (f) { var w = styleField(f); if (w) panel.appendChild(w); });
    // Progressive disclosure — keep advanced controls tucked away so the panel isn't overwhelming.
    var adv = el('div', { style: 'display:none' });
    var moreBtn = el('button', { 'class': 'engine-mini', style: 'margin:8px 0', text: 'More style options', on: { click: function () { var show = adv.style.display === 'none'; adv.style.display = show ? 'block' : 'none'; moreBtn.textContent = show ? 'Fewer style options' : 'More style options'; } } });
    panel.appendChild(moreBtn); panel.appendChild(adv);
    sfields.filter(function (f) { return !BASIC[f.key]; }).forEach(function (f) { var w = styleField(f); if (w) adv.appendChild(w); });

    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }

  // Site-wide theme colors (CSS variables in global.json). Applies live.
  function qInputStyle() { return 'width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #ddcfb1;border-radius:8px;font:14px var(--engine-ui);background:#fffdf8;color:#2a2317;margin-bottom:6px'; }
  // Edit the signup survey (questions + intro), stored in global.json.questionnaire.
  function openQuestionnairePanel() {
    var g = state.models.global;
    if (!g) { toast('Global data not loaded yet — try again in a moment'); return; }
    if (!g.questionnaire) g.questionnaire = { confirm: { heading: '', body: '' }, submitLabel: 'Submit', fields: [] };
    var q = g.questionnaire;
    if (!q.confirm) q.confirm = { heading: '', body: '' };
    if (!Array.isArray(q.fields)) q.fields = [];

    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Questionnaire' }), helpBtn('questionnaire'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Done', on: { click: function () { overlay.remove(); save(true); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'The signup survey shown after a fan enters their email. Edit the intro and questions — changes save when you click Done.' }));

    panel.appendChild(sectionLabel('Intro heading'));
    var hIn = el('input', { type: 'text', style: qInputStyle() }); hIn.value = q.confirm.heading || '';
    hIn.addEventListener('input', function () { q.confirm.heading = hIn.value; markDirty('global'); });
    panel.appendChild(hIn);

    panel.appendChild(sectionLabel('Intro text'));
    var bIn = el('textarea', { style: qInputStyle() + 'min-height:84px' }); bIn.value = q.confirm.body || '';
    bIn.addEventListener('input', function () { q.confirm.body = bIn.value; markDirty('global'); });
    panel.appendChild(bIn);

    panel.appendChild(sectionLabel('Submit button label'));
    var sIn = el('input', { type: 'text', style: qInputStyle() }); sIn.value = q.submitLabel || 'Submit';
    sIn.addEventListener('input', function () { q.submitLabel = sIn.value; markDirty('global'); });
    panel.appendChild(sIn);

    panel.appendChild(sectionLabel('Questions'));
    panel.appendChild(el('button', { 'class': 'engine-listbtn', text: 'Edit questions (' + q.fields.length + ')', on: { click: function () {
      var ghost = el('div'); ghost.setAttribute('data-edit-file', 'global');
      overlay.remove();
      openListPanel(ghost, 'questionnaire.fields', 'questions');
    } } }));

    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }
  // Edit analytics/pixel IDs (global.json.tracking). They only run on the live site.
  function openTrackingPanel() {
    var g = state.models.global;
    if (!g) { toast('Site settings not loaded yet — try again in a moment'); return; }
    if (!g.tracking) g.tracking = {};
    var t = g.tracking;
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Tracking & analytics' }), helpBtn('tracking'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Done', on: { click: function () { overlay.remove(); save(true); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Paste your IDs to turn on analytics. They run only on the live published site — never here in the editor. Leave a box blank to keep that service off.' }));
    function field(label, key, ph) {
      panel.appendChild(sectionLabel(label));
      var inp = el('input', { type: 'text', style: qInputStyle(), placeholder: ph }); inp.value = t[key] || '';
      inp.addEventListener('input', function () { t[key] = inp.value.trim(); markDirty('global'); });
      panel.appendChild(inp);
    }
    field('Meta (Facebook) Pixel ID', 'metaPixelId', 'e.g. 123456789012345');
    field('Google Analytics measurement ID', 'gaMeasurementId', 'e.g. G-XXXXXXXXXX');
    panel.appendChild(el('div', { 'class': 'engine-navhint', style: 'margin-top:14px', text: 'Want another service (TikTok, Plausible, etc.)? It’s one ID here + a few lines in global.js → initTracking.' }));
    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }
  function openThemePanel() {
    var g = state.models.global;
    if (!g) { toast('Theme loads with the site menu — try again in a moment.'); return; }
    if (!g.theme) g.theme = {};
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });
    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Theme' }), helpBtn('theme'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Done', on: { click: function () { overlay.remove(); } } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Site-wide colors used across every page. Applies live; Save Changes to keep. Give one section its own look with its Style panel.' }));
    [{ key: 'accent', type: 'color', label: 'Accent' }, { key: 'text', type: 'color', label: 'Text color' }, { key: 'bg', type: 'color', label: 'Page background' }].forEach(function (f) {
      panel.appendChild(inspectorField(f, g.theme, function (v) {
        if (v === '' || v == null) delete g.theme[f.key]; else g.theme[f.key] = v;
        if (window.EngineTheme) window.EngineTheme.apply(g.theme);
        markDirty('global');
      }));
    });

    // ---- Chart & stat colors (the data-viz palette --cat-1..6 that paints stat
    // numbers, chart bars and ranking rows) + the spotlight number gradient.
    // Same theme object, so Save Changes persists them like the colors above.
    var PAL_DEFAULT = ['#ff3b8e', '#00e5ff', '#ffd23f', '#a06bff', '#5af78e', '#ff8c42'];
    function catColor(i) {
      try { var v = getComputedStyle(document.documentElement).getPropertyValue('--cat-' + (i + 1)).trim(); if (/^#[0-9a-f]{6}$/i.test(v)) return v; } catch (_) {}
      return PAL_DEFAULT[i];
    }
    function rerenderPage() {
      if (window.EngineTheme) window.EngineTheme.apply(g.theme);
      // Stat/chart blocks bake palette colors in at render time → re-render the page's sections.
      document.dispatchEvent(new CustomEvent('engine-data-ready', { detail: { file: state.file, data: state.data } }));
    }
    var swatchStyle = 'width:42px;height:30px;padding:0;border:1px solid #ddcfb1;border-radius:6px;background:#fff;cursor:pointer';

    panel.appendChild(sectionLabel('Chart & stat colors'));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'The six colors that paint stat numbers, chart bars and ranking rows (used in order, repeating).' }));
    var palRow = el('div', { style: 'display:flex;gap:8px;flex-wrap:wrap;align-items:center' });
    PAL_DEFAULT.forEach(function (_, i) {
      var cin = el('input', { type: 'color', value: catColor(i), title: 'Color ' + (i + 1), style: swatchStyle });
      cin.addEventListener('input', function () {
        if (!Array.isArray(g.theme.palette) || g.theme.palette.length < PAL_DEFAULT.length) {
          g.theme.palette = PAL_DEFAULT.map(function (_2, j) { return catColor(j); }); // materialize so one edit doesn't lose the rest
        }
        g.theme.palette[i] = cin.value;
        markDirty('global'); rerenderPage();
      });
      palRow.appendChild(cin);
    });
    palRow.appendChild(el('button', { 'class': 'engine-mini', type: 'button', text: 'Auto', title: 'Back to the automatic palette', on: { click: function () {
      delete g.theme.palette;
      for (var i = 1; i <= 8; i++) document.documentElement.style.removeProperty('--cat-' + i);
      markDirty('global'); rerenderPage(); overlay.remove(); openThemePanel();
    } } }));
    panel.appendChild(palRow);

    panel.appendChild(sectionLabel('Spotlight number'));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'The big gradient headline number. Pick the gradient’s two ends — same color twice for a solid color.' }));
    var spotHexes = (String(g.theme.spotlight || '').match(/#[0-9a-fA-F]{6}/g)) || [];
    var spotDef = ['#ff5e9c', '#65c4ff'];
    var srow = el('div', { style: 'display:flex;gap:8px;align-items:center' });
    var sIn = [0, 1].map(function (i) {
      var v = i === 0 ? (spotHexes[0] || spotDef[0]) : (spotHexes[spotHexes.length - 1] || spotDef[1]);
      var cin = el('input', { type: 'color', value: v, title: i === 0 ? 'Gradient start' : 'Gradient end', style: swatchStyle });
      cin.addEventListener('input', function () {
        g.theme.spotlight = 'linear-gradient(96deg,' + sIn[0].value + ',' + sIn[1].value + ')';
        markDirty('global'); rerenderPage();
      });
      srow.appendChild(cin);
      return cin;
    });
    srow.appendChild(el('button', { 'class': 'engine-mini', type: 'button', text: 'Auto', title: 'Back to the automatic gradient', on: { click: function () {
      delete g.theme.spotlight;
      document.documentElement.style.removeProperty('--cat-spot');
      markDirty('global'); rerenderPage(); overlay.remove(); openThemePanel();
    } } }));
    panel.appendChild(srow);

    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }
  // Floating "Style" button shown on hover over any block in Edit mode — the
  // direct, select-the-object entry into the inspector.
  function styleFloatEl() {
    if (styleFloat) return styleFloat;
    styleFloat = el('button', { id: 'engine-stylefloat', text: 'Style', on: { click: function () { if (styleFloat.__path) openInspector(styleFloat.__path); } } });
    styleFloat.addEventListener('mouseenter', function () { clearTimeout(styleFloatTimer); });
    styleFloat.addEventListener('mouseleave', hideStyleFloatSoon);
    document.body.appendChild(styleFloat);
    return styleFloat;
  }
  function showStyleFloat(node) {
    var path = node.getAttribute('data-engine-path');
    if (!path) return;
    var b = styleFloatEl();
    b.__path = path;
    clearTimeout(styleFloatTimer);
    b.classList.add('show');
    var r = node.getBoundingClientRect();
    b.style.top = Math.max(60, r.top + 6) + 'px';
    b.style.left = Math.min(Math.max(8, r.left + 6), window.innerWidth - 12 - b.offsetWidth) + 'px';
  }
  function hideStyleFloat() { if (styleFloat) styleFloat.classList.remove('show'); }
  function hideStyleFloatSoon() { clearTimeout(styleFloatTimer); styleFloatTimer = setTimeout(hideStyleFloat, 300); }
  function bindBlockHover(n) {
    if (n.__engineBlockBound) return; n.__engineBlockBound = true;
    n.addEventListener('mouseenter', function (e) { if (state.editing) { e.stopPropagation(); showStyleFloat(n); } });
    n.addEventListener('mouseleave', hideStyleFloatSoon);
  }

  // THE page-builder panel: every section — built-in HTML blocks AND added zoo
  // blocks — in ONE list with the SAME verbs (reorder, Hide/Show, Edit, Delete).
  // This is what makes "add vs hide vs delete" finally coherent: nothing is
  // special-cased, so there's no "why is this one hideable and that one not".
  function openSectionsNav() {
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) overlay.remove(); } } });
    var panel = el('div', { 'class': 'engine-panel' });

    var builtinEls = [].slice.call(document.querySelectorAll('[data-section]'));
    var blocks = (state.data && Array.isArray(state.data.sections)) ? state.data.sections : [];
    var hiddenBuiltin = {};
    ((state.data && Array.isArray(state.data.hiddenSections)) ? state.data.hiddenSections : []).forEach(function (k) { hiddenBuiltin[k] = true; });

    // Reorderable rows: built-ins first, then added blocks (matching how the
    // page actually renders). Standalone edit-lists (e.g. the shows list on a
    // page with no data-section wrapper) are listed separately as edit-only.
    var working = [];
    builtinEls.forEach(function (s) {
      working.push({ kind: 'builtin', key: s.getAttribute('data-section'), label: s.getAttribute('data-section-label') || s.getAttribute('data-section'), el: s, hidden: !!hiddenBuiltin[s.getAttribute('data-section')] });
    });
    blocks.forEach(function (b) {
      var def = window.CatalogBlocks && window.CatalogBlocks[b.type];
      working.push({ kind: 'block', block: b, label: (b.title && String(b.title).trim()) || (def && def.label) || b.type, typeLabel: (def && def.label) || b.type, hidden: b.visible === false, deleted: false });
    });
    var extraLists = [].slice.call(document.querySelectorAll('[data-edit-list]')).filter(function (c) {
      if (c.id === 'engine-sections') return false;
      if (c.closest && (c.closest('[data-section]') || c.closest('#engine-sections'))) return false;
      return true;
    });

    function applySave() {
      if (!state.data) { overlay.remove(); return; }
      var builtinOrder = working.filter(function (w) { return w.kind === 'builtin'; });
      if (builtinOrder.length) state.data.order = builtinOrder.map(function (w) { return w.key; }); else delete state.data.order;
      var hk = builtinOrder.filter(function (w) { return w.hidden; }).map(function (w) { return w.key; });
      if (hk.length) state.data.hiddenSections = hk; else delete state.data.hiddenSections;
      var hadBlocks = Array.isArray(state.data.sections) && state.data.sections.length;
      var newBlocks = working.filter(function (w) { return w.kind === 'block' && !w.deleted; }).map(function (w) {
        if (w.hidden) w.block.visible = false; else if ('visible' in w.block) delete w.block.visible;
        return w.block;
      });
      if (newBlocks.length || hadBlocks) state.data.sections = newBlocks;
      markDirty(state.file);
      overlay.remove();
      save(true);
    }

    panel.appendChild(el('div', { 'class': 'engine-phead' }, [
      el('h3', { text: 'Sections' }), helpBtn('sections'),
      el('span', { 'class': 'sp', style: 'flex:1' }),
      el('button', { 'class': 'ghost', text: 'Close', on: { click: function () { overlay.remove(); } } }),
      el('button', { text: 'Apply & Save', on: { click: applySave } })
    ]));
    panel.appendChild(el('div', { 'class': 'engine-navhint', text: 'Drag to reorder. Hide/Show toggles a section on the page (reversible). View scrolls to it. Edit opens its content. Delete removes an added section (Undo before you Save). Use "+ Add section" for the parts library.' }));
    panel.appendChild(el('button', { 'class': 'engine-listbtn', style: 'margin:0 0 12px', text: '+ Add section', on: { click: function () { overlay.remove(); openAddPicker(null); } } }));
    var listWrap = el('div'); panel.appendChild(listWrap);

    var dragI = null;
    function render() {
      listWrap.innerHTML = '';
      if (!working.length && !extraLists.length) { listWrap.appendChild(el('div', { 'class': 'engine-navhint', text: 'No sections yet — use "+ Add section".' })); return; }
      working.forEach(function (w, idx) {
        var kids = [
          el('span', { 'class': 'engine-drag', text: 'Drag', title: 'Drag to reorder' }),
          el('b', { text: w.label + (w.hidden ? ' (hidden)' : '') + (w.deleted ? ' (deleted)' : '') }),
          el('span', { 'class': 'engine-navfile', text: w.kind === 'block' ? w.typeLabel : 'built-in' }),
          el('span', { 'class': 'sp', style: 'flex:1' })
        ];
        if (w.deleted) {
          kids.push(el('button', { 'class': 'engine-mini', text: 'Undo', on: { click: function () { w.deleted = false; render(); } } }));
        } else {
          kids.push(el('button', { 'class': 'engine-mini', text: w.hidden ? 'Show' : 'Hide', title: w.hidden ? 'Show on the page' : 'Hide from the page (reversible)', on: { click: function () { w.hidden = !w.hidden; render(); } } }));
          var node = w.kind === 'builtin' ? w.el : (w.block && w.block.id ? document.querySelector('[data-engine-block="' + w.block.id + '"]') : null);
          kids.push(el('button', { 'class': 'engine-mini', text: 'View', title: 'Scroll to this section on the page', on: { click: function () { if (node) { overlay.remove(); flashTo(node); } else toast('Show this section first to view it.'); } } }));
          if (w.kind === 'block') {
            kids.push(el('button', { 'class': 'engine-mini', text: 'Edit', title: "Edit this section's content & style", on: { click: function () { var at = blocks.indexOf(w.block); overlay.remove(); openSectionEditor('sections[' + at + ']'); } } }));
            kids.push(el('button', { 'class': 'engine-mini', text: 'Delete', title: 'Remove this added section', on: { click: function () { w.deleted = true; render(); } } }));
          } else {
            var editList = node ? ((node.matches && node.matches('[data-edit-list]')) ? node : (node.querySelector ? node.querySelector('[data-edit-list]') : null)) : null;
            if (editList) kids.push(el('button', { 'class': 'engine-mini', text: 'Edit', on: { click: function () { overlay.remove(); openListPanel(editList, editList.getAttribute('data-edit-list'), editList.getAttribute('data-edit-list-label') || w.label); } } }));
          }
        }
        var row = el('div', { 'class': 'engine-navrow' + (w.hidden ? ' engine-rowhidden' : '') }, kids);
        row.addEventListener('mousedown', function (e) { row.draggable = !(e.target.closest && e.target.closest('button,input')); });
        row.addEventListener('dragstart', function (e) { dragI = idx; try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', String(idx)); } catch (_) {} row.classList.add('engine-dragging'); });
        row.addEventListener('dragend', function () { row.classList.remove('engine-dragging'); });
        row.addEventListener('dragover', function (e) { e.preventDefault(); row.classList.add('engine-dragover'); });
        row.addEventListener('dragleave', function () { row.classList.remove('engine-dragover'); });
        row.addEventListener('drop', function (e) { e.preventDefault(); row.classList.remove('engine-dragover'); if (dragI != null && dragI !== idx) { working.splice(idx, 0, working.splice(dragI, 1)[0]); dragI = null; render(); } });
        listWrap.appendChild(row);
      });
      // Site-wide editable lists (header/footer menu, social links, signup survey)
      // — not page sections, so label them clearly so they don't look out of place.
      if (extraLists.length) {
        listWrap.appendChild(el('div', { 'class': 'engine-navhint', style: 'margin-top:16px;font-weight:700;color:#a9742a', text: 'Across the whole site (shown on every page)' }));
      }
      extraLists.forEach(function (c) {
        var lbl = c.getAttribute('data-edit-list-label') || 'list';
        listWrap.appendChild(el('div', { 'class': 'engine-navrow' }, [
          el('b', { text: lbl }), el('span', { 'class': 'engine-navfile', text: 'site-wide' }), el('span', { 'class': 'sp', style: 'flex:1' }),
          el('button', { 'class': 'engine-mini', text: 'View', title: 'Scroll to it on the page', on: { click: function () { overlay.remove(); flashTo(c); } } }),
          el('button', { 'class': 'engine-mini', text: 'Edit', on: { click: function () { overlay.remove(); openListPanel(c, c.getAttribute('data-edit-list'), lbl); } } })
        ]));
      });
    }
    render();
    panelize(panel); overlay.appendChild(panel); document.body.appendChild(overlay);
  }
  function flashTo(c) {
    c.scrollIntoView({ behavior: 'smooth', block: 'center' });
    c.classList.add('engine-flash');
    setTimeout(function () { c.classList.remove('engine-flash'); }, 1700);
  }
  // Discard ALL unpublished edits: copy prod → dev (server-side), then reload.
  function revert() {
    if (!window.confirm('Discard ALL unpublished changes and restore the last published version? This cannot be undone.')) return;
    toast('Reverting…', 8000);
    fetchJson('/api/revert', { method: 'POST' }).then(function (r) {
      state.dirtyFiles = {};
      try { sessionStorage.setItem('engine-scroll', String(window.scrollY)); } catch (_) {}
      toast(r.message || 'Reverted to the published version', 2500);
      setTimeout(function () { location.reload(); }, 900);
    }).catch(function (e) { toast('Revert failed: ' + e.message, 5000); });
  }
  function publish() {
    if (isDirty() && !confirm('You have unsaved edits that won\'t be included.\n\nClick Cancel to go back and Save first, or OK to publish your last saved draft.')) return;
    if (!confirm('Publish your draft to the LIVE website?\n\nEveryone who visits the site will see these changes.')) return;
    toast('Publishing…', 6000);
    fetchJson('/api/promote', { method: 'POST' }).then(function (r) {
      toast('Published — your changes are now live on the website.', 4000);
    }).catch(function (e) { toast('Couldn’t publish — your draft is safe; please try again in a moment.', 6000); });
  }

  // ------------------------------------------------------------------- boot
  function onData(file, data) {
    state.models[file] = data;
    if (file !== 'global') { state.file = file; state.data = data; }
    bindAll(); refreshToolbar();
    if (pendingScroll != null) {
      var y = parseInt(pendingScroll, 10); pendingScroll = null;
      if (!isNaN(y)) setTimeout(function () { window.scrollTo(0, y); }, 250);
    }
  }
  // First-run orientation so a new (non-technical) editor isn't left guessing.
  function maybeWelcome() { var seen; try { seen = localStorage.getItem('engine-welcomed'); } catch (_) { seen = '1'; } if (!seen) showWelcome(); }
  function showWelcome() {
    var overlay = el('div', { 'class': 'engine-overlay', on: { click: function (e) { if (e.target === overlay) close(); } } });
    var modal = el('div', { 'class': 'engine-modal', style: 'max-width:540px' });
    overlay.appendChild(modal); document.body.appendChild(overlay);
    function close() { try { localStorage.setItem('engine-welcomed', '1'); } catch (_) {} overlay.remove(); document.removeEventListener('keydown', esc); }
    function esc(e) { if (e.key === 'Escape') close(); }
    document.addEventListener('keydown', esc);
    modal.appendChild(el('h3', { text: 'Welcome — you can edit your site right here' }));
    [
      ['Edit vs Preview', 'Use the toggle at the top left. In Edit, anything you can change is outlined — click outlined text to type, or a photo or video to replace it.'],
      ['Your edits are private', 'Everything you do is a draft only you can see. Click Save draft to keep it, then Publish when you want visitors to see it.'],
      ['Sections, photos and lists', 'Use the Sections button (or the “Edit ‹section›” tab on the right) to add, reorder, hide, or edit a section and its lists — like team members or link grids.'],
      ['Nothing is permanent', 'Hide is reversible, Delete has an Undo, and Revert restores the last published version. Take your time and click around.']
    ].forEach(function (pt) {
      modal.appendChild(el('div', { style: 'margin-top:13px;font:700 11px var(--engine-mono,monospace);text-transform:uppercase;letter-spacing:1.2px;color:#a9742a', text: pt[0] }));
      modal.appendChild(el('p', { style: 'margin:3px 0 0;color:#2a2317;font:14.5px/1.55 var(--engine-ui,system-ui)', text: pt[1] }));
    });
    modal.appendChild(el('div', { 'class': 'engine-btnrow' }, [
      el('button', { 'class': 'ghost', text: 'Open full help', on: { click: function () { close(); openHelp(); } } }),
      el('button', { text: 'Got it — start editing', on: { click: close } })
    ]));
  }
  // A one-time amber pulse of the editable regions when you first enter Edit mode.
  function maybePulseEditable() {
    var pulsed; try { pulsed = sessionStorage.getItem('engine-pulsed'); } catch (_) { pulsed = '1'; }
    if (pulsed) return;
    try { sessionStorage.setItem('engine-pulsed', '1'); } catch (_) {}
    document.body.classList.add('engine-pulse-edit');
    setTimeout(function () { document.body.classList.remove('engine-pulse-edit'); }, 2700);
  }
  function boot() {
    fetch('/.auth/me').then(function (r) { return r.ok ? r.json() : null; }).then(function (me) {
      var p = me && me.clientPrincipal;
      state.principal = p;
      var roles = (p && p.userRoles) || [];
      if (roles.indexOf('editor') === -1) {
        console.info('[engine-edit] not an editor (roles: ' + roles.join(',') + ') — editor disabled.');
        return;
      }
      try { state.editing = sessionStorage.getItem('engine-editing') === '1'; } catch (_) {}
      pendingScroll = (function () { try { var s = sessionStorage.getItem('engine-scroll'); sessionStorage.removeItem('engine-scroll'); return s; } catch (_) { return null; } })();
      buildToolbar();
      maybeWelcome();
      dragHandleEl(); // create the grip + attach the shared drag-over/drop listeners
      window.addEventListener('scroll', hideMediaTools, { passive: true });
      window.addEventListener('scroll', hideListFloat, { passive: true });
      window.addEventListener('scroll', hideDragHandle, { passive: true });
      window.addEventListener('scroll', hideStyleFloat, { passive: true });
      // Capture phase so this fires for scrolling INSIDE a scroll container too
      // (standalone pages where <body> scrolls, not the window) — otherwise the
      // "Edit <section>" tab would be stuck on the first section.
      document.addEventListener('scroll', scheduleEditTab, { passive: true, capture: true });
      window.addEventListener('resize', scheduleEditTab, { passive: true });
      if (window.__ENGINE_DATA__) onData(window.__ENGINE_DATA__.file, window.__ENGINE_DATA__.data);
      // global.js may have announced its data before this listener attached
      // (esp. on header-less standalone pages) — replay it so Theme/Pages work.
      if (window.__ENGINE_GLOBAL__) onData('global', window.__ENGINE_GLOBAL__);
      document.addEventListener('engine-data-ready', function (e) { onData(e.detail.file, e.detail.data); });
      document.addEventListener('engine-rebind', function () { bindAll(); });
      window.addEventListener('beforeunload', function (e) { if (isDirty()) { e.preventDefault(); e.returnValue = ''; } });
      window.EngineEdit = { state: state, save: save, publish: publish, addSection: addSection, addInside: addInside, addItem: addItem, moveChild: moveChild };
    }).catch(function (e) { console.warn('[engine-edit] /.auth/me failed', e); });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
