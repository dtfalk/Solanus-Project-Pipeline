// =============================================================================
// sections.js — render a page's sections[] into <main>.
//
// This is the bridge between the skeleton and the catalogue. page.js announces a
// page's data; this listens, walks the sections[] array, and for each block asks
// the catalogue (window.Catalog, defined in blocks.js) to build a DOM node for
// it. It then appends them inside a #engine-sections host in <main>.
//
// Note how little this file knows: it never hard-codes a block type. New part
// types are added by registering them in blocks.js / site-blocks.js — this file
// keeps working unchanged. That is the point of the catalogue pattern.
// =============================================================================
(function () {
  'use strict';

  // On a LIVE host we never inject editor affordances or empty states.
  var PROD = !!(window.SITE && window.SITE.isProd());

  function mediaUrl(p) { return (window.mediaUrl ? window.mediaUrl(p) : p) || ''; }

  // Tiny DOM helper: el('div', {class:'x', text:'hi'}, [childNodes]). Used all
  // over the catalogue too (passed in via ctx). text/html are special-cased;
  // everything else becomes an attribute.
  function el(tag, attrs, kids) {
    var n = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (k === 'text') n.textContent = attrs[k];
      else if (k === 'html') n.innerHTML = attrs[k];
      else if (attrs[k] != null) n.setAttribute(k, attrs[k]);
    });
    [].concat(kids || []).forEach(function (c) { if (c) n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return n;
  }

  // The styles for the host container + the editor's "empty page" / "+ Add"
  // affordances. Injected once, lazily, namespaced under engine-. The
  // body:not(.engine-edit) rules hide the editor-only bits on the published page.
  function injectCSS() {
    if (document.getElementById('engine-sections-styles')) return;
    var css = [
      '.engine-sections{max-width:1000px;margin:2.5rem auto;padding:0 1rem;box-sizing:border-box}',
      '.engine-section{margin:2rem 0}',
      '.engine-section-title{margin:0 0 .6rem}',
      '.engine-section-body{line-height:1.6}',
      '.engine-gallery{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}',
      '.engine-gallery img{width:100%;height:190px;object-fit:cover;border-radius:8px;cursor:zoom-in;background:rgba(127,127,127,.15)}',
      '.engine-addsection{display:inline-block;margin:1.5rem 0;padding:10px 16px;border:1px dashed currentColor;opacity:.7;border-radius:8px;background:transparent;color:inherit;cursor:pointer;font:inherit}',
      '.engine-addsection:hover{opacity:1}',
      'body:not(.engine-edit) .engine-addsection{display:none}',
      '.engine-emptystate{max-width:560px;margin:5rem auto;padding:2.5rem 1.5rem;text-align:center;border:1px dashed rgba(127,127,127,.45);border-radius:16px;background:rgba(127,127,127,.06)}',
      '.engine-empty-h{margin:0 0 .5rem;font-size:1.5rem}',
      '.engine-empty-p{margin:0 auto 1.4rem;max-width:42ch;opacity:.85;line-height:1.6}',
      '.engine-empty-btn{display:inline-block;padding:12px 22px;border-radius:10px;border:0;cursor:pointer;font:600 1rem inherit;background:var(--site-accent,#4f7cff);color:#10131a}',
      '.engine-empty-btn:hover{filter:brightness(1.08)}',
      'body:not(.engine-edit) .engine-emptystate{display:none}'
    ].join('\n');
    document.head.appendChild(el('style', { id: 'engine-sections-styles', text: css }));
  }

  // Render one already-normalised block by handing it to the catalogue, which
  // owns the markup/styling/behaviour. The skeleton just asks for a node and
  // hands over a small `ctx` (the DOM helper, the media resolver, the page file,
  // the index, and this block's data-path used for inline editing).
  function renderSection(block, i, file) {
    var ctx = { el: el, mediaUrl: mediaUrl, file: file, index: i, path: 'sections[' + i + ']' };
    if (window.Catalog && window.Catalog.render) return window.Catalog.render(block, ctx);
    // Degraded fallback if the catalogue somehow didn't load (it loads first).
    var sec = el('section', { 'class': 'engine-section' });
    sec.appendChild(el('h2', { 'class': 'engine-section-title', text: block.title || '', 'data-edit-file': file, 'data-edit-text': 'sections[' + i + '].title' }));
    return sec;
  }

  function render(file, data) {
    var main = document.querySelector('main');
    if (!main) return;
    var secs = (data && Array.isArray(data.sections)) ? data.sections : [];
    var hostEl = document.getElementById('engine-sections');

    // On a live host with nothing to show, leave the page completely untouched.
    if (!secs.length && PROD) { if (hostEl) hostEl.remove(); return; }

    injectCSS();
    if (!hostEl) { hostEl = el('section', { id: 'engine-sections', 'class': 'engine-sections' }); main.appendChild(hostEl); }
    hostEl.innerHTML = '';
    hostEl.setAttribute('data-edit-file', file);
    // Section add/remove/reorder/hide is driven by the editor's Sections panel
    // (keyed on block ids), so the container itself is NOT a generic edit-list.
    secs.forEach(function (s, i) {
      var block = window.Catalog ? window.Catalog.normalize(s, i) : (s || {});
      if (window.Catalog && !window.Catalog.isVisible(block)) return; // hidden → skip on the page
      hostEl.appendChild(renderSection(block, i, file));
    });

    // Editor-only affordances: a friendly empty state for a blank page, or a
    // "+ Add section" button otherwise. Both are hidden on the live site via CSS.
    if (!secs.length) {
      var addFirst = el('button', { 'class': 'engine-empty-btn', type: 'button', text: 'Add your first section' });
      addFirst.addEventListener('click', function () { if (window.EngineEdit && window.EngineEdit.addSection) window.EngineEdit.addSection(); });
      hostEl.appendChild(el('div', { 'class': 'engine-emptystate' }, [
        el('h2', { 'class': 'engine-empty-h', text: 'This page is empty' }),
        el('p', { 'class': 'engine-empty-p', text: 'Build it from the parts library — add a heading, photos, a video, text, or a button. You can rearrange and restyle everything afterwards.' }),
        addFirst
      ]));
    } else {
      var add = el('button', { 'class': 'engine-addsection', type: 'button', text: '+ Add section' });
      add.addEventListener('click', function () {
        if (window.EngineEdit && window.EngineEdit.addSection) window.EngineEdit.addSection();
      });
      hostEl.appendChild(add);
    }

    // Tell the editor (if loaded) to bind the freshly-rendered nodes.
    document.dispatchEvent(new CustomEvent('engine-rebind'));
  }

  // Reorder pre-existing [data-section] nodes (used by sites that have static,
  // hand-built sections in their shells) to match an optional `order` array in
  // the page JSON. Absent → original DOM order. Most pages don't use this.
  function applyOrder(data) {
    var order = data && Array.isArray(data.order) ? data.order : null;
    if (!order || !order.length) return;
    var els = [].slice.call(document.querySelectorAll('[data-section]'));
    if (!els.length) return;
    var groups = [];
    els.forEach(function (e2) {
      var g = groups.filter(function (x) { return x.parent === e2.parentNode; })[0];
      if (!g) { g = { parent: e2.parentNode, items: [] }; groups.push(g); }
      g.items.push(e2);
    });
    groups.forEach(function (g) {
      var anchor = g.items[g.items.length - 1].nextSibling; // keep the block in its original slot
      var rank = function (e2) { var i = order.indexOf(e2.getAttribute('data-section')); return i < 0 ? 9999 : i; };
      g.items.slice().sort(function (a, b) { return rank(a) - rank(b); }).forEach(function (e2) { g.parent.insertBefore(e2, anchor); });
    });
  }

  // Hide static [data-section] nodes the editor marked removed
  // (data.hiddenSections). Reversible: restoring the key brings them back.
  function applyHidden(data) {
    var hidden = data && Array.isArray(data.hiddenSections) ? data.hiddenSections : [];
    [].slice.call(document.querySelectorAll('[data-section]')).forEach(function (e2) {
      e2.style.display = hidden.indexOf(e2.getAttribute('data-section')) !== -1 ? 'none' : '';
    });
  }

  // Each step is wrapped so one failure can't take down the others.
  function handle(file, data) {
    try { applyOrder(data); } catch (err) { console.warn('[sections] order failed:', err.message); }
    try { applyHidden(data); } catch (err) { console.warn('[sections] hide failed:', err.message); }
    try { render(file, data); } catch (err) { console.warn('[sections] render failed:', err.message); }
  }

  document.addEventListener('engine-data-ready', function (e) {
    if (!e.detail || e.detail.file === 'global') return; // global.json is chrome, not a page
    handle(e.detail.file, e.detail.data);
  });

  // Fallback for the dynamic-injection race: if the page already announced its
  // data before this script loaded, use the captured global now.
  if (window.__ENGINE_DATA__ && window.__ENGINE_DATA__.file !== 'global') {
    handle(window.__ENGINE_DATA__.file, window.__ENGINE_DATA__.data);
  }
})();
