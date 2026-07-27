// =============================================================================
// blocks.js — THE CATALOGUE (the heart of Pagesmith).
//
// The skeleton (page.js / sections.js) is deliberately dumb: it never knows what
// a "carousel" or a "chart" is. It just hands each block to the catalogue and
// says "render this". THIS file is the catalogue — a registry of part types,
// where each entry is a fully self-contained component:
//
//     • its markup        (a render() function returning a DOM node)
//     • its styling       (CSS, injected once, namespaced under catalog-)
//     • its edit schema   (what fields the editor shows for it)
//     • its behaviour     (an optional init() run after render, e.g. a carousel)
//
// Adding a capability to the whole system = adding ONE entry here. Nothing leaks
// into the skeleton or the editor, which is what makes the engine portable: drop
// this file (plus the skeleton) into any project and it works.
//
// ----- THE PART CONTRACT -----------------------------------------------------
// A registered part is an object on the BLOCKS map below. Keys it may define:
//
//   render(block, ctx)   REQUIRED. Return a DOM node. `block` is the data;
//                        `ctx` gives you { el, mediaUrl, file, index, path }.
//                        Emit data-edit-* attributes (see below) to make text,
//                        images and lists editable. Namespace any CSS classes
//                        under `catalog-`.
//   label, category, icon   For the "+ Add section" picker.
//   blank()              Return the default block object when one is added.
//                        It MUST set `type`. Keep its placeholder copy generic.
//   fields[]             Inspector schema for SCALAR fields (text/url/number/
//                        select/color/bool/image/embed). The editor renders these.
//   lists{ key: {label, fields[]} }   Schema for ARRAY fields (e.g. a gallery's
//                        images). Each item is edited with the given fields.
//   init(node, block, ctx)   Optional behaviour wiring after the node is in the DOM.
//   hidden               If true, omit from the picker (used by site-specific packs).
//
// ----- THE EDITING PROTOCOL (data-edit-* attributes) -------------------------
// You don't write any editor code. You just tag your DOM and the editor wires it:
//   data-edit-file + data-edit-text   → click-to-edit plain text at a path
//   data-edit-file + data-edit-html   → click-to-edit rich text
//   data-edit-file + data-edit-image  → image picker at a path
//   data-edit-file + data-edit-list   → a reorderable/editable array at a path
//   data-item-index="<i>"             → on each LIST CHILD, its true array index
//                                       (so on-page drag reorders the right item
//                                       even when some items aren't rendered)
// A "path" looks like sections[2].items[0].value — see ctx.path.
//
// Loaded before sections.js (media-base.js orders them).
// =============================================================================
(function () {
  'use strict';

  // A short, collision-resistant id for newly-created blocks.
  function uid() { return 'b' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6); }

  // The shared, inline-editable section title most parts use.
  function titleEl(block, ctx) {
    return ctx.el('h2', {
      'class': 'engine-section-title',
      'data-edit-file': ctx.file,
      'data-edit-text': ctx.path + '.title',
      text: block.title || ''
    });
  }

  // ---------------------------------------------------------------------------
  // A tiny dependency-free SVG chart (bar or line) from [{label,value}].
  // Theme-aware: it draws with the data-viz palette (--cat-* via STATPAL) and
  // currentColor, and scales via a viewBox so it's responsive.
  // ---------------------------------------------------------------------------
  function chartFmt(v) { v = Math.round(v); var a = Math.abs(v); if (a >= 1e6) return (v / 1e6).toFixed(a >= 1e7 ? 0 : 1) + 'M'; if (a >= 1e3) return (v / 1e3).toFixed(a >= 1e4 ? 0 : 1) + 'k'; return '' + v; }
  function chartEsc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }

  // The default "fun" data-viz palette. A site overrides it by setting --cat-1..N
  // (theme.js does this from a page/site theme.palette); absent → these, so the
  // catalogue looks good out of the box without any config.
  var STATPAL_DEFAULT = ['#ff3b8e', '#00e5ff', '#ffd23f', '#a06bff', '#5af78e', '#ff8c42'];
  var STATPAL = STATPAL_DEFAULT.slice();
  function palette() {
    if (typeof document === 'undefined') return STATPAL_DEFAULT;
    try {
      var cs = getComputedStyle(document.documentElement), out = [];
      for (var i = 1; i <= 8; i++) { var v = cs.getPropertyValue('--cat-' + i).trim(); if (v) out.push(v); }
      return out.length ? out : STATPAL_DEFAULT;
    } catch (_) { return STATPAL_DEFAULT; }
  }

  // Build the chart SVG. `gid` is a UNIQUE gradient id for THIS chart — without
  // it, two charts on one page would both define id="catalog-cg" and the second
  // chart's area fill would resolve to the first chart's gradient.
  function chartSvg(series, kind, gid) {
    gid = gid || 'catalog-cg';
    var n = series.length;
    if (!n) return '<div style="opacity:.6;padding:1rem 0">No data yet — add data points in the editor.</div>';
    var W = 640, H = 280, padL = 46, padR = 14, padT = 22, padB = 38, iw = W - padL - padR, ih = H - padT - padB;
    var vals = series.map(function (d) { return +d.value || 0; });
    var max = Math.max.apply(null, vals.concat([1])), min = Math.min.apply(null, vals.concat([0]));
    if (min > 0) min = 0;
    var range = (max - min) || 1, accent = STATPAL[0], Y = function (v) { return padT + ih - ((v - min) / range) * ih; };
    var p = ['<defs><linearGradient id="' + gid + '" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="' + accent + '" stop-opacity=".5"/><stop offset="1" stop-color="' + accent + '" stop-opacity="0"/></linearGradient></defs>'];
    p.push('<line x1="' + padL + '" y1="' + (padT + ih) + '" x2="' + (W - padR) + '" y2="' + (padT + ih) + '" stroke="currentColor" stroke-opacity=".3"/>');
    for (var g = 0; g <= 3; g++) { var gv = min + range * g / 3, gy = Y(gv); p.push('<line x1="' + padL + '" y1="' + gy + '" x2="' + (W - padR) + '" y2="' + gy + '" stroke="currentColor" stroke-opacity=".09"/>'); p.push('<text x="' + (padL - 7) + '" y="' + (gy + 4) + '" text-anchor="end" font-size="11" fill="currentColor" fill-opacity=".55">' + chartFmt(gv) + '</text>'); }
    if (kind === 'line') {
      var step = n > 1 ? iw / (n - 1) : 0;
      var lp = series.map(function (d, i) { return (padL + i * step) + ',' + Y(+d.value || 0); });
      p.push('<polygon points="' + (padL + ',' + (padT + ih) + ' ' + lp.join(' ') + ' ' + (padL + (n - 1) * step) + ',' + (padT + ih)) + '" fill="url(#' + gid + ')"/>');
      p.push('<polyline fill="none" stroke="' + accent + '" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="' + lp.join(' ') + '"/>');
      series.forEach(function (d, i) { var cx = padL + i * step, cy = Y(+d.value || 0); p.push('<circle cx="' + cx + '" cy="' + cy + '" r="4" fill="#fff" stroke="' + accent + '" stroke-width="2.5"/>'); p.push('<text x="' + cx + '" y="' + (padT + ih + 16) + '" text-anchor="middle" font-size="11" fill="currentColor" fill-opacity=".7">' + chartEsc(d.label) + '</text>'); });
    } else {
      var gapw = iw / n, bw = gapw * 0.6;
      series.forEach(function (d, i) { var v = +d.value || 0, bx = padL + i * gapw + (gapw - bw) / 2, by = Y(v), bh = (padT + ih) - by, col = STATPAL[i % STATPAL.length]; p.push('<rect x="' + bx + '" y="' + Math.min(by, padT + ih) + '" width="' + bw + '" height="' + Math.max(0, Math.abs(bh)) + '" rx="5" fill="' + col + '"/>'); p.push('<text x="' + (bx + bw / 2) + '" y="' + (by - 6) + '" text-anchor="middle" font-size="11" font-weight="700" fill="' + col + '">' + chartFmt(v) + '</text>'); p.push('<text x="' + (bx + bw / 2) + '" y="' + (padT + ih + 16) + '" text-anchor="middle" font-size="11" fill="currentColor" fill-opacity=".7">' + chartEsc(d.label) + '</text>'); });
    }
    return '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%;height:auto;max-height:360px;overflow:visible" role="img" aria-label="chart">' + p.join('') + '</svg>';
  }

  // ---------------------------------------------------------------------------
  // Component CSS. Injected ONCE, lazily, on first render — so a part looks right
  // on ANY page without depending on a page's own stylesheet. Everything is
  // namespaced under catalog- and uses theme variables with neutral fallbacks.
  // ---------------------------------------------------------------------------
  function ensureCSS() {
    if (typeof document === 'undefined' || document.getElementById('catalog-styles')) return;
    var css = [
      // video grid (tiles + download button)
      '.catalog-vgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:1.5rem;margin:1rem .5rem}',
      '.catalog-vitem{display:flex;flex-direction:column;align-items:center;gap:.5rem}',
      '.catalog-vgrid video{width:100%;height:auto;display:block;border-radius:6px;box-shadow:0 4px 12px rgba(0,0,0,.5)}',
      '.catalog-dl{display:inline-block;margin-top:.5rem;background:var(--site-accent,#4f7cff);color:#fff;padding:.5rem 1.5rem;border-radius:4px;text-decoration:none;font-weight:bold}',
      // carousel
      '.catalog-carousel{width:100%;max-width:900px;margin:0 auto;padding:1rem}',
      '.catalog-carousel-container{position:relative;display:flex;align-items:center;gap:1rem;width:100%}',
      '.catalog-carousel-wrapper{flex:1;overflow:hidden;border-radius:8px;width:100%;position:relative;touch-action:pan-y}',
      '.catalog-carousel-track{display:flex;transition:transform .5s ease;width:100%;will-change:transform}',
      '.catalog-carousel-slide{min-width:100%;flex-shrink:0;display:flex;justify-content:center;box-sizing:border-box}',
      '.catalog-carousel-slide iframe{width:100%;max-width:560px;height:315px;border:none;border-radius:4px;margin:0 auto;display:block}',
      '.catalog-carousel-btn{background:rgba(255,255,255,.1);border:2px solid rgba(255,255,255,.3);color:#fff;width:50px;height:50px;border-radius:50%;cursor:pointer;font-size:1.2rem;font-weight:bold;transition:all .3s ease;display:flex;align-items:center;justify-content:center;z-index:10;user-select:none}',
      '.catalog-carousel-btn:hover{background:rgba(255,255,255,.2);transform:scale(1.1)}',
      '.catalog-carousel-btn:disabled{opacity:.3;cursor:not-allowed;transform:none}',
      '.catalog-carousel-dots{display:flex;justify-content:center;gap:.5rem;margin-top:1rem}',
      '.catalog-carousel-dot{width:10px;height:10px;border-radius:50%;background:rgba(255,255,255,.3);cursor:pointer;transition:all .3s ease}',
      '.catalog-carousel-dot.active{background:rgba(255,255,255,.8);transform:scale(1.2)}',
      '@media(max-width:768px){.catalog-carousel-btn{display:none}.catalog-carousel-slide iframe{width:95%;max-width:95%;height:auto;min-height:200px;aspect-ratio:16/9}}',
      // embed
      '.catalog-embed{width:100%;max-width:760px;margin:0 auto}',
      '.catalog-embed-frame{position:relative;width:100%;aspect-ratio:16/9;border-radius:6px;overflow:hidden;background:#000}',
      '.catalog-embed-frame iframe{position:absolute;inset:0;width:100%;height:100%;border:0}',
      '.catalog-embed-cell{display:flex;justify-content:center;width:100%;min-width:0;max-width:100%}',
      '.catalog-embed-cell iframe{max-width:100%}',
      // link grid
      '.catalog-links{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1.5rem;margin-top:1rem}',
      '.catalog-links a{background:rgba(255,255,255,.06);padding:.75rem 1rem;border-radius:4px;text-align:center;text-decoration:none;display:block;color:inherit;transition:background .3s}',
      '.catalog-links a:hover{background:rgba(255,255,255,.12)}',
      // quotes
      '.catalog-quote{background:rgba(255,255,255,.08);border-left:4px solid var(--site-accent,#4f7cff);font-style:italic;padding:1rem 1.5rem;margin:1.5rem auto;max-width:800px}',
      // button / cta
      '.catalog-cta-row{text-align:center;margin:1.5rem 0}',
      '.catalog-cta{display:inline-block;padding:1rem 2rem;border-radius:8px;text-decoration:none;font-weight:600;background:var(--site-accent,#4f7cff);color:#fff;transition:filter .2s}',
      '.catalog-cta:hover{filter:brightness(1.08);color:#fff}',
      // group (nested container)
      '.catalog-group{margin:1.5rem 0;padding:1rem;border-radius:8px}',
      '.catalog-group-children{display:flex;flex-direction:column;gap:1rem}',
      '.catalog-childwrap{position:relative}',
      '.catalog-childtools{display:none;position:absolute;top:6px;right:6px;z-index:7;gap:5px}',
      'body.engine-edit .catalog-childtools{display:flex}',
      '.catalog-childtools button{text-decoration:none;background:rgba(20,16,9,.82);color:#fff;border:1px solid rgba(255,255,255,.18);border-radius:6px;padding:3px 9px;font:600 11px var(--engine-mono,monospace);cursor:pointer}',
      '.catalog-childtools button:hover{background:var(--site-accent,#4f7cff);color:#10131a}',
      // data: chart + stat cards
      '.catalog-chart-holder{margin:1rem 0;color:inherit}',
      '.catalog-chart-data{margin-top:.6rem;display:flex;flex-wrap:wrap;gap:8px}',
      '.catalog-chart-chip{font:12px var(--engine-mono,monospace);opacity:.8;padding:3px 9px;border:1px solid rgba(127,127,127,.3);border-radius:999px}',
      // Stat "cards" are intentionally NOT boxes — a clean row of bold, colourful
      // facts so they read editorial, not dashboard.
      '.catalog-statcards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(140px,100%),1fr));gap:1.7rem 1.1rem;margin:1.7rem 0}',
      '.catalog-statcard{position:relative;padding:.3rem;text-align:center}',
      '.catalog-statval{font:800 2.8rem/1 var(--site-display,inherit);color:var(--c,#4f7cff);letter-spacing:-.02em}',
      '.catalog-statlbl{margin-top:.4rem;font-size:.98rem;opacity:.92}',
      '.catalog-statsub{display:inline-block;margin-top:.4rem;font:600 .82rem var(--engine-mono,monospace);color:var(--c,#4f7cff);opacity:.82}',
      '.catalog-stat-up{color:#5af78e!important}',
      '.catalog-stat-down{color:#ff7e8a!important}',
      // Spotlight: an editorial gradient headline number (no banner box).
      '.catalog-spotlight{margin:2.2rem 0;text-align:center}',
      '.catalog-spot-inner{padding:1rem .5rem;position:relative}',
      '.catalog-spot-val{font:900 clamp(3.4rem,13vw,7rem)/.9 var(--site-display,inherit);letter-spacing:-.03em;color:#fff;background:var(--cat-spot,linear-gradient(96deg,#ff5e9c,#ffd23f 42%,#4f7cff));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;filter:drop-shadow(0 6px 22px rgba(0,0,0,.4))}',
      '.catalog-spot-lbl{margin-top:.3rem;font:800 1.1rem var(--site-display,inherit);text-transform:uppercase;letter-spacing:.16em;color:inherit}',
      '.catalog-spot-cap{margin:.55rem auto 0;max-width:46ch;font-size:1.02rem;opacity:.8;color:inherit}',
      '.catalog-ranking{display:flex;flex-direction:column;gap:.7rem;margin:1.2rem 0}',
      '.catalog-rankrow{display:flex;align-items:center;gap:.8rem}',
      '.catalog-rank-n{flex:0 0 1.6rem;font:800 1.1rem var(--site-display,inherit);opacity:.55;text-align:center}',
      '.catalog-rank-barwrap{flex:1;background:rgba(127,127,127,.12);border-radius:999px;overflow:hidden;min-width:0}',
      '.catalog-rank-bar{height:38px;border-radius:999px;display:flex;align-items:center;padding:0 14px;min-width:fit-content;transition:width .6s cubic-bezier(.2,.8,.2,1)}',
      '.catalog-rank-lbl{font-weight:700;color:#10131a;white-space:nowrap;text-shadow:0 1px 2px rgba(255,255,255,.3)}',
      '.catalog-rank-val{flex:0 0 auto;font:700 .95rem var(--engine-mono,monospace);opacity:.8}',
      // edit-only affordances (shown only when the editor is in Edit mode)
      '.catalog-eo{display:none}',
      'body.engine-edit .catalog-eo{display:block;margin-top:.5rem}',
      '.catalog-eo a,.catalog-eo button{font:600 12px var(--engine-ui,system-ui);color:var(--site-accent,#4f7cff);cursor:pointer;text-decoration:underline;background:none;border:0;padding:0}'
    ].join('\n');
    var s = document.createElement('style');
    s.id = 'catalog-styles';
    s.textContent = css;
    document.head.appendChild(s);
  }

  // Self-contained carousel behaviour, scoped to a root node so any number of
  // carousels can coexist on a page. Wraps around (loops) at the ends.
  function initCarousel(root) {
    if (!root || root.__catalogCarousel) return; root.__catalogCarousel = true;
    var track = root.querySelector('.catalog-carousel-track');
    var slides = root.querySelectorAll('.catalog-carousel-slide');
    var dots = root.querySelectorAll('.catalog-carousel-dot');
    var prev = root.querySelector('.catalog-carousel-prev');
    var next = root.querySelector('.catalog-carousel-next');
    if (!track || !slides.length) return;
    var idx = 0, total = slides.length;
    function update() {
      track.style.transform = 'translateX(' + (-idx * 100) + '%)';
      for (var d = 0; d < dots.length; d++) dots[d].classList.toggle('active', d === idx);
    }
    function go(i) { idx = ((i % total) + total) % total; update(); }  // wraps around
    if (next) next.addEventListener('click', function () { go(idx + 1); });
    if (prev) prev.addEventListener('click', function () { go(idx - 1); });
    for (var d2 = 0; d2 < dots.length; d2++) (function (i) { dots[i].addEventListener('click', function () { go(i); }); })(d2);
    update();
  }

  // The iframe sandbox/allow list shared by embed + carousel.
  var IFRAME_ALLOW = 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share';

  // ===========================================================================
  // THE REGISTRY. Each key is a part type; each value follows the part contract.
  // ===========================================================================
  var BLOCKS = {

    // ---- Basic text ----
    text: {
      label: 'Text', category: 'Basic', icon: '¶',
      blank: function () { return { type: 'text', title: 'New section', body: 'Add your text here.' }; },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        sec.appendChild(ctx.el('div', { 'class': 'engine-section-body', html: block.body || '', 'data-edit-file': ctx.file, 'data-edit-html': ctx.path + '.body' }));
        return sec;
      }
    },

    // ---- Photo gallery (a reorderable list of images) ----
    gallery: {
      label: 'Photo gallery', category: 'Media', icon: '▦',
      blank: function () { return { type: 'gallery', title: 'New gallery', images: [] }; },
      lists: { images: { label: 'Image', fields: [ { key: 'src', type: 'image', label: 'Image' }, { key: 'alt', type: 'text', label: 'Alt text' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        var grid = ctx.el('div', { 'class': 'engine-gallery', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.images', 'data-edit-list-label': 'images' });
        (Array.isArray(block.images) ? block.images : []).forEach(function (im, j) {
          var src = (im && im.src) ? im.src : '';
          // data-item-index = the true array index, so on-page drag reorders the
          // right item even though edits target the [j] path.
          grid.appendChild(ctx.el('img', {
            src: ctx.mediaUrl(src), alt: (im && im.alt) || '', loading: 'lazy', 'data-item-index': j,
            'data-full': ctx.mediaUrl(src), 'data-lightbox': 'section-' + ctx.index,
            'data-edit-file': ctx.file, 'data-edit-image': ctx.path + '.images[' + j + '].src', 'data-edit-label': 'image'
          }));
        });
        sec.appendChild(grid);
        return sec;
      }
    },

    // ---- Heading ----
    heading: {
      label: 'Heading', category: 'Basic', icon: 'H',
      blank: function () { return { type: 'heading', text: 'Heading' }; },
      fields: [ { key: 'text', type: 'text', label: 'Heading text' } ],
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(ctx.el('h2', { 'class': 'engine-section-title', 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.text', text: block.text || '' }));
        return sec;
      }
    },

    // ---- Video grid (uploaded video files, each with a download link) ----
    video: {
      label: 'Video', category: 'Media', icon: '▷',
      blank: function () { return { type: 'video', title: '', items: [] }; },
      lists: { items: { label: 'Video', fields: [ { key: 'src', type: 'image', label: 'Video file' }, { key: 'name', type: 'text', label: 'Name' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        var grid = ctx.el('div', { 'class': 'catalog-vgrid', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.items', 'data-edit-list-label': 'videos' });
        (Array.isArray(block.items) ? block.items : []).forEach(function (v, j) {
          if (!v) return;
          var url = ctx.mediaUrl(v.src || '');
          var item = ctx.el('div', { 'class': 'catalog-vitem', 'data-item-index': j });
          var video = ctx.el('video', { controls: '', 'aria-label': v.name || '', 'data-edit-file': ctx.file, 'data-edit-image': ctx.path + '.items[' + j + '].src', 'data-edit-label': 'video' });
          video.appendChild(ctx.el('source', { src: url, type: 'video/mp4' }));
          item.appendChild(video);
          if (v.name) item.appendChild(ctx.el('a', { 'class': 'catalog-dl', href: url, download: (v.src || 'video').split('/').pop(), text: 'Download ' + v.name }));
          grid.appendChild(item);
        });
        sec.appendChild(grid);
        return sec;
      }
    },

    // ---- Carousel of embeds (YouTube/Spotify/… iframes) ----
    carousel: {
      label: 'Carousel', category: 'Media', icon: '⇆',
      blank: function () { return { type: 'carousel', title: '', slides: [] }; },
      lists: { slides: { label: 'Slide', fields: [ { key: 'embedUrl', type: 'url', label: 'Embed URL' }, { key: 'title', type: 'text', label: 'Title' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section catalog-carousel' });
        sec.appendChild(titleEl(block, ctx));
        var container = ctx.el('div', { 'class': 'catalog-carousel-container' });
        container.appendChild(ctx.el('button', { 'class': 'catalog-carousel-btn catalog-carousel-prev', type: 'button', 'aria-label': 'Previous', text: '<' }));
        var wrap = ctx.el('div', { 'class': 'catalog-carousel-wrapper' });
        var track = ctx.el('div', { 'class': 'catalog-carousel-track', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.slides', 'data-edit-list-label': 'slides' });
        var slides = Array.isArray(block.slides) ? block.slides : [];
        slides.forEach(function (s, j) {
          var slide = ctx.el('div', { 'class': 'catalog-carousel-slide', 'data-item-index': j });
          slide.appendChild(ctx.el('iframe', { src: (s && s.embedUrl) || '', title: (s && s.title) || '', frameborder: '0', allow: IFRAME_ALLOW, referrerpolicy: 'strict-origin-when-cross-origin', allowfullscreen: 'true' }));
          track.appendChild(slide);
        });
        wrap.appendChild(track);
        container.appendChild(wrap);
        container.appendChild(ctx.el('button', { 'class': 'catalog-carousel-btn catalog-carousel-next', type: 'button', 'aria-label': 'Next', text: '>' }));
        sec.appendChild(container);
        var dots = ctx.el('div', { 'class': 'catalog-carousel-dots' });
        slides.forEach(function (_, j) { dots.appendChild(ctx.el('div', { 'class': 'catalog-carousel-dot' + (j === 0 ? ' active' : '') })); });
        sec.appendChild(dots);
        return sec;
      },
      init: function (node) { initCarousel(node); }
    },

    // ---- Single embed (one YouTube/Spotify/… iframe) ----
    embed: {
      label: 'Embed', category: 'Media', icon: '❏',
      blank: function () { return { type: 'embed', title: '', embedUrl: '' }; },
      fields: [ { key: 'embedUrl', type: 'embed', label: 'Embed URL (YouTube/Spotify…)' } ],
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section catalog-embed' });
        sec.appendChild(titleEl(block, ctx));
        var frame = ctx.el('div', { 'class': 'catalog-embed-frame' });
        frame.appendChild(ctx.el('iframe', { src: block.embedUrl || '', title: block.title || 'embed', frameborder: '0', allow: IFRAME_ALLOW, referrerpolicy: 'strict-origin-when-cross-origin', allowfullscreen: 'true' }));
        sec.appendChild(frame);
        // Edit-only hint pointing at the inspector (which runs share→embed
        // conversion). Editing it here opens the inspector field, not a raw prompt.
        var eo = ctx.el('div', { 'class': 'catalog-eo' });
        eo.appendChild(ctx.el('span', { text: block.embedUrl ? ('Embed: ' + block.embedUrl) : 'Open this block’s editor to set the embed URL.' }));
        sec.appendChild(eo);
        return sec;
      }
    },

    // ---- Link grid ----
    links: {
      label: 'Link grid', category: 'Basic', icon: '⛓',
      blank: function () { return { type: 'links', title: '', links: [] }; },
      lists: { links: { label: 'Link', fields: [ { key: 'title', type: 'text', label: 'Label' }, { key: 'url', type: 'url', label: 'URL' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        var grid = ctx.el('div', { 'class': 'catalog-links', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.links', 'data-edit-list-label': 'links' });
        (Array.isArray(block.links) ? block.links : []).forEach(function (l, j) {
          if (!l) return;
          grid.appendChild(ctx.el('a', { href: l.url || '#', target: '_blank', rel: 'noopener', 'data-item-index': j, text: l.title || '' }));
        });
        sec.appendChild(grid);
        return sec;
      }
    },

    // ---- Pull quotes ----
    quotes: {
      label: 'Quotes', category: 'Basic', icon: '❝',
      blank: function () { return { type: 'quotes', title: '', quotes: [] }; },
      lists: { quotes: { label: 'Quote', fields: [ { key: 'quote', type: 'textarea', label: 'Quote' }, { key: 'source', type: 'text', label: 'Source' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        var host = ctx.el('div', { 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.quotes', 'data-edit-list-label': 'quotes' });
        (Array.isArray(block.quotes) ? block.quotes : []).forEach(function (q, j) {
          if (!q) return;
          var bq = ctx.el('blockquote', { 'class': 'catalog-quote', 'data-item-index': j });
          bq.appendChild(ctx.el('span', { text: '"' + (q.quote || '') + '"' }));
          bq.appendChild(ctx.el('br'));
          bq.appendChild(ctx.el('span', { text: '— ' + (q.source || '') }));
          host.appendChild(bq);
        });
        sec.appendChild(host);
        return sec;
      }
    },

    // ---- Call-to-action button ----
    button: {
      label: 'Button', category: 'Basic', icon: '▭',
      blank: function () { return { type: 'button', label: 'Click here', url: '#' }; },
      fields: [ { key: 'label', type: 'text', label: 'Label' }, { key: 'url', type: 'url', label: 'Link URL' } ],
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section catalog-cta-row' });
        sec.appendChild(ctx.el('a', { 'class': 'catalog-cta', href: block.url || '#', 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.label', text: block.label || '' }));
        var eo = ctx.el('div', { 'class': 'catalog-eo' });
        eo.appendChild(ctx.el('a', { href: '#', 'data-edit-file': ctx.file, 'data-edit-attr': ctx.path + '.url|text', 'data-edit-label': 'button link', text: block.url ? ('Link: ' + block.url) : 'Set link' }));
        sec.appendChild(eo);
        return sec;
      }
    },

    // ---- Chart (bar/line) ----
    chart: {
      label: 'Chart', category: 'Data', icon: '#',
      blank: function () { return { type: 'chart', title: 'Monthly visitors', kind: 'bar', series: [ { label: 'Jan', value: 1200 }, { label: 'Feb', value: 1800 }, { label: 'Mar', value: 2600 }, { label: 'Apr', value: 3100 } ] }; },
      fields: [ { key: 'kind', type: 'select', label: 'Chart type', options: [['bar', 'Bar'], ['line', 'Line']] } ],
      lists: { series: { label: 'Data point', fields: [ { key: 'label', type: 'text', label: 'Label' }, { key: 'value', type: 'number', label: 'Value' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section catalog-chart' });
        sec.appendChild(titleEl(block, ctx));
        var full = Array.isArray(block.series) ? block.series : [];
        var series = full.filter(function (d) { return d && d.label != null; });
        var holder = ctx.el('div', { 'class': 'catalog-chart-holder' });
        // Unique gradient id per chart instance (so two charts don't share one).
        holder.innerHTML = chartSvg(series, block.kind === 'line' ? 'line' : 'bar', 'catalog-cg-' + (block.id || ctx.index));
        sec.appendChild(holder);
        // Legend = the editable list. Iterate the FULL array (skipping unlabelled
        // points) so each chip carries its TRUE array index for reordering.
        var legend = ctx.el('div', { 'class': 'catalog-chart-data', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.series', 'data-edit-list-label': 'data points' });
        full.forEach(function (d, j) {
          if (!d || d.label == null) return;
          legend.appendChild(ctx.el('span', { 'class': 'catalog-chart-chip', 'data-item-index': j, text: (d.label || '') + ': ' + (d.value != null ? d.value : '') }));
        });
        sec.appendChild(legend);
        return sec;
      }
    },

    // ---- Stat cards (a row of big numbers) ----
    'stat-cards': {
      label: 'Stat cards', category: 'Data', icon: '=',
      blank: function () { return { type: 'stat-cards', title: '', items: [ { label: 'Visitors', value: '12.4k', sub: '+18% this month' }, { label: 'Sign-ups', value: '3.1k', sub: '' }, { label: 'Page views', value: '480k', sub: 'all time' } ] }; },
      lists: { items: { label: 'Stat', fields: [ { key: 'label', type: 'text', label: 'Label' }, { key: 'value', type: 'text', label: 'Big number' }, { key: 'sub', type: 'text', label: 'Sub-text (optional)' }, { key: 'color', type: 'color', label: 'Color' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        if (block.title) sec.appendChild(titleEl(block, ctx));
        var grid = ctx.el('div', { 'class': 'catalog-statcards', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.items', 'data-edit-list-label': 'stats' });
        (Array.isArray(block.items) ? block.items : []).forEach(function (it, i) {
          if (!it) return;
          var ip = ctx.path + '.items[' + i + ']';  // inline-editable, like the spotlight
          var card = ctx.el('div', { 'class': 'catalog-statcard', 'data-item-index': i, style: '--c:' + (it.color || STATPAL[i % STATPAL.length]) });
          card.appendChild(ctx.el('div', { 'class': 'catalog-statval', 'data-edit-file': ctx.file, 'data-edit-text': ip + '.value', text: it.value || '' }));
          card.appendChild(ctx.el('div', { 'class': 'catalog-statlbl', 'data-edit-file': ctx.file, 'data-edit-text': ip + '.label', text: it.label || '' }));
          if (it.sub) { var up = /^\s*\+|\bup\b|rising|on the rise/i.test(it.sub), down = /^\s*-\d|\bdown\b|falling/i.test(it.sub); card.appendChild(ctx.el('div', { 'class': 'catalog-statsub' + (up ? ' catalog-stat-up' : down ? ' catalog-stat-down' : ''), 'data-edit-file': ctx.file, 'data-edit-text': ip + '.sub', text: it.sub })); }
          grid.appendChild(card);
        });
        sec.appendChild(grid);
        return sec;
      }
    },

    // ---- Spotlight (one giant gradient number) ----
    spotlight: {
      label: 'Spotlight stat', category: 'Data', icon: '*',
      blank: function () { return { type: 'spotlight', value: '10k', label: 'visitors and counting', caption: 'since launch' }; },
      fields: [ { key: 'value', type: 'text', label: 'Big number' }, { key: 'label', type: 'text', label: 'Label' }, { key: 'caption', type: 'text', label: 'Caption' },
        { key: 'color', type: 'color', label: 'Number color' }, { key: 'color2', type: 'color', label: 'Second gradient color (optional)' } ],
      render: function (block, ctx) {
        var attrs = { 'class': 'engine-section catalog-spotlight' };
        // Per-block override of the gradient (else --cat-spot / theme default).
        if (block.color || block.color2) {
          var c1 = block.color || block.color2, c2 = block.color2 || block.color;
          attrs.style = '--cat-spot:' + (c1 === c2 ? c1 : 'linear-gradient(96deg,' + c1 + ',' + c2 + ')');
        }
        var sec = ctx.el('section', attrs);
        var inner = ctx.el('div', { 'class': 'catalog-spot-inner' });
        inner.appendChild(ctx.el('div', { 'class': 'catalog-spot-val', 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.value', text: block.value || '' }));
        inner.appendChild(ctx.el('div', { 'class': 'catalog-spot-lbl', 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.label', text: block.label || '' }));
        if (block.caption != null) inner.appendChild(ctx.el('div', { 'class': 'catalog-spot-cap', 'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.caption', text: block.caption || '' }));
        sec.appendChild(inner);
        return sec;
      }
    },

    // ---- Ranking (horizontal bars) ----
    ranking: {
      label: 'Ranking', category: 'Data', icon: '|',
      blank: function () { return { type: 'ranking', title: 'Top five', items: [ { label: 'First item', value: 290000 }, { label: 'Second item', value: 210000 }, { label: 'Third item', value: 150000 } ] }; },
      lists: { items: { label: 'Row', fields: [ { key: 'label', type: 'text', label: 'Label' }, { key: 'value', type: 'number', label: 'Value' } ] } },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section' });
        sec.appendChild(titleEl(block, ctx));
        var full = Array.isArray(block.items) ? block.items : [];
        var visible = full.filter(function (x) { return x && x.label != null; });
        var max = Math.max.apply(null, visible.map(function (x) { return +x.value || 0; }).concat([1]));
        var list = ctx.el('div', { 'class': 'catalog-ranking', 'data-edit-file': ctx.file, 'data-edit-list': ctx.path + '.items', 'data-edit-list-label': 'rows' });
        // Iterate the FULL array (skipping unlabelled rows) so each row carries
        // its true array index; the visible rank number uses a separate counter.
        var rank = 0;
        full.forEach(function (it, j) {
          if (!it || it.label == null) return;
          rank++;
          var ci = rank - 1;
          var row = ctx.el('div', { 'class': 'catalog-rankrow', 'data-item-index': j });
          row.appendChild(ctx.el('div', { 'class': 'catalog-rank-n', text: String(rank) }));
          var wrap = ctx.el('div', { 'class': 'catalog-rank-barwrap' });
          var fill = ctx.el('div', { 'class': 'catalog-rank-bar', style: 'width:' + Math.max(8, (+it.value || 0) / max * 100) + '%;background:linear-gradient(90deg,' + STATPAL[ci % STATPAL.length] + ',' + STATPAL[(ci + 2) % STATPAL.length] + ')' });
          fill.appendChild(ctx.el('span', { 'class': 'catalog-rank-lbl', text: it.label || '' }));
          wrap.appendChild(fill);
          row.appendChild(wrap);
          row.appendChild(ctx.el('div', { 'class': 'catalog-rank-val', text: chartFmt(+it.value || 0) }));
          list.appendChild(row);
        });
        sec.appendChild(list);
        return sec;
      }
    },

    // ---- Group (a container that nests other parts) ----
    group: {
      label: 'Group', category: 'Layout', icon: '▤',
      blank: function () { return { type: 'group', title: 'Group', children: [] }; },
      render: function (block, ctx) {
        var sec = ctx.el('section', { 'class': 'engine-section catalog-group' });
        sec.appendChild(titleEl(block, ctx));
        var kids = ctx.el('div', { 'class': 'catalog-group-children' });
        (Array.isArray(block.children) ? block.children : []).forEach(function (child, j) {
          var cn = normalize(child, j);
          if (!isVisible(cn)) return;
          // Recurse: a child is rendered exactly like a top-level block, with its
          // path extended (…children[j]) so editing/addressing still works.
          var childNode = renderOne(cn, { el: ctx.el, mediaUrl: ctx.mediaUrl, file: ctx.file, index: j, path: ctx.path + '.children[' + j + ']' });
          // Edit-only Up/Down controls to reorder this child within the group.
          var tools = ctx.el('div', { 'class': 'catalog-childtools' }, [
            ctx.el('button', { type: 'button', 'class': 'catalog-childmove', text: 'Up', 'data-idx': String(j), 'data-dir': '-1' }),
            ctx.el('button', { type: 'button', 'class': 'catalog-childmove', text: 'Down', 'data-idx': String(j), 'data-dir': '1' })
          ]);
          kids.appendChild(ctx.el('div', { 'class': 'catalog-childwrap' }, [tools, childNode]));
        });
        sec.appendChild(kids);
        var eo = ctx.el('div', { 'class': 'catalog-eo' });
        eo.appendChild(ctx.el('button', { type: 'button', 'class': 'catalog-addinside', text: '+ Add inside' }));
        sec.appendChild(eo);
        return sec;
      },
      init: function (node, block, ctx) {
        var btn = node.querySelector('.catalog-addinside');
        if (btn && !btn.__wired) {
          btn.__wired = true;
          btn.addEventListener('click', function () {
            if (window.EngineEdit && window.EngineEdit.addInside) window.EngineEdit.addInside(ctx.path);
          });
        }
        // Wire only THIS group's direct-child reorder buttons (not nested groups').
        var kidsC = node.querySelector('.catalog-group-children');
        if (kidsC) {
          [].forEach.call(kidsC.children, function (wrap) {
            var tools = wrap.querySelector(':scope > .catalog-childtools');
            if (!tools) return;
            [].forEach.call(tools.querySelectorAll('.catalog-childmove'), function (mb) {
              if (mb.__wired) return; mb.__wired = true;
              mb.addEventListener('click', function () {
                if (window.EngineEdit && window.EngineEdit.moveChild) window.EngineEdit.moveChild(ctx.path, parseInt(mb.getAttribute('data-idx'), 10), parseInt(mb.getAttribute('data-dir'), 10));
              });
            });
          });
        }
      }
    }
  };

  // ===========================================================================
  // Skeleton-facing API. The skeleton/editor only ever call THESE — they never
  // touch BLOCKS directly, so the registry can grow without anyone else changing.
  // ===========================================================================

  // Render one block: refresh the palette, look up its part (unknown type →
  // text), build the node, tag it with engine attrs, apply block + text styles,
  // then run the part's optional init().
  function renderOne(block, ctx) {
    ensureCSS();
    STATPAL = palette();  // pick up any per-site --cat-* palette override
    var def = BLOCKS[block.type] || BLOCKS.text;
    var node = def.render(block, ctx);
    if (node && node.setAttribute) {
      node.setAttribute('data-engine-block', block.id || ('' + ctx.index));
      node.setAttribute('data-engine-index', ctx.index);
      node.setAttribute('data-engine-path', ctx.path);
      if (window.EngineStyle && block.style) window.EngineStyle.apply(node, block.style);
      // Per-element text styling — MUST run after data-engine-path is set so
      // nested blocks' text isn't double-styled.
      if (window.EngineStyle && block.textStyles) window.EngineStyle.applyTextStyles(node, block, ctx.path);
    }
    if (def.init) { try { def.init(node, block, ctx); } catch (e) { /* non-fatal */ } }
    return node;
  }

  // Ensure a stored block has the envelope fields the engine relies on.
  function normalize(block, i) {
    block = (block && typeof block === 'object') ? block : {};
    if (!block.type) block.type = 'text';
    if (!block.id) block.id = 'b' + (i + 1) + '-' + Math.random().toString(36).slice(2, 7);
    return block;
  }
  function isVisible(block) { return !block || block.visible !== false; }

  // The picker list: every non-hidden, renderable part, with its display meta.
  function list() {
    return Object.keys(BLOCKS).filter(function (t) { return BLOCKS[t] && BLOCKS[t].render && !BLOCKS[t].hidden; }).map(function (t) {
      var d = BLOCKS[t];
      return { type: t, label: d.label || t, category: d.category || 'Other', icon: d.icon || '◧' };
    });
  }

  // Create a fresh block of a type (used when the editor adds one).
  function make(type) {
    var d = BLOCKS[type] || BLOCKS.text;
    var b = (d.blank ? d.blank() : {}) || {};
    b.type = b.type || type;
    if (!b.id) b.id = uid();
    return b;
  }

  // The edit schema for one of a block's list fields (e.g. carousel.slides).
  function listSchema(type, field) {
    var d = BLOCKS[type];
    return (d && d.lists && d.lists[field]) || null;
  }

  window.CatalogBlocks = BLOCKS;                                   // the registry
  window.Catalog = {
    types: BLOCKS, render: renderOne, normalize: normalize, isVisible: isVisible,
    list: list, make: make, listSchema: listSchema, uid: uid
  };
})();

// =============================================================================
// EXTENDED CATALOGUE — interactive, data-driven parts. GENERAL by construction:
// each talks ONLY to endpoints named in its own block config, so the same part
// serves any backend/dataset. No project knowledge, no project-specific names.
// =============================================================================
(function () {
  'use strict';
  if (!window.CatalogBlocks) return;
  function injectCSS(id, css) {
    if (document.getElementById(id)) return;
    var s = document.createElement('style'); s.id = id; s.textContent = css;
    document.head.appendChild(s);
  }

  // ---- shared: speech I/O. Browser Web Speech API by default (works now, no backend). A 'server' voice
  // POSTs to a TTS endpoint instead (for a custom/cloned voice once that backend is wired), and falls back
  // to the browser voice if the endpoint isn't ready. ----
  var Voice = {
    // speak `text`. opts.voice === 'server' POSTs to opts.endpoint for a custom/cloned voice, falling back
    // to the browser voice if the endpoint isn't ready; otherwise the browser voice speaks directly.
    // opts.onStatus(state, message) — OPTIONAL progress callback so a UI can surface voice feedback
    // (e.g. 'speaking…' / 'server voice unavailable — using browser voice'). GENERAL: the message strings
    // describe the voice transport only, with no project knowledge.
    speak: function (text, opts) {
      opts = opts || {};
      var status = (typeof opts.onStatus === 'function') ? opts.onStatus : function () {};
      if (opts.voice === 'server' && opts.endpoint) {
        status('speaking', 'speaking…');
        fetch(opts.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text: text, provider: opts.provider, voice: opts.ttsVoice }) })
          .then(function (r) { return (r.ok && (r.headers.get('Content-Type') || '').indexOf('audio') >= 0) ? r.blob() : null; })
          .then(function (b) {
            if (b) { new Audio(URL.createObjectURL(b)).play(); status('done', ''); }
            else { status('fallback', 'server voice unavailable — using browser voice.'); Voice._browser(text); }
          })
          .catch(function () { status('fallback', 'server voice unavailable — using browser voice.'); Voice._browser(text); });
      } else { status('browser', ''); Voice._browser(text); }
    },
    _browser: function (text) {
      if (!window.speechSynthesis) return;
      try { window.speechSynthesis.cancel(); var u = new SpeechSynthesisUtterance(text); u.rate = 0.95; window.speechSynthesis.speak(u); } catch (e) {}
    },
    canListen: function () { return !!(window.SpeechRecognition || window.webkitSpeechRecognition); },
    listen: function (onText, onEnd) {
      var R = window.SpeechRecognition || window.webkitSpeechRecognition; if (!R) { if (onEnd) onEnd(); return null; }
      var rec = new R(); rec.lang = 'en-US'; rec.interimResults = false; rec.maxAlternatives = 1;
      rec.onresult = function (e) { onText(e.results[0][0].transcript); };
      rec.onerror = function () { if (onEnd) onEnd(); };
      rec.onend = function () { if (onEnd) onEnd(); };
      try { rec.start(); } catch (e) { if (onEnd) onEnd(); }
      return rec;
    },
    canRecord: function () { return !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia && window.MediaRecorder); },
    // SERVER STT: record the mic, POST the clip to an endpoint, get back {text}. Works in every browser and
    // keeps audio off third-party servers. Returns a controller with .stop(). Falls back to the browser API.
    listenServer: function (endpoint, onText, onState, provider) {
      if (!Voice.canRecord()) { var r = Voice.listen(onText, function () { if (onState) onState(false); }); return { stop: function () { try { r && r.stop(); } catch (e) {} } }; }
      var ctrl = { rec: null, stopped: false };
      var url = endpoint + (provider ? (endpoint.indexOf('?') >= 0 ? '&' : '?') + 'provider=' + encodeURIComponent(provider) : '');
      navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
        if (ctrl.stopped) { stream.getTracks().forEach(function (t) { t.stop(); }); if (onState) onState(false); return; }
        var chunks = [], rec = new MediaRecorder(stream); ctrl.rec = rec;
        rec.ondataavailable = function (e) { if (e.data && e.data.size) chunks.push(e.data); };
        rec.onstop = function () {
          stream.getTracks().forEach(function (t) { t.stop(); });
          if (onState) onState('transcribing');
          var blob = new Blob(chunks, { type: rec.mimeType || 'audio/webm' });
          fetch(url, { method: 'POST', headers: { 'Content-Type': blob.type }, body: blob })
            .then(function (r) { return r.json(); })
            .then(function (d) { onText((d && d.text) || ''); if (onState) onState(false); })
            .catch(function () { if (onState) onState(false); });
        };
        rec.start(); if (onState) onState('recording');
      }).catch(function () { if (onState) onState('error'); });   // permission denied / no device / insecure
      return { stop: function () { ctrl.stopped = true; try { if (ctrl.rec && ctrl.rec.state !== 'inactive') ctrl.rec.stop(); } catch (e) {} } };
    }
  };

  // ---- shared: a slide-in entity DOSSIER (opened by clicking a graph node or a map marker) ----
  // A TABBED reading-room dossier — Overview / Connections / Sources / Biography. Each connection
  // carries TWO buttons (jump to the entity, or expand the connecting document(s) + LLM explanation);
  // each source passage opens its scanned region. Mirrors the OLD reading-room app exactly.
  injectCSS('catalog-dossier-css',
    // semi-transparent backdrop BEHIND the slide-in dossier (z-index just under it) — click it to
    // close, mirroring the OLD reading-room app. Fades in/out with the panel.
    '.catalog-dossier-bd{position:fixed;inset:0;background:rgba(0,0,0,.3);z-index:1999;opacity:0;pointer-events:none;transition:opacity .24s ease}' +
    '.catalog-dossier-bd.open{opacity:1;pointer-events:auto}' +
    '.catalog-dossier{position:fixed;top:0;right:0;height:100vh;width:min(500px,95vw);background:#fff;border-left:1px solid var(--provincial-mid,#ccc);box-shadow:-6px 0 28px rgba(60,22,5,.14);z-index:2000;transform:translateX(100%);transition:transform .24s ease;overflow-y:auto;padding:1.7em 1.8em;font-family:var(--font-body,sans-serif)}' +
    '.catalog-dossier.open{transform:translateX(0)}' +
    '.catalog-dossier .cd-close{position:absolute;top:.6em;right:.8em;border:0;background:none;font-size:1.7rem;line-height:1;cursor:pointer;color:var(--provincial-mid,#999)}' +
    '.catalog-dossier .cd-kind{font-size:.66rem;text-transform:uppercase;letter-spacing:.18em;color:var(--provincial-mid,#999);font-weight:700;margin-top:.3em}' +
    '.catalog-dossier h3{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);margin:.1em 0 .2em;font-size:1.55rem;line-height:1.15}' +
    '.catalog-dossier h4{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);font-size:.95rem;margin:1em 0 .3em}' +
    '.catalog-dossier .cd-sub{font-size:.74rem;color:var(--provincial-dark,#826962);margin:.15em 0 .2em}' +
    '.catalog-dossier .cd-desc{line-height:1.66;margin:.5em 0 .2em;color:var(--site-ink,#2A262A)}' +
    '.catalog-dossier .muted-note{font-size:12px;color:var(--provincial-dark,#826962);line-height:1.45}' +
    // tab strip
    '.catalog-dossier .ep-tabstrip{display:flex;gap:.2em;flex-wrap:wrap;border-bottom:1px solid var(--provincial-mid,#ccc);margin:1.1em 0 .9em}' +
    '.catalog-dossier .ep-tab{background:none;border:0;border-bottom:2px solid transparent;padding:.45em .7em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.72rem;letter-spacing:.04em;color:var(--provincial-dark,#826962);cursor:pointer}' +
    '.catalog-dossier .ep-tab:hover{color:var(--provincial,#3C1605)}' +
    '.catalog-dossier .ep-tab.active{color:var(--provincial,#3C1605);border-bottom-color:var(--provincial,#3C1605)}' +
    '.catalog-dossier .ep-tabpanel.hidden{display:none}' +
    // actions
    '.catalog-dossier .ep-actions{display:flex;flex-wrap:wrap;gap:.5em;margin:.4em 0 .2em}' +
    '.catalog-dossier .ep-btn{background:var(--provincial,#3C1605);color:#fff;border:0;border-radius:3px;padding:.5em 1em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.72rem;letter-spacing:.04em;text-transform:uppercase;cursor:pointer;text-decoration:none;display:inline-block}' +
    '.catalog-dossier .ep-btn:hover{background:var(--provincial-dark,#826962);color:#fff}' +
    // light text cards (record meta, source passages, biography)
    '.catalog-dossier .ep-text{background:var(--provincial-light,#f1ece7);border-radius:4px;padding:.6em .7em;margin:.5em 0}' +
    '.catalog-dossier .ep-text-meta{font-size:11px;color:var(--provincial-dark,#826962);margin-bottom:.3em;font-family:var(--font-head,sans-serif);font-weight:600}' +
    '.catalog-dossier .ep-open{display:inline-block;margin-top:.35em;font-size:12px;color:var(--provincial,#3C1605);cursor:pointer;font-weight:600}' +
    '.catalog-dossier .ep-open:hover{text-decoration:underline}' +
    // connection rows
    '.catalog-dossier .ep-rel{padding:.55em 0;border-bottom:1px dotted var(--provincial-mid,#ddd)}' +
    '.catalog-dossier .ep-rel-head{font-size:13px;line-height:1.4}' +
    '.catalog-dossier .ep-rel-head b{color:var(--provincial,#3C1605);font-weight:600}' +
    '.catalog-dossier .ep-rel-verb{color:var(--provincial-dark,#826962)}' +
    '.catalog-dossier .rel-dot{display:inline-block;width:.7em;height:.7em;border-radius:50%;margin-right:.4em;vertical-align:middle}' +
    '.catalog-dossier .rel-tag{font-size:10px;font-weight:600;padding:.05em .45em;border-radius:8px;margin-left:.35em;vertical-align:middle}' +
    '.catalog-dossier .rel-tag.coinc{background:var(--provincial-light,#eee);color:var(--provincial-dark,#826962)}' +
    '.catalog-dossier .rel-tag.real{background:#3f7d4e;color:#fff}' +
    '.catalog-dossier .ep-rel-btns{display:flex;gap:.5em;margin-top:.4em;flex-wrap:wrap}' +
    '.catalog-dossier .ep-rel-btn{background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd);border-radius:3px;padding:.35em .65em;font-size:12px;font-weight:600;cursor:pointer}' +
    '.catalog-dossier .ep-rel-btn:hover{background:var(--provincial-mid,#ddd)}' +
    '.catalog-dossier .ep-rel-detail{margin-top:.5em;padding-left:.2em}' +
    '.catalog-dossier .ep-rel-detail.hidden{display:none}' +
    '.catalog-dossier .ep-explain{background:#f3ece4;border-left:3px solid var(--provincial,#3C1605);border-radius:4px;padding:.55em .7em;font-size:13px;line-height:1.45;margin-bottom:.5em}');
  function _esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

  // Relationship verb styling — colour the dot by relation kind so a glance tells WROTE_TO from
  // APPEARS_WITH. CVD-safe (Okabe-Ito) palette, mirrors the OLD app.
  var EDGE_STYLE = {
    WROTE_TO: { color: '#2b6ca3', line: 'solid', arrow: true, label: 'wrote to' },
    APPEARS_WITH: { color: '#9c8e88', line: 'dashed', arrow: false, label: 'appears with' },
    LOCATED_AT: { color: '#d2792a', line: 'solid', arrow: true, label: 'located at' },
    HAS_CONDITION: { color: '#c0504d', line: 'solid', arrow: true, label: 'has condition' },
    ENROLLED: { color: '#3a9151', line: 'solid', arrow: true, label: 'enrolled' },
    HAS_OUTCOME: { color: '#3a9188', line: 'solid', arrow: true, label: 'has outcome' },
    FAMILY: { color: '#7b5b9a', line: 'solid', arrow: false, label: 'family' },
    MEMBER_OF: { color: '#b0894c', line: 'solid', arrow: true, label: 'member of' },
    CONTINUES_ON: { color: '#9c8e88', line: 'dotted', arrow: true, label: 'continues on' },
    MENTIONED_IN: { color: '#cfc4bf', line: 'dotted', arrow: false, label: 'mentioned in' },
    DATED_IN: { color: '#a98f6f', line: 'dotted', arrow: false, label: 'dated in' }
  };
  var EDGE_DEFAULT = { color: '#C9BDB8', line: 'solid', arrow: true, label: 'related' };
  function edgeStyleFor(k) { return EDGE_STYLE[(k || '').toUpperCase()] || EDGE_DEFAULT; }

  // ---- shared: a deep-zoom CITATION modal (OpenSeadragon) — opens the EXACT scanned region behind a
  // citation, draws a translucent highlight over the cited polygon, frames it with padding, and offers a
  // PDF download + a metadata sidebar. Mirrors the OLD reading-room app's mountViewer()/region modal.
  // GENERAL: it talks ONLY to the region endpoint named in block config — no project knowledge leaks in.
  injectCSS('catalog-citation-css',
    '.catalog-citebd{position:fixed;inset:0;background:rgba(60,22,5,.34);z-index:2400;display:none;align-items:stretch;justify-content:center;padding:3vh 3vw}' +
    '.catalog-citebd.open{display:flex}' +
    '.catalog-citemodal{background:#fff;border-radius:10px;width:min(1180px,97vw);max-height:94vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 22px 70px rgba(42,38,42,.5);position:relative}' +
    '.catalog-citehead{display:flex;align-items:baseline;gap:.8em;padding:.85em 1.2em;border-bottom:1px solid var(--provincial-mid,#ddd);flex:0 0 auto}' +
    '.catalog-citehead .ct-title{font-family:var(--font-head,sans-serif);font-weight:800;font-size:1.05rem;color:var(--provincial,#3C1605)}' +
    '.catalog-citehead .ct-meta{font-size:.72rem;color:var(--provincial-dark,#826962);font-family:var(--engine-mono,monospace)}' +
    '.catalog-citehead .ct-x{margin-left:auto;border:0;background:none;font-size:1.7rem;line-height:1;cursor:pointer;color:var(--provincial-mid,#999)}' +
    '.catalog-citebody{display:grid;grid-template-columns:1fr 380px;flex:1;min-height:0}' +
    '.catalog-citeosd{position:relative;background:#2A262A;min-height:340px}' +
    '.catalog-citeside{border-left:1px solid var(--provincial-mid,#ddd);padding:1.1em 1.2em;overflow-y:auto;background:var(--provincial-light,#f7f3ef)}' +
    '.catalog-citeside .reg-text{white-space:pre-wrap;line-height:1.6;font-size:.92rem;color:var(--site-ink,#2A262A);margin-bottom:.9em}' +
    '.catalog-citeside .reg-kv{font-size:.82rem;line-height:1.5;margin:.18em 0;color:var(--site-ink,#2A262A)}' +
    '.catalog-citeside .reg-kv.muted{font-size:.74rem;color:var(--provincial-dark,#826962)}' +
    '.catalog-citeside .reg-kv b{color:var(--provincial,#3C1605)}' +
    '.catalog-citefoot{flex:0 0 auto;padding:.7em 1.2em;border-top:1px solid var(--provincial-mid,#ddd);display:flex;gap:.7em;justify-content:flex-end;align-items:center}' +
    '.catalog-citefoot .ct-btn{background:var(--provincial,#3C1605);color:#fff;border:0;border-radius:3px;padding:.55em 1.3em;font-family:var(--font-head,sans-serif);font-weight:700;letter-spacing:.06em;text-transform:uppercase;font-size:.72rem;cursor:pointer;text-decoration:none}' +
    '.catalog-citefoot .ct-btn.secondary{background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd)}' +
    '.catalog-citefoot .ct-btn:hover{background:var(--provincial-dark,#826962);color:#fff}' +
    '.catalog-citebook{padding:1.4em 1.6em;overflow-y:auto;line-height:1.7;font-size:.98rem;color:var(--site-ink,#2A262A)}' +
    '@media(max-width:760px){.catalog-citebody{grid-template-columns:1fr}.catalog-citeside{border-left:0;border-top:1px solid var(--provincial-mid,#ddd);max-height:38vh}}');

  var OSD_JS = 'https://cdn.jsdelivr.net/npm/openseadragon@4.1.0/build/openseadragon/openseadragon.min.js';
  var OSD_PREFIX = 'https://cdn.jsdelivr.net/npm/openseadragon@4.1.0/build/openseadragon/images/';

  var Citation = {
    bd: null, modal: null, _osd: null,
    _ensure: function () {
      if (Citation.bd) return;
      var bd = document.createElement('div'); bd.className = 'catalog-citebd';
      var modal = document.createElement('div'); modal.className = 'catalog-citemodal';
      bd.appendChild(modal); document.body.appendChild(bd);
      bd.addEventListener('click', function (e) { if (e.target === bd) Citation.close(); });
      document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && bd.classList.contains('open')) Citation.close(); });
      Citation.bd = bd; Citation.modal = modal;
    },
    close: function () {
      if (Citation._osd) { try { Citation._osd.destroy(); } catch (e) {} Citation._osd = null; }
      if (Citation.bd) Citation.bd.classList.remove('open');
      document.body.style.overflow = '';
    },
    // a citation label for the modal header (mirrors the OLD app's sourceLabel)
    _label: function (c) {
      if (c.source === 'book') return (c.title || 'Thank God Ahead of Time') + (c.page != null ? ', p.' + c.page : '') + ' (Crosby — biography)';
      var kind = c.kind === 'notebook_entry' ? 'notebook' : (c.kind || 'source');
      var pg = (c.page != null) ? 'p.' + c.page : '';
      return ((c.section || c.doc_id || '') + ' ' + pg + ' (' + kind + ')').trim();
    },
    open: function (prov, opts) {
      opts = opts || {};
      Citation._ensure();
      var bd = Citation.bd, modal = Citation.modal, E = Dossier._el;
      modal.innerHTML = '';
      modal.appendChild(E('div', { 'class': 'catalog-citehead' }, [
        E('span', { 'class': 'ct-title', text: Citation._label(prov) }),
        E('span', { 'class': 'ct-meta', text: (prov.doc_id || '') + (prov.rid != null ? '  rid ' + prov.rid : '') }),
        (function () { var x = E('button', { 'class': 'ct-x', 'aria-label': 'close', text: '×' }); x.addEventListener('click', Citation.close); return x; })()
      ]));
      bd.classList.add('open');
      document.body.style.overflow = 'hidden';

      // BOOK citations have no scanned region — show the passage as text only, no viewer.
      if (prov.source === 'book') {
        var bookBody = E('div', { 'class': 'catalog-citebook' });
        bookBody.appendChild(E('p', { 'class': 'muted-note', text: 'Michael Crosby, Thank God Ahead of Time — published biography (secondary source). This citation is from the printed book, not the archive; there is no scanned page to deep-zoom.' }));
        bookBody.appendChild(E('p', { text: prov.preview || prov.snippet || prov.text || '' }));
        modal.appendChild(bookBody);
        return;
      }

      var body = E('div', { 'class': 'catalog-citebody' });
      var osdHost = E('div', { 'class': 'catalog-citeosd' });
      var side = E('div', { 'class': 'catalog-citeside' });
      side.appendChild(E('div', { 'class': 'reg-text', text: prov.snippet || prov.text || 'loading…' }));
      body.appendChild(osdHost); body.appendChild(side);
      modal.appendChild(body);
      var foot = E('div', { 'class': 'catalog-citefoot' });
      var dl = E('a', { 'class': 'ct-btn secondary', target: '_blank', rel: 'noopener', text: 'Download source PDF' });
      dl.style.display = 'none'; foot.appendChild(dl);
      var done = E('button', { 'class': 'ct-btn', text: 'Done' }); done.addEventListener('click', Citation.close);
      foot.appendChild(done); modal.appendChild(foot);

      var ep = opts.regionEndpoint || '/api/region';
      var url = ep + '?doc_id=' + encodeURIComponent(prov.doc_id || '') +
        (prov.rid != null ? '&rid=' + encodeURIComponent(prov.rid) : '') +
        (prov.page != null ? '&page=' + encodeURIComponent(prov.page) : '');
      fetch(url).then(function (r) { return r.json(); })
        .then(function (region) { Citation._fill(region, side, dl); Citation._mount(region, osdHost); })
        .catch(function () {
          // backend/region unavailable — fall back to whatever the citation itself carries
          var region = { text: prov.snippet || prov.text || '', vertices: prov.vertices || null, section: prov.section,
            page: prov.page, min_conf: prov.min_conf, image_width: prov.image_width, image_height: prov.image_height,
            image_url: (prov.section && prov.pdf_page) ? '/api/image/' + prov.section + '/' + prov.pdf_page : null,
            pdf_url: prov.section ? '/api/pdf/' + prov.section + '?page=' + (prov.pdf_page || '') : null };
          Citation._fill(region, side, dl); Citation._mount(region, osdHost);
        });
    },
    // the metadata sidebar: transcribed text first, then human context, then technical region details
    _fill: function (region, side, dl) {
      var E = Dossier._el;
      side.innerHTML = '';
      side.appendChild(E('div', { 'class': 'reg-text', text: region.text || '(no transcribed text for this region)' }));
      var rec = region.record || {};
      [['Date written', rec.date], ['Written from', rec.sent_from], ['Recipient', rec.recipient],
       ['Sent to', rec.sent_to], ['Notebook page', rec.page_label]].forEach(function (kv) {
        if (kv[1] == null || kv[1] === '') return;
        side.appendChild(E('div', { 'class': 'reg-kv' }, [E('b', { text: kv[0] + ': ' }), String(kv[1])]));
      });
      if (rec.multipage) side.appendChild(E('div', { 'class': 'reg-kv muted', text: 'This is one page of a multi-page letter; the scan shows the cited page.' }));
      var conf = (region.min_conf != null) ? (region.min_conf * 100).toFixed(1) + '%' : null;
      [['Source', region.section], ['Page', region.page], ['Region kind', region.category], ['OCR confidence', conf]].forEach(function (kv) {
        if (kv[1] == null || kv[1] === '') return;
        side.appendChild(E('div', { 'class': 'reg-kv muted' }, [E('b', { text: kv[0] + ': ' }), String(kv[1])]));
      });
      if (region.pdf_url) { dl.href = region.pdf_url; dl.style.display = ''; } else { dl.style.display = 'none'; }
    },
    // mount OpenSeadragon over the page image and draw the cited region highlight (loads OSD on demand)
    _mount: function (region, host) {
      if (Citation._osd) { try { Citation._osd.destroy(); } catch (e) {} Citation._osd = null; }
      host.innerHTML = '';
      if (!region.image_url) {
        host.appendChild(Dossier._el('div', { style: 'color:#fff;padding:1.2em;font-style:italic', text: 'Page image unavailable — start the backend so the scanned page can be served.' }));
        return;
      }
      loadScriptOnce(OSD_JS).then(function () {
        if (!window.OpenSeadragon) { host.innerHTML = '<div style="color:#fff;padding:1.2em">viewer failed to load</div>'; return; }
        var osd = window.OpenSeadragon({
          element: host, prefixUrl: OSD_PREFIX,
          tileSources: { type: 'image', url: region.image_url, buildPyramid: true },
          showNavigator: true, navigatorPosition: 'BOTTOM_RIGHT',
          gestureSettingsMouse: { clickToZoom: false }, visibilityRatio: 1, minZoomImageRatio: 0.5
        });
        Citation._osd = osd;
        osd.addHandler('open', function () {
          var verts = region.vertices;
          if (!verts || !verts.length) { osd.viewport.goHome(true); return; }
          var size = osd.world.getItemAt(0).getContentSize();
          var W = region.image_width || size.x, H = region.image_height || size.y;
          var xs = verts.map(function (p) { return p[0]; }), ys = verts.map(function (p) { return p[1]; });
          var x = Math.min.apply(null, xs), y = Math.min.apply(null, ys);
          var w = Math.max.apply(null, xs) - x, h = Math.max.apply(null, ys) - y;
          // degenerate / out-of-bounds polygon (bad OCR geometry) → fit the whole page instead of a sliver
          if (w <= 0 || h <= 0 || x < 0 || y < 0 || x + w > W * 1.02 || y + h > H * 1.02) { osd.viewport.goHome(true); return; }
          var rect = osd.viewport.imageToViewportRectangle(new window.OpenSeadragon.Rect(x, y, w, h));
          var bx = document.createElement('div');
          bx.style.border = '3px solid #3C1605';
          bx.style.background = 'rgba(60,22,5,.14)';
          bx.style.boxShadow = '0 0 0 9999px rgba(42,38,42,.30)';
          bx.style.borderRadius = '2px';
          osd.addOverlay({ element: bx, location: rect });
          var pad = 0.30;   // ~1.6x area, centred on the region
          osd.viewport.fitBoundsWithConstraints(new window.OpenSeadragon.Rect(
            rect.x - rect.width * pad, rect.y - rect.height * pad,
            rect.width * (1 + 2 * pad), rect.height * (1 + 2 * pad)), false);
        });
      }).catch(function () { host.innerHTML = '<div style="color:#fff;padding:1.2em">viewer failed to load</div>'; });
    }
  };

  // open the scanned region for a citation in the deep-zoom modal (book citations show text only).
  function openCitation(prov, opts) {
    if (!prov || (!prov.doc_id && prov.source !== 'book')) return;
    Citation.open(prov, opts || {});
  }

  var Dossier = {
    el: null, bd: null, _id: null, _opts: {},
    _wire: function (d) { var c = d.querySelector('.cd-close'); if (c) c.addEventListener('click', Dossier.close); },
    _ensure: function () {
      if (Dossier.el) return Dossier.el;
      // backdrop FIRST (lower in the DOM + z-index) so the panel paints over it; click-outside closes.
      var bd = document.createElement('div'); bd.className = 'catalog-dossier-bd';
      bd.addEventListener('click', Dossier.close);
      document.body.appendChild(bd); Dossier.bd = bd;
      var d = document.createElement('aside'); d.className = 'catalog-dossier';
      document.body.appendChild(d); Dossier.el = d;
      // Escape closes the dossier (matches the citation modal's keyboard behaviour).
      document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && d.classList.contains('open')) Dossier.close(); });
      return d;
    },
    close: function () { if (Dossier.el) Dossier.el.classList.remove('open'); if (Dossier.bd) Dossier.bd.classList.remove('open'); },
    // small DOM helper (mirrors ctx.el so the dossier can build nodes without a render ctx)
    _el: function (tag, attrs, kids) {
      var n = document.createElement(tag); attrs = attrs || {};
      for (var k in attrs) { if (k === 'text') n.textContent = attrs[k]; else if (k === 'html') n.innerHTML = attrs[k]; else n.setAttribute(k, attrs[k]); }
      (kids || []).forEach(function (c) { if (c != null) n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
      return n;
    },
    // opts: { entityEndpoint, connectionEndpoint, regionEndpoint, onCenter } — all from block config.
    open: function (endpoint, id, opts) {
      var d = Dossier._ensure();
      Dossier._id = id; Dossier._opts = opts || { entityEndpoint: endpoint };
      d.innerHTML = '<button class="cd-close" aria-label="close">×</button><div class="cd-desc">loading…</div>';
      Dossier._wire(d); d.classList.add('open'); if (Dossier.bd) Dossier.bd.classList.add('open');
      fetch(endpoint + (endpoint.indexOf('?') >= 0 ? '&' : '?') + 'id=' + encodeURIComponent(id))
        .then(function (r) { return r.json(); })
        .then(function (e) { if (Dossier._id !== id) return; Dossier._render(e, id); })
        .catch(function () { d.innerHTML = '<button class="cd-close">×</button><div class="cd-desc">failed to load</div>'; Dossier._wire(d); });
    },
    _render: function (e, id) {
      var E = Dossier._el, opts = Dossier._opts, d = Dossier.el;
      d.innerHTML = '';
      d.appendChild(E('button', { 'class': 'cd-close', 'aria-label': 'close', text: '×' }));
      d.appendChild(E('div', { 'class': 'cd-kind', text: (e.kind || '') + (e.mention_count != null ? ' · ' + e.mention_count + ' mentions' : '') }));
      d.appendChild(E('h3', { text: e.label || id }));
      var subBits = [e.location, (e.mention_count != null ? e.mention_count + ' mentions' : '')].filter(Boolean);
      if (e.location) d.appendChild(E('div', { 'class': 'cd-sub', text: e.location }));

      var sections = [];

      // ---- OVERVIEW ----
      var ov = E('div', {});
      var actions = E('div', { 'class': 'ep-actions' });
      if (opts.onCenter) {
        var center = E('button', { 'class': 'ep-btn', text: 'Center the graph on this' });
        center.addEventListener('click', function () { opts.onCenter(e.label || id); Dossier.close(); });
        actions.appendChild(center);
      }
      if (e.lat != null && e.lon != null) {
        actions.appendChild(E('a', { 'class': 'ep-btn', target: '_blank', rel: 'noopener',
          href: 'https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=' + e.lat + ',' + e.lon, text: 'Open Street View' }));
      }
      if (actions.children.length) ov.appendChild(actions);
      var summ = e.description || e.what_it_is;
      if (summ) { ov.appendChild(E('h4', { text: 'Summary' })); ov.appendChild(E('p', { 'class': 'cd-desc', text: summ })); }
      var facts = [];
      if (e.role) facts.push('Role: ' + e.role);
      if (e.relation_to_solanus && ['unknown', 'none', ''].indexOf(String(e.relation_to_solanus)) < 0)
        facts.push('Relation to Fr. Solanus: ' + String(e.relation_to_solanus).replace(/_/g, ' '));
      if (e.location) facts.push('Location: ' + e.location);
      if (facts.length) ov.appendChild(E('p', { 'class': 'muted-note', text: facts.join('  -  ') }));
      if (e.variants && e.variants.length)
        ov.appendChild(E('p', { 'class': 'muted-note', text: 'Also written: ' + e.variants.slice(0, 8).join(', ') }));
      var rec = e.record || {};
      if (rec.recipient || rec.sent_from || rec.sent_to || rec.date) {
        var box = E('div', { 'class': 'ep-text' });
        [['When', rec.date], ['Written from', rec.sent_from], ['Recipient', rec.recipient], ['Sent to', rec.sent_to]].forEach(function (kv) {
          if (kv[1]) box.appendChild(E('div', {}, [E('b', { text: kv[0] + ': ' }), kv[1]]));
        });
        ov.appendChild(box);
      }
      sections.push({ key: 'overview', label: 'Overview', node: ov });

      // ---- CONNECTIONS ----
      var rels = e.relations || [];
      if (rels.length) {
        var cn = E('div', {});
        cn.appendChild(E('p', { 'class': 'muted-note', text: 'Each connection: one button jumps to that entity; the other shows the actual document(s) that link them (with a short explanation).' }));
        rels.slice().sort(function (a, b) { return String(a.when || '~').localeCompare(String(b.when || '~')); })
          .forEach(function (r) { cn.appendChild(Dossier._connectionRow(r, id)); });
        sections.push({ key: 'connections', label: 'Connections (' + rels.length + ')', node: cn });
      }

      // ---- SOURCES ----
      var sr = E('div', {});
      (e.texts || []).forEach(function (t) {
        var card = E('div', { 'class': 'ep-text' });
        card.appendChild(E('div', { 'class': 'ep-text-meta',
          text: (t.section || t.doc_id || '') + '  p.' + (t.page != null ? t.page : '?') + (t.date ? '  (' + t.date + ')' : '') }));
        card.appendChild(E('div', { text: (t.text || t.surface || '').slice(0, 700) }));
        var open = E('span', { 'class': 'ep-open', text: 'Open the scan' });
        open.addEventListener('click', function () { openCitation({ doc_id: t.doc_id, rid: t.rid, page: t.page, pdf_page: t.pdf_page, section: t.section, vertices: t.vertices, kind: e.kind, snippet: t.text }, opts); });
        card.appendChild(open); sr.appendChild(card);
      });
      if (!(e.texts || []).length) sr.appendChild(E('p', { 'class': 'muted-note', text: 'No source passages found.' }));
      sections.push({ key: 'sources', label: 'Sources (' + (e.texts || []).length + ')', node: sr });

      // ---- BIOGRAPHY ----
      if (e.book && e.book.length) {
        var bk = E('div', {});
        bk.appendChild(E('p', { 'class': 'muted-note', text: 'Michael Crosby, Thank God Ahead of Time — a published biography (secondary source).' }));
        e.book.forEach(function (b) {
          var card = E('div', { 'class': 'ep-text' });
          card.appendChild(E('div', { 'class': 'ep-text-meta', text: (b.title || 'Thank God Ahead of Time') + (b.page != null ? ', p. ' + b.page : '') }));
          card.appendChild(E('div', { text: (b.text || '').slice(0, 520) }));
          bk.appendChild(card);
        });
        sections.push({ key: 'book', label: 'Biography (' + e.book.length + ')', node: bk });
      }

      d.appendChild(Dossier._tabs(sections));
      Dossier._wire(d);
    },
    _tabs: function (sections) {
      var E = Dossier._el;
      var wrap = E('div', { 'class': 'ep-tabs' });
      var strip = E('div', { 'class': 'ep-tabstrip' });
      var panels = E('div', { 'class': 'ep-tabpanels' });
      sections.forEach(function (s, i) {
        var btn = E('button', { 'class': 'ep-tab' + (i === 0 ? ' active' : ''), text: s.label });
        var panel = E('div', { 'class': 'ep-tabpanel' + (i === 0 ? '' : ' hidden') });
        panel.appendChild(s.node);
        btn.addEventListener('click', function () {
          [].forEach.call(strip.querySelectorAll('.ep-tab'), function (x) { x.classList.remove('active'); });
          [].forEach.call(panels.querySelectorAll('.ep-tabpanel'), function (x) { x.classList.add('hidden'); });
          btn.classList.add('active'); panel.classList.remove('hidden');
        });
        strip.appendChild(btn); panels.appendChild(panel);
      });
      wrap.appendChild(strip); wrap.appendChild(panels);
      return wrap;
    },
    _connectionRow: function (r, fromId) {
      var E = Dossier._el, opts = Dossier._opts;
      var row = E('div', { 'class': 'ep-rel' });
      var head = E('div', { 'class': 'ep-rel-head' });
      head.appendChild(E('span', { 'class': 'rel-dot', style: 'background:' + edgeStyleFor(r.rel_kind || r.rel).color }));
      var verb = r.subtype || ((r.verified === 'real' && r.relationship && r.relationship !== 'co-listed') ? r.relationship : (r.rel || 'related'));
      head.appendChild(E('span', { 'class': 'ep-rel-verb', text: verb + ' ' }));
      head.appendChild(E('b', { text: r.other || r.other_id || '' }));
      if (r.rel_kind === 'APPEARS_WITH' && r.verified === 'coincidental')
        head.appendChild(E('span', { 'class': 'rel-tag coinc', text: ' same-page only' }));
      else if (r.verified === 'real')
        head.appendChild(E('span', { 'class': 'rel-tag real', text: ' verified' }));
      var meta = [];
      if (r.when) meta.push(r.when);
      if (r.weight) meta.push(r.weight + ' shared records');
      if (meta.length) head.appendChild(E('span', { 'class': 'muted-note', text: '  (' + meta.join(', ') + ')' }));
      row.appendChild(head);

      var btns = E('div', { 'class': 'ep-rel-btns' });
      var go = E('button', { 'class': 'ep-rel-btn', text: 'Go to this entity' });
      go.addEventListener('click', function () { Dossier.open(opts.entityEndpoint || '/api/entity', r.other_id, opts); });
      var detail = E('div', { 'class': 'ep-rel-detail hidden' });
      var docBtn = E('button', { 'class': 'ep-rel-btn', text: 'Connecting document(s)' });
      docBtn.addEventListener('click', function () { Dossier._toggleConn(fromId, r, detail, docBtn); });
      btns.appendChild(go); btns.appendChild(docBtn);
      row.appendChild(btns); row.appendChild(detail);
      return row;
    },
    _toggleConn: function (fromId, r, detail, btn) {
      var E = Dossier._el, opts = Dossier._opts;
      if (detail.dataset.loaded) {
        detail.classList.toggle('hidden');
        btn.textContent = detail.classList.contains('hidden') ? 'Connecting document(s)' : 'Hide document(s)';
        return;
      }
      detail.classList.remove('hidden'); btn.textContent = 'Hide document(s)';
      detail.textContent = 'Loading the connecting document(s)...';
      var ep = opts.connectionEndpoint || '/api/connection';
      fetch(ep + '?a=' + encodeURIComponent(fromId) + '&b=' + encodeURIComponent(r.other_id))
        .then(function (res) { return res.json(); })
        .then(function (c) {
          detail.innerHTML = '';
          if (c.explanation) detail.appendChild(E('div', { 'class': 'ep-explain' }, [E('b', { text: 'How they are connected: ' }), c.explanation]));
          var docs = c.documents || [];
          if (docs.length) {
            detail.appendChild(E('div', { 'class': 'muted-note', style: 'margin:.5em 0 .2em', text: 'The document(s) that link them — open to read the page yourself:' }));
            docs.forEach(function (doc) {
              var card = E('div', { 'class': 'ep-text' });
              card.appendChild(E('div', { 'class': 'ep-text-meta', text: doc.cite_label || doc.doc_id || '' }));
              card.appendChild(E('div', { text: (doc.text || '').slice(0, 500) }));
              var open = E('span', { 'class': 'ep-open', text: 'Open the scan' });
              open.addEventListener('click', function () { openCitation({ doc_id: doc.doc_id, rid: doc.rid, page: doc.page, pdf_page: doc.pdf_page, section: doc.section, snippet: doc.text }, opts); });
              card.appendChild(open); detail.appendChild(card);
            });
          } else {
            detail.appendChild(E('p', { 'class': 'muted-note', text: 'No single connecting document was found for this link.' }));
          }
          detail.dataset.loaded = '1';
        })
        .catch(function () { detail.textContent = 'Could not load the connection.'; });
    }
  };

  // ---- conversation: a cited chat panel wired to a RAG-style POST endpoint ----
  // Static config used ONLY when the config endpoint is unreachable, so the model/tool pickers are never
  // empty while the backend is down. GENERAL: it's a plain default shape (model lists + tool toggles); a
  // block can override it via block.fallbackConfig. Mirrors the OLD app's FALLBACK_CONFIG.
  var CONV_FALLBACK_CONFIG = {
    llms: ['gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.5-pro',
           'gpt-5.5', 'gpt-5.4', 'gpt-5.4-mini', 'claude-opus-4-8', 'claude-sonnet-4-6'],
    embeddings: ['gemini-embedding-001@1536', 'gemini-embedding-001@768', 'gemini-embedding-001@3072',
                 'text-embedding-3-small@1536', 'text-embedding-3-large@3072', 'voyage-4-large@1024',
                 'voyage-4@1024', 'voyage-4-lite@1024', 'bge-large-en-v1.5@1024'],
    rerankers: ['rerank-2.5', 'rerank-2.5-lite', 'rerank-4-pro', 'bge-reranker-v2-m3'],
    providers: { llms: {}, embeddings: {}, rerankers: {} }, availability: {},
    defaults: { llm: 'gemini-2.5-flash', embedding: 'gemini-embedding-001@1536', reranker: 'rerank-2.5' },
    tools: [
      { key: 'use_bm25', label: 'BM25 (lexical)', default: true, paid: false, hint: 'sparse keyword' },
      { key: 'use_dense', label: 'Dense (semantic)', default: true, paid: false, hint: 'vector search' },
      { key: 'use_rerank', label: 'Rerank', default: false, paid: true, hint: 'cross-encoder' },
      { key: 'use_hyde', label: 'HyDE', default: false, paid: true, hint: 'hypothetical doc' },
      { key: 'use_multiquery', label: 'Multi-query (RAG-Fusion)', default: false, paid: true, hint: 'paraphrase + RRF' },
      { key: 'use_llm_router', label: 'LLM router', default: false, paid: true, hint: 'complexity routing' },
      { key: 'auto_apply_route', label: 'Auto-escalate route', default: false, paid: true, hint: 'let router spend' }
    ]
  };

  injectCSS('catalog-conversation-css',
    '.catalog-conversation{display:flex;gap:.7em;align-items:stretch}' +
    '.catalog-conv-rail{flex:0 0 208px;width:208px;background:var(--provincial,#3C1605);color:var(--provincial-light,#E5DFDE);border-radius:8px;display:flex;flex-direction:column;gap:.4em;padding:.9em .7em;overflow:hidden;transition:flex-basis .2s,width .2s,padding .2s}' +
    '.catalog-conv-rail.collapsed{flex-basis:0;width:0;padding:0}' +
    '.catalog-conv-new{font-family:var(--font-head,inherit);font-weight:700;font-size:11px;letter-spacing:.06em;text-transform:uppercase;background:var(--provincial-light,#E5DFDE);color:var(--provincial,#3C1605);border:0;border-radius:4px;padding:.65em;cursor:pointer;white-space:nowrap}' +
    '.catalog-conv-rlabel{font-family:var(--font-head,inherit);font-weight:800;font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--provincial-mid,#B3A29D);margin:.7em 0 .1em}' +
    '.catalog-conv-hist{display:flex;flex-direction:column;gap:1px;overflow-y:auto}' +
    '.catalog-conv-hitem{font-size:12px;color:var(--provincial-light,#E5DFDE);background:none;border:0;text-align:left;padding:.45em .5em;border-radius:4px;cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--font-body,sans-serif)}' +
    '.catalog-conv-hitem:hover{background:rgba(255,255,255,.1)}' +
    '.catalog-conv-hitem.active{background:rgba(255,255,255,.18);color:#fff}' +
    '.catalog-conv-hempty{font-size:11px;color:var(--provincial-mid,#B3A29D);font-style:italic;padding:.3em .5em}' +
    '.catalog-conv-toggle{flex:0 0 12px;width:12px;align-self:stretch;background:var(--provincial-light,#E5DFDE);border:0;border-radius:4px;cursor:pointer}' +
    '.catalog-conv-toggle:hover{background:var(--provincial-mid,#B3A29D)}' +
    '.catalog-conv-main{flex:1;min-width:0;display:flex;flex-direction:column;gap:.8em}' +
    '.catalog-conv-log{flex:1;min-height:360px;max-height:76vh;overflow-y:auto;display:flex;flex-direction:column;gap:1.5em;padding:.4em .2em}' +
    '.catalog-conv-empty{color:var(--provincial-mid,#999);font-style:italic}' +
    '.catalog-conv-msg{line-height:1.65;max-width:82ch;animation:catalog-msg-rise .5s cubic-bezier(.2,.7,.2,1) both}' +
    '@keyframes catalog-msg-rise{from{opacity:0;transform:translateY(8px)}}' +
    '.catalog-conv-msg.user{font-family:var(--font-head,inherit);font-weight:600;font-size:1.18rem;color:var(--provincial,#3C1605)}' +
    '.catalog-conv-msg.user::before{content:"Asked";display:block;font-size:.62rem;letter-spacing:.22em;text-transform:uppercase;color:var(--provincial-mid,#999);margin-bottom:.25em}' +
    // decorative provincial-coloured rule under the user question (mirrors the OLD app)
    '.catalog-conv-msg.user::after{content:"";display:block;width:2.4rem;height:2px;background:var(--provincial,#3C1605);margin:.7em 0 0}' +
    '.catalog-conv-msg.bot{font-size:1.04rem;color:var(--site-ink,#2A262A);white-space:pre-wrap}' +
    // loading spinner shown in the pending bot bubble while retrieving + grading
    '.catalog-conv-spin{display:inline-block;width:1em;height:1em;border:2px solid var(--provincial-mid,#B3A29D);border-top-color:var(--provincial,#3C1605);border-radius:50%;animation:catalog-conv-spin .7s linear infinite;vertical-align:-.15em;margin-right:.55em}' +
    '@keyframes catalog-conv-spin{to{transform:rotate(360deg)}}' +
    '.catalog-conv-pending{color:var(--provincial-dark,#826962);font-style:italic;font-size:.95rem}' +
    // clear-chat control in the rail (resets the current conversation log)
    '.catalog-conv-clear{font-family:var(--font-head,inherit);font-weight:700;font-size:10px;letter-spacing:.05em;text-transform:uppercase;background:none;color:var(--provincial-mid,#B3A29D);border:1px solid var(--provincial-mid,#826962);border-radius:4px;padding:.5em;cursor:pointer;white-space:nowrap}' +
    '.catalog-conv-clear:hover{background:rgba(255,255,255,.08);color:var(--provincial-light,#E5DFDE)}' +
    '.catalog-conv-htime{display:block;font-size:9px;color:var(--provincial-mid,#B3A29D);margin-top:1px;font-family:var(--font-body,sans-serif)}' +
    '.catalog-conv-sources{font-size:.8rem;border-top:1px solid var(--provincial-mid,#ccc);margin-top:.6em;padding-top:.5em;color:var(--provincial-dark,#666)}' +
    '.catalog-conv-sources-h{font-weight:700;text-transform:uppercase;letter-spacing:.16em;font-size:.62rem;margin-bottom:.35em}' +
    '.catalog-conv-source{margin:.25em 0;line-height:1.4}' +
    '.catalog-conv-cn{color:var(--provincial,#3C1605);font-weight:700}' +
    '.catalog-conv-cref{color:var(--provincial,#3C1605);font-weight:700;cursor:pointer;padding:0 1px}' +
    '.catalog-conv-cref:hover{text-decoration:underline}' +
    '.catalog-conv-source.hl{background:var(--provincial-light,#f1e9e5);border-radius:3px;transition:background .3s}' +
    // clickable source rows (open the scanned region) — mirrors the OLD app's .source-item
    '.catalog-conv-source.clickable{cursor:pointer}' +
    '.catalog-conv-source.clickable:hover .catalog-conv-snip{color:var(--site-ink,#2A262A)}' +
    '.catalog-conv-clabel{color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-snip{color:var(--provincial-mid,#999)}' +
    // grade badge (Self-RAG/CRAG verdict) below the answer
    '.catalog-conv-grade{display:inline-block;margin-top:.7em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.62rem;letter-spacing:.12em;text-transform:uppercase;padding:.15em .6em;border:1px solid currentColor;border-radius:2px;color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-grade.answer{color:#3f7d4e}' +
    '.catalog-conv-grade.caveat{color:#a4742a}' +
    '.catalog-conv-grade.abstain{color:#9c3b2e}' +
    '.catalog-conv-row{display:flex;gap:.6em;align-items:center;border-top:2px solid var(--provincial,#3C1605);padding-top:.7em}' +
    '.catalog-conv-input{flex:1;border:0;border-bottom:1px solid var(--provincial-mid,#ccc);background:transparent;font-size:1.2rem;padding:.5em .2em;font-family:var(--font-head,inherit);color:var(--site-ink,#2A262A)}' +
    '.catalog-conv-input:focus{outline:none;border-bottom-color:var(--provincial,#3C1605)}' +
    '.catalog-conv-send{background:var(--provincial,#3C1605);color:#fff;border:0;border-radius:3px;padding:.7em 1.5em;font-family:var(--font-head,inherit);font-weight:700;letter-spacing:.08em;text-transform:uppercase;font-size:.78rem;cursor:pointer}' +
    '.catalog-conv-send:hover{background:var(--provincial-dark,#826962)}' +
    '.catalog-conv-controls{display:flex;align-items:center;gap:1.4em;flex-wrap:wrap;font-size:.82rem;color:var(--provincial-dark,#826962);padding:.2em 0 .1em}' +
    '.catalog-conv-mic{background:none;border:0;padding:0;cursor:pointer;color:var(--provincial,#3C1605);text-decoration:underline;text-underline-offset:2px;font-family:var(--font-body,inherit);font-size:.85rem}' +
    '.catalog-conv-mic:hover{color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-mic.listening{color:#b5462f;font-weight:700;text-decoration:none}' +
    '.catalog-conv-aloud{display:inline-flex;align-items:center;gap:.4em;cursor:pointer;white-space:nowrap}' +
    '.catalog-conv-plabel{display:inline-flex;align-items:center;gap:.45em;white-space:nowrap}' +
    '.catalog-conv-provider{border:0;border-bottom:1px solid var(--provincial-mid,#ccc);background:transparent;font-size:.8rem;color:var(--provincial,#3C1605);padding:.2em .1em;cursor:pointer;font-family:var(--font-body,inherit)}' +
    // streaming toggle + spoken-voice status feedback (mirrors the OLD app's #stream-on + #voice-status)
    '.catalog-conv-stream{display:inline-flex;align-items:center;gap:.4em;cursor:pointer;white-space:nowrap}' +
    '.catalog-conv-vstatus{font-size:.74rem;color:var(--provincial-dark,#826962);font-style:italic;min-height:1em}' +
    // settings drawer: model/embedding/reranker pickers + retrieval-tool toggles, wired into /api/ask
    '.catalog-conv-settings-btn{background:none;border:0;padding:0;cursor:pointer;color:var(--provincial,#3C1605);text-decoration:underline;text-underline-offset:2px;font-family:var(--font-body,inherit);font-size:.85rem}' +
    '.catalog-conv-settings-btn:hover{color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-settings{display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:2100;width:min(660px,93vw);max-height:88vh;overflow-y:auto;border:1px solid var(--provincial-mid,#ccc);border-radius:10px;padding:1.5em 1.7em;background:#fff;box-shadow:0 14px 54px rgba(42,38,42,.34)}' +
    '.catalog-conv-settings.open{display:block}' +
    '.catalog-conv-settings-bd{display:none;position:fixed;inset:0;background:rgba(42,38,42,.42);z-index:2099}' +
    '.catalog-conv-settings-bd.open{display:block}' +
    '.catalog-conv-settings h4{font-family:var(--font-head,sans-serif);font-size:.72rem;text-transform:uppercase;letter-spacing:.1em;color:var(--provincial-dark,#826962);margin:.2em 0 .6em}' +
    '.catalog-conv-fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.7em;margin-bottom:.9em}' +
    '.catalog-conv-field{display:flex;flex-direction:column;gap:.25em;font-size:.72rem;color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-field span{font-family:var(--font-head,sans-serif);font-weight:700;letter-spacing:.04em}' +
    '.catalog-conv-field select{border:1px solid var(--provincial-mid,#ccc);border-radius:3px;padding:.4em;background:#fff;font-size:.8rem;font-family:var(--font-body,inherit)}' +
    '.catalog-conv-toggles{display:flex;flex-direction:column;gap:.45em}' +
    '.catalog-conv-tog{display:flex;flex-direction:column;gap:.15em;background:#fff;border:1px solid var(--provincial-mid,#ddd);border-radius:5px;padding:.5em .65em;cursor:pointer;text-align:left}' +
    '.catalog-conv-tog .tog-head{display:flex;align-items:center;justify-content:space-between;gap:.6em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.82rem;color:var(--provincial,#3C1605)}' +
    '.catalog-conv-tog .tog-pip{font-size:.62rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;border-radius:8px;padding:.1em .5em;background:var(--provincial-mid,#ccc);color:#fff}' +
    '.catalog-conv-tog.on .tog-pip{background:var(--provincial,#3C1605)}' +
    '.catalog-conv-tog .tog-desc{font-size:.72rem;color:var(--provincial-dark,#826962);line-height:1.4;font-weight:400}' +
    '.catalog-conv-settings .cs-headrow{display:flex;align-items:baseline;justify-content:space-between;gap:.6em}' +
    '.catalog-conv-settings .cs-x{border:0;background:none;font-size:1.5rem;line-height:1;cursor:pointer;color:var(--provincial-mid,#999);padding:0 .1em}' +
    '.catalog-conv-settings .cs-x:hover{color:var(--provincial,#3C1605)}' +
    '.catalog-conv-settings .cs-header{font-family:var(--font-head,sans-serif);font-weight:800;font-size:.92rem;color:var(--provincial,#3C1605);margin:.1em 0 .15em}' +
    '.catalog-conv-settings .cs-intro{font-size:.76rem;color:var(--provincial-dark,#826962);line-height:1.45;margin:0 0 .9em}' +
    '.catalog-conv-settings .cs-note{font-size:.68rem;color:var(--provincial-dark,#826962);line-height:1.4;margin:.3em 0 .2em;grid-column:1/-1}' +
    '.catalog-conv-settings .cs-field-note{font-size:.66rem;color:var(--provincial-dark,#826962);line-height:1.35;margin:.25em 0 0;font-weight:400}' +
    '.catalog-conv-settings .cs-persona-edit{font-size:.66rem;font-weight:600;text-transform:none;letter-spacing:0}' +
    '.catalog-conv-settings .cs-system{width:100%;box-sizing:border-box;margin-top:.5em;border:1px solid var(--provincial-mid,#ccc);border-radius:4px;padding:.5em .6em;font-family:var(--font-body,sans-serif);font-size:.8rem;line-height:1.5;color:var(--site-ink,#2A262A);background:#fff;resize:vertical}' +
    // agent trace ("under the hood"): route decision, queries fused, tool steps, cost ledger
    '.catalog-conv-trace{margin-top:.6em;border-top:1px dotted var(--provincial-mid,#ccc);padding-top:.5em;font-size:.78rem;color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-trace summary{cursor:pointer;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.62rem;letter-spacing:.14em;text-transform:uppercase;color:var(--provincial-dark,#826962)}' +
    '.catalog-conv-trace ol{margin:.5em 0 .3em;padding-left:1.3em;line-height:1.55}' +
    '.catalog-conv-trace li{margin:.2em 0}' +
    '.catalog-conv-trace .tr-k{font-family:var(--engine-mono,monospace);font-weight:700;color:var(--provincial,#3C1605);text-transform:uppercase;font-size:.66rem;letter-spacing:.04em}' +
    '.catalog-conv-trace .tr-cost{margin-top:.35em;font-family:var(--engine-mono,monospace);font-size:.72rem;color:var(--provincial-dark,#826962)}');

  window.CatalogBlocks['conversation'] = {
    label: 'Conversation', category: 'Interactive', icon: 'Q',
    blank: function () {
      return { type: 'conversation', endpoint: '/api/ask', queryField: 'query',
               answerField: 'answer', citationsField: 'citations', placeholder: 'Ask a question…',
               intro: '', history: true, settings: true, configEndpoint: '/api/config',
               personas: true, personasEndpoint: '/api/personas', trace: true, streaming: false };
    },
    fields: [
      { key: 'endpoint', type: 'text', label: 'POST endpoint' },
      { key: 'queryField', type: 'text', label: 'Request field' },
      { key: 'answerField', type: 'text', label: 'Answer field' },
      { key: 'citationsField', type: 'text', label: 'Citations field' },
      { key: 'placeholder', type: 'text', label: 'Input placeholder' },
      { key: 'intro', type: 'text', label: 'Intro line' },
      { key: 'history', type: 'bool', label: 'Show conversation sidebar' },
      { key: 'settings', type: 'bool', label: 'Show model/tool settings' },
      { key: 'configEndpoint', type: 'text', label: 'Config endpoint (models+tools)' },
      { key: 'personas', type: 'bool', label: 'Show voice/persona picker' },
      { key: 'personasEndpoint', type: 'text', label: 'Personas endpoint' },
      { key: 'trace', type: 'bool', label: 'Show agent trace (under the hood)' },
      { key: 'streaming', type: 'bool', label: 'Stream the answer (SSE)' },
      { key: 'streamEndpoint', type: 'text', label: 'Streaming endpoint (SSE)' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-conversation' });
      if (block.history !== false) {
        // collapsed by default so the chat gets the full width; the chevron toggle re-opens it
        var rail = ctx.el('aside', { 'class': 'catalog-conv-rail' + (block.railOpen ? '' : ' collapsed') });
        rail.appendChild(ctx.el('button', { 'class': 'catalog-conv-new', type: 'button', text: 'New conversation' }));
        rail.appendChild(ctx.el('button', { 'class': 'catalog-conv-clear', type: 'button', title: 'clear the current conversation', text: 'Clear chat' }));
        rail.appendChild(ctx.el('div', { 'class': 'catalog-conv-rlabel', text: 'Conversations' }));
        rail.appendChild(ctx.el('div', { 'class': 'catalog-conv-hist' }));
        sec.appendChild(rail);
        sec.appendChild(ctx.el('button', { 'class': 'catalog-conv-toggle', type: 'button', 'aria-label': 'toggle sidebar' }));
      }
      var main = ctx.el('div', { 'class': 'catalog-conv-main' });
      // settings drawer (model/embedding/reranker pickers + retrieval-tool toggles) — populated from
      // the config endpoint in init(); shown only when 'settings' isn't disabled.
      if (block.settings !== false) {
        var panel = ctx.el('div', { 'class': 'catalog-conv-settings' });
        // header row with title + an x-close button (mirrors the OLD app's settings-modal header)
        var headRow = ctx.el('div', { 'class': 'cs-headrow' });
        headRow.appendChild(ctx.el('div', { 'class': 'cs-header', text: 'Settings — models & retrieval' }));
        headRow.appendChild(ctx.el('button', { 'class': 'cs-x', type: 'button', 'aria-label': 'close settings', text: '×' }));
        panel.appendChild(headRow);
        panel.appendChild(ctx.el('div', { 'class': 'cs-intro', text: 'Swap any model or toggle a technique; changes apply to your next question.' }));
        panel.appendChild(ctx.el('h4', { text: 'Models' }));
        var fields = ctx.el('div', { 'class': 'catalog-conv-fields' });
        // each model picker, with an explanatory note under the embedding + reranker selects
        var NOTES = {
          'cs-llm': 'Writes the answer. Independent of retrieval — any LLM can answer over passages from any embedding space.',
          'cs-embedding': "Your question is embedded by the same model that built this space, so spaces are not interchangeable — you can't search a Gemini space with OpenAI vectors.",
          'cs-reranker': 'Re-orders retrieved passages by reading their text — works with any embedding space or LLM. Listed best-match-provider first.'
        };
        [['cs-llm', 'Language model'], ['cs-embedding', 'Embedding space'], ['cs-reranker', 'Reranker']].forEach(function (f) {
          var lab = ctx.el('label', { 'class': 'catalog-conv-field' });
          lab.appendChild(ctx.el('span', { text: f[1] }));
          lab.appendChild(ctx.el('select', { 'class': f[0] }));
          if (NOTES[f[0]]) lab.appendChild(ctx.el('div', { 'class': 'cs-field-note', text: NOTES[f[0]] }));
          fields.appendChild(lab);
        });
        panel.appendChild(fields);
        // SCOPE section — restrict the corpus the answer may draw on (letters vs notebook entries)
        panel.appendChild(ctx.el('h4', { text: 'Scope' }));
        var scope = ctx.el('div', { 'class': 'catalog-conv-fields' });
        var scopeLab = ctx.el('label', { 'class': 'catalog-conv-field' });
        scopeLab.appendChild(ctx.el('span', { text: 'Restrict to' }));
        var kindsSel = ctx.el('select', { 'class': 'cs-kinds' });
        [['', 'All sources'], ['letter', 'Letters only'], ['notebook_entry', 'Notebook entries only']]
          .forEach(function (o) { kindsSel.appendChild(ctx.el('option', { value: o[0], text: o[1] })); });
        scopeLab.appendChild(kindsSel);
        scope.appendChild(scopeLab);
        scope.appendChild(ctx.el('div', { 'class': 'cs-note', text: '570 letters + 716 notebook pages. Citations resolve to the exact scanned region via the page’s IIIF manifest.' }));
        panel.appendChild(scope);
        // PERSONA section — the chat voice (default / archivist / first-person), with an editable system
        // prompt revealed by an "edit prompt" link. Populated from the personas endpoint in init().
        if (block.personas !== false) {
          panel.appendChild(ctx.el('h4', { text: 'Voice' }));
          var pwrap = ctx.el('div', { 'class': 'catalog-conv-fields' });
          var plab = ctx.el('label', { 'class': 'catalog-conv-field' });
          var phead = ctx.el('span', {});
          phead.appendChild(document.createTextNode('Persona '));
          phead.appendChild(ctx.el('a', { 'class': 'cs-persona-edit', href: '#', text: '(edit prompt)' }));
          plab.appendChild(phead);
          plab.appendChild(ctx.el('select', { 'class': 'cs-persona' }));
          pwrap.appendChild(plab);
          panel.appendChild(pwrap);
          panel.appendChild(ctx.el('textarea', { 'class': 'cs-system', rows: '4', hidden: 'hidden',
            placeholder: 'System prompt for the chosen voice…' }));
        }
        panel.appendChild(ctx.el('h4', { text: 'Retrieval tools' }));
        panel.appendChild(ctx.el('div', { 'class': 'catalog-conv-toggles' }));
        main.appendChild(panel);
      }
      var log = ctx.el('div', { 'class': 'catalog-conv-log' });
      if (block.intro) log.appendChild(ctx.el('div', { 'class': 'catalog-conv-empty', text: block.intro }));
      main.appendChild(log);
      // controls bar (mic + read-aloud + spoken-voice + settings) — a quiet row above the composer
      var ctrls = ctx.el('div', { 'class': 'catalog-conv-controls' });
      if (block.settings !== false) {
        ctrls.appendChild(ctx.el('button', { 'class': 'catalog-conv-settings-btn', type: 'button',
          title: 'choose models and retrieval tools', text: 'settings' }));
      }
      if (block.mic !== false) {
        ctrls.appendChild(ctx.el('button', { 'class': 'catalog-conv-mic', type: 'button',
          title: 'dictate your question', text: 'use microphone' }));
      }
      if (block.readAloud !== false) {
        var aloud = ctx.el('label', { 'class': 'catalog-conv-aloud' });
        aloud.appendChild(ctx.el('input', { type: 'checkbox', 'class': 'catalog-conv-aloud-cb' }));
        aloud.appendChild(document.createTextNode('read answers aloud'));
        ctrls.appendChild(aloud);
      }
      if (block.voice === 'server' && block.voicePicker !== false) {
        var pl = ctx.el('label', { 'class': 'catalog-conv-plabel' });
        pl.appendChild(document.createTextNode('spoken voice'));
        pl.appendChild(ctx.el('select', { 'class': 'catalog-conv-provider', title: 'voice provider' }));
        ctrls.appendChild(pl);
      }
      // streaming toggle — let the user turn live token streaming (SSE) on/off per question. Shown only
      // when a streaming endpoint is available (block.streaming OR an explicit streamEndpoint); its initial
      // checked state mirrors block.streaming. Mirrors the OLD app's #stream-on checkbox.
      if (block.streaming || block.streamEndpoint) {
        var streamLbl = ctx.el('label', { 'class': 'catalog-conv-stream', title: 'stream the answer token-by-token' });
        var streamCb = ctx.el('input', { type: 'checkbox', 'class': 'catalog-conv-stream-cb' });
        if (block.streaming) streamCb.checked = true;
        streamLbl.appendChild(streamCb);
        streamLbl.appendChild(document.createTextNode('stream answer'));
        ctrls.appendChild(streamLbl);
      }
      // spoken-voice status feedback (e.g. 'speaking…' / 'server voice unavailable — using browser voice')
      ctrls.appendChild(ctx.el('span', { 'class': 'catalog-conv-vstatus', 'aria-live': 'polite' }));
      if (ctrls.children.length) main.appendChild(ctrls);
      // composer — clean: an underlined question line + the ASK button
      var row = ctx.el('form', { 'class': 'catalog-conv-row' });
      row.appendChild(ctx.el('input', { 'class': 'catalog-conv-input', type: 'text', autocomplete: 'off',
        placeholder: block.placeholder || 'Ask a question about the archive…' }));
      row.appendChild(ctx.el('button', { 'class': 'catalog-conv-send', type: 'submit', text: 'Ask' }));
      main.appendChild(row);
      sec.appendChild(main);
      return sec;
    },
    init: function (node, block, ctx) {
      var log = node.querySelector('.catalog-conv-log');
      var form = node.querySelector('.catalog-conv-row');
      var input = node.querySelector('.catalog-conv-input');
      var rail = node.querySelector('.catalog-conv-rail');
      var hist = node.querySelector('.catalog-conv-hist');
      var mic = node.querySelector('.catalog-conv-mic');
      var aloudCb = node.querySelector('.catalog-conv-aloud-cb');
      var provSel = node.querySelector('.catalog-conv-provider');
      var streamCb = node.querySelector('.catalog-conv-stream-cb');   // per-question streaming toggle
      var vStatus = node.querySelector('.catalog-conv-vstatus');      // spoken-voice status feedback
      var endpoint = block.endpoint || '/api/ask';
      function setVoiceStatus(msg) { if (vStatus) vStatus.textContent = msg || ''; }

      // ---- settings: model pickers + retrieval-tool toggles, built from the config endpoint ----
      // _settings is the live RetrievalConfig the request body merges in (the ONE place the UI's
      // controls become the backend's request). GENERAL: configEndpoint comes from block config.
      var settingsBtn = node.querySelector('.catalog-conv-settings-btn');
      var settingsPanel = node.querySelector('.catalog-conv-settings');
      var llmSel = node.querySelector('.cs-llm'), embSel = node.querySelector('.cs-embedding'), rrSel = node.querySelector('.cs-reranker');
      var kindsSel = node.querySelector('.cs-kinds');
      var togBox = node.querySelector('.catalog-conv-toggles');
      var personaSel = node.querySelector('.cs-persona'), systemBox = node.querySelector('.cs-system'), personaEdit = node.querySelector('.cs-persona-edit');
      var _toggles = {};
      if (settingsBtn && settingsPanel) {
        // Settings is a centered MODAL with a dimmed backdrop (close via ×, backdrop click, or Escape).
        var sBd = document.createElement('div'); sBd.className = 'catalog-conv-settings-bd'; document.body.appendChild(sBd);
        var hbtn = function () { return document.querySelector('.header-settings-btn'); };
        var openS = function () { settingsPanel.classList.add('open'); sBd.classList.add('open'); var hb = hbtn(); if (hb) hb.classList.add('active'); };
        var closeS = function () { settingsPanel.classList.remove('open'); sBd.classList.remove('open'); var hb = hbtn(); if (hb) hb.classList.remove('active'); };
        var toggleS = function (ev) { if (ev) ev.stopPropagation(); if (settingsPanel.classList.contains('open')) closeS(); else openS(); };
        settingsBtn.addEventListener('click', toggleS);
        sBd.addEventListener('click', closeS);
        var settingsX = settingsPanel.querySelector('.cs-x');
        if (settingsX) settingsX.addEventListener('click', function (ev) { ev.stopPropagation(); closeS(); });
        document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && settingsPanel.classList.contains('open')) closeS(); });
        // PAGE-HEADER settings button (shared header.html carries a hidden .header-settings-btn, injected
        // async by load-fragments.js): reveal it and open this block's modal. Poll a bounded number of times.
        (function wireHeaderSettings(tries) {
          var hb = document.querySelector('.header-settings-btn');
          if (!hb) { if (tries > 0) setTimeout(function () { wireHeaderSettings(tries - 1); }, 120); return; }
          if (hb.dataset.wired) return;          // only one conversation block claims the header button
          hb.dataset.wired = '1'; hb.hidden = false;
          hb.addEventListener('click', toggleS);
        })(40);
      }
      if (settingsPanel) {
        var _avail = {}, _provs = { llms: {}, embeddings: {}, rerankers: {} };
        var provAvailable = function (p) { return _avail[p] !== false; };
        // fill a <select>; when matchProvider is given, order options so the chosen LLM's provider
        // comes first (then available before unavailable) — mirrors the OLD provider-match ordering.
        var fillSel = function (sel, options, chosen, provMap, matchProvider) {
          if (!sel) return; provMap = provMap || {};
          var opts = (options || []).slice();
          if (matchProvider) {
            opts.sort(function (a, b) {
              var ma = provMap[a] === matchProvider ? 0 : 1, mb = provMap[b] === matchProvider ? 0 : 1;
              if (ma !== mb) return ma - mb;
              return (provAvailable(provMap[a]) ? 0 : 1) - (provAvailable(provMap[b]) ? 0 : 1);
            });
          }
          sel.innerHTML = '';
          opts.forEach(function (opt) {
            var prov = provMap[opt], ok = provAvailable(prov);
            var o = ctx.el('option', { value: opt, text: opt + (ok ? '' : '  (needs ' + prov + ' key)') });
            o.disabled = !ok; if (opt === chosen && ok) o.selected = true;
            sel.appendChild(o);
          });
          if (sel.selectedIndex < 0 || (sel.options[sel.selectedIndex] && sel.options[sel.selectedIndex].disabled)) {
            for (var i = 0; i < sel.options.length; i++) { if (!sel.options[i].disabled) { sel.options[i].selected = true; break; } }
          }
        };
        var _cfg = null;   // cached config so the LLM-change listener can re-sort dependent pickers
        var TOG_DESC = {
          use_bm25: 'Sparse keyword search (BM25). Matches exact words, names, and dates. Free, local.',
          use_dense: 'Semantic vector search in the chosen embedding space — finds passages by meaning, not just shared words.',
          use_rerank: 'Re-orders the top candidates with a cross-encoder for sharper relevance.',
          use_hyde: 'HyDE — drafts a hypothetical answer, then retrieves passages similar to that. One paid LLM call.',
          use_multiquery: 'RAG-Fusion — rephrases the question several ways and fuses results for better recall. Paid.',
          use_llm_router: 'An LLM judges the question complexity and recommends a retrieval strategy. Paid.',
          auto_apply_route: 'Let the router actually escalate to the (paid) strategy it recommends.'
        };
        var buildToggles = function (tools) {
          togBox.innerHTML = ''; _toggles = {};
          (tools || []).forEach(function (t) {
            _toggles[t.key] = !!t.default;
            var btn = ctx.el('button', { 'class': 'catalog-conv-tog' + (t.default ? ' on' : ''), type: 'button' });
            var head = ctx.el('div', { 'class': 'tog-head' });
            head.appendChild(ctx.el('span', { text: t.label + (t.paid ? ' $' : '') }));
            head.appendChild(ctx.el('span', { 'class': 'tog-pip', text: t.default ? 'on' : 'off' }));
            btn.appendChild(head);
            btn.appendChild(ctx.el('div', { 'class': 'tog-desc', text: TOG_DESC[t.key] || t.hint || t.description || '' }));
            btn.addEventListener('click', function () {
              _toggles[t.key] = !_toggles[t.key];
              btn.classList.toggle('on', _toggles[t.key]);
              btn.querySelector('.tog-pip').textContent = _toggles[t.key] ? 'on' : 'off';
            });
            togBox.appendChild(btn);
          });
        };
        // apply a config object (from the endpoint OR the static fallback) to every picker + toggle
        var applyConfig = function (cfg) {
          _cfg = cfg; _avail = cfg.availability || {}; _provs = cfg.providers || _provs;
          var def = cfg.defaults || {};
          fillSel(llmSel, cfg.llms, def.llm, _provs.llms);
          var llmProv = llmSel && _provs.llms[llmSel.value] || null;
          fillSel(embSel, cfg.embeddings, def.embedding, _provs.embeddings, llmProv);
          fillSel(rrSel, cfg.rerankers, def.reranker, _provs.rerankers, llmProv);
          buildToggles(cfg.tools);
          // when the LLM changes, re-order embedding + reranker options by provider match (the user's ask)
          if (llmSel) llmSel.addEventListener('change', function () {
            if (!_cfg) return;
            var p = _provs.llms[llmSel.value] || null;
            fillSel(embSel, _cfg.embeddings, embSel.value, _provs.embeddings, p);
            fillSel(rrSel, _cfg.rerankers, rrSel.value, _provs.rerankers, p);
          });
        };
        fetch(block.configEndpoint || '/api/config').then(function (r) { return r.json(); }).then(applyConfig)
          .catch(function () {
            // backend unreachable: fall back to the static config so the rail is never empty (mirrors the OLD app)
            applyConfig(block.fallbackConfig || CONV_FALLBACK_CONFIG);
          });
      }
      // PERSONA — the chat voice + its editable system prompt. The select loads presets from the personas
      // endpoint; choosing one seeds the editable prompt; "edit prompt" reveals the textarea. The chosen/
      // edited prompt rides along in the request body (system_prompt) — exactly like the OLD app.
      var _personas = [];
      if (personaSel) {
        fetch(block.personasEndpoint || '/api/personas').then(function (r) { return r.json(); }).then(function (d) {
          _personas = d.personas || [];
          personaSel.innerHTML = '';
          _personas.forEach(function (p) { personaSel.appendChild(ctx.el('option', { value: p.key, text: p.label })); });
          var applyPersona = function () {
            var p = _personas.filter(function (x) { return x.key === personaSel.value; })[0];
            if (p && systemBox) systemBox.value = p.prompt;
          };
          applyPersona();                       // seed with the default persona's prompt
          personaSel.addEventListener('change', applyPersona);
        }).catch(function () { if (personaSel.parentNode) personaSel.parentNode.style.display = 'none'; });
        if (personaEdit && systemBox) personaEdit.addEventListener('click', function (e) {
          e.preventDefault();
          systemBox.hidden = !systemBox.hidden;
          personaEdit.textContent = systemBox.hidden ? '(edit prompt)' : '(hide prompt)';
          if (!systemBox.hidden) systemBox.focus();
        });
      }

      // read the rail into the request fields /api/ask expects
      function settingsBody() {
        var b = {};
        if (llmSel && llmSel.value) b.llm = llmSel.value;
        if (embSel && embSel.value) b.embedding = embSel.value;
        if (rrSel && rrSel.value) b.reranker = rrSel.value;
        if (kindsSel && kindsSel.value) b.kinds = [kindsSel.value];   // restrict corpus (letters vs notebook)
        b.toggles = {}; for (var k in _toggles) b.toggles[k] = _toggles[k];
        if (systemBox && systemBox.value.trim()) b.system_prompt = systemBox.value.trim();   // the chosen/edited persona
        return b;
      }

      if (provSel) {                          // populate the voice-provider picker (local|azure|gcp)
        fetch(block.voicesEndpoint || '/api/voices').then(function (r) { return r.json(); }).then(function (d) {
          (d.tts || []).forEach(function (pv) { provSel.appendChild(ctx.el('option', { value: pv.id, text: pv.label + (pv.configured ? '' : ' (no key)') })); });
          provSel.value = d.default_tts || 'local';
        }).catch(function () { ['local', 'azure', 'gcp'].forEach(function (id) { provSel.appendChild(ctx.el('option', { value: id, text: id })); }); });
      }
      function curProvider() { return (provSel && provSel.value) || undefined; }
      var qf = block.queryField || 'query', af = block.answerField || 'answer', cf = block.citationsField || 'citations';
      var KEY = 'catalog-conv:' + (block.id || endpoint);
      var convos = [], current = null;
      function load() { try { convos = JSON.parse(localStorage.getItem(KEY) || '[]'); } catch (e) { convos = []; } }
      function save() { try { localStorage.setItem(KEY, JSON.stringify(convos.slice(-40))); } catch (e) {} }
      function fillAnswer(b, answer) {     // render answer prose with clickable [n] footnote markers
        b.innerHTML = '';
        String(answer == null ? '' : answer).split(/(\[\d+\])/g).forEach(function (seg) {
          var m = seg.match(/^\[(\d+)\]$/);
          if (m) {
            var sup = ctx.el('sup', { 'class': 'catalog-conv-cref', title: 'source ' + m[1], text: m[1] });
            sup.addEventListener('click', function () {
              var t = log.querySelector('.catalog-conv-source[data-n="' + m[1] + '"]');
              if (t) { t.scrollIntoView({ behavior: 'smooth', block: 'center' }); t.classList.add('hl'); setTimeout(function () { t.classList.remove('hl'); }, 1700); }
            });
            b.appendChild(sup);
          } else if (seg) { b.appendChild(document.createTextNode(seg)); }
        });
      }
      // format a citation as a short archive/book label (mirrors the OLD app's sourceLabel)
      function sourceLabel(c) {
        if (c.source === 'book')
          return (c.title || 'Thank God Ahead of Time') + (c.page != null ? ', p.' + c.page : '') + ' (Crosby — biography)';
        var kind = c.kind === 'notebook_entry' ? 'notebook' : (c.kind || 'source');
        var pg = (c.page != null) ? 'p.' + c.page : '';
        return ((c.section || c.doc_id || '') + ' ' + pg + ' (' + kind + ')').trim();
      }
      var regionOpts = { regionEndpoint: block.regionEndpoint || '/api/region' };
      function sourcesBox(cites) {
        var box = ctx.el('div', { 'class': 'catalog-conv-sources' });
        box.appendChild(ctx.el('div', { 'class': 'catalog-conv-sources-h', text: 'Sources' }));
        cites.forEach(function (c) {
          // a source row is CLICKABLE when it resolves to a scanned region (or a book passage)
          var clickable = !!(c.doc_id || c.source === 'book');
          var li = ctx.el('div', { 'class': 'catalog-conv-source' + (clickable ? ' clickable' : ''), 'data-n': (c.n != null ? c.n : ''),
            title: clickable ? 'open this source' : '' });
          li.appendChild(ctx.el('span', { 'class': 'catalog-conv-cn', text: '[' + (c.n != null ? c.n : '?') + '] ' }));
          li.appendChild(ctx.el('span', { 'class': 'catalog-conv-clabel', text: sourceLabel(c) + ' ' }));
          var snip = (c.snippet || c.label || '').slice(0, 90);
          if (snip) li.appendChild(ctx.el('span', { 'class': 'catalog-conv-snip', text: '“' + snip + '”' }));
          if (clickable && c.source !== 'book') {
            li.addEventListener('click', function () {
              openCitation({ doc_id: c.doc_id, rid: c.rid, page: c.page, pdf_page: c.pdf_page, section: c.section,
                vertices: c.vertices, kind: c.kind, snippet: c.snippet }, regionOpts);
            });
          }
          box.appendChild(li);
        });
        return box;
      }
      // grade badge (Self-RAG/CRAG verdict) — verdict + confidence% + reason, colour-coded
      function gradeBadge(g) {
        if (!g || !g.verdict) return null;
        var conf = (g.confidence != null) ? ' (' + Math.round(g.confidence * 100) + '%)' : '';
        return ctx.el('div', { 'class': 'catalog-conv-grade ' + (g.verdict || ''),
          text: 'verdict: ' + g.verdict + conf + (g.reason ? ' — ' + g.reason : '') });
      }
      // agent trace ("under the hood"): the router's decision, query rephrasings fused, each retrieval
      // tool that fired (with item/ms counts), and the cost ledger. Collapsible, shown when enabled.
      function traceBox(trace) {
        if (block.trace === false || !trace || (!trace.route && !(trace.steps || []).length && !trace.cost)) return null;
        var det = ctx.el('details', { 'class': 'catalog-conv-trace' });
        det.appendChild(ctx.el('summary', { text: 'Under the hood' }));
        var ol = ctx.el('ol');
        if (trace.route) {
          ol.appendChild(ctx.el('li', {}, [ctx.el('span', { 'class': 'tr-k', text: 'route ' }),
            (trace.route.complexity || '?') + ' → ' + (trace.route.strategy || '') + (trace.route.reason ? '  (' + trace.route.reason + ')' : '')]));
        }
        if (trace.queries_used && trace.queries_used.length > 1) {
          ol.appendChild(ctx.el('li', {}, [ctx.el('span', { 'class': 'tr-k', text: 'queries ' }),
            trace.queries_used.length + ' phrasings fused (RAG-Fusion)']));
        }
        (trace.steps || []).forEach(function (s) {
          var bits = [];
          if (s.items != null) bits.push(s.items + ' items');
          if (s.ms != null) bits.push(s.ms + ' ms');
          ol.appendChild(ctx.el('li', {}, [ctx.el('span', { 'class': 'tr-k', text: (s.tool || 'step') + ' ' }),
            (s.detail || '') + (bits.length ? '  [' + bits.join(', ') + ']' : '')]));
        });
        det.appendChild(ol);
        if (trace.cost) {
          var c = trace.cost;
          if (c.cumulative_usd != null) _sessionCost = c.cumulative_usd;
          else if (c.usd != null) _sessionCost += c.usd;
          var cum = '   (session total $' + _sessionCost.toFixed(4) + ')';
          det.appendChild(ctx.el('div', { 'class': 'tr-cost',
            text: 'this query: $' + (c.usd != null ? c.usd : 0).toFixed(6) + '  ·  in ' + (c.input_tokens || 0) + ' tok  ·  out ' + (c.output_tokens || 0) + ' tok' + cum }));
        }
        return det;
      }
      var _sessionCost = 0;
      function paint(q, answer, cites, grade, trace) {
        var u = ctx.el('div', { 'class': 'catalog-conv-msg user' }); u.textContent = q; log.appendChild(u);
        var b = ctx.el('div', { 'class': 'catalog-conv-msg bot' }); fillAnswer(b, answer);
        var gb = gradeBadge(grade); if (gb) b.appendChild(gb);
        log.appendChild(b);
        if (Array.isArray(cites) && cites.length) log.appendChild(sourcesBox(cites));
        var tb = traceBox(trace); if (tb) log.appendChild(tb);
        log.scrollTop = log.scrollHeight;
        return b;
      }
      function renderHist() {
        if (!hist) return;
        hist.innerHTML = '';
        var withTurns = convos.filter(function (c) { return c.turns.length; });
        if (!withTurns.length) { hist.appendChild(ctx.el('div', { 'class': 'catalog-conv-hempty', text: 'no conversations yet' })); return; }
        withTurns.slice().reverse().forEach(function (c) {
          var when = c.ts ? new Date(c.ts).toLocaleDateString() : '';
          var btn = ctx.el('button', { 'class': 'catalog-conv-hitem' + (current && c.id === current.id ? ' active' : ''),
            type: 'button', title: c.title + (when ? ' · ' + when : '') });
          btn.appendChild(ctx.el('span', { text: c.title }));
          if (when) btn.appendChild(ctx.el('span', { 'class': 'catalog-conv-htime', text: when }));
          btn.addEventListener('click', function () { open(c.id); });
          hist.appendChild(btn);
        });
      }
      function clearLog() { log.innerHTML = ''; if (block.intro) log.appendChild(ctx.el('div', { 'class': 'catalog-conv-empty', text: block.intro })); }
      function newConvo() {
        current = { id: 'c' + Date.now() + Math.floor(Math.random() * 1000), title: '', turns: [], ts: Date.now() };
        convos.push(current); clearLog(); renderHist();
      }
      function open(id) {
        current = convos.filter(function (c) { return c.id === id; })[0] || current;
        clearLog();
        var e = log.querySelector('.catalog-conv-empty'); if (e && current.turns.length) e.remove();
        current.turns.forEach(function (t) { paint(t.q, t.answer, t.cites, t.grade, t.trace); });
        renderHist();
      }
      load();
      newConvo();                 // start a fresh conversation each visit; history holds the past ones
      renderHist();
      if (rail) {
        node.querySelector('.catalog-conv-new').addEventListener('click', newConvo);
        node.querySelector('.catalog-conv-toggle').addEventListener('click', function () { rail.classList.toggle('collapsed'); });
        // Clear chat: wipe the current conversation's turns + the visible log + any open citation modal
        var clearBtn = node.querySelector('.catalog-conv-clear');
        if (clearBtn) clearBtn.addEventListener('click', function () {
          if (current) { current.turns = []; current.title = ''; }
          clearLog();
          if (Citation && Citation.close) Citation.close();   // also dismiss an open citation modal
          save(); renderHist();
        });
      }
      if (mic) {                              // STT: server (record -> /api/stt) if sttEndpoint, else browser
        var rec = null;
        var micReset = function () { mic.classList.remove('listening'); mic.textContent = 'use microphone'; rec = null; };
        var askQ = function (t) { t = (t || '').trim(); if (!t) return; input.value = t;     // auto-submit to the pipeline
          if (form.requestSubmit) form.requestSubmit(); else form.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true })); };
        mic.addEventListener('click', function () {
          if (rec) { try { rec.stop(); } catch (e) {} micReset(); return; }
          // the mic APIs are blocked on insecure origins (plain http on a LAN IP) — tell the user instead of silently doing nothing
          if (!window.isSecureContext) { setVoiceStatus('Microphone is blocked on this address — open the site at http://127.0.0.1:8092 (localhost) or over https.'); return; }
          if (!Voice.canRecord() && !Voice.canListen()) { setVoiceStatus('No microphone support in this browser.'); return; }
          mic.classList.add('listening'); mic.textContent = 'listening… (click to stop)'; setVoiceStatus('listening… click the mic again to stop.');
          if (block.sttEndpoint) {
            rec = Voice.listenServer(block.sttEndpoint, askQ,
              function (s) { if (s === false) { micReset(); setVoiceStatus(''); } else if (s === 'transcribing') { mic.textContent = 'transcribing…'; setVoiceStatus('transcribing…'); } else if (s === 'error') { micReset(); setVoiceStatus('Microphone blocked or unavailable — check the browser permission.'); } }, curProvider());
          } else {
            rec = Voice.listen(askQ, function () { micReset(); setVoiceStatus(''); });
          }
        });
      }
      // record a finished turn (persist + title + history), and read the answer aloud if enabled
      function commitTurn(q, answer, cites, grade, trace) {
        if (aloudCb && aloudCb.checked) {
          // strip [n] citation markers before speaking (mirrors the OLD app's maybeSpeak), and surface
          // voice transport status (speaking / server-voice-unavailable) in the controls bar.
          var spoken = String(answer == null ? '' : answer).replace(/\[\d+\]/g, '').slice(0, 4000);
          Voice.speak(spoken, { voice: block.voice, endpoint: block.ttsEndpoint || '/api/tts', provider: curProvider(),
            onStatus: function (state, msg) { setVoiceStatus(msg); } });
        }
        current.turns.push({ q: q, answer: answer, cites: cites, grade: grade, trace: trace });
        if (!current.title) current.title = q.slice(0, 48);
        save(); renderHist();
      }
      // finish a bot message: render grade + sources + trace below it (shared by both transports)
      function finishMsg(pending, answer, cites, grade, trace) {
        var gb = gradeBadge(grade); if (gb) pending.appendChild(gb);   // Self-RAG verdict below the answer
        if (Array.isArray(cites) && cites.length) log.appendChild(sourcesBox(cites));
        var tb = traceBox(trace); if (tb) log.appendChild(tb);
        log.scrollTop = log.scrollHeight;
      }

      // single POST → JSON {answer, citations, grade, trace}
      function submitOnce(q, pending, body) {
        fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
          .then(function (r) { return r.json(); })
          .then(function (d) {
            var answer = (d && d[af]) || '(no answer returned)', cites = (d && d[cf]) || [], grade = (d && d.grade) || null, trace = (d && d.trace) || null;
            fillAnswer(pending, answer);
            finishMsg(pending, answer, cites, grade, trace);
            commitTurn(q, answer, cites, grade, trace);
          })
          .catch(function (err) { pending.textContent = 'Error contacting the source: ' + err.message; });
      }

      // STREAMING (SSE over fetch): a `meta` event carries citations+grade+trace up front, `token` events
      // append live, `done` finalizes. On done we re-render rich (clickable [n] + sources + trace).
      function submitStream(q, pending, body) {
        var streamEp = block.streamEndpoint || (endpoint.replace(/\/ask$/, '/ask_stream'));
        var resp = { answer: '', citations: [], grade: null, trace: null }, acc = '';
        fetch(streamEp, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
          .then(function (res) {
            if (!res.ok || !res.body) throw new Error('HTTP ' + res.status);
            pending.textContent = '';
            var reader = res.body.getReader(), dec = new TextDecoder(), buf = '';
            function pump() {
              return reader.read().then(function (r) {
                if (r.done) return finalize();
                buf += dec.decode(r.value, { stream: true });
                var i;
                while ((i = buf.indexOf('\n\n')) >= 0) {
                  var line = buf.slice(0, i).trim(); buf = buf.slice(i + 2);
                  if (line.indexOf('data:') !== 0) continue;
                  var ev; try { ev = JSON.parse(line.slice(5).trim()); } catch (e) { continue; }
                  if (ev.type === 'meta') { resp.citations = ev.citations || []; resp.grade = ev.grade || null; resp.trace = ev.trace || null; }
                  else if (ev.type === 'token') { acc += ev.text; pending.textContent = acc; log.scrollTop = log.scrollHeight; }
                  else if (ev.type === 'done') { resp.answer = ev.answer || acc; }
                  else if (ev.type === 'error') { throw new Error(ev.detail || 'stream error'); }
                }
                return pump();
              });
            }
            function finalize() {
              resp.answer = resp.answer || acc;
              fillAnswer(pending, resp.answer);   // swap live text for clickable-[n] rich render
              finishMsg(pending, resp.answer, resp.citations, resp.grade, resp.trace);
              commitTurn(q, resp.answer, resp.citations, resp.grade, resp.trace);
            }
            return pump();
          })
          .catch(function (err) {
            if (acc) { pending.appendChild(ctx.el('span', { 'class': 'muted-note', text: '  [stream error: ' + err.message + ']' })); }
            else { submitOnce(q, pending, body); }   // streaming unsupported → fall back to a single POST
          });
      }

      form.addEventListener('submit', function (e) {
        e.preventDefault();
        var q = (input.value || '').trim(); if (!q) return;
        setVoiceStatus('');   // clear any stale spoken-voice status from the previous answer
        var empty = log.querySelector('.catalog-conv-empty'); if (empty) empty.remove();
        var u = ctx.el('div', { 'class': 'catalog-conv-msg user' }); u.textContent = q; log.appendChild(u);
        input.value = '';
        // pending bubble: an animated spinner + "retrieving + grading…" while we wait (mirrors the OLD app)
        var pending = ctx.el('div', { 'class': 'catalog-conv-msg bot' });
        pending.appendChild(ctx.el('span', { 'class': 'catalog-conv-spin' }));
        pending.appendChild(ctx.el('span', { 'class': 'catalog-conv-pending', text: 'retrieving + grading…' }));
        log.appendChild(pending); log.scrollTop = log.scrollHeight;
        var body = settingsBody(); body[qf] = q;   // merge model pickers + tool toggles into the request
        // the live streaming toggle (when present) decides per-question; else the block's default
        var stream = streamCb ? streamCb.checked : !!block.streaming;
        if (stream) submitStream(q, pending, body); else submitOnce(q, pending, body);
      });
    }
  };

  // ---- shared: load an external library from a CDN exactly once -------------
  function loadScriptOnce(src) {
    window.__catScripts = window.__catScripts || {};
    if (window.__catScripts[src]) return window.__catScripts[src];
    window.__catScripts[src] = new Promise(function (res, rej) {
      var s = document.createElement('script'); s.src = src; s.onload = res; s.onerror = function () { rej(new Error('failed to load ' + src)); };
      document.head.appendChild(s);
    });
    return window.__catScripts[src];
  }
  var VIZ_PALETTE = ['#3C1605', '#826962', '#9a7b5b', '#5a3a22', '#B3A29D', '#6b4a2e', '#a8843c', '#4a6b5b'];

  // ---- graph-explorer: an interactive node-graph (Cytoscape) over a JSON endpoint ----
  // A full knowledge-graph explorer: visible node labels (semantic-zoom tiers), a layout switcher
  // (clusters / importance-rings), an entities-only filter, a clickable kind legend (hide/spotlight),
  // a search-to-focus box, a breadcrumb trail, hover degree-of-interest, single-click dossier +
  // double-click expand. GENERAL: every endpoint comes from block config — no project knowledge.
  injectCSS('catalog-graph-css',
    '.catalog-graph-wrap{display:flex;flex-direction:column;gap:.6em}' +
    '.catalog-graph-bar{display:flex;gap:.5em;align-items:center;flex-wrap:wrap;font-size:.8rem}' +
    '.catalog-graph-bar input[type=text]{border:1px solid var(--provincial-mid,#ccc);border-radius:3px;padding:.4em .55em;font-family:var(--font-body,sans-serif);font-size:.82rem;min-width:170px}' +
    '.catalog-graph-bar select{border:1px solid var(--provincial-mid,#ccc);border-radius:3px;padding:.38em .45em;font-family:var(--font-body,sans-serif);font-size:.8rem;background:#fff}' +
    '.catalog-graph-bar button{background:var(--provincial,#3C1605);color:#fff;border:0;border-radius:3px;padding:.42em .8em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.7rem;letter-spacing:.04em;text-transform:uppercase;cursor:pointer}' +
    '.catalog-graph-bar button.ghost{background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd)}' +
    '.catalog-graph-bar button:hover{background:var(--provincial-dark,#826962);color:#fff}' +
    '.catalog-graph-bar label{display:inline-flex;align-items:center;gap:.3em;color:var(--provincial-dark,#826962)}' +
    '.catalog-graph-bar .gsep{flex:1}' +
    '.catalog-graph-bar input[type=range]{vertical-align:middle}' +
    '.catalog-graph-crumbs{font-size:.76rem;color:var(--provincial-dark,#826962);min-height:1em}' +
    '.catalog-graph-crumbs a{color:var(--provincial,#3C1605);cursor:pointer;text-decoration:none}' +
    '.catalog-graph-crumbs a:hover{text-decoration:underline}' +
    '.catalog-graph-crumbs .crumb-sep{color:var(--provincial-mid,#B3A29D)}' +
    '.catalog-graph{position:relative;width:100%;border:1px solid var(--provincial-mid,#ccc);border-radius:8px;background:#fff;overflow:hidden}' +
    '.catalog-graph-legend{position:absolute;top:8px;left:8px;right:8px;background:rgba(255,255,255,.92);border:1px solid var(--provincial-mid,#ddd);border-radius:6px;padding:.4em .6em;font-size:.7rem;display:flex;flex-wrap:wrap;gap:.5em;align-items:center;max-height:38%;overflow:auto}' +
    '.catalog-graph-legend .lg-head{font-family:var(--font-head,sans-serif);font-weight:800;font-size:.6rem;text-transform:uppercase;letter-spacing:.1em;color:var(--provincial-dark,#826962);width:100%;margin:.2em 0 -.1em}' +
    '.catalog-graph-legend .lg-chip{display:inline-flex;align-items:center;gap:.3em;text-transform:capitalize;cursor:pointer;padding:.1em .3em;border-radius:3px}' +
    '.catalog-graph-legend .lg-chip:hover{background:var(--provincial-light,#f1ece7)}' +
    '.catalog-graph-legend .lg-chip.off{opacity:.38;text-decoration:line-through}' +
    '.catalog-graph-legend .lg-chip i{width:10px;height:10px;border-radius:50%;display:inline-block}' +
    '.catalog-graph-legend .lg-chip .eswatch{display:inline-block;width:16px;height:0;border-top-width:3px;border-top-style:solid}' +
    '.catalog-graph-meta{font-size:.74rem;color:var(--provincial-dark,#826962);min-height:1em}' +
    '.catalog-graph-meta a{color:var(--provincial,#3C1605);cursor:pointer;text-decoration:underline;margin-left:.3em}' +
    '.catalog-graph-loading{padding:2.5em;text-align:center;color:var(--provincial-mid,#999);font-style:italic}' +
    // a small "?" help button beside the toolbar
    '.catalog-graph-help{flex:0 0 auto;width:1.7em;height:1.7em;border-radius:50%;border:1px solid var(--provincial-mid,#ccc)!important;background:var(--provincial-light,#f1ece7)!important;color:var(--provincial-dark,#826962)!important;font-weight:800;cursor:pointer;padding:0!important;line-height:1;font-size:.85rem!important}' +
    '.catalog-graph-help:hover{background:var(--provincial,#3C1605)!important;color:#fff!important}' +
    // career timeline strip (records per year): click a bar to focus, drag to brush a date range
    '.catalog-graph-timeline{display:flex;align-items:flex-end;gap:1px;height:78px;padding:.3em .2em 1.1em;border:1px solid var(--provincial-mid,#ddd);border-radius:6px;background:var(--provincial-light,#f7f3ef);overflow-x:auto;user-select:none;cursor:crosshair}' +
    '.catalog-graph-timeline .tlbar{flex:1 0 5px;min-width:5px;background:var(--provincial-mid,#B3A29D);border-radius:2px 2px 0 0;position:relative;transition:background .15s}' +
    '.catalog-graph-timeline .tlbar:hover{background:var(--provincial,#3C1605)}' +
    '.catalog-graph-timeline .tlbar.inrange{background:var(--provincial,#3C1605)}' +
    '.catalog-graph-timeline .tlbar .yl{position:absolute;bottom:-1.05em;left:50%;transform:translateX(-50%);font-size:.56rem;color:var(--provincial-dark,#826962);white-space:nowrap}' +
    // stage: the graph on the left + a collapsible right-hand SETTINGS sidebar (mirrors the OLD app's
    // .graph-settings aside, so the graph itself gets the room and controls tuck away when not needed)
    '.catalog-graph-stage{display:flex;gap:.7em;align-items:stretch}' +
    '.catalog-graph-main{flex:1;min-width:0;display:flex;flex-direction:column}' +
    '.catalog-graph-settings{flex:0 0 264px;width:264px;border:1px solid var(--provincial-mid,#ccc);border-radius:8px;background:var(--provincial-light,#f7f3ef);padding:.7em .8em;display:flex;flex-direction:column;gap:.5em;overflow-y:auto;transition:flex-basis .2s,width .2s,padding .2s}' +
    '.catalog-graph-settings.collapsed{flex-basis:0;width:0;padding:0;border:none;overflow:hidden}' +
    '.catalog-graph-settings .gs-title{font-family:var(--font-head,sans-serif);font-weight:800;font-size:.62rem;text-transform:uppercase;letter-spacing:.12em;color:var(--provincial-dark,#826962);margin:.2em 0 -.1em}' +
    '.catalog-graph-settings label{display:flex;flex-direction:column;gap:.25em;font-size:.72rem;color:var(--provincial-dark,#826962)}' +
    '.catalog-graph-settings label select,.catalog-graph-settings label input[type=range]{width:100%}' +
    '.catalog-graph-settings .gs-val{float:right;color:var(--provincial-dark,#826962);font-weight:600}' +
    '.catalog-graph-settings label.gs-check{flex-direction:row;align-items:center;gap:.4em;cursor:pointer}' +
    '.catalog-graph-settings .gs-buttons{display:flex;gap:.5em}' +
    '.catalog-graph-settings .gs-buttons button{flex:1;background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd);border-radius:3px;padding:.42em .5em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.7rem;letter-spacing:.04em;text-transform:uppercase;cursor:pointer}' +
    '.catalog-graph-settings .gs-buttons button:hover{background:var(--provincial,#3C1605);color:#fff}' +
    '.catalog-graph-settings input[type=text],.catalog-graph-settings select{border:1px solid var(--provincial-mid,#ccc);border-radius:3px;padding:.34em .45em;font-family:var(--font-body,sans-serif);font-size:.78rem;background:#fff}' +
    // the bar-level toggle button that shows/hides the sidebar
    '.catalog-graph-bar .cg-settings-toggle{margin-left:auto;background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd)}' +
    '@media(max-width:760px){.catalog-graph-stage{flex-direction:column}.catalog-graph-settings{flex-basis:auto;width:auto}}' +
    // "Explore" panel: entity browser (search + kind filter + list) and communities browser —
    // now a VERTICAL stack inside the settings sidebar (each browser is a bordered card)
    '.catalog-graph-explore{display:flex;flex-direction:column;gap:.6em;align-items:stretch}' +
    '.catalog-graph-explore details{border:1px solid var(--provincial-mid,#ddd);border-radius:6px;background:#fff;padding:.4em .6em}' +
    '.catalog-graph-explore summary{font-family:var(--font-head,sans-serif);font-weight:700;font-size:.74rem;color:var(--provincial,#3C1605);cursor:pointer;letter-spacing:.02em}' +
    '.catalog-graph-explore .ex-row{display:flex;flex-direction:column;gap:.4em;margin:.5em 0 .4em}' +
    '.catalog-graph-explore input[type=text],.catalog-graph-explore select{border:1px solid var(--provincial-mid,#ccc);border-radius:3px;padding:.34em .45em;font-family:var(--font-body,sans-serif);font-size:.78rem;background:#fff;width:100%;box-sizing:border-box}' +
    '.catalog-graph-explore .gb-list{max-height:200px;overflow-y:auto;display:flex;flex-direction:column;gap:1px}' +
    '.catalog-graph-explore .gb-item{display:flex;justify-content:space-between;gap:.6em;align-items:center;padding:.34em .45em;border-radius:3px;cursor:pointer;font-size:.78rem;color:var(--site-ink,#2A262A)}' +
    '.catalog-graph-explore .gb-item:hover{background:var(--provincial-light,#f1ece7)}' +
    '.catalog-graph-explore .gb-kind{font-size:.66rem;color:var(--provincial-dark,#826962);text-transform:capitalize;white-space:nowrap}' +
    '.catalog-graph-explore label.gb-cbc{display:inline-flex;align-items:center;gap:.4em;font-size:.76rem;color:var(--provincial-dark,#826962);margin-top:.3em;cursor:pointer}' +
    '.catalog-graph-explore .gb-empty{font-size:.74rem;color:var(--provincial-mid,#999);font-style:italic;padding:.4em}' +
    // graph help modal (scrollable walkthrough)
    '.catalog-graph-helpbd{position:fixed;inset:0;background:rgba(42,38,42,.55);z-index:2200;display:none;align-items:center;justify-content:center;padding:4vh 4vw}' +
    '.catalog-graph-helpbd.open{display:flex}' +
    '.catalog-graph-helpbd .ghmodal{background:#fff;border-radius:10px;max-width:760px;width:100%;max-height:90vh;overflow-y:auto;padding:1.8em 2em;box-shadow:0 18px 60px rgba(60,22,5,.3);position:relative;font-family:var(--font-body,sans-serif);line-height:1.6;color:var(--site-ink,#2A262A)}' +
    '.catalog-graph-helpbd .ghmodal h3{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);margin:.1em 0 .5em}' +
    '.catalog-graph-helpbd .ghmodal h4{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);margin:1em 0 .25em;font-size:1rem}' +
    '.catalog-graph-helpbd .ghmodal ul{margin:.2em 0 .6em;padding-left:1.2em}' +
    '.catalog-graph-helpbd .ghmodal li{margin:.25em 0}' +
    '.catalog-graph-helpbd .gh-x{position:absolute;top:.5em;right:.7em;border:0;background:none;font-size:1.7rem;line-height:1;cursor:pointer;color:var(--provincial-mid,#999)}');

  window.CatalogBlocks['graph-explorer'] = {
    label: 'Graph explorer', category: 'Interactive', icon: 'G',
    blank: function () { return { type: 'graph-explorer', endpoint: '/api/graph', entityEndpoint: '/api/entity',
      connectionEndpoint: '/api/connection', entitiesEndpoint: '/api/entities', regionEndpoint: '/api/region',
      timelineEndpoint: '/api/timeline', communitiesEndpoint: '/api/communities',
      params: 'limit=120&entities_only=true', height: '64vh', layout: 'fcose', entitiesOnly: true, minCoOccurrence: 1,
      timeline: true, explore: true }; },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Graph endpoint (JSON {nodes,edges,meta})' },
      { key: 'entityEndpoint', type: 'text', label: 'Entity dossier endpoint' },
      { key: 'connectionEndpoint', type: 'text', label: 'Connection endpoint' },
      { key: 'entitiesEndpoint', type: 'text', label: 'Entity search endpoint' },
      { key: 'timelineEndpoint', type: 'text', label: 'Timeline endpoint (records/year)' },
      { key: 'communitiesEndpoint', type: 'text', label: 'Communities endpoint (clusters)' },
      { key: 'params', type: 'text', label: 'Default query params' },
      { key: 'height', type: 'text', label: 'Height (CSS)' },
      { key: 'layout', type: 'select', label: 'Layout', options: [['fcose', 'Clusters'], ['concentric', 'Importance rings'], ['cose', 'Cose']] },
      { key: 'entitiesOnly', type: 'bool', label: 'Entities only (hide records)' },
      { key: 'minCoOccurrence', type: 'number', label: 'Min co-occurrence weight' },
      { key: 'timeline', type: 'bool', label: 'Show career-timeline strip' },
      { key: 'explore', type: 'bool', label: 'Show entity/community explorer' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section' });
      var wrap = ctx.el('div', { 'class': 'catalog-graph-wrap' });
      // TOP BAR — the "navigate" controls that stay visible: search-to-focus, fit/reset, help, and the
      // settings-panel toggle (right-aligned). The "tune" controls live in the right sidebar below.
      var bar = ctx.el('div', { 'class': 'catalog-graph-bar' });
      bar.appendChild(ctx.el('input', { 'class': 'cg-focus', type: 'text', placeholder: 'Search a name, place, year…' }));
      bar.appendChild(ctx.el('button', { 'class': 'cg-focus-btn', type: 'button', text: 'Focus' }));
      bar.appendChild(ctx.el('span', { 'class': 'gsep' }));
      bar.appendChild(ctx.el('button', { 'class': 'cg-fit ghost', type: 'button', text: 'Fit' }));
      bar.appendChild(ctx.el('button', { 'class': 'cg-reset ghost', type: 'button', text: 'Reset' }));
      bar.appendChild(ctx.el('button', { 'class': 'catalog-graph-help', type: 'button', title: 'how to use the graph', text: '?' }));
      bar.appendChild(ctx.el('button', { 'class': 'cg-settings-toggle ghost', type: 'button',
        'aria-expanded': 'false', title: 'show/hide the explore + settings panel', text: 'Show settings panel' }));
      wrap.appendChild(bar);
      wrap.appendChild(ctx.el('div', { 'class': 'catalog-graph-crumbs' }));

      // STAGE — graph (+ legend) on the left, the collapsible settings sidebar on the right.
      var stage = ctx.el('div', { 'class': 'catalog-graph-stage' });
      var main = ctx.el('div', { 'class': 'catalog-graph-main' });
      var box = ctx.el('div', { 'class': 'catalog-graph' });
      box.style.height = block.height || '64vh';
      box.appendChild(ctx.el('div', { 'class': 'catalog-graph-loading', text: 'Loading graph…' }));
      main.appendChild(box);
      main.appendChild(ctx.el('div', { 'class': 'catalog-graph-meta' }));
      // career-timeline strip (records per year): click a bar to focus a year, drag to brush a range
      if (block.timeline !== false) {
        main.appendChild(ctx.el('div', { 'class': 'catalog-graph-timeline', title: 'click a year to focus it; drag across bars to filter to a date range; double-click to clear' }));
      }
      stage.appendChild(main);

      // RIGHT SIDEBAR — collapsible. Holds the EXPLORE panel (entity browser, colour-by-community,
      // verified-only, communities) and the SETTINGS panel (relationships, layout, spacing, min-weight,
      // entities-only, fit/reset). Mirrors the OLD app's .graph-settings aside exactly.
      var side = ctx.el('aside', { 'class': 'catalog-graph-settings collapsed' });   // collapsed by default — graph gets the full width

      if (block.explore !== false) {
        side.appendChild(ctx.el('div', { 'class': 'gs-title', text: 'Explore' }));
        var explore = ctx.el('div', { 'class': 'catalog-graph-explore' });
        var browse = ctx.el('details', { 'class': 'cg-browse-wrap', open: 'open' });
        browse.appendChild(ctx.el('summary', { text: 'Find an entity to start from' }));
        var brow = ctx.el('div', { 'class': 'ex-row' });
        brow.appendChild(ctx.el('input', { 'class': 'cg-browse', type: 'text', placeholder: 'Search entities…' }));
        var bkind = ctx.el('select', { 'class': 'cg-browse-kind', title: 'filter by kind' });
        [['', 'all kinds'], ['person', 'person'], ['place', 'place'], ['organization', 'organization'],
         ['condition', 'condition'], ['favor', 'favor'], ['role', 'role'], ['event', 'event'], ['outcome', 'outcome']]
          .forEach(function (o) { bkind.appendChild(ctx.el('option', { value: o[0], text: o[1] })); });
        brow.appendChild(bkind);
        browse.appendChild(brow);
        browse.appendChild(ctx.el('div', { 'class': 'cg-browse-list gb-list' }));
        explore.appendChild(browse);
        side.appendChild(explore);
        // colour-by-community + hide same-page-only live just under the entity browser (as in the OLD app)
        var cbcLbl = ctx.el('label', { 'class': 'gs-check' });
        cbcLbl.appendChild(ctx.el('input', { 'class': 'cg-color-community', type: 'checkbox' }));
        cbcLbl.appendChild(document.createTextNode('colour nodes by community'));
        side.appendChild(cbcLbl);
        var coLbl2 = ctx.el('label', { 'class': 'gs-check' });
        var coCb2 = ctx.el('input', { 'class': 'cg-verified', type: 'checkbox' }); coCb2.checked = true;
        coLbl2.appendChild(coCb2); coLbl2.appendChild(document.createTextNode('hide same-page coincidences'));
        side.appendChild(coLbl2);
        var comm = ctx.el('details', { 'class': 'cg-communities-wrap' });
        comm.appendChild(ctx.el('summary', { text: 'Communities (thematic clusters)' }));
        comm.appendChild(ctx.el('div', { 'class': 'cg-communities gb-list' }));
        side.appendChild(comm);
      } else {
        // even without the explore panel, the verified-only control must exist (init queries it)
        var coLbl3 = ctx.el('label', { 'class': 'gs-check' });
        var coCb3 = ctx.el('input', { 'class': 'cg-verified', type: 'checkbox' }); coCb3.checked = true;
        coLbl3.appendChild(coCb3); coLbl3.appendChild(document.createTextNode('hide same-page coincidences'));
        side.appendChild(coLbl3);
      }

      // ---- SETTINGS sub-panel ----
      side.appendChild(ctx.el('div', { 'class': 'gs-title', style: 'margin-top:.4em', text: 'Settings' }));
      var relLbl = ctx.el('label', {});
      relLbl.appendChild(document.createTextNode('Relationships'));
      var relSel = ctx.el('select', { 'class': 'cg-rels', title: 'relationship filter' });
      [['', 'All relationships'], ['WROTE_TO', 'wrote to'], ['APPEARS_WITH', 'appears with'], ['LOCATED_AT', 'located at'],
       ['HAS_CONDITION', 'has condition'], ['ENROLLED', 'enrolled'], ['HAS_OUTCOME', 'has outcome'],
       ['FAMILY', 'family'], ['MEMBER_OF', 'member of']].forEach(function (o) { relSel.appendChild(ctx.el('option', { value: o[0], text: o[1] })); });
      relLbl.appendChild(relSel);
      side.appendChild(relLbl);

      var layLbl = ctx.el('label', {});
      layLbl.appendChild(document.createTextNode('Layout'));
      var lsel = ctx.el('select', { 'class': 'cg-layout', title: 'how to arrange the nodes' });
      [['fcose', 'Clusters (force)'], ['concentric', 'Importance rings'], ['cose', 'Cose']].forEach(function (o) {
        lsel.appendChild(ctx.el('option', { value: o[0], text: o[1] }));
      });
      lsel.value = block.layout || 'fcose';
      layLbl.appendChild(lsel);
      side.appendChild(layLbl);

      var spLbl = ctx.el('label', {});
      spLbl.appendChild(ctx.el('span', { 'class': 'cg-spacing-val gs-val', text: '2.0x' }));
      spLbl.appendChild(document.createTextNode('Spacing'));
      spLbl.appendChild(ctx.el('input', { 'class': 'cg-spacing', type: 'range', min: '0.5', max: '4', step: '0.1', value: '2.0' }));
      side.appendChild(spLbl);

      var mwLbl = ctx.el('label', {});
      mwLbl.appendChild(ctx.el('span', { 'class': 'cg-minweight-val gs-val', text: String(block.minCoOccurrence || 1) }));
      mwLbl.appendChild(document.createTextNode('Min co-occurrence'));
      mwLbl.appendChild(ctx.el('input', { 'class': 'cg-minweight', type: 'range', min: '1', max: '7', step: '1', value: String(block.minCoOccurrence || 1) }));
      side.appendChild(mwLbl);

      var entLbl = ctx.el('label', { 'class': 'gs-check' });
      var entCb = ctx.el('input', { 'class': 'cg-entities', type: 'checkbox' });
      if (block.entitiesOnly !== false) entCb.checked = true;
      entLbl.appendChild(entCb); entLbl.appendChild(document.createTextNode('entities only'));
      side.appendChild(entLbl);

      var btns = ctx.el('div', { 'class': 'gs-buttons' });
      btns.appendChild(ctx.el('button', { 'class': 'cg-fit-side', type: 'button', text: 'Fit' }));
      btns.appendChild(ctx.el('button', { 'class': 'cg-reset-side', type: 'button', text: 'Reset' }));
      side.appendChild(btns);

      stage.appendChild(side);
      wrap.appendChild(stage);

      sec.appendChild(wrap);
      return sec;
    },
    init: function (node, block, ctx) {
      var box = node.querySelector('.catalog-graph');
      var crumbsEl = node.querySelector('.catalog-graph-crumbs');
      var metaEl = node.querySelector('.catalog-graph-meta');
      var focusInput = node.querySelector('.cg-focus');
      var layoutSel = node.querySelector('.cg-layout');
      var entCb = node.querySelector('.cg-entities');
      var relSel = node.querySelector('.cg-rels');
      var spSlider = node.querySelector('.cg-spacing'), spVal = node.querySelector('.cg-spacing-val');
      var mwSlider = node.querySelector('.cg-minweight'), mwVal = node.querySelector('.cg-minweight-val');
      var verifiedCb = node.querySelector('.cg-verified');
      var fitBtn = node.querySelector('.cg-fit'), resetBtn = node.querySelector('.cg-reset');

      var ENDPOINT = block.endpoint || '/api/graph';
      var dossierOpts = { entityEndpoint: block.entityEndpoint || '/api/entity',
        connectionEndpoint: block.connectionEndpoint || '/api/connection',
        regionEndpoint: block.regionEndpoint || '/api/region',
        onCenter: function (label) { focusInput.value = label; _limitBoost = 0; loadGraph(); } };

      var COLORS = block.colors || { letter: '#3C1605', notebook_page: '#826962', notebook_entry: '#B3A29D',
        year: '#5a4632', person: '#5b7b9a', place: '#6a8a5b', organization: '#9a7b5b', condition: '#9c5b6a',
        favor: '#b0894c', outcome: '#5b9a8a', role: '#7b6a8a', event: '#8a6a9a', other: '#B3A29D' };
      var SHAPES = { person: 'ellipse', place: 'round-diamond', organization: 'round-rectangle', condition: 'hexagon',
        favor: 'round-tag', outcome: 'round-pentagon', role: 'round-triangle', event: 'star', year: 'barrel',
        letter: 'round-rectangle', notebook_page: 'round-rectangle', notebook_entry: 'round-rectangle', other: 'ellipse' };
      var ENTITY_KINDS = { person: 1, place: 1, organization: 1, condition: 1, favor: 1, outcome: 1, role: 1, event: 1 };

      var CDN = 'https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.30.2/cytoscape.min.js';
      // fcose needs layout-base + cose-base loaded BEFORE it or it never registers, and the layout
      // silently falls back to the tight built-in 'cose'. Load the whole chain, in order.
      var LAYOUT_BASE = 'https://cdn.jsdelivr.net/npm/layout-base@2.0.1/layout-base.js';
      var COSE_BASE = 'https://cdn.jsdelivr.net/npm/cose-base@2.2.0/cose-base.js';
      var FCOSE_CDN = 'https://cdn.jsdelivr.net/npm/cytoscape-fcose@2.2.0/cytoscape-fcose.js';

      // ---- state ----
      var _cy = null, _curNodeCount = 0, _lastFocal = null, _limitBoost = 0;
      var _crumbs = [], _hiddenKinds = {}, _legendSpot = null, _zoomTimer = null, _spTimer = null;
      var _yearRange = null, _colorByCommunity = false, _browseTimer = null;
      var timelineEl = node.querySelector('.catalog-graph-timeline');
      var browseInput = node.querySelector('.cg-browse'), browseKind = node.querySelector('.cg-browse-kind');
      var browseList = node.querySelector('.cg-browse-list');
      var commWrap = node.querySelector('.cg-communities-wrap'), commList = node.querySelector('.cg-communities');
      var cbcCb = node.querySelector('.cg-color-community');
      var helpBtn = node.querySelector('.catalog-graph-help');

      function _layoutName() { return layoutSel.value || 'fcose'; }
      function _minWeight() { return parseInt(mwSlider.value, 10) || 1; }
      function _spacing() { return parseFloat(spSlider.value) || 2.0; }
      function _fcoseReady() { try { if (window.cytoscapeFcose && !window.__fcoseReg) { window.cytoscape.use(window.cytoscapeFcose); window.__fcoseReg = 1; } } catch (e) { window.__fcoseReg = 1; } return !!window.__fcoseReg; }

      // ADAPTIVE layout: fcose (topology) or concentric (importance rings); scales cost with node count.
      function _layoutOpts(name, focalId) {
        var k = _spacing(), n = _curNodeCount || 80, mid = n > 80, big = n > 180;
        if (name === 'concentric') {
          var _REC = { letter: 1, notebook_entry: 1, notebook_page: 1, document: 1, year: 1, date: 1 };
          var imp = function (nd) { return _REC[nd.data('kind')] ? 1 : (((nd.data('raw') || {}).mention_count || 0) + nd.degree() + 1); };
          return { name: 'concentric', animate: !mid, animationDuration: 450, fit: true, padding: 50,
            concentric: function (nd) { return nd.data('isFocus') ? 1e9 : imp(nd); },
            levelWidth: function (nodes) { var mx = 1; nodes.forEach(function (nd) { if (!nd.data('isFocus')) mx = Math.max(mx, imp(nd)); }); return Math.max(1, mx / 6); },
            minNodeSpacing: Math.round(34 * k), spacingFactor: 1.05 * k, startAngle: 1.5 * Math.PI, equidistant: false };
        }
        if (name === 'fcose' && _fcoseReady()) {
          return { name: 'fcose', animate: !mid, animationDuration: 500, randomize: true,
            quality: big ? 'draft' : 'default', nodeDimensionsIncludeLabels: !mid,
            nodeSeparation: Math.round(180 * k), idealEdgeLength: Math.round(150 * k),
            nodeRepulsion: Math.round(11000 * k), gravity: 0.25 / k, gravityRange: 4.5,
            numIter: big ? 800 : (mid ? 1500 : 2500), padding: 60,
            fixedNodeConstraint: focalId ? [{ nodeId: focalId, position: { x: 0, y: 0 } }] : undefined };
        }
        return { name: 'cose', animate: !mid, fit: true, padding: 30,
          nodeRepulsion: Math.round(9000 * k), idealEdgeLength: Math.round(70 * k) };
      }

      function edgeConfidence(method) {
        var ASSERTED = { source: 1, gold_link: 1, resolved: 1 }, STRUCTURAL = { structure: 1, normalize_dates: 1, stitch: 1 };
        if (ASSERTED[method]) return 'asserted'; if (STRUCTURAL[method]) return 'structural'; return 'inferred';
      }
      function edgeOpacity(e) {
        var raw = e.data('raw') || {};
        if (raw.weight) return Math.min(0.95, 0.42 + 0.11 * raw.weight);
        var c = edgeConfidence(raw.method); return c === 'asserted' ? 0.95 : c === 'structural' ? 0.72 : 0.5;
      }
      function sizeFor(n) { var r = n.data('raw') || {}, m = (r.mention_count != null) ? r.mention_count : n.degree(); return 22 + Math.min(46, 4 * Math.sqrt(m || 1)); }

      var _nodeEl = function (n, focalId) { return { data: { id: n.id, label: n.label || n.id, kind: n.kind || 'other', raw: n, isFocus: n.id === focalId ? 1 : 0 } }; };
      var _edgeEl = function (e) { return { data: { id: e.source + '__' + (e.kind || '').toUpperCase() + '__' + e.target, source: e.source, target: e.target, label: edgeStyleFor(e.kind).label, kind: (e.kind || '').toUpperCase(), raw: e } }; };

      // ---- semantic-zoom labels: rank by in-view degree, reveal labels by zoom tier ----
      function applyZoomTier() {
        if (!_cy) return;
        var z = _cy.zoom(), n = _cy.nodes().length;
        var topHub = Math.max(6, Math.round(n * 0.12)), topMid = Math.max(14, Math.round(n * 0.4));
        var cutoff = z < 0.45 ? topHub : z < 1.0 ? topMid : n;
        _cy.batch(function () { _cy.nodes().forEach(function (nd) {
          var show = nd.data('isFocus') || (nd.data('_rank') == null ? 1e9 : nd.data('_rank')) < cutoff;
          nd.toggleClass('nolabel', !show);
        }); });
      }
      function applyConfidenceFilter() {
        if (!_cy) return; var mw = _minWeight();
        _cy.batch(function () { _cy.edges().forEach(function (e) { var w = (e.data('raw') || {}).weight; e.toggleClass('weak', !!w && w < mw); }); });
      }
      function applyConnVerify() {
        if (!_cy) return; var only = verifiedCb.checked;
        _cy.batch(function () { _cy.edges().forEach(function (e) { var coinc = ((e.data('raw') || {}).verified) === 'coincidental'; e.toggleClass('coinc', coinc); e.toggleClass('coinc-hidden', coinc && only); }); });
      }
      function applyHiddenKinds() {
        if (!_cy) return;
        _cy.batch(function () { _cy.nodes().forEach(function (n) { n.toggleClass('kind-hidden', !!_hiddenKinds[n.data('kind')]); }); });
      }
      function spotlight(kind, isEdge) {
        if (!_cy) return;
        if (_legendSpot === kind) { _cy.elements().removeClass('legend-dim'); _legendSpot = null; return; }
        _legendSpot = kind; _cy.elements().addClass('legend-dim');
        var match = isEdge ? _cy.edges().filter(function (e) { return e.data('kind') === kind; }) : _cy.nodes().filter(function (n) { return n.data('kind') === kind; });
        var keep = isEdge ? match.connectedNodes().union(match) : match.union(match.connectedEdges());
        keep.removeClass('legend-dim');
      }

      // ---- colour nodes by Louvain community (distinct, stable hue per community id) ----
      function communityColor(cid) { return cid == null ? '#B3A29D' : 'hsl(' + ((cid * 53) % 360) + ' 58% 55%)'; }
      function applyNodeColoring() {
        if (!_cy) return;
        _cy.batch(function () { _cy.nodes().forEach(function (n) {
          var raw = n.data('raw') || {};
          n.style('background-color', _colorByCommunity ? communityColor(raw.community) : (COLORS[n.data('kind')] || '#B3A29D'));
        }); });
      }

      // ---- date brush: hide edges whose year_span falls entirely outside [lo,hi]; then orphan-hide nodes ----
      function applyDateBrush() {
        if (!_cy) return;
        _cy.batch(function () {
          _cy.edges().forEach(function (e) {
            var span = (e.data('raw') || {}).year_span, out = false;
            if (_yearRange && span && span.length === 2) out = span[1] < _yearRange[0] || span[0] > _yearRange[1];
            e.toggleClass('outdate', out);
          });
          _cy.nodes().forEach(function (nd) {
            if (nd.data('isFocus')) { nd.removeClass('outdate'); return; }
            var visible = nd.connectedEdges().filter(function (e) { return !e.hasClass('outdate'); }).length;
            nd.toggleClass('outdate', !!_yearRange && visible === 0);
          });
        });
      }
      function paintBrush(lo, hi) {
        if (!timelineEl) return;
        [].forEach.call(timelineEl.querySelectorAll('.tlbar'), function (b) {
          var y = parseInt(b.dataset.year, 10);
          b.classList.toggle('inrange', y >= lo && y <= hi);
        });
      }
      function updateBrushMeta() {
        if (metaEl && _yearRange) metaEl.textContent = 'date filter: ' + _yearRange[0] + '–' + _yearRange[1] + ' (double-click timeline to clear)';
      }
      function clearDateBrush() {
        _yearRange = null;
        if (timelineEl) [].forEach.call(timelineEl.querySelectorAll('.tlbar.inrange'), function (b) { b.classList.remove('inrange'); });
        applyDateBrush();
        loadGraph();
      }
      // career-timeline strip — records per year; click a bar to focus that year, drag to brush a range
      function renderTimeline() {
        if (!timelineEl) return;
        var ep = block.timelineEndpoint || '/api/timeline';
        fetch(ep).then(function (r) { return r.json(); }).then(function (d) {
          var ys = d.years || [];
          if (!ys.length) { timelineEl.innerHTML = '<span class="gb-empty">no dated records yet</span>'; return; }
          var max = Math.max.apply(null, ys.map(function (y) { return y.total; })) || 1;
          timelineEl.innerHTML = '';
          var dragStart = null, moved = false;
          ys.forEach(function (y, i) {
            var bar = ctx.el('div', { 'class': 'tlbar' });
            bar.dataset.year = String(y.year);
            bar.style.height = Math.max(3, Math.round(64 * y.total / max)) + 'px';
            bar.title = y.year + ': ' + y.total + ' records (' + (y.letter || 0) + ' letters, ' + (y.notebook_entry || 0) + ' entries)';
            if (i % 5 === 0 || i === ys.length - 1) bar.appendChild(ctx.el('span', { 'class': 'yl', text: String(y.year) }));
            bar.addEventListener('mousedown', function (e) { dragStart = y.year; moved = false; e.preventDefault(); });
            bar.addEventListener('mouseenter', function () {
              if (dragStart != null) { moved = true; paintBrush(Math.min(dragStart, y.year), Math.max(dragStart, y.year)); }
            });
            timelineEl.appendChild(bar);
          });
          var finish = function (e) {
            if (dragStart == null) return;
            var endBar = e.target && e.target.closest && e.target.closest('.tlbar');
            var endYear = endBar ? parseInt(endBar.dataset.year, 10) : dragStart;
            if (!moved) { focusInput.value = String(dragStart); dragStart = null; _limitBoost = 0; loadGraph(); return; }
            _yearRange = [Math.min(dragStart, endYear), Math.max(dragStart, endYear)];
            dragStart = null;
            paintBrush(_yearRange[0], _yearRange[1]); updateBrushMeta(); applyDateBrush();
          };
          timelineEl.onmouseup = finish;
          timelineEl.onmouseleave = function () { if (dragStart != null && moved) finish({ target: timelineEl }); };
          timelineEl.ondblclick = clearDateBrush;
          if (_yearRange) paintBrush(_yearRange[0], _yearRange[1]);
        }).catch(function () { timelineEl.innerHTML = '<span class="gb-empty">timeline unavailable</span>'; });
      }

      // ---- entity browser: search/list top-connected entities so the user can discover where to start ----
      function loadBrowse() {
        if (!browseList) return;
        var q = (browseInput && browseInput.value || '').trim(), kind = (browseKind && browseKind.value) || '';
        var params = new URLSearchParams({ limit: '40' });
        if (q) params.set('q', q);
        if (kind) params.set('kind', kind);
        fetch((block.entitiesEndpoint || '/api/entities') + '?' + params.toString()).then(function (r) { return r.json(); }).then(function (r) {
          browseList.innerHTML = '';
          (r.entities || []).forEach(function (e) {
            var item = ctx.el('div', { 'class': 'gb-item', title: 'focus the graph on ' + e.label });
            item.appendChild(ctx.el('span', { text: (e.label || e.id).length > 26 ? (e.label || e.id).slice(0, 25) + '…' : (e.label || e.id) }));
            item.appendChild(ctx.el('span', { 'class': 'gb-kind', text: e.kind + ' (' + e.degree + ')' }));
            item.addEventListener('click', function () { focusInput.value = e.label || e.id; _limitBoost = 0; loadGraph(); });
            browseList.appendChild(item);
          });
          if (!(r.entities || []).length) browseList.appendChild(ctx.el('div', { 'class': 'gb-empty', text: 'no matches' }));
        }).catch(function () { browseList.innerHTML = ''; });
      }

      // ---- communities browser: jump into a thematic Louvain cluster ----
      function loadCommunities() {
        if (!commList || commList.dataset.loaded) return;
        fetch(block.communitiesEndpoint || '/api/communities').then(function (r) { return r.json(); }).then(function (r) {
          commList.dataset.loaded = '1'; commList.innerHTML = '';
          var comms = (r.communities || []).filter(function (c) { return c.size >= 3; }).slice(0, 30);
          if (!comms.length) { commList.appendChild(ctx.el('div', { 'class': 'gb-empty', text: 'not computed yet' })); return; }
          comms.forEach(function (c) {
            var names = (c.top_members || []).slice(0, 3).map(function (m) { return m.label; }).join(', ');
            if (names.length > 30) names = names.slice(0, 29) + '…';
            var item = ctx.el('div', { 'class': 'gb-item', title: 'open this cluster in the graph' });
            item.appendChild(ctx.el('span', { text: (c.dominant_kind || 'cluster') + ' — ' + names }));
            item.appendChild(ctx.el('span', { 'class': 'gb-kind', text: String(c.size) }));
            item.addEventListener('click', function () {
              var top = (c.top_members || [])[0];
              if (top) { focusInput.value = top.label || top.id; _limitBoost = 150; loadGraph(); }
            });
            commList.appendChild(item);
          });
        }).catch(function () {});
      }

      // ---- breadcrumb trail ----
      function pushCrumb(id, label) {
        if (!id) return;
        if (_crumbs.length && _crumbs[_crumbs.length - 1].id === id) return;
        _crumbs.push({ id: id, label: label || id });
        if (_crumbs.length > 8) _crumbs = _crumbs.slice(-8);
        renderCrumbs();
      }
      function renderCrumbs() {
        crumbsEl.innerHTML = '';
        if (!_crumbs.length) return;
        crumbsEl.appendChild(ctx.el('span', { text: 'trail: ' }));
        _crumbs.forEach(function (c, i) {
          if (i) crumbsEl.appendChild(ctx.el('span', { 'class': 'crumb-sep', text: ' / ' }));
          var a = ctx.el('a', { text: c.label.length > 22 ? c.label.slice(0, 21) + '…' : c.label });
          a.addEventListener('click', function () { focusInput.value = c.label; loadGraph(); });
          crumbsEl.appendChild(a);
        });
      }

      // ---- legend (decoder ring + filter): entity chips hide/show, relation chips spotlight ----
      function buildLegend(g) {
        var legend = box.querySelector('.catalog-graph-legend');
        if (!legend) { legend = ctx.el('div', { 'class': 'catalog-graph-legend' }); box.appendChild(legend); }
        legend.innerHTML = '';
        var seen = {}, nodeKinds = [];
        (g.nodes || []).forEach(function (n) { var k = n.kind; if (k && !seen[k]) { seen[k] = 1; nodeKinds.push(k); } });
        if (nodeKinds.length) {
          legend.appendChild(ctx.el('span', { 'class': 'lg-head', text: 'entities (click to hide/show)' }));
          nodeKinds.forEach(function (k) {
            var chip = ctx.el('span', { 'class': 'lg-chip' + (_hiddenKinds[k] ? ' off' : ''), title: 'click to hide/show ' + k });
            var dot = ctx.el('i'); dot.style.background = COLORS[k] || '#B3A29D';
            chip.appendChild(dot); chip.appendChild(document.createTextNode(k));
            chip.addEventListener('click', function () { if (_hiddenKinds[k]) delete _hiddenKinds[k]; else _hiddenKinds[k] = 1; chip.classList.toggle('off', !!_hiddenKinds[k]); applyHiddenKinds(); });
            legend.appendChild(chip);
          });
        }
        var eseen = {}, edgeKinds = [];
        (g.edges || []).forEach(function (e) { var k = (e.kind || '').toUpperCase(); if (k && !eseen[k]) { eseen[k] = 1; edgeKinds.push(k); } });
        if (edgeKinds.length) {
          legend.appendChild(ctx.el('span', { 'class': 'lg-head', text: 'relationships (click to spotlight)' }));
          edgeKinds.forEach(function (k) {
            var st = edgeStyleFor(k);
            var chip = ctx.el('span', { 'class': 'lg-chip', title: 'click to spotlight ' + st.label });
            var sw = ctx.el('span', { 'class': 'eswatch' }); sw.style.borderTopColor = st.color; sw.style.borderTopStyle = st.line;
            chip.appendChild(sw); chip.appendChild(document.createTextNode(st.label));
            chip.addEventListener('click', function () { spotlight(k, true); });
            legend.appendChild(chip);
          });
        }
        applyHiddenKinds();
      }

      // ---- expand a node IN PLACE on double-click (walk the graph outward) ----
      function expandNode(id) {
        if (!_cy) return;
        fetch(ENDPOINT + '?center=' + encodeURIComponent(id) + '&hops=1&limit=80').then(function (r) { return r.json(); }).then(function (g) {
          var haveN = {}; _cy.nodes().forEach(function (n) { haveN[n.id()] = 1; });
          var haveE = {}; _cy.edges().forEach(function (e) { haveE[e.id()] = 1; });
          var newNodes = (g.nodes || []).filter(function (n) { return !haveN[n.id]; }).map(function (n) { return _nodeEl(n, _lastFocal); });
          var added = {}; for (var k in haveN) added[k] = 1; newNodes.forEach(function (n) { added[n.data.id] = 1; });
          var newEdges = (g.edges || []).map(_edgeEl).filter(function (e) { return !haveE[e.data.id] && added[e.data.source] && added[e.data.target]; });
          if (!newNodes.length && !newEdges.length) { metaEl.textContent = 'no further connections to expand here'; return; }
          _cy.add(newNodes); _cy.add(newEdges);
          _curNodeCount = _cy.nodes().length;
          var ranked = _cy.nodes().sort(function (a, b) { return b.degree(false) - a.degree(false); });
          ranked.forEach(function (nd, i) { nd.data('_rank', i); });
          _cy.layout(_layoutOpts(_layoutName(), _lastFocal)).run();
          applyZoomTier(); applyConfidenceFilter(); applyConnVerify(); applyHiddenKinds(); applyDateBrush(); applyNodeColoring();
          buildLegend({ nodes: _cy.nodes().map(function (n) { return n.data('raw') || { kind: n.data('kind') }; }),
                        edges: _cy.edges().map(function (e) { return e.data('raw') || { kind: e.data('kind') }; }) });
          metaEl.textContent = 'expanded — now showing ' + _cy.nodes().length + ' nodes, ' + _cy.edges().length + ' edges (double-click any node to keep expanding)';
        }).catch(function () {});
      }

      // ---- render a freshly-fetched graph ----
      function renderGraph(g) {
        var entitiesOnly = entCb.checked;
        var focalId = g.meta && g.meta.center ? g.meta.center : null;
        _lastFocal = focalId;
        var nodes = (g.nodes || []).filter(function (n) { return !entitiesOnly || ENTITY_KINDS[n.kind]; }).map(function (n) { return _nodeEl(n, focalId); });
        var keep = {}; nodes.forEach(function (n) { keep[n.data.id] = 1; });
        var edges = (g.edges || []).filter(function (e) { return keep[e.source] && keep[e.target]; }).map(_edgeEl);
        if (_cy) { _cy.destroy(); _cy = null; }
        var canvas = box.querySelector('.cg-canvas');
        if (!canvas) { box.innerHTML = ''; canvas = ctx.el('div', { 'class': 'cg-canvas' }); canvas.style.cssText = 'width:100%;height:100%'; box.appendChild(canvas); }
        _curNodeCount = nodes.length;
        _cy = window.cytoscape({
          container: canvas, elements: nodes.concat(edges), wheelSensitivity: 0.3,
          textureOnViewport: true, hideEdgesOnViewport: true, motionBlur: false, pixelRatio: 1,
          style: [
            { selector: 'node', style: {
                'background-color': function (n) { return COLORS[n.data('kind')] || '#B3A29D'; },
                'shape': function (n) { return SHAPES[n.data('kind')] || 'ellipse'; },
                'label': 'data(label)', 'font-size': 11, 'font-family': 'Open Sans, sans-serif', 'color': '#2A262A',
                'text-wrap': 'wrap', 'text-max-width': 140, 'min-zoomed-font-size': 7,
                'text-background-color': '#FFFFFF', 'text-background-opacity': 0.85, 'text-background-padding': 2,
                'text-background-shape': 'roundrectangle', 'width': sizeFor, 'height': sizeFor,
                'border-width': 2, 'border-color': '#FFFFFF' } },
            { selector: 'node[?isFocus]', style: { 'border-width': 5, 'border-color': '#3C1605', 'font-size': 14, 'font-weight': 'bold',
                'width': function (n) { return sizeFor(n) + 14; }, 'height': function (n) { return sizeFor(n) + 14; },
                'z-index': 99, 'shadow-blur': 18, 'shadow-color': '#3C1605', 'shadow-opacity': 0.4 } },
            { selector: 'node:selected', style: { 'border-width': 4, 'border-color': '#3C1605' } },
            { selector: 'edge', style: {
                'width': function (e) { return 1.2 + Math.min(4, Math.sqrt((e.data('raw') || {}).weight || 1) - 1); },
                'line-color': function (e) { return edgeStyleFor(e.data('kind')).color; },
                'line-style': function (e) { return edgeStyleFor(e.data('kind')).line; },
                'target-arrow-color': function (e) { return edgeStyleFor(e.data('kind')).color; },
                'target-arrow-shape': function (e) { return edgeStyleFor(e.data('kind')).arrow ? 'triangle' : 'none'; },
                'curve-style': 'bezier', 'arrow-scale': 0.9, 'opacity': edgeOpacity,
                'font-size': 9, 'font-family': 'Open Sans, sans-serif', 'color': '#5a4632',
                'text-rotation': 'autorotate', 'text-background-color': '#FFFFFF', 'text-background-opacity': 0.9, 'text-background-padding': 1 } },
            { selector: 'edge.hl, edge:selected', style: { 'label': 'data(label)', 'opacity': 1 } },
            { selector: '.faded', style: { 'opacity': 0.1, 'text-opacity': 0.08 } },
            { selector: 'node.hl', style: { 'border-color': '#3C1605', 'border-width': 3 } },
            { selector: '.legend-dim', style: { 'opacity': 0.08, 'text-opacity': 0.06 } },
            { selector: 'node.nolabel', style: { 'text-opacity': 0 } },
            { selector: 'edge.weak', style: { 'display': 'none' } },
            { selector: 'edge.coinc', style: { 'line-style': 'dashed', 'opacity': 0.18 } },
            { selector: 'edge.coinc-hidden', style: { 'display': 'none' } },
            { selector: 'node.kind-hidden', style: { 'display': 'none' } }
          ],
          layout: _layoutOpts(_layoutName(), focalId)
        });
        _cy.fit(undefined, 40);
        var ranked = _cy.nodes().sort(function (a, b) { return b.degree(false) - a.degree(false); });
        ranked.forEach(function (nd, i) { nd.data('_rank', i); });
        applyZoomTier();
        _cy.on('zoom', function () { if (_zoomTimer) clearTimeout(_zoomTimer); _zoomTimer = setTimeout(applyZoomTier, 90); });
        applyConfidenceFilter(); applyConnVerify(); applyHiddenKinds(); applyDateBrush(); applyNodeColoring();

        var _lastTap = { id: null, t: 0 };
        _cy.on('tap', 'node', function (evt) {
          var nid = evt.target.id(), now = Date.now();
          if (_lastTap.id === nid && now - _lastTap.t < 350) expandNode(nid);
          else Dossier.open(dossierOpts.entityEndpoint, nid, dossierOpts);
          _lastTap = { id: nid, t: now };
        });
        _cy.on('mouseover', 'node', function (evt) {
          if (_cy.nodes().length > 350) return;
          var nb = evt.target.closedNeighborhood();
          _cy.elements().difference(nb).addClass('faded'); nb.addClass('hl');
        });
        _cy.on('mouseout', 'node', function () { if (_cy.nodes().length <= 350) _cy.elements().removeClass('faded hl'); });
        buildLegend(g);
      }

      // ---- fetch + render the graph for the current controls ----
      function loadGraph() {
        var q = (focusInput.value || '').trim();
        var params = new URLSearchParams({ entities_only: entCb.checked ? 'true' : 'false' });
        var rel = relSel.value; if (rel) params.set('rels', rel);
        if (q) {
          params.set('q', q); params.set('hops', '2'); params.set('limit', String(120 + _limitBoost));
          if (/^\d{4}$/.test(q)) { params.set('structural', 'true'); params.set('limit', String(160 + _limitBoost)); }
        } else { params.set('limit', String(70 + _limitBoost)); }
        fetch(ENDPOINT + '?' + params.toString()).then(function (r) { return r.json(); }).then(function (g) {
          renderGraph(g);
          if (g.meta && g.meta.center) {
            var c = (g.nodes || []).filter(function (n) { return n.id === g.meta.center; })[0];
            pushCrumb(g.meta.center, c ? (c.label || c.id) : q);
          }
          metaEl.innerHTML = '';
          if (g.meta) {
            var shownN = g.meta.shown_nodes != null ? g.meta.shown_nodes : (g.nodes || []).length;
            var totN = g.meta.eligible_nodes != null ? g.meta.eligible_nodes : (g.meta.total_nodes != null ? g.meta.total_nodes : g.meta.n_nodes);
            var shownE = g.meta.shown_edges != null ? g.meta.shown_edges : (g.edges || []).length;
            metaEl.appendChild(ctx.el('span', { text: 'showing ' + shownN + ' of ' + totN + ' relevant nodes, ' + shownE + ' edges  -  double-click a node to expand it' }));
            if (g.meta.truncated) {
              var more = ctx.el('a', { text: 'Load more', title: 'raise the cap and pull in more neighbors' });
              more.addEventListener('click', function () { _limitBoost += 150; loadGraph(); });
              metaEl.appendChild(more);
            }
          }
          if (_yearRange) updateBrushMeta();   // keep the date-filter notice if a range is active
        }).catch(function (err) {
          box.innerHTML = '<div class="catalog-graph-loading">Graph failed to load: ' + err.message + '</div>';
        });
      }

      // ---- wire controls ----
      var doFocus = function () { _limitBoost = 0; loadGraph(); };
      node.querySelector('.cg-focus-btn').addEventListener('click', doFocus);
      focusInput.addEventListener('keydown', function (e) { if (e.key === 'Enter') doFocus(); });
      layoutSel.addEventListener('change', function () { if (_cy) { _cy.layout(_layoutOpts(_layoutName(), _lastFocal)).run(); _cy.fit(undefined, 40); } });
      entCb.addEventListener('change', loadGraph);
      relSel.addEventListener('change', function () { focusInput.value = ''; _limitBoost = 0; loadGraph(); });
      spSlider.addEventListener('input', function () {
        spVal.textContent = _spacing().toFixed(1) + 'x';
        if (_spTimer) clearTimeout(_spTimer);
        _spTimer = setTimeout(function () { if (_cy) { _cy.layout(_layoutOpts(_layoutName(), _lastFocal)).run(); _cy.fit(undefined, 50); } }, 200);
      });
      mwSlider.addEventListener('input', function () { mwVal.textContent = String(_minWeight()); applyConfidenceFilter(); });
      if (verifiedCb) verifiedCb.addEventListener('change', applyConnVerify);
      // Fit/Reset appear both in the top bar and the sidebar — bind whichever exist to one shared handler.
      var doFit = function () { if (_cy) _cy.fit(undefined, 30); };
      var doReset = function () { focusInput.value = ''; relSel.value = ''; _limitBoost = 0; _yearRange = null; if (timelineEl) [].forEach.call(timelineEl.querySelectorAll('.tlbar.inrange'), function (b) { b.classList.remove('inrange'); }); loadGraph(); };
      [fitBtn, node.querySelector('.cg-fit-side')].forEach(function (b) { if (b) b.addEventListener('click', doFit); });
      [resetBtn, node.querySelector('.cg-reset-side')].forEach(function (b) { if (b) b.addEventListener('click', doReset); });

      // settings-panel toggle (right sidebar collapse) — mirrors the OLD app's "Hide settings panel"
      var settingsToggle = node.querySelector('.cg-settings-toggle');
      var settingsAside = node.querySelector('.catalog-graph-settings');
      if (settingsToggle && settingsAside) {
        settingsToggle.addEventListener('click', function () {
          var collapsed = settingsAside.classList.toggle('collapsed');
          settingsToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
          settingsToggle.textContent = collapsed ? 'Show settings panel' : 'Hide settings panel';
          // give cytoscape a beat to see the new width, then refit so the graph fills the freed space
          setTimeout(function () { if (_cy) { _cy.resize(); _cy.fit(undefined, 30); } }, 240);
        });
      }

      // entity browser: debounced search + kind filter; colour-by-community toggle
      if (browseInput) browseInput.addEventListener('input', function () { if (_browseTimer) clearTimeout(_browseTimer); _browseTimer = setTimeout(loadBrowse, 220); });
      if (browseKind) browseKind.addEventListener('change', loadBrowse);
      if (cbcCb) cbcCb.addEventListener('change', function () { _colorByCommunity = cbcCb.checked; applyNodeColoring(); });
      // communities browser: load on first open of the <details>
      if (commWrap) commWrap.addEventListener('toggle', function () { if (commWrap.open) loadCommunities(); });

      // help modal ("?" button): scrollable walkthrough, background scroll locked while open
      if (helpBtn) {
        var helpBd = ctx.el('div', { 'class': 'catalog-graph-helpbd' });
        var modal = ctx.el('div', { 'class': 'ghmodal' });
        modal.appendChild(ctx.el('button', { 'class': 'gh-x', 'aria-label': 'close', text: '×' }));
        modal.innerHTML += GRAPH_HELP_HTML;
        helpBd.appendChild(modal);
        document.body.appendChild(helpBd);
        var closeHelp = function () { helpBd.classList.remove('open'); document.body.style.overflow = ''; };
        helpBtn.addEventListener('click', function () { helpBd.classList.add('open'); document.body.style.overflow = 'hidden'; });
        helpBd.addEventListener('click', function (e) { if (e.target === helpBd) closeHelp(); });
        var xb = modal.querySelector('.gh-x'); if (xb) xb.addEventListener('click', closeHelp);
        document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeHelp(); });
      }

      // load cytoscape + fcose (best-effort), then first fetch + timeline
      loadScriptOnce(CDN)
        .then(function () { return loadScriptOnce(LAYOUT_BASE).catch(function () {}); })
        .then(function () { return loadScriptOnce(COSE_BASE).catch(function () {}); })
        .then(function () { return loadScriptOnce(FCOSE_CDN).catch(function () {}); })
        .then(function () { _fcoseReady(); loadGraph(); if (block.timeline !== false) renderTimeline(); loadBrowse(); })
        .catch(function (err) { box.innerHTML = '<div class="catalog-graph-loading">Graph engine failed to load: ' + err.message + '</div>'; });
    }
  };

  // walkthrough copy for the graph "?" help modal (mirrors the OLD reading-room help, generalised)
  var GRAPH_HELP_HTML = '<h3>Using the knowledge graph</h3>' +
    '<p>The full graph is large — tens of thousands of entities and connections drawn from every letter and notebook entry. You never see it all at once. Instead you <b>explore it</b>: start somewhere, then walk outward and reorganize as you go.</p>' +
    '<h4>1. Start somewhere</h4><ul>' +
    '<li><b>Search</b> in the focus box for any name, place, condition, or year and press Focus.</li>' +
    '<li>Or use <b>Find an entity to start from</b> below the graph — it lists the most-connected entities (filter by kind). Click one to focus it.</li>' +
    '<li>Click a <b>timeline bar</b> to start from a given year.</li></ul>' +
    '<h4>2. Walk the graph outward</h4><ul>' +
    '<li><b>Double-click any node to expand it</b> — its connections are pulled into the view. Keep double-clicking to travel across the whole graph.</li>' +
    '<li><b>Single-click</b> a node for its full dossier (summary, connections with sources, biography).</li>' +
    '<li><b>Load more</b> pulls in neighbours when a busy hub was capped; <b>Reset</b> returns to the high-level backbone; the <b>trail</b> is your path back.</li></ul>' +
    '<h4>3. Reorganize what you see</h4><ul>' +
    '<li><b>Legend entities</b> — click a kind to hide or show it. <b>Legend relationships</b> — click to spotlight just those links.</li>' +
    '<li><b>Relationships</b> dropdown shows only one kind of link. <b>Layout</b> — "clusters" groups what connects; "importance rings" puts the most-connected in the centre. <b>Spacing</b> spreads nodes apart (then press <b>Fit</b>).</li>' +
    '<li><b>Min weight</b> hides weak "appears with" links; <b>drag the timeline</b> to filter to a year range (double-click the strip to clear).</li>' +
    '<li><b>Colour nodes by community</b> recolours everything by its thematic cluster; open <b>Communities</b> to jump straight into a cluster.</li></ul>' +
    '<h4>Reading it</h4><ul>' +
    '<li><b>Colour + shape</b> = entity kind (person round, place diamond, organization box, condition hexagon…). <b>Size</b> = how often it is mentioned.</li>' +
    '<li><b>Line colour</b> = relationship kind. <b>Fainter / dashed</b> lines are less certain. The thick-ringed node is the one you focused.</li></ul>';

  // ---- marker-map: points on a Leaflet map from a JSON endpoint -------------
  injectCSS('catalog-map-css',
    '.catalog-map{width:100%;border:1px solid var(--provincial-mid,#ccc);border-radius:8px;overflow:hidden;background:#fff}' +
    '.catalog-map .leaflet-popup-content{font-family:var(--font-body,sans-serif);font-size:.82rem;line-height:1.45}' +
    '.catalog-map .leaflet-popup-content b{font-family:var(--font-head,inherit);color:var(--provincial,#3C1605)}');
  function loadCSSOnce(href) {
    if (document.querySelector('link[href="' + href + '"]')) return;
    var l = document.createElement('link'); l.rel = 'stylesheet'; l.href = href; document.head.appendChild(l);
  }

  window.CatalogBlocks['marker-map'] = {
    label: 'Marker map', category: 'Interactive', icon: 'M',
    blank: function () {
      return { type: 'marker-map', endpoint: '/api/map', listField: 'places', latField: 'lat', lonField: 'lon',
               labelField: 'name', descField: 'description', center: [42.33, -83.05], zoom: 4, height: '62vh',
               tileUrl: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png' };
    },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Markers endpoint' },
      { key: 'listField', type: 'text', label: 'List field' },
      { key: 'height', type: 'text', label: 'Height (CSS)' },
      { key: 'zoom', type: 'number', label: 'Initial zoom' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section' });
      var box = ctx.el('div', { 'class': 'catalog-map' });
      box.style.height = block.height || '62vh';
      box.appendChild(ctx.el('div', { 'class': 'catalog-graph-loading', text: 'Loading map…' }));
      sec.appendChild(box);
      return sec;
    },
    init: function (node, block, ctx) {
      var box = node.querySelector('.catalog-map');
      loadCSSOnce('https://unpkg.com/leaflet@1.9.4/dist/leaflet.css');
      var endpoint = block.endpoint || '/api/map';
      var lf = block.listField || 'places', latf = block.latField || 'lat', lonf = block.lonField || 'lon',
          labf = block.labelField || 'name', df = block.descField || 'description';
      Promise.all([loadScriptOnce('https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'),
                   fetch(endpoint).then(function (r) { return r.json(); })])
        .then(function (res) {
          var L = window.L, items = (res[1] && res[1][lf]) || []; box.innerHTML = '';
          var div = ctx.el('div'); div.style.cssText = 'width:100%;height:100%'; box.appendChild(div);
          var map = L.map(div, { scrollWheelZoom: true }).setView(block.center || [41.8, -85], block.zoom || 5);
          // CARTO Voyager basemap — clean + warm behind the brand markers (like the old app)
          L.tileLayer(block.tileUrl || 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
            { attribution: block.attribution || '© OpenStreetMap © CARTO', subdomains: 'abcd', maxZoom: 19 }).addTo(map);
          var max = 1; items.forEach(function (it) { if ((it.mentions || 0) > max) max = it.mentions; });
          var stack = {}, pts = [];
          items.forEach(function (it) {
            var la = it[latf], lo = it[lonf]; if (la == null || lo == null) return;
            var key = la.toFixed(3) + ',' + lo.toFixed(3), n = (stack[key] = (stack[key] || 0) + 1);
            if (n > 1) { var a = n * 2.399, rad = 0.012 * Math.sqrt(n); la += rad * Math.cos(a); lo += rad * Math.sin(a); }   // spiral so co-located places don't hide
            var r = 4 + 18 * Math.sqrt((it.mentions || 1) / max);                         // size RELATIVE to the busiest place
            var m = L.circleMarker([la, lo], { radius: r, color: '#3C1605', fillColor: '#9a7b5b', fillOpacity: 0.6, weight: 1 }).addTo(map);
            var sv = 'https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=' + it[latf] + ',' + it[lonf];
            var desc = it[df] || '';
            m.bindPopup('<b>' + (it[labf] || '') + '</b>' + (it.role ? '<br>' + it.role : '') +
              (it.mentions ? '  (' + it.mentions + ' mentions)' : '') + (desc ? '<br>' + desc.slice(0, 240) : '') +
              '<br><a href="' + sv + '" target="_blank" rel="noopener">Open Street View here</a>' +
              (it.id ? '<br><i>Click the dot for all source passages.</i>' : ''));
            if (it.id) { (function (eid) { m.on('click', function () { Dossier.open(block.entityEndpoint || '/api/entity', eid, { entityEndpoint: block.entityEndpoint || '/api/entity', connectionEndpoint: block.connectionEndpoint || '/api/connection', regionEndpoint: block.regionEndpoint || '/api/region' }); }); })(it.id); }
            pts.push([la, lo]);
          });
          setTimeout(function () { map.invalidateSize(); }, 120);
        })
        .catch(function (err) { box.innerHTML = '<div class="catalog-graph-loading">Map failed to load: ' + err.message + '</div>'; });
    }
  };

  // ---- flow-diagram: a positioned node/edge pipeline (lightweight SVG, no deps) ----
  injectCSS('catalog-flow-css',
    '.catalog-flow{width:100%;border:1px solid var(--provincial-mid,#ccc);border-radius:8px;background:#fff;overflow:auto;padding:.5em}' +
    '.catalog-flow svg{display:block;width:100%;height:auto}' +
    '.catalog-flow .cf-node rect{fill:#fff;stroke:var(--provincial,#3C1605);stroke-width:1.5}' +
    '.catalog-flow .cf-node.io rect{fill:var(--provincial-light,#E5DFDE)}' +
    '.catalog-flow .cf-node text{font-family:var(--font-head,sans-serif);font-size:10px;fill:var(--provincial,#3C1605)}' +
    '.catalog-flow .cf-edge{stroke:var(--provincial-mid,#B3A29D);stroke-width:1.4;fill:none;marker-end:url(#cf-arrow)}');

  window.CatalogBlocks['flow-diagram'] = {
    label: 'Flow diagram', category: 'Interactive', icon: 'F',
    blank: function () { return { type: 'flow-diagram', endpoint: '/api/agent_graph', height: '58vh' }; },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Topology endpoint (JSON {nodes,edges})' },
      { key: 'height', type: 'text', label: 'Max height (CSS)' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section' });
      var box = ctx.el('div', { 'class': 'catalog-flow' }); box.style.maxHeight = block.height || '58vh';
      box.appendChild(ctx.el('div', { 'class': 'catalog-graph-loading', text: 'Loading pipeline…' }));
      sec.appendChild(box); return sec;
    },
    init: function (node, block, ctx) {
      var box = node.querySelector('.catalog-flow');
      fetch(block.endpoint || '/api/agent_graph').then(function (r) { return r.json(); }).then(function (d) {
        var nodes = d.nodes || [], edges = d.edges || [], by = {}; nodes.forEach(function (n) { by[n.id] = n; });
        if (!nodes.length) { box.innerHTML = '<div class="catalog-graph-loading">No pipeline.</div>'; return; }
        var NW = 120, NH = 40, NS = 'http://www.w3.org/2000/svg';
        function E(t, a) { var e = document.createElementNS(NS, t); for (var k in a) e.setAttribute(k, a[k]); return e; }
        var xs = nodes.map(function (n) { return n.x; }), ys = nodes.map(function (n) { return n.y; });
        var minX = Math.min.apply(null, xs) - 10, minY = Math.min.apply(null, ys) - 10;
        var W = Math.max.apply(null, xs) + NW + 10 - minX, H = Math.max.apply(null, ys) + NH + 10 - minY;
        var svg = E('svg', { viewBox: minX + ' ' + minY + ' ' + W + ' ' + H });
        var defs = E('defs'), mk = E('marker', { id: 'cf-arrow', markerWidth: 8, markerHeight: 8, refX: 7, refY: 3, orient: 'auto' });
        mk.appendChild(E('path', { d: 'M0,0 L7,3 L0,6 Z', fill: '#B3A29D' })); defs.appendChild(mk); svg.appendChild(defs);
        edges.forEach(function (ed) {
          var a = by[ed.from], b = by[ed.to]; if (!a || !b) return;
          svg.appendChild(E('line', { 'class': 'cf-edge', x1: a.x + NW / 2, y1: a.y + NH / 2, x2: b.x + NW / 2, y2: b.y + NH / 2 }));
        });
        nodes.forEach(function (n) {
          var g = E('g', { 'class': 'cf-node ' + (n.kind || '') });
          g.appendChild(E('rect', { x: n.x, y: n.y, width: NW, height: NH, rx: 6 }));
          var t = E('text', { x: n.x + NW / 2, y: n.y + NH / 2 + 4, 'text-anchor': 'middle' });
          t.textContent = n.label || n.id; g.appendChild(t); svg.appendChild(g);
        });
        box.innerHTML = ''; box.appendChild(svg);
      }).catch(function (err) { box.innerHTML = '<div class="catalog-graph-loading">Flow failed: ' + err.message + '</div>'; });
    }
  };

  // ---- avatar-stage: a speaking-avatar panel wired to a TTS endpoint (3D-ready) ----
  injectCSS('catalog-avatar-css',
    '.catalog-avatar{display:flex;flex-direction:column;align-items:center;gap:1.2em;border:1px solid var(--provincial-mid,#ccc);border-radius:8px;background:#fff;padding:1.6em}' +
    '.catalog-avatar-stage{width:100%;max-width:420px;aspect-ratio:1/1;border-radius:8px;background:radial-gradient(circle at 50% 32%, var(--provincial-light,#E5DFDE), #fff);display:flex;align-items:center;justify-content:center;color:var(--provincial-mid,#999);font-style:italic;overflow:hidden}' +
    '.catalog-avatar-say{display:flex;gap:.5em;width:100%;max-width:560px}' +
    '.catalog-avatar-say input{flex:1;border:0;border-bottom:1px solid var(--provincial-mid,#ccc);font-size:1rem;padding:.5em;background:transparent;color:var(--site-ink,#2A262A)}' +
    '.catalog-avatar-say button{background:var(--provincial,#3C1605);color:#fff;border:0;border-radius:3px;padding:.6em 1.3em;font-family:var(--font-head,inherit);font-weight:700;text-transform:uppercase;letter-spacing:.06em;font-size:.72rem;cursor:pointer}');

  window.CatalogBlocks['avatar-stage'] = {
    label: 'Avatar stage', category: 'Interactive', icon: 'A',
    blank: function () { return { type: 'avatar-stage', glbUrl: '', ttsEndpoint: '/api/tts', portraitUrl: '', placeholder: 'Avatar' }; },
    fields: [
      { key: 'glbUrl', type: 'text', label: '3D model (.glb) URL' },
      { key: 'ttsEndpoint', type: 'text', label: 'TTS endpoint' },
      { key: 'portraitUrl', type: 'image', label: 'Portrait (fallback)' },
      { key: 'placeholder', type: 'text', label: 'Placeholder text' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-avatar' });
      var stage = ctx.el('div', { 'class': 'catalog-avatar-stage' });
      if (block.portraitUrl) {
        var img = ctx.el('img', { src: ctx.mediaUrl(block.portraitUrl), alt: '' });
        img.style.cssText = 'width:100%;height:100%;object-fit:cover'; stage.appendChild(img);
      } else { stage.appendChild(ctx.el('div', { text: block.placeholder || 'Avatar' })); }
      sec.appendChild(stage);
      var say = ctx.el('form', { 'class': 'catalog-avatar-say' });
      say.appendChild(ctx.el('input', { type: 'text', placeholder: 'Type something for the avatar to say…' }));
      if (block.voice === 'server' && block.voicePicker !== false) {
        say.appendChild(ctx.el('select', { 'class': 'catalog-conv-provider', title: 'voice provider' }));
      }
      say.appendChild(ctx.el('button', { type: 'submit', text: 'Speak' }));
      sec.appendChild(say);
      return sec;
    },
    init: function (node, block, ctx) {
      var form = node.querySelector('.catalog-avatar-say'), input = form.querySelector('input');
      var provSel = node.querySelector('.catalog-conv-provider');
      if (provSel) {
        fetch(block.voicesEndpoint || '/api/voices').then(function (r) { return r.json(); }).then(function (d) {
          (d.tts || []).forEach(function (pv) { provSel.appendChild(ctx.el('option', { value: pv.id, text: pv.label + (pv.configured ? '' : ' (no key)') })); });
          provSel.value = d.default_tts || 'local';
        }).catch(function () { ['local', 'azure', 'gcp'].forEach(function (id) { provSel.appendChild(ctx.el('option', { value: id, text: id })); }); });
      }
      form.addEventListener('submit', function (e) {
        e.preventDefault();
        var t = (input.value || '').trim(); if (!t) { return; }
        // local|azure|gcp per the picker; browser is the fallback when no server voice is selected
        Voice.speak(t, { voice: block.voice || 'browser', endpoint: block.ttsEndpoint, provider: (provSel && provSel.value) || undefined });
        input.value = '';
      });
    }
  };

  // ===========================================================================
  // CONTENT-PAGE PARTS — markdown-driven explainer / step / catalogue blocks.
  // GENERAL by construction: each fetches ONLY the endpoint(s) named in its own
  // block config and renders whatever JSON shape they describe, so the same part
  // serves any backend. (Mirrors the OLD reading-room app's Walkthrough / How-it-
  // works / Tools & data / Explanations tabs, ported to standalone Pagesmith pages.)
  // ===========================================================================

  // Minimal, safe-enough markdown → HTML (headings, bold, inline code, fenced code,
  // lists, paragraphs). Escapes first so untrusted markdown can't inject. Mirrors the
  // OLD app's mdToHtml() including the brand-styled headings/code blocks.
  function mdToHtml(md) {
    var esc = function (s) { return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); };
    var lines = (md || '').split('\n'), html = '', inCode = false, inList = false;
    for (var i = 0; i < lines.length; i++) {
      var ln = lines[i];
      if (ln.indexOf('```') === 0) {
        if (inCode) { html += '</pre>'; inCode = false; }
        else { if (inList) { html += '</ul>'; inList = false; } html += "<pre style='background:var(--provincial-light,#f1ece7);padding:.6em;border-radius:6px;overflow:auto;font-size:12px;'>"; inCode = true; }
        continue;
      }
      if (inCode) { html += esc(ln) + '\n'; continue; }
      var t = esc(ln).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>').replace(/`([^`]+)`/g, '<code>$1</code>');
      if (/^#{1,6}\s/.test(ln)) {
        if (inList) { html += '</ul>'; inList = false; }
        var lvl = Math.min(ln.match(/^#+/)[0].length + 1, 5);
        html += "<h" + lvl + " style='font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);margin:.8em 0 .3em;'>" + t.replace(/^#{1,6}\s/, '') + "</h" + lvl + ">";
      } else if (/^[-*]\s/.test(ln)) {
        if (!inList) { html += '<ul>'; inList = true; }
        html += '<li>' + t.replace(/^[-*]\s/, '') + '</li>';
      } else if (ln.trim() === '') {
        if (inList) { html += '</ul>'; inList = false; }
      } else {
        if (inList) { html += '</ul>'; inList = false; }
        html += '<p>' + t + '</p>';
      }
    }
    if (inList) html += '</ul>';
    if (inCode) html += '</pre>';
    return html;
  }

  // Shared styling for the content parts — explainer cards, stat grid, text-flow
  // strip, nav buttons, markdown body. Ported 1:1 from the OLD app's theme so the
  // pages match the reading-room look exactly.
  injectCSS('catalog-content-css',
    '.catalog-content{font-family:var(--font-body,sans-serif);color:var(--site-ink,#2A262A)}' +
    '.catalog-content .cc-intro{font-size:14px;line-height:1.6;color:var(--provincial-dark,#826962);margin:.2em 0 1.1em}' +
    '.catalog-content .cc-narr{font-size:14px;line-height:1.55;margin-bottom:1.3em}' +
    '.catalog-content .cc-narr h3{color:var(--provincial,#3C1605);font-family:var(--font-head,sans-serif);margin:.2em 0 .4em}' +
    '.explain-card{border:1px solid var(--provincial-mid,#ddd);border-radius:8px;padding:.7em .85em;margin-bottom:.7em;background:#fff;animation:catRiseIn .5s cubic-bezier(.2,.7,.2,1) both}' +
    '.explain-card h4{margin:0 0 .2em;font-size:14px;font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);display:flex;align-items:center;flex-wrap:wrap;gap:.4em}' +
    '.explain-card>div{font-size:13.5px;line-height:1.55}' +
    '.explain-card .tag{font-size:11px;font-weight:600;padding:.1em .45em;border-radius:4px;margin-left:.2em}' +
    '.explain-card .tag.paid{background:#f6e3c8;color:#8a5a13}' +
    '.explain-card .tag.free{background:#d9ead9;color:#2f6b3a}' +
    '.explain-card .tag.done{background:#d9ead9;color:#2f6b3a}' +
    '.explain-card .tag.progress{background:#f6e3c8;color:#8a5a13}' +
    '.explain-card .muted-note{font-size:11.5px;color:var(--provincial-dark,#826962);font-weight:400}' +
    '.statgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:.5em;margin:.4em 0 1em}' +
    '.statgrid .stat{background:var(--provincial-light,#f1ece7);border-radius:8px;padding:.6em .7em}' +
    '.statgrid .stat .n{font-family:var(--font-head,sans-serif);font-weight:800;font-size:20px;color:var(--provincial,#3C1605)}' +
    '.statgrid .stat .l{font-size:11px;color:var(--provincial-dark,#826962)}' +
    '.catalog-content .cc-flow{display:flex;align-items:stretch;gap:.4em;flex-wrap:wrap;margin:.6em 0 1.2em}' +
    '.catalog-content .cc-flow .step{flex:1 1 120px;background:var(--provincial-light,#f1ece7);border:1px solid var(--provincial-mid,#ddd);border-radius:6px;padding:.6em .7em;font-size:12px;line-height:1.4}' +
    '.catalog-content .cc-flow .step b{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);display:block;font-size:12px;margin-bottom:.15em}' +
    '.catalog-content .cc-flow .arrow{align-self:center;color:var(--provincial-mid,#B3A29D);font-weight:700}' +
    '.catalog-content .cc-nav{display:flex;gap:.35em;flex-wrap:wrap;margin-bottom:.9em}' +
    '.catalog-content .cc-navbtn{background:var(--provincial-light,#f1ece7);color:var(--provincial-dark,#826962);border:1px solid var(--provincial-mid,#ddd);border-radius:4px;padding:.45em .8em;font-family:var(--font-head,sans-serif);font-weight:700;font-size:.72rem;letter-spacing:.03em;cursor:pointer}' +
    '.catalog-content .cc-navbtn:hover{background:var(--provincial-mid,#ddd)}' +
    '.catalog-content .cc-navbtn.active{background:var(--provincial,#3C1605);color:#fff;border-color:var(--provincial,#3C1605)}' +
    '.catalog-content .cc-mdbody{font-size:14px;line-height:1.6}' +
    // list styling for rendered markdown (mirrors the OLD app's help-body lists)
    '.catalog-content .cc-mdbody ul,.catalog-content .cc-mdbody ol{margin:.3em 0 .8em 1.2em;padding:0}' +
    '.catalog-content .cc-mdbody li{margin:.3em 0}' +
    '.catalog-content .cc-mdbody p{margin:.5em 0}' +
    '.catalog-content .cc-narr ul,.catalog-content .cc-narr ol{margin:.3em 0 .8em 1.2em;padding:0}' +
    '.catalog-content .cc-narr li{margin:.3em 0}' +
    '.catalog-content .cc-mdbody code{background:var(--provincial-light,#f1ece7);padding:.05em .35em;border-radius:3px;font-family:var(--engine-mono,monospace);font-size:.92em}' +
    '.catalog-content .cc-h{font-family:var(--font-head,sans-serif);color:var(--provincial,#3C1605);font-size:1.2rem;margin:1.3em 0 .5em}' +
    '.catalog-content .cc-muted{font-size:12px;color:var(--provincial-dark,#826962);line-height:1.5}' +
    '@keyframes catRiseIn{from{opacity:0;transform:translateY(8px)}}');

  function fmtNum(n) { return (typeof n === 'number') ? n.toLocaleString() : n; }

  // ---- step-cards: numbered status-tagged step cards + a "what's left" list, from an endpoint
  // returning {title, intro, steps:[{n,title,summary,status,folder,next}], todo:[{title,detail}]}.
  // Ported from the OLD app's Walkthrough tab. GENERAL: endpoint + field names are config. ----
  window.CatalogBlocks['step-cards'] = {
    label: 'Step cards', category: 'Interactive', icon: 'S',
    blank: function () { return { type: 'step-cards', endpoint: '/api/walkthrough', todoHeading: "What's left to build" }; },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Steps endpoint (JSON {title,intro,steps,todo})' },
      { key: 'todoHeading', type: 'text', label: 'Heading for the todo list' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-content' });
      sec.appendChild(ctx.el('div', { 'class': 'cc-intro', text: 'Loading…' }));
      return sec;
    },
    init: function (node, block, ctx) {
      var E = ctx.el, host = node;
      fetch(block.endpoint || '/api/walkthrough').then(function (r) { return r.json(); }).then(function (w) {
        host.innerHTML = '';
        if (w.intro) host.appendChild(E('div', { 'class': 'cc-intro', text: w.intro }));
        (w.steps || []).forEach(function (s) {
          var status = s.status || '';
          var cls = status.indexOf('done') >= 0 ? 'done' : (status.indexOf('progress') >= 0 ? 'progress' : '');
          var h4 = E('h4', {}, ['Step ' + (s.n != null ? s.n + '. ' : '') + (s.title || '')]);
          if (status) h4.appendChild(E('span', { 'class': 'tag ' + cls, text: status }));
          if (s.folder) h4.appendChild(E('span', { 'class': 'muted-note', text: s.folder }));
          var card = E('div', { 'class': 'explain-card' }, [h4, E('div', { text: s.summary || '' })]);
          if (s.next) card.appendChild(E('div', { 'class': 'muted-note', style: 'margin-top:.35em', text: 'Next: ' + s.next }));
          host.appendChild(card);
        });
        if ((w.todo || []).length) {
          host.appendChild(E('div', { 'class': 'cc-h', text: block.todoHeading || "What's left to build" }));
          (w.todo || []).forEach(function (t) {
            host.appendChild(E('div', { 'class': 'explain-card' }, [E('h4', { text: t.title || '' }), E('div', { text: t.detail || '' })]));
          });
        }
      }).catch(function () { host.innerHTML = '<div class="cc-muted">Walkthrough unavailable — start the backend.</div>'; });
    }
  };

  // ---- doc-explorer: a button-nav switcher over long-form markdown docs, from an endpoint returning
  // {docs:[{name,title,markdown}]}. Ported from the OLD app's Explanations tab. GENERAL. ----
  window.CatalogBlocks['doc-explorer'] = {
    label: 'Doc explorer', category: 'Interactive', icon: 'D',
    blank: function () { return { type: 'doc-explorer', endpoint: '/api/explain', listField: 'docs' }; },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Docs endpoint (JSON {docs:[{title,markdown}]})' },
      { key: 'listField', type: 'text', label: 'List field' }
    ],
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-content' });
      sec.appendChild(ctx.el('div', { 'class': 'cc-nav' }));
      sec.appendChild(ctx.el('div', { 'class': 'cc-mdbody', text: 'Loading…' }));
      return sec;
    },
    init: function (node, block, ctx) {
      var E = ctx.el, nav = node.querySelector('.cc-nav'), body = node.querySelector('.cc-mdbody');
      var lf = block.listField || 'docs';
      fetch(block.endpoint || '/api/explain').then(function (r) { return r.json(); }).then(function (d) {
        var docs = d[lf] || d.docs || [];
        nav.innerHTML = ''; body.innerHTML = '';
        if (!docs.length) { body.innerHTML = '<div class="cc-muted">No docs found.</div>'; return; }
        var btns = [];
        var show = function (i) {
          btns.forEach(function (b, j) { b.className = 'cc-navbtn' + (j === i ? ' active' : ''); });
          body.innerHTML = mdToHtml(docs[i].markdown || '');
        };
        docs.forEach(function (doc, i) {
          var b = E('button', { 'class': 'cc-navbtn', type: 'button', text: doc.title || doc.name || ('Doc ' + (i + 1)) });
          b.addEventListener('click', function () { show(i); });
          btns.push(b); nav.appendChild(b);
        });
        show(0);
      }).catch(function () { body.innerHTML = '<div class="cc-muted">Explanations unavailable — start the backend.</div>'; });
    }
  };

  // ---- pipeline-overview: a narrative-markdown intro + a fixed left-to-right flow strip of named
  // stages + a live stat grid pulled from a graph/meta endpoint. Ported from the OLD app's
  // "How it works" tab. GENERAL: narrative endpoint, stats endpoint, and the stage list are all config. ----
  window.CatalogBlocks['pipeline-overview'] = {
    label: 'Pipeline overview', category: 'Interactive', icon: 'P',
    blank: function () {
      return { type: 'pipeline-overview', narrativeEndpoint: '/api/his_life', narrativeField: 'markdown',
        narrativeHeading: 'The life this archive documents', statsEndpoint: '/api/graph?limit=10',
        stages: [
          ['Scanned pages', "OCR'd page images"],
          ['Documents', 'segmented: letters + notebook entries'],
          ['NER', 'people, places, conditions, favors, dates extracted'],
          ['Resolve', 'cautious merge → canonical entities'],
          ['Knowledge graph', 'RiC-O temporal graph: entities + records + dates'],
          ['Embed + index', 'hybrid BM25 + dense vectors; FTS; IIIF'],
          ['Ask', 'tool-using agent answers with cited sources']
        ],
        footnote: 'This is a developer & evaluation harness: you can swap the LLM, embedding space, and reranker, toggle retrieval techniques, and watch the agent’s trace + cost for every question. Every answer cites the exact scanned region.' };
    },
    fields: [
      { key: 'narrativeEndpoint', type: 'text', label: 'Narrative endpoint (JSON {markdown})' },
      { key: 'narrativeHeading', type: 'text', label: 'Narrative heading' },
      { key: 'statsEndpoint', type: 'text', label: 'Stats endpoint (JSON {meta})' },
      { key: 'footnote', type: 'textarea', label: 'Footnote' }
    ],
    lists: { stages: { label: 'Stage', fields: [ { key: '0', type: 'text', label: 'Title' }, { key: '1', type: 'text', label: 'Detail' } ] } },
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-content' });
      sec.appendChild(ctx.el('div', { 'class': 'cc-narr' }));
      var flow = ctx.el('div', { 'class': 'cc-flow' });
      var stages = block.stages || [];
      stages.forEach(function (s, i) {
        var step = ctx.el('div', { 'class': 'step' }, [ctx.el('b', { text: (s && s[0]) || '' }), (s && s[1]) || '']);
        flow.appendChild(step);
        if (i < stages.length - 1) flow.appendChild(ctx.el('div', { 'class': 'arrow', text: '→' }));
      });
      sec.appendChild(flow);
      sec.appendChild(ctx.el('div', { 'class': 'statgrid' }));
      if (block.footnote) sec.appendChild(ctx.el('p', { 'class': 'cc-muted', text: block.footnote }));
      return sec;
    },
    init: function (node, block, ctx) {
      var E = ctx.el, narr = node.querySelector('.cc-narr'), stats = node.querySelector('.statgrid');
      // archive-derived narrative up top (the human point of the machinery below)
      fetch(block.narrativeEndpoint || '/api/his_life').then(function (r) { return r.json(); }).then(function (hl) {
        var md = hl[block.narrativeField || 'markdown'] || hl.markdown || '';
        if (!md) { narr.style.display = 'none'; return; }
        narr.innerHTML = '';
        if (block.narrativeHeading) narr.appendChild(E('h3', { text: block.narrativeHeading }));
        narr.appendChild(E('div', { html: mdToHtml(md) }));
      }).catch(function () { narr.style.display = 'none'; });
      // live counts from the graph meta
      fetch(block.statsEndpoint || '/api/graph?limit=10').then(function (r) { return r.json(); }).then(function (g) {
        var m = g.meta || {}, kinds = m.node_kinds || {};
        var pairs = [
          [m.total_nodes != null ? m.total_nodes : m.n_nodes, 'graph nodes'],
          [m.total_edges != null ? m.total_edges : m.n_edges, 'graph edges'],
          [kinds.person, 'people'], [kinds.place, 'places'],
          [kinds.organization, 'organizations'], [kinds.condition, 'conditions'],
          [kinds.favor, 'favors'], [kinds.year, 'years on the timeline']
        ];
        stats.innerHTML = '';
        pairs.forEach(function (p) {
          if (p[0] == null) return;
          stats.appendChild(E('div', { 'class': 'stat' }, [E('div', { 'class': 'n', text: fmtNum(p[0]) }), E('div', { 'class': 'l', text: p[1] })]));
        });
        if (!stats.children.length) stats.appendChild(E('div', { 'class': 'cc-muted', text: 'graph not built yet' }));
      }).catch(function () { stats.innerHTML = '<div class="cc-muted">graph not built yet</div>'; });
    }
  };

  // ---- tool-catalog: explainer cards for each tool (free/paid tagged) + a stat grid combining a
  // running-cost endpoint with static corpus counts. Ported from the OLD app's "Tools & data" tab.
  // GENERAL: tools endpoint, cost endpoint, and the static stat rows are all config. ----
  window.CatalogBlocks['tool-catalog'] = {
    label: 'Tool catalogue', category: 'Interactive', icon: 'T',
    blank: function () {
      return { type: 'tool-catalog', endpoint: '/api/tools', listField: 'tools', costEndpoint: '/api/cost',
        costLabel: 'session API cost (all stages)',
        stats: [['570', 'letters'], ['8,657', 'notebook entries']] };
    },
    fields: [
      { key: 'endpoint', type: 'text', label: 'Tools endpoint (JSON {tools:[{name,description,cost_note}]})' },
      { key: 'listField', type: 'text', label: 'List field' },
      { key: 'costEndpoint', type: 'text', label: 'Cost endpoint (JSON {summary})' },
      { key: 'costLabel', type: 'text', label: 'Cost stat label' }
    ],
    lists: { stats: { label: 'Static stat', fields: [ { key: '0', type: 'text', label: 'Big number' }, { key: '1', type: 'text', label: 'Label' } ] } },
    render: function (block, ctx) {
      var sec = ctx.el('section', { 'class': 'engine-section catalog-content' });
      sec.appendChild(ctx.el('div', { 'class': 'cc-cards' }, [ctx.el('div', { 'class': 'cc-muted', text: 'Loading tools…' })]));
      sec.appendChild(ctx.el('div', { 'class': 'cc-h', text: 'Data' }));
      sec.appendChild(ctx.el('div', { 'class': 'statgrid' }));
      return sec;
    },
    init: function (node, block, ctx) {
      var E = ctx.el, cards = node.querySelector('.cc-cards'), stats = node.querySelector('.statgrid');
      var lf = block.listField || 'tools';
      fetch(block.endpoint || '/api/tools').then(function (r) { return r.json(); }).then(function (r) {
        var tools = r[lf] || r.tools || (Array.isArray(r) ? r : []);
        cards.innerHTML = '';
        if (!tools.length) { cards.innerHTML = '<div class="cc-muted">tools registry unavailable</div>'; return; }
        tools.forEach(function (t) {
          var free = !/paid|\$/i.test(t.cost_note || '');
          var h4 = E('h4', {}, [t.name || '']);
          h4.appendChild(E('span', { 'class': 'tag ' + (free ? 'free' : 'paid'), text: free ? 'free / local' : 'paid' }));
          var card = E('div', { 'class': 'explain-card' }, [h4, E('div', { text: t.description || '' })]);
          if (t.cost_note) card.appendChild(E('div', { 'class': 'muted-note', style: 'margin-top:.3em', text: t.cost_note }));
          cards.appendChild(card);
        });
      }).catch(function () { cards.innerHTML = '<div class="cc-muted">tools registry unavailable</div>'; });
      // running cost ledger (the summary is keyed by model → {usd,...}) + static corpus counts
      fetch(block.costEndpoint || '/api/cost').then(function (r) { return r.json(); }).then(function (cost) {
        var summary = cost.summary || cost || {};
        var total = Object.keys(summary).reduce(function (a, k) { return a + ((summary[k] && summary[k].usd) || 0); }, 0);
        stats.innerHTML = '';
        stats.appendChild(E('div', { 'class': 'stat' }, [E('div', { 'class': 'n', text: '$' + total.toFixed(2) }), E('div', { 'class': 'l', text: block.costLabel || 'session API cost' })]));
        (block.stats || []).forEach(function (s) {
          stats.appendChild(E('div', { 'class': 'stat' }, [E('div', { 'class': 'n', text: (s && s[0]) || '' }), E('div', { 'class': 'l', text: (s && s[1]) || '' })]));
        });
      }).catch(function () {
        stats.innerHTML = '';
        (block.stats || []).forEach(function (s) {
          stats.appendChild(E('div', { 'class': 'stat' }, [E('div', { 'class': 'n', text: (s && s[0]) || '' }), E('div', { 'class': 'l', text: (s && s[1]) || '' })]));
        });
      });
    }
  };

  // ---- header health indicator: poll the backend and paint the dot up/down ----
  // GENERAL: the markup is a conventional .header-health element in the shared header; the endpoint
  // and interval are read from window.CATALOG_HEALTH (a page can override) and default to /api/health
  // every 20s. Mirrors the OLD reading-room app's #health-dot / checkHealth() polling.
  (function headerHealth(tries) {
    var box = document.querySelector('.header-health');
    if (!box) { if (tries > 0) setTimeout(function () { headerHealth(tries - 1); }, 150); return; }
    if (box.dataset.wired) return;
    box.dataset.wired = '1';
    var cfg = window.CATALOG_HEALTH || {};
    var endpoint = cfg.endpoint || '/api/health', interval = cfg.interval || 20000;
    var dot = box.querySelector('.dot'), txt = box.querySelector('.ht');
    box.hidden = false;
    function paint(up, degraded) {
      if (dot) { dot.classList.toggle('up', !!up && !degraded); dot.classList.toggle('down', !up); }
      if (txt) txt.textContent = !up ? 'backend offline' : (degraded ? 'backend degraded' : 'backend connected');
    }
    function ping() {
      fetch(endpoint, { cache: 'no-store' })
        .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
        .then(function (h) { paint(true, !(h && h.ok)); })
        .catch(function () { paint(false, false); });
    }
    ping();
    setInterval(ping, interval);
  })(40);
})();
