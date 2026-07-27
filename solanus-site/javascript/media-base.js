// media-base.js — Dev-only media URL rewriter + content-base fetch redirector.
//
// Two responsibilities, both driven by <meta> tags in <head>:
//
//   <meta name="media-base"   content="https://...storage.../website-media-...">
//   <meta name="content-base" content="https://...storage.../website-content-dev">
//
// (1) When media-base is set, every reference to "/media-content/..." is
//     rewritten on the fly to "<media-base>/..." so the same HTML/CSS/JSON
//     can be served from local files (legacy) or from Azure Blob (current)
//     without per-file edits.
//
// (2) When content-base is set, every fetch() to "/data/X.json" is rewritten
//     to "<content-base>/X.json". This is what lets the SWA frontend pull its
//     editable JSON straight from blob storage (written by the editor) instead
//     of from static files committed in the repo.
//
// Coverage:
//   - fetch() URLs for /data/*.json (URL rewrite to content-base)
//   - fetch() responses for /data/*.json AND content-base/*.json
//     (string-replace /media-content/ → media-base in the response body
//      before .json() parses it)
//   - DOM attributes: img[src], source[src|srcset], video[poster|src],
//     link[rel=icon][href], a[href*="/media-content/"], elements with
//     data-back / data-full / data-download / style*="media-content"
//   - Same-origin CSS rules with background-image referencing /media-content/
//   - Future DOM mutations (header/footer fragments, dynamically rendered cards)
//
// MUST be loaded synchronously in <head> BEFORE any other script that fetches
// /data/*.json or renders media.
(function () {
  'use strict';

  var mediaMeta = document.querySelector('meta[name="media-base"]');
  var mediaBase = mediaMeta && mediaMeta.getAttribute('content') ? mediaMeta.getAttribute('content').trim() : '';
  mediaBase = mediaBase.replace(/\/+$/, '');
  var mediaMetaProd = document.querySelector('meta[name="media-base-prod"]');
  var mediaBaseProd = mediaMetaProd && mediaMetaProd.getAttribute('content') ? mediaMetaProd.getAttribute('content').trim().replace(/\/+$/, '') : '';

  var contentMeta = document.querySelector('meta[name="content-base"]');
  var contentMetaProd = document.querySelector('meta[name="content-base-prod"]');
  var contentBaseDev = contentMeta && contentMeta.getAttribute('content') ? contentMeta.getAttribute('content').trim() : '';
  var contentBaseProd = contentMetaProd && contentMetaProd.getAttribute('content') ? contentMetaProd.getAttribute('content').trim() : '';
  contentBaseDev = contentBaseDev.replace(/\/+$/, '');
  contentBaseProd = contentBaseProd.replace(/\/+$/, '');
  // Hostname-based selection so a single set of HTML files works for both
  // dev and prod SWAs. A configured prod host (SITE.prodHosts) → prod container;
  // everything else (SWA preview hostnames + localhost) → dev container.
  var host = (typeof location !== 'undefined' && location.hostname) ? location.hostname.toLowerCase() : '';
  var isProdHost = !!(window.SITE && window.SITE.isProd(host));
  var contentBase = isProdHost && contentBaseProd ? contentBaseProd : contentBaseDev;
  // Live host → published media container; any other host → draft media.
  if (isProdHost && mediaBaseProd) mediaBase = mediaBaseProd;

  // --- Local prototype override: when served from localhost (SWA CLI dev),
  // read content + media straight from the local Azurite blob emulator so the
  // editor round-trips without any cloud resources. ---
  // STATIC no-cloud deploy: serve /data + /media-content from this folder on EVERY host (incl. localhost),
  // so 127.0.0.1 — a SECURE context where the mic/getUserMedia work — renders without Azurite. (No override.)

  // No media base configured (or it points at the legacy local path) ⇒ no-op
  // for media. We still proceed to set up content-base fetch redirection if
  // that meta is present.
  var mediaActive = !!mediaBase && mediaBase !== '/media-content';

  if (!mediaActive) {
    window.MEDIA_BASE = '/media-content';
    window.mediaUrl = function (p) { return p; };
  } else {
    window.MEDIA_BASE = mediaBase;
  }
  window.CONTENT_BASE = contentBase || null;

  // Data-drive the shared header/footer from global.json on every host
  // (global.js falls back to the static HTML if it can't load).
  (function () {
    var __gs = document.createElement('script');
    __gs.src = '/javascript/global.js';
    __gs.defer = true;
    (document.head || document.documentElement).appendChild(__gs);
  })();

  // Site-wide image lightbox (all hosts).
  (function () {
    var __lb = document.createElement('script');
    __lb.src = '/javascript/lightbox.js';
    __lb.defer = true;
    (document.head || document.documentElement).appendChild(__lb);
  })();

  // Per-object styling (EngineStyle) — before the zoo, which applies a block's
  // style on render.
  (function () {
    var __st = document.createElement('script');
    __st.src = '/javascript/style.js';
    __st.async = false;
    (document.head || document.documentElement).appendChild(__st);
  })();

  // The "zoo": registry of section/part types (blocks.js). Loaded before the
  // renderer; async=false keeps the two ordered so the registry is ready first.
  (function () {
    var __bk = document.createElement('script');
    __bk.src = '/javascript/blocks.js';
    __bk.async = false;
    (document.head || document.documentElement).appendChild(__bk);
  })();

  // Site-specific catalogue pieces (reproduce the hand-built site page parts). After
  // blocks.js (extends its registry), before sections.js (which renders).
  (function () {
    var __bm = document.createElement('script');
    __bm.src = '/javascript/site-blocks.js';
    __bm.async = false;
    (document.head || document.documentElement).appendChild(__bm);
  })();

  // Generic custom sections renderer (all hosts; no-ops if a page has none).
  (function () {
    var __sx = document.createElement('script');
    __sx.src = '/javascript/sections.js';
    __sx.async = false;       // run after blocks.js (ordered execution)
    __sx.defer = true;
    (document.head || document.documentElement).appendChild(__sx);
  })();

  // Site-wide theme variables (EngineTheme) — event-driven, order-independent.
  (function () {
    var __th = document.createElement('script');
    __th.src = '/javascript/theme.js';
    __th.defer = true;
    (document.head || document.documentElement).appendChild(__th);
  })();

  // Load the inline editor runtime on dev/local hosts only (never on prod).
  // edit-mode.js self-gates further: it no-ops unless /.auth/me has the editor role.
  if (!isProdHost) {
    var __es = document.createElement('script');
    __es.src = '/javascript/edit-mode.js';
    __es.defer = true;
    (document.head || document.documentElement).appendChild(__es);
  }

  // Match "/media-content/<rest>" but NOT a fully-qualified URL whose path
  // happens to contain "/media-content/" inside it.
  var PATH_RE = /\/media-content\//g;

  function rewriteString(s) {
    if (!mediaActive) return s;
    if (typeof s !== 'string' || s.indexOf('/media-content/') === -1) return s;
    return s.replace(PATH_RE, mediaBase + '/');
  }

  if (mediaActive) {
    window.mediaUrl = function (p) {
      if (!p) return p;
      if (p.charAt(0) === '/' && p.indexOf('/media-content/') === 0) {
        return mediaBase + p.slice('/media-content'.length);
      }
      return rewriteString(p);
    };
  }

  // Detect fetches that should be treated as content JSON (so we apply the
  // media-string rewrite to the response body too). Match either path-based
  // /data/X.json or full content-base URL ending in .json.
  function isContentJsonUrl(url) {
    if (!url) return false;
    if (/(^|\/)data\/[^/]+\.json(\?|$)/.test(url)) return true;
    if (contentBase && url.indexOf(contentBase) === 0 && /\.json(\?|$)/.test(url)) return true;
    return false;
  }

  // ---- 1. Patch fetch:
  //   (a) redirect /data/*.json → contentBase/*.json (when contentBase set)
  //   (b) rewrite /media-content/ paths inside JSON response bodies
  var origFetch = window.fetch ? window.fetch.bind(window) : null;
  if (origFetch) {
    window.fetch = function (input, init) {
      var url = typeof input === 'string' ? input : (input && input.url) || '';

      // (a) URL redirect for content JSON.
      if (contentBase) {
        var m = url.match(/^(?:.*?)\/data\/([^/]+\.json)(\?.*)?$/);
        if (m) {
          var newUrl = contentBase + '/' + m[1] + (m[2] || '');
          if (typeof input === 'string') input = newUrl;
          else input = new Request(newUrl, input);
          url = newUrl;
          // Dev/preview: always read the freshest content so editor saves show
          // on reload (not a stale ~60s-cached copy). Prod keeps normal caching.
          if (!isProdHost) init = Object.assign({}, init, { cache: 'no-store' });
        }
      }

      var shouldRewriteBody = mediaActive && isContentJsonUrl(url);
      var p = origFetch(input, init);
      if (!shouldRewriteBody) return p;
      return p.then(function (res) {
        if (!res || !res.ok) return res;
        return res.clone().text().then(function (txt) {
          var rewritten = rewriteString(txt);
          var headers = new Headers(res.headers);
          return new Response(rewritten, {
            status: res.status,
            statusText: res.statusText,
            headers: headers
          });
        });
      });
    };
  }

  // From here on, the DOM/CSS rewriting only matters when media-base is active.
  if (!mediaActive) return;

  // ---- 2. DOM attribute rewriter.
  var ATTR_TARGETS = [
    { sel: 'img[src*="/media-content/"]',                 attr: 'src' },
    { sel: 'source[src*="/media-content/"]',              attr: 'src' },
    { sel: 'source[srcset*="/media-content/"]',           attr: 'srcset' },
    { sel: 'video[poster*="/media-content/"]',            attr: 'poster' },
    { sel: 'video[src*="/media-content/"]',               attr: 'src' },
    { sel: 'link[href*="/media-content/"]',               attr: 'href' },
    { sel: 'a[href^="/media-content/"]',                  attr: 'href' },
    { sel: '[data-back*="/media-content/"]',              attr: 'data-back' },
    { sel: '[data-full*="/media-content/"]',              attr: 'data-full' },
    { sel: '[data-download*="/media-content/"]',          attr: 'data-download' }
  ];

  function rewriteAttrsIn(root) {
    if (!root || !root.querySelectorAll) return;
    for (var i = 0; i < ATTR_TARGETS.length; i++) {
      var t = ATTR_TARGETS[i];
      var nodes = root.querySelectorAll(t.sel);
      for (var j = 0; j < nodes.length; j++) {
        var n = nodes[j];
        var v = n.getAttribute(t.attr);
        var nv = rewriteString(v);
        if (nv !== v) n.setAttribute(t.attr, nv);
      }
    }
    // Inline style="background:url(/media-content/...)"
    var styled = root.querySelectorAll('[style*="/media-content/"]');
    for (var k = 0; k < styled.length; k++) {
      var s = styled[k];
      var sv = s.getAttribute('style');
      var nsv = rewriteString(sv);
      if (nsv !== sv) s.setAttribute('style', nsv);
    }
    // Also rewrite the root element itself if it matches.
    if (root.nodeType === 1) {
      for (var m = 0; m < ATTR_TARGETS.length; m++) {
        var tt = ATTR_TARGETS[m];
        if (root.matches && root.matches(tt.sel)) {
          var rv = root.getAttribute(tt.attr);
          var rnv = rewriteString(rv);
          if (rnv !== rv) root.setAttribute(tt.attr, rnv);
        }
      }
    }
  }

  // ---- 3. CSS rule rewriter (background-image: url(/media-content/...)).
  function rewriteStylesheets() {
    var sheets = document.styleSheets;
    for (var i = 0; i < sheets.length; i++) {
      var sheet = sheets[i];
      var rules;
      try { rules = sheet.cssRules || sheet.rules; }
      catch (e) { continue; } // cross-origin sheet, skip
      if (!rules) continue;
      for (var j = 0; j < rules.length; j++) {
        var r = rules[j];
        rewriteCssRule(r);
      }
    }
  }

  function rewriteCssRule(r) {
    if (!r) return;
    // Recurse into @media / @supports.
    if (r.cssRules) {
      for (var i = 0; i < r.cssRules.length; i++) rewriteCssRule(r.cssRules[i]);
    }
    if (!r.style) return;
    var props = ['background', 'backgroundImage', 'background-image', 'borderImage', 'border-image', 'listStyleImage', 'list-style-image', 'maskImage', 'mask-image'];
    for (var p = 0; p < props.length; p++) {
      var prop = props[p];
      var val;
      try { val = r.style.getPropertyValue ? r.style.getPropertyValue(prop) : r.style[prop]; }
      catch (e) { continue; }
      if (val && val.indexOf('/media-content/') !== -1) {
        var nv = rewriteString(val);
        try { r.style.setProperty(prop, nv, r.style.getPropertyPriority ? r.style.getPropertyPriority(prop) : ''); }
        catch (e2) { try { r.style[prop] = nv; } catch (e3) {} }
      }
    }
  }

  // ---- 4. Run on DOM ready, then watch for mutations.
  function onReady() {
    rewriteAttrsIn(document);
    rewriteStylesheets();

    var mo = new MutationObserver(function (mutations) {
      for (var i = 0; i < mutations.length; i++) {
        var added = mutations[i].addedNodes;
        for (var j = 0; j < added.length; j++) {
          var node = added[j];
          if (node.nodeType === 1) rewriteAttrsIn(node);
        }
        // Attribute mutations on existing nodes (e.g. JS swapping img.src).
        if (mutations[i].type === 'attributes' && mutations[i].target) {
          var tgt = mutations[i].target;
          if (tgt.nodeType === 1) rewriteAttrsIn(tgt);
        }
      }
    });
    mo.observe(document.documentElement, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['src', 'srcset', 'href', 'poster', 'style', 'data-back', 'data-full', 'data-download']
    });

    // Stylesheets may finish loading after DOMContentLoaded — re-scan on full load.
    window.addEventListener('load', rewriteStylesheets, { once: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady, { once: true });
  } else {
    onReady();
  }
})();
