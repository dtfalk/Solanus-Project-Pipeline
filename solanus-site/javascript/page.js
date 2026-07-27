// =============================================================================
// page.js — the generic page loader. This is the whole "skeleton" of a page.
//
// A Pagesmith page has NO per-page code. A page is just a content file
// (page-<slug>.json) shaped like { title?, theme?, sections:[...] }. This script
// figures out which content file the current URL wants, fetches it, and
// ANNOUNCES it via the `engine-data-ready` event. Everyone else listens:
//   • sections.js renders the sections[] into <main>,
//   • theme.js applies the page's theme,
//   • edit-mode.js (on a draft host) makes it all editable.
//
// Because this one file drives every page, adding a page is just adding a JSON
// file — which is exactly what keeps the engine small and reusable.
// =============================================================================
(function () {
  'use strict';

  // Work out the content-file key for this page. Three ways, in priority order:
  //   1. An explicit <meta name="engine-page" content="home"> in the shell
  //      (used by pages that have their own HTML file, like the home page).
  //   2. A /p/<slug> URL — the universal route every new page gets for free.
  //   3. A clean top-level slug (e.g. /showcase), or the root (→ "home").
  // Whatever we land on becomes "page-<slug>" — the name of the JSON file.
  function pageKey() {
    var meta = document.querySelector('meta[name="engine-page"]');
    if (meta && meta.getAttribute('content')) return 'page-' + meta.getAttribute('content');

    var path = (location.pathname || '');
    var m = path.match(/\/p\/([^/?#]+)/);
    var slug = m ? m[1] : (path.replace(/^\/+|\/+$/g, '').split('/')[0] || 'home');
    // Sanitise the slug to the same shape the API allows (lowercase, digits,
    // hyphens) so the fetched filename can never traverse out of /data/.
    return 'page-' + decodeURIComponent(slug).toLowerCase().replace(/[^a-z0-9-]/g, '');
  }

  // Tell the rest of the engine the page's data is ready. We always normalise to
  // a valid shape first, set the tab title, stash the data globally (for the
  // load-order race below), and fire the event.
  function announce(file, data) {
    if (!data || typeof data !== 'object') data = { sections: [] };
    if (!Array.isArray(data.sections)) data.sections = [];
    var sn = (window.SITE && window.SITE.siteName) || '';
    if (data.title) document.title = data.title + (sn ? ' — ' + sn : '');
    else if (sn) document.title = sn; // untitled page → just the site name
    window.__ENGINE_DATA__ = { file: file, data: data };
    document.dispatchEvent(new CustomEvent('engine-data-ready', { detail: { file: file, data: data } }));
  }

  document.addEventListener('DOMContentLoaded', function () {
    var file = pageKey();
    // media-base.js has patched fetch() to redirect /data/*.json to the right
    // content container. A brand-new page has no JSON yet → the 404 becomes an
    // empty page, so the editor can start adding sections and the first Save
    // creates the file. (No error screen for a page that simply doesn't exist
    // yet — that's the normal "new page" state.)
    fetch('/data/' + file + '.json')
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; })
      .then(function (data) { announce(file, data); });
  });
})();
