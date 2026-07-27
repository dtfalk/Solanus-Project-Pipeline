// =============================================================================
// header.js — generic behaviour for the shared header (header.html).
//
// Two jobs, both site-agnostic:
//   1. Highlight the current page's nav link (adds aria-current="page").
//   2. Open/close the mobile menu, and make the bar "solid" once you scroll.
//
// The header MARKUP is injected asynchronously by load-fragments.js, so this
// script can't assume the header exists when it first runs. Instead of relying
// on a one-shot callback (which is what caused the old double-init bug), it
// POLLS for the header element a bounded number of times and wires everything
// up exactly once when it appears. global.js, which rebuilds the nav from
// global.json, calls window.setCurrentNavLink() afterwards to re-highlight.
// =============================================================================
(function () {
  'use strict';

  // Normalize a URL path so "/about/", "/about", "/about/index.html" all compare
  // equal. We strip leading/trailing slashes, lowercase, and drop index.html.
  function normalizePath(path) {
    return String(path || '')
      .toLowerCase()
      .replace(/^\/+/, '')
      .replace(/\/+$/, '')
      .replace(/index\.html$/, '');
  }

  // Mark the nav link whose href matches the current URL. EXACT match (not a
  // substring), so "/showcase" doesn't accidentally light up because some other
  // slug contains "show". Exposed on window because global.js calls it again
  // after it rebuilds the menu from global.json.
  function setCurrentNavLink() {
    var current = normalizePath(window.location.pathname);
    var links = document.querySelectorAll('.main-nav a, .mobile-menu-links a');
    for (var i = 0; i < links.length; i++) {
      var href = links[i].getAttribute('href');
      if (!href || /^https?:/i.test(href)) { links[i].removeAttribute('aria-current'); continue; }
      if (normalizePath(href) === current) links[i].setAttribute('aria-current', 'page');
      else links[i].removeAttribute('aria-current');
    }
  }
  window.setCurrentNavLink = setCurrentNavLink;

  // Add/remove the `.solid` class as the page scrolls. Purely generic: once you
  // scroll past a small threshold the header gets its opaque background. (Any
  // fancier per-page scroll behaviour belongs in a site-specific script, not in
  // the engine's header.)
  function updateHeaderSolid() {
    var header = document.querySelector('.site-header');
    if (!header) return;
    if (window.scrollY > 24) header.classList.add('solid');
    else header.classList.remove('solid');
  }

  // Wire the header's interactivity. Called once, after the fragment exists.
  function initHeader() {
    setCurrentNavLink();
    updateHeaderSolid();
    window.addEventListener('scroll', updateHeaderSolid, { passive: true });

    var toggle = document.querySelector('.menu-toggle');
    var closeBtn = document.querySelector('.close-menu');
    var mobileMenu = document.getElementById('mobileMenu');
    if (!mobileMenu) return; // a header without a mobile menu is still fine

    function open() { mobileMenu.classList.add('open'); document.body.classList.add('modal-open'); }
    function close() { mobileMenu.classList.remove('open'); document.body.classList.remove('modal-open'); }

    if (toggle) toggle.addEventListener('click', open);
    if (closeBtn) closeBtn.addEventListener('click', close);
    // Close on backdrop tap or when a link is tapped (delegated, so it keeps
    // working even after global.js re-renders the links).
    mobileMenu.addEventListener('click', function (e) {
      if (e.target === mobileMenu || (e.target.closest && e.target.closest('a'))) close();
    });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') close(); });
  }

  // The fragment is injected async, so poll for it — but with a CAP, so we never
  // spin forever on a page that has no header (e.g. the standalone login page).
  function waitForHeader() {
    var tries = 0;
    var timer = setInterval(function () {
      if (document.querySelector('.site-header')) { clearInterval(timer); initHeader(); }
      else if (++tries > 80) { clearInterval(timer); } // ~4s, then give up
    }, 50);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', waitForHeader);
  else waitForHeader();
})();
