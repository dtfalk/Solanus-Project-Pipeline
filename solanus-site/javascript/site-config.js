// =============================================================================
// site-config.js — THE one file that makes this deployment "your site".
//
// Pagesmith is split into two halves:
//
//   1. The ENGINE  — a generic, brand-neutral page builder: the skeleton
//      (page.js / sections.js / theme.js / style.js), the catalogue of parts
//      (blocks.js), the inline editor (edit-mode.js), the chrome hydrator
//      (global.js) and the API. NONE of it knows your site's name, colours,
//      pages, or social links. It reads them from this file at runtime.
//
//   2. The INSTANCE — everything that makes THIS deployment specific: this
//      config, the content in /data/*.json, the styling in /css, and the page
//      shells in /html-pages. THIS is the half you edit.
//
// So to stand up a brand-new site you copy the project, change the values in
// `SITE_CONFIG` below, and swap the content/CSS. You never touch the engine.
// That is the whole point: the engine stays constant and battle-tested while
// every site is just a different set of values fed into it.
//
// LOAD ORDER MATTERS. This is the FIRST script on every page (before
// media-base.js), so `window.SITE` is always available synchronously by the
// time any engine code runs. If you add a new HTML shell, it MUST include this
// script first — otherwise prod-gating, the wordmark, media tabs, etc. all
// silently fall back to their (harmless) defaults.
// =============================================================================
(function () {
  'use strict';

  // ===========================================================================
  // The manifest. Every field here is site-specific. Edit freely.
  // ===========================================================================
  window.SITE_CONFIG = {

    // The site's name. Used as the browser-tab title suffix on builder pages
    // (e.g. a page titled "About" becomes "About — My Site").
    siteName: 'Solanus Casey Archive',

    // Which hostnames count as the LIVE, published site. This is the single
    // most important switch in the whole system, because three behaviours hang
    // off it:
    //    • On a live host the inline editor never loads (visitors can't edit).
    //    • On a live host analytics/tracking fire (and ONLY there).
    //    • On a live host "dev" and "unlisted" pages are hidden from the menu.
    // Every OTHER host — localhost, a preview/staging URL — is treated as the
    // editable DRAFT, where the editor loads and nothing is tracked.
    //
    // Start empty: with no live host configured, every host is a draft (which
    // is exactly what you want while building). When you go live, add your real
    // domain here, e.g. prodHosts: ['example.com', 'www.example.com'].
    prodHosts: [],

    // Editor chrome: the little wordmark shown in the editor toolbar so you can
    // tell at a glance which site you're editing.
    branding: { wordmark: 'Solanus Casey Archive' },

    // The built-in pages. Each key is a page slug (its content file is
    // page-<slug>.json) and the value gives a friendly `name` shown in the
    // editor's Pages panel, plus OPTIONAL background-image blobs the Background
    // tool can replace (`bg` = large screens, `bgMobile` = the phone variant).
    // A page does NOT need an entry here to exist — any /p/<slug> URL works on
    // its own through the generic page shell. Entries just give the editor a
    // friendlier name and a background-image slot.
    pages: {
      home:     { name: 'Home' },
      showcase: { name: 'Showcase' },
      global:   { name: 'Header & footer' }   // the shared chrome (global.json)
    },

    // The media picker's folder tabs. Each tab scopes uploads/browsing to one
    // folder prefix inside your media storage ('' = the storage root, e.g. for
    // the logo). Add a tab per logical group of assets you want to keep apart.
    mediaTabs: [
      { prefix: '', label: 'Media / logo' }
    ],

    // Social platforms. This maps a platform key (used in global.json's
    // `social[].platform`) to the inline-SVG <symbol> id defined in
    // header.html, a screen-reader label, and an analytics event key. To add a
    // platform: drop its <symbol> into header.html, then add a line here. The
    // starter ships a handful of common icons; delete the ones you don't use.
    socialIcons: {
      instagram:  { id: 'instagram-unauth-icon', label: 'Instagram',   track: 'instagramclick' },
      youtube:    { id: 'youtube-unauth-icon',   label: 'YouTube',     track: 'youtubeclick' },
      facebook:   { id: 'facebook-unauth-icon',  label: 'Facebook',    track: 'facebookclick' },
      tiktok:     { id: 'tiktok-unauth-icon',    label: 'TikTok',      track: 'tiktokclick' },
      spotify:    { id: 'spotify-unauth-icon',   label: 'Spotify',     track: 'spotifyclick' },
      applemusic: { id: 'itunes-icon',           label: 'Apple Music', track: 'applemusicclick' }
    },

    // Draft-only nav shortcuts: extra links added to the menu ONLY on a draft
    // host (never on the live site). Handy for reaching unlisted/work-in-
    // progress pages without typing the URL. Empty by default.
    devLinks: [],

    // Media folders whose uploads keep FULL QUALITY — no WebP conversion and no
    // size cap below the transport ceiling. Most sites want everything
    // compressed (the default), so this stays empty. If you DO add a prefix
    // here (e.g. a press kit that needs original files), you must also set the
    // matching MEDIA_FULL_QUALITY_PREFIXES app setting on the API so the server
    // enforces the same rule. The two MUST stay in sync.
    fullQualityMedia: []
  };

  // ===========================================================================
  // Generic accessor — leave this as-is.
  //
  // The engine never reads window.SITE_CONFIG directly. It goes through this
  // small read-only `window.SITE` object, whose methods all degrade safely when
  // a field is missing (a half-filled config makes the site fall back to
  // defaults, never throw). Treat everything below as part of the engine.
  // ===========================================================================
  var C = window.SITE_CONFIG || {};
  window.SITE = {
    cfg: C,
    siteName: C.siteName || '',
    prodHosts: C.prodHosts || [],
    wordmark: (C.branding && C.branding.wordmark) || '',

    // Is the given host (default: the current one) a LIVE host? This is the
    // gate the whole "draft vs published" behaviour hangs on (see prodHosts).
    isProd: function (h) {
      h = (h != null ? h : (typeof location !== 'undefined' ? location.hostname : '')) || '';
      return this.prodHosts.indexOf(String(h).toLowerCase()) !== -1;
    },

    // Per-page config lookups (all tolerant of a missing/partial `pages` map).
    page: function (k) { return (C.pages || {})[k] || {}; },
    pageName: function (k) { var p = (C.pages || {})[k]; return p && p.name; },
    pageBg: function (k) { var p = (C.pages || {})[k]; return p && p.bg; },
    pageBgMobile: function (k) { var p = (C.pages || {})[k]; return p && p.bgMobile; },

    mediaTabs: C.mediaTabs || [],
    socialIcons: C.socialIcons || {},
    devLinks: C.devLinks || [],

    // Does a media folder prefix keep full quality? (See fullQualityMedia.)
    isFullQuality: function (prefix) {
      var list = C.fullQualityMedia || [];
      for (var i = 0; i < list.length; i++) {
        if (prefix && String(prefix).indexOf(list[i]) === 0) return true;
      }
      return false;
    }
  };
})();
