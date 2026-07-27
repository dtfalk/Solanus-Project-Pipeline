// =============================================================================
// theme.js — the site's "look" expressed as CSS variables.
//
// Pagesmith keeps colour and brand OUT of the code. Instead, the engine and the
// catalogue parts reference CSS custom properties like var(--site-accent), and
// this file is the ONE place that sets those properties from data. It reads a
// `theme` object — from global.json (the site-wide default) and/or from a single
// page's own JSON (a per-page override) — and writes the matching --site-* /
// --cat-* variables onto <html>. Change the data, the whole site re-colours; no
// code changes, no per-component edits.
//
// It is event-driven (it listens for `engine-data-ready`), so it does not care
// what order the scripts load in. It runs on every host, draft and live alike.
// =============================================================================
(function () {
  'use strict';

  // Map a friendly theme key (what you write in JSON) to the CSS variable the
  // stylesheets actually read. These are generic ROLES — accent/ink/bg — never
  // brand-specific colour names, so any site can fill them in.
  //   accent → the highlight colour (links, buttons, chart bars)
  //   text   → the main foreground/ink colour
  //   bg     → the page background colour
  var MAP = { accent: '--site-accent', text: '--site-ink', bg: '--site-bg' };

  function apply(theme) {
    if (!theme || typeof theme !== 'object') return;
    var r = document.documentElement.style;

    // Set each role that's present; CLEAR (remove) the ones that aren't. The
    // "clear" half matters: when the editor deletes a colour, the user expects
    // the page to fall back to the stylesheet default immediately — which only
    // happens if we actually remove the inline custom property, not just skip it.
    Object.keys(MAP).forEach(function (k) {
      if (theme[k]) r.setProperty(MAP[k], theme[k]);
      else r.removeProperty(MAP[k]);
    });

    // Optional data-viz palette: theme.palette = ['#..', ...] → --cat-1..N, used
    // by the chart / stat-cards / ranking / spotlight parts. Absent → those
    // parts fall back to their built-in default palette, so the catalogue stays
    // generic. (The editor's palette "Auto" buttons clear --cat-* themselves.)
    if (Array.isArray(theme.palette)) theme.palette.forEach(function (c, i) { if (c) r.setProperty('--cat-' + (i + 1), c); });

    // The spotlight gradient (and its glow). Set when present, clear when not —
    // same "clear reverts live" reasoning as the primary colours above.
    if (theme.spotlight) r.setProperty('--cat-spot', theme.spotlight); else r.removeProperty('--cat-spot');
    if (theme.spotlightGlow) r.setProperty('--cat-spot-glow', theme.spotlightGlow); else r.removeProperty('--cat-spot-glow');

    // ----- optional full-bleed page background image -----
    // Turns "photos/x.jpg" into a CSS background value, optionally layering a
    // dark scrim over it so text stays readable. The page shells read
    // --site-bg-image / --site-bg-image-mobile (see index.html / page.html).
    function bgLayers(img) {
      // A bare path like "photos/x.jpg" is assumed to live under /media-content/
      // so mediaUrl() can resolve it to the real blob/Azurite URL (mediaUrl only
      // rewrites paths that start with /media-content/). Absolute URLs pass through.
      var path = /^(https?:|\/)/.test(img) ? img : ('/media-content/' + img);
      var url = (typeof window !== 'undefined' && window.mediaUrl) ? window.mediaUrl(path) : path;
      var layers = []; if (theme.bgScrim) layers.push(theme.bgScrim); layers.push('url("' + url + '")');
      return layers.join(', ');
    }
    if (theme.bgImage) {
      r.setProperty('--site-bg-image', bgLayers(theme.bgImage));
      // With a photo behind it, content sits on a dark readable panel
      // (--site-panel) while the photo frames the page in the margins.
      r.setProperty('--site-panel', theme.panel || 'rgba(11,13,19,0.9)');
    } else {
      // No background image → clear both, so a page without one reverts cleanly.
      r.removeProperty('--site-bg-image');
      r.removeProperty('--site-panel');
    }
    if (theme.bgImageMobile) r.setProperty('--site-bg-image-mobile', bgLayers(theme.bgImageMobile));
    else r.removeProperty('--site-bg-image-mobile');
  }

  // Apply whenever a page or the global config announces its data. global.json's
  // theme arrives first (site default); a page's own theme arrives after and
  // overrides it. Both come through the same event.
  document.addEventListener('engine-data-ready', function (e) {
    if (e && e.detail && e.detail.data && e.detail.data.theme) apply(e.detail.data.theme);
  });
  // Fallback for the race where data was announced before this script loaded.
  if (window.__ENGINE_DATA__ && window.__ENGINE_DATA__.data && window.__ENGINE_DATA__.data.theme) apply(window.__ENGINE_DATA__.data.theme);

  // The editor calls EngineTheme.apply() to preview theme edits live.
  window.EngineTheme = { apply: apply };
})();
