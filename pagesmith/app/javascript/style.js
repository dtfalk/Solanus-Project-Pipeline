// =============================================================================
// style.js — per-object ("inline") styling.
//
// theme.js handles the SITE-wide look. This handles the ONE-block look: any
// block can carry a `style` object (and any single text node a `textStyles`
// entry), and this file turns that data into inline CSS on the rendered node.
// Because it writes real inline styles, the look survives onto the published
// page too — it is not an editor-only preview.
//
// It also publishes the SCHEMA the editor's inspector uses to draw its styling
// controls (window.EngineStyle.fields). That is the trick that keeps the editor
// generic: the editor doesn't know what "padding" means, it just renders the
// fields this file declares and writes the values back into the block's `style`.
//
// Loaded before blocks.js, because the catalogue calls EngineStyle.apply() as it
// renders each block.
// =============================================================================
(function () {
  'use strict';

  // Friendly preset name → real CSS value. Presets (rather than free-form CSS)
  // keep the editor's controls simple and the output consistent across a site.
  var PAD = { sm: '.5rem', md: '1rem', lg: '2rem' };
  var RAD = { sm: '4px', md: '8px', lg: '16px', pill: '999px' };
  var MAXW = { narrow: '640px', normal: '900px', wide: '1100px', full: '100%' };
  var FONT = { sm: '.85rem', md: '1rem', lg: '1.25rem', xl: '1.6rem' };
  var GAP = { sm: '.5rem', md: '1rem', lg: '2rem' };
  var BORDER = { thin: '1px', med: '2px', thick: '4px' };

  // A whole block's style. Every property is set to '' when its key is absent,
  // which REMOVES it — so clearing a control in the editor reverts to the CSS
  // default rather than leaving a stale value behind.
  function apply(node, style) {
    if (!node || !node.style) return;
    style = style || {};
    var s = node.style;
    s.color = style.color || '';
    s.backgroundColor = style.bg || '';
    s.textAlign = style.align || '';
    s.padding = PAD[style.pad] || '';
    s.borderRadius = RAD[style.radius] || '';
    // Text size accepts either a raw number (interpreted as px) or a legacy
    // preset name (sm/md/lg/xl). The regex matches a plain integer or decimal
    // ("18", "1.5") — NOT junk like "1.2.3" — so a bad value falls through to
    // the preset lookup (and then to '') instead of producing invalid CSS.
    var fz = style.font;
    s.fontSize = (fz == null || fz === '') ? '' : (/^\d+(\.\d+)?$/.test(String(fz)) ? (fz + 'px') : (FONT[fz] || ''));
    s.border = (style.border && BORDER[style.border]) ? (BORDER[style.border] + ' solid ' + (style.borderColor || 'currentColor')) : '';
    s.gap = GAP[style.gap] || '';
    if (style.bgImage) {
      var url = (typeof window !== 'undefined' && window.mediaUrl) ? window.mediaUrl(style.bgImage) : style.bgImage;
      s.backgroundImage = 'url("' + url + '")'; s.backgroundSize = 'cover'; s.backgroundPosition = 'center';
    } else { s.backgroundImage = ''; s.backgroundSize = ''; s.backgroundPosition = ''; }
    if (MAXW[style.maxw]) { s.maxWidth = MAXW[style.maxw]; s.marginLeft = 'auto'; s.marginRight = 'auto'; }
    else { s.maxWidth = ''; }
  }

  // Per-TEXT-element styling: style ONE heading/paragraph/label at a time,
  // independent of the block-level apply() above. A block carries a `textStyles`
  // map keyed by each editable text node's data-path SUFFIX (e.g. ".title" or
  // ".items[0].label"); this writes those as inline CSS so the look persists on
  // the published site too.
  function applyText(el, ts) {
    if (!el || !el.style) return;
    ts = ts || {};
    var fz = ts.font;
    el.style.fontSize = (fz == null || fz === '') ? '' : (/^\d+(\.\d+)?$/.test(String(fz)) ? (fz + 'px') : (FONT[fz] || String(fz)));
    el.style.color = ts.color || '';
    el.style.fontWeight = ts.bold ? '700' : '';
    el.style.fontStyle = ts.italic ? 'italic' : '';
    el.style.textAlign = ts.align || '';
  }

  // Walk a freshly-rendered block and apply each entry in its textStyles map to
  // the matching text node. The path-suffix keying is what lets one block style
  // several of its own text nodes independently.
  function applyTextStyles(root, block, blockPath) {
    if (!root || !root.querySelectorAll || !block || !block.textStyles) return;
    var map = block.textStyles;
    var nodes = root.querySelectorAll('[data-edit-text],[data-edit-html]');
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      // Skip text that belongs to a NESTED block — that child styles its own
      // text when IT renders, so we must not double-apply here.
      if (n.closest('[data-engine-path]') !== root) continue;
      var p = n.getAttribute('data-edit-text') || n.getAttribute('data-edit-html') || '';
      if (blockPath && p.indexOf(blockPath) === 0) {
        var ts = map[p.slice(blockPath.length)];
        if (ts) applyText(n, ts);
      }
    }
  }

  // Public API. `fields` is the inspector schema the editor reads to draw the
  // per-block Style controls — add a field here and it appears in the editor,
  // no editor code change. Option arrays are [value, label] so the labels stay
  // friendly while the stored values stay terse.
  window.EngineStyle = {
    apply: apply,
    applyText: applyText,
    applyTextStyles: applyTextStyles,
    fields: [
      { key: 'color', type: 'color', label: 'Text color' },
      { key: 'bg', type: 'color', label: 'Background color' },
      { key: 'bgImage', type: 'image', label: 'Background image' },
      { key: 'font', type: 'number', label: 'Text size (px)', placeholder: 'e.g. 18 — blank for default' },
      { key: 'align', type: 'select', label: 'Align', options: [['', 'Default'], ['left', 'Left'], ['center', 'Center'], ['right', 'Right']] },
      { key: 'gap', type: 'select', label: 'Grid spacing', options: [['', 'Default'], ['sm', 'Tight'], ['md', 'Medium'], ['lg', 'Wide']] },
      { key: 'pad', type: 'select', label: 'Padding', options: [['', 'Default'], ['sm', 'Small'], ['md', 'Medium'], ['lg', 'Large']] },
      { key: 'radius', type: 'select', label: 'Corners', options: [['', 'Default'], ['sm', 'Subtle'], ['md', 'Rounded'], ['lg', 'Large'], ['pill', 'Pill']] },
      { key: 'border', type: 'select', label: 'Border', options: [['', 'None'], ['thin', 'Thin'], ['med', 'Medium'], ['thick', 'Thick']] },
      { key: 'borderColor', type: 'color', label: 'Border color' },
      { key: 'maxw', type: 'select', label: 'Width', options: [['', 'Default'], ['narrow', 'Narrow'], ['normal', 'Normal'], ['wide', 'Wide'], ['full', 'Full']] }
    ]
  };
})();
