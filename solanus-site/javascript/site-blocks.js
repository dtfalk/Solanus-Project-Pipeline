// =============================================================================
// site-blocks.js — YOUR site-specific catalogue parts (the extension point).
//
// blocks.js ships the GENERIC catalogue (text, heading, gallery, video,
// carousel, embed, links, quotes, button, chart, stat-cards, spotlight,
// ranking, group). This file is where YOU add parts that only make sense for
// THIS site — a bespoke hero, a product grid, a tour-dates list, whatever.
//
// It is loaded automatically by media-base.js right AFTER blocks.js and BEFORE
// sections.js, so by the time you run, `window.CatalogBlocks` (the registry)
// already exists and you just add to it. Keeping site parts here (instead of in
// blocks.js) is what keeps the engine generic and reusable across sites.
//
// The starter ships this file EMPTY on purpose — a clean site has no bespoke
// parts yet. Delete the example below or fill it in. (The file must exist even
// when empty, because media-base.js always loads it.)
// =============================================================================
(function () {
  'use strict';

  // Nothing registered yet. A site-specific part follows exactly the same
  // contract as a generic one (see blocks.js and the dev guide). Uncomment and
  // adapt this template to add your first one:
  //
  // if (!window.CatalogBlocks) return;            // safety: engine not loaded
  // window.CatalogBlocks['hero'] = {
  //   label: 'Hero', category: 'Layout', icon: '★',
  //   // blank() returns the default block when someone adds it from the picker.
  //   blank: function () { return { type: 'hero', title: 'Big headline', subtitle: 'Supporting line' }; },
  //   // fields[] = scalar inputs shown in the editor's inspector.
  //   fields: [
  //     { key: 'title',    type: 'text', label: 'Headline' },
  //     { key: 'subtitle', type: 'text', label: 'Subtitle' }
  //   ],
  //   // render() returns a DOM node. Use ctx.el to build it and emit
  //   // data-edit-* attributes so the text is click-to-edit. Namespace any CSS
  //   // classes under `catalog-`.
  //   render: function (block, ctx) {
  //     var sec = ctx.el('section', { 'class': 'engine-section catalog-hero' });
  //     sec.appendChild(ctx.el('h1', {
  //       text: block.title || '',
  //       'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.title'
  //     }));
  //     sec.appendChild(ctx.el('p', {
  //       text: block.subtitle || '',
  //       'data-edit-file': ctx.file, 'data-edit-text': ctx.path + '.subtitle'
  //     }));
  //     return sec;
  //   }
  // };
})();
