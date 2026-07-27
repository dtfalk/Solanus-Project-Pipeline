#!/usr/bin/env node
// =============================================================================
// validate-content.js — static sanity checks for the site's content JSON.
//
// Run:  node scripts/validate-content.js
//
// What it checks (no dependencies, no network, no emulator needed):
//   • every app/data/*.json parses as valid JSON
//   • page files (anything with a `sections` array) are well-formed: sections is
//     an array, each block is an object with a non-empty string `type`, ids are
//     unique within the file, and nested group children follow the same rules
//   • every block `type` used is one the catalogue registers (parsed out of
//     blocks.js + site-blocks.js), so a typo'd type can't silently fall back
//   • global.json's chrome shape (nav/social/questionnaire.fields are arrays)
//
// It does NOT check media existence: media is served from blob storage, not the
// repo. Exit code is non-zero if any ERROR (not just warnings) is found, so it
// can gate a deploy.
// =============================================================================
'use strict';
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const DATA = path.join(ROOT, 'app', 'data');
const JS = path.join(ROOT, 'app', 'javascript');

let errors = 0, warnings = 0;
const err = (m) => { errors++; console.error('  ERROR  ' + m); };
const warn = (m) => { warnings++; console.warn('  warn   ' + m); };

// ---- Discover the catalogue's registered block types from the source ---------
// blocks.js uses `var BLOCKS = { text: {…}, 'stat-cards': {…} }` and parts also
// register via `BLOCKS['epk-photos'] = {…}` (site-blocks.js). We scrape both so
// the validator knows the legal type set without running a browser.
function knownTypes() {
  const types = new Set();
  for (const file of ['blocks.js', 'site-blocks.js']) {
    const p = path.join(JS, file);
    if (!fs.existsSync(p)) continue;
    const src = fs.readFileSync(p, 'utf8');
    // BLOCKS['name'] = …
    for (const m of src.matchAll(/BLOCKS\[['"]([a-z0-9-]+)['"]\]\s*=/gi)) types.add(m[1]);
    // entries inside the `var BLOCKS = { … }` literal: `name:` or `'name':`
    const lit = src.match(/var BLOCKS\s*=\s*\{([\s\S]*?)\n\s*\};/);
    if (lit) {
      for (const m of lit[1].matchAll(/(?:^|\n)\s*['"]?([a-zA-Z0-9_-]+)['"]?\s*:\s*\{/g)) types.add(m[1]);
    }
  }
  // Drop part-definition meta keys that the literal scrape can mistake for types.
  ['lists', 'fields', 'blank', 'render', 'init', 'label', 'category', 'icon', 'hidden'].forEach((k) => types.delete(k));
  return types;
}

const TYPES = knownTypes();
if (!TYPES.size) warn('could not parse any block types from blocks.js — type checks skipped');

// ---- Validate one block (recursively for groups) ----------------------------
function checkBlock(b, where, seenIds) {
  if (!b || typeof b !== 'object') { err(`${where}: block is not an object`); return; }
  if (typeof b.type !== 'string' || !b.type) { err(`${where}: block has no string "type"`); return; }
  if (TYPES.size && !TYPES.has(b.type)) err(`${where}: unknown block type "${b.type}" (not in the catalogue)`);
  if (b.id != null) {
    if (seenIds.has(b.id)) warn(`${where}: duplicate block id "${b.id}"`);
    seenIds.add(b.id);
  } else {
    warn(`${where}: block of type "${b.type}" has no id (the engine will assign one)`);
  }
  if (b.type === 'group' && b.children != null) {
    if (!Array.isArray(b.children)) err(`${where}: group.children is not an array`);
    else b.children.forEach((c, i) => checkBlock(c, `${where} > children[${i}]`, seenIds));
  }
}

// ---- Walk the data folder ---------------------------------------------------
if (!fs.existsSync(DATA)) { console.error('No app/data folder at ' + DATA); process.exit(2); }
const files = fs.readdirSync(DATA).filter((f) => f.endsWith('.json')).sort();
console.log(`Validating ${files.length} content file(s) in app/data …\n`);

for (const f of files) {
  const full = path.join(DATA, f);
  let data;
  try { data = JSON.parse(fs.readFileSync(full, 'utf8')); }
  catch (e) { err(`${f}: invalid JSON — ${e.message}`); continue; }

  // global.json: chrome shape.
  if (f === 'global.json') {
    if (data.nav != null && !Array.isArray(data.nav)) err('global.json: nav is not an array');
    if (data.social != null && !Array.isArray(data.social)) err('global.json: social is not an array');
    if (data.questionnaire && data.questionnaire.fields != null && !Array.isArray(data.questionnaire.fields))
      err('global.json: questionnaire.fields is not an array');
    console.log(`  ok     ${f}  (chrome: ${(data.nav || []).length} nav, ${(data.social || []).length} social)`);
    continue;
  }

  // page files: a `sections` array.
  if (Array.isArray(data.sections)) {
    const seen = new Set();
    data.sections.forEach((b, i) => checkBlock(b, `${f} > sections[${i}]`, seen));
    const used = {};
    data.sections.forEach((b) => { if (b && b.type) used[b.type] = (used[b.type] || 0) + 1; });
    const summary = Object.keys(used).map((t) => `${t}×${used[t]}`).join(', ') || '(empty)';
    console.log(`  ok     ${f}  (${data.sections.length} sections: ${summary})`);
  } else {
    console.log(`  ok     ${f}  (parsed; no sections[] — not a page file)`);
  }
}

console.log(`\nKnown catalogue types (${TYPES.size}): ${[...TYPES].sort().join(', ')}`);
console.log(`\n${errors} error(s), ${warnings} warning(s).`);
process.exit(errors ? 1 : 0);
