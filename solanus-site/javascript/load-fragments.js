// =============================================================================
// load-fragments.js — inject the shared header/footer HTML into every page.
//
// The header and footer live in their own files (html-pages/header.html and
// footer.html) so every page can share one copy. This script fetches them and
// drops them into the #header-placeholder / #footer-placeholder divs.
//
// It does ONE thing: load fragments. It deliberately does NOT initialise the
// header or footer behaviour — header.js and footer.js own their own setup and
// each waits (by polling) for its markup to appear. Keeping init out of here is
// what prevents the same handlers being wired twice.
// =============================================================================

// Fetch an HTML fragment and inject it into the element with id `id`.
//   id:       target container id (e.g. "header-placeholder")
//   filePath: URL of the fragment (e.g. "/html-pages/header.html")
//   callback: optional function to run once the fragment is in the DOM
async function loadFragment(id, filePath, callback) {
  const container = document.getElementById(id);
  if (!container) return; // nothing to fill on this page — that's fine

  try {
    const res = await fetch(filePath);
    if (!res.ok) throw new Error(`Failed to load ${filePath} (HTTP ${res.status})`);
    container.innerHTML = await res.text();
    if (typeof callback === 'function') callback();
  } catch (err) {
    // A missing fragment shouldn't take the whole page down — log and move on.
    console.error(`Error loading fragment "${filePath}":`, err);
  }
}

// On load, pull in the header then the footer. (header.js / footer.js notice
// the injected markup on their own.)
document.addEventListener('DOMContentLoaded', async () => {
  await loadFragment('header-placeholder', '/html-pages/header.html');
  await loadFragment('footer-placeholder', '/html-pages/footer.html');
});
