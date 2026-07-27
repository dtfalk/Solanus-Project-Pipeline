// global.js — data-drive the shared header + footer from /data/global.json
// (nav links, social links, logo, footer text) and make them editable.
//
// Runs on every page/host. If global.json can't load OR anything throws, the
// original hand-crafted header/footer HTML is left exactly as-is (defensive:
// each section is built into a fragment first, then swapped atomically).
(function () {
  'use strict';

  // platform -> inline SVG symbol id + accessible label + tracking key, from the
  // site config (window.SITE.socialIcons). Falls back to no-icon labels.
  var ICON = (window.SITE && window.SITE.socialIcons) || {};

  function slug(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ''); }
  function isExternal(u) { return /^https?:\/\//i.test(u || ''); }
  function svgEl(tag) { return document.createElementNS('http://www.w3.org/2000/svg', tag); }

  function ONLIVE() { return !!(window.SITE && window.SITE.isProd()); }
  function navItem(item, i, editable, vis) {
    var li = document.createElement('li');
    var a = document.createElement('a');
    a.href = item.url || '#';
    a.textContent = item.label || '';
    a.setAttribute('data-track', 'nav' + (slug(item.label) || i));
    if (item.external || isExternal(item.url)) { a.target = '_blank'; a.rel = 'noopener'; }
    if (vis === 'dev') { a.style.color = '#ff3b30'; a.title = 'Dev only — not published to the live site'; }
    else if (vis === 'unlisted') { a.style.color = '#ffb454'; a.title = 'Unlisted — reachable by link, hidden from the live menu'; }
    if (editable) {
      a.setAttribute('data-edit-file', 'global');
      a.setAttribute('data-edit-text', 'nav[' + i + '].label');
    }
    li.appendChild(a);
    return li;
  }

  function fillNav(ul, nav, editable) {
    if (!ul) return;
    var onLive = ONLIVE();
    var frag = document.createDocumentFragment();
    nav.forEach(function (item, i) {
      var vis = item.visibility || 'public';
      if (onLive && vis !== 'public') return; // the live menu shows only public pages
      frag.appendChild(navItem(item, i, editable, vis));
    });
    ul.innerHTML = '';
    ul.appendChild(frag);
    if (editable) {
      ul.setAttribute('data-edit-file', 'global');
      ul.setAttribute('data-edit-list', 'nav');
      ul.setAttribute('data-edit-list-label', 'nav links');
    }
  }

  function socialAnchor(item, i, editable) {
    var meta = ICON[item.platform] || { id: '', label: item.platform || 'link', track: '' };
    var a = document.createElement('a');
    a.href = item.url || '#';
    a.target = '_blank'; a.rel = 'noopener';
    a.setAttribute('aria-label', meta.label);
    if (meta.track) a.setAttribute('data-track', meta.track);
    var svg = svgEl('svg'); svg.setAttribute('viewBox', '0 0 64 64'); svg.setAttribute('fill', 'currentColor');
    var use = svgEl('use');
    use.setAttribute('href', '#' + meta.id);
    use.setAttributeNS('http://www.w3.org/1999/xlink', 'xlink:href', '#' + meta.id);
    svg.appendChild(use); a.appendChild(svg);
    if (editable) {
      a.setAttribute('data-edit-file', 'global');
      a.setAttribute('data-edit-attr', 'social[' + i + '].url|href');
      a.setAttribute('data-edit-label', meta.label + ' link');
    }
    return a;
  }

  function fillSocial(container, social, editable) {
    if (!container) return;
    var frag = document.createDocumentFragment();
    social.forEach(function (item, i) { frag.appendChild(socialAnchor(item, i, editable)); });
    container.innerHTML = '';
    container.appendChild(frag);
    if (editable) {
      container.setAttribute('data-edit-file', 'global');
      container.setAttribute('data-edit-list', 'social');
      container.setAttribute('data-edit-list-label', 'social links');
    }
  }

  function setText(node, val, path) {
    if (!node) return;
    if (val != null) node.textContent = val;
    node.setAttribute('data-edit-file', 'global');
    node.setAttribute('data-edit-text', path);
  }

  // Dev-only: red nav shortcuts (SITE.devLinks) so editors can reach unlisted
  // pages without typing the URL. Host-gated (never appear on the live site),
  // added AFTER the nav is (re)built, idempotent. Desktop link sits beside the editable menu
  // (not inside it, so it doesn't disturb nav editing); mobile link goes in the
  // (non-editable) mobile menu.
  function addDevLinks() {
    if (ONLIVE()) return;
    var links = (window.SITE && window.SITE.devLinks) || [];
    if (!links.length) return;
    var menu = document.querySelector('.main-nav .menu');
    var mob = document.querySelector('.mobile-menu-links');
    links.forEach(function (lk) {
      if (!lk || !lk.url) return;
      var sel = '.engine-dev-link[data-url="' + lk.url + '"]';
      if (menu && !menu.querySelector(sel)) {
        var li = document.createElement('li');
        var a = document.createElement('a');
        a.href = lk.url; a.textContent = lk.label || lk.url; a.className = 'engine-dev-link';
        a.setAttribute('data-url', lk.url);
        a.title = 'Dev only — ' + (lk.label || lk.url);
        a.style.cssText = 'color:#ff3b30;font-weight:800;text-decoration:none;letter-spacing:.04em';
        li.appendChild(a);
        menu.appendChild(li);  // inline with the nav items (no wrapping)
      }
      if (mob && !mob.querySelector(sel)) {
        var li2 = document.createElement('li');
        var ma = document.createElement('a');
        ma.href = lk.url; ma.textContent = (lk.label || lk.url) + ' (dev)'; ma.className = 'engine-dev-link';
        ma.setAttribute('data-url', lk.url);
        ma.style.cssText = 'color:#ff3b30;font-weight:800';
        li2.appendChild(ma); mob.appendChild(li2);
      }
    });
  }

  // Build the email-signup questionnaire (footer modal) from global.json so the
  // site owner can edit the questions. Rebuilds only the field rows inside #fanForm —
  // the form element, submit button, and modal wiring (footer.js) are untouched.
  function fillQuestionnaire(q, editable) {
    if (!q) return;
    var form = document.getElementById('fanForm');
    if (form && Array.isArray(q.fields)) {
      var submitBtn = document.getElementById('questionnaire-submit-btn');
      [].slice.call(form.children).forEach(function (c) { if (c !== submitBtn) form.removeChild(c); });
      var frag = document.createDocumentFragment();
      q.fields.forEach(function (f) {
        if (!f || !f.label) return;
        var name = f.name || slug(f.label);
        var lab = document.createElement('label'); lab.textContent = f.label; frag.appendChild(lab);
        if (f.type === 'select') {
          var sel = document.createElement('select'); sel.name = name;
          var none = document.createElement('option'); none.value = ''; none.textContent = 'None'; sel.appendChild(none);
          (f.options || []).forEach(function (o) { var op = document.createElement('option'); op.textContent = o; sel.appendChild(op); });
          frag.appendChild(sel);
        } else {
          var inp = document.createElement('input'); inp.type = 'text'; inp.name = name; if (f.placeholder) inp.placeholder = f.placeholder;
          frag.appendChild(inp);
        }
      });
      if (submitBtn) form.insertBefore(frag, submitBtn); else form.appendChild(frag);
      if (editable) { form.setAttribute('data-edit-file', 'global'); form.setAttribute('data-edit-list', 'questionnaire.fields'); form.setAttribute('data-edit-list-label', 'questions'); }
    }
    var cm = document.getElementById('confirm-modal');
    if (cm && q.confirm) {
      var h = cm.querySelector('h2'); if (h && q.confirm.heading != null) h.textContent = q.confirm.heading;
      var pb = cm.querySelector('p'); if (pb && q.confirm.body != null) pb.textContent = q.confirm.body;
    }
    var sb = document.getElementById('questionnaire-submit-btn'); if (sb && q.submitLabel != null) sb.textContent = q.submitLabel;
  }
  // Analytics / pixels from global.json.tracking — editable in the Tracking panel,
  // so adding a service is just another ID + a small block here. Fires only on the
  // live site (never localhost / the draft), and never twice.
  var __trackDone = {};
  function initTracking(t) {
    t = t || {};
    if (!ONLIVE()) return;
    if (t.metaPixelId && !__trackDone.meta) {
      __trackDone.meta = true;
      !function (f, b, e, v, n, s, x) { if (f.fbq) return; n = f.fbq = function () { n.callMethod ? n.callMethod.apply(n, arguments) : n.queue.push(arguments); }; if (!f._fbq) f._fbq = n; n.push = n; n.loaded = !0; n.version = '2.0'; n.queue = []; s = b.createElement(e); s.async = !0; s.src = v; x = b.getElementsByTagName(e)[0]; x.parentNode.insertBefore(s, x); }(window, document, 'script', 'https://connect.facebook.net/en_US/fbevents.js');
      window.fbq('init', String(t.metaPixelId)); window.fbq('track', 'PageView');
    }
    if (t.gaMeasurementId && !__trackDone.ga) {
      __trackDone.ga = true;
      var g = document.createElement('script'); g.async = true; g.src = 'https://www.googletagmanager.com/gtag/js?id=' + encodeURIComponent(t.gaMeasurementId); document.head.appendChild(g);
      window.dataLayer = window.dataLayer || []; window.gtag = function () { window.dataLayer.push(arguments); }; window.gtag('js', new Date()); window.gtag('config', t.gaMeasurementId);
    }
  }
  function hydrate(data) {
    var header = document.querySelector('.site-header');
    var footer = document.querySelector('#footer-placeholder footer') || document.querySelector('footer');

    var logo = header && header.querySelector('.logo img');
    if (logo && data.logo) {
      logo.src = (window.mediaUrl ? window.mediaUrl(data.logo) : data.logo);
      logo.setAttribute('data-edit-file', 'global');
      logo.setAttribute('data-edit-image', 'logo');
      logo.setAttribute('data-edit-label', 'logo');
    }

    if (Array.isArray(data.nav)) {
      fillNav(document.querySelector('.main-nav .menu'), data.nav, true);   // desktop (editable)
      fillNav(document.querySelector('.mobile-menu-links'), data.nav, false); // mobile mirror
    }

    if (Array.isArray(data.social)) {
      fillSocial(header && header.querySelector(':scope > .social-icons'), data.social, true);
      fillSocial(document.querySelector('.mobile-social-icons .social-icons'), data.social, false);
      fillSocial(document.querySelector('.social-icons-footer'), data.social, false);
    }

    if (data.footer && footer) {
      setText(footer.querySelector('#footer-blurb'), data.footer.blurb, 'footer.blurb');
      setText(footer.querySelector('#footer-copyright'), data.footer.copyright, 'footer.copyright');
      setText(document.getElementById('signup-btn'), data.footer.signupLabel, 'footer.signupLabel');
    }

    if (data.questionnaire) { try { fillQuestionnaire(data.questionnaire, true); } catch (e) { console.warn('[global] questionnaire render failed:', e.message); } }

    if (data.tracking) { try { initTracking(data.tracking); } catch (e) { console.warn('[global] tracking init failed:', e.message); } }

    if (typeof window.setCurrentNavLink === 'function') { try { window.setCurrentNavLink(); } catch (_) {} }

    try { addDevLinks(); } catch (_) {}
    window.__ENGINE_GLOBAL__ = data;
    document.dispatchEvent(new CustomEvent('engine-data-ready', { detail: { file: 'global', data: data } }));
  }

  function start() {
    fetch('/data/global.json')
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (data) { try { hydrate(data); } catch (e) { console.warn('[global] hydrate failed, keeping static chrome:', e.message); } })
      .catch(function (e) { console.warn('[global] using static header/footer (global.json: ' + e.message + ')'); try { addDevLinks(); } catch (_) {} });
  }

  // Wait until the header + footer fragments are injected, then hydrate. On a
  // standalone page with no header/footer placeholders, there are
  // no fragments to wait for — load global.json immediately anyway so the global
  // model is announced (Theme/Pages panels + site theme work everywhere; hydrate
  // safely no-ops on the missing header/footer).
  function waitForFragments() {
    if (!document.getElementById('header-placeholder') && !document.getElementById('footer-placeholder')) { start(); return; }
    var tries = 0;
    var t = setInterval(function () {
      var header = document.querySelector('.site-header');
      var footer = document.querySelector('#footer-placeholder footer') || document.querySelector('footer');
      if ((header && footer) || tries > 80) { clearInterval(t); start(); }
      tries++;
    }, 50);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', waitForFragments);
  else waitForFragments();
})();
