// lightbox.js — one shared image lightbox for the whole site.
//
// Any element with a data-lightbox="<group>" attribute opens a fullscreen
// viewer: tap the backdrop / × / Escape to close, ‹ › / arrow keys / swipe to
// move through the group. Images use object-fit:contain, so they never stretch.
//
// Suppressed while editing (the editor's media tools handle image clicks then).
(function () {
  'use strict';

  var box, imgEl, capEl, prevBtn, nextBtn, dlBtn, slides = [], idx = 0;

  function injectCSS() {
    if (document.getElementById('engine-lb-styles')) return;
    var css = [
      '#engine-lb{--lb-w:min(92vw,900px);position:fixed;inset:0;z-index:2147482000;display:none;background:rgba(8,6,3,.93);align-items:center;justify-content:center}',
      '#engine-lb.open{display:flex}',
      'body.engine-lb-open{overflow:hidden}',
      '.engine-lb-fig{margin:0;display:flex;flex-direction:column;align-items:center;gap:10px;max-width:var(--lb-w);max-height:88vh}',
      '.engine-lb-fig img{max-width:var(--lb-w);max-height:80vh;object-fit:contain;border-radius:8px;box-shadow:0 20px 60px -10px rgba(0,0,0,.8)}',
      '.engine-lb-fig figcaption{color:#f4ead4;font:600 12px system-ui,sans-serif;letter-spacing:1.5px;opacity:.7}',
      '.engine-lb-dl{display:inline-flex;align-items:center;gap:8px;background:#e3a23c;color:#241a08;border:0;border-radius:8px;padding:9px 18px;font:700 13px system-ui,sans-serif;cursor:pointer;letter-spacing:.3px}',
      '.engine-lb-dl[hidden]{display:none}',
      '.engine-lb-dl:hover{filter:brightness(1.07)}',
      '.engine-lb-dl:disabled{opacity:.6;cursor:default}',
      '.engine-lb-close,.engine-lb-prev,.engine-lb-next{position:fixed;background:rgba(0,0,0,.35);color:#fff;border:1px solid rgba(255,255,255,.25);border-radius:999px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1}',
      '.engine-lb-close{top:14px;right:16px;width:44px;height:44px;font-size:26px}',
      '.engine-lb-prev,.engine-lb-next{top:50%;transform:translateY(-50%);width:48px;height:48px;font-size:30px}',
      '.engine-lb-prev{left:max(6px,calc(50% - var(--lb-w)/2 - 56px))}',
      '.engine-lb-next{right:max(6px,calc(50% - var(--lb-w)/2 - 56px))}',
      '.engine-lb-close:hover,.engine-lb-prev:hover,.engine-lb-next:hover{background:rgba(0,0,0,.65)}',
      '@media (max-width:600px){.engine-lb-prev,.engine-lb-next{width:40px;height:40px;font-size:24px}.engine-lb-fig img{max-height:74vh}}'
    ].join('\n');
    var s = document.createElement('style'); s.id = 'engine-lb-styles'; s.textContent = css;
    document.head.appendChild(s);
  }

  function build() {
    if (box) return;
    injectCSS();
    box = document.createElement('div');
    box.id = 'engine-lb';
    box.setAttribute('aria-hidden', 'true');
    box.innerHTML =
      '<button class="engine-lb-close" aria-label="Close">×</button>' +
      '<button class="engine-lb-prev" aria-label="Previous">‹</button>' +
      '<figure class="engine-lb-fig"><img alt=""><figcaption></figcaption><button class="engine-lb-dl" type="button" hidden>↓ Download</button></figure>' +
      '<button class="engine-lb-next" aria-label="Next">›</button>';
    document.body.appendChild(box);
    imgEl = box.querySelector('img');
    capEl = box.querySelector('figcaption');
    prevBtn = box.querySelector('.engine-lb-prev');
    nextBtn = box.querySelector('.engine-lb-next');
    dlBtn = box.querySelector('.engine-lb-dl');
    dlBtn.addEventListener('click', downloadCurrent);
    box.querySelector('.engine-lb-close').addEventListener('click', close);
    prevBtn.addEventListener('click', function (e) { e.stopPropagation(); step(-1); });
    nextBtn.addEventListener('click', function (e) { e.stopPropagation(); step(1); });
    box.addEventListener('click', function (e) { if (e.target === box) close(); });
    var x0 = null;
    box.addEventListener('touchstart', function (e) { x0 = e.touches[0].clientX; }, { passive: true });
    box.addEventListener('touchend', function (e) {
      if (x0 == null) return;
      var dx = e.changedTouches[0].clientX - x0; x0 = null;
      if (Math.abs(dx) > 40) step(dx < 0 ? 1 : -1);
    });
  }

  function imgSrcOf(elm) {
    if (elm.getAttribute('data-full')) return elm.getAttribute('data-full');
    if (elm.tagName === 'IMG') return elm.src;
    var im = elm.querySelector && elm.querySelector('img');
    return im ? im.src : '';
  }
  // Build the slide list for a group. An element with a data-back (e.g. a
  // product's front/back) contributes two slides, so a "flip" becomes a swipe.
  function collectSlides(groupKey, clicked) {
    var els = [].slice.call(document.querySelectorAll('[data-lightbox="' + groupKey.replace(/"/g, '\\"') + '"]'));
    if (!els.length) els = [clicked];
    var out = [];
    els.forEach(function (elm) {
      var im = elm.querySelector && elm.querySelector('img');
      var alt = elm.alt || (im ? im.alt : '') || '';
      var dl = elm.getAttribute('data-download') || '';
      out.push({ src: imgSrcOf(elm), alt: alt, el: elm, download: dl });
      var back = elm.getAttribute('data-back');
      if (back) out.push({ src: back, alt: alt + ' — back', el: elm, download: '' });
    });
    return out;
  }

  function show() {
    var s = slides[idx];
    if (!s) return;
    imgEl.src = s.src;
    imgEl.alt = s.alt;
    capEl.textContent = slides.length > 1 ? (idx + 1) + ' / ' + slides.length : '';
    var multi = slides.length > 1;
    prevBtn.style.display = multi ? '' : 'none';
    nextBtn.style.display = multi ? '' : 'none';
    if (dlBtn) { dlBtn.hidden = !s.download; dlBtn.__url = s.download || ''; }
  }
  function step(d) { if (!slides.length) return; idx = (idx + d + slides.length) % slides.length; show(); }
  // Download the current image straight from blob storage. fetch→blob→object-URL
  // forces a real "save as" (cross-origin <a download> alone won't); storage CORS
  // allows GET from any origin. Falls back to opening the file in a new tab.
  function downloadCurrent(e) {
    if (e) e.stopPropagation();
    var url = dlBtn.__url; if (!url) return;
    var name = decodeURIComponent((url.split('/').pop() || 'download').split('?')[0]) || 'download';
    var orig = dlBtn.textContent; dlBtn.disabled = true; dlBtn.textContent = 'Downloading…';
    fetch(url).then(function (r) { if (!r.ok) throw new Error('http ' + r.status); return r.blob(); }).then(function (blob) {
      var u = URL.createObjectURL(blob);
      var a = document.createElement('a'); a.href = u; a.download = name;
      document.body.appendChild(a); a.click();
      setTimeout(function () { URL.revokeObjectURL(u); a.remove(); }, 1500);
    }).catch(function () { window.open(url, '_blank', 'noopener'); })
      .then(function () { dlBtn.disabled = false; dlBtn.textContent = orig; });
  }

  function open(elm) {
    build();
    var g = elm.getAttribute('data-lightbox') || '';
    slides = collectSlides(g, elm);
    idx = 0;
    for (var i = 0; i < slides.length; i++) { if (slides[i].el === elm) { idx = i; break; } }
    show();
    box.classList.add('open');
    box.setAttribute('aria-hidden', 'false');
    document.body.classList.add('engine-lb-open');
  }
  function close() {
    if (box) { box.classList.remove('open'); box.setAttribute('aria-hidden', 'true'); }
    document.body.classList.remove('engine-lb-open');
  }

  document.addEventListener('click', function (e) {
    if (document.body.classList.contains('engine-edit')) return; // editing → media tools, not lightbox
    var t = e.target.closest && e.target.closest('[data-lightbox]');
    if (!t) return;
    e.preventDefault();
    open(t);
  });
  document.addEventListener('keydown', function (e) {
    if (!box || !box.classList.contains('open')) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowLeft') step(-1);
    else if (e.key === 'ArrowRight') step(1);
  });
})();
