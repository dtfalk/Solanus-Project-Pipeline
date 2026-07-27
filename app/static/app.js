/* ============================================================================
 * app.js — Solanus Casey Archival Tool (frontend behavior)
 * ============================================================================
 * Vanilla JS, no framework, no build step. It wires the five panes in
 * index.html to the FastAPI backend at /api/* (see app/server.py).
 *
 * The mental model: the LEFT RAIL is a live picture of one `RetrievalConfig`
 * (the same dataclass lib/retrieval.py uses). Every model picker and tool
 * toggle just edits a field on the in-memory `STATE` object; when you Ask, we
 * POST that object to /api/ask and render the grounded, *cited* answer. A
 * citation [n] carries enough provenance (doc_id + rid + page + vertices) to
 * open the EXACT scanned region in a deep-zoom IIIF viewer — provenance-first,
 * exactly as the research plan demands.
 *
 * ----------------------------------------------------------------------------
 * The /api/* contract this UI expects the backend to honor
 * ----------------------------------------------------------------------------
 *   GET  /api/health   -> { "ok": true }
 *
 *   GET  /api/config   -> describes the swappable axes + tools so the UI builds
 *                         itself from the server (single source of truth):
 *     {
 *       "llms":       ["gemini-2.5-flash", ...],            // config.LLMS keys
 *       "embeddings": ["gemini-embedding-001@1536", ...],   // built vectorstore spaces
 *       "rerankers":  ["rerank-2.5", ...],                  // config.RERANKERS keys
 *       "defaults":   { "llm":..., "embedding":"model@dim", "reranker":... },
 *       "tools": [    // one per toggleable retrieval technique (RESEARCH_PLAN part D)
 *         { "key":"use_bm25",       "label":"BM25 (lexical)",     "default":true,  "paid":false,
 *           "hint":"sparse keyword search" },
 *         { "key":"use_dense",      "label":"Dense (semantic)",   "default":true,  "paid":false },
 *         { "key":"use_rerank",     "label":"Rerank",             "default":false, "paid":true  },
 *         { "key":"use_hyde",       "label":"HyDE",               "default":false, "paid":true  },
 *         { "key":"use_multiquery", "label":"Multi-query (RAG-Fusion)", "default":false, "paid":true },
 *         { "key":"use_llm_router", "label":"LLM router",         "default":false, "paid":true  },
 *         { "key":"auto_apply_route","label":"Auto-escalate route","default":false,"paid":true  }
 *       ]
 *     }
 *
 *   POST /api/ask  { "query":..., "llm":..., "embedding":"model@dim",
 *                    "reranker":..., "kinds":[...]|null, "toggles": {use_bm25:true,...} }
 *     -> {
 *       "answer": "…with inline markers [1] [2]…",   // grounded prose
 *       "citations": [                               // index == the [n] in the answer (1-based)
 *         { "n":1, "doc_id":"Volume_1__p001", "rid":"doc_1.src_content.0",
 *           "kind":"letter"|"notebook_entry", "section":"Volume_1",
 *           "page":1, "pdf_page":6, "snippet":"…", "score":0.83,
 *           "vertices":[[x,y],...], "min_conf":0.948 }
 *       ],
 *       "grade":  { "verdict":"answer"|"caveat"|"abstain", "confidence":0.0-1.0, "reason":"…" },
 *       "trace":  { "route":{complexity,strategy,reason}, "queries_used":[...],
 *                   "steps":[ {"tool":"bm25","detail":"…","ms":12,"items":50}, ... ],
 *                   "cost":{ "usd":0.0, "input_tokens":0, "output_tokens":0 } }
 *     }
 *
 *   GET  /api/graph  -> data/graph.json verbatim (build_graph.export_graph_json):
 *     { "meta":{n_nodes,n_edges,node_kinds,edge_kinds},
 *       "nodes":[{id,kind,label,...}], "edges":[{source,target,kind,label,...}] }
 *
 *   GET  /api/region?doc_id=&rid=  -> everything the citation modal needs to deep-zoom:
 *     { "doc_id":..., "rid":..., "section":"Volume_1", "page":1, "pdf_page":6,
 *       "canvas":"p6", "image_url":"/api/image/Volume_1/6",
 *       "image_width":5313, "image_height":6875,
 *       "vertices":[[x,y],...], "min_conf":0.948, "category":"src_content",
 *       "text":"…", "manifest_url":"/api/manifest/Volume_1",
 *       "pdf_url":"/api/pdf/Volume_1?page=6" }
 *
 *   GET  /api/image/<section>/<pdf_page>     -> the page PNG (for OpenSeadragon)
 *   GET  /api/pdf/<section>?page=<pdf_page>  -> the searchable source PDF (download)
 *
 * The frontend degrades gracefully: if an endpoint 404s (server.py not built
 * yet, or a PDF not generated), it shows a friendly note instead of breaking.
 * ============================================================================ */

(() => {
  "use strict";

  // ==========================================================================
  // STATE — the in-memory picture of the current RetrievalConfig + last answer
  // ==========================================================================
  // Think of STATE as the single mutable "settings" object the whole UI reads.
  // The rail writes to it; Ask serializes it; nothing else holds config. Keeping
  // one source of truth is what lets every control be a pure "set a field" action.
  const STATE = {
    config:    null,          // /api/config response (axes + tool descriptors)
    toggles:   {},            // {use_bm25:true, use_dense:true, ...}
    citations: [],            // citations from the most recent answer (for the modal)
  };

  // Small DOM helpers — terse on purpose so the logic below reads clearly.
  const $  = (sel) => document.querySelector(sel);
  const el = (tag, attrs = {}, ...kids) => {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "html") node.innerHTML = v;
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v);
    }
    for (const kid of kids) node.append(kid && kid.nodeType ? kid : document.createTextNode(kid ?? ""));
    return node;
  };

  // Single network helper. Returns parsed JSON, or throws with a useful message.
  // We centralize fetch so error handling + the "is the backend awake?" story
  // live in exactly one place.
  async function api(path, opts) {
    const res = await fetch(path, opts);
    if (!res.ok) throw new Error(`${path} -> HTTP ${res.status}`);
    const ct = res.headers.get("content-type") || "";
    return ct.includes("application/json") ? res.json() : res.text();
  }

  // ==========================================================================
  // BOOT — health check, then build the rail from /api/config, then load graph
  // ==========================================================================
  async function boot() {
    try {
      STATE.config = await api("/api/config");
      buildModelPickers(STATE.config);
      buildToggles(STATE.config);
    } catch (err) {
      // No backend yet? Fall back to the static config we know from config.py so
      // the UI is still demonstrable. (server.py will supersede this once built.)
      console.warn("Falling back to static config:", err.message);
      STATE.config = FALLBACK_CONFIG;
      buildModelPickers(STATE.config);
      buildToggles(STATE.config);
    }
    wireChat();
    loadPersonas();
    wireVoice();
    wireHistory();
    wireModal();
    wireTabs();
    wireNavDropdowns();
    wireAvatarChooser();
    wireAvatarFullscreen();
    wireVoicePicker();
    wireEntityResize();
    wireRailSplitter();
    wireChatResize();
    wireGraphSettingsResize();
    wireTranslate();
    wirePipelinePanel();   // the live pipeline is now an optional, draggable floating window
    // graph/pipeline/tools render lazily when their tab is first opened (a Cytoscape canvas sized
    // inside a hidden tab renders wrong), so we don't loadGraph() here.
  }

  // Avatar chooser (Settings): pick which 3D head shows in avatar mode; remembered across reloads.
  function wireAvatarChooser() {
    const sel = document.getElementById("pick-avatar"); if (!sel) return;
    const KEY = "solanus_avatar_model";
    const apply = (val, swap) => {
      const [url, yaw] = val.split("|");
      window.SOLANUS_AVATAR_GLB = url;
      window.SOLANUS_AVATAR_YAW = parseFloat(yaw);
      if (swap && window.solanusAvatar && window.solanusAvatar.setModel && window.solanusAvatar.ready)
        window.solanusAvatar.setModel(url, parseFloat(yaw));
    };
    const saved = localStorage.getItem(KEY);
    if (saved) { sel.value = saved; apply(saved, false); }     // pre-select before the avatar mounts
    sel.addEventListener("change", () => { localStorage.setItem(KEY, sel.value); apply(sel.value, true); });
  }

  // FULL-SCREEN AVATAR MODE — fill the viewport with Solanus; show only the last exchange as a caption
  // (toggle the full transcript on demand). The floating input + a mic proxy keep the conversation going.
  function wireAvatarFullscreen() {
    const enter = () => {
      document.body.classList.add("avatar-fs");
      if (window.solanusAvatar && !window.solanusAvatar.ready) window.solanusAvatar.mount();
      requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));   // re-fit the WebGL canvas
    };
    const exit = () => {
      document.body.classList.remove("avatar-fs", "show-transcript");
      requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
    };
    window._avatarExitFs = exit;                                  // so disabling avatar mode also exits
    const fsBtn = $("#avatar-fs-btn"); if (fsBtn) fsBtn.addEventListener("click", enter);
    const exitBtn = $("#fs-exit"); if (exitBtn) exitBtn.addEventListener("click", exit);
    const tr = $("#fs-transcript");
    if (tr) tr.addEventListener("click", () => document.body.classList.toggle("show-transcript"));
    const fsMic = $("#fs-mic"), realMic = $("#mic-btn");
    if (fsMic && realMic) {
      fsMic.addEventListener("click", () => realMic.click());     // reuse the real STT pipeline
      new MutationObserver(() => fsMic.classList.toggle("listening", realMic.classList.contains("listening")))
        .observe(realMic, { attributes: true, attributeFilter: ["class"] });
    }
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && document.body.classList.contains("avatar-fs")) exit();
    });
  }

  // VOICE PICKER (Settings) — choose the Azure voice Solanus speaks in, or import a custom voice name.
  // The chosen voice rides on window.SOLANUS_TTS_VOICE → sent with /api/tts (read-aloud + avatar).
  function wireVoicePicker() {
    const sel = $("#pick-voice"); if (!sel) return;
    const customField = $("#pick-voice-custom-field"), customInput = $("#pick-voice-custom");
    const KEY = "solanus_tts_voice";
    const setVoice = (v) => { window.SOLANUS_TTS_VOICE = v || null; };
    const persist = (v) => { try { if (v) localStorage.setItem(KEY, v); } catch (_) {} };
    fetch("/api/voice_catalog").then((r) => r.json()).then((cat) => {
      const voices = (cat && cat.voices) || [];
      sel.innerHTML = "";
      for (const v of voices) sel.append(el("option", { value: v.name }, v.label));
      sel.append(el("option", { value: "__custom__" }, "Custom… (import a voice name)"));
      const saved = localStorage.getItem(KEY) || (cat && cat.default) || (voices[0] && voices[0].name);
      const known = voices.some((v) => v.name === saved);
      if (saved && !known) {                                  // a previously imported custom voice
        sel.value = "__custom__";
        if (customInput) customInput.value = saved;
        if (customField) customField.hidden = false;
        setVoice(saved);
      } else if (saved) { sel.value = saved; setVoice(saved); }
    }).catch(() => { /* catalog optional — server default voice still applies */ });
    sel.addEventListener("change", () => {
      if (sel.value === "__custom__") {
        if (customField) customField.hidden = false;
        const v = (customInput && customInput.value.trim()) || "";
        setVoice(v); persist(v);
      } else {
        if (customField) customField.hidden = true;
        setVoice(sel.value); persist(sel.value);
      }
    });
    if (customInput) customInput.addEventListener("input", () => {
      const v = customInput.value.trim(); setVoice(v); persist(v);
    });
  }

  // SOLANUS VOICE TRANSLATOR — calls /api/translate (Qwen style model). Gated: shows a clear
  // "configure Azure" note until the endpoint env vars are set (see TRANSLATOR_AZURE_SETUP.md).
  function wireTranslate() {
    const go = $("#tr-go"), inp = $("#tr-input"), out = $("#tr-out"), st = $("#tr-status");
    if (!go || !inp) return;
    fetch("/api/translate").then((r) => r.json()).then((d) => {
      if (st) st.textContent = d && d.configured
        ? "Ready."
        : "Not yet connected — deploy the model and set AZURE_TRANSLATOR_* (see TRANSLATOR_AZURE_SETUP.md).";
    }).catch(() => { if (st) st.textContent = ""; });
    go.addEventListener("click", () => {
      const text = inp.value.trim(); if (!text) return;
      go.textContent = "rendering…"; if (out) out.style.display = "none";
      fetch("/api/translate", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }) })
        .then(async (r) => { const d = await r.json().catch(() => ({})); if (!r.ok) throw new Error(d.detail || ("HTTP " + r.status)); return d; })
        .then((d) => { if (out) { out.textContent = d.styled || "(no output)"; out.style.display = "block"; } })
        .catch((e) => { if (out) { out.textContent = "Translator unavailable: " + e.message; out.style.display = "block"; } })
        .finally(() => { go.textContent = "Render in his voice →"; });
    });
  }

  // TEXT SIZE — the A−/A+ widget was removed; font size is now the single --ui-scale knob in theme.css.
  // Clear any stale inline override a previous session's A−/A+ may have left on <html>.
  (function clearOldTextSize() {
    try { document.documentElement.style.removeProperty("font-size"); } catch (_) {}
  })();

  // Drag the entity/dossier panel's left edge to resize it; width persists. Panel is fixed to the right.
  function wireEntityResize() {
    const panel = $("#entity-panel");
    if (!panel) return;
    const KEY = "solanus_entity_w", MIN = 360;
    const maxW = () => Math.round(window.innerWidth * 0.96);
    const clamp = (v) => Math.max(MIN, Math.min(maxW(), v));
    const setW = (w) => { w = clamp(w); panel.style.width = w + "px";
      try { localStorage.setItem(KEY, String(Math.round(w))); } catch (_) {} };
    const saved = parseInt(localStorage.getItem(KEY) || "", 10);
    if (Number.isFinite(saved)) panel.style.width = clamp(saved) + "px";
    const handle = document.createElement("div");
    handle.className = "ep-resize"; handle.tabIndex = 0;
    handle.setAttribute("role", "separator"); handle.setAttribute("aria-orientation", "vertical");
    handle.setAttribute("aria-label", "resize panel width");
    panel.appendChild(handle);
    let dragging = false;
    const onMove = (e) => { if (!dragging) return; e.preventDefault(); setW(window.innerWidth - e.clientX); };
    const onUp = () => { dragging = false; handle.classList.remove("dragging"); document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    handle.addEventListener("pointerdown", (e) => { e.preventDefault(); dragging = true;
      handle.classList.add("dragging"); document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });
    handle.addEventListener("keydown", (e) => { const cur = panel.getBoundingClientRect().width;
      if (e.key === "ArrowLeft") { e.preventDefault(); setW(cur + 24); }
      if (e.key === "ArrowRight") { e.preventDefault(); setW(cur - 24); } });
  }

  // Drag the dossier header to DETACH the right-hand panel into a floating, fully-resizable window
  // (then resize:both from the corner). Once detached it stays floating and just re-renders on each click.
  function wireDossierDrag() {
    const panel = $("#entity-panel"), head = $("#ep-head");
    if (!panel || !head) return;
    let dragging = false, sx = 0, sy = 0, started = false;
    const onMove = (e) => {
      if (!dragging) return; e.preventDefault();
      if (!started) {                                   // first movement: detach in place, then follow the cursor
        const r = panel.getBoundingClientRect();
        panel.classList.add("floating");
        panel.style.left = r.left + "px"; panel.style.top = r.top + "px";
        panel.style.right = "auto"; panel.style.width = r.width + "px"; panel.style.height = r.height + "px";
        started = true;
      }
      const nl = parseFloat(panel.style.left) + (e.clientX - sx);
      const nt = parseFloat(panel.style.top) + (e.clientY - sy);
      sx = e.clientX; sy = e.clientY;
      panel.style.left = Math.max(0, Math.min(window.innerWidth - 80, nl)) + "px";
      panel.style.top = Math.max(0, Math.min(window.innerHeight - 44, nt)) + "px";
    };
    const onUp = () => { dragging = false; head.classList.remove("dragging"); document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    head.addEventListener("pointerdown", (e) => {
      if (e.target.closest("button")) return;           // back / pop-out / close are clicks, not drags
      dragging = true; started = false; sx = e.clientX; sy = e.clientY;
      head.classList.add("dragging"); document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp);
    });
  }

  // Drag the splitter to resize the conversations side-rail; width persists via --rail-w (so .collapsed + mobile still win).
  function wireRailSplitter() {
    const rail = $("#side-rail"), row = document.querySelector(".chat-row"), split = $("#rail-splitter");
    if (!rail || !row || !split) return;
    const KEY = "solanus_rail_w", MIN = 170, MAX = 560;
    const clamp = (v) => Math.max(MIN, Math.min(MAX, v));
    const apply = (w) => { w = clamp(w); rail.style.setProperty("--rail-w", w + "px");
      try { localStorage.setItem(KEY, String(Math.round(w))); } catch (_) {} };
    const saved = parseInt(localStorage.getItem(KEY) || "", 10);
    if (Number.isFinite(saved)) apply(saved);
    let dragging = false;
    const onMove = (e) => { if (!dragging) return; e.preventDefault(); apply(e.clientX - row.getBoundingClientRect().left); };
    const onUp = () => { dragging = false; split.classList.remove("dragging"); rail.classList.remove("resizing");
      document.body.style.userSelect = ""; window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    split.addEventListener("pointerdown", (e) => { if (rail.classList.contains("collapsed")) return;
      e.preventDefault(); dragging = true; split.classList.add("dragging"); rail.classList.add("resizing");
      document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });
    split.addEventListener("keydown", (e) => { const cur = rail.getBoundingClientRect().width;
      if (e.key === "ArrowLeft") { e.preventDefault(); apply(cur - 24); }
      if (e.key === "ArrowRight") { e.preventDefault(); apply(cur + 24); } });
  }

  // Drag the conversation's right edge to set its reading width (persisted via --ask-card-w on the card).
  function wireChatResize() {
    const card = document.querySelector('.tab-panel[data-tab="ask"]');
    const handle = $("#chat-resize");
    if (!card || !handle) return;
    // set the width var on the shared <main> so BOTH ask cards (conversation + live-pipeline) track together
    const host = card.closest("main") || card;
    const KEY = "solanus_chat_w", MIN = 520;
    const maxW = () => Math.max(MIN, Math.round((card.parentElement || card).getBoundingClientRect().width));
    const clamp = (v) => Math.max(MIN, Math.min(maxW(), v));
    const apply = (w, save) => { w = clamp(w); host.style.setProperty("--ask-card-w", w + "px");
      if (save) { try { localStorage.setItem(KEY, String(Math.round(w))); } catch (_) {} } };
    const saved = parseInt(localStorage.getItem(KEY) || "", 10);
    if (Number.isFinite(saved)) apply(saved, false);
    let dragging = false;
    const onMove = (e) => { if (!dragging) return; e.preventDefault();
      apply(e.clientX - card.getBoundingClientRect().left, true); };
    const onUp = () => { dragging = false; handle.classList.remove("dragging"); document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    handle.addEventListener("pointerdown", (e) => { e.preventDefault(); dragging = true;
      handle.classList.add("dragging"); document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });
    handle.addEventListener("keydown", (e) => { const cur = card.getBoundingClientRect().width;
      if (e.key === "ArrowLeft") { e.preventDefault(); apply(cur - 32, true); }
      if (e.key === "ArrowRight") { e.preventDefault(); apply(cur + 32, true); }
      if (e.key === "Home") { e.preventDefault(); host.style.removeProperty("--ask-card-w");
        try { localStorage.removeItem(KEY); } catch (_) {} } });
  }

  // Drag the splitter to resize the knowledge-graph settings panel; width persists via --gs-w.
  function wireGraphSettingsResize() {
    const panel = $("#graph-settings"), split = $("#graph-splitter");
    if (!panel || !split) return;
    const KEY = "solanus_graph_settings_w", MIN = 180, MAX = 680;
    const clamp = (v) => Math.max(MIN, Math.min(MAX, v));
    const apply = (w, save) => { w = clamp(w); panel.style.setProperty("--gs-w", w + "px");
      if (save) { try { localStorage.setItem(KEY, String(Math.round(w))); } catch (_) {} } };
    const saved = parseInt(localStorage.getItem(KEY) || "", 10);
    if (Number.isFinite(saved)) apply(saved, false);
    let dragging = false;
    const onMove = (e) => { if (!dragging) return; e.preventDefault();
      apply(panel.getBoundingClientRect().right - e.clientX, true); };
    const onUp = () => { dragging = false; split.classList.remove("dragging"); panel.classList.remove("resizing");
      document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    split.addEventListener("pointerdown", (e) => { if (panel.classList.contains("collapsed")) return;
      e.preventDefault(); dragging = true; split.classList.add("dragging"); panel.classList.add("resizing");
      document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });
    split.addEventListener("keydown", (e) => { const cur = panel.getBoundingClientRect().width;
      if (e.key === "ArrowLeft") { e.preventDefault(); apply(cur + 28, true); }
      if (e.key === "ArrowRight") { e.preventDefault(); apply(cur - 28, true); } });
  }

  // keep the full-screen caption (last question + Solanus's reply) in sync
  function setCaption(q, a) {
    const cq = $("#cap-q"), ca = $("#cap-a");
    if (cq && q != null) cq.textContent = q ? "“" + q + "”" : "";
    if (ca && a != null) ca.textContent = String(a).replace(/\[\d+\]/g, "");
  }

  // ==========================================================================
  // TABS — show one main panel at a time; lazy-render heavy tabs on first open
  // ==========================================================================
  const _tabLoaded = {};
  function showTab(name) {                       // programmatic tab switch (reuses the click handler)
    const t = document.querySelector('#tabs .tab[data-tab="' + name + '"]');
    if (t) t.click();
  }
  function wireTabs() {
    const tabs = document.querySelectorAll("#tabs .tab");
    tabs.forEach((t) => t.addEventListener("click", () => {
      const name = t.dataset.tab;
      if (("#" + name) !== location.hash) { try { history.replaceState(null, "", "#" + name); } catch (e) {} }
      const ep = document.getElementById("entity-panel");   // dossier overlay belongs to the clicked node, not every tab
      if (ep) ep.classList.remove("open");
      tabs.forEach((x) => x.classList.toggle("active", x === t));
      document.querySelectorAll(".tab-panel").forEach((p) => { p.hidden = (p.dataset.tab !== name); });
      // the conversation sidebar belongs to the chat page only — other tabs go full-width
      const onAsk = (name === "ask");
      const sr = document.getElementById("side-rail"), rtg = document.getElementById("rail-toggle");
      if (sr) sr.style.display = onAsk ? "" : "none";
      if (rtg) rtg.style.display = onAsk ? "" : "none";
      const spl = document.getElementById("rail-splitter");
      if (spl) spl.style.display = onAsk ? "" : "none";
      if (name === "graph") {
        // first open: start on Solanus's own network — an immediately-sensible view, not a backbone blob
        if (!_tabLoaded.graph) { $("#graph-focus").value = "Solanus"; _tabLoaded.graph = true; loadBrowse(); }
        loadGraph(); renderTimeline();
      }
      if (name === "pipeline" && !_tabLoaded.pipeline) { renderPipeline(); _tabLoaded.pipeline = true; }
      // (live pipeline is no longer a tab — it's the optional draggable #flow-panel window)
      if (name === "avatar" && window.solanusAvatar) { window.solanusAvatar.mount(); }   // WebGL needs visibility
      if (name === "tools" && !_tabLoaded.tools) { renderToolsTab(); _tabLoaded.tools = true; }
      if (name === "map") renderMap();                     // re-render (Leaflet needs a visible container)
      if (name === "life" && !_tabLoaded.life) { renderLife(); _tabLoaded.life = true; }
      if (name === "reading" && !_tabLoaded.reading) { renderReadingRoom(); _tabLoaded.reading = true; }
      if (name === "people" && !_tabLoaded.people) { renderPeople(); _tabLoaded.people = true; }
      if (name === "family" && !_tabLoaded.family) { renderFamily(); _tabLoaded.family = true; }
      if (name === "walkthrough") renderWalkthrough();     // re-fetch (file is editable)
      if (name === "explain" && !_tabLoaded.explain) { renderExplain(); _tabLoaded.explain = true; }
      updateReopen();   // dossier reopen handle belongs to map/graph only
    }));
    // deep-link / shareable tab URLs: honor #map, #graph, etc. on load + back/forward
    const fromHash = () => {
      const n = (location.hash || "").replace(/^#/, "");
      if (n && document.querySelector('#tabs .tab[data-tab="' + n + '"]')) showTab(n);
    };
    window.addEventListener("hashchange", fromHash);
    fromHash();
  }

  // Grouped header nav: hover opens the dropdowns (CSS); this adds click/touch toggle, closes the menu
  // after a choice, and closes on outside-click or Escape. wireTabs already drives the .tab items inside.
  function wireNavDropdowns() {
    const groups = Array.from(document.querySelectorAll("#tabs .nav-group"));
    if (!groups.length) return;
    const closeAll = (except) => groups.forEach((g) => {
      if (g !== except) { g.classList.remove("open"); const b = g.querySelector(".nav-top"); if (b) b.setAttribute("aria-expanded", "false"); }
    });
    groups.forEach((g) => {
      const top = g.querySelector(".nav-top");
      top.addEventListener("click", (e) => {
        e.stopPropagation();
        const open = g.classList.toggle("open");
        top.setAttribute("aria-expanded", open ? "true" : "false");
        closeAll(g);
      });
      g.querySelectorAll(".nav-menu .tab").forEach((t) => t.addEventListener("click", () => closeAll(null)));
    });
    document.addEventListener("click", (e) => { if (!e.target.closest("#tabs .nav-group")) closeAll(null); });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeAll(null); });
  }

  // ==========================================================================
  // WALKTHROUGH tab — editable steps 1-7 + to-dos (content lives in a JSON file)
  // ==========================================================================
  async function renderWalkthrough() {
    let w;
    try { w = await api("/api/walkthrough"); }
    catch { $("#walk-steps").innerHTML = "<div class='muted-note'>walkthrough unavailable</div>"; return; }
    $("#walk-title").textContent = w.title || "Walkthrough";
    $("#walk-intro").textContent = w.intro || "";
    const steps = $("#walk-steps"); steps.innerHTML = "";
    for (const s of (w.steps || [])) {
      const cls = (s.status || "").includes("done") ? "free" : (s.status || "").includes("progress") ? "paid" : "";
      steps.append(el("div", { class: "explain-card" },
        el("h4", {}, `Step ${s.n}. ${s.title}`,
           s.status ? el("span", { class: "tag " + cls }, s.status) : "",
           s.folder ? el("span", { class: "muted-note", style: "margin-left:.5em;" }, s.folder) : ""),
        el("div", {}, s.summary || ""),
        s.next ? el("div", { class: "muted-note", style: "margin-top:.35em;" }, "Next: " + s.next) : ""));
    }
    const todo = $("#walk-todo"); todo.innerHTML = "";
    for (const t of (w.todo || []))
      todo.append(el("div", { class: "explain-card" }, el("h4", {}, t.title), el("div", {}, t.detail || "")));
  }

  // ==========================================================================
  // EXPLANATIONS tab — the thorough docs/*.md, rendered with a tiny markdown pass
  // ==========================================================================
  let _explainDocs = [];
  async function renderExplain() {
    try { const r = await api("/api/explain"); _explainDocs = r.docs || []; }
    catch { $("#explain-body").innerHTML = "<div class='muted-note'>explanations unavailable</div>"; return; }
    const nav = $("#explain-nav"); nav.innerHTML = "";
    _explainDocs.forEach((d, i) => {
      const b = el("button", { class: "secondary", type: "button" }, d.title || d.name);
      b.addEventListener("click", () => showExplain(i));
      nav.append(b);
    });
    if (_explainDocs.length) showExplain(0);
    else $("#explain-body").innerHTML = "<div class='muted-note'>No docs found in step_7/docs/.</div>";
  }
  function showExplain(i) {
    const d = _explainDocs[i];
    $("#explain-body").innerHTML = d ? mdToHtml(d.markdown) : "";
  }

  // Minimal, safe-enough markdown → HTML (headings, bold, inline code, fenced code, lists, paragraphs).
  function mdToHtml(md) {
    const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    const lines = (md || "").split("\n");
    let html = "", inCode = false, inList = false;
    for (let ln of lines) {
      if (ln.startsWith("```")) {
        if (inCode) { html += "</pre>"; inCode = false; }
        else { if (inList) { html += "</ul>"; inList = false; } html += "<pre style='background:var(--provincial-light);padding:.6em;border-radius:6px;overflow:auto;font-size:12px;'>"; inCode = true; }
        continue;
      }
      if (inCode) { html += esc(ln) + "\n"; continue; }
      let t = esc(ln).replace(/\*\*(.+?)\*\*/g, "<b>$1</b>").replace(/`([^`]+)`/g, "<code>$1</code>");
      if (/^#{1,6}\s/.test(ln)) {
        if (inList) { html += "</ul>"; inList = false; }
        const lvl = ln.match(/^#+/)[0].length;
        html += `<h${Math.min(lvl + 1, 5)} style='font-family:var(--font-head);color:var(--provincial);margin:.8em 0 .3em;'>${t.replace(/^#{1,6}\s/, "")}</h${Math.min(lvl + 1, 5)}>`;
      } else if (/^[-*]\s/.test(ln)) {
        if (!inList) { html += "<ul>"; inList = true; }
        html += "<li>" + t.replace(/^[-*]\s/, "") + "</li>";
      } else if (ln.trim() === "") {
        if (inList) { html += "</ul>"; inList = false; }
      } else {
        if (inList) { html += "</ul>"; inList = false; }
        html += "<p>" + t + "</p>";
      }
    }
    if (inList) html += "</ul>";
    if (inCode) html += "</pre>";
    return html;
  }

  // ==========================================================================
  // "HOW IT WORKS" tab — the pipeline as a flow + live counts
  // ==========================================================================
  async function renderPipeline() {
    // archive-derived narrative of his life, up top (the human point of all the machinery below)
    try {
      const hl = await api("/api/his_life");
      const host = $("#pipeline-detail-top") || (() => {
        const div = el("div", { id: "pipeline-detail-top" });
        $("#pipeline-stages").parentNode.insertBefore(div, $("#pipeline-stages"));
        return div;
      })();
      if (hl.markdown) {
        host.innerHTML = "";
        host.append(el("h3", { style: "color:var(--provincial);font-family:var(--font-head);margin:.2em 0 .4em;" },
          "The life this archive documents"));
        const body = el("div", { style: "font-size:14px;line-height:1.55;margin-bottom:1.2em;" });
        body.innerHTML = mdToHtml(hl.markdown);
        host.append(body);
      }
    } catch (e) { /* narrative optional */ }
    const flow = $("#pipeline-stages");
    const STAGES = [
      ["Scanned pages", "OCR'd page images (step 5)"],
      ["Documents", "segmented: letters + notebook entries (step 6)"],
      ["NER", "people, places, conditions, favors, dates extracted"],
      ["Resolve", "cautious merge → canonical entities (precision-first)"],
      ["Knowledge graph", "RiC-O temporal graph: entities + records + dates"],
      ["Embed + index", "hybrid BM25 + dense vectors; FTS; IIIF"],
      ["Ask", "tool-using agent answers with cited sources"],
    ];
    flow.innerHTML = "";
    STAGES.forEach((s, i) => {
      flow.append(el("div", { class: "step" }, el("b", {}, s[0]), s[1]));
      if (i < STAGES.length - 1) flow.append(el("div", { class: "arrow" }, "→"));
    });
    // live counts from the graph meta + cost ledger
    const stats = $("#pipeline-stats"); stats.innerHTML = "";
    try {
      const g = await api("/api/graph?limit=10");           // meta carries totals
      const m = g.meta || {};
      const kinds = m.node_kinds || {};
      const pairs = [
        [m.total_nodes ?? m.n_nodes, "graph nodes"],
        [m.total_edges ?? m.n_edges, "graph edges"],
        [kinds.person, "people"], [kinds.place, "places"],
        [kinds.organization, "organizations"], [kinds.condition, "conditions"],
        [kinds.favor, "favors"], [kinds.year, "years on the timeline"],
      ];
      for (const [n, l] of pairs) if (n != null)
        stats.append(el("div", { class: "stat" }, el("div", { class: "n" }, fmt(n)), el("div", { class: "l" }, l)));
    } catch { stats.append(el("div", { class: "muted-note" }, "graph not built yet")); }
    $("#pipeline-detail").innerHTML =
      "<p class='muted-note'>This is a developer & evaluation harness: you can swap the LLM, embedding " +
      "space, and reranker (left rail), toggle retrieval techniques, and watch the agent's trace + cost " +
      "for every question. Every answer cites the exact scanned region.</p>";
  }

  // ==========================================================================
  // "TOOLS & DATA" tab — each retrieval tool explained + corpus/eval/cost stats
  // ==========================================================================
  async function renderToolsTab() {
    const box = $("#tools-cards"); box.innerHTML = "";
    try {
      const r = await api("/api/tools");
      const tools = r.tools || r || [];
      for (const t of tools) {
        box.append(el("div", { class: "explain-card" },
          el("h4", {}, t.name),
          el("div", {}, t.description || "")));
      }
    } catch { box.append(el("div", { class: "muted-note" }, "tools registry unavailable")); }
    const ds = $("#data-stats"); ds.innerHTML = "";
    // (session API cost stat removed)
    ds.append(el("div", { class: "stat" }, el("div", { class: "n" }, "570"), el("div", { class: "l" }, "letters")));
    ds.append(el("div", { class: "stat" }, el("div", { class: "n" }, "8,657"), el("div", { class: "l" }, "notebook entries")));
  }

  function fmt(n) { return (typeof n === "number") ? n.toLocaleString() : n; }

  // (backend-connected status indicator removed per request — no header badge)

  // The config we use ONLY when /api/config is unreachable. Mirrors config.py so
  // the rail is never empty during development before server.py lands.
  const FALLBACK_CONFIG = {
    llms: ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.5-pro",
           "gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "claude-opus-4-8", "claude-sonnet-4-6"],
    embeddings: ["gemini-embedding-001@1536", "gemini-embedding-001@768",
                 "gemini-embedding-001@3072", "text-embedding-3-small@1536",
                 "text-embedding-3-large@3072", "voyage-4-large@1024", "voyage-4@1024",
                 "voyage-4-lite@1024", "bge-large-en-v1.5@1024"],
    rerankers: ["rerank-2.5", "rerank-2.5-lite", "rerank-4-pro", "bge-reranker-v2-m3"],
    providers: { llms: {}, embeddings: {}, rerankers: {} }, availability: {},
    defaults: { llm: "gemini-2.5-flash", embedding: "gemini-embedding-001@1536", reranker: "rerank-2.5" },
    tools: [
      { key: "use_bm25",        label: "BM25 (lexical)",          default: true,  paid: false, hint: "sparse keyword" },
      { key: "use_dense",       label: "Dense (semantic)",        default: true,  paid: false, hint: "vector search" },
      { key: "use_rerank",      label: "Rerank",                  default: false, paid: true,  hint: "cross-encoder" },
      { key: "use_hyde",        label: "HyDE",                    default: false, paid: true,  hint: "hypothetical doc" },
      { key: "use_multiquery",  label: "Multi-query (RAG-Fusion)", default: false, paid: true,  hint: "paraphrase + RRF" },
      { key: "use_llm_router",  label: "LLM router",              default: false, paid: true,  hint: "complexity routing" },
      { key: "auto_apply_route", label: "Auto-escalate route",    default: false, paid: true,  hint: "let router spend" },
    ],
  };

  // ==========================================================================
  // LEFT RAIL — model pickers + tool toggles, built from /api/config
  // ==========================================================================
  // provider/availability info from /api/config — drives availability marking + provider-match ordering
  let _cfgProv = { llms: {}, embeddings: {}, rerankers: {} };
  let _cfgAvail = {};
  const _provAvailable = (p) => _cfgAvail[p] !== false;   // null/unknown -> assume available (e.g. local)

  function buildModelPickers(cfg) {
    _cfgProv = cfg.providers || { llms: {}, embeddings: {}, rerankers: {} };
    _cfgAvail = cfg.availability || {};
    fillModelSelect($("#pick-llm"), cfg.llms, cfg.defaults.llm, _cfgProv.llms, null);
    refreshDependentPickers(cfg);
    // when the LLM changes, re-order the embedding + reranker options by provider match (the user's ask)
    $("#pick-llm").addEventListener("change", () => refreshDependentPickers(cfg));
  }

  // Embeddings are SCOPED to the chosen CHAT model: only embedding spaces from that LLM's provider
  // (plus always-available free `local`) are shown — choosing an OpenAI GPT hides Gemini/Voyage spaces.
  // Rerankers stay cross-compatible (they re-rank text regardless of provider), just ordered match-first.
  function refreshDependentPickers(cfg) {
    const llmProv = _cfgProv.llms[$("#pick-llm").value] || null;
    const compat = (cfg.embed_compat && cfg.embed_compat[llmProv]) || (llmProv ? [llmProv] : []);
    const allowed = new Set([...compat, "local"]);
    let embOpts = cfg.embeddings.filter((sp) => allowed.has(_cfgProv.embeddings[sp]));
    if (!embOpts.length) embOpts = cfg.embeddings;                 // never strand the user with an empty list
    const curEmb = $("#pick-embedding").value;
    const chosenEmb = embOpts.includes(curEmb) ? curEmb
      : (embOpts.includes(cfg.defaults.embedding) ? cfg.defaults.embedding : embOpts[0]);
    fillModelSelect($("#pick-embedding"), embOpts, chosenEmb, _cfgProv.embeddings, llmProv);
    fillModelSelect($("#pick-reranker"), cfg.rerankers,
                    $("#pick-reranker").value || cfg.defaults.reranker, _cfgProv.rerankers, llmProv);
  }

  // Fill a <select> with options, optionally ordered so the chosen LLM's provider comes first; options
  // whose provider has no API key are shown but disabled with a "(needs X key)" note.
  function fillModelSelect(sel, options, chosen, provMap, matchProvider) {
    provMap = provMap || {};
    let opts = [...options];
    if (matchProvider) {
      opts.sort((a, b) => {
        const ma = provMap[a] === matchProvider ? 0 : 1, mb = provMap[b] === matchProvider ? 0 : 1;
        if (ma !== mb) return ma - mb;                       // provider-match first
        const aa = _provAvailable(provMap[a]) ? 0 : 1, ab = _provAvailable(provMap[b]) ? 0 : 1;
        return aa - ab;                                      // then available before unavailable
      });
    }
    sel.innerHTML = "";
    for (const opt of opts) {
      const prov = provMap[opt];
      const avail = _provAvailable(prov);
      let label = opt;
      if (!avail) label += `  (needs ${prov} key)`;
      const o = el("option", { value: opt }, label);
      o.disabled = !avail;
      if (opt === chosen && avail) o.selected = true;
      sel.append(o);
    }
    if (sel.selectedIndex < 0 || (sel.options[sel.selectedIndex] && sel.options[sel.selectedIndex].disabled)) {
      const firstAvail = [...sel.options].find((o) => !o.disabled);
      if (firstAvail) firstAvail.selected = true;
    }
  }

  // Full, plain-English explanations of each retrieval technique (David asked for "complete
  // explanations of what exactly each thing does"). Keyed by the toggle key from /api/config.
  const TOGGLE_DESC = {
    use_bm25: "Sparse keyword search (BM25). Matches exact words, names, and dates literally. Free, local.",
    use_dense: "Semantic vector search in the chosen embedding space — finds passages by MEANING, not just shared words. Free with the local embedding model.",
    use_rerank: "Re-orders the top candidates with a cross-encoder for sharper relevance. The local reranker is free; hosted Voyage/Cohere is paid.",
    use_hyde: "HyDE — the LLM drafts a hypothetical answer, then retrieves passages similar to THAT (closer to the real wording than a terse question). One paid LLM call.",
    use_multiquery: "RAG-Fusion — rephrases your question several ways, retrieves each, and fuses results with Reciprocal Rank Fusion for better recall. Paid (one call per rephrase).",
    use_llm_router: "An LLM first judges the question's complexity and recommends a retrieval strategy. Paid.",
    auto_apply_route: "Let the router actually ESCALATE to the (paid) strategy it recommends — not just suggest it.",
  };

  function buildToggles(cfg) {
    const box = $("#toggles");
    box.innerHTML = "";
    STATE.toggles = {};
    for (const t of cfg.tools) {
      STATE.toggles[t.key] = !!t.default;
      // Each toggle is a button that flips a boolean. We render the on/off look with the brand classes
      // so state reads at a glance, flag paid tools with "$", and show a full explanation underneath.
      const labelText = t.label + (t.paid ? " $" : "");
      const head = el("span", {}, labelText);
      if (t.hint) head.append(el("span", { class: "hint" }, t.hint));
      const btn = el("button", { class: "toggle", type: "button" },
        head,
        el("span", { class: "pip" }, ""),
        el("span", { class: "desc" }, TOGGLE_DESC[t.key] || t.description || ""),
      );
      paintToggle(btn, STATE.toggles[t.key]);
      btn.addEventListener("click", () => {
        STATE.toggles[t.key] = !STATE.toggles[t.key];
        paintToggle(btn, STATE.toggles[t.key]);
      });
      box.append(btn);
    }
  }

  function paintToggle(btn, on) {
    btn.classList.toggle("toggle-on", on);
    btn.classList.toggle("toggle-off", !on);
    btn.querySelector(".pip").textContent = on ? "ON" : "off";
  }

  // Read the rail into the request body /api/ask expects. This is the ONE place
  // the UI's controls become the backend's RetrievalConfig.
  function currentRequest(query) {
    const kindsVal = $("#pick-kinds").value;
    const sys = $("#ask-system");
    // Conversational MEMORY: send the recent turns of the active conversation so the model can hold a
    // thread (follow-ups, "tell me more", "what about his sister?"). histRecord runs AFTER the response,
    // so _convo.turns here is exactly the PRIOR turns. Bounded to the last 6, answers only (no citations).
    const prior = (_convo && _convo.turns ? _convo.turns : []).slice(-6)
      .map((t) => ({ q: t.query, a: (t.resp && t.resp.answer) || "" }))
      .filter((t) => t.q || t.a);
    return {
      query,
      llm:       $("#pick-llm").value,
      embedding: $("#pick-embedding").value,   // "model@dim" — server splits it
      reranker:  $("#pick-reranker").value,
      kinds:     kindsVal ? [kindsVal] : null,
      toggles:   { ...STATE.toggles },
      system_prompt: sys ? (sys.value.trim() || null) : null,   // the chosen/edited persona
      history:   prior.length ? prior : null,                   // prior turns → conversational memory
      styled:    !!($("#voice-styled") && $("#voice-styled").checked),  // restyle into Solanus's voice
    };
  }

  // PERSONAS — the chat voice (default / archivist / first-person Solanus), editable in the UI.
  let _personas = [];
  async function loadPersonas() {
    const sel = $("#ask-persona"), box = $("#ask-system");
    if (!sel) return;
    try { _personas = (await api("/api/personas")).personas || []; }
    catch (e) { return; }
    sel.innerHTML = "";
    for (const p of _personas) sel.append(el("option", { value: p.key }, p.label));
    const apply = () => {
      const p = _personas.find((x) => x.key === sel.value);
      if (p && box) box.value = p.prompt;     // load the preset into the editable box
      // "His voice" (the reverse-desanitizer) is the point of the Solanus persona → default it on there,
      // off for the neutral assistant/archivist. The user can still override the checkbox by hand.
      const vs = $("#voice-styled");
      if (vs && !vs.dataset.touched) vs.checked = (sel.value === "solanus");
    };
    apply();                                  // seed with the default persona's prompt
    sel.addEventListener("change", apply);
    const vs = $("#voice-styled");
    if (vs) vs.addEventListener("change", () => { vs.dataset.touched = "1"; });
    const toggle = $("#persona-edit-toggle");
    if (toggle) toggle.addEventListener("click", () => {
      box.hidden = !box.hidden;
      toggle.textContent = box.hidden ? "edit prompt" : "hide prompt";
      if (!box.hidden) box.focus();
    });
  }

  // ==========================================================================
  // CHAT — ask a question, render a grounded answer with inline citations
  // ==========================================================================
  function wireChat() {
    const input = $("#ask-input"), btn = $("#ask-btn");
    const ask = () => {
      const q = input.value.trim();
      if (q) { input.value = ""; submitQuestion(q); }
    };
    if (input && btn) {
      btn.addEventListener("click", ask);
      input.addEventListener("keydown", (e) => { if (e.key === "Enter") ask(); });
    }
    // remember the opening welcome message so "clear chat" restores a clean slate
    const log = $("#chat-log");
    if (log && _chatWelcome === null) _chatWelcome = log.innerHTML;
    const clr = $("#chat-clear");
    if (clr) clr.addEventListener("click", () => {
      if (log) log.innerHTML = _chatWelcome || "";
      STATE.citations = [];
      STATE.lastAnswer = ""; STATE.lastUser = ""; setCaption("", "");   // clear voice/caption memory too
      const ts = $("#trace-steps"); if (ts) ts.innerHTML = "";   // also reset the under-the-hood trace
      const tc = $("#trace-cost"); if (tc) tc.innerHTML = "";
    });
  }
  let _chatWelcome = null;

  // ==========================================================================
  // VOICE I/O — mic dictation (STT) + read-aloud (TTS), with a PLUGGABLE voice
  // provider so David's Solanus voice-clone can drop in behind /api/tts later.
  // ==========================================================================
  function wireVoice() {
    const status = $("#voice-status");
    // AVATAR MODE — show the talking head in the chat and speak answers through it (lip-synced).
    // TTS turns on automatically; the mic (STT) stays an independent enable/disable choice.
    const avatarMode = $("#avatar-mode");
    if (avatarMode) {
      avatarMode.addEventListener("change", () => {
        const on = avatarMode.checked;
        const panel = $("#avatar-panel");
        const card = document.querySelector('.tab-panel[data-tab="ask"][aria-label="conversation"]');
        if (panel) panel.hidden = !on;
        if (card) card.classList.toggle("avatar-on", on);
        if (!on && window._avatarExitFs) window._avatarExitFs();   // leaving avatar mode also exits full screen
        if (!on && window.solanusAvatar && window.solanusAvatar.teardown) window.solanusAvatar.teardown();  // stop the render loop (no CPU leak)
        if (on) {
          if ($("#tts-on")) $("#tts-on").checked = true;          // answers are spoken aloud in avatar mode
          if ($("#tts-provider")) $("#tts-provider").value = "avatar";
          if (window.solanusAvatar) window.solanusAvatar.mount();
        }
        updateVoiceVis();
      });
    }
    // progressive disclosure: the spoken-voice picker only appears when answers are actually
    // spoken (read-aloud or avatar mode) — keeps the toolbar uncluttered by default.
    const ttsOnVis = $("#tts-on"), provField = $("#voice-provider-field");
    function updateVoiceVis() {
      const show = (ttsOnVis && ttsOnVis.checked) || (avatarMode && avatarMode.checked);
      if (provField) provField.hidden = !show;
    }
    if (ttsOnVis) ttsOnVis.addEventListener("change", updateVoiceVis);
    updateVoiceVis();
    // --- STT: dictate the question. Server providers (azure/gcp/local) record -> /api/stt; 'browser'
    //     falls back to the Web Speech API. Either way the final transcript auto-submits to the pipeline. ---
    const micBtn = $("#mic-btn");
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    const canRecord = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia && window.MediaRecorder);
    if (micBtn) {
      let active = null;
      const stopMic = () => { micBtn.classList.remove("listening"); micBtn.textContent = "use microphone"; active = null; };
      const ask = (q) => { q = (q || "").trim(); if (q) { const i = $("#ask-input"); if (i) i.value = ""; submitQuestion(q); } };
      micBtn.addEventListener("click", () => {
        if (active) { try { active.stop(); } catch (e) {} return; }
        const provider = ($("#tts-provider") || {}).value || "azure";
        const serverSTT = canRecord && provider !== "browser" && provider !== "avatar";
        if (serverSTT) {
          micBtn.classList.add("listening"); micBtn.textContent = "listening… (click to stop)";
          if (status) status.textContent = "";
          let stopped = false, mr = null;
          navigator.mediaDevices.getUserMedia({ audio: true }).then((stream) => {
            if (stopped) { stream.getTracks().forEach((t) => t.stop()); stopMic(); return; }
            const chunks = []; mr = new MediaRecorder(stream);
            mr.ondataavailable = (e) => { if (e.data && e.data.size) chunks.push(e.data); };
            mr.onstop = () => {
              stream.getTracks().forEach((t) => t.stop());
              micBtn.textContent = "transcribing…";
              const blob = new Blob(chunks, { type: mr.mimeType || "audio/webm" });
              fetch("/api/stt?provider=" + encodeURIComponent(provider), { method: "POST", headers: { "Content-Type": blob.type }, body: blob })
                .then((r) => r.json())
                .then((d) => { stopMic(); ask(d && d.text); })
                .catch(() => { stopMic(); if (status) status.textContent = "transcription failed"; });
            };
            mr.start();
          }).catch(() => { stopMic(); if (status) status.textContent = "mic blocked"; });
          active = { stop: () => { stopped = true; try { if (mr && mr.state !== "inactive") mr.stop(); } catch (e) {} } };
        } else if (SR) {
          const rec = new SR(); rec.lang = "en-US"; rec.interimResults = true; rec.continuous = false;
          micBtn.classList.add("listening"); micBtn.textContent = "listening… (click to stop)";
          if (status) status.textContent = "";
          rec.onresult = (e) => {
            const txt = Array.from(e.results).map((r) => r[0].transcript).join("");
            const input = $("#ask-input"); if (input) input.value = txt;
            if (e.results[e.results.length - 1].isFinal) ask(txt);
          };
          rec.onerror = (e) => { if (status) status.textContent = "mic: " + (e.error || "error"); };
          rec.onend = stopMic;
          active = rec; rec.start();
        } else if (status) { status.textContent = "no microphone support in this browser"; }
      });
    }
  }

  // speak `text` if read-aloud is on, via the chosen provider (browser speechSynthesis, or the server
  // /api/tts voice model). The server path is the slot for the trained Solanus voice; until it's wired it
  // returns 501 and we fall back to the browser voice with a note.
  // Prepare answer text for SPEECH (TTS + avatar): drop citation numbers and de-mangle the things TTS
  // reads weirdly — religious-title abbreviations (Fr./Sr./Msgr.…), number ranges read as "minus", dashes.
  function cleanForSpeech(text) {
    let t = String(text || "");
    // citation markers: [1], [1, 2], [1-3], [1 & 2]  — never spoken
    t = t.replace(/\[\s*\d+(?:\s*[,&–-]\s*\d+)*\s*\]/g, "");
    // also any "(see [1])"-style leftovers and stray double spaces around removed marks
    t = t.replace(/\(\s*\)/g, "");
    // expand abbreviations TTS mispronounces (this corpus is full of them)
    const ABBR = [
      [/\bFr\.\s/g, "Father "], [/\bFrs\.\s/g, "Fathers "], [/\bSr\.\s/g, "Sister "], [/\bSrs\.\s/g, "Sisters "],
      [/\bBr\.\s/g, "Brother "], [/\bMsgr\.\s/g, "Monsignor "], [/\bRev\.\s/g, "Reverend "],
      [/\bVen\.\s/g, "Venerable "], [/\bBl\.\s/g, "Blessed "], [/\bMt\.\s/g, "Mount "],
      [/\bSts\.\s/g, "Saints "], [/\bAve\.\s/g, "Avenue "], [/\bBlvd\.\s/g, "Boulevard "], [/\bRd\.\s/g, "Road "],
      [/\bSt\.\s(?=[A-Z])/g, "Saint "],   // St. Felix / St. Louis — in spoken answers this is ~always "Saint"
    ];
    for (const [re, rep] of ABBR) t = t.replace(re, rep);
    // number ranges read as subtraction → say "to" (1925-1928, pages 12-15)
    t = t.replace(/(\d)\s*[–—-]\s*(\d)/g, "$1 to $2");
    // remaining em/en dashes between clauses → a comma pause (so it isn't read "dash")
    t = t.replace(/\s*[—–]\s*/g, ", ");
    t = t.replace(/\s+([.,;:!?])/g, "$1");   // a removed citation can leave " ." → tidy to "."
    return t.replace(/\s{2,}/g, " ").trim();
  }

  function maybeSpeak(text) {
    const on = $("#tts-on"); if (!on || !on.checked || !text) return;
    const provider = ($("#tts-provider") || {}).value || "azure";
    const status = $("#voice-status");
    const clean = cleanForSpeech(text).slice(0, 4000);   // drop citations + de-mangle dates/titles for speech
    if (provider === "avatar") {
      if (window.solanusAvatar) { window.solanusAvatar.speak(clean); if (status) status.textContent = ""; }
      else { if (status) status.textContent = "avatar not loaded — open the Avatar tab once."; speakBrowser(clean); }
    } else if (provider === "browser") {
      speakBrowser(clean);
    } else {                                // server voice: azure | gcp | local (Piper)
      if (status) status.textContent = "speaking…";
      fetch("/api/tts", { method: "POST", headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ text: clean, provider, voice: window.SOLANUS_TTS_VOICE || undefined }) })
        .then((r) => { if (!r.ok) throw new Error("tts " + r.status); return r.blob(); })
        .then((blob) => { new Audio(URL.createObjectURL(blob)).play(); if (status) status.textContent = ""; })
        .catch(() => { if (status) status.textContent = "server voice unavailable — using browser voice."; speakBrowser(clean); });
    }
  }
  function speakBrowser(text) {
    if (!window.speechSynthesis) return;
    window.speechSynthesis.cancel();
    const u = new SpeechSynthesisUtterance(text);
    u.rate = 0.95; u.pitch = 1.0;
    window.speechSynthesis.speak(u);
  }

  // ==========================================================================
  // CHAT HISTORY — local (localStorage) now; a cloud store can replace _histSave/_histLoadAll later.
  // ==========================================================================
  const HIST_KEY = "solanus_chat_history_v1";
  let _history = [];        // [{id, title, ts, turns:[{query, resp}]}]
  let _convo = null;        // the active conversation
  function _histLoadAll() { try { _history = JSON.parse(localStorage.getItem(HIST_KEY)) || []; } catch { _history = []; } }
  function _histSave() { try { localStorage.setItem(HIST_KEY, JSON.stringify(_history.slice(-50))); } catch { /* quota */ } }

  function _newConvo() { _convo = { id: "c" + Date.now() + Math.floor(performance.now()), title: "", ts: Date.now(), turns: [] }; }
  // Persist a SLIM copy: citations carry per-region polygon `vertices` + full page text that, across 50
  // conversations, overrun the localStorage quota (and _histSave silently swallows the failure, so the
  // whole history would just stop saving). Drop what openCitation re-fetches from /api/region anyway.
  function _slimResp(resp) {
    if (!resp) return resp;
    const cites = (resp.citations || []).map((c) => ({
      n: c.n, doc_id: c.doc_id, rid: c.rid, kind: c.kind, section: c.section,
      page: c.page, pdf_page: c.pdf_page, score: c.score, min_conf: c.min_conf,
      snippet: (c.snippet || "").slice(0, 240),
    }));
    return { answer: resp.answer, answer_plain: resp.answer_plain, styled: resp.styled,
             voice_note: resp.voice_note, voice_consistent: resp.voice_consistent,
             grade: resp.grade, trace: resp.trace, citations: cites };
  }
  function histRecord(query, resp) {
    if (!_convo) _newConvo();
    _convo.turns.push({ query, resp: _slimResp(resp) });
    if (!_convo.title) _convo.title = query.slice(0, 48);
    if (!_history.find((c) => c.id === _convo.id)) _history.push(_convo);
    _convo.ts = Date.now();
    _histSave(); _histRefreshSelect(); renderRailHistory();
  }
  function _histRefreshSelect() {
    const sel = $("#chat-history"); if (!sel) return;
    sel.innerHTML = '<option value="">history…</option>';
    for (const c of [..._history].reverse()) {
      if (!c.turns || !c.turns.length) continue;
      const d = new Date(c.ts);
      sel.append(el("option", { value: c.id }, `${c.title || "(untitled)"} · ${d.toLocaleDateString()}`));
    }
  }
  function histOpen(id) {
    const c = _history.find((x) => x.id === id); if (!c) return;
    _convo = c;
    const log = $("#chat-log"); if (log) log.innerHTML = _chatWelcome || "";
    for (const t of c.turns) {
      appendMessage("user", el("div", {}, t.query));
      try { renderAnswer(t.resp); renderTrace(t.resp.trace); } catch { /* tolerate old shapes */ }
    }
  }
  function histNewChat() {
    _newConvo();
    const log = $("#chat-log"); if (log) log.innerHTML = _chatWelcome || "";
    STATE.citations = []; STATE.lastAnswer = ""; STATE.lastUser = ""; setCaption("", "");   // reset voice/caption memory
    const ts = $("#trace-steps"); if (ts) ts.innerHTML = "";
    const tc = $("#trace-cost"); if (tc) tc.innerHTML = "";
    const sel = $("#chat-history"); if (sel) sel.value = "";
  }
  // the sidebar conversation list (ChatGPT/Claude style)
  function renderRailHistory() {
    const host = $("#rail-history"); if (!host) return;
    host.innerHTML = "";
    const convos = [..._history].reverse().filter((c) => c.turns && c.turns.length);
    if (!convos.length) { host.append(el("div", { class: "rail-empty" }, "no conversations yet")); return; }
    for (const c of convos) {
      const row = el("div", { class: "hist-row" });
      const b = el("button", { class: "hist-item" + (_convo && c.id === _convo.id ? " active" : ""),
                               title: c.title || "(untitled)" }, c.title || "(untitled)");
      b.addEventListener("click", () => { histOpen(c.id); renderRailHistory(); showTab("ask"); });
      const del = el("button", { class: "hist-del", title: "delete this conversation", "aria-label": "delete conversation" }, "×");
      del.addEventListener("click", (e) => {
        e.stopPropagation();
        const wasActive = _convo && _convo.id === c.id;
        _history = _history.filter((x) => x.id !== c.id);
        _histSave();
        if (wasActive) histNewChat();
        renderRailHistory(); _histRefreshSelect();
      });
      row.append(b, del);
      host.append(row);
    }
  }
  function wireHistory() {
    _histLoadAll(); _newConvo(); _histRefreshSelect(); renderRailHistory();
    const sel = $("#chat-history"); if (sel) sel.addEventListener("change", () => sel.value && histOpen(sel.value));
    const nw = $("#chat-new"); if (nw) nw.addEventListener("click", () => { histNewChat(); renderRailHistory(); });
    const rn = $("#rail-newchat"); if (rn) rn.addEventListener("click", () => { histNewChat(); renderRailHistory(); showTab("ask"); });
    const rt = $("#rail-toggle"); if (rt) rt.addEventListener("click", () => { const r = $("#side-rail"); if (r) r.classList.toggle("collapsed"); });
  }

  // ==========================================================================
  // LIVE PIPELINE — Drawflow flow-node view; nodes light active/inactive per query (from the trace+toggles).
  // ==========================================================================
  let _flowEd = null, _flowStageId = {}, _flowReady = false, _lastFlow = null;
  const _FLOW_SUB = { io: "in / out", control: "routing", retrieval: "retrieval", rerank: "re-rank",
                      llm: "generation", model: "model", output: "output" };
  // plain-language role for each pipeline node — shown when you click it
  const _FLOW_ROLE = {
    question: "Your question, as typed. Entry point; family terms like “his brothers” are expanded to real names before retrieval.",
    router: "Judges the question’s complexity and picks a retrieval strategy. Heuristic by default; the paid LLM router (toggle) reasons about it explicitly.",
    bm25: "Sparse lexical search — matches exact words, names, and dates. Free, local.",
    dense: "Dense semantic search — finds passages by meaning in the embedding space. Free with the local model.",
    multiquery: "RAG-Fusion — rephrases your question several ways, retrieves each, fuses with Reciprocal Rank Fusion. Paid.",
    hyde: "HyDE — drafts a hypothetical answer, then retrieves passages similar to that draft. Paid.",
    rerank: "Cross-encoder re-orders the top candidates for sharper relevance. Local is free; hosted is paid.",
    synthesize: "Writes the grounded, cited answer from the retrieved passages.",
    answer: "The final cited prose returned to you (with clickable [n] sources).",
    tts: "Text-to-speech — reads the answer aloud when voice is on.",
    avatar: "Drives the in-browser 3D Solanus head (lip-sync) when avatar mode is on.",
    llm: "The generation model used to synthesize the answer.",
    embed: "The embedding model that powers dense search.",
    reranker_model: "The cross-encoder model used by Rerank.",
  };
  let _flowIdStage = {};   // reverse of _flowStageId: drawflow node id -> stage id (for click lookup)

  // The live pipeline is an OPTIONAL, detached, draggable floating window (toggled in the composer).
  function wirePipelinePanel() {
    const panel = $("#flow-panel"), toggle = $("#pipeline-on");
    if (!panel || !toggle) return;
    const KEY = "solanus_pipeline_open", POS = "solanus_pipeline_pos";
    // The pipeline toggle opens a REAL OS window directly (drag it to another monitor). It stays live via
    // BroadcastChannel — pipeline.html 'hello'-replays the current state on load. The in-page #flow-panel
    // is kept ONLY as a fallback for when the browser blocks the pop-up.
    let pipeWin = null;
    const openWindow = () => {
      if (pipeWin && !pipeWin.closed) { try { pipeWin.focus(); } catch (_) {} return true; }
      pipeWin = window.open("/static/pipeline.html", "solanus_pipeline", "width=980,height=720");
      if (!pipeWin) return false;                 // blocked → caller falls back to the in-page panel
      try { pipeWin.focus(); } catch (_) {}
      setTimeout(() => setPipelineState(_lastFlowState.active, _lastFlowState.running), 150);
      try { localStorage.setItem(KEY, "1"); } catch (_) {}
      return true;
    };
    const show = () => {                          // in-page fallback (pop-up blocked)
      panel.hidden = false;
      initPipelineFlow();                         // build Drawflow now that the host has a real size
      setTimeout(() => { if (_flowEd) { try { _flowEd.zoom_refresh(); } catch (_) {} } applyTraceToFlow(); }, 60);
    };
    const hide = () => {
      panel.hidden = true; toggle.checked = false;
      try { if (pipeWin && !pipeWin.closed) pipeWin.close(); } catch (_) {} pipeWin = null;
      try { localStorage.setItem(KEY, "0"); } catch (_) {}
    };
    toggle.addEventListener("change", () => {
      if (!toggle.checked) { hide(); return; }
      if (!openWindow()) show();                   // real window; in-page panel only if the pop-up is blocked
    });
    const closeBtn = $("#flow-panel-close"); if (closeBtn) closeBtn.addEventListener("click", hide);

    // node-size zoom (+/-) — Drawflow scales the whole canvas, so spacing stays clean (in-page fallback only)
    const zin = $("#flow-zoom-in"), zout = $("#flow-zoom-out"), zfit = $("#flow-zoom-fit");
    if (zin) zin.addEventListener("click", () => { if (_flowEd) _flowEd.zoom_in(); });
    if (zout) zout.addEventListener("click", () => { if (_flowEd) _flowEd.zoom_out(); });
    if (zfit) zfit.addEventListener("click", () => { if (_flowEd) { _flowEd.zoom = 1; _flowEd.canvas_x = 0; _flowEd.canvas_y = 0; _flowEd.zoom_refresh(); } });

    // drag the window by its title bar; remember where the user puts it
    const head = $("#flow-panel-head");
    const applyPos = (x, y) => {
      const w = panel.offsetWidth, h = panel.offsetHeight;
      x = Math.max(0, Math.min(window.innerWidth - Math.min(w, 120), x));
      y = Math.max(0, Math.min(window.innerHeight - 44, y));
      panel.style.left = x + "px"; panel.style.top = y + "px"; panel.style.transform = "none";
    };
    try { const p = JSON.parse(localStorage.getItem(POS) || "null"); if (p) applyPos(p.x, p.y); } catch (_) {}
    let dragging = false, dx = 0, dy = 0;
    const onMove = (e) => { if (!dragging) return; e.preventDefault(); applyPos(e.clientX - dx, e.clientY - dy); };
    const onUp = () => { if (!dragging) return; dragging = false; document.body.style.userSelect = "";
      const r = panel.getBoundingClientRect();
      try { localStorage.setItem(POS, JSON.stringify({ x: r.left, y: r.top })); } catch (_) {}
      window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
    if (head) head.addEventListener("pointerdown", (e) => {
      if (e.target.closest("button")) return;            // don't drag when hitting a tool button
      const r = panel.getBoundingClientRect(); dx = e.clientX - r.left; dy = e.clientY - r.top;
      dragging = true; document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });

    // Do NOT auto-open on load — window.open needs a user gesture, so the toggle starts unchecked and the
    // user opens the pipeline window with a click. (#flow deep-link / restore can't spawn a window silently.)
  }

  async function initPipelineFlow() {
    const host = $("#pipeline-flow");
    if (!host) return;
    if (_flowReady) { applyTraceToFlow(); return; }
    if (typeof Drawflow === "undefined") {
      host.innerHTML = "<p class='muted-note' style='padding:1em'>flow library failed to load.</p>"; return;
    }
    let g;
    try { g = await api("/api/agent_graph"); }
    catch { host.innerHTML = "<p class='muted-note' style='padding:1em'>pipeline graph unavailable.</p>"; return; }
    const ed = new Drawflow(host); ed.reroute = false; ed.start(); ed.editor_mode = "fixed";  // pan canvas + wheel-zoom; nodes stay put
    _flowStageId = {}; _flowIdStage = {};
    const SX = 1.12, SY = 1.32;   // spread coords out so the bigger/more-legible nodes don't overlap
    for (const n of g.nodes) {
      const html = `<div class="pnode"><b>${n.label}</b><span class="pnode-sub">${_FLOW_SUB[n.kind] || n.kind}</span></div>`;
      const nid = ed.addNode(n.id, 1, 1, Math.round(n.x * SX), Math.round(n.y * SY), "kind-" + n.kind + " st-inactive", {}, html, false);
      _flowStageId[n.id] = nid; _flowIdStage[nid] = n.id;
    }
    for (const e of g.edges) {
      const a = _flowStageId[e.from], b = _flowStageId[e.to];
      if (a != null && b != null) { try { ed.addConnection(a, b, "output_1", "input_1"); } catch (_) { /* ok */ } }
    }
    // ONE delegated click listener (survives re-render; independent of Drawflow's view-mode events):
    // click a node → a small panel explaining what that stage actually is + its real numbers this query.
    host.addEventListener("click", (e) => {
      const dom = e.target.closest(".drawflow-node");
      if (!dom) { closeNodePanel(); return; }
      const stage = _flowIdStage[Number((dom.id || "").replace("node-", ""))];
      if (stage) showNodePanel(stage, dom);
    });
    _flowEd = ed; _flowReady = true;
    applyTraceToFlow();
  }
  function setFlowNodeState(stage, state) {
    const id = _flowStageId[stage]; if (id == null) return;
    const dom = document.getElementById("node-" + id); if (!dom) return;
    dom.classList.remove("st-active", "st-running", "st-inactive", "st-error");
    dom.classList.add("st-" + state);
  }
  // ONE place that sets the whole pipeline state: paints the in-page nodes (if built) AND broadcasts to
  // any popped-out window over a BroadcastChannel, so a pipeline opened on another monitor stays live.
  let _lastFlowState = { active: [], running: [] };
  const _flowChan = ("BroadcastChannel" in window) ? new BroadcastChannel("solanus-pipeline") : null;
  if (_flowChan) _flowChan.onmessage = (e) => {                 // a fresh pop-out says hello → replay state
    if (e.data && e.data.type === "hello") _flowChan.postMessage({ type: "state", ..._lastFlowState });
  };
  function setPipelineState(active, running) {
    _lastFlowState = { active: active || [], running: running || [] };
    if (_flowReady) {
      Object.keys(_flowStageId).forEach((k) => setFlowNodeState(k, "inactive"));
      _lastFlowState.active.forEach((s) => setFlowNodeState(s, "active"));
      _lastFlowState.running.forEach((s) => setFlowNodeState(s, "running"));
    }
    if (_flowChan) _flowChan.postMessage({ type: "state", active: _lastFlowState.active, running: _lastFlowState.running });
  }
  // LIVE progress from the SSE stream: backend sends cumulative `active` + the currently `running` stage.
  function applyStageEvent(ev) { setPipelineState(ev.active, ev.running); }
  // light nodes for the LAST query: enabled toggles + route/multi-query from the trace decide what fired.
  function applyTraceToFlow() {
    const f = _lastFlow; if (!f) { setPipelineState([], []); return; }
    const t = f.toggles || {}, tr = f.trace || {}, active = ["question"];
    if (t.use_bm25) active.push("bm25");
    if (t.use_dense !== false) active.push("dense", "embed");
    if (t.use_hyde) active.push("hyde");
    if (t.use_multiquery || (tr.queries_used && tr.queries_used.length > 1)) active.push("multiquery");
    if (t.use_rerank) active.push("rerank", "reranker_model");
    if (t.use_llm_router || /LLM-routed/.test((tr.route && tr.route.strategy) || "")) active.push("router");
    if (f.avatarOn) active.push("avatar");
    active.push("synthesize", "llm", "answer");
    if (f.speak) active.push("tts");
    setPipelineState(active, []);
  }

  // At query START: pulse the likely-firing nodes "running" so the pipeline feels live (the SSE `stage`
  // events then refine it). Uses the current toggles.
  function flowRunning() {
    const t = STATE.toggles || {}, running = ["question"];
    if (t.use_bm25 !== false) running.push("bm25");
    if (t.use_dense !== false) running.push("dense", "embed");
    if (t.use_rerank) running.push("rerank", "reranker_model");
    running.push("synthesize", "llm");
    setPipelineState([], running);
  }

  // Click a pipeline node → a small panel: what that stage IS + the real numbers from your last query.
  let _flowPop = null;
  function closeNodePanel() { if (_flowPop) { _flowPop.remove(); _flowPop = null; } }
  function nodeFacts(stage) {
    const f = _lastFlow; if (!f) return [];
    const tr = f.trace || {}, steps = tr.steps || [];
    const retr = steps.find((s) => /retriev/i.test(s.tool || ""));
    const syn = steps.find((s) => /synth/i.test(s.tool || ""));
    const out = [];
    const dom = document.getElementById("node-" + _flowStageId[stage]);
    out.push(["status", dom && dom.classList.contains("st-active") ? "fired this query" : "not used this query"]);
    if (["bm25", "dense", "multiquery", "hyde", "rerank"].includes(stage) && retr) {
      if (retr.items != null) out.push(["passages kept", retr.items]);
      if (retr.ms != null) out.push(["retrieval", retr.ms + " ms"]);
    }
    if (stage === "multiquery" && tr.queries_used) out.push(["phrasings fused", tr.queries_used.length]);
    if (stage === "router" && tr.route) out.push(["decision", `${tr.route.complexity || "?"} → ${tr.route.strategy || ""}`]);
    if (stage === "synthesize" || stage === "llm") {
      if (syn && syn.items != null) out.push(["citations", syn.items]);
      if (syn && syn.detail) out.push(["detail", syn.detail]);
      if (tr.cost && tr.cost.usd != null) out.push(["cost", `$${Number(tr.cost.usd).toFixed(6)}`]);
    }
    return out;
  }
  function showNodePanel(stage, domNode) {
    closeNodePanel();
    const facts = nodeFacts(stage);
    const rows = facts.map(([k, v]) => el("div", { class: "fp-row" },
      el("span", { class: "fp-k" }, k), el("span", { class: "fp-v" }, String(v))));
    _flowPop = el("div", { class: "flow-pop", role: "dialog" },
      el("button", { class: "fp-x", title: "close", onclick: closeNodePanel }, "×"),
      el("div", { class: "fp-title" }, (domNode.querySelector(".pnode b") || {}).textContent || stage),
      el("div", { class: "fp-role" }, _FLOW_ROLE[stage] || ""),
      rows.length ? el("div", { class: "fp-facts" }, ...rows)
                  : el("div", { class: "fp-role muted-note" }, "Ask a question, then click again to see this stage’s live numbers."));
    const host = $("#pipeline-flow"), hb = host.getBoundingClientRect(), nb = domNode.getBoundingClientRect();
    if (getComputedStyle(host).position === "static") host.style.position = "relative";
    _flowPop.style.cssText = `position:absolute; z-index:30; left:${Math.max(4, Math.min(nb.left - hb.left, hb.width - 264))}px; top:${nb.bottom - hb.top + 8}px;`;
    host.appendChild(_flowPop);
  }
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeNodePanel(); });

  // ==========================================================================
  // VOICE-MODE COMMANDS — when you're *talking to* Solanus (avatar or read-aloud on),
  // certain meta-asks act on the LAST answer instead of triggering a new retrieval:
  //   "which sources did you use" · "summarize the sources" · "open source 2" /
  //   "pull up that page" · "say that again". Gated on avatar/tts being on, and the
  //   patterns require self-reference, so normal typed questions are never hijacked.
  // ==========================================================================
  const _WORDNUM = { one:1, two:2, three:3, four:4, five:5, six:6, seven:7, eight:8, nine:9, ten:10,
                     first:1, second:2, third:3, fourth:4, fifth:5, sixth:6, seventh:7, eighth:8, ninth:9, tenth:10, last:-1 };
  function _ordinal(t) {
    const d = t.match(/\b(\d{1,2})\b/); if (d) return Number(d[1]);
    for (const w in _WORDNUM) if (new RegExp("\\b" + w + "\\b").test(t)) return _WORDNUM[w];
    return 0;
  }
  function voiceReply(text) {
    appendMessage("bot", el("div", {}, text));
    setCaption(null, text);                            // reflect the reply in the full-screen caption
    const clean = cleanForSpeech(text);
    const avatarOn = $("#avatar-mode") && $("#avatar-mode").checked;
    const ttsOn = $("#tts-on") && $("#tts-on").checked;
    if (avatarOn && window.solanusAvatar) window.solanusAvatar.speak(clean);
    else if (ttsOn) maybeSpeak(text);
    else speakBrowser(clean);
  }
  function handleVoiceCommand(query) {
    const voiceMode = ($("#avatar-mode") && $("#avatar-mode").checked) ||
                      ($("#tts-on") && $("#tts-on").checked);
    if (!voiceMode) return false;                      // only intercept while "talking to" Solanus
    const t = String(query || "").toLowerCase().trim().replace(/[.?!]+$/, "");
    const cites = STATE.citations || [];
    let num = _ordinal(t);
    if (num === -1) num = cites.length;                // "the last source"

    // OPEN / pull up a cited page
    if (/\b(open|show|pull up|bring up|go to|view|display|read)\b/.test(t) &&
        (/\b(source|citation|cite|reference|footnote)s?\b/.test(t) || (/\bpage\b/.test(t) && num))) {
      if (!cites.length) { voiceReply("There are no sources yet — ask a question first, then I can open one."); return true; }
      const c = cites.find((x) => x.n === num) || cites[0];
      voiceReply(`Opening source ${c.n}: ${sourceLabel(c)}.`);
      openCitation(c); return true;
    }
    // WHICH / LIST sources (requires self-reference so "which sources mention Detroit" still retrieves)
    if (/\bsources?\b/.test(t) && /\b(which|what|list|name|tell me)\b/.test(t) &&
        /\b(you|these|those|use|used|cited|did|were|are they)\b/.test(t)) {
      if (!cites.length) { voiceReply("I haven't cited any sources yet."); return true; }
      const labels = cites.map((c) => `${c.n}, ${sourceLabel(c)}`).join("; ");
      voiceReply(`I drew on ${cites.length} source${cites.length > 1 ? "s" : ""}: ${labels}.`); return true;
    }
    // SUMMARIZE the cited sources (client-side synopsis of the snippets)
    if (/\bsummar/.test(t) && /\bsources?\b/.test(t)) {
      if (!cites.length) { voiceReply("No sources to summarize yet."); return true; }
      const parts = cites.map((c) => `Source ${c.n}, ${sourceLabel(c)}: ${truncate(c.snippet || "", 140)}`);
      voiceReply(`Here is what the ${cites.length} cited source${cites.length > 1 ? "s" : ""} say. ` + parts.join(". ")); return true;
    }
    // REPEAT the last answer
    if (/\b(repeat( that| it| the answer)?|say (that|it) again|read (that|it) again|come again|what did you (just )?say)\b/.test(t)) {
      if (!STATE.lastAnswer) { voiceReply("I haven't said anything yet — ask me a question first."); return true; }
      voiceReply(STATE.lastAnswer); return true;
    }
    return false;
  }

  async function submitQuestion(query) {
    if (handleVoiceCommand(query)) return;             // voice meta-commands act on the last answer
    flowRunning();                                      // pulse the live-pipeline nodes while the query runs
    STATE.lastUser = query; setCaption(query, "");     // full-screen caption: show Q, clear A while waiting
    if ($("#stream-on") && $("#stream-on").checked) { return submitQuestionStream(query); }
    appendMessage("user", el("div", {}, query));
    const thinking = appendMessage("bot",
      el("div", {}, el("span", { class: "spinner" }), " retrieving + grading…"));

    try {
      const resp = await api("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(currentRequest(query)),
      });
      thinking.remove();
      renderAnswer(resp);
      renderTrace(resp.trace);
      histRecord(query, resp);          // persist this turn (local chat history)
      maybeSpeak(resp.answer);          // read the answer aloud if enabled
      _lastFlow = { trace: resp.trace, toggles: { ...STATE.toggles },   // light up the live-pipeline view
                    speak: !!($("#tts-on") && $("#tts-on").checked),
                    avatarOn: !!($("#avatar-mode") && $("#avatar-mode").checked) };
      applyTraceToFlow();
    } catch (err) {
      thinking.remove();
      appendMessage("bot", el("div", {},
        el("strong", {}, "Couldn't reach the answer service. "),
        el("span", { class: "muted-note" },
          "Start the backend (app/server.py) — then your question routes through " +
          "lib/retrieval.route_and_retrieve and returns cited, graded prose. (" + err.message + ")")));
    }
  }

  // STREAMING path (toggle #stream-on): SSE over fetch — show tokens live, then re-render rich (clickable
  // [n] chips + sources) on done. Falls back to a clear error if the stream breaks mid-flight.
  async function submitQuestionStream(query) {
    appendMessage("user", el("div", {}, query));
    const botMsg = appendMessage("bot", el("div", {}, el("span", { class: "spinner" }), " retrieving…"));
    const resp = { answer: "", citations: [], grade: {}, trace: {} };
    let acc = "", body = null;
    try {
      const res = await fetch("/api/ask_stream", { method: "POST",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(currentRequest(query)) });
      if (!res.ok || !res.body) throw new Error("HTTP " + res.status);
      botMsg.innerHTML = ""; body = el("div", { class: "stream-body" }); botMsg.append(body);
      const reader = res.body.getReader(), dec = new TextDecoder();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read(); if (done) break;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const line = buf.slice(0, i).trim(); buf = buf.slice(i + 2);
          if (!line.startsWith("data:")) continue;
          let ev; try { ev = JSON.parse(line.slice(5).trim()); } catch { continue; }
          if (ev.type === "stage") { applyStageEvent(ev); }
          else if (ev.type === "meta") { resp.citations = ev.citations || []; resp.grade = ev.grade || {}; resp.trace = ev.trace || {}; }
          else if (ev.type === "token") { acc += ev.text; body.textContent = acc;
            const log = $("#chat-log"); if (log) log.scrollTop = log.scrollHeight; }
          else if (ev.type === "done") {
            resp.answer = ev.answer || acc;
            // voice layer: the streamed tokens were the PLAIN reply; `done` may swap in the styled text.
            if (ev.styled) { resp.styled = true; resp.answer_plain = ev.answer_plain || acc;
                             resp.voice_note = ev.voice_note; resp.voice_consistent = ev.voice_consistent; }
          }
          else if (ev.type === "error") { throw new Error(ev.detail || "stream error"); }
        }
      }
      resp.answer = resp.answer || acc;
      botMsg.remove();                          // replace the live text with the rich, cited render
      renderAnswer(resp); renderTrace(resp.trace);
      histRecord(query, resp); maybeSpeak(resp.answer);
      _lastFlow = { trace: resp.trace, toggles: { ...STATE.toggles },
                    speak: !!($("#tts-on") && $("#tts-on").checked),
                    avatarOn: !!($("#avatar-mode") && $("#avatar-mode").checked) };
      applyTraceToFlow();
    } catch (err) {
      if (body) body.append(el("span", { class: "muted-note" }, "  [stream error: " + err.message + "]"));
      else { botMsg.remove(); appendMessage("bot", el("div", {},
        el("strong", {}, "Stream failed. "), el("span", { class: "muted-note" }, err.message))); }
    }
  }

  function appendMessage(role, contentNode) {
    const log = $("#chat-log");
    const msg = el("div", { class: "msg " + role });
    msg.append(contentNode);
    if (log) { log.append(msg); log.scrollTop = log.scrollHeight; }
    return msg;
  }

  // Render the grounded answer: prose with clickable [n] markers, a grade badge,
  // and a "sources" list mirroring the inline markers. Each citation knows its
  // exact region, so clicking it opens the deep-zoom modal.
  function renderAnswer(resp) {
    // Capture THIS turn's citations once. The swap handler below re-renders later (deferred), and by
    // then the global STATE.citations may belong to a newer turn — so bind to `cites`, not the global,
    // or an older message's [n] chips would map onto the wrong sources.
    const cites = resp.citations || [];
    STATE.citations = cites;
    STATE.lastAnswer = resp.answer || "";              // for voice "repeat"/"summarize sources"
    setCaption(STATE.lastUser || "", resp.answer || "");   // keep the full-screen caption current
    const body = el("div", {});

    // Turn the answer string's [n] markers into clickable footnote chips. We
    // build text + chips as DOM nodes (never innerHTML the model output — that
    // would be an XSS hole; the model's text is untrusted).
    let answerNode = renderAnswerText(resp.answer || "", cites);
    body.append(answerNode);

    // VOICE LAYER: when the answer was restyled into Solanus's voice, say so honestly (it's an
    // impression, not his literal words) and offer a one-click swap to the plain grounded answer that
    // produced it — the plain text rides along in `answer_plain` for exactly this.
    if (resp.styled && resp.answer_plain && resp.answer_plain !== resp.answer) {
      let showingPlain = false;
      const swap = el("button", { class: "link-btn", type: "button", style: "margin-left:.5em;" }, "show plain answer");
      swap.addEventListener("click", () => {
        showingPlain = !showingPlain;
        const t = showingPlain ? resp.answer_plain : resp.answer;
        answerNode.replaceWith(answerNode = renderAnswerText(t, cites));
        swap.textContent = showingPlain ? "show his voice" : "show plain answer";
        answerNode.after(note);
      });
      const note = el("div", { class: "muted-note voice-note", style: "margin:.35em 0 .1em; font-size:12px;" },
        el("em", {}, resp.voice_note || "Rendered in Fr. Solanus's voice — a faithful impression, not his literal words."),
        " ", swap);
      answerNode.after(note);
    }

    // Grade badge (Self-RAG/CRAG verdict) — tells the dev whether the system
    // trusted its own context (answer / caveat / abstain) and how confidently.
    if (resp.grade) {
      const g = resp.grade;
      const conf = (g.confidence != null) ? ` (${Math.round(g.confidence * 100)}%)` : "";
      body.append(el("div", { class: "grade " + (g.verdict || "") },
        `verdict: ${g.verdict}${conf}${g.reason ? " — " + g.reason : ""}`));
    }

    // Sources list — one row per citation, mirroring the [n] markers above.
    if (cites.length) {
      const list = el("div", { class: "sources" });
      list.append(el("div", { class: "section-title", style: "font-size:12px;" }, "Sources"));
      for (const c of cites) {
        const row = el("div", { class: "source-item", onclick: () => openCitation(c) },
          el("span", { class: "num" }, `[${c.n}] `),
          el("span", {}, sourceLabel(c) + " "),
          el("span", { class: "snippet" }, "“" + truncate(c.snippet || "", 90) + "”"));
        list.append(row);
      }
      body.append(list);
    }

    appendMessage("bot", body);
  }

  // Parse the answer text and replace [n] with clickable chips bound to citation n.
  function renderAnswerText(text, citations) {
    const wrap = el("div", {});
    const byN = new Map(citations.map((c) => [c.n, c]));
    const parts = text.split(/(\[\d+\])/g);   // keep the [n] tokens as their own pieces
    for (const part of parts) {
      const m = part.match(/^\[(\d+)\]$/);
      if (m && byN.has(Number(m[1]))) {
        const c = byN.get(Number(m[1]));
        wrap.append(el("a", { class: "cite-ref", title: sourceLabel(c),
          onclick: (e) => { e.preventDefault(); openCitation(c); } }, m[1]));
      } else {
        wrap.append(document.createTextNode(part));
      }
    }
    return wrap;
  }

  function sourceLabel(c) {
    // the biography is a printed secondary source — cite it by book + page, not by archive region.
    if (c.source === "book")
      return `${c.title || "Thank God Ahead of Time"}${c.page != null ? ", p." + c.page : ""} (Crosby — biography)`;
    const kind = c.kind === "notebook_entry" ? "notebook" : (c.kind || "source");
    const pg = (c.page != null) ? `p.${c.page}` : "";
    return `${c.section || c.doc_id || ""} ${pg} (${kind})`.trim();
  }

  // ==========================================================================
  // TRACE — the agent's "under the hood" steps + cost ledger
  // ==========================================================================
  function renderTrace(trace) {
    return;   // agent trace removed for now — the #trace-steps panel was deleted; keep as a safe no-op
    const ol = $("#trace-steps");                                    // eslint-disable-line no-unreachable
    ol.innerHTML = "";
    if (!trace) { ol.append(el("li", { class: "muted-note" }, "no trace returned")); return; }

    // Route line first — the adaptive router's decision (RESEARCH_PLAN part D.7).
    if (trace.route) {
      ol.append(el("li", {},
        el("span", { class: "k" }, "route "),
        `${trace.route.complexity || "?"} → ${trace.route.strategy || ""}` +
        (trace.route.reason ? `  (${trace.route.reason})` : "")));
    }
    if (trace.queries_used && trace.queries_used.length > 1) {
      ol.append(el("li", {}, el("span", { class: "k" }, "queries "),
        trace.queries_used.length + " phrasings fused (RAG-Fusion)"));
    }
    // One line per tool that fired, in order — the toggleable-tools harness made visible.
    for (const s of (trace.steps || [])) {
      const bits = [];
      if (s.items != null) bits.push(`${s.items} items`);
      if (s.ms != null)    bits.push(`${s.ms} ms`);
      ol.append(el("li", {}, el("span", { class: "k" }, (s.tool || "step") + " "),
        (s.detail || "") + (bits.length ? `  [${bits.join(", ")}]` : "")));
    }

    // Cost — THIS query's own price (the delta), with the cumulative session total shown smaller
    // for context. (Earlier this showed the running ledger, so a $0.01 question read as the whole
    // project's spend — alarming and wrong.)
    const costEl = $("#trace-cost");
    if (trace.cost) {
      const c = trace.cost;
      const cum = c.cumulative_usd != null ? `   (session total $${c.cumulative_usd.toFixed(4)})` : "";
      costEl.textContent =
        `this query: $${(c.usd ?? 0).toFixed(6)}  ·  in ${c.input_tokens ?? 0} tok  ·  out ${c.output_tokens ?? 0} tok${cum}`;
    } else {
      costEl.textContent = "";
    }
  }

  // ==========================================================================
  // CITATION MODAL — OpenSeadragon deep-zoom + region highlight + PDF download
  // ==========================================================================
  let _osd = null;          // the live OpenSeadragon instance (recreated per open)
  let _lastCiteUrl = null;  // pop-out target: the current source page's PDF/image URL
  let _citePos = { x: 0, y: 0 };   // drag offset of the floating viewer (reset on each open)

  function wireModal() {
    $("#modal-close").addEventListener("click", closeModal);
    $("#modal-done").addEventListener("click", closeModal);
    // NB: no click-outside-to-close — the viewer is a non-dimming floating window; the page behind it
    // stays interactive so you can read a doc AND keep navigating. Close via × / Done / Escape.
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && $("#modal-backdrop").classList.contains("open")) closeModal();
    });
    // POP OUT the source page into its own browser window (movable to another monitor)
    const po = $("#modal-popout");
    if (po) po.addEventListener("click", () => {
      if (_lastCiteUrl) window.open(_lastCiteUrl, "solanus_source", "width=1000,height=900,resizable=yes");
      else alert("This citation is from the printed biography — there is no scanned page to open separately.");
    });
    // DRAG the floating viewer by its header (transform offset from the flex-centered position)
    const modal = document.querySelector("#modal-backdrop .modal");
    const header = modal ? modal.querySelector("header") : null;
    if (modal && header) {
      let dragging = false, sx = 0, sy = 0;
      const onMove = (e) => { if (!dragging) return; e.preventDefault();
        _citePos.x += e.clientX - sx; _citePos.y += e.clientY - sy; sx = e.clientX; sy = e.clientY;
        modal.style.transform = `translate(${_citePos.x}px, ${_citePos.y}px)`; };
      const onUp = () => { dragging = false; document.body.style.userSelect = "";
        window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
      header.addEventListener("pointerdown", (e) => {
        if (e.target.closest("button")) return;        // don't drag when hitting the × button
        dragging = true; sx = e.clientX; sy = e.clientY; document.body.style.userSelect = "none";
        window.addEventListener("pointermove", onMove); window.addEventListener("pointerup", onUp); });
    }
  }

  // Open the modal for a citation. The citation usually carries enough to draw
  // the region itself, but we still call /api/region to get the canonical image
  // URL/size + PDF link (and to backfill anything the answer omitted).
  async function openCitation(c) {
    if (!c) return;
    const backdrop = $("#modal-backdrop");
    if (!backdrop) return;
    backdrop.classList.add("open");
    _citePos = { x: 0, y: 0 };                               // re-centre the floating viewer on each open
    const _m = backdrop.querySelector(".modal"); if (_m) _m.style.transform = "";
    $("#modal-title").textContent = sourceLabel(c);
    // BOOK citations have no scanned archive region — show the passage as text only, no viewer.
    if (c.source === "book") {
      _lastCiteUrl = null;
      $("#modal-meta").textContent = "Michael Crosby, Thank God Ahead of Time — published biography (secondary source)";
      $("#region-text").textContent = c.preview || c.snippet || c.text || "";
      $("#region-kv").innerHTML = "";
      const v = $("#osd");
      if (v) v.innerHTML = "<div class='muted-note' style='padding:1em;'>This citation is from the printed biography, not the archive — there is no scanned page to deep-zoom.</div>";
      return;
    }
    $("#modal-meta").textContent = `${c.doc_id || ""}   rid ${c.rid || "-"}`;
    $("#region-text").textContent = c.full_text || c.snippet || c.text || "loading...";
    $("#region-kv").innerHTML = "";

    let region = null;
    try {
      const qs = new URLSearchParams({ doc_id: c.doc_id || "", rid: c.rid || "" });
      if (c.page != null) qs.set("page", String(c.page));   // disambiguate which page of a multi-page letter
      region = await api("/api/region?" + qs.toString());
    } catch (err) {
      // Backend not up / region unknown — fall back to whatever the citation has.
      region = fallbackRegion(c);
      $("#region-kv").append(el("div", { class: "kv" },
        "(live region service unavailable; showing citation data — " + err.message + ")"));
    }
    renderRegionSidebar(region, c);
    mountViewer(region);
  }

  function fallbackRegion(c) {
    // Reconstruct the minimum the viewer needs from the citation alone. We can't
    // know the exact image pixel size offline, so we leave it null and let the
    // viewer fit to the image's natural size once it loads.
    return {
      doc_id: c.doc_id, rid: c.rid, section: c.section, page: c.page, pdf_page: c.pdf_page,
      vertices: c.vertices || null, min_conf: c.min_conf, category: c.category,
      text: c.snippet || c.text || "",
      image_url: c.section && c.pdf_page ? `/api/image/${c.section}/${c.pdf_page}` : null,
      image_width: c.image_width || null, image_height: c.image_height || null,
      pdf_url: c.section ? `/api/pdf/${c.section}?page=${c.pdf_page || ""}` : null,
      manifest_url: c.section ? `/api/manifest/${c.section}` : null,
    };
  }

  function renderRegionSidebar(region, c) {
    // Prefer the FULL grouped-document text (whole letter / whole notebook entry) over the single cited
    // region — the user reads the document in context; the scan still highlights the matched region.
    $("#region-text").textContent = (c && c.full_text) || region.text || "(no transcribed text for this region)";
    const kv = $("#region-kv");
    kv.innerHTML = "";
    // Document context first (what the user asked for): when it was written, where Solanus wrote FROM,
    // and to whom — the human-readable facts, before the technical region details.
    const rec = region.record || {};
    const ctx = [
      ["Date written", rec.date],
      ["Written from", rec.sent_from],
      ["Recipient", rec.recipient],
      ["Sent to", rec.sent_to],
      ["Notebook page", rec.page_label],
    ];
    for (const [k, v] of ctx) {
      if (v == null || v === "") continue;
      kv.append(el("div", { class: "kv" }, el("strong", {}, k + ": "), String(v)));
    }
    if (rec.multipage)
      kv.append(el("div", { class: "muted-note", style: "margin:.3em 0;" },
        "This is one page of a multi-page letter; the scan shows the cited page."));
    // technical region details, de-emphasized below
    const tech = [["Source", region.section], ["Page", region.page],
                  ["Region kind", region.category], ["OCR confidence", region.min_conf]];
    for (const [k, v] of tech) {
      if (v == null || v === "") continue;
      kv.append(el("div", { class: "kv muted-note" }, el("strong", {}, k + ": "), String(v)));
    }
    _lastCiteUrl = region.pdf_url || region.image_url || null;   // pop-out target for this page
    const dl = $("#download-pdf");
    if (region.pdf_url) {
      dl.href = region.pdf_url;
      dl.style.display = "";
      dl.textContent = "Download source PDF";
    } else {
      dl.style.display = "none";
    }
    const po = $("#modal-popout"); if (po) po.style.display = _lastCiteUrl ? "" : "none";
  }

  // Build the OpenSeadragon viewer over the page image and draw the cited region
  // as a translucent highlight overlay. OSD wants normalized [0..1] coordinates
  // (its "viewport" space), so we convert pixel vertices using the image size.
  function mountViewer(region) {
    if (_osd) { _osd.destroy(); _osd = null; }
    const host = $("#osd");
    host.innerHTML = "";

    if (!region.image_url) {
      host.append(el("div", { class: "muted-note", style: "color:#fff; padding:1em;" },
        "Page image unavailable — start the backend so /api/image/<section>/<pdf_page> can serve the scan."));
      return;
    }

    // A "simple image" source: OSD will tile a single big PNG on the fly. We
    // pass the known pixel width/height when we have them so overlay math is
    // exact from the first frame; otherwise OSD infers them after load.
    _osd = OpenSeadragon({
      element:            host,
      prefixUrl:          "https://cdn.jsdelivr.net/npm/openseadragon@4.1.0/build/openseadragon/images/",
      tileSources:        { type: "image", url: region.image_url,
                            buildPyramid: true },
      showNavigator:      true,
      navigatorPosition:  "BOTTOM_RIGHT",
      gestureSettingsMouse: { clickToZoom: false },
      visibilityRatio:    1,
      minZoomImageRatio:  0.5,
    });

    _osd.addHandler("open", () => {
      const verts = region.vertices;
      // No usable polygon: don't guess — fit the whole page deterministically.
      if (!verts || !verts.length) { _osd.viewport.goHome(true); return; }

      // Image pixel size: prefer what the API told us; else read it off the
      // loaded tiled image. This is the denominator that turns pixels -> [0..1].
      const size = _osd.world.getItemAt(0).getContentSize();
      const W = region.image_width || size.x;
      const H = region.image_height || size.y;

      // Axis-aligned bounding box of the polygon, in pixels -> viewport units.
      // (A box is the right highlight here: the source vertices are rectangles.)
      const xs = verts.map((p) => p[0]), ys = verts.map((p) => p[1]);
      const x = Math.min(...xs), y = Math.min(...ys);
      const w = Math.max(...xs) - x, h = Math.max(...ys) - y;
      // Degenerate or out-of-bounds box (bad OCR geometry): fit the whole page instead of zooming
      // into a sliver of margin/whitespace.
      if (w <= 0 || h <= 0 || x < 0 || y < 0 || x + w > W * 1.02 || y + h > H * 1.02) {
        _osd.viewport.goHome(true); return;
      }
      const rect = _osd.viewport.imageToViewportRectangle(
        new OpenSeadragon.Rect(x, y, w, h));

      // The highlight box. We style it with brand colors via inline style (the
      // overlay is created by OSD so theme.css can't reach it cleanly).
      const box = document.createElement("div");
      box.style.border = "3px solid #3C1605";              // --provincial
      box.style.background = "rgba(60, 22, 5, 0.14)";
      box.style.boxShadow = "0 0 0 9999px rgba(42,38,42,0.30)";  // dim everything else
      box.style.borderRadius = "2px";
      _osd.addOverlay({ element: box, location: rect });

      // Frame the region with breathing room. NB: rect.times(f) scales x/y from the ORIGIN (pushing
      // the view down-and-right into the bottom margin) — expand ABOUT THE CENTER instead, and clamp
      // to the page so a near-edge box can't spill into the blank margin.
      const pad = 0.30;                                    // ~1.6x area, centered
      const framed = new OpenSeadragon.Rect(
        rect.x - rect.width * pad, rect.y - rect.height * pad,
        rect.width * (1 + 2 * pad), rect.height * (1 + 2 * pad));
      _osd.viewport.fitBoundsWithConstraints(framed, false);
    });
  }

  function closeModal() {
    $("#modal-backdrop").classList.remove("open");
    if (_osd) { _osd.destroy(); _osd = null; }
  }

  // ==========================================================================
  let _graphRO = null, _mapRO = null;   // ResizeObservers that re-fit the libs when their pane is dragged
  // GRAPH — Cytoscape.js over /api/graph (the knowledge graph)
  // ==========================================================================
  let _cy = null;

  // Brand-tinted palette by node kind. Records (letters/pages/entries) lean on
  // the deep browns; entity kinds get distinguishable accents so the graph reads
  // at a glance. Anything unknown falls back to the mid grey.
  const NODE_COLORS = {
    letter:         "#3C1605",   // provincial (primary records)
    notebook_page:  "#826962",   // provincial-dark
    notebook_entry: "#B3A29D",   // provincial-mid
    year:           "#5a4632",   // temporal spine (deep warm brown)
    person:         "#5b7b9a",
    place:          "#6a8a5b",
    organization:   "#9a7b5b",   // graph kind is "organization" (not "org")
    condition:      "#9c5b6a",
    favor:          "#b0894c",   // warm amber
    outcome:        "#5b9a8a",
    role:           "#7b6a8a",
    other:          "#B3A29D",
    event:          "#8a6a9a",
  };
  const ENTITY_KINDS = new Set(["person", "place", "organization", "condition",
                                "favor", "outcome", "role", "event"]);

  // A SECOND, redundant channel for entity kind: shape. Color alone fails in grayscale and for the
  // ~8% with colour-vision deficiency; shape makes "people vs places vs ailments" read instantly.
  // Capped to a handful of distinct silhouettes (KG-viz best practice).
  const NODE_SHAPE = {
    person: "ellipse", place: "round-diamond", organization: "round-rectangle",
    condition: "hexagon", favor: "round-tag", outcome: "round-pentagon",
    role: "round-triangle", event: "star", year: "barrel",
    letter: "round-rectangle", notebook_page: "round-rectangle", notebook_entry: "round-rectangle",
    other: "ellipse",
  };
  // Relationship styling: each verb gets a distinct CVD-safe (Okabe-Ito) colour + line style + arrow,
  // so a glance tells WROTE_TO from APPEARS_WITH without reading any text. This is the single biggest
  // "what am I looking at" fix — relation type becomes a visual property, not a label.
  const EDGE_STYLE = {
    WROTE_TO:      { color: "#2b6ca3", line: "solid",  arrow: true,  label: "wrote to" },
    APPEARS_WITH:  { color: "#9c8e88", line: "dashed", arrow: false, label: "appears with" },
    LOCATED_AT:    { color: "#d2792a", line: "solid",  arrow: true,  label: "located at" },
    HAS_CONDITION: { color: "#c0504d", line: "solid",  arrow: true,  label: "has condition" },
    ENROLLED:      { color: "#3a9151", line: "solid",  arrow: true,  label: "enrolled" },
    HAS_OUTCOME:   { color: "#3a9188", line: "solid",  arrow: true,  label: "has outcome" },
    FAMILY:        { color: "#7b5b9a", line: "solid",  arrow: false, label: "family" },
    MEMBER_OF:     { color: "#b0894c", line: "solid",  arrow: true,  label: "member of" },
    CONTINUES_ON:  { color: "#9c8e88", line: "dotted", arrow: true,  label: "continues on" },
    MENTIONED_IN:  { color: "#cfc4bf", line: "dotted", arrow: false, label: "mentioned in" },
    DATED_IN:      { color: "#a98f6f", line: "dotted", arrow: false, label: "dated in" },
  };
  const EDGE_DEFAULT = { color: "#C9BDB8", line: "solid", arrow: true, label: "related" };
  const edgeStyleFor = (k) => EDGE_STYLE[(k || "").toUpperCase()] || EDGE_DEFAULT;

  // TIER-3 (confidence): an edge's epistemic status from its provenance `method` (build_graph sets it).
  // ASSERTED = straight from a structured letter field / David's gold links; STRUCTURAL = derived from
  // region or date structure; INFERRED = co-occurrence / lexical cue / NER mention. Drives opacity so
  // the eye trusts solid asserted ties over faint inferred ones.
  const _ASSERTED = new Set(["source", "gold_link", "resolved"]);
  const _STRUCTURAL = new Set(["structure", "normalize_dates", "stitch"]);
  function edgeConfidence(method) {
    if (_ASSERTED.has(method)) return "asserted";
    if (_STRUCTURAL.has(method)) return "structural";
    return "inferred";
  }
  function edgeOpacity(e) {
    const raw = e.data("raw") || {};
    const conf = edgeConfidence(raw.method);
    if (raw.weight) return Math.min(0.95, 0.42 + 0.11 * raw.weight);   // co-occ: heavier = more opaque
    return conf === "asserted" ? 0.95 : conf === "structural" ? 0.72 : 0.5;
  }
  let _minWeight = 1;            // TIER-3 confidence slider: hide weighted (co-occurrence) edges below this

  let _fcoseReady = false;
  function ensureFcose() {                       // register the extension once, tolerate its absence
    if (_fcoseReady) return true;
    try { if (window.cytoscapeFcose) { cytoscape.use(window.cytoscapeFcose); _fcoseReady = true; } }
    catch (_) { /* already registered */ _fcoseReady = true; }
    return _fcoseReady;
  }

  let _crumbs = [];   // breadcrumb trail of focused entities: [{id, label}] — orientation aid
  let _limitBoost = 0;   // TIER-3 super-hub: "Load more" raises the node cap to reveal dropped neighbors

  async function loadGraph() {
    const meta = $("#graph-meta");
    const q = ($("#graph-focus") ? $("#graph-focus").value : "").trim();
    const entities = $("#graph-filter-entities") && $("#graph-filter-entities").checked;
    const params = new URLSearchParams({ entities_only: entities ? "true" : "false" });
    const rel = $("#graph-rels") ? $("#graph-rels").value : "";
    if (rel) params.set("rels", rel);   // show only one relationship kind
    // TIER-3 super-hub: _limitBoost lets "Load more" pull the neighbors the backend would otherwise
    // drop silently (it caps the node count to stay legible).
    if (q) {
      params.set("q", q); params.set("hops", "2"); params.set("limit", String(120 + _limitBoost));
      // focusing a YEAR needs the structural date edges to show that year's records
      if (/^\d{4}$/.test(q)) { params.set("structural", "true"); params.set("limit", String(160 + _limitBoost)); }
    } else { params.set("limit", String(70 + _limitBoost)); }   // small backbone; focus to go deeper
    try {
      const g = await api("/api/graph?" + params.toString());
      renderGraph(g);
      // drop a breadcrumb for the resolved focal entity (so the trail reflects real navigation)
      if (g.meta && g.meta.center) {
        const c = (g.nodes || []).find((n) => n.id === g.meta.center);
        pushCrumb(g.meta.center, c ? (c.label || c.id) : q);
      }
      if (g.meta) {
        meta.textContent = `showing ${g.meta.shown_nodes ?? (g.nodes || []).length} of ` +
          `${g.meta.eligible_nodes ?? g.meta.total_nodes ?? g.meta.n_nodes} relevant nodes, ` +
          `${g.meta.shown_edges ?? (g.edges || []).length} edges   -   double-click a node to expand it`;
        // TIER-3 super-hub: honestly surface that neighbors were dropped, with a way to pull more in
        // (instead of the old silent truncation).
        if (g.meta.truncated) {
          meta.append(document.createTextNode("   -   "));
          const more = el("a", { class: "crumb", title: "raise the cap and pull in more neighbors" },
            "Load more");
          more.addEventListener("click", () => { _limitBoost += 150; loadGraph(); });
          meta.append(more);
        }
      } else { meta.textContent = ""; }
      if (_yearRange) updateBrushMeta();        // keep the date-filter notice if a range is active
      buildLegend(g);
    } catch (err) {
      meta.textContent = "graph unavailable — run stages/build_graph.py, then start the backend";
      $("#graph").innerHTML =
        `<div class="muted-note" style="padding:1em;">No /api/graph yet (${err.message}). ` +
        `Once <code>data/graph.json</code> is built and the backend serves it, the entity/record ` +
        `network renders here.</div>`;
    }
  }

  // Career-timeline strip: records per year from /api/timeline. CLICK a bar to focus that year;
  // DRAG across bars to brush a year RANGE that filters the graph's edges (TIER-3 date-brush).
  let _yearRange = null;     // [loYear, hiYear] or null (all-time)
  async function renderTimeline() {
    const box = $("#timeline");
    if (!box) return;
    let ys;
    try { ys = (await api("/api/timeline")).years || []; }
    catch { box.innerHTML = "<span class='muted-note'>timeline unavailable</span>"; return; }
    if (!ys.length) { box.innerHTML = "<span class='muted-note'>no dated records yet</span>"; return; }
    const max = Math.max(...ys.map((y) => y.total)) || 1;
    box.innerHTML = "";
    let dragStart = null, moved = false;
    ys.forEach((y, i) => {
      const bar = el("div", { class: "bar" });
      bar.dataset.year = String(y.year);
      bar.style.height = Math.max(3, Math.round(70 * y.total / max)) + "px";
      bar.title = `${y.year}: ${y.total} records (${y.letter || 0} letters, ${y.notebook_entry || 0} entries)`;
      if (i % 5 === 0 || i === ys.length - 1) bar.append(el("span", { class: "yl" }, String(y.year)));
      bar.addEventListener("mousedown", (e) => { dragStart = y.year; moved = false; e.preventDefault(); });
      bar.addEventListener("mouseenter", () => {
        if (dragStart != null) { moved = true; paintBrush(box, Math.min(dragStart, y.year), Math.max(dragStart, y.year)); }
      });
      box.append(bar);
    });
    // finish a drag (or treat a no-move press as a click → focus that single year)
    const finish = (e) => {
      if (dragStart == null) return;
      const endBar = e.target.closest && e.target.closest(".bar");
      const endYear = endBar ? parseInt(endBar.dataset.year, 10) : dragStart;
      if (!moved) { $("#graph-focus").value = String(dragStart); dragStart = null; loadGraph(); return; }
      _yearRange = [Math.min(dragStart, endYear), Math.max(dragStart, endYear)];
      dragStart = null;
      paintBrush(box, _yearRange[0], _yearRange[1]);
      updateBrushMeta();
      applyDateBrush();
    };
    box.onmouseup = finish;
    box.onmouseleave = () => { if (dragStart != null && moved) finish({ target: box }); };
    box.ondblclick = clearDateBrush;          // double-click the strip to clear the range
    if (_yearRange) paintBrush(box, _yearRange[0], _yearRange[1]);
  }

  function paintBrush(box, lo, hi) {
    box.querySelectorAll(".bar").forEach((b) => {
      const y = parseInt(b.dataset.year, 10);
      b.classList.toggle("inrange", y >= lo && y <= hi);
    });
  }
  function updateBrushMeta() {
    const m = $("#graph-meta");
    if (m && _yearRange) m.textContent = `date filter: ${_yearRange[0]}–${_yearRange[1]} (double-click timeline to clear)`;
  }
  function clearDateBrush() {
    _yearRange = null;
    const box = $("#timeline"); if (box) box.querySelectorAll(".bar.inrange").forEach((b) => b.classList.remove("inrange"));
    applyDateBrush();
    loadGraph();                              // restore full meta line + view
  }
  // TIER-3 date-brush filter: hide graph edges whose year_span falls entirely outside [lo,hi]
  // (undatable edges stay), then hide any node left with no visible edges.
  function applyDateBrush() {
    if (!_cy) return;
    _cy.batch(() => {
      _cy.edges().forEach((e) => {
        const span = (e.data("raw") || {}).year_span;
        let out = false;
        if (_yearRange && span && span.length === 2) out = span[1] < _yearRange[0] || span[0] > _yearRange[1];
        e.toggleClass("outdate", out);
      });
      _cy.nodes().forEach((nd) => {
        if (nd.data("isFocus")) { nd.removeClass("outdate"); return; }
        const visible = nd.connectedEdges().filter((e) => !e.hasClass("outdate")).length;
        nd.toggleClass("outdate", !!_yearRange && visible === 0);
      });
    });
  }

  // ENTITY DETAIL PANEL — a TABBED dossier: Overview / Connections / Sources / Biography.
  let _dossierId = null;
  // The dossier panel is an overlay opened by clicking a map marker or graph node. Once closed it
  // had no way back; this tab restores it. Only shown on map/graph, only when there's a last dossier.
  function _curTab() { const a = document.querySelector("#tabs .tab.active"); return a ? a.dataset.tab : ""; }
  function updateReopen() {
    const btn = $("#ep-reopen"), panel = $("#entity-panel");
    if (!btn || !panel) return;
    const onGraphLike = (_curTab() === "map" || _curTab() === "graph");
    btn.classList.toggle("show", !!(onGraphLike && _dossierId && !panel.classList.contains("open")));
  }

  // dossier navigation history (back through the entities you've viewed) + a channel so a popped-out
  // dossier window follows along when you click a new marker/node in the main page.
  let _dossierStack = [], _dossierPos = -1;
  const _dossierChan = ("BroadcastChannel" in window) ? new BroadcastChannel("solanus-dossier") : null;
  if (_dossierChan) _dossierChan.onmessage = (e) => {   // a fresh pop-out asks for the current entity
    if (e.data && e.data.type === "hello" && _dossierId) _dossierChan.postMessage({ type: "open", id: _dossierId });
  };
  function updateDossierNav() {
    const b = $("#ep-back"); if (b) b.hidden = !(_dossierPos > 0);
  }
  function dossierBack() {
    if (_dossierPos > 0) { _dossierPos -= 1; openEntityPanel(_dossierStack[_dossierPos], { fromHistory: true }); }
  }

  async function openEntityPanel(id, opts = {}) {
    const panel = $("#entity-panel");
    panel.classList.add("open");
    updateReopen();
    _dossierId = id;
    if (!opts.fromHistory) {                            // new click → push onto the history (drop any forward)
      _dossierStack = _dossierStack.slice(0, _dossierPos + 1);
      if (_dossierStack[_dossierPos] !== id) { _dossierStack.push(id); _dossierPos = _dossierStack.length - 1; }
    }
    updateDossierNav();
    if (_dossierChan && !opts.fromBroadcast) _dossierChan.postMessage({ type: "open", id });  // keep a pop-out in sync
    $("#ep-title").textContent = "loading...";
    $("#ep-sub").textContent = "";
    $("#ep-body").innerHTML = "";
    let d;
    try { d = await api("/api/entity?id=" + encodeURIComponent(id)); }
    catch (e) { $("#ep-title").textContent = "not found"; return; }
    if (_dossierId !== id) return;                 // a newer open superseded this one
    $("#ep-title").textContent = d.label || id;
    // headline count = distinct source passages (matches the "Sources (N)" section below); show the raw
    // mention tally too, but only when it differs, so "6 mentions on 5 pages" reads honestly.
    const _sc = d.source_count, _mc = d.mention_count;
    const countLabel = _sc != null
      ? (_sc + (_sc === 1 ? " source" : " sources") + (_mc != null && _mc !== _sc ? ` · ${_mc} mentions` : ""))
      : (_mc != null ? _mc + " mentions" : "");
    $("#ep-sub").textContent = [d.kind, d.location, countLabel].filter(Boolean).join("    -    ");
    const body = $("#ep-body");

    // ---- build each tab's content into its own container ----
    const sections = [];

    // OVERVIEW
    const ov = el("div", {});
    const actions = el("div", { class: "ep-actions" });
    const center = el("button", { class: "ep-btn" }, "Center the graph on this");
    center.addEventListener("click", () => {
      showTab("graph"); $("#graph-focus").value = d.label || id; _limitBoost = 0; loadGraph();
      $("#entity-panel").classList.remove("open");
    });
    actions.append(center);
    if (d.lat != null && d.lon != null) {
      const sv = el("a", { class: "ep-btn",
        href: `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${d.lat},${d.lon}`,
        target: "_blank", rel: "noopener" }, "Open Street View");
      actions.append(sv);
    }
    ov.append(actions);
    // reconciled authority (name_authority side-car): link out to the Wikidata + VIAF records
    const _auth = d.authority || {};
    if (_auth.wikidata_url || _auth.viaf_url) {
      const arow = el("div", { class: "ppl-auth", style: "margin:.55em 0 .1em; flex-wrap:wrap;" });
      if (_auth.wikidata_url) arow.append(el("a", { href: _auth.wikidata_url, target: "_blank", rel: "noopener", class: "auth-badge wd" }, "Wikidata"));
      if (_auth.viaf_url) arow.append(el("a", { href: _auth.viaf_url, target: "_blank", rel: "noopener", class: "auth-badge viaf" }, "VIAF"));
      if (_auth.wikidata_desc) arow.append(el("span", { class: "muted-note", style: "margin-left:.5em; align-self:center;" }, _auth.wikidata_desc));
      ov.append(arow);
    }
    // RESEARCHED PLACE DOSSIER (side-car): precise address, what it was + its fate, significance, the
    // Fr. Solanus connection, and scholarly sources. Shown for places we've enriched; absent otherwise.
    const pe = d.place || {};
    const hasPE = pe.what_it_was || pe.fate || pe.significance || pe.solanus_connection ||
                  (pe.scholarly_sources || []).length || pe.address;
    if (hasPE) {
      const pbox = el("div", { class: "ep-place" });
      if (pe.address)
        pbox.append(el("p", { class: "muted-note" },
          "\u{1F4CD} " + pe.address + (pe.precise ? "  (pin is on the building)" : "")));
      const ppara = (h, t) => { if (t) { pbox.append(el("h4", {}, h)); pbox.append(el("p", {}, t)); } };
      ppara("What it was", pe.what_it_was);
      ppara("What happened to it", pe.fate);
      ppara("Significance", pe.significance);
      ppara("Fr. Solanus & this place", pe.solanus_connection);
      if ((pe.scholarly_sources || []).length) {
        pbox.append(el("h4", {}, "Scholarly sources"));
        for (const s of pe.scholarly_sources) {
          if (!s || !(s.url || s.title)) continue;
          const a = el("a", { href: s.url || "#", target: "_blank", rel: "noopener" }, s.title || s.url);
          pbox.append(el("div", { class: "muted-note", style: "margin:.25em 0;" },
            a, s.note ? " — " + s.note : ""));
        }
      }
      ov.append(pbox);
    }
    const summ = d.description || d.what_it_is;
    // avoid repeating the same text when the corpus summary duplicates the researched connection
    if (summ && summ !== pe.solanus_connection) { ov.append(el("h4", {}, "From the archive")); ov.append(el("p", {}, summ)); }
    const facts = [];
    if (d.role) facts.push("Role: " + d.role);
    if (d.relation_to_solanus && !["unknown", "none", "", null].includes(d.relation_to_solanus))
      facts.push("Relation to Fr. Solanus: " + String(d.relation_to_solanus).replace(/_/g, " "));
    if (d.location) facts.push("Location: " + d.location);
    if (facts.length) ov.append(el("p", { class: "muted-note" }, facts.join("    -    ")));
    if (d.variants && d.variants.length)
      ov.append(el("p", { class: "muted-note" }, "Also written: " + d.variants.slice(0, 8).join(", ")));
    const rec = d.record || {};
    if (rec.recipient || rec.sent_from || rec.sent_to || rec.date) {
      const box = el("div", { class: "ep-text" });
      for (const [k, v] of [["When", rec.date], ["Written from", rec.sent_from],
                            ["Recipient", rec.recipient], ["Sent to", rec.sent_to]])
        if (v) box.append(el("div", {}, el("b", {}, k + ": "), v));
      ov.append(box);
    }
    sections.push({ key: "overview", label: "Overview", node: ov });

    // CONNECTIONS — each with two SEPARATE buttons (go to entity / connecting documents + explanation)
    if (d.relations && d.relations.length) {
      const cn = el("div", {});
      cn.append(el("p", { class: "muted-note" },
        "Each connection: one button jumps to that entity; the other shows the actual document(s) that link them (with a short explanation)."));
      const _seenRel = new Set();
      const rels = d.relations
        .filter((r) => { const k = r.other_id || r.other; if (!k || _seenRel.has(k)) return false; _seenRel.add(k); return true; })
        .sort((a, b) => (a.when || "~").localeCompare(b.when || "~"));
      for (const r of rels) cn.append(connectionRow(r, id));
      sections.push({ key: "connections", label: `Connections (${rels.length})`, node: cn });
    }

    // SOURCES — every archive passage behind this entity
    const sr = el("div", {});
    const _seenTxt = new Set();
    const texts = (d.texts || []).filter((t) => {
      // key by doc_id+rid+page (matches the backend's per-record dedup). rid ALONE over-merges:
      // it's a per-document field id (e.g. "doc_1.src_location_recipient.0") that repeats across pages,
      // which used to collapse a place's many appendix passages down to one.
      const k = `${t.doc_id || ""}|${t.rid ?? ""}|${t.page ?? ""}|${(t.text || t.surface || "").slice(0, 24)}`;
      if (_seenTxt.has(k)) return false; _seenTxt.add(k); return true;
    });
    for (const t of texts) {
      const card = el("div", { class: "ep-text" });
      card.append(el("div", { class: "ep-text-meta" },
        `${t.section || t.doc_id || ""}  p.${t.page ?? "?"}${t.date ? "  (" + t.date + ")" : ""}`));
      card.append(el("div", {}, (t.text || t.surface || "").slice(0, 700)));
      const open = el("span", { class: "ep-open" }, "Open the scan");
      open.addEventListener("click", () => openCitation({
        doc_id: t.doc_id, rid: t.rid, page: t.page, pdf_page: t.pdf_page, section: t.section,
        vertices: t.vertices, kind: d.kind, snippet: t.text }));
      card.append(open);
      sr.append(card);
    }
    if (!texts.length) sr.append(el("p", { class: "muted-note" }, "No source passages found."));
    sections.push({ key: "sources", label: `Sources (${texts.length})`, node: sr });

    // BIOGRAPHY — Crosby passages (secondary source)
    if (d.book && d.book.length) {
      const bk = el("div", {});
      bk.append(el("p", { class: "muted-note" },
        "Michael Crosby, Thank God Ahead of Time — a published biography (secondary source)."));
      for (const b of d.book) {
        const card = el("div", { class: "ep-text" });
        card.append(el("div", { class: "ep-text-meta" },
          (b.title || "Thank God Ahead of Time") + (b.page != null ? ", p. " + b.page : "")));
        card.append(el("div", {}, (b.text || "").slice(0, 520)));
        bk.append(card);
      }
      sections.push({ key: "book", label: `Biography (${d.book.length})`, node: bk });
    }

    body.append(buildDossierTabs(sections));
  }

  // a tab strip + panels inside the dossier; first tab active. Plain, no glyphs.
  function buildDossierTabs(sections) {
    const wrap = el("div", { class: "ep-tabs" });
    const strip = el("div", { class: "ep-tabstrip" });
    const panels = el("div", { class: "ep-tabpanels" });
    sections.forEach((s, i) => {
      const btn = el("button", { class: "ep-tab" + (i === 0 ? " active" : "") }, s.label);
      const panel = el("div", { class: "ep-tabpanel" + (i === 0 ? "" : " hidden") });
      panel.append(s.node);
      btn.addEventListener("click", () => {
        strip.querySelectorAll(".ep-tab").forEach((x) => x.classList.remove("active"));
        panels.querySelectorAll(".ep-tabpanel").forEach((x) => x.classList.add("hidden"));
        btn.classList.add("active"); panel.classList.remove("hidden");
      });
      strip.append(btn); panels.append(panel);
    });
    wrap.append(strip, panels);
    return wrap;
  }

  // one connection row: relation + TWO buttons. "Go to" jumps; "Connecting document(s)" expands the
  // documents that link them + an LLM explanation (fetched on demand).
  function connectionRow(r, fromId) {
    const row = el("div", { class: "ep-rel" });
    const head = el("div", { class: "ep-rel-head" });
    head.append(el("span", { class: "rel-dot", style: `background:${edgeStyleFor(r.rel_kind || r.rel).color}` }));
    // prefer the LLM-adjudicated relationship/kinship subtype over the raw edge verb
    const verb = r.subtype || (r.verified === "real" && r.relationship && r.relationship !== "co-listed"
                               ? r.relationship : (r.rel || "related"));
    head.append(el("span", { class: "ep-rel-verb" }, verb + " "));
    head.append(el("b", {}, r.other || r.other_id));
    if (r.rel_kind === "APPEARS_WITH" && r.verified === "coincidental")
      head.append(el("span", { class: "rel-tag coinc" }, " same-page only"));
    else if (r.verified === "real")
      head.append(el("span", { class: "rel-tag real" }, " verified"));
    const meta = [];
    if (r.when) meta.push(r.when);
    if (r.weight) meta.push(`${r.weight} shared records`);
    if (meta.length) head.append(el("span", { class: "muted-note" }, "  (" + meta.join(", ") + ")"));
    row.append(head);

    const btns = el("div", { class: "ep-rel-btns" });
    const go = el("button", { class: "ep-rel-btn" }, "Go to this entity");
    go.addEventListener("click", () => openEntityPanel(r.other_id));
    const detail = el("div", { class: "ep-rel-detail hidden" });
    const docBtn = el("button", { class: "ep-rel-btn" }, "Connecting document(s)");
    docBtn.addEventListener("click", () => toggleConnectionDetail(fromId, r, detail, docBtn));
    btns.append(go, docBtn);
    row.append(btns, detail);
    return row;
  }

  async function toggleConnectionDetail(fromId, r, detail, btn) {
    if (detail.dataset.loaded) {                    // already fetched: just toggle
      detail.classList.toggle("hidden");
      btn.textContent = detail.classList.contains("hidden") ? "Connecting document(s)" : "Hide document(s)";
      return;
    }
    detail.classList.remove("hidden");
    btn.textContent = "Hide document(s)";
    detail.textContent = "Loading the connecting document(s)...";
    let c;
    try { c = await api(`/api/connection?a=${encodeURIComponent(fromId)}&b=${encodeURIComponent(r.other_id)}`); }
    catch (e) { detail.textContent = "Could not load the connection."; return; }
    detail.innerHTML = "";
    if (c.explanation)
      detail.append(el("div", { class: "ep-explain" }, el("b", {}, "How they are connected: "), c.explanation));
    const docs = c.documents || [];
    if (docs.length) {
      detail.append(el("div", { class: "muted-note", style: "margin:.5em 0 .2em;" },
        "The document(s) that link them — open to read the page yourself:"));
      for (const doc of docs) {
        const card = el("div", { class: "ep-text" });
        card.append(el("div", { class: "ep-text-meta" }, doc.cite_label || doc.doc_id || ""));
        card.append(el("div", {}, (doc.text || "").slice(0, 500)));
        const open = el("span", { class: "ep-open" }, "Open the scan");
        open.addEventListener("click", () => openCitation({
          doc_id: doc.doc_id, rid: doc.rid, page: doc.page, pdf_page: doc.pdf_page,
          section: doc.section, snippet: doc.text }));
        card.append(open);
        detail.append(card);
      }
    } else {
      detail.append(el("p", { class: "muted-note" }, "No single connecting document was found for this link."));
    }
    detail.dataset.loaded = "1";
  }

  // MAP — geocoded places via Leaflet (circle markers sized by mention frequency).
  let _map = null;
  async function renderMap() {
    let places;
    try { places = (await api("/api/map")).places || []; }
    catch { $("#map-meta").textContent = "map unavailable"; return; }
    if (typeof L === "undefined") { $("#map-meta").textContent = "Leaflet failed to load"; return; }
    if (!_map) {
      // worldCopyJump: markers follow you to whichever copy of the wrapped world you scroll to (so they
      // don't vanish when you pan around the globe back to America). maxBounds keeps panning sane.
      _map = L.map("map", { scrollWheelZoom: true, worldCopyJump: true }).setView([41.8, -85], 5);  // upper Midwest — Detroit home friary
      _map.setMaxBounds([[-85, -400], [85, 400]]);
      // CARTO "Voyager" basemap: clean, warm, low-clutter — reads better behind the brand markers than raw OSM.
      L.tileLayer("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        { attribution: "© OpenStreetMap © CARTO", subdomains: "abcd", maxZoom: 19, noWrap: false }).addTo(_map);
      // keep Leaflet correct when the user drags the map's resize handle (CSS resize:vertical)
      if (!_mapRO && "ResizeObserver" in window) {
        let raf = 0;
        _mapRO = new ResizeObserver(() => { cancelAnimationFrame(raf);
          raf = requestAnimationFrame(() => { if (_map) _map.invalidateSize(); }); });
        _mapRO.observe(document.getElementById("map"));
      }
    }
    setTimeout(() => _map.invalidateSize(), 60);   // the map tab renders lazily; fix tile sizing on (re)show
    if (_map._solGroup) _map.removeLayer(_map._solGroup);
    const group = L.layerGroup();
    // size + count by DISTINCT source passages (what the dossier shows), not raw mentions — keeps the
    // marker, the popup, and the dossier "Sources (N)" in agreement.
    const cnt = (p) => (p.sources != null ? p.sources : (p.mentions || 1));
    const max = Math.max(1, ...places.map(cnt));
    const stack = {};   // many buildings geocode to one city centroid; spiral them out so none hide
    let plotted = 0;
    for (const p of places) {
      if (p.lat == null || p.lon == null) continue;
      const key = p.lat.toFixed(3) + "," + p.lon.toFixed(3);
      const n = (stack[key] = (stack[key] || 0) + 1);
      let lat = p.lat, lon = p.lon;
      if (n > 1) { const a = n * 2.399, rad = 0.012 * Math.sqrt(n); lat += rad * Math.cos(a); lon += rad * Math.sin(a); }
      const r = 4 + 18 * Math.sqrt(cnt(p) / max);
      const m = L.circleMarker([lat, lon],
        { radius: r, color: "#3C1605", fillColor: "#9a7b5b", fillOpacity: 0.6, weight: 1 });
      // Street View: Google's pano action URL opens at the coords with no API key needed.
      const sv = `https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${p.lat},${p.lon}`;
      const nSrc = cnt(p);
      const blurb = p.what_it_was || (p.description ? String(p.description).slice(0, 170) +
        (String(p.description).length > 170 ? "…" : "") : "");
      const pin = p.precise ? `<br><span style="color:#2f6b41;font-weight:600;">\u{1F4CD} pin is on the building</span>` : "";
      m.bindPopup(`<b>${p.name || ""}</b><br>${p.role || ""}` +
        `${nSrc ? "  (" + nSrc + (nSrc === 1 ? " source page" : " source pages") + ")" : ""}` +
        `${blurb ? "<br>" + blurb : ""}` + pin +
        `<br><a href="${sv}" target="_blank" rel="noopener">Open Street View here</a>` +
        `<br><i>Click the dot for the full dossier.</i>`);
      m.on("click", () => openEntityPanel(p.id));   // full dossier (texts behind this place)
      group.addLayer(m); plotted++;
    }
    group.addTo(_map); _map._solGroup = group;
    $("#map-meta").textContent = `${plotted} geocoded places` +
      (plotted === 0 ? " — run geocode_places to populate the map" : "");
    setTimeout(() => _map.invalidateSize(), 120);   // container was hidden until the tab opened
  }

  // SPACING multiplier — how far apart the nodes sit. Defaults HIGH (deliberately roomy, per request);
  // the toolbar slider (#graph-spacing) scales every spacing knob live from 0.5x (compact) to 4x (vast).
  let _spacing = 2.0;
  function _spacingVal() {
    const s = $("#graph-spacing");
    return s ? (parseFloat(s.value) || _spacing) : _spacing;
  }
  function _layoutOpts(name, focalId) {
    const k = _spacingVal();                                // user spacing multiplier
    // adapt cost to graph size: animating + "proof" quality + label-aware sizing is gorgeous on a small
    // ego-net but crawls once you expand to a few hundred nodes. Scale the work down as the graph grows.
    const n = _curNodeCount || 80;
    const mid = n > 80, big = n > 180;
    if (name === "concentric") {
      // "rank by importance": rings by GLOBAL importance (mention_count) + in-view connectivity, bucketed
      // into ~6 readable rings; the focused entity sits dead-centre. (The old version ranked by in-view
      // degree and pinned focus at 1e6 with one-ring-per-degree + equidistant, which blew the radial scale
      // out so everything fit to tiny dots — "can't see a thing".)
      // importance = an ENTITY's recurrence (mention_count) + connectivity. RECORD nodes (letters/notebook
      // entries) have huge degree only because everything is MENTIONED_IN them — they are not "important
      // people", so we floor them to the outer ring (fixes records like "…Gorman…" sitting dead-centre).
      const _REC = { letter: 1, notebook_entry: 1, notebook_page: 1, document: 1, date: 1 };
      const imp = (nd) => _REC[nd.data("kind")] ? 1
        : ((nd.data("raw") || {}).mention_count || 0) + nd.degree() + 1;
      return { name: "concentric", animate: !mid, animationDuration: 450, fit: true, padding: 50,
               concentric: (nd) => (nd.data("isFocus") ? 1e9 : imp(nd)),
               levelWidth: (nodes) => {
                 let mx = 1; nodes.forEach((nd) => { if (!nd.data("isFocus")) mx = Math.max(mx, imp(nd)); });
                 return Math.max(1, mx / 6);                 // ~6 importance rings, not one-per-degree
               },
               minNodeSpacing: Math.round(34 * k), spacingFactor: 1.05 * k,
               startAngle: 1.5 * Math.PI, equidistant: false };
    }
    // DEFAULT: fcose — topology becomes the spatial signal. Pinned focal node. Spacing scales with k;
    // gravity eases as k grows. QUALITY/iterations/animation scale DOWN with node count for speed.
    if (ensureFcose()) {
      return { name: "fcose", animate: !mid, animationDuration: 500, randomize: true,
               quality: big ? "draft" : "default", nodeDimensionsIncludeLabels: !mid,
               nodeSeparation: Math.round(180 * k), idealEdgeLength: Math.round(150 * k),
               nodeRepulsion: Math.round(11000 * k), gravity: 0.25 / k, gravityRange: 4.5,
               numIter: big ? 800 : (mid ? 1500 : 2500), padding: 60,
               fixedNodeConstraint: focalId ? [{ nodeId: focalId, position: { x: 0, y: 0 } }] : undefined };
    }
    return _layoutOpts("concentric", focalId);   // fcose missing -> graceful fallback
  }
  let _curNodeCount = 0;   // node count of the current graph (drives adaptive layout cost)

  // stable element ids so EXPAND can merge a node's neighborhood into the live graph without dupes.
  const _nodeEl = (n, focalId) => ({ data: { id: n.id, label: n.label || n.id, kind: n.kind || "node",
                                             raw: n, isFocus: n.id === focalId ? 1 : 0 } });
  const _edgeEl = (e) => ({ data: { id: `${e.source}__${(e.kind || "").toUpperCase()}__${e.target}`,
                                    source: e.source, target: e.target,
                                    label: edgeStyleFor(e.kind).label, kind: (e.kind || "").toUpperCase(),
                                    raw: e } });

  // EXPAND a node IN PLACE: pull its neighbors into the current graph (walk the full graph outward).
  async function expandNode(id) {
    if (!_cy) return;
    let g;
    try { g = await api(`/api/graph?center=${encodeURIComponent(id)}&hops=1&limit=80`); }
    catch (e) { return; }
    const haveN = new Set(_cy.nodes().map((n) => n.id()));
    const haveE = new Set(_cy.edges().map((e) => e.id()));
    const newNodes = (g.nodes || []).filter((n) => !haveN.has(n.id)).map((n) => _nodeEl(n, _lastFocal));
    const addedIds = new Set([...haveN, ...newNodes.map((n) => n.data.id)]);
    const newEdges = (g.edges || []).map(_edgeEl)
      .filter((e) => !haveE.has(e.data.id) && addedIds.has(e.data.source) && addedIds.has(e.data.target));
    if (!newNodes.length && !newEdges.length) {
      $("#graph-meta").textContent = "no further connections to expand here";
      return;
    }
    _cy.add(newNodes); _cy.add(newEdges);
    _curNodeCount = _cy.nodes().length;           // re-layout uses adaptive cost for the bigger graph
    _cy.layout(_layoutOpts(_graphLayout(), _lastFocal)).run();
    applyZoomTier(); applyConfidenceFilter(); applyDateBrush(); applyNodeColoring(); applyConnVerify();
    buildLegend({ nodes: _cy.nodes().map((n) => n.data("raw") || { kind: n.data("kind") }),
                  edges: _cy.edges().map((e) => e.data("raw") || { kind: e.data("kind") }) });
    $("#graph-meta").textContent = `expanded — now showing ${_cy.nodes().length} nodes, ${_cy.edges().length} edges (double-click any node to keep expanding)`;
  }

  let _lastTap = { id: null, t: 0 };   // manual double-tap detection (single = dossier, double = expand)
  let _lastFocal = null;   // focal node id of the current graph (for re-running the layout switcher)
  function renderGraph(g) {
    const entitiesOnly = $("#graph-filter-entities").checked;
    const focalId = g.meta && g.meta.center ? g.meta.center : null;
    _lastFocal = focalId;

    // Cytoscape wants {data:{...}} wrappers. We keep id/kind/label and stash the rest so a node click
    // can open its dossier. isFocus marks the searched entity so we can pin + spotlight it.
    const nodes = (g.nodes || [])
      .filter((n) => !entitiesOnly || ENTITY_KINDS.has(n.kind))
      .map((n) => ({ data: { id: n.id, label: n.label || n.id, kind: n.kind || "node",
                             raw: n, isFocus: n.id === focalId ? 1 : 0 } }));

    const keep = new Set(nodes.map((n) => n.data.id));
    const edges = (g.edges || [])
      .filter((e) => keep.has(e.source) && keep.has(e.target))
      .map((e) => _edgeEl(e));

    if (_cy) { _cy.destroy(); _cy = null; }
    const sizeFor = (n) => 22 + Math.min(46, 4 * Math.sqrt((n.data("raw") || {}).mention_count || 1));
    const style = [
      { selector: "node", style: {
          "background-color": (n) => NODE_COLORS[n.data("kind")] || "#B3A29D",
          "shape": (n) => NODE_SHAPE[n.data("kind")] || "ellipse",
          "label": "data(label)", "font-size": 11, "font-family": "Open Sans, sans-serif",
          "color": "#2A262A", "text-wrap": "wrap", "text-max-width": 140,
          "min-zoomed-font-size": 7,
          "text-background-color": "#FFFFFF", "text-background-opacity": 0.85,
          "text-background-padding": 2, "text-background-shape": "roundrectangle",
          "width": sizeFor, "height": sizeFor,
          "border-width": 2, "border-color": "#FFFFFF" } },
      // the focused entity: thick brand ring, larger, shadowed, always labelled = "the thing you searched"
      { selector: "node[?isFocus]", style: {
          "border-width": 5, "border-color": "#3C1605", "font-size": 14, "font-weight": "bold",
          "width": (n) => sizeFor(n) + 14, "height": (n) => sizeFor(n) + 14,
          "z-index": 99, "shadow-blur": 18, "shadow-color": "#3C1605", "shadow-opacity": 0.4 } },
      { selector: "node:selected", style: { "border-width": 4, "border-color": "#3C1605" } },
      { selector: "edge", style: {
          "width": (e) => 1.2 + Math.min(4, Math.sqrt((e.data("raw") || {}).weight || 1) - 1),
          "line-color": (e) => edgeStyleFor(e.data("kind")).color,
          "line-style": (e) => edgeStyleFor(e.data("kind")).line,
          "target-arrow-color": (e) => edgeStyleFor(e.data("kind")).color,
          "target-arrow-shape": (e) => (edgeStyleFor(e.data("kind")).arrow ? "triangle" : "none"),
          "curve-style": "bezier", "arrow-scale": 0.9, "opacity": edgeOpacity,
          // edge VERB is the loudest source of clutter -> show it only on hover/select, not always.
          "font-size": 9, "font-family": "Open Sans, sans-serif", "color": "#5a4632",
          "text-rotation": "autorotate", "text-background-color": "#FFFFFF",
          "text-background-opacity": 0.9, "text-background-padding": 1 } },
      { selector: "edge.hl, edge:selected", style: { "label": "data(label)", "opacity": 1,
          "width": (e) => 2 + Math.min(4, Math.sqrt((e.data("raw") || {}).weight || 1) - 1) } },
      { selector: "edge:selected", style: { "color": "#3C1605", "font-size": 12 } },
      // degree-of-interest: hovering a node fades everything outside its neighbourhood.
      { selector: ".faded", style: { "opacity": 0.1, "text-opacity": 0.08 } },
      { selector: "node.hl", style: { "border-color": "#3C1605", "border-width": 3 } },
      // legend-driven spotlight (click a legend chip): dim everything that doesn't match.
      { selector: ".legend-dim", style: { "opacity": 0.08, "text-opacity": 0.06 } },
      // TIER-3 semantic zoom: hide a node's label until you zoom in to its tier (set by applyZoomTier).
      { selector: "node.nolabel", style: { "text-opacity": 0 } },
      // TIER-3 confidence slider: edges below the min co-occurrence strength are hidden outright.
      { selector: "edge.weak", style: { "display": "none" } },
      // co-occurrence adjudication: same-page coincidences (LLM-verified) are dashed + faint when shown...
      { selector: "edge.coinc", style: { "line-style": "dashed", "opacity": 0.18 } },
      { selector: "edge.coinc-hidden", style: { "display": "none" } },   // ...and hidden by default
      // TIER-3 date-brush: elements outside the selected year range are hidden.
      { selector: ".outdate", style: { "display": "none" } },
      // legend kind-filter: hide entity kinds toggled off in the legend ("reorganize by kind").
      { selector: "node.kind-hidden", style: { "display": "none" } },
    ];

    _curNodeCount = nodes.length;                 // lets _layoutOpts pick cheaper settings for big graphs
    _cy = cytoscape({
      container: $("#graph"), elements: [...nodes, ...edges], style,
      layout: _layoutOpts(_graphLayout(), focalId), wheelSensitivity: 0.3,
      // viewport performance: cache a texture + hide edges while panning/zooming, cap pixel ratio. These
      // keep pan/zoom smooth as the graph grows (the main source of slowness when you expand a lot).
      textureOnViewport: true, hideEdgesOnViewport: true, motionBlur: false, pixelRatio: 1,
    });
    _cy.fit(undefined, 40);
    // keep Cytoscape's canvas correct when the user drags the graph's resize handle (CSS resize:vertical)
    if (!_graphRO && "ResizeObserver" in window) {
      let raf = 0;
      _graphRO = new ResizeObserver(() => { cancelAnimationFrame(raf);
        raf = requestAnimationFrame(() => { if (_cy) _cy.resize(); }); });
      _graphRO.observe($("#graph"));
    }

    // TIER-3 semantic zoom: rank nodes by in-view degree (a per-render rank, since absolute degree is
    // meaningless here), tag the top tiers, and reveal labels progressively as you zoom in. Overview
    // first (focus + hubs), details on demand.
    const ranked = _cy.nodes().sort((a, b) => b.degree(false) - a.degree(false));
    const n = ranked.length;
    ranked.forEach((nd, i) => { nd.data("_rank", i); });
    applyZoomTier();
    _cy.on("zoom", _debouncedZoomTier);
    applyConfidenceFilter();
    applyDateBrush();              // preserve any active timeline date-brush across re-renders
    applyNodeColoring();          // honour the color-by-community toggle
    applyConnVerify();            // hide same-page coincidences (verified connections)

    // single-click = full dossier; DOUBLE-click = expand the node's connections in place (walk the
    // graph outward). Hover = fade neighbours.
    _cy.on("tap", "node", (evt) => {
      const nid = evt.target.id(), now = Date.now();
      if (_lastTap.id === nid && now - _lastTap.t < 350) expandNode(nid);   // 2nd tap on same node
      else openEntityPanel(nid);
      _lastTap = { id: nid, t: now };
    });
    _cy.on("mouseover", "node", (evt) => {
      if (_cy.nodes().length > 350) return;       // restyling everything on hover is too costly when huge
      const nb = evt.target.closedNeighborhood();
      _cy.elements().difference(nb).addClass("faded");
      nb.addClass("hl");
    });
    _cy.on("mouseout", "node", () => { if (_cy.nodes().length <= 350) _cy.elements().removeClass("faded hl"); });
  }

  // TIER-3 semantic-zoom: which labels show depends on zoom. Far out = focus + top hubs only; mid = top
  // third; zoomed in = everything. Edge verbs only appear once you're close. Keeps the overview clean.
  function applyZoomTier() {
    if (!_cy) return;
    const z = _cy.zoom(), n = _cy.nodes().length;
    const topHub = Math.max(6, Math.round(n * 0.12));     // "hubs" = top ~12% by in-view degree
    const topMid = Math.max(14, Math.round(n * 0.4));
    const cutoff = z < 0.45 ? topHub : z < 1.0 ? topMid : n;   // how many ranks get a label
    _cy.batch(() => {
      _cy.nodes().forEach((nd) => {
        const show = nd.data("isFocus") || (nd.data("_rank") ?? 1e9) < cutoff;
        nd.toggleClass("nolabel", !show);
      });
    });
  }
  let _zoomTimer = null;
  function _debouncedZoomTier() {
    if (_zoomTimer) clearTimeout(_zoomTimer);
    _zoomTimer = setTimeout(applyZoomTier, 90);
  }

  // TIER-3 confidence slider: hide co-occurrence (weighted) edges below the chosen strength. Pure
  // client-side display toggle — no refetch. Unweighted edges (asserted/structural) always stay.
  function applyConfidenceFilter() {
    if (!_cy) return;
    _cy.batch(() => {
      _cy.edges().forEach((e) => {
        const w = (e.data("raw") || {}).weight;
        e.toggleClass("weak", !!w && w < _minWeight);
      });
    });
  }

  function _graphLayout() { const s = $("#graph-layout"); return s ? s.value : "fcose"; }

  // BREADCRUMB: record the trail of focused entities so the user keeps their bearings.
  function pushCrumb(id, label) {
    if (!id) return;
    if (_crumbs.length && _crumbs[_crumbs.length - 1].id === id) return;   // skip consecutive dupes
    _crumbs.push({ id, label: label || id });
    if (_crumbs.length > 8) _crumbs = _crumbs.slice(-8);
    renderCrumbs();
  }
  function renderCrumbs() {
    const box = $("#graph-crumbs");
    if (!box) return;
    box.innerHTML = "";
    if (_crumbs.length < 1) return;
    box.append(el("span", { class: "muted-note" }, "trail: "));
    _crumbs.forEach((c, i) => {
      if (i) box.append(el("span", { class: "crumb-sep" }, " / "));
      const chip = el("a", { class: "crumb" }, truncate(c.label, 22));
      chip.addEventListener("click", () => { $("#graph-focus").value = c.label; loadGraph(); });
      box.append(chip);
    });
  }

  // legend-driven spotlight: dim everything that isn't the chosen node-kind / edge-kind.
  let _legendSpot = null;
  function spotlight(kind, isEdge) {
    if (!_cy) return;
    if (_legendSpot === kind) { _cy.elements().removeClass("legend-dim"); _legendSpot = null; return; }
    _legendSpot = kind;
    _cy.elements().addClass("legend-dim");
    const match = isEdge ? _cy.edges().filter((e) => e.data("kind") === kind)
                         : _cy.nodes().filter((n) => n.data("kind") === kind);
    const keep = isEdge ? match.connectedNodes().union(match) : match.union(match.connectedEdges());
    keep.removeClass("legend-dim");
  }

  // co-occurrence adjudication (verify_connections): hide/dim same-page coincidences, keep real ties.
  let _verifiedOnly = true;
  function applyConnVerify() {
    if (!_cy) return;
    _cy.batch(() => _cy.edges().forEach((e) => {
      const coinc = ((e.data("raw") || {}).verified) === "coincidental";
      e.toggleClass("coinc", coinc);
      e.toggleClass("coinc-hidden", coinc && _verifiedOnly);
    }));
  }

  const _hiddenKinds = new Set();   // entity kinds toggled off via the legend (reorganize by kind)
  function applyHiddenKinds() {
    if (!_cy) return;
    _cy.batch(() => _cy.nodes().forEach((n) =>
      n.toggleClass("kind-hidden", _hiddenKinds.has(n.data("kind")))));
  }
  function toggleKind(k, chip) {
    if (_hiddenKinds.has(k)) _hiddenKinds.delete(k); else _hiddenKinds.add(k);
    if (chip) chip.classList.toggle("off", _hiddenKinds.has(k));
    applyHiddenKinds();
  }

  function buildLegend(g) {
    const legend = $("#graph-legend");
    legend.innerHTML = "";
    // The legend is the decoder ring AND a filter. ENTITY chips toggle that kind's VISIBILITY (click to
    // hide/show — "reorganize by kind"); RELATIONSHIP chips spotlight that link type.
    const nodeKinds = [...new Set((g.nodes || []).map((n) => n.kind).filter(Boolean))];
    if (nodeKinds.length) {
      legend.append(el("span", { class: "legend-head" }, "entities (click to hide/show):"));
      for (const k of nodeKinds) {
        const color = NODE_COLORS[k] || "#B3A29D";
        const chip = el("span", { class: "legend-chip" + (_hiddenKinds.has(k) ? " off" : ""),
          title: "click to hide/show " + k },
          el("span", { class: "swatch swatch-" + (NODE_SHAPE[k] || "ellipse"), style: `background:${color}` }), k);
        chip.addEventListener("click", () => toggleKind(k, chip));
        legend.append(chip);
      }
    }
    const edgeKinds = [...new Set((g.edges || []).map((e) => (e.kind || "").toUpperCase()).filter(Boolean))];
    if (edgeKinds.length) {
      legend.append(el("span", { class: "legend-head" }, "relationships (click to spotlight):"));
      for (const k of edgeKinds) {
        const st = edgeStyleFor(k);
        const chip = el("span", { class: "legend-chip", title: "click to spotlight " + st.label },
          el("span", { class: "edge-swatch", style: `border-top:3px ${st.line} ${st.color}` }), st.label);
        chip.addEventListener("click", () => spotlight(k, true));
        legend.append(chip);
      }
    }
    applyHiddenKinds();
  }

  // ENTITY BROWSER — search/list top-connected entities so the user can DISCOVER starting points.
  let _browseTimer = null;
  async function loadBrowse() {
    const box = $("#graph-browse-list");
    if (!box) return;
    const q = ($("#graph-browse") ? $("#graph-browse").value : "").trim();
    const kind = $("#graph-browse-kind") ? $("#graph-browse-kind").value : "";
    const params = new URLSearchParams({ limit: "40" });
    if (q) params.set("q", q);
    if (kind) params.set("kind", kind);
    let r;
    try { r = await api("/api/entities?" + params.toString()); }
    catch (e) { box.innerHTML = ""; return; }
    box.innerHTML = "";
    for (const e of (r.entities || [])) {
      const item = el("div", { class: "gb-item", title: "focus the graph on " + e.label },
        el("span", {}, truncate(e.label || e.id, 26)),
        el("span", { class: "gb-kind" }, `${e.kind} (${e.degree})`));
      item.addEventListener("click", () => {
        $("#graph-focus").value = e.label || e.id; _limitBoost = 0; loadGraph();
      });
      box.append(item);
    }
    if (!(r.entities || []).length) box.append(el("div", { class: "muted-note", style: "padding:.4em;" }, "no matches"));
  }

  // COMMUNITY colouring (Louvain): distinct, stable hue per community id.
  let _colorByCommunity = false;
  function communityColor(cid) {
    if (cid == null) return "#B3A29D";
    return `hsl(${(cid * 53) % 360} 58% 55%)`;
  }
  function applyNodeColoring() {
    if (!_cy) return;
    _cy.batch(() => _cy.nodes().forEach((n) => {
      const raw = n.data("raw") || {};
      n.style("background-color", _colorByCommunity ? communityColor(raw.community)
                                                    : (NODE_COLORS[n.data("kind")] || "#B3A29D"));
    }));
  }

  // COMMUNITIES browser: jump into a thematic cluster (Louvain). Built by stages/graph_analysis.py.
  async function loadCommunities() {
    const box = $("#graph-communities");
    if (!box || box.dataset.loaded) return;
    let r;
    try { r = await api("/api/communities"); }
    catch (e) { return; }
    box.dataset.loaded = "1";
    box.innerHTML = "";
    const comms = (r.communities || []).filter((c) => c.size >= 3).slice(0, 30);
    if (!comms.length) {
      box.append(el("div", { class: "muted-note", style: "padding:.4em;" },
        "not computed yet (run stages/graph_analysis.py)"));
      return;
    }
    for (const c of comms) {
      const names = (c.top_members || []).slice(0, 3).map((m) => m.label).join(", ");
      const item = el("div", { class: "gb-item", title: "open this cluster in the graph" },
        el("span", {}, `${c.dominant_kind} cluster - ${truncate(names, 30)}`),
        el("span", { class: "gb-kind" }, `${c.size}`));
      item.addEventListener("click", () => {
        const top = (c.top_members || [])[0];
        if (top) { $("#graph-focus").value = top.label || top.id; _limitBoost = 150; loadGraph(); }
      });
      box.append(item);
    }
  }

  // ==========================================================================
  // tiny utilities
  // ==========================================================================
  function truncate(s, n) { return s.length > n ? s.slice(0, n - 1) + "…" : s; }

  // Graph toolbar wiring (kept here so all graph code is together).
  document.addEventListener("DOMContentLoaded", () => {
    // Right-hand settings panel: collapse it to give the graph the full width. Resize Cytoscape after
    // the CSS transition so the canvas reclaims (or yields) the space and re-fits.
    const gToggle = $("#graph-settings-toggle");
    if (gToggle) gToggle.addEventListener("click", () => {
      const panel = $("#graph-settings");
      const collapsed = panel.classList.toggle("collapsed");
      gToggle.textContent = collapsed ? "Show settings panel" : "Hide settings panel";
      gToggle.setAttribute("aria-expanded", String(!collapsed));
      setTimeout(() => { if (_cy) { _cy.resize(); _cy.fit(undefined, 40); } }, 220);
    });
    $("#graph-fit").addEventListener("click", () => { if (_cy) _cy.fit(undefined, 30); });
    $("#graph-reload").addEventListener("click", () => { $("#graph-focus").value = ""; _limitBoost = 0; _yearRange = null; loadGraph(); });
    $("#graph-filter-entities").addEventListener("change", loadGraph);
    // picking a relation shows THAT relationship's whole network — clear the focus first, else we'd
    // filter the focused node's edges (e.g. Solanus has no "enrolled" edges -> a lone node).
    $("#graph-rels").addEventListener("change", () => { $("#graph-focus").value = ""; _limitBoost = 0; loadGraph(); });
    // layout switch: re-run the layout on the EXISTING graph (no refetch) for a snappy toggle.
    const lsel = $("#graph-layout");
    if (lsel) lsel.addEventListener("change", () => {
      if (_cy) { _cy.layout(_layoutOpts(_graphLayout(), _lastFocal)).run(); _cy.fit(undefined, 40); }
    });
    // SPACING slider: re-run the layout on the existing graph at the new spacing (no refetch). Debounced
    // so dragging doesn't fire a hundred relayouts; updates the live "2.0×" label as you drag.
    const sp = $("#graph-spacing");
    if (sp) {
      let _spTimer = null;
      sp.addEventListener("input", () => {
        const lbl = $("#graph-spacing-val"); if (lbl) lbl.textContent = parseFloat(sp.value).toFixed(1) + "x";
        if (_spTimer) clearTimeout(_spTimer);
        _spTimer = setTimeout(() => {
          if (_cy) { _cy.layout(_layoutOpts(_graphLayout(), _lastFocal)).run(); _cy.fit(undefined, 50); }
        }, 200);
      });
    }
    // TIER-3 confidence slider: hide weak co-occurrence edges live (no refetch).
    const mw = $("#graph-minweight");
    if (mw) mw.addEventListener("input", () => {
      _minWeight = parseInt(mw.value, 10) || 1;
      const lbl = $("#graph-minweight-val"); if (lbl) lbl.textContent = String(_minWeight);
      applyConfidenceFilter();
    });
    const focus = () => { _limitBoost = 0; loadGraph(); };
    $("#graph-focus-btn").addEventListener("click", focus);
    $("#graph-focus").addEventListener("keydown", (e) => { if (e.key === "Enter") focus(); });

    // Entity browser (discover starting points): debounced search + kind filter.
    const gb = $("#graph-browse");
    if (gb) gb.addEventListener("input", () => {
      if (_browseTimer) clearTimeout(_browseTimer);
      _browseTimer = setTimeout(loadBrowse, 220);
    });
    const gbk = $("#graph-browse-kind");
    if (gbk) gbk.addEventListener("change", loadBrowse);
    const cbc = $("#graph-color-community");
    if (cbc) cbc.addEventListener("change", () => { _colorByCommunity = cbc.checked; applyNodeColoring(); });
    const vo = $("#graph-verified-only");
    if (vo) vo.addEventListener("change", () => { _verifiedOnly = vo.checked; applyConnVerify(); });
    const cwrap = $("#graph-communities-wrap");
    if (cwrap) cwrap.addEventListener("toggle", () => { if (cwrap.open) loadCommunities(); });

    // Entity detail panel close
    $("#ep-close").addEventListener("click", () => { $("#entity-panel").classList.remove("open"); updateReopen(); });
    $("#ep-reopen").addEventListener("click", () => { $("#entity-panel").classList.add("open"); updateReopen(); });
    const epBack = $("#ep-back"); if (epBack) epBack.addEventListener("click", dossierBack);
    const epPop = $("#ep-popout");
    if (epPop) epPop.addEventListener("click", () => {
      const w = window.open("/static/dossier.html", "solanus_dossier", "width=560,height=820,resizable=yes");
      if (w) { try { w.focus(); } catch (_) {} }
      else alert("Pop-out was blocked by the browser. Allow pop-ups for this site, then try again.");
    });
    wireDossierDrag();

    // Settings modal (models + toggles, opened from the header gear)
    const sb = $("#settings-backdrop");
    if (sb) {
      const openS = () => { sb.classList.add("open"); document.body.style.overflow = "hidden"; };
      const closeS = () => { sb.classList.remove("open"); document.body.style.overflow = ""; };
      const sBtn = $("#settings-btn"); if (sBtn) sBtn.addEventListener("click", openS);
      const sClose = $("#settings-close"); if (sClose) sClose.addEventListener("click", closeS);
      sb.addEventListener("click", (e) => { if (e.target === sb) closeS(); });
      document.addEventListener("keydown", (e) => { if (e.key === "Escape" && sb.classList.contains("open")) closeS(); });
    }

    // Help modals ("?" buttons): scrollable content, background scroll LOCKED while open.
    document.querySelectorAll("[data-help]").forEach((btn) =>
      btn.addEventListener("click", () => openHelp(btn.getAttribute("data-help"))));
    $("#help-close").addEventListener("click", closeHelp);
    $("#help-backdrop").addEventListener("click", (e) => { if (e.target === $("#help-backdrop")) closeHelp(); });
    document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeHelp(); });
    // deep-link: /?rel=<person id> opens a shareable "Solanus and ___" story on load
    const _rel = new URLSearchParams(location.search).get("rel");
    if (_rel) setTimeout(() => openRelationship(_rel), 120);
  });

  // ==========================================================================
  // ARCHIVE-CONTENT PAGES — His Life, Reading Room, People, Family, and the
  // "Solanus and ___" relationship view. Additive tabs, each built from the
  // additive /api endpoints; every card reuses openEntityPanel() for dossiers.
  // ==========================================================================
  const escHtml = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const firstName = (s) => (String(s || "")
    .replace(/^(Mr|Mrs|Miss|Ms|Rev|Fr|Father|Sr|Sister|Br|Dr|Rt|Very|Mgr|Msgr|St)\.?\s+/i, "")
    .split(/\s+/)[0] || "—");

  // ---- HIS LIFE : HORIZONTAL timeline — a static overview map + "that year" readout above a snap-
  // scrolling track of milestone cards. All map pins are placed ONCE (no per-step fly-to → fast), and
  // every card is reachable by scrolling right (no vertical dead-zone that hid the last node). ----
  let _lifeMap = null, _lifeMarkers = [];
  async function renderLife() {
    const root = $("#life-root");
    root.innerHTML = "";
    let data;
    try { data = await api("/api/life"); }
    catch { root.append(el("div", { class: "page-empty" }, "His Life is unavailable.")); return; }
    const ms = data.milestones || [];

    root.append(el("div", { class: "life-hero" },
      el("div", { class: "life-eyebrow" }, "1870  —  2017"),
      el("h1", { class: "life-title" }, "The life of Fr. Solanus Casey"),
      el("p", { class: "life-lede" },
        "A doorkeeper for fifty years. Move along his life left to right — each year opens onto the letters and petitions the archive still holds.")));

    // stage bar: a small static overview map + the "that year" readout
    const stage = el("div", { class: "life-stage" });
    const mapDiv = el("div", { class: "life-map", id: "life-map" });
    const readout = el("div", { class: "life-readout" });
    const rYear = el("div", { class: "sr-year" }, "");
    const rPlace = el("div", { class: "sr-place" }, "");
    const rStats = el("div", { class: "sr-stats" }, "");
    readout.append(rYear, rPlace, rStats);
    stage.append(mapDiv, readout);
    root.append(stage);

    // horizontal timeline track + prev/next controls
    const wrap = el("div", { class: "life-trackwrap" });
    const track = el("div", { class: "life-track" }); track.tabIndex = 0;
    const prev = el("button", { class: "life-nav prev", type: "button", "aria-label": "earlier" }, "‹");
    const next = el("button", { class: "life-nav next", type: "button", "aria-label": "later" }, "›");
    wrap.append(prev, track, next);
    root.append(wrap);

    ms.forEach((m, i) => {
      const card = el("div", { class: "ms-card" },
        el("div", { class: "ms-date" }, m.date || String(m.year || "")),
        el("h2", { class: "ms-title" }, m.title || ""),
        el("div", { class: "ms-place" }, m.place || ""),
        el("p", { class: "ms-body" }, m.body || ""));
      const a = m.activity || {};
      if ((a.letters || 0) + (a.notebook_entries || 0) > 0) {
        const ac = el("div", { class: "ms-activity" });
        if (a.letters) ac.append(el("span", { class: "ms-chip" }, `${a.letters} letter${a.letters === 1 ? "" : "s"}`));
        if (a.notebook_entries) ac.append(el("span", { class: "ms-chip" }, `${a.notebook_entries} petition${a.notebook_entries === 1 ? "" : "s"}`));
        card.append(ac);
      }
      (m.letters || []).slice(0, 3).forEach((l) => {
        const b = el("button", { class: "ms-letter" }, `${l.date || ""} — to ${l.recipient || "?"}`);
        b.addEventListener("click", () => openLetterInReading(l.id));
        card.append(b);
      });
      card._m = m; card._i = i;
      track.append(card);
    });

    const setActive = (m, i) => {
      rYear.textContent = m.year || "";
      rPlace.textContent = m.place || "";
      const a = m.activity || {};
      rStats.textContent = `${a.letters || 0} letters · ${a.notebook_entries || 0} petitions this year`;
      _lifeMarkers.forEach((mk, j) => {                 // just restyle — no fly-to, no tile reload
        if (!mk) return;
        const on = j === i;
        mk.setStyle({ radius: on ? 11 : 6, fillColor: on ? "#3C1605" : "#c9b39c",
          color: on ? "#3C1605" : "#9a7b5b", fillOpacity: on ? 1 : .7, weight: on ? 3 : 1 });
        if (on) mk.bringToFront();
      });
    };

    // one overview map: place ALL milestone pins ONCE, fit to them; active change only restyles a marker
    try {
      if (_lifeMap) { try { _lifeMap.remove(); } catch (_) {} _lifeMap = null; }
      if (typeof L !== "undefined") {
        _lifeMap = L.map(mapDiv, { zoomControl: false, attributionControl: false, dragging: false,
          scrollWheelZoom: false, doubleClickZoom: false, keyboard: false }).setView([43, -92], 4);
        L.tileLayer("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
          { subdomains: "abcd", maxZoom: 19 }).addTo(_lifeMap);
        const pts = [];
        _lifeMarkers = ms.map((m) => {
          if (m.lat == null || m.lon == null) return null;
          pts.push([m.lat, m.lon]);
          return L.circleMarker([m.lat, m.lon],
            { radius: 6, color: "#9a7b5b", fillColor: "#c9b39c", fillOpacity: .7, weight: 1 }).addTo(_lifeMap);
        });
        if (pts.length) _lifeMap.fitBounds(pts, { padding: [24, 24], maxZoom: 6 });
      }
    } catch (e) { _lifeMap = null; }
    setTimeout(() => { if (_lifeMap) _lifeMap.invalidateSize(); }, 90);
    if (ms.length) setActive(ms[0], 0);

    // active card = the one crossing the track's horizontal centre (observer scoped to the track)
    const io = new IntersectionObserver((entries) => {
      entries.forEach((en) => {
        en.target.classList.toggle("in", en.isIntersecting);
        if (en.isIntersecting && en.target._m) setActive(en.target._m, en.target._i);
      });
    }, { root: track, rootMargin: "0px -45% 0px -45%", threshold: 0 });
    track.querySelectorAll(".ms-card").forEach((c) => io.observe(c));

    // mouse wheel → move along the timeline; arrows step one card
    track.addEventListener("wheel", (e) => {
      if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) { track.scrollLeft += e.deltaY; e.preventDefault(); }
    }, { passive: false });
    const step = (dir) => {
      const c = track.querySelector(".ms-card");
      track.scrollBy({ left: dir * ((c ? c.offsetWidth : 320) + 22), behavior: "smooth" });
    };
    prev.addEventListener("click", () => step(-1));
    next.addEventListener("click", () => step(1));
  }

  // ---- READING ROOM : letters (transcription beside scan) + notebook petitions ----
  let _readingBuilt = false, _lettersCache = null;
  async function renderReadingRoom() {
    const root = $("#reading-root");
    if (_readingBuilt) return;
    _readingBuilt = true;
    root.innerHTML = "";
    const segLetters = el("button", { class: "seg-btn active" }, "Letters");
    const segNotes = el("button", { class: "seg-btn" }, "Notebooks");
    const seg = el("div", { class: "seg" }, segLetters, segNotes);
    root.append(el("div", { class: "reading-head" },
      el("div", {},
        el("h1", { class: "page-h1" }, "The reading room"),
        el("p", { class: "page-lede" }, "His correspondence and the notebooks of favors — the transcription beside the original hand.")),
      seg));
    const body = el("div", { class: "reading-body", id: "reading-view" });
    root.append(body);
    segLetters.addEventListener("click", () => { segLetters.classList.add("active"); segNotes.classList.remove("active"); renderLettersView(body); });
    segNotes.addEventListener("click", () => { segNotes.classList.add("active"); segLetters.classList.remove("active"); renderNotebooksView(body); });
    // deep-link: /#reading?rr=notebooks opens the notebook room straight away
    if (new URLSearchParams(location.search).get("rr") === "notebooks") segNotes.click();
    else renderLettersView(body);
  }

  async function renderLettersView(container) {
    container.innerHTML = "";
    const search = el("input", { class: "rr-search", type: "search", placeholder: "Search letters — recipient or text…" });
    const yearSel = el("select", { class: "rr-year" }, el("option", { value: "" }, "All years"));
    const listWrap = el("div", { class: "rr-items" });
    const listCol = el("div", { class: "rr-list" }, el("div", { class: "rr-controls" }, search, yearSel), listWrap);
    const readCol = el("div", { class: "rr-reader", id: "rr-reader" },
      el("div", { class: "rr-reader-empty" }, "Select a letter to read it beside its scan."));
    container.append(el("div", { class: "rr-layout" }, listCol, readCol));

    if (!_lettersCache) {
      try { _lettersCache = await api("/api/letters"); }
      catch { listWrap.append(el("div", { class: "page-empty" }, "Letters unavailable.")); return; }
    }
    (_lettersCache.years || []).forEach((y) => yearSel.append(el("option", { value: y }, y)));
    const draw = () => {
      const q = search.value.toLowerCase().trim();
      const yr = yearSel.value;
      listWrap.innerHTML = "";
      let n = 0;
      for (const L of _lettersCache.letters) {
        if (yr && String(L.year) !== yr) continue;
        if (q && !(L.recipient || "").toLowerCase().includes(q) && !(L.snippet || "").toLowerCase().includes(q)
            && !(L.summary || "").toLowerCase().includes(q)) continue;
        const item = el("button", { class: "rr-item", "data-lid": L.id },
          el("div", { class: "rr-item-top" },
            el("span", { class: "rr-item-to" }, L.recipient || "—"),
            el("span", { class: "rr-item-date" }, (L.date || "").slice(0, 18))),
          el("div", { class: "rr-item-snip" + (L.summary ? " is-summary" : "") }, L.summary || L.snippet || ""));
        item.addEventListener("click", () => { listWrap.querySelectorAll(".rr-item").forEach((x) => x.classList.remove("active")); item.classList.add("active"); openLetter(L.id); });
        listWrap.append(item);
        n++;
      }
      if (!n) listWrap.append(el("div", { class: "page-empty" }, "No letters match."));
    };
    search.addEventListener("input", draw);
    yearSel.addEventListener("change", draw);
    draw();
    // shareable deep-link: /#reading with ?letter=<id> opens that letter in the reader on load
    const want = new URLSearchParams(location.search).get("letter");
    if (want) { const it = listWrap.querySelector(`.rr-item[data-lid="${CSS.escape(want)}"]`); if (it) it.classList.add("active"); openLetter(want); }
  }

  async function openLetter(id) {
    showTab("reading");
    const reader = $("#rr-reader");
    if (!reader) return;
    reader.innerHTML = "<div class='rr-loading'>loading…</div>";
    let d;
    try { d = await api("/api/letter?id=" + encodeURIComponent(id)); }
    catch { reader.innerHTML = "<div class='page-empty'>Could not load this letter.</div>"; return; }
    reader.innerHTML = "";
    const lhead = el("div", { class: "lt-head" },
      el("h2", { class: "lt-to" }, "To " + (d.recipient || "—")),
      el("div", { class: "lt-meta" }, [d.date, (d.from ? "from " + d.from : ""), (d.to ? "to " + d.to : "")].filter(Boolean).join("  ·  ")));
    if (d.summary) lhead.append(el("div", { class: "lt-summary" }, d.summary));
    reader.append(lhead);
    const prev = el("button", { class: "lt-navbtn" }, "← Previous");
    const next = el("button", { class: "lt-navbtn" }, "Next →");
    prev.disabled = !d.prev; next.disabled = !d.next;
    prev.addEventListener("click", () => d.prev && openLetter(d.prev));
    next.addEventListener("click", () => d.next && openLetter(d.next));
    reader.append(el("div", { class: "lt-nav" }, prev, next));

    const txt = el("div", { class: "lt-text" });
    if (d.greeting) txt.append(el("p", { class: "lt-greeting" }, d.greeting));
    const bodyP = el("p", { class: "lt-body", html: highlightEntities(d.body || "", d.entities || []) });
    txt.append(bodyP);
    if (d.farewell) txt.append(el("p", { class: "lt-farewell" }, d.farewell));
    if (d.signature) txt.append(el("p", { class: "lt-signature" }, d.signature));
    if (d.commentary) txt.append(el("div", { class: "lt-commentary" }, d.commentary));
    bodyP.querySelectorAll(".ent").forEach((s) => s.addEventListener("click", () => openEntityPanel(s.getAttribute("data-id"))));

    const img = el("img", { class: "lt-img", src: d.image_url, alt: "scan of the letter", loading: "lazy" });
    const scan = el("div", { class: "lt-scan" }, img,
      el("a", { class: "lt-openscan", href: d.pdf_url, target: "_blank", rel: "noopener" }, "Open the scan ↗"));
    reader.append(el("div", { class: "lt-cols" }, txt, scan));

    if ((d.entities || []).length) {
      const chips = el("div", { class: "lt-entities" }, el("div", { class: "lt-entities-h" }, "Named in this letter"));
      d.entities.forEach((e) => {
        const c = el("button", { class: "ent-chip ent-" + e.kind }, e.label || "?");
        c.addEventListener("click", () => openEntityPanel(e.id));
        chips.append(c);
      });
      reader.append(chips);
    }
    reader.scrollTop = 0;
  }

  function highlightEntities(text, entities) {
    let safe = escHtml(text);
    if (!entities || !entities.length) return safe;
    const seen = new Set(), cands = [];
    for (const e of entities) {
      for (const nm of [e.label, ...(e.variants || [])]) {
        const t = (nm || "").trim();
        if (t.length < 4) continue;
        const key = t.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        cands.push({ t, id: e.id, kind: e.kind });
      }
    }
    cands.sort((a, b) => b.t.length - a.t.length);
    for (const c of cands) {
      const esc = escHtml(c.t).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const pat = new RegExp("(?<![\\w>&])(" + esc + ")(?![\\w<])", "g");
      let done = false;
      safe = safe.replace(pat, (m) => {
        if (done) return m;
        done = true;
        return `<span class="ent ent-${escHtml(c.kind)}" data-id="${escHtml(c.id)}">${m}</span>`;
      });
    }
    return safe;
  }

  let _nbState = { section: "", q: "", offset: 0, total: 0 };
  async function renderNotebooksView(container) {
    container.innerHTML = "";
    _nbState = { section: "", q: "", offset: 0, total: 0 };
    const search = el("input", { class: "rr-search", type: "search", placeholder: "Search petitions…" });
    const volSel = el("select", { class: "rr-year" }, el("option", { value: "" }, "All volumes"));
    const listWrap = el("div", { class: "rr-items" });
    const moreBtn = el("button", { class: "rr-more" }, "Load more");
    const listCol = el("div", { class: "rr-list" }, el("div", { class: "rr-controls" }, search, volSel), listWrap, moreBtn);
    const readCol = el("div", { class: "rr-reader", id: "rr-reader" },
      el("div", { class: "rr-reader-empty" }, "Select a petition to read it beside its page."));
    container.append(el("div", { class: "rr-layout" }, listCol, readCol));

    try {
      const nb = await api("/api/notebooks");
      (nb.notebooks || []).forEach((v) => volSel.append(el("option", { value: v.section }, `${v.title} (${v.entries})`)));
    } catch (e) {}

    const load = async (reset) => {
      if (reset) { _nbState.offset = 0; listWrap.innerHTML = ""; }
      const p = new URLSearchParams({ offset: _nbState.offset, limit: 40 });
      if (_nbState.section) p.set("section", _nbState.section);
      if (_nbState.q) p.set("q", _nbState.q);
      let d;
      try { d = await api("/api/notebook_entries?" + p); } catch (e) { return; }
      _nbState.total = d.total;
      for (const e of d.entries) {
        const item = el("button", { class: "rr-item" },
          el("div", { class: "rr-item-top" },
            el("span", { class: "rr-item-to" }, e.date || e.page_label || "petition"),
            el("span", { class: "rr-item-date" }, (e.edtf || "").slice(0, 4))));
        if (e.page_theme) item.append(el("div", { class: "rr-item-theme" }, e.page_theme));
        item.append(el("div", { class: "rr-item-snip" }, (e.text || "").slice(0, 150)));
        item.addEventListener("click", () => { listWrap.querySelectorAll(".rr-item").forEach((x) => x.classList.remove("active")); item.classList.add("active"); showNotebookEntry(e); });
        listWrap.append(item);
      }
      _nbState.offset += d.entries.length;
      moreBtn.style.display = _nbState.offset < _nbState.total ? "" : "none";
      moreBtn.textContent = `Load more (${_nbState.offset} / ${_nbState.total})`;
    };
    let t;
    search.addEventListener("input", () => { clearTimeout(t); t = setTimeout(() => { _nbState.q = search.value.trim(); load(true); }, 250); });
    volSel.addEventListener("change", () => { _nbState.section = volSel.value; load(true); });
    moreBtn.addEventListener("click", () => load(false));
    load(true);
  }

  function showNotebookEntry(e) {
    const reader = $("#rr-reader");
    if (!reader) return;
    reader.innerHTML = "";
    const nhead = el("div", { class: "lt-head" },
      el("h2", { class: "lt-to" }, e.page_theme || e.notebook || "Petition"),
      el("div", { class: "lt-meta" }, [e.notebook, e.page_label, e.date, (e.edtf || "").slice(0, 4)].filter(Boolean).join("  ·  ")));
    if (e.page_gloss) nhead.append(el("div", { class: "lt-summary" }, e.page_gloss));
    reader.append(nhead);
    const txt = el("div", { class: "lt-text" }, el("p", { class: "lt-body" }, e.text || ""));
    const scan = el("div", { class: "lt-scan" });
    if (e.image_url) {
      scan.append(el("img", { class: "lt-img", src: e.image_url, alt: "notebook page scan", loading: "lazy" }));
      scan.append(el("a", { class: "lt-openscan", href: `/api/pdf/${e.section}?page=${e.pdf_page}`, target: "_blank", rel: "noopener" }, "Open the page ↗"));
    }
    reader.append(el("div", { class: "lt-cols" }, txt, scan));
    const more = el("button", { class: "lt-navbtn" }, "Open the full record ↗");
    more.addEventListener("click", () => openEntityPanel(e.id));
    reader.append(el("div", { class: "lt-nav" }, more));
  }

  function openLetterInReading(id) {
    showTab("reading");
    setTimeout(() => openLetter(id), 70);
  }

  // ---- PEOPLE directory ----
  let _peopleState = { q: "", role: "", sort: "prominence", has_authority: false, offset: 0, total: 0 };
  async function renderPeople() {
    const root = $("#people-root");
    root.innerHTML = "";
    _peopleState = { q: "", role: "", sort: "prominence", has_authority: false, offset: 0, total: 0 };
    root.append(el("div", { class: "ppl-head" },
      el("h1", { class: "page-h1" }, "People of the archive"),
      el("p", { class: "page-lede", id: "ppl-count" }, "")));
    const search = el("input", { class: "ppl-search", type: "search", placeholder: "Search a name…" });
    const sortSel = el("select", { class: "ppl-sort" },
      el("option", { value: "prominence" }, "Most connected"),
      el("option", { value: "letters" }, "Most letters received"),
      el("option", { value: "name" }, "Name (A–Z)"));
    const authInput = el("input", { type: "checkbox" });
    const authChk = el("label", { class: "ppl-check" }, authInput, " Reconciled only (Wikidata / VIAF)");
    root.append(el("div", { class: "ppl-controls" }, search, sortSel, authChk));
    const chips = el("div", { class: "ppl-roles", id: "ppl-roles" });
    const grid = el("div", { class: "ppl-grid", id: "ppl-grid" });
    const more = el("button", { class: "rr-more", id: "ppl-more" }, "Load more");
    root.append(chips, grid, more);

    const setActiveChip = (active) => chips.querySelectorAll(".role-chip").forEach((c) => c.classList.toggle("active", c === active));
    const load = async (reset) => {
      if (reset) { _peopleState.offset = 0; grid.innerHTML = ""; }
      const p = new URLSearchParams({ offset: _peopleState.offset, limit: 48, sort: _peopleState.sort });
      if (_peopleState.q) p.set("q", _peopleState.q);
      if (_peopleState.role) p.set("role", _peopleState.role);
      if (_peopleState.has_authority) p.set("has_authority", "true");
      let d;
      try { d = await api("/api/people?" + p); }
      catch (e) { grid.append(el("div", { class: "page-empty" }, "People unavailable.")); return; }
      _peopleState.total = d.total;
      $("#ppl-count").textContent =
        `${(d.n_people || 0).toLocaleString()} people named across the letters & notebooks — ${(d.total || 0).toLocaleString()} match your view.`;
      if (reset && !chips.childElementCount) {
        const allChip = el("button", { class: "role-chip active" }, "All");
        allChip.addEventListener("click", () => { _peopleState.role = ""; setActiveChip(allChip); load(true); });
        chips.append(allChip);
        (d.roles || []).filter((r) => r.role && r.role !== "—").slice(0, 12).forEach((r) => {
          const c = el("button", { class: "role-chip" }, `${r.role} · ${r.n}`);
          c.addEventListener("click", () => { _peopleState.role = r.role; setActiveChip(c); load(true); });
          chips.append(c);
        });
      }
      for (const person of d.people) grid.append(personCard(person));
      _peopleState.offset += d.people.length;
      more.style.display = _peopleState.offset < _peopleState.total ? "" : "none";
      more.textContent = `Load more (${_peopleState.offset} / ${_peopleState.total})`;
    };
    let t;
    search.addEventListener("input", () => { clearTimeout(t); t = setTimeout(() => { _peopleState.q = search.value.trim(); load(true); }, 250); });
    sortSel.addEventListener("change", () => { _peopleState.sort = sortSel.value; load(true); });
    authInput.addEventListener("change", (e) => { _peopleState.has_authority = e.target.checked; load(true); });
    more.addEventListener("click", () => load(false));
    load(true);
  }

  function personCard(p) {
    const card = el("div", { class: "ppl-card" });
    const name = el("button", { class: "ppl-name" }, p.name || "—");
    name.addEventListener("click", () => openEntityPanel(p.id));
    card.append(name);
    const meta = el("div", { class: "ppl-meta" });
    if (p.role) meta.append(el("span", { class: "ppl-role" }, p.role));
    if (p.relation_to_solanus && p.relation_to_solanus !== "unknown")
      meta.append(el("span", { class: "ppl-rel" }, p.relation_to_solanus.replace(/_/g, " ")));
    if (meta.childElementCount) card.append(meta);
    const stats = el("div", { class: "ppl-stats" });
    if (p.letters) stats.append(el("span", { class: "ppl-stat" }, `${p.letters} letter${p.letters === 1 ? "" : "s"}`));
    if (p.mentions) stats.append(el("span", { class: "ppl-stat muted" }, `${p.mentions} mention${p.mentions === 1 ? "" : "s"}`));
    if (stats.childElementCount) card.append(stats);
    if (p.wikidata_url || p.viaf_url) {
      const auth = el("div", { class: "ppl-auth" });
      if (p.wikidata_url) auth.append(el("a", { href: p.wikidata_url, target: "_blank", rel: "noopener", class: "auth-badge wd" }, "Wikidata"));
      if (p.viaf_url) auth.append(el("a", { href: p.viaf_url, target: "_blank", rel: "noopener", class: "auth-badge viaf" }, "VIAF"));
      card.append(auth);
    }
    if (p.letters) {   // only genuine correspondents get the "Solanus and ___" story
      const story = el("button", { class: "ppl-story" }, "Solanus & " + firstName(p.name));
      story.addEventListener("click", () => openRelationship(p.id));
      card.append(story);
    }
    return card;
  }

  // ---- FAMILY tree ----
  const lifespan = (b, d) => {
    const yr = (s) => { const mm = String(s || "").match(/\d{4}/); return mm ? mm[0] : ""; };
    const a = yr(b), z = yr(d);
    return (a || z) ? `${a || "?"} – ${z || ""}`.trim() : "";
  };
  async function renderFamily() {
    const root = $("#family-root");
    root.innerHTML = "";
    let d;
    try { d = await api("/api/family"); }
    catch (e) { root.append(el("div", { class: "page-empty" }, "Family tree unavailable.")); return; }
    root.append(el("div", { class: "fam-hero" },
      el("h1", { class: "page-h1" }, "The Casey family"),
      el("p", { class: "page-lede" }, d.note || "")));
    const members = d.members || [];
    const byRel = (r) => members.filter((m) => (m.relation || "").toLowerCase() === r);
    const group = (title, arr) => {
      if (!arr.length) return;
      const row = el("div", { class: "fam-row" });
      arr.forEach((m) => row.append(familyCard(m)));
      root.append(el("div", { class: "fam-gen" }, el("div", { class: "fam-gen-label" }, title), row));
    };
    group("Parents", [...byRel("father"), ...byRel("mother")]);
    const sibs = [...byRel("brother"), ...byRel("sister")];
    const sibRow = el("div", { class: "fam-row fam-sibs" });
    sibRow.append(familyCard({ name: "Bernard “Solanus” Casey", relation: "the friar",
      entity_id: d.solanus_id, birth: "1870", death: "1957", in_corpus: true }, { solanus: true }));
    sibs.forEach((m) => sibRow.append(familyCard(m)));
    root.append(el("div", { class: "fam-gen" },
      el("div", { class: "fam-gen-label" }, `The sixteen children (${sibs.length + 1} traced)`), sibRow));
    group("In-laws", byRel("in-law"));
    group("Nieces & nephews", [...byRel("niece"), ...byRel("nephew"), ...byRel("nun")]);
  }

  function familyCard(m, opts = {}) {
    const card = el("div", { class: "fam-card" + (opts.solanus ? " fam-solanus" : "") + (m.entity_id ? " linked" : "") });
    card.append(el("div", { class: "fam-name" }, m.name || "—"));
    card.append(el("div", { class: "fam-rel" }, m.relation || ""));
    const ls = lifespan(m.birth, m.death);
    if (ls) card.append(el("div", { class: "fam-life" }, ls));
    if (m.n_letters) card.append(el("div", { class: "fam-letters" }, `${m.n_letters} letter${m.n_letters === 1 ? "" : "s"} in the archive`));
    else if (m.in_corpus) card.append(el("div", { class: "fam-letters muted" }, "appears in the letters"));
    if (m.entity_id) {
      const acts = el("div", { class: "fam-acts" });
      const dossier = el("button", { class: "fam-btn" }, "Dossier");
      dossier.addEventListener("click", () => openEntityPanel(m.entity_id));
      acts.append(dossier);
      if (!opts.solanus) {
        const story = el("button", { class: "fam-btn ghost" }, "Letters with Solanus");
        story.addEventListener("click", () => openRelationship(m.entity_id));
        acts.append(story);
      }
      card.append(acts);
    }
    return card;
  }

  // ---- "Solanus and ___" relationship view (a light floating card) ----
  async function openRelationship(personId) {
    let ov = $("#rel-overlay");
    if (!ov) {
      ov = el("div", { class: "rel-overlay", id: "rel-overlay" });
      ov.addEventListener("click", (e) => { if (e.target === ov) ov.classList.remove("open"); });
      document.body.append(ov);
    }
    const card = el("div", { class: "rel-card" });
    const close = el("button", { class: "rel-close", "aria-label": "close" }, "×");
    close.addEventListener("click", () => ov.classList.remove("open"));
    card.append(close, el("div", { class: "rel-loading" }, "loading…"));
    ov.innerHTML = ""; ov.append(card); ov.classList.add("open");
    let d;
    try { d = await api("/api/relationship?person=" + encodeURIComponent(personId)); }
    catch (e) { card.innerHTML = ""; card.append(close, el("div", { class: "page-empty" }, "Could not load this relationship.")); return; }
    card.innerHTML = ""; card.append(close);
    card.append(el("div", { class: "rel-eyebrow" }, "Solanus and " + (d.label || "—")));
    const bits = [];
    if (d.letters) bits.push(`${d.letters} letter${d.letters === 1 ? "" : "s"}`);
    if (d.petitions) bits.push(`${d.petitions} petition${d.petitions === 1 ? "" : "s"}`);
    if (!d.letters && !d.petitions && d.n_records) bits.push(`named in ${d.n_records} document${d.n_records === 1 ? "" : "s"}`);
    if (d.relation_to_solanus && d.relation_to_solanus !== "unknown") bits.push(d.relation_to_solanus.replace(/_/g, " "));
    card.append(el("div", { class: "rel-sub" }, bits.join("  ·  ")));
    if (d.narrative && d.narrative.narrative) card.append(el("p", { class: "rel-narr" }, d.narrative.narrative));
    else card.append(el("p", { class: "rel-narr muted" },
      "A written account of this correspondence hasn’t been generated yet — but the documents that tie them together are below."));
    if ((d.arc || []).length) {
      const arc = el("div", { class: "rel-arc" });
      const max = Math.max(...d.arc.map((a) => a.n));
      d.arc.forEach((a) => {
        const bar = el("div", { class: "rel-bar", title: `${a.year}: ${a.n}` });
        bar.style.height = (8 + 44 * (a.n / max)) + "px";
        arc.append(el("div", { class: "rel-barcol" }, bar, el("div", { class: "rel-baryr" }, "’" + String(a.year).slice(2))));
      });
      card.append(el("div", { class: "rel-arc-label" }, "When they appear together"), arc);
    }
    const recs = el("div", { class: "rel-recs" });
    (d.records || []).slice(0, 30).forEach((r) => {
      const item = el("button", { class: "rel-rec" },
        el("span", { class: "rel-rec-date" }, r.date || (r.edtf || "").slice(0, 10) || ""),
        el("span", { class: "rel-rec-snip" }, r.snippet || r.label || ""));
      item.addEventListener("click", () => { ov.classList.remove("open"); if (r.kind === "letter") openLetterInReading(r.id); else openEntityPanel(r.id); });
      recs.append(item);
    });
    if (recs.childElementCount) card.append(el("div", { class: "rel-recs-label" }, "In the archive"), recs);
    document.addEventListener("keydown", function esc(ev) { if (ev.key === "Escape") { ov.classList.remove("open"); document.removeEventListener("keydown", esc); } });
  }

  // ==========================================================================
  // HELP — plain-language instructions per surface, in a scrollable modal.
  // ==========================================================================
  const HELP = {
    graph: { title: "Using the knowledge graph", html: `
      <p>The full graph is large — about 30,000 entities and 60,000 connections drawn from every letter
      and notebook entry. You never see it all at once (that would be an unreadable hairball). Instead you
      <b>explore it</b>: start somewhere, then walk outward and reorganize as you go. The view always shows
      a focused slice; the count under the search box tells you how much is shown.</p>
      <h4>1. Start somewhere</h4>
      <ul>
        <li><b>Search</b> in the focus box (top) for any name, place, condition, or year and press Focus.</li>
        <li>Or use <b>Find an entity to start from</b> in the settings panel — it lists the most-connected
        entities (filter by kind), so you can discover good starting points. Click one to focus it.</li>
        <li>Click a <b>timeline bar</b> to start from a given year. It opens on Fr. Solanus by default.</li>
      </ul>
      <h4>2. Walk the graph outward (this is the key)</h4>
      <ul>
        <li><b>Double-click any node to EXPAND it</b> — its connections are pulled into the view and added
        to what's already there. Keep double-clicking to travel across the whole graph, node by node,
        reaching any of the thousands of connections on demand.</li>
        <li><b>Single-click</b> a node for its full dossier (summary, connections with sources, biography).</li>
        <li><b>Load more</b> (settings panel) pulls in neighbours when a busy hub was capped for legibility.</li>
        <li><b>Reset</b> returns to the high-level backbone; the <b>trail</b> above the graph is your path back.</li>
      </ul>
      <h4>3. Reorganize what you see</h4>
      <ul>
        <li><b>Legend, entities row</b> — click a kind to <b>hide or show</b> it (e.g. hide conditions to see
        just people and places).</li>
        <li><b>Legend, relationships row</b> — click a relationship to spotlight just those links.</li>
        <li><b>Relationships</b> dropdown — show only one kind of link across the network.</li>
        <li><b>Layout</b> — "clusters" groups things that connect; "importance rings" puts the most-connected
        in the centre. <b>Spacing</b> spreads nodes apart (then press <b>Fit</b>).</li>
        <li><b>Min co-occurrence</b> hides weak "appears with" links; <b>timeline drag</b> filters to a year range.</li>
        <li><b>Colour nodes by community</b> (settings) recolours everything by its thematic cluster
        (found automatically); open <b>Communities</b> to jump straight into a cluster.</li>
      </ul>
      <h4>Reading it</h4>
      <ul>
        <li><b>Colour + shape</b> = entity kind (person round, place diamond, organization box, condition
        hexagon, ...). <b>Size</b> = how often it's mentioned.</li>
        <li><b>Line colour</b> = relationship kind. <b>Fainter / dashed</b> lines are less certain (an inferred
        co-occurrence rather than a stated fact). The thick-ringed node is the one you focused.</li>
      </ul>` },
    map: { title: "Using the map", html: `
      <p>The map plots the <b>real places</b> named in the archive — Fr. Solanus's friaries, the towns
      petitioners wrote from, and the places he travelled.</p>
      <ul>
        <li>A marker's <b>size</b> reflects how often the place is mentioned.</li>
        <li><b>Click a marker</b> to open that place's dossier: a short summary plus every source passage
        that mentions it (each opens the scanned page).</li>
        <li><b>Open Street View here</b> opens Google Street View at the marker's coordinates.</li>
        <li>Streets and vague labels (a continent, a river) are not pinned; a building or institution is
        placed at its city so the map stays meaningful.</li>
      </ul>` },
    tools: { title: "The retrieval tools", html: `
      <p>The <b>Ask</b> tab is a research assistant that answers <b>only</b> from this archive and cites
      every claim. It works by calling <b>tools</b> — each one a capability you can switch on or off in
      <b>Settings</b>. The trace under each answer shows exactly which tools it used.</p>
      <ul>
        <li><b>vector search</b> — finds passages by meaning (semantic search over the letters & notebooks).</li>
        <li><b>full-text search</b> — finds an exact word or phrase on a page.</li>
        <li><b>entity lookup</b> — resolves a name to a canonical person/place and its mentions.</li>
        <li><b>graph query</b> — walks the knowledge graph to find relationships and dates.</li>
        <li><b>temporal query</b> — lists records inside a date range, in order.</li>
        <li><b>community summary</b> — themes across a whole cluster of the archive.</li>
        <li><b>book search</b> — the Crosby biography (<i>Thank God Ahead of Time</i>), a secondary source,
        cited by book page. Off by default; turn it on in Settings to let answers draw on it.</li>
      </ul>
      <p>Every answer's sources are listed beneath it — click one to open the scanned page it came from.</p>` },
  };
  function openHelp(topic) {
    const h = HELP[topic];
    if (!h) return;
    $("#help-title").textContent = h.title;
    $("#help-body").innerHTML = h.html;
    $("#help-backdrop").classList.add("open");
    document.body.style.overflow = "hidden";   // lock background scroll so only the modal scrolls
  }
  function closeHelp() {
    const b = $("#help-backdrop");
    if (b) b.classList.remove("open");
    document.body.style.overflow = "";
  }

  // Go.
  document.addEventListener("DOMContentLoaded", boot);
})();
