// avatar.mjs — photoreal Solanus head viewer (plain three.js; NO rig required).
//
// David's Solanus is a sealed photoreal scan (Tripo): one mesh, one texture, zero blendshapes/bones —
// so the rig-based TalkingHead engine can't drive it. Instead we render the model directly and speak
// each answer in the Azure Solanus voice, with subtle VOICE-REACTIVE head motion so it feels alive.
// (Full mouth lip-sync needs an animation-ready head — the separate audio-driven track — but this keeps
// David's EXACT likeness working in the app today.)
//
// Swap the model: drop any .glb at app/static/vendor/avatar.glb (or set window.SOLANUS_AVATAR_GLB).
import * as THREE from 'three';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { RoomEnvironment } from 'three/addons/environments/RoomEnvironment.js';

const GLB = () => window.SOLANUS_AVATAR_GLB || '/static/vendor/avatar.glb';
let renderer, scene, camera, controls, model, clock, mounted = false;
let speaking = false, speakAmp = 0, baseY = 0, baseRotY = 0;
let jawMesh = null, jawIdx = -1, jawCur = 0;   // jawOpen blendshape (baked in Blender) driven by the voice
let visMesh = null, visIdx = {}, visMode = false, visSeq = null, visStart = 0; // viseme set (real lip-sync)
const visCur = {};                              // eased influence per viseme morph
let alive = false;   // render-loop gate (so hot-swapping the model doesn't stack loops)
// Azure viseme id (0..21) -> one of our 5 baked mouth shapes (null = silence/closed).
const VIS_MAP = [null, 'vOpen', 'vOpen', 'vRound', 'vWide', 'vWide', 'vWide', 'vRound', 'vRound',
  'vOpen', 'vRound', 'vOpen', 'vOpen', 'vRound', 'vWide', 'vWide', 'vRound', 'vWide', 'vFV', 'vWide',
  'vOpen', 'vClose'];
const b64ToBuf = (b64) => { const s = atob(b64), a = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) a[i] = s.charCodeAt(i); return a.buffer; };
const status = (m) => { const el = document.getElementById('avatar-status'); if (el) el.textContent = m || ''; };

async function mount() {
  if (mounted) return;
  const stage = document.getElementById('avatar-stage');
  if (!stage) return;
  mounted = true; status('loading Solanus…');
  const w = stage.clientWidth || 640, h = stage.clientHeight || 340;

  renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  renderer.setSize(w, h);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;   // filmic response → photoreal skin, no blown highlights
  renderer.toneMappingExposure = 1.04;
  stage.appendChild(renderer.domElement);

  scene = new THREE.Scene();
  camera = new THREE.PerspectiveCamera(28, w / h, 0.01, 100);

  // image-based lighting (soft room env) makes the PBR skin read naturally, then a warm 3-point on top.
  const pmrem = new THREE.PMREMGenerator(renderer);
  scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
  scene.add(new THREE.HemisphereLight(0xffffff, 0x6b4a2e, 0.35));
  const key = new THREE.DirectionalLight(0xfff2e2, 1.25); key.position.set(1.4, 1.8, 2.4); scene.add(key);
  const fill = new THREE.DirectionalLight(0xdfe7ff, 0.35); fill.position.set(-2.0, 0.5, 1.4); scene.add(fill);
  const rim = new THREE.DirectionalLight(0xffffff, 0.7); rim.position.set(-0.6, 1.4, -2.4); scene.add(rim);

  clock = new THREE.Clock();

  try {
    const gltf = await new GLTFLoader().loadAsync(GLB());
    model = gltf.scene;
    // center the model at the origin and frame the head/upper body
    const box = new THREE.Box3().setFromObject(model);
    const size = new THREE.Vector3(), center = new THREE.Vector3();
    box.getSize(size); box.getCenter(center);
    model.position.sub(center);
    scene.add(model);
    // find the mouth morphs: either a viseme set (vOpen/vWide/vRound/vClose/vFV — real lip-sync)
    // or the older single jawOpen (amplitude-driven). Viseme set wins when present.
    const VNAMES = ['vOpen', 'vWide', 'vRound', 'vClose', 'vFV'];
    model.traverse((o) => {
      if (!o.isMesh || !o.morphTargetDictionary) return;
      const d = o.morphTargetDictionary;
      if ('jawOpen' in d) { jawMesh = o; jawIdx = d['jawOpen']; }
      if (VNAMES.some((n) => n in d)) {
        visMesh = o; visIdx = {};
        for (const n of VNAMES) if (n in d) visIdx[n] = d[n];
        if (jawIdx < 0 && 'vOpen' in d) { jawMesh = o; jawIdx = d['vOpen']; }   // amplitude fallback
      }
    });
    visMode = !!visMesh;
    window._avatarJawMesh = () => jawMesh;   // debug hook
    window._avatarVis = () => ({ visMode, visIdx });
    baseY = model.position.y;
    // ChatAvatar (current model) faces the camera natively → yaw 0. (Tripo faced +X and needed -PI/2.)
    // Override per-model with window.SOLANUS_AVATAR_YAW.
    baseRotY = (typeof window.SOLANUS_AVATAR_YAW === 'number') ? window.SOLANUS_AVATAR_YAW : 0;
    model.rotation.y = baseRotY;

    const topY = size.y / 2;
    const fitDist = (size.y) / (2 * Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)));
    // pan the framing DOWN so Solanus sits HIGHER in the (tall) window — head in the upper third,
    // shoulders below, rather than floating mid-frame. (Override hooks left for fine-tuning.)
    const camY = (typeof window.SOLANUS_AVATAR_CAMY === 'number') ? window.SOLANUS_AVATAR_CAMY : 0.30;
    const tgtY = (typeof window.SOLANUS_AVATAR_TGTY === 'number') ? window.SOLANUS_AVATAR_TGTY : 0.25;
    camera.position.set(0, topY * camY, fitDist * 1.02);
    const target = new THREE.Vector3(0, topY * tgtY, 0);
    camera.lookAt(target);

    // drag to orient / scroll to zoom — so framing is never a blocker
    controls = new OrbitControls(camera, renderer.domElement);
    controls.target.copy(target);
    controls.enableDamping = true; controls.dampingFactor = 0.08;
    controls.minDistance = fitDist * 0.4; controls.maxDistance = fitDist * 3;
    controls.enablePan = false;
    controls.update();
    status('');
  } catch (e) {
    mounted = false;   // allow a retry — don't get stuck "mounted" after a failed load
    status('could not load the model (' + (e && e.message ? e.message : e) + ')');
  }

  window.addEventListener('resize', onResize);
  alive = true;
  loop();
}

// tear down the current viewer so a different model can be hot-swapped in (avatar chooser)
function teardown() {
  alive = false;
  try { if (renderer) { renderer.dispose(); renderer.domElement.remove(); } } catch (_) {}
  const stage = document.getElementById('avatar-stage');
  if (stage) stage.innerHTML = '';
  renderer = scene = camera = controls = model = null;
  jawMesh = null; jawIdx = -1; jawCur = 0; mounted = false;
  visMesh = null; visIdx = {}; visMode = false; visSeq = null;
  for (const k in visCur) delete visCur[k];
}

function onResize() {
  const s = document.getElementById('avatar-stage');
  if (!s || !renderer || !camera) return;
  const w = s.clientWidth, h = s.clientHeight;
  renderer.setSize(w, h); camera.aspect = w / h; camera.updateProjectionMatrix();
}

function loop() {
  if (!alive) return;
  requestAnimationFrame(loop);
  const t = clock ? clock.getElapsedTime() : 0;
  if (model) {
    // idle: gentle breathe + sway. speaking: layer in amplitude-driven nod so it reads as talking.
    const sway = Math.sin(t * 0.6) * 0.04 + Math.sin(t * 1.3) * 0.012;
    const breathe = Math.sin(t * 1.1) * 0.006;
    model.rotation.y = baseRotY + sway + (speaking ? Math.sin(t * 7) * 0.015 * speakAmp : 0);
    model.rotation.x = breathe + (speaking ? Math.sin(t * 9) * 0.022 * speakAmp : 0);
    model.position.y = baseY + breathe + (speaking ? Math.abs(Math.sin(t * 6)) * 0.009 * speakAmp : 0);
    // drive the mouth. Viseme set: follow the Azure viseme timeline (real phoneme lip-sync); if a
    // viseme answer hasn't loaded, fall back to amplitude on vOpen. Single jawOpen: amplitude open/close.
    if (visMode && visMesh) {
      let active = null;
      if (speaking && visSeq) {
        const elapsed = performance.now() - visStart;
        for (const ev of visSeq) { if (ev.t <= elapsed) active = ev.shape; else break; }
      }
      for (const name in visIdx) {
        let tgt = (name === active) ? 1 : 0;
        if (speaking && !visSeq && name === 'vOpen') tgt = Math.min(1, speakAmp * 1.1);  // amplitude fallback
        visCur[name] = (visCur[name] || 0) + (tgt - (visCur[name] || 0)) * 0.35;
        visMesh.morphTargetInfluences[visIdx[name]] = visCur[name];
      }
    } else if (jawMesh && jawIdx >= 0) {
      const tgt = speaking ? Math.min(1.0, speakAmp * 1.1) : 0;
      jawCur += (tgt - jawCur) * 0.4;
      jawMesh.morphTargetInfluences[jawIdx] = jawCur;
    }
    speakAmp *= 0.9;
  }
  if (controls) controls.update();
  if (renderer && scene && camera) renderer.render(scene, camera);
}

// Speak the answer in the Azure Solanus voice + drive voice-reactive head motion from the audio amplitude.
async function speak(text) {
  text = (text || '').replace(/\[\d+\]/g, '').trim().slice(0, 4000);
  if (!mounted) await mount();
  if (!text) return false;
  if (visMode) {                                  // real viseme lip-sync (falls through to amplitude on failure)
    try { if (await speakVisemes(text)) return true; } catch (_) {}
  }
  try {
    const r = await fetch('/api/tts', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, provider: 'azure', voice: window.SOLANUS_TTS_VOICE || undefined }),
    });
    if (!r.ok) throw new Error('tts ' + r.status);
    const buf = await r.arrayBuffer();
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const audio = await ctx.decodeAudioData(buf.slice(0));
    const src = ctx.createBufferSource(); src.buffer = audio;
    const an = ctx.createAnalyser(); an.fftSize = 256;
    src.connect(an); an.connect(ctx.destination);
    const data = new Uint8Array(an.frequencyBinCount);
    speaking = true;
    const probe = () => {
      if (!speaking) return;
      an.getByteTimeDomainData(data);
      let s = 0; for (let i = 0; i < data.length; i++) { const v = (data[i] - 128) / 128; s += v * v; }
      speakAmp = Math.min(1, Math.sqrt(s / data.length) * 6);
      requestAnimationFrame(probe);
    };
    src.onended = () => { speaking = false; speakAmp = 0; try { ctx.close(); } catch (_) {} };
    src.start(); probe();
    return true;
  } catch (e) {
    if (window.speechSynthesis) { speechSynthesis.cancel(); speechSynthesis.speak(new SpeechSynthesisUtterance(text)); }
    return true;
  }
}

// Viseme lip-sync: fetch Azure audio + viseme timings and drive the 5 mouth morphs on that timeline.
async function speakVisemes(text) {
  const r = await fetch('/api/tts_visemes', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text, provider: 'azure', voice: window.SOLANUS_TTS_VOICE || undefined }),
  });
  if (!r.ok) throw new Error('visemes ' + r.status);
  const j = await r.json();
  const ctx = new (window.AudioContext || window.webkitAudioContext)();
  const audio = await ctx.decodeAudioData(b64ToBuf(j.audio));
  const src = ctx.createBufferSource(); src.buffer = audio; src.connect(ctx.destination);
  visSeq = (j.visemes || []).map((v) => ({ t: v.t, shape: VIS_MAP[v.id] ?? null }))
                            .sort((a, b) => a.t - b.t);
  speaking = true; speakAmp = 0.6;
  src.onended = () => { speaking = false; speakAmp = 0; visSeq = null; try { ctx.close(); } catch (_) {} };
  src.start(); visStart = performance.now();
  return true;
}

// switch which model is shown (avatar chooser in Settings): set the url + per-model yaw, then re-mount
function setModel(url, yaw) {
  window.SOLANUS_AVATAR_GLB = url;
  if (typeof yaw === 'number') window.SOLANUS_AVATAR_YAW = yaw;
  teardown();
  mount();
}
window.solanusAvatar = { mount, speak, setModel, teardown, get ready() { return mounted; } };
window.dispatchEvent(new Event('solanus-avatar-ready'));
