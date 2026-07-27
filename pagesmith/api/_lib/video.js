// video.js — best-effort server-side video optimization via ffmpeg.
//
// Non-technical users upload huge phone/camera videos. Outside full-quality
// folders we re-encode anything over a size threshold to a web-friendly H.264 MP4:
// downscaled to <= maxWidth, sane CRF, yuv420p, +faststart (so it streams while
// loading). This is a *generic* optimize — it does NOT trim or build loops
// (that's a per-video, content-specific job). It never throws to the caller:
// callers fall back to storing the original if ffmpeg is missing or fails.
//
// Cloud note: a vanilla Consumption Function has no ffmpeg and caps execution
// time. Point FFMPEG_PATH at a bundled static binary (or run the API on a
// container / Premium plan) and keep the preset light. Locally we shell to the
// system ffmpeg (set FFMPEG_PATH; snap builds need MEDIA_TMP_DIR under $HOME).

const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const FFMPEG = process.env.FFMPEG_PATH || 'ffmpeg';
const TMP_BASE = process.env.MEDIA_TMP_DIR || os.tmpdir();

// Re-encode `buffer` to a web-optimized MP4. Resolves to a Buffer, or rejects.
function transcodeToWebMp4(buffer, opts) {
  opts = opts || {};
  const maxWidth = opts.maxWidth || 1920;
  const crf = opts.crf || 28;
  const preset = opts.preset || 'veryfast';

  return new Promise((resolve, reject) => {
    let dir;
    try {
      fs.mkdirSync(TMP_BASE, { recursive: true });
      dir = fs.mkdtempSync(path.join(TMP_BASE, 'engine-vid-'));
    } catch (e) { return reject(new Error('temp dir failed: ' + e.message)); }

    const inPath = path.join(dir, 'in');
    const outPath = path.join(dir, 'out.mp4');
    const cleanup = () => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch (_) {} };

    try { fs.writeFileSync(inPath, buffer); }
    catch (e) { cleanup(); return reject(new Error('write failed: ' + e.message)); }

    const args = [
      '-y', '-i', inPath,
      // downscale only (never upscale); keep even dimensions for yuv420p
      '-vf', `scale='min(${maxWidth},iw)':-2:flags=lanczos`,
      '-c:v', 'libx264', '-preset', preset, '-crf', String(crf),
      '-pix_fmt', 'yuv420p', '-movflags', '+faststart',
      '-c:a', 'aac', '-b:a', '128k',
      outPath
    ];

    let stderr = '';
    let proc;
    try { proc = spawn(FFMPEG, args, { stdio: ['ignore', 'ignore', 'pipe'] }); }
    catch (e) { cleanup(); return reject(new Error('spawn failed: ' + e.message)); }

    proc.stderr.on('data', (d) => { stderr += d.toString(); if (stderr.length > 4000) stderr = stderr.slice(-4000); });
    proc.on('error', (e) => { cleanup(); reject(new Error('ffmpeg error: ' + e.message)); });
    proc.on('close', (code) => {
      if (code !== 0) { cleanup(); return reject(new Error('ffmpeg exited ' + code + ': ' + stderr.slice(-300))); }
      let out;
      try { out = fs.readFileSync(outPath); }
      catch (e) { cleanup(); return reject(new Error('read out failed: ' + e.message)); }
      cleanup();
      resolve(out);
    });
  });
}

module.exports = { transcodeToWebMp4, FFMPEG_PATH: FFMPEG };
