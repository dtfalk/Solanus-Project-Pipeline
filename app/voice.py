"""voice.py — backend voice I/O with SELECTABLE providers: local, Azure, GCP.

TTS:  local = Piper (neural, offline)  | azure = Azure Speech  | gcp = Google Cloud Text-to-Speech
STT:  local = faster-whisper (offline) | azure = Azure Speech  | gcp = Google Cloud Speech-to-Text

The provider is chosen per request (the site picker sends it) or falls back to env defaults. Cloud calls use
plain REST + a key, so no heavy SDKs. The cloned Solanus voice later becomes another provider here.

Credentials (env):
  AZURE_SPEECH_KEY, AZURE_SPEECH_REGION        (e.g. eastus)
  GCP_API_KEY  (or GOOGLE_API_KEY)             (API key with TTS + STT enabled)
Optional defaults:
  VOICE_TTS_PROVIDER / VOICE_STT_PROVIDER      (local|azure|gcp; default local)
  PIPER_VOICE, WHISPER_MODEL, AZURE_TTS_VOICE, GCP_TTS_VOICE
"""
from __future__ import annotations
import base64
import io
import json
import os
import re
import tempfile
import urllib.request
import wave
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
# Load the project's keys (same .env the rest of the app uses) so the cloud providers see their creds
# regardless of import order. Add AZURE_SPEECH_KEY / AZURE_SPEECH_REGION / GCP_API_KEY there.
try:
    from dotenv import load_dotenv
    load_dotenv(_REPO / "pipeline_v3" / "step_7" / ".env")
except Exception:
    pass

PIPER_VOICE = os.environ.get("PIPER_VOICE", str(_REPO / "pipeline_v3" / "voice" / "piper" / "en_US-lessac-medium.onnx"))
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "base.en")
# Azure AI Foundry resource (custom endpoint → bearer-token auth). Reads the project's actual var names
# (AZURE_AI_CORE_TOOLS_*), with AZURE_FOUNDRY_* / AZURE_SPEECH_* as fallbacks.
AZURE_KEY = (os.environ.get("AZURE_AI_CORE_TOOLS_KEY") or os.environ.get("AZURE_FOUNDRY_KEY")
             or os.environ.get("AZURE_SPEECH_KEY", ""))
AZURE_ENDPOINT = (os.environ.get("AZURE_AI_CORE_TOOLS_ENDPOINT") or os.environ.get("AZURE_FOUNDRY_ENDPOINT")
                  or os.environ.get("AZURE_SPEECH_ENDPOINT", "")).rstrip("/")
AZURE_REGION = (os.environ.get("AZURE_AI_CORE_TOOLS_REGION") or os.environ.get("AZURE_FOUNDRY_REGION")
                or os.environ.get("AZURE_SPEECH_REGION", ""))
AZURE_TTS_VOICE = os.environ.get("AZURE_TTS_VOICE", "en-US-AndrewNeural")          # a calm male voice
# Dignified-priest delivery: measured pace + slightly lower pitch (SSML prosody, applied to all Azure TTS).
# Tune live via env AZURE_TTS_RATE / AZURE_TTS_PITCH; set both to "" to disable.
AZURE_TTS_RATE = os.environ.get("AZURE_TTS_RATE", "-12%")
AZURE_TTS_PITCH = os.environ.get("AZURE_TTS_PITCH", "-6%")
# Curated voices that read as an older, dignified clergyman — the site's voice picker lists these,
# and any custom Azure voice name can be imported (typed in) since the name passes straight through.
AZURE_VOICE_CATALOG = [
    {"name": "en-US-AndrewNeural",              "label": "Andrew — warm, calm (US)"},
    {"name": "en-US-AndrewMultilingualNeural",  "label": "Andrew — natural & warm (US, flagship)"},
    {"name": "en-US-RogerNeural",               "label": "Roger — mature, steady (US)"},
    {"name": "en-US-GuyNeural",                 "label": "Guy — measured, gravitas (US)"},
    {"name": "en-US-DavisNeural",               "label": "Davis — gentle, reflective (US)"},
    {"name": "en-US-BrianMultilingualNeural",   "label": "Brian — soft, kindly (US)"},
    {"name": "en-GB-RyanNeural",                "label": "Ryan — refined (British)"},
    {"name": "en-GB-AlfieNeural",               "label": "Alfie — older, gentle (British)"},
    {"name": "en-IE-ConnorNeural",              "label": "Connor — Irish (Solanus's heritage)"},
]


def azure_voice_catalog() -> dict:
    """The voice picker's options + the active default/prosody, for the Settings UI."""
    return {"default": AZURE_TTS_VOICE, "rate": AZURE_TTS_RATE, "pitch": AZURE_TTS_PITCH,
            "voices": AZURE_VOICE_CATALOG}


# A standalone 1700-2099 number — i.e. a year OR a 4-digit house number. Both should be spoken as digit
# PAIRS ("nineteen fifty-seven", "seventeen forty Mount Elliott Ave"), which is exactly what date/format=y
# does — and what raw TTS gets wrong ("one thousand seven hundred forty"). Bounded so it can't fire inside a
# longer number or a decimal. (3-digit house numbers like "225 Jerome St" already read correctly as cardinals.)
_YEAR_RE = re.compile(r"\b(1[789]\d{2}|20\d{2})\b(?!\.\d)")   # \b excludes longer numbers; (?!\.\d) excludes decimals but allows a sentence-ending '1957.'


def _say_as_types(escaped: str) -> str:
    """Tag the spans whose TYPE we know so the engine says them right (SSML say-as). Operates on
    already-XML-escaped text; the tags it inserts are the only '<' in the result."""
    return _YEAR_RE.sub(lambda m: f"<say-as interpret-as='date' format='y'>{m.group(1)}</say-as>", escaped)


def _build_ssml(text: str, voice: str | None) -> str:
    """SSML wrapper with dignified prosody + type-aware say-as, shared by plain TTS and the viseme/avatar path."""
    v = voice or AZURE_TTS_VOICE
    safe = (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe = _say_as_types(safe)                       # speak years/4-digit numbers as digit pairs, not cardinals
    inner = safe
    if AZURE_TTS_RATE or AZURE_TTS_PITCH:
        inner = (f"<prosody rate='{AZURE_TTS_RATE or '0%'}' pitch='{AZURE_TTS_PITCH or '0%'}'>"
                 f"{safe}</prosody>")
    return (f"<speak version='1.0' xmlns:mstts='http://www.w3.org/2001/mstts' xml:lang='en-US'>"
            f"<voice xml:lang='en-US' name='{v}'>{inner}</voice></speak>")
GCP_KEY = os.environ.get("GCP_API_KEY") or os.environ.get("GOOGLE_API_KEY", "")
GCP_TTS_VOICE = os.environ.get("GCP_TTS_VOICE", "en-US-Neural2-D")                 # male
DEFAULT_TTS = os.environ.get("VOICE_TTS_PROVIDER", "local")
DEFAULT_STT = os.environ.get("VOICE_STT_PROVIDER", "local")

_piper = _whisper = None


def _post(url, data, headers, timeout=60):
    import urllib.error
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        body = e.read()[:300].decode("utf-8", "ignore")
        hint = ""
        if e.code in (401, 403):
            hint = (" — check the key/region; on an Azure AI Foundry resource make sure KEY-BASED AUTH IS "
                    "ENABLED (Foundry can default to Entra-only)") if "speech.microsoft" in url or "cognitive" in url else \
                   " — check the API key and that the API is enabled"
        raise RuntimeError(f"http_{e.code}: {body}{hint}")


# ----------------------------------------------------------------------------- format helper
def _to_wav(data: bytes, rate: int = 16000) -> bytes:
    """Decode any recorded clip (webm/opus, ogg, mp3, wav) to 16k mono PCM WAV via PyAV (a faster-whisper dep)."""
    import av
    container = av.open(io.BytesIO(data))
    resampler = av.audio.resampler.AudioResampler(format="s16", layout="mono", rate=rate)
    pcm = bytearray()
    for frame in container.decode(audio=0):
        out = resampler.resample(frame)
        for f in (out if isinstance(out, list) else [out]):
            if f:
                pcm += bytes(f.planes[0])
    buf = io.BytesIO()
    w = wave.open(buf, "wb"); w.setnchannels(1); w.setsampwidth(2); w.setframerate(rate); w.writeframes(bytes(pcm)); w.close()
    return buf.getvalue()


# ============================================================================= TTS
def _piper_tts(text: str) -> bytes:
    global _piper
    if _piper is None:
        from piper import PiperVoice
        _piper = PiperVoice.load(PIPER_VOICE)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        _piper.synthesize_wav(text, w)
    return buf.getvalue()


_azure_tok = {"v": None, "exp": 0.0}


def _azure_token() -> str:
    """Exchange the key for a ~10-min bearer token at the resource's STS endpoint. Foundry custom-subdomain
    resources require token auth; the regional STS is the fallback when no endpoint is set."""
    import time
    if _azure_tok["v"] and time.time() < _azure_tok["exp"]:
        return _azure_tok["v"]
    sts = (AZURE_ENDPOINT + "/sts/v1.0/issueToken") if AZURE_ENDPOINT else \
          f"https://{AZURE_REGION}.api.cognitive.microsoft.com/sts/v1.0/issueToken"
    tok = _post(sts, b"", {"Ocp-Apim-Subscription-Key": AZURE_KEY, "Content-Length": "0"}).decode("utf-8").strip()
    _azure_tok["v"], _azure_tok["exp"] = tok, time.time() + 540
    return tok


def _azure_auth() -> dict:
    """Prefer bearer-token (works for Foundry custom-subdomain resources); fall back to the raw key."""
    if AZURE_ENDPOINT:
        try:
            return {"Authorization": "Bearer " + _azure_token()}
        except Exception:
            pass
    return {"Ocp-Apim-Subscription-Key": AZURE_KEY}


def _azure_tts(text: str, voice: str | None) -> bytes:
    if not (AZURE_KEY and AZURE_REGION):
        raise RuntimeError("azure_tts_unconfigured: set AZURE_FOUNDRY_KEY + AZURE_FOUNDRY_REGION (+ AZURE_FOUNDRY_ENDPOINT)")
    ssml = _build_ssml(text, voice)
    h = _azure_auth()
    h.update({"Content-Type": "application/ssml+xml", "X-Microsoft-OutputFormat": "riff-22050hz-16bit-mono-pcm",
              "User-Agent": "solanus-archive"})
    return _post(f"https://{AZURE_REGION}.tts.speech.microsoft.com/cognitiveservices/v1", ssml.encode("utf-8"), h)


def synthesize_with_visemes(text: str, voice: str | None = None):
    """Azure Speech SDK synthesis that ALSO captures viseme events, so the in-browser avatar can lip-sync
    to the REAL Solanus voice. Returns (wav_bytes, [{'t': ms_offset, 'id': azure_viseme_id}, ...]).
    Azure-only (GCP/local have no viseme stream); callers fall back to plain /api/tts audio."""
    if not (AZURE_KEY and AZURE_REGION):
        raise RuntimeError("azure_visemes_unconfigured: needs AZURE_FOUNDRY_KEY + AZURE_FOUNDRY_REGION")
    import azure.cognitiveservices.speech as speechsdk
    # auth: prefer the bearer token (Foundry custom-subdomain resources reject the raw key on the
    # regional endpoint — same reason the REST path uses a token); fall back to subscription+region.
    try:
        cfg = speechsdk.SpeechConfig(auth_token=_azure_token(), region=AZURE_REGION)
    except Exception:
        cfg = speechsdk.SpeechConfig(subscription=AZURE_KEY, region=AZURE_REGION)
    cfg.speech_synthesis_voice_name = voice or AZURE_TTS_VOICE
    cfg.set_speech_synthesis_output_format(
        speechsdk.SpeechSynthesisOutputFormat.Riff22050Hz16BitMonoPcm)
    synth = speechsdk.SpeechSynthesizer(speech_config=cfg, audio_config=None)   # in-memory, no speaker
    visemes: list[dict] = []
    # audio_offset is in 100-ns ticks → milliseconds
    synth.viseme_received.connect(
        lambda e: visemes.append({"t": e.audio_offset / 10000.0, "id": e.viseme_id}))
    result = synth.speak_ssml_async(_build_ssml(text, voice)).get()   # SSML → same dignified prosody
    if result.reason != speechsdk.ResultReason.SynthesizingAudioCompleted:
        detail = getattr(result, "cancellation_details", None)
        raise RuntimeError(f"azure_viseme_synth_failed: {result.reason} "
                           f"{getattr(detail, 'error_details', '') if detail else ''}")
    return bytes(result.audio_data), visemes


def _gcp_tts(text: str, voice: str | None) -> bytes:
    if not GCP_KEY:
        raise RuntimeError("gcp_tts_unconfigured: set GCP_API_KEY")
    body = json.dumps({"input": {"text": text}, "voice": {"languageCode": "en-US", "name": voice or GCP_TTS_VOICE},
                       "audioConfig": {"audioEncoding": "LINEAR16"}}).encode("utf-8")
    out = _post(f"https://texttospeech.googleapis.com/v1/text:synthesize?key={GCP_KEY}", body,
                {"Content-Type": "application/json"})
    return base64.b64decode(json.loads(out)["audioContent"])


def synthesize(text: str, provider: str | None = None, voice: str | None = None) -> bytes:
    p = (provider or DEFAULT_TTS).lower()
    if p == "azure":
        return _azure_tts(text, voice)
    if p == "gcp":
        return _gcp_tts(text, voice)
    return _piper_tts(text)


# ============================================================================= STT
def _whisper_stt(audio: bytes) -> str:
    global _whisper
    if _whisper is None:
        from faster_whisper import WhisperModel
        _whisper = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")
    with tempfile.NamedTemporaryFile(suffix=".audio", delete=False) as f:
        f.write(audio); path = f.name
    try:
        segs, _ = _whisper.transcribe(path)
        return " ".join(s.text for s in segs).strip()
    finally:
        try: os.unlink(path)
        except OSError: pass


def _azure_stt(audio: bytes) -> str:
    if not (AZURE_KEY and AZURE_REGION):
        raise RuntimeError("azure_stt_unconfigured: set AZURE_SPEECH_KEY + AZURE_SPEECH_REGION")
    wav = _to_wav(audio)
    url = f"https://{AZURE_REGION}.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US"
    h = _azure_auth()
    h.update({"Content-Type": "audio/wav; codecs=audio/pcm; samplerate=16000", "Accept": "application/json"})
    out = _post(url, wav, h)
    return (json.loads(out).get("DisplayText") or "").strip()


def _gcp_stt(audio: bytes) -> str:
    if not GCP_KEY:
        raise RuntimeError("gcp_stt_unconfigured: set GCP_API_KEY")
    wav = _to_wav(audio)
    body = json.dumps({"config": {"encoding": "LINEAR16", "sampleRateHertz": 16000, "languageCode": "en-US"},
                       "audio": {"content": base64.b64encode(wav).decode("ascii")}}).encode("utf-8")
    out = _post(f"https://speech.googleapis.com/v1/speech:recognize?key={GCP_KEY}", body,
                {"Content-Type": "application/json"})
    res = json.loads(out).get("results") or []
    return " ".join(r["alternatives"][0]["transcript"] for r in res if r.get("alternatives")).strip()


def transcribe(audio: bytes, provider: str | None = None) -> str:
    p = (provider or DEFAULT_STT).lower()
    if p == "azure":
        return _azure_stt(audio)
    if p == "gcp":
        return _gcp_stt(audio)
    return _whisper_stt(audio)


# ============================================================================= capabilities (for the site picker)
def providers() -> dict:
    azure_ok = bool(AZURE_KEY and AZURE_REGION)
    gcp_ok = bool(GCP_KEY)
    local_ok = Path(PIPER_VOICE).exists()
    return {
        "tts": [{"id": "local", "label": "Local (Piper)", "configured": local_ok},
                {"id": "azure", "label": "Azure", "configured": azure_ok},
                {"id": "gcp", "label": "Google", "configured": gcp_ok}],
        "stt": [{"id": "local", "label": "Local (Whisper)", "configured": True},
                {"id": "azure", "label": "Azure", "configured": azure_ok},
                {"id": "gcp", "label": "Google", "configured": gcp_ok}],
        "default_tts": DEFAULT_TTS, "default_stt": DEFAULT_STT,
    }
