# Wiring the Solanus "translator" (style model) to Azure — morning checklist

**What it is:** a fine-tuned **Qwen2.5-7B-Instruct + LoRA** (`adapters/translator-qwen_fast`) that rewrites
**plain modern English → Father Solanus Casey's writing voice** (a *style* translator, fact-preserving).
The app code is already in place and **no-ops until you set two env vars** — nothing is deployed.

## Already done (in the repo)
- `app/translate.py` — provider adapter. Sends `[{system: TRANSLATOR_SYS}, {user: text}]` to your endpoint
  over plain `urllib`. `configured()` is true only when the endpoint + key envs exist.
- `app/server.py` — `GET /api/translate` (status) and `POST /api/translate {text}` → `{input, styled, direction}`.
  Verified: with no env set, GET reports `configured:false` and POST returns 503.
- The system prompt is imported verbatim from `pipeline_v3/mimicker/prompts.py` (`TRANSLATOR_SYS`) — the LoRA
  was trained on it, so it must match at inference (the route already sends it).

## What you do in the morning (deploy + set env)
1. **Merge the LoRA → full weights** (so the endpoint serves one model):
   ```python
   from peft import PeftModel; from transformers import AutoModelForCausalLM, AutoTokenizer
   base = AutoModelForCausalLM.from_pretrained("Qwen/Qwen2.5-7B-Instruct")
   m = PeftModel.from_pretrained(base, "pipeline_v3/mimicker/adapters/translator-qwen_fast").merge_and_unload()
   m.save_pretrained("merged-solanus-translator"); AutoTokenizer.from_pretrained("Qwen/Qwen2.5-7B-Instruct").save_pretrained("merged-solanus-translator")
   ```
2. **Deploy** the merged model to a GPU endpoint that exposes an OpenAI-style `/chat/completions`:
   - **Azure ML managed online endpoint** with the vLLM / foundation-model image (e.g. `Standard_NC24ads_A100_v4`), or
   - **Azure AI Foundry** → upload as a custom model → managed/serverless deployment.
3. **Copy two values** from the deployment: the **scoring/Consume URL** and the **primary key**.
4. **Add to `pipeline_v3/step_7/.env`** (the file the app already loads):
   ```
   AZURE_TRANSLATOR_ENDPOINT=<the /chat/completions (or /score) URL>
   AZURE_TRANSLATOR_KEY=<primary key>
   # only if you used a custom Azure ML scoring script instead of an OpenAI-shaped server:
   # AZURE_TRANSLATOR_API_STYLE=azureml
   # AZURE_TRANSLATOR_DEPLOYMENT=<deployment name>
   ```
5. **Smoke test** (no redeploy needed — the app reads env at startup, so restart the server first):
   ```
   curl localhost:8000/api/translate                                  # -> configured:true
   curl -X POST localhost:8000/api/translate -H 'Content-Type: application/json' \
        -d '{"text":"Please be kind to people who treat you badly."}'  # -> {styled: "..."}
   ```
   If the response shape differs, set `AZURE_TRANSLATOR_API_STYLE=azureml` or tweak the parse keys in
   `app/translate.py` (`_call`).

## Gotchas
- The endpoint must serve **exactly** base `Qwen2.5-7B-Instruct` + this LoRA (or their merge), or the style breaks.
- Keep the served system prompt = `TRANSLATOR_SYS` (the route sends it; don't let the deployment inject its own).
- This is a single restyle call — it does **not** include the big-model faithfulness verify/repair loop from
  `restyle_flow.py`. That loop can later call this endpoint for the restyle step if you want the guarded version.
- Direction is modern→Solanus. If you actually want archive→modern, that's a *different* adapter (not trained yet).
