# Conversation architecture — the three message layers (Solanus voice)

The chat is a **three-layer conversation**. Each turn produces three texts, and it matters which one
the user sees and which one becomes the conversation's memory.

```
turn N arrives
   │
   ▼
[ Orchestrator + BIG model ]   ← mutable user-facing system prompt (persona), FULL history,
   │                              RAG context. Owns the SUBSTANCE.
   │  plain reasoned reply for THIS turn, with [n] citations   → the "plain" layer
   ▼
[ Translator ]                 ← stateless, per-turn; sees ONLY this reply; owns the VOICE.
   │  the trained reverse-desanitizer (Azure) if live, else a frozen big-model restyler
   ▼
[ Verify ⇄ Repair + re-attach [n] ]   ← faithfulness of styled vs plain; the styled answer is
   │                                     rejected/retried unless its [n] set EXACTLY equals plain's
   ▼
styled_cited → user            ← what the visitor SEES (his voice, footnotes intact)
```

## The three layers

| Layer | Field | Who sees it | Role |
|-------|-------|-------------|------|
| **user** | `query` | — | what the visitor typed |
| **plain** | `answer_plain` | nobody (backstop) | the big model's reasoned, cited reply; kept for debug/eval/regeneration |
| **styled_cited** | `answer` | the visitor | the same content in Fr. Solanus's voice, `[n]` preserved |

**The canonical conversation is `styled_cited`.** On turn N+1 the model sees the prior turns'
`styled_cited` as its own assistant messages — *exactly what the user saw and is reacting to* — so
references resolve and citations survive. `plain` never needs to enter the model's context (a one-line
switch if a very long chat ever drifts the model's own register archaic).

## Where it lives in code

- **Reasoning layer:** `app/server.py` `single_shot_rag` → `_synthesize_cited_answer` → `plain`.
- **Voice layer:** `app/server.py` `_to_solanus_voice` reuses `pipeline_v3/mimicker/restyle_flow.py`
  (`desanitize`: restyle → `verify` → `repair` → `attach_citations` with the exact-set guarantee) and
  **injects** the app's backend via `_restyle_module` (rebinds `restyle_flow.restyle`):
  - `app/translate.py` `translate()` — the **trained** LoRA reverse-desanitizer, hosted on Azure, when
    `translate.configured()`; else
  - `restyle_flow.restyle_frozen` — a frozen big model + his real exemplars (works **today**, no training).
  All generation routes through `lib.providers.llm`, so it's cost-logged. `RESTYLE_MAX_ITERS` (env,
  default 1) bounds cost/latency: 1 = restyle + one faithfulness check + citation re-attach.
- **Request/response:** `AskRequest.styled` toggles it; the response adds `answer_plain`, `styled`,
  `voice_note`, `voice_consistent`. Non-styled requests are byte-for-byte the old shape.
- **Memory:** `app/static/app.js` `currentRequest` sends the last 6 prior turns as `history`
  (`{q, a: resp.answer}`) — `resp.answer` is `styled_cited` when styling is on, so the styled transcript
  is what feeds back. `_history_block` / `_synthesis_inputs` (server) thread it into the prompt.
- **UI:** a "his voice" toggle (`#voice-styled`, defaults on for the Solanus persona); a small honest
  label + "show plain answer" swap under styled replies (`renderAnswer`).

## Honesty / faithfulness

Styled prose is **an impression for study, not his literal words** — a modern model's reasoning in his
voice. This is deliberately separate from the public avatar/voice track, which is bound to his
DOCUMENTED words only. The `voice_note` label surfaces this in the UI; the vice-postulator authorized
the recreation tracks (no ethics gating), but the label keeps the provenance line clear.

## Status (2026-07-08)

Wired end-to-end and verified with the **frozen** backend (restyle + verify consistent, `[n]` set
preserved on the Bonaventure Frey example; two-turn styled memory holds). The **trained** translator is
not deployed yet — set `AZURE_TRANSLATOR_ENDPOINT` + `AZURE_TRANSLATOR_KEY` in `pipeline_v3/step_7/.env`
and it hot-swaps with no code change (`TRANSLATOR_AZURE_SETUP.md`).
