# Style guide — match David's code & writing

Learned from `/Workspace/Projects/Solanus-Project-Pipeline/examples/`. ALL step_7/app code and ALL
documentation must follow this. When in doubt, open the examples and imitate them directly.

## Code (Python) — see `examples/code_examples/ipsos_code/*.py`, `diffusion_training/train.py`
The voice is **pedagogical and generous**: code should teach a smart beginner *why*, not just *what*.

- **Section banners** with full-width dividers:
  ```python
  # ==================================================================
  # What this section does, in plain language
  # ==================================================================
  ```
  and `# -----` sub-dividers inside functions for distinct steps.
- **Grouped imports** under little headers (`# Core Python Imports`, `# Local File Imports`).
- **Aligned `=`** in multi-line keyword args / config blocks:
  ```python
  client = AzureOpenAI(
      api_version    = os.environ["API_VERSION"],
      azure_endpoint = os.environ["AZURE_ENDPOINT"],
  )
  ```
- **Google-style docstrings** (Args:/Returns:) on every function.
- **Teach-the-why inline comments** — frequent, conversational, with analogies where a concept is
  subtle (cf. the `f.tell()` mini-lesson). It is normal and welcome to explain a tricky idea in a
  short paragraph comment, even suggesting how to learn more. Comment density is HIGH.
- **Cost/token tracking is first-class** (cf. run_prompts.py cost block) — route every model call
  through `lib/costlog.py`; print a `=`*60 SUMMARY block at the end of runnable scripts.
- Descriptive names; type hints; `logging` for progress; `if __name__ == "__main__": main()`.
- Non-destructive: write new artifacts, never overwrite source data in place.

## Writing / documentation — see `examples/writing_examples/*/*.tex` (+ .pdf) and the mimicry reports
The voice **builds intuition from first principles, warmly and concretely** — the reader should
come away *understanding*, not just informed.

- Structure: a **TL;DR** (bold-lead bullets) → **Key points** → **Details** (subsections that build
  up) → **Recommendations / how-to** → **Caveats** → **Sources**. (cf. `style-mimickry-report.md`.)
- Lead with intuition and analogy, then formalize. Bold the key terms. Use concrete examples from
  *this* corpus (Solanus letters/notebooks). Cite sources inline.
- Each major component ships docs in **BOTH** forms:
  - **Markdown** (`<name>.md`) — the quick-read version.
  - **PDF from LaTeX** — `.tex` matching the example explainers' packages/section style/voice;
    compiled to `<name>.pdf`. **All LaTeX aux/intermediary files (.aux/.log/.out/.fls/.fdb/.toc,
    etc.) live in a `build/` subfolder** (e.g. `latexmk -outdir=build`), so only `<name>.tex` and
    `<name>.pdf` sit in the doc directory.

## Quick rules
- Imitate the examples over inventing conventions.
- Comments and docs explain *why*, teaching tone, generous.
- Every paid call cost-logged; nothing destructive; reproducible.
