# The Style Mimicker — teaching a frozen model to write in my voice

## TL;DR
- **The whole problem, in one sentence.** I want a model to write the way *I* write, but I have
  forbidden fine-tuning — so I cannot change the model's weights, and I have to smuggle my voice in
  some *other* way and then prove it worked.
- **The trick is to split "voice" into two jobs.** First, *carry* the voice into the model at request
  time by showing it real examples of my writing (retrieval + few-shot). Second, *measure* the voice
  from the outside with an embedding "center of mass" of my prose plus a plain-English stylometry
  audit. One half makes the model sound like me; the other half tells me, honestly, whether it did.
- **Three layers, cheapest and most reliable first.** Layer 1 is retrieved-exemplar prompting (it
  runs today). Layer 2 is a StyleVector-style nudge (scaffolded — the real version needs an open
  model). Layer 3 is a Horikawa-style refinement loop that *evolves* a draft toward my voice
  (scaffolded and gated, because it needs a model I haven't downloaded). Nothing here trains, and
  nothing costs a cent until I explicitly ask.

## Key points
- **No fine-tuning means the style lives in the prompt, not the weights.** This sounds like a
  limitation, and it is, but it is also clarifying: it forces every part of the design to be
  inspectable. There is no mystery tensor that "knows" my voice; there is a folder of my actual
  sentences and a number that says how close the output is to them.
- **Retrieval beats a generic instruction.** Telling a model "write like David" is weak. Showing it
  the *three paragraphs of mine that are most relevant to this exact topic* is strong, because it
  anchors the model on my subject and my manner at the same time.
- **A frozen model will happily fake my opinions, so we only ask it to fake my manner.** The
  research is blunt about this: prompting reproduces surface voice well but confabulates *stances*.
  So the instruction is explicitly "imitate the manner, not the content," and the evaluation measures
  style, not belief.
- **You must measure, or you are fooling yourself.** A model that *feels* like me on one example
  tells you nothing. The evaluator gives a single neural-cosine score, a dimension-by-dimension
  z-score audit, and an adversarial "could a classifier catch this?" estimate.

## Details

### 1. What "data prep" actually does (and why it is the real product)
`data_prep.py` reads my writings — right now the LaTeX explainers in `examples/writing_examples` —
and does three things. It **strips the LaTeX scaffolding** so what remains is prose, because my style
lives in the sentences, not in `\usepackage{tikz}`. It **segments** that prose into exemplars of
roughly 40–180 words. That band is not arbitrary: the stylometry literature says count-based features
are basically noise below ~100 words and get diluted across too many ideas above ~180, so a
paragraph-ish window is the sweet spot. And, when I pass `--embed`, it **embeds** every exemplar and
saves two artifacts: a retrieval index (so I can later fetch the exemplars nearest a new prompt) and
a **style centroid**.

The centroid deserves a sentence of intuition. Imagine every passage I have ever written as a point
in a high-dimensional space, where "near" means "stylistically similar." The centroid is the *center
of mass* of that cloud — the single point that best represents "the average of my voice." Because the
embedder hands back unit vectors, asking "how close is this new text to the centroid?" is just a
cosine, a number between −1 and 1 where bigger means more like me. That one number is the target the
refinement loop steers toward and the anchor the evaluator scores against.

### 2. Generation, in three layers
**Layer 1 — retrieved exemplars + few-shot (this runs today).** Given a prompt, embed it, pull the
`k` nearest exemplars from the index, and lay them in front of a *frozen* model with the instruction:
here is how David writes; now answer in that voice, imitating the manner and not inventing his
opinions. This is the workhorse. It is also the honest baseline the research recommends: prompting
does style "okay," and showing relevant real examples is the cheapest way to push "okay" toward
"good."

**Layer 2 — a StyleVector-style nudge (scaffolded).** There is a lovely result that a person's style
is, roughly, a single *direction* in a model's internal activation space, and that you can steer a
frozen model along that direction without any training. The catch is that you need to reach inside the
model to do it, and a closed API like Gemini will not let you. So the version I can run honestly today
is a *described* direction — I write down the axes where my voice leans (analogy-first, explain the
why, vary the sentence rhythm) and put that in the prompt. The genuine activation-space version is
written up as a clear TODO that needs a small open model with hidden-state access.

**Layer 3 — Horikawa-style refinement (scaffolded and gated).** This is the ambitious, research-y
part. Horikawa's "mind captioning" work evolves a sentence so that its meaning-vector matches a target
that was *decoded from a brain*. The transfer is almost too clean: replace "brain-decoded target"
with "my style centroid," and you get a loop that repeatedly **masks** a few words, asks a masked
language model to **propose** natural fillers, and **keeps** whichever candidate's style embedding is
closest to my centroid — all while a length penalty stops it from cheating by collapsing to something
trivially short. I have implemented the control flow and the fitness function (the fitness is just
cosine-to-centroid times that length penalty, which I can compute right now). The one missing piece is
the masked-LM proposer, which needs RoBERTa-large locally — a heavy download I have not approved — so
the loop is `enabled=False` by default and, if you turn it on, it tells you exactly what it would do
and changes nothing.

### 3. Evaluation, three ways
The first metric is the **neural cosine** to my centroid — the primary "how much like David?" number,
in the spirit of LUAR and StyleDistance. I report it alongside the cosine to the *single nearest*
exemplar, because a bland, generic output can drift toward the bland average and score deceptively
well; it is much harder to also sit close to a real, specific passage of mine.

The second metric is the **interpretable stylometry dashboard**, and this is the one I actually read.
It profiles my exemplars on a small, robust feature set — function-word rates (pronouns, articles,
prepositions: the famously involuntary, topic-independent tells), sentence-length mean and variance
(rhythm and burstiness), punctuation habits, vocabulary richness — and then reports, per feature, how
many standard deviations a candidate deviates from my range. The output reads like a teacher's margin
notes: "sentences too long (z = +1.2), too few first-person pronouns (z = −2.4)." Embeddings can tell
you *that* something is off; only this can tell you *what*.

The third metric is the **authorship-attribution discriminator** — the adversarial judge. The
strongest honest test of a clone is whether a classifier can separate it from the real thing. The
proper version trains a small model on my writing versus the clone's and reports how easily it
separates them (closer to a coin flip is better). That needs scikit-learn, which is not yet in the
environment, so for now there is a transparent nearest-centroid fallback that returns a
"clone-probability," and the trained classifier is written up as a TODO.

### 4. Why this is honest about its limits
Two failure modes are documented in the research and worth saying out loud. First, **embedding
metrics can be gamed** — you can land near the centroid while sounding nothing like me — which is
precisely why the stylometry dashboard exists as a tripwire. Second, **interpretable features can
also be gamed** — you can hit my function-word rates while butchering the syntax — which is why the
embedding cosine and (eventually) the discriminator sit beside them. No single number is trustworthy;
the three together are much harder to fool, and a human who knows my writing is the final judge.

## Recommendations / how-to
- **Start free.** Run `data_prep.py` with no flags to build the exemplars and read
  `style_profile.json`. Run `evaluate.py` with no arguments — it scores a real exemplar of mine as a
  sanity check, and a real passage *should* come out very David-like.
- **Build the index when ready.** `data_prep.py --embed` is the first paid step; it embeds the
  exemplars and saves the centroid. Everything in `generate.py --run` and `evaluate.py --neural`
  depends on it.
- **Generate, then always evaluate.** Treat `generate.py --run` and `evaluate.py` as a pair. A draft
  you have not measured is just a vibe.
- **Turn on the advanced layers deliberately.** `--steer` is safe and cheap. `--refine` is gated and
  will not do anything until RoBERTa-large is wired in — by design.

## Caveats
- **Confabulated opinions are the real danger.** The model can invent stances I never held. The
  guardrail is the instruction ("manner, not content") plus the fact that the evaluation only scores
  *style*. If I ever want faithful *opinions*, that is a different, harder system (retrieval-grounded
  stance) and not what this toolkit promises.
- **Short text is unreliable.** All the count-based features wobble on short inputs; judge on
  paragraph-sized outputs and aggregate over several.
- **The corpus is small for now.** Three explainers is enough to prototype, not to capture the full
  range of my voice. The fix is simply to point `CORPUS_DIRS` at more of my writing.
- **The neuroscience analogy has a seam.** Horikawa optimizes toward a *measured* neural target; here
  the target is a corpus-derived embedding, which is itself a lossy proxy for "my voice." The idea is
  promising but unproven for this use, so it is a final polish, not the foundation.

## Sources
- `Style-Mimickry-Research/style-mimickry-report.md` — the architecture report (STRAP, LUAR,
  StyleDistance, StyleVector, ASTRAPOP, Horikawa mind-captioning, proxy-tuning).
- `Style-Mimickry-Research/LICW-incorporation.md` — the interpretable-stylometry report (LIWC-22,
  Writeprints, MTLD, the composite-objective + dashboard design).
- Reused infrastructure: `pipeline_v3/step_7` (`config`, `lib/costlog`, `lib/providers/embed`,
  `lib/providers/llm`, `lib/vectorstore`).
