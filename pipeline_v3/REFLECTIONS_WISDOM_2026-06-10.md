# Recovering Holy Wisdom — a reflection on the project through the science of wisdom

_2026-06-10. Written at David's request ("free time to explore and think"), reading
Project_Thoughts.docx against the University of Chicago Center for Practical Wisdom's research
program and the project's own six-week engineering record. A thinking document, not a plan.
Sources at the end._

---

## 1. The pipeline has been running a wisdom experiment without calling it that

The Center for Practical Wisdom's working consensus — the "common wisdom model" from the
Toronto Wisdom Task Force — says wisdom has two load-bearing parts: **perspectival
metacognition** (epistemic humility, awareness of context and change, perspective-taking,
balancing viewpoints) and **moral grounding** (orientation toward shared humanity and truth
over self-interest). Nusbaum's formulation of the first part is the sharpest: epistemic
humility is *knowing the size of your piece of knowledge relative to how much there is to
know* — and his strongest empirical claim is that wisdom is not a native talent but a skill
that grows through specific kinds of experience.

Set that next to this repo's iteration log and the mapping is almost embarrassing:

- **Epistemic humility, learned through humiliation.** Every major failure in six weeks had
  one shape: a machine treating its inference as knowledge. The 55-agent "gold consistency
  audit" that David dismissed wholesale; the prompt rules an assistant invented that his gold
  later contradicted; the browse-pollution episode where the system *manufactured fake gold*
  and then showed it back to him as evidence. The fix was never better inference. It was
  architectural: evidence classes ([D]avid / [M]easured / [W]orkflow-verdict /
  [A]ssistant-assumed) in EPISTEMIC_AUDIT.md, GATE dispositions, `reviewed/` as David-only
  ground truth, the standing rule *machines measure, David decides*. That rule is Nusbaum's
  definition of epistemic humility translated into file permissions.
- **Context awareness.** The single most expensive lesson — Volume_4 failing twice — was the
  discovery that labeling conventions are *local*: the general notebook rule was wrong for a
  casebook of individual people. The system's answer (per-volume and per-cluster convention
  notes that David authors as plain text files) is context-sensitivity made mechanical.
- **Perspective-taking.** The recent tooling is all perspective machinery: contact sheets so
  David can see what the clustering sees; the connection overlay so he can see what the model
  asserted without clicking through boxes; triage lists so the machine can show him where it
  *doubts itself*. Grossmann's "Solomon's paradox" result — people reason more wisely about
  problems viewed from a third-person stance — is effectively what the review tooling does: it
  converts first-person labeling into third-person inspection, cheaply.
- **Balancing viewpoints.** The staged HITL flow is an integration mechanism: machine
  throughput and human authority alternating at chunk-sized checkpoints, so neither runs
  unsupervised for long. Even yesterday's small feature request carried the theme — David
  asked the color gradient to respect the just-noticeable difference of human vision, i.e.,
  *do not assert distinctions the perceiver cannot perceive; when you exceed perception,
  reorder rather than insist.* That is epistemic humility as UI design.
- **Moral grounding.** The Volume_4 per-person convention is the quiet center of this. David
  fought two failed runs to establish that the atomic unit of the casebook is *one suffering
  person* — name, condition, what happened — never merged into a date-span for geometric
  convenience. That is a decision about what deserves attention, and it happens to be the same
  decision Solanus made every day at the door. The data structure now encodes the man's
  attention structure.

The conclusion I draw from the record: **wisdom, in this project, has so far been a property
of the system, not of any model in it.** The models barely changed across the failures and
recoveries; the *arrangement* changed — who gates what, what counts as evidence, where human
judgment is irreplaceable. This matches the strongest finding in the wisdom literature, that
wise reasoning varies more across situations than across persons: ecology beats disposition.
For the grant, that converts into a thesis: do not ask whether an AI can be wise; ask whether
an assemblage — corpus, retrieval constraints, interaction norms, human stewards, and the
ritual context of the Detroit Center — can reliably produce *wise encounters*.

## 2. Solanus is not just the subject of the system; he is its design spec

The fact about Solanus Casey that should govern the entire technical architecture is the one
the project documents mention least: he was a **simplex priest**. Judged academically
inadequate, he was ordained without faculties to preach doctrine or hear confessions — a
formal, lifelong epistemic restriction. His response: "In order to practice humility we must
experience humiliations." And his entire ministry happened *through* the restriction, not
despite it: fifty years as a doorkeeper, one visitor at a time, listening rather than
expounding, enrolling the sick, deflecting every success ("Blessed be God in all His
designs"), and telling people to *thank God ahead of time* — which, read with a wisdom
scientist's eyes, is epistemic humility about the future fused to gratitude, exactly the
interaction of intellectual and moral virtue Nusbaum's lab studies.

So the deepest fidelity requirement for an AI Solanus is not stylistic or factual. It is
**structural: the system must inherit his restriction.** A hologram that answers every
theological question fluently would get the words right and the man wrong — it would be, in
the precise sense, a *simplex violation*. Concretely:

1. **A simplex architecture.** Enumerate what the AI may NOT do as its core fidelity feature,
   mirroring the canonical restriction: no doctrinal exposition, nothing confession-shaped,
   explicit deflection of authority ("ask the friars"; "that belongs to God"). Refusal
   behavior, designed well, is the highest-fidelity behavior available. The most authentic
   sentence the system could utter is some variant of *"I don't know — but thank God ahead of
   time."*
2. **Retrieval as humility, provenance as honesty.** RAG grounding ("strictly tethered" to the
   corpus) is the right instinct — it is mechanized epistemic humility — but it bounds *facts*,
   not *manner*. The Mimicker (style transfer) is the epistemically riskiest module in the
   grant: style without provenance counterfeits a voice. Every styled utterance should remain
   traceable to retrievable sources. The connection graph this pipeline labels — content
   linked to its dates, its page markers, its persons — is exactly the provenance substrate
   that makes that possible later. The labeling work is not preprocessing; it is the future
   system's conscience.
3. **The retrieval unit is a person.** Because of David's per-person convention, the index
   Phase 2 builds will retrieve *people Solanus prayed over*, not paragraphs of abstraction.
   When a pilgrim says "my daughter is sick," the system can answer from the casebook the way
   Solanus answered at the door — with the memory of particular Mrs. Sharps and Baby Trombles,
   not with generalized comfort. The granularity ruling David still owes the pipeline for
   Volumes 2–3 is therefore not a labeling detail; it decides the units of meaning the
   eventual encounter is made of.

## 3. The empirical heart: measure the visitor, not the model

The grant promises "groundbreaking data on how interactive media impacts contemporary
theological dialogue." The wisdom center's instruments make that promise concrete and
falsifiable. The question that matters is not whether the hologram *seems* wise; it is whether
visitors *leave* wiser: state measures of epistemic humility, gratitude, awe, and wise
reasoning, taken around the encounter, against the obvious dark-outcome hypothesis — that a
spectacular talking saint produces **certainty and parasocial authority** (the exact opposite
of Solanus's effect on people). Pre-registering those constructs now, with Nusbaum's lab's
instruments, would turn the installation from a marvel into an experiment. If the effect runs
the wrong way, the team should want to be the first to know — that is the project applying its
own subject matter to itself.

There is also a quieter dataset already in hand: this repo. LABEL_REVIEW.md plus the audit is
a longitudinal record of a human-AI system *acquiring* epistemic humility through documented
failure and architectural correction — six weeks of timestamped evidence for the whitepaper's
methodology section. The honest version of the grant's "zero hallucinations" claim (which is,
as written, an [A]-class assertion of the kind the audit flags) is the version this pipeline
already practices: *measured error bounds plus human-gated correction loops, with the receipts
in CSV.* Templeton's science reviewers will trust the bounded claim more than the absolute
one.

## 4. The tension to hold, not resolve

A hologram of a man who hid behind a door is close to a contradiction in form. I don't think
that kills the idea — icons, relics, and recorded sermons are all technologies of presence the
tradition has metabolized before — but the contradiction should be *designed against*
deliberately: let the medium perform smallness. Plainness over spectacle where possible;
deflection built into the persona; silence as an available answer; the visitor's particular
suffering, not the system's knowledge, as the center of every exchange. The team should decide
what the hologram cannot do with the same care it gives to what it can — and write that down
as a covenant document that humans author and machines merely load, which is precisely the
pattern `volume_notes/` already established in miniature.

And one last mirror, because epistemic humility demands it: this reflection was written by the
kind of system the project proposes to build. My own six weeks in this repo are the case study
in miniature — ungated, I confidently corrupted gold and mistook my inferences for his
conventions; gated, I became useful. If the whitepaper needs a one-sentence finding about AI
and wisdom, the record suggests this one: **the wisdom was never in the model; it was in where
the humans put the doors.** Solanus, of all people, would appreciate that the most important
component turned out to be the doorkeeper.

---

### Sources

- [UChicago Center for Practical Wisdom](https://wisdomcenter.uchicago.edu/) — mission and program.
- [Howard C. Nusbaum, Center profile](https://wisdomcenter.uchicago.edu/people/howard-c-nusbaum) and
  [Conversations on Wisdom interview](https://wisdomcenter.uchicago.edu/news/discussions/conversations-wisdom-howard-c-nusbaum) —
  epistemic humility as knowing the size of one's knowledge; wisdom as trainable skill;
  intellectual×moral virtue interaction.
- [Toronto Wisdom Task Force / common wisdom model announcement](https://wisdomcenter.uchicago.edu/news/wisdom-news/toronto-wisdom-task-force-publishes-common-model-wisdom-guide-future-research) and
  [Grossmann et al., *Psychological Inquiry* 2020](https://www.tandfonline.com/doi/abs/10.1080/1047840X.2020.1750920) —
  perspectival metacognition + moral aspirations.
- [Grossmann, "Wisdom in Context," *Perspectives on Psychological Science* 2017](https://journals.sagepub.com/doi/10.1177/1745691616672066) and
  [Grossmann, Gerlach & Denissen, *SPPS* 2016](https://journals.sagepub.com/doi/abs/10.1177/1948550616652206) —
  wise reasoning as state over trait; intellectual humility, uncertainty/change recognition,
  perspective-taking; third-person framing (Solomon's paradox).
- [Wikipedia: Solanus Casey](https://en.wikipedia.org/wiki/Solanus_Casey),
  [Archdiocese of Detroit](https://www.aod.org/blessed-solanus-casey),
  [Franciscan Media, "Thank God Ahead of Time"](https://www.franciscanmedia.org/franciscan-spirit-blog/thank-god-ahead-of-time-a-look-at-solanus-casey/) —
  simplex ordination, porter ministry, "In order to practice humility we must experience
  humiliations."
- Internal: `Project_Thoughts.docx`, `step_4/LABEL_REVIEW.md` (Iter 1–15),
  `step_4/EPISTEMIC_AUDIT.md`, `step_4/HITL_BOOTSTRAP.md`,
  `step_4/session_backups/CHAT_HISTORY_DIGEST.md`.
