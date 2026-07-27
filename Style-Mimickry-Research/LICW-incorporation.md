# Integrating LIWC and Interpretable Stylometry into a Style Mimicry System

## TL;DR
- **Use LIWC-22 and classic stylometry as your interpretable evaluation/diagnostic layer and as a data-filtering signal — NOT as the primary generation target.** Neural style embeddings (LUAR, StyleDistance, Wegmann) should remain your main steering/fitness targets because lexicon counts are bag-of-words, coarse, and gameable (Goodhart), but interpretable features give you the dimension-by-dimension audit ("too few I-words, sentences too long, Authentic too low") that embeddings cannot.
- **The single most defensible architecture is a composite, multi-component objective plus an interpretable dashboard:** neural style-embedding cosine (primary) + a small set of robust stylometric distances (function-word distribution, sentence-length/burstiness, punctuation rate, vocabulary-richness) as auxiliary terms and as a tripwire against embedding gaming; published RL style-transfer systems (ASTRAPOP, STEER, StyleRemix) already combine embedding/classifier rewards with length and content penalties for exactly this reason.
- **Avoid LIWC-22's commercial license cost where possible:** for an automated pipeline use free, scriptable equivalents (function-word lists + spaCy POS/dependency features + the `stylo` R package + Empath/NRC/VADER for affect + readability/lexical-diversity libraries). Reserve paid LIWC-22/Receptiviti for the final human-readable audit if you specifically want its validated Analytic/Clout/Authentic/Tone summary dimensions.

## Key Findings

1. **Function words and LIWC's summary dimensions are genuinely diagnostic of individual style** because they are produced largely involuntarily and are topic-independent. This is the Pennebaker "function-word/Secret Life of Pronouns" thesis, formalized in Tausczik & Pennebaker (2010) and operationalized in LIWC-22 (Boyd, Ashokkumar, Seraj & Pennebaker 2022). The most style-diagnostic LIWC categories are the function-word families (pronouns — especially first-person singular — articles, prepositions, auxiliary verbs, conjunctions, negations) and the four summary dimensions (Analytic, Clout, Authentic, Emotional Tone).
2. **In stylometry proper, function words are the classic strongest authorship signal**, but this is contested: a 2024 PLOS One study found content words (especially nouns) can carry more authorial information. Character n-grams are consistently among the most powerful single feature types.
3. **Neural style embeddings outperform LIWC/lexical features on controlled style-discrimination tasks.** Wegmann & Nguyen (2021) found BERT-based style measures beat 3-grams, punctuation frequency, and LIWC-based approaches. But all methods — neural included — collapse on the adversarial STEL-or-Content task, showing that apparent style signal is often content leakage.
4. **Interpretable-neural hybrids exist and are directly relevant:** LISA (interpretable style embeddings via prompting LLMs), StyleDistance (40 content-controlled style features), and iBERT (decomposable interpretable embeddings) give you per-dimension interpretability close to neural performance.
5. **Interpretable features have been used as generation control, but mostly as evaluation rather than active steering.** PPLM uses bag-of-words attribute models as gradient steering; CTRL uses control codes; PILOT (2025) is the most direct recent example of using LIWC dimensions as a *proactive* control signal for generation rather than post-hoc analysis. Style-transfer RL systems use a style classifier/embedding reward plus a content-preservation (BLEU/SBERT) reward.
6. **Matching a neural style-embedding centroid reliably moves text AWAY from the source author but struggles to convincingly move TOWARD a target author** (Patel et al. STYLL). This asymmetry, plus documented content-leakage and degenerate-output failure modes, is the central reason a composite objective beats any single target.
7. **LIWC misses syntax, word order, and rhythm.** Complementary interpretable tools — POS/dependency n-grams (syntactic n-grams / sn-grams), sentence-length variance and burstiness, punctuation-sequence modeling, vocabulary-richness measures (MTLD, Yule's K, TTR), and discourse/cohesion tools (Coh-Metrix) — are needed for a full interpretable fingerprint.

## Details

### 1. LIWC-22 as a style-fidelity feature set

The theoretical foundation is Pennebaker's claim that **function words (pronouns, articles, prepositions, auxiliary verbs, conjunctions, negations, quantifiers) are "style words" processed largely unconsciously, produced at stable rates regardless of topic, and therefore betray the individual.** Tausczik & Pennebaker (2010, *Journal of Language and Social Psychology* 29(1):24–54) is the canonical review: LIWC "counts words in psychologically meaningful categories" and detects "attentional focus, emotionality, social relationships, thinking styles, and individual differences." Articles ("a," "an," "the") are treated as a coherent category because all signal an upcoming concrete noun — a function-level, not content-level, marker.

LIWC-22 (Boyd, Ashokkumar, Seraj & Pennebaker 2022, University of Texas at Austin technical report) is the fifth-generation version with an expanded dictionary and four **summary dimensions**: Analytical Thinking, Clout, Authenticity, and Emotional Tone. These are the most relevant for style fingerprinting:
- **Analytic** captures formal, logical, hierarchical thinking (high article + preposition use) vs. narrative/informal style.
- **Clout** captures social status/confidence (pronoun patterns; high "we"/"you," low "I").
- **Authentic** captures personal, disclosing, unguarded style.
- **Tone** captures affective valence.

For an individual-style fingerprint, the **most diagnostic LIWC categories are**: first-person singular (i), first-person plural (we), articles, prepositions, auxiliary verbs, conjunctions, negations, the Analytic and Authentic summary scores, informal-language/netspeak categories, fillers, and (where the corpus retains them) punctuation counts. These are precisely the categories that are most topic-invariant.

**Reliability caveat:** LIWC categories are word-count proportions, so they are length-sensitive and unstable on short texts. A 2025 forensic-security review (ScienceDirect S2666799125000012) examines whether LIWC is "reliable, efficient, and effective" for large online datasets and flags validity concerns. The summary dimensions are validated psychometric constructs, but their *individual-discriminative* validity (fingerprinting one person vs. characterizing a psychological state) is weaker than their construct validity.

### 2. LIWC and stylometry for authorship/style fingerprinting

The classic stylometric toolkit (Abbasi & Chen 2008 Writeprints; the `stylo` R package, Eder, Rybicki & Kestemont 2016; JStylo) draws on:
- **Lexical:** word/character n-grams, word-length distribution, vocabulary richness (TTR, hapax legomena, Yule's K, Sichel's S, MTLD).
- **Syntactic:** function-word frequencies, POS-tag n-grams, syntactic dependency n-grams (sn-grams; Sidorov), parse-tree features.
- **Structural / orthographic:** punctuation patterns, sentence/paragraph length, capitalization.
- **Idiosyncratic:** misspellings, characteristic collocations.

Per Abbasi & Chen 2008 (*ACM TOIS* Vol. 26, No. 2, Article 7), "Writeprints outperformed benchmark techniques, including SVM, Ensemble SVM, PCA, and standard Karhunen-Loeve transforms... with accuracy as high as 94% when differentiating between 100 authors," using a rich lexical+syntactic+structural+idiosyncratic+content feature set with a Karhunen-Loève / sliding-window technique. Burrows's Delta (2002), implemented in `stylo`, is the workhorse distance metric: it z-scores the frequencies of the most frequent words (function words dominate) and computes a Manhattan/cosine distance. `stylo` (GitHub computationalstylistics/stylo, GPL-3) provides PCA, cluster analysis, bootstrap consensus trees, and supervised classifiers (Delta, SVM, NB, kNN, NSC) and `rolling.delta`/`oppose` for within-document and contrastive analysis.

**Function words vs. content words:** the long-standing assumption (Mosteller & Wallace 1964 Federalist Papers; Argamon & Levitan 2005) is that function words are the best authorship markers because they are topic-neutral. This was challenged by a 2024 PLOS One study ("Attributing authorship via the perplexity of authorial language models") finding "content words, especially nouns, contain more authorial information than function words." Character n-grams remain among the most robust single feature types (Stamatatos).

**Interpretable vs. neural, and hybrids:**
- **Neural embeddings:** LUAR (Rivera-Soto, Miano, Ordonez, Chen, Khan, Bishop & Andrews, EMNLP 2021, aclanthology 2021.emnlp-main.70), a universal authorship embedding (512-dim) trained on the Reddit Million User Dataset (Khan et al. 2021); Wegmann et al. 2022 content-independent style embeddings (RoBERTa sentence-transformer, triplet loss); StyleDistance (Patel, Zhu, Qiu, Horvitz, Apidianaki, McKeown & Callison-Burch, NAACL 2025 long.436, pp. 8662–8685), contrastively trained on "positive and negative examples across 40 distinct style features" generated as 100 GPT-4 paraphrase pairs per feature across 7 categories, in a synthetic dataset named SYNTHSTEL. The TACL paper "Can Authorship Representation Learning Capture Stylistic Features?" (Wang et al. 2023) probes LUAR and concludes the representations are "indeed sensitive to writing style," supporting their use as style targets and noting style transfer as a downstream application.
- **Interpretability-vs-power tradeoff:** neural embeddings discriminate better but are uninterpretable (you cannot tell *why* two texts differ). LIWC/stylometry discriminate slightly worse but tell you exactly which dimension diverges.
- **Hybrids:** LISA (Patel et al. 2023 EMNLP Findings; StyleGenome ~5.5M examples; 768-dim interpretable embedding from GPT-prompted stylometry) matches neural performance on STEL while being interpretable. StyleDistance gives 40 named style axes. iBERT (2025/2026) produces decomposable interpretable embeddings. SenteCon is a related interpretable-lexicon-layer approach.

### 3. Interpretable features as control / reward / target for style transfer

- **(a) Control/conditioning:** PPLM (Dathathri et al. 2019/2020) steers a frozen LM using gradients from simple bag-of-words or one-layer attribute classifiers — directly analogous to using LIWC category lists as steering signals. CTRL uses control codes. **PILOT (Cisar et al. 2025, arXiv:2509.15447)** is the most directly relevant recent work: it translates persona descriptions into structured psycholinguistic profiles and uses **LIWC dimensions as a proactive generation control signal**, noting verbatim that "Psycholinguistic frameworks like LIWC and SEANCE offer validated dimensions across affective, cognitive, and social processes, but have primarily served post-hoc analysis rather than active generation control (Pennebaker et al. 2015; Mairesse and Walker 2011)" — the gap PILOT fills. PERSONAGE/Mairesse-Walker (2011) is the foundational personality-controlled generation system.
- **(b) Reward signals:** RL style transfer uses a style-classifier reward + content-preservation (BLEU/SBERT cyclic) reward, often combined as a harmonic mean (Dual RL, Luo et al. 2019; RL-ST; Reinforced Rewards Framework). **ASTRAPOP (Liu, Agarwal & May 2024, ACL Findings, arXiv:2403.08043)** uses LUAR cosine similarity *directly* as the RL reward for authorship style transfer: SIM_sty(x,s) = cossim(LUAR({x}), v_s), with a "toward" reward, an "away" reward, and a length penalty. **STEER (Hallinan et al. 2023, EMNLP Findings)** uses the QUARK RL algorithm with fine-grained style rewards. A 2024 survey (arXiv:2406.11581) notes "two of the policy optimization based approaches, STEER ... and ASTRAPOP ... achieve the best performance on text style transfer and authorship style transfer, respectively."
- **(c) Targets/constraints:** lexically-constrained decoding and attribute-controlled decoding can enforce category word rates. Bevendorff et al. (2019, ACL) model style difference as Jensen-Shannon distance between character-n-gram distributions for obfuscation.
- **(d) Evaluation:** LIWC/stylometric feature distances are widely used to evaluate style-transfer output.

**Does conditioning on low-dimensional interpretable features work as well as conditioning on neural embeddings for surface fidelity?** No — not alone. Wegmann & Nguyen (2021) show LIWC/n-gram measures are outperformed by BERT-based measures. Conditioning on interpretable features is controllable and auditable but coarse: you can hit target word-category rates while sounding nothing like the person. Neural embeddings capture more holistic style but leak content and are uninterpretable. The published systems that work best combine both (StyleRemix, below).

### 4. LIWC/stylometry as a style evaluation & diagnostic axis

This is the strongest, most defensible role for interpretable features in your build. The procedure:
1. Compute the author's **profile vector** (LIWC categories + stylometric features) over their ~2M-word corpus, with per-feature mean and standard deviation (and covariance for Mahalanobis).
2. Compute the same vector over generated text.
3. Measure per-dimension distance to produce an **interpretable dimension-by-dimension report**: per-feature **z-scores** (how many SDs the clone deviates on each dimension), **Mahalanobis distance** (covariance-aware overall distance), and **KL / Jensen-Shannon divergence** on distributional features (function-word distribution, sentence-length distribution, punctuation distribution).

**This is done in practice.** "Catch Me If You Can? Not Yet" (2025, arXiv:2509.14543) builds individualized style models from **LIWC (Boyd et al. 2022) + WritePrints (Abbasi & Chen 2008)** feature sets and computes **Mahalanobis distance** between each generated sample and the author's style model, alongside Longformer/ModernBERT authorship-verification models — finding that original human samples have the lowest distance and LLM imitations sit measurably farther. StyleRemix (Fisher et al. 2024, arXiv:2408.15666) builds a 7-dimensional interpretable author vector (length, function-word count, grade level, formality, etc.), computes the deviation from corpus average, and selects the axes where the author deviates most using standard-deviation bands.

**Statistical considerations** (must be handled or the audit is misleading):
- **Length normalization:** LIWC categories are rates; compare like-length texts or bootstrap over fixed-length windows.
- **Reliability on short texts:** stylometric features stabilize only above ~500–1,000 words; below ~100 words results are essentially meaningless. Use length-robust metrics: McCarthy & Jarvis 2010 (*Behavior Research Methods* 42(2):381–392, doi:10.3758/BRM.42.2.381) found MTLD "performs well with respect to all four types of validity and is, in fact, the only index not found to vary as a function of text length" (vs. vocd-D, raw TTR, Maas, and Yule's K).
- **Base rates:** many categories are near-zero; use the author's own distribution as the reference, not population norms.
- **Confidence intervals:** bootstrap over corpus windows to get a confidence band for each feature so the audit distinguishes real divergence from sampling noise.

### 5. LIWC/stylometric features as a fitness/steering target alongside neural embeddings (the Horikawa loop)

The Horikawa (2025, *Science Advances*, "mind captioning," doi:10.1126/sciadv.adw1464) method is a **gradient-free iterative MLM optimization**: starting from a seed, it repeatedly (i) masks words, (ii) uses a frozen masked-LM (RoBERTa-large) to propose replacements that stay on the natural-text manifold, and (iii) selects candidates whose feature vector best matches a target feature vector (in the original paper, brain-decoded semantic features). **Replacing the fitness function with a style objective is a clean adaptation:** the target becomes the author's style-embedding centroid (LUAR/StyleDistance) and the fitness is cosine similarity to it.

**Interpretable features can and should be part of that composite fitness function**, for three reasons:
- **They are controllable dimension-by-dimension** — you can add explicit terms pulling function-word rates, sentence-length mean/variance, and punctuation rate toward the author's values.
- **They are a tripwire against embedding gaming.** The central risk is Goodhart: ASTRAPOP found that optimizing pure toward/away embedding rewards "sometimes results in models that only generate empty or very short outputs," requiring a length penalty. Patel et al. (STYLL, AAAI 2024, arXiv:2212.08986) found embedding optimization moves *away* from the source reliably but is "rather unable to adopt, or move toward, the intended target author." StyleDistance documents content leakage in style embeddings. An interpretable term (e.g., penalize sentence-length divergence, reward function-word JS-distance) anchors the search.
- **Conversely, LIWC is itself gameable** — you can hit target category rates while sounding nothing like the person, because LIWC ignores syntax, order, and rhythm. So neither target alone is robust.

**Therefore the robust design is a composite/multi-reward style objective:** primary = neural style-embedding cosine to the author centroid (LUAR + StyleDistance, averaged or ensembled); auxiliary = a small set of robust interpretable distances (function-word distribution JS, sentence-length mean+variance, punctuation rate, MTLD); plus content/fluency guards (MLM likelihood, SBERT content similarity to the seed, length penalty). Weight the embedding term highest; use interpretable terms as regularizers and the dashboard as the gate. This mirrors how the best published systems are actually built (StyleRemix's per-axis adapters + neural classifier eval; ASTRAPOP's embedding reward + length penalty). Note that StyleRemix reports grammaticality degrades when more than ~5 style axes are optimized simultaneously — keep the auxiliary set small.

### 6. Capturing fine-grained style that LIWC misses

LIWC is bag-of-words: it cannot see word order, syntax, or rhythm. Complementary **interpretable** tools:
- **Syntactic stylometry:** POS-tag n-grams (robust, low-dimensional, hard to consciously manipulate — Gamon 2004); syntactic dependency n-grams ("sn-grams," Sidorov et al.; "mixed sn-grams" integrating words+POS+dependency relations report superior accuracy to homogeneous n-grams on CCAT50); constituency/dependency parse features; syntactic-complexity metrics (Yngve/Frazier depth, T-unit analysis). Compute with spaCy (free).
- **Prose rhythm/cadence:** sentence-length mean and **variance**, **burstiness**, distribution shape — captures the "rhythm" LIWC and most embeddings under-weight.
- **Punctuation-sequence modeling:** punctuation n-grams and rates (comma vs. semicolon vs. dash habits) — highly individual.
- **Character-level features:** character n-grams (among the most discriminative single feature types).
- **Vocabulary richness:** MTLD (length-robust, the default), Yule's K, Sichel's S, hapax ratio, TTR/STTR.
- **Discourse/cohesion:** Coh-Metrix (referential cohesion, latent semantic analysis overlap, connectives, situation-model indices) for paragraph-level style.

Combined with LIWC's lexical/affect axes, these give a **fuller interpretable fingerprint** spanning lexicon (LIWC/Empath), syntax (POS/dependency n-grams), rhythm (sentence-length variance/burstiness), orthography (punctuation), richness (MTLD/Yule's K), and discourse (Coh-Metrix).

### 7. Practical integration recommendation

**Where each piece slots in:**
- **(a) Interpretable evaluation/diagnostic dashboard — PRIMARY ROLE.** Build the author profile vector (LIWC-or-free-equivalent categories + function-word distribution + POS n-grams + sentence-length stats + punctuation + MTLD/Yule's K + Coh-Metrix) over the corpus with bootstrapped confidence bands, and produce a per-dimension z-score / Mahalanobis / JS report on every batch of generated text. This is the highest-value, lowest-risk use.
- **(b) Auxiliary loss/reward/fitness term — SECONDARY, combined with neural embeddings.** Add a small number of robust interpretable distances to the Horikawa-loop fitness and/or the RL reward, weighted below the neural embedding term, as a regularizer and anti-gaming tripwire.
- **(c) Authorship/style discriminator as evaluator/adversarial signal — YES.** Train a stylometric+neural classifier (à la "Catch Me If You Can? Not Yet") as an evaluator; optionally use it adversarially. A few interpretable features (XGBoost on ~8 stylometric features) can expose imitations even when surface stats are matched, so this is a strong honest evaluator.
- **(d) Data-filtering/selection signal — YES, high value.** Use the profile distance to **filter/weight your synthetic neutral→styled pairs**: keep the pairs whose styled side best matches the author's profile, down-weight off-profile pairs before QLoRA finetuning. With 2M words this lets you curate the highest-fidelity training signal cheaply.

**Recommended combination:** (a) + (d) as the backbone, (b) as a regularizer in the refinement loop, (c) as the held-out evaluator.

**Licensing:** LIWC-22 is academic-only on the web store; commercial use requires a Receptiviti license (LIWC is a Receptiviti "Core" framework, with usage tiers starting around 250,000 words/month and a three-month minimum commitment; exact pricing via sales). For an automated, high-throughput, possibly-commercial style-mimicry pipeline, the per-word commercial cost is a real constraint. **Free/open alternatives that cover the same ground:**
- **Lexicon/affect:** Empath (Fast, Chen & Bernstein 2016, Proc. CHI), whose "data-driven, human validated categories are highly correlated (r=0.906) with similar categories in LIWC" (r=0.90 without the crowd filter); plus NRC EmoLex and VADER. Empath is Python (GitHub Ejhfast/empath-client) with 200 categories.
- **Function words / Delta / richness:** the `stylo` R package (free, GPL-3), JStylo.
- **POS/dependency/syntax:** spaCy (free).
- **Readability/lexical diversity:** standard Python libraries (textstat; MTLD implementations).
- **Cohesion:** Coh-Metrix (free web) or open alternatives.

Use free tools for the automated in-loop signals and data filtering; reserve a paid LIWC-22/Receptiviti license, if at all, for the final human-facing audit where the validated Analytic/Clout/Authentic/Tone dimensions add interpretive value.

## Recommendations

**Stage 1 — Build the interpretable profile and audit first (before any finetuning).**
- Compute the author's full interpretable profile over the 2M-word corpus using free tools (function-word distribution, spaCy POS/dependency n-grams, sentence-length mean/variance/burstiness, punctuation rates, MTLD/Yule's K, Empath/NRC affect, Coh-Metrix cohesion), with bootstrapped per-feature confidence bands.
- Also compute the author's neural style centroid (LUAR + StyleDistance).
- This profile is reused everywhere downstream.

**Stage 2 — Use the profile to filter/weight synthetic training pairs (data selection).**
- Generate neutral→styled pairs, score the styled side against the author profile (Mahalanobis + embedding cosine), and keep/weight the best-matching pairs for QLoRA finetuning. Threshold: drop pairs beyond ~2–3 SD on multiple key dimensions.

**Stage 3 — Finetune (QLoRA on 70B or full 7–13B) on the filtered pairs; make the neural embedding the primary objective in any refinement/RL loop.**
- Primary reward/fitness: cosine to author centroid (LUAR + StyleDistance ensemble).
- Auxiliary regularizers: function-word JS distance, sentence-length mean+variance, punctuation rate, MTLD — weighted ~0.1–0.3 of the primary, and keep the auxiliary axis count low (≤5) to avoid the grammaticality degradation StyleRemix documents.
- Guards: MLM likelihood (fluency), content similarity to seed, length penalty (ASTRAPOP's documented fix for degenerate outputs).

**Stage 4 — Evaluate with the dimension-by-dimension dashboard + an independent discriminator.**
- Report per-dimension z-scores and overall Mahalanobis distance vs. the author; require generated text to fall within the author's bootstrapped confidence bands on the key style dimensions.
- Train a held-out stylometric+neural authorship-verification classifier; track whether it can distinguish clone from author (lower is better).

**Benchmarks/thresholds that change the plan:**
- If the discriminator easily separates clone from author despite good embedding cosine → embedding is being gamed; increase interpretable-regularizer weights and inspect which dimensions diverge.
- If interpretable dimensions match but humans still reject the style → the gap is syntactic/rhythmic; add POS/dependency-n-gram and sentence-rhythm terms.
- If short-text outputs are unstable → enforce minimum output length for audit and aggregate over windows.

## Caveats

- **Bag-of-words coarseness:** LIWC and Empath ignore syntax, word order, and rhythm; they cannot alone certify stylistic fidelity. Pair them with syntactic and rhythm features.
- **Gameability / Goodhart (documented, not hypothetical):** pure embedding rewards yield degenerate outputs (ASTRAPOP); embedding optimization moves away from source but fails to adopt target (STYLL); LIWC category rates can be hit without sounding like the person; rubric/multi-component rewards can be gamed item-by-item. The composite objective + adversarial discriminator is the mitigation, not a guarantee.
- **Length sensitivity:** all count-based features are unreliable on short texts; use length-robust metrics (MTLD) and window aggregation.
- **Validity concerns:** LIWC's psychometric validity is for psychological constructs, not necessarily for individual fingerprinting; its forensic reliability has been questioned (ScienceDirect 2025). The function-word-supremacy assumption is itself contested (PLOS One 2024).
- **Content leakage in neural embeddings:** LUAR and authorship embeddings encode topic as well as style; StyleDistance/Wegmann content-controlled embeddings mitigate but do not eliminate this. The STEL-or-Content collapse shows all current style measures retain a content confound.
- **Recency/vetting:** several 2025–2026 items (PILOT 2509.15447; "Catch Me If You Can? Not Yet" 2509.14543; StyleRemix 2408.15666; ASTRAPOP 2403.08043; some 2026-stamped stylometry-vs-LLM papers) are recent and lightly peer-reviewed or in-press; treat their specific numbers as provisional. The foundational references (Tausczik & Pennebaker 2010; Boyd et al. 2022; Abbasi & Chen 2008; Eder/Rybicki/Kestemont 2016 stylo; Fast et al. 2016 Empath; PPLM 2019; LUAR 2021; Wegmann 2021/2022; McCarthy & Jarvis 2010 MTLD) are well-established.