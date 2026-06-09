# Gold-Consistency Audit — 2026-06-06 (multi-agent, 55 agents)

Audited all 98 Appendix_1/Appendix_3 gold pages against the conventions David established
in the fresh Appendix_2 review (archivist-vs-Solanus voice, Roman-numeral handling, struct_*
usage). Every finding adversarially verified by an independent agent. **No gold was modified** —
this is a review worklist. Raw data: `gold_audit_2026-06-06.json` (incl. the distilled
convention spec used as ground truth).

**21 candidate convention-drift fixes** (one-click each in the editor) + 
13 findings excluded below as artifacts of the stale A3 PDF vintage on this machine
(boxes over whited-out archival annotations — resolve by syncing the laptop's A3 PDFs, not by editing gold).

## Candidate fixes (by page)

- **Appendix_1/page_003** bbox 3922,795,4240,901 — `src_content` → `struct_other` (high; other_convention_drift)
  - This box is the printed column header 'Pages' at the top of the table-of-contents sheet. The spec's QUICK DECISION TABLE explicitly maps the printed sheet header 'Page' -> struct_other (cf. Appendix_2 page_003 #62 / page_004 #70). Here it is labeled src_content, which is page-furniture/header text m
- **Appendix_1/page_003** bbox 832,984,4700,6083 — `src_content` → `src_content (left entry titles) + struct_doc (dot-leader runs and page-number references, one box each)` (high; other_convention_drift)
  - The entire ToC body is captured in a single src_content box. Per the spec's struct_doc rules for table-of-contents pages: the right-hand page-number references and the dot-leader runs ('..........') are each their own struct_doc boxes, while only the left-hand entry title text is src_content. This b
- **Appendix_1/page_011** bbox 3008,1382,4618,1695 — `archv_other` → `src_content (or a src_* category)` (medium; other_convention_drift)
  - Box #9 = the parenthetical '(17th anniversary of my ordination - Deo Gratias)' sitting in the letter's date/heading block (just below 'East 142nd St. N.Y.City' and 'July 24th, 1921'). It is written in the first person ('my ordination') and reads as part of the dateline Fr. Solanus himself wrote, i.e
- **Appendix_1/page_018** bbox 789,5175,3565,5405 — `src_other` → `archv_date + archv_commentary (archivist marginal annotation, not src_*)` (medium; archivist_as_src)
  - Box reads 'Mar. 13, 1934    Prize story found this A.M.   O.K.' Sits below the signature, separate from the letter body. This is an archivist/editor processing annotation (a later date plus a note that the prize story was found), not Solanus's own transcribed words. Per spec a marginal date the arch
- **Appendix_1/page_035** bbox 801,4336,4420,4530 — `archv_format_note` → `archv_commentary` (medium; other_convention_drift)
  - Box reads "Written on reverse side on Picture of Saint Felix Capuchin Friary:". This is the SAME construct as page_033 boxes #10 "Written on front of picture post-card picture of the Friary:" and #11 "Written on reverse side:", which on page_033 are both labeled archv_commentary. Per the spec quick-
- **Appendix_1/page_047** bbox 3535,2366,4291,2555 — `src_date` → `archv_date` (medium; other_convention_drift)
  - The right-aligned marginal date "1953, April 6" sits between doc_1's sign-off ("Sincerely yours....." / "Fr. Solanus, O.F.M.Cap.") and the archivist's descriptive note (archv_format_note #11: "Message written on the back of a photo of Fr. Solanus") that introduces the photo-message item (doc_2). It 
- **Appendix_1/page_052** bbox 1677,1486,3439,2497 — `archv_commentary` → `split: collection identifier ('APPENDIX to VOLUME I') -> struct_id; descriptive title lines remain archv_commentary` (low; other_convention_drift)
  - This Appendix title block is one merged archv_commentary box. The parallel title page Appendix_3/page_001 (also in this audit) treats the equivalent text differently: 'APPENDIX III' is its own struct_id box and 'COLLECTED WRITINGS ATTRIBUTED TO / THE SERVANT OF GOD / FATHER SOLANUS CASEY, O.F.M.Cap.
- **Appendix_3/page_002** bbox 2289,1015,3822,1191 — `src_date` → `struct_doc (or archivist-side index metadata, not src_*)` (high; archivist_as_src)
  - This is a table-of-contents/index page (same kind the spec's page_003/004 describe). The middle column holds the archivist's index DATE for each listed item ('1927, December 27.....', with dot leaders), i.e. a date-as-description ABOUT the indexed letter/card, not Solanus's own transcribed in-text d
- **Appendix_3/page_002** bbox 2285,1228,3824,1405 — `src_date` → `struct_doc (index metadata, not src_*)` (high; archivist_as_src)
  - '1946, December 1.....' index-row date. Same issue as #18: archivist's index date-as-description on a TOC page mislabeled src_date. Representative of the systematic #18-#42 column.
- **Appendix_3/page_002** bbox 2108,2697,3822,2870 — `src_date` → `struct_doc (index metadata, not src_*)` (high; archivist_as_src)
  - 'c. 1946,' index-row approximate date (the 'c.' = circa makes the archivist-estimate nature explicit). On a TOC/index page this is the archivist characterizing when the item was written, not Solanus's transcribed text; should not be src_date. Part of the systematic #18-#42 column.
- **Appendix_3/page_002** bbox 1322,6039,3201,6217 — `src_date` → `struct_doc (index metadata, not src_*)` (high; archivist_as_src)
  - 'January 1, 1911 to January 1, 1915' — the date-range index entry for the Notebook No. 12 row. Clearly an archivist index span describing the notebook's coverage, not Solanus's own in-text date; mislabeled src_date. Part of the systematic #18-#42 column.
- **Appendix_3/page_005** bbox 2797,2118,4425,2514 — `archv_other` → `src_greeting (or src_content)` (medium; archivist_as_src)
  - Box #15 covers the typed letter header line 'Blessed be God / In all His designs.' -- Fr. Solanus's own devotional motto, i.e. the document's own reproduced words. It is labeled archv_other (an archivist category), but on page_009 the IDENTICAL phrase 'Blessed be God. / In all His designs.' is label
- **Appendix_3/page_016** bbox 2026,4202,3262,4401 — `archv_possessor` → `src_signature` (high; other_convention_drift)
  - Box reads "Fr. Solanus, ofm, Cap." — this is Solanus's own signature/sign-off, the canonical src_signature exemplar in the spec ("Fr. Sol."). Here it is given an ARCHIVIST category (archv_possessor), i.e. src content labeled with an archivist label. On the sibling letter pages (012, 013, 014, 018) t
- **Appendix_3/page_017** bbox 1874,4745,3169,4918 — `src_farewell` → `src_signature` (high; other_convention_drift)
  - Box reads "Fr. Solanus. O.F.M.Cap." — the signature line, labeled src_farewell. Per the spec, Solanus's sign-off ("Fr. Sol.") is src_signature, and pages 012/013/014/018 label the identical line src_signature. Page_017 has NO src_signature box at all; the signature was absorbed into src_farewell.
- **Appendix_3/page_023** bbox 194,1481,1663,1657 — `archv_other` → `archv_format_note` (medium; other_convention_drift)
  - Box reads 'Page 2 and 3 are blank' - an archivist note about the physical layout of the document. The spec defines archv_format_note for exactly this kind of structural/format note about physical layout (its example is 'Inside front cover.'). Labeled archv_other instead.
- **Appendix_3/page_023** bbox 895,1895,4613,2082 — `struct_commentary` → `archv_commentary` (high; other_convention_drift)
  - Box reads 'Here begins the notes of Father Solanus Casey in his hand.' This is an archivist meta-description introducing/heading the transcribed notebook section (third-person framing of the artifact). Per spec this descriptive header is archv_commentary. The category 'struct_commentary' does not ex
- **Appendix_3/page_035** bbox 31,4389,578,4592 — `src_date` → `struct_doc` (high; other_convention_drift)
  - Box #14 reads 'Page 15' (verified by zoom). This is a mid-page page-position marker, identical in kind to the 'Page 13' (#26) and 'Page 14' markers correctly labeled struct_doc on pages 033/034. Per spec, 'Pg. N'/'Page N' location markers are struct_doc, one box each. Here it was swept into the left
- **Appendix_3/page_036** bbox 58,4474,568,4671 — `src_date` → `struct_doc` (high; other_convention_drift)
  - Box #24 reads 'Page 16' (verified by zoom). Same pattern as page_035 #14: a mid-page page-position marker mislabeled as src_date because it sits in the left date column. Spec mandates struct_doc for 'Page N' markers; compare the top-of-page 'page 15 Cont.' (#32) and 'Page 16' on page_037 which are c
- **Appendix_3/page_039** bbox 823,3594,4286,3767 — `struct_other` → `src_content` (low; other_convention_drift)
  - Box #17 reads '1913 A.D.  In Nomine Jesu                1913'. The spec restricts struct_other to printed-sheet page-furniture (e.g. the 'Page' column header on index sheets). This is the chronicler's own year-divider heading written within the document body, so it reads more like src_content (the d
- **Appendix_3/page_045** bbox 940,3030,4027,3519 — `archv_other` → `archv_commentary` (high; other_convention_drift)
  - Box #12. Text: 'End of book has 14 pages of names and dues paid. The handwriting is by several persons. The last 6 pages may be by Fr. Solanus. - Just names of servers listed.' This is the archivist describing the ARTIFACT's contents/provenance/authorship (handwriting, 'may be by Fr. Solanus'), whic
- **Appendix_3/page_045** bbox 960,2618,1867,2794 — `archv_other` → `archv_commentary` (high; other_convention_drift)
  - Box #11. Text: 'are all blank'. Paired with left-column marker 'Pages 24 to 81' (struct_doc). This is the archivist stating that pages 24-81 of the book are blank -- a meta-description OF the artifact, which the spec routes to archv_commentary. 'archv_other' is a non-spec category not present in the

## Excluded as stale-PDF artifacts (do NOT edit gold for these)

- Appendix_3/page_026 bbox 272,574,2245,750 `struct_id` — region blank in local render
- Appendix_3/page_026 bbox 3850,249,4175,433 `archv_date` — region blank in local render
- Appendix_3/page_027 bbox 195,474,2162,647 `struct_id` — region blank in local render
- Appendix_3/page_027 bbox 3833,150,4096,329 `archv_date` — region blank in local render
- Appendix_3/page_028 bbox 206,356,2175,531 `struct_id` — region blank in local render
- Appendix_3/page_028 bbox 3849,135,4167,316 `archv_date` — region blank in local render
- Appendix_3/page_029 bbox 230,438,2201,613 `struct_id` — region blank in local render
- Appendix_3/page_029 bbox 3870,173,4202,354 `archv_date` — region blank in local render
- Appendix_3/page_030 bbox 270,422,2240,595 `struct_id` — region blank in local render
- Appendix_3/page_030 bbox 3908,157,4240,345 `archv_date` — region blank in local render
- Appendix_3/page_031 bbox 233,439,2200,614 `struct_id` — region blank in local render
- Appendix_3/page_031 bbox 4053,168,4388,347 `archv_date` — region blank in local render
- Appendix_3/page_032 bbox 3984,231,4318,408 `archv_date` — region blank in local render