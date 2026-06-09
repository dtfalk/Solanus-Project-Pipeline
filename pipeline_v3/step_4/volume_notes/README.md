# volume_notes/ — David-editable per-volume prompt conventions

`<Volume>.md` here REPLACES the python `VOLUME_PROMPT_NOTES` constant for that volume —
edit plain text, never code. Optional `<Volume>.cluster_<N>.md` is appended only for pages
that `qa_output/<Volume>/clusters.json` maps to cluster N (see HITL_BOOTSTRAP.md §8).
Write the convention in imperative labeling language (see the Volume_4 constant in
auto_labeler.py for the style); it is appended to the system prompt verbatim under a
"VOLUME-SPECIFIC NOTE" header.
