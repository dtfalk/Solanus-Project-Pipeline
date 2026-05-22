from pathlib import Path
import json

INPUT_ROOT = Path("step_3/extracted_samples")
OUTPUT_ROOT = Path("step_3/normalized")
if not OUTPUT_ROOT.exists:
    OUTPUT_ROOT.mkdir(parents = True, exist_ok = True)

VOLUME4_KEY_ORDER = [
    "src_content",
    "src_origin",
    "src_recipient",
    "src_location_recipient",
    "src_location_sender",
    "src_date",
    "src_greeting",
    "src_farewell",
    "src_signature",
    "src_margin_note",
    "src_insertion",
    "src_other",
    "archv_commentary",
    "archv_format_note",
    "archv_date",
    "archv_possessor",
    "archv_other",
    "struct_id",
    "struct_doc",
    "struct_commentary",
    "struct_other",
    "other"
]


def normalize_doc(doc):
    # preserve existing struct values
    struct_id = []
    if "struct_id" in doc:
        struct_id.extend(doc["struct_id"])
    if "doc_id" in doc:
        struct_id.extend(doc["doc_id"])

    struct_doc = []
    if "struct_doc" in doc:
        struct_doc.extend(doc["struct_doc"])
    if "doc_struct" in doc:
        struct_doc.extend(doc["doc_struct"])

    # start from a shallow copy of original doc so original operations happen first
    new_doc = dict(doc)

    # apply merges
    new_doc["struct_id"] = struct_id
    new_doc["struct_doc"] = struct_doc

    # remove old-schema keys
    if "doc_id" in new_doc:
        del new_doc["doc_id"]
    if "doc_struct" in new_doc:
        del new_doc["doc_struct"]

    # ensure all Volume_4 keys exist
    for key in VOLUME4_KEY_ORDER:
        if key not in new_doc:
            new_doc[key] = []

    # remove any keys not in Volume_4 target schema
    for key in list(new_doc.keys()):
        if key not in VOLUME4_KEY_ORDER:
            del new_doc[key]

    return new_doc


def reorder_doc_keys(doc):
    ordered_doc = {}
    for key in VOLUME4_KEY_ORDER:
        ordered_doc[key] = doc[key]
    return ordered_doc


def normalize_page(data):
    docs = data.get("documents", {})

    for doc_name in list(docs.keys()):
        normalized_doc = normalize_doc(docs[doc_name])
        ordered_doc = reorder_doc_keys(normalized_doc)
        docs[doc_name] = ordered_doc

    return data


def main():
    for in_path in INPUT_ROOT.rglob("*.json"):
        with open(in_path, "r") as f:
            data = json.load(f)

        data = normalize_page(data)

        out_path = OUTPUT_ROOT / in_path.relative_to(INPUT_ROOT)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()