import sys
from pathlib import Path
import json
from shutil import copy2

SCHEMA = set([
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
])

CURDIR = Path(__file__).parent.resolve()
pages_dir = CURDIR / "final"
out_dir = CURDIR / "CLEANED_FINAL"
out_dir.mkdir(parents = True, exist_ok = True)

usages = {key: 0 for key in SCHEMA}

for src_folder in pages_dir.iterdir():
    print(f"\n\nDOCUMENT FOLDER: {src_folder.name}")
    for page_folder in src_folder.iterdir():
        print(f"    PAGE {page_folder.name}")
        for file_object in page_folder.iterdir():
            if file_object.suffix == ".pdf":
                continue
            with open(file_object, mode = "r") as fp:
                cur_data = json.load(fp)
            
            for document_name in cur_data["documents"]:
                cur_document = cur_data["documents"][document_name]
                cur_document_keys = set(cur_document.keys()) 
                if cur_document_keys != set(usages.keys()):
                    print(f"{page_folder} key mismatch found:\n  {cur_document_keys}")
                    sys.exit()
                
                for key, poly_list in cur_document.items():
                    if not poly_list:
                        continue
                    usages[key] += 1
                    for i, entry in enumerate(poly_list):
                        cur_entry = cur_document[key][i]
                        if not set(cur_entry.keys()) == set(["vertices", "connections", "id"]):
                            print(cur_entry.keys())
                            sys.exit()
                del cur_document["src_insertion"]
                del cur_document["src_margin_note"]
            final_path = out_dir / src_folder.name / page_folder.name
            final_path.mkdir(parents = True, exist_ok = True)
            with open(final_path / f"{page_folder.name}.json", mode = "w", encoding = "utf8") as fp:
                json.dump(cur_data, fp, indent = 4)
            copy2(file_object.with_suffix(".pdf"), final_path / f"{page_folder.name}.pdf")


print("\n\nUNUSED KEYS")
for key, val in usages.items():
    if int(val) == 0: 
        print(f"    {key}")