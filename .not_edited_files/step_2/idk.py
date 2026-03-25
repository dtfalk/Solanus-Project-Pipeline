import os
import json
import re

FOLDER = "/home/david/Workspace/Projects/Solanus-Project-Pipeline/step_2/polygon_page_data/Volume_4/polygons"

NEW_POLYGON = [
    {"x": 342, "y": 649},
    {"x": 2655, "y": 874},
    {"x": 5273, "y": 833},
    {"x": 5270, "y": 5919},
    {"x": 3273, "y": 5933},
    {"x": 3211, "y": 6770},
    {"x": 40, "y": 6835},
    {"x": 40, "y": 3438}
]

pattern = re.compile(r"page_(\d+)\.json$")

for filename in os.listdir(FOLDER):
    match = pattern.match(filename)
    if not match:
        continue

    page_num = int(match.group(1))

    if page_num >= 61:
        filepath = os.path.join(FOLDER, filename)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            data["polygon"] = NEW_POLYGON

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            print(f"Updated: {filename}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")