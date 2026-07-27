import argparse
import json
from pathlib import Path


SECTIONS = [
    ("concept_name", "Concept Name"),
    ("insight_statement", "Insight Statement"),
    ("benefit_statement", "Benefit Statement"),
    ("reason_to_believe", "Reason to Believe"),
]


def render_output(payload: dict) -> str:
    chunks = []
    for field_name, header in SECTIONS:
        text = payload[field_name].strip()
        underline = "=" * len(header)
        chunks.append(f"{header}\n{underline}\n{text}")
    return "\n\n".join(chunks) + "\n"


def main() -> None:

    input_dir = Path(__file__).parent.resolve() / "RED_results"
    output_dir = Path(__file__).parent.resolve() / "RED_human_readable"
    output_dir.mkdir(parents = True, exist_ok = True)

    for input_path in Path.iterdir(input_dir):
        with open(input_path, "r", encoding = "utf-8") as f:
            payload = json.load(f)

        output_text = render_output(payload)
        output_path = output_dir / f"{input_path.stem}.txt"

        with open(output_path, "w", encoding = "utf-8") as f:
            f.write(output_text)


if __name__ == "__main__":
    main()
