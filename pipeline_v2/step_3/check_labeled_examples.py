"""
check_labeled_examples.py — inspect DPI, dimensions, and encoding for every
PDF in pipeline_v2/step_3/labeled_examples/.

Uses pdfimages (poppler-utils) which must be installed:
    sudo apt install poppler-utils

No files are modified.  Run from any directory:
    python pipeline_v2/step_3/check_labeled_examples.py

Output columns:
    Path        — relative path inside labeled_examples/
    W×H (px)    — pixel dimensions of the embedded raster image
    DPI         — x-dpi × y-dpi as stored in the PDF
    Enc         — image encoding (jpeg, jbig2, ccitt, raw, ...)
    Color       — color space (rgb, gray, ...)
    BPC         — bits per component
    ImgKB       — size of the raw/compressed image data inside the PDF
    FileKB      — total PDF file size on disk
"""

import subprocess
from pathlib import Path

SCRIPT_DIR           = Path(__file__).resolve().parent
LABELED_EXAMPLES_DIR = SCRIPT_DIR / "labeled_examples"


def inspect_pdf(pdf_path: Path) -> dict:
    """Call pdfimages -list and parse the first image row."""
    result = subprocess.run(
        ["pdfimages", "-list", str(pdf_path)],
        capture_output=True, text=True,
    )
    # Skip header lines (start with "page" or "---")
    data_lines = [
        line for line in result.stdout.splitlines()
        if line and not line.startswith("page") and not line.startswith("---")
    ]
    if not data_lines:
        return {"error": result.stderr.strip() or "no images found"}

    # pdfimages -list columns (0-indexed):
    # 0:page 1:num 2:type 3:width 4:height 5:color 6:comp 7:bpc 8:enc
    # 9:interp 10:object 11:ID 12:x-ppi 13:y-ppi 14:size 15:ratio
    parts = data_lines[0].split()
    if len(parts) < 14:
        return {"error": f"unexpected pdfimages output: {data_lines[0]!r}"}

    return {
        "width_px":    int(parts[3]),
        "height_px":   int(parts[4]),
        "color":       parts[5],
        "bpc":         parts[7],
        "encoding":    parts[8],
        "x_dpi":       int(parts[12]),
        "y_dpi":       int(parts[13]),
        "img_size":    parts[14],
        "file_kb":     round(pdf_path.stat().st_size / 1024, 1),
    }


def main():
    pdfs = sorted(LABELED_EXAMPLES_DIR.rglob("page_*.pdf"))

    if not pdfs:
        print(f"No page PDFs found under: {LABELED_EXAMPLES_DIR}")
        return

    print(f"Checking {len(pdfs)} PDFs in:\n  {LABELED_EXAMPLES_DIR}\n")

    col_path  = 52
    col_dims  = 16
    col_dpi   = 9
    col_enc   = 6
    col_color = 6
    col_bpc   = 4
    col_img   = 8
    col_file  = 8

    header = (
        f"{'Path':<{col_path}} "
        f"{'W×H (px)':<{col_dims}} "
        f"{'DPI':<{col_dpi}} "
        f"{'Enc':<{col_enc}} "
        f"{'Color':<{col_color}} "
        f"{'BPC':<{col_bpc}} "
        f"{'ImgKB':<{col_img}} "
        f"{'FileKB':<{col_file}}"
    )
    print(header)
    print("-" * len(header))

    dpi_counts: dict[tuple, int] = {}
    enc_counts: dict[str, int]   = {}
    errors = []

    for pdf in pdfs:
        rel  = str(pdf.relative_to(LABELED_EXAMPLES_DIR))
        info = inspect_pdf(pdf)

        if "error" in info:
            errors.append((rel, info["error"]))
            print(f"{'ERROR':<{col_path}} {rel}: {info['error']}")
            continue

        dims     = f"{info['width_px']}×{info['height_px']}"
        dpi_str  = f"{info['x_dpi']}×{info['y_dpi']}"
        dpi_key  = (info["x_dpi"], info["y_dpi"])
        dpi_counts[dpi_key]         = dpi_counts.get(dpi_key, 0) + 1
        enc_counts[info["encoding"]] = enc_counts.get(info["encoding"], 0) + 1

        print(
            f"{rel:<{col_path}} "
            f"{dims:<{col_dims}} "
            f"{dpi_str:<{col_dpi}} "
            f"{info['encoding']:<{col_enc}} "
            f"{info['color']:<{col_color}} "
            f"{info['bpc']:<{col_bpc}} "
            f"{info['img_size']:<{col_img}} "
            f"{info['file_kb']:<{col_file}}"
        )

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    print("=" * len(header))
    print(f"Total PDFs checked : {len(pdfs)}")
    if errors:
        print(f"Errors             : {len(errors)}")

    print("\nDPI breakdown:")
    for (x, y), count in sorted(dpi_counts.items()):
        flag = "  <-- uniform" if len(dpi_counts) == 1 else ""
        print(f"  {x}×{y} DPI  :  {count} file(s){flag}")

    print("\nEncoding breakdown:")
    for enc, count in sorted(enc_counts.items()):
        print(f"  {enc:<8} :  {count} file(s)")


if __name__ == "__main__":
    main()
