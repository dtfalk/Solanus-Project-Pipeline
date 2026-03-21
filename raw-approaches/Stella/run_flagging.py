import os
import re
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

import fitz  # PyMuPDF
from PIL import Image

# =========================
# Config
# =========================
DEFAULT_DPI = 300

# Root = folder that contains output/ and pdf-pages/
CUR_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(CUR_DIR, "output")
PDFPAGES_DIR = os.path.join(CUR_DIR, "pdf-pages")
REVIEWS_DIR = os.path.join(CUR_DIR, "reviews")

# Images root per your spec
IMGS_ROOT = os.path.join(REVIEWS_DIR, "imgs_SC")

# If you want one JSON per doc, set True
PER_DOC_JSON = False

# =========================
# Geometry helpers
# =========================
def poly_to_bbox(poly: List[float]) -> Tuple[float, float, float, float]:
    xs = poly[0::2]
    ys = poly[1::2]
    return (min(xs), min(ys), max(xs), max(ys))

def bbox_union(a, b):
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))

def bbox_to_poly(b: Tuple[float,float,float,float]) -> List[float]:
    x0,y0,x1,y1 = b
    return [x0,y0, x1,y0, x1,y1, x0,y1]

def crop_polygon(img: Image.Image, poly_in: List[float], dpi: int = DEFAULT_DPI, pad_in: float = 0.08) -> Image.Image:
    x0,y0,x1,y1 = poly_to_bbox(poly_in)
    x0 = max(0.0, x0 - pad_in); y0 = max(0.0, y0 - pad_in)
    x1 = x1 + pad_in; y1 = y1 + pad_in
    left = int(x0 * dpi); top = int(y0 * dpi)
    right = int(x1 * dpi); bottom = int(y1 * dpi)
    return img.crop((left, top, right, bottom))

# =========================
# PDF rendering
# =========================
def render_pdf_page(pdf_page_path: str, dpi: int = DEFAULT_DPI) -> Image.Image:
    # Each file is a single-page PDF like appendix-1-page-7.pdf
    doc = fitz.open(pdf_page_path)
    page = doc.load_page(0)
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img

# =========================
# OCR word model
# =========================
@dataclass
class Word:
    content: str
    polygon: List[float]
    confidence: float
    offset: int
    length: int

def load_words_from_read(read_json_path: str) -> Tuple[int, List[Word]]:
    data = json.load(open(read_json_path, "r", encoding="utf-8"))
    page = data["pages"][0]
    page_number = page["pageNumber"]  # 1-based within the original doc
    words: List[Word] = []
    for w in page["words"]:
        words.append(
            Word(
                content=w["content"],
                polygon=w["polygon"],
                confidence=float(w["confidence"]),
                offset=int(w["span"]["offset"]),
                length=int(w["span"]["length"]),
            )
        )
    return page_number, words

def word_center(word: Word) -> Tuple[float, float]:
    x0,y0,x1,y1 = poly_to_bbox(word.polygon)
    return ((x0+x1)/2, (y0+y1)/2)

def group_words_into_lines(words: List[Word], y_threshold_in: float = 0.12) -> List[List[Word]]:
    sorted_words = sorted(words, key=lambda w: (word_center(w)[1], word_center(w)[0]))
    lines: List[List[Word]] = []
    current: List[Word] = []
    current_y: Optional[float] = None

    for w in sorted_words:
        _, yc = word_center(w)
        if current_y is None:
            current = [w]
            current_y = yc
            continue

        if abs(yc - current_y) <= y_threshold_in:
            current.append(w)
            current_y = (current_y * (len(current)-1) + yc) / len(current)
        else:
            current = sorted(current, key=lambda ww: word_center(ww)[0])
            lines.append(current)
            current = [w]
            current_y = yc

    if current:
        current = sorted(current, key=lambda ww: word_center(ww)[0])
        lines.append(current)

    return lines

def line_text(line_words: List[Word]) -> str:
    return " ".join(w.content for w in line_words)

def find_word_location(lines: List[List[Word]], target: Word) -> Tuple[int, int]:
    for li, lw in enumerate(lines):
        for wi, w in enumerate(lw):
            if w is target:
                return (li+1, wi+1)
    return (-1, -1)

def context_poly_for_line(line_words: List[Word]) -> List[float]:
    b = None
    for w in line_words:
        wb = poly_to_bbox(w.polygon)
        b = wb if b is None else bbox_union(b, wb)
    return bbox_to_poly(b)

# =========================
# Error rules
# =========================
HOTWORDS_EXACT = {
    "O.F.M.": "abbreviations",
    "O.F.M.Cap.": "abbreviations",
    "c.i.c.m.": "abbreviations",
    "O.": "short-words",
    "O": "short-words",
    "Notarius": "general",
    "Charles": "general",
    "O'Donnell": "general",
    "Ist": "general",
    "1st": "general",
    "transcription": "general",
    "NOTEBOOK": "all-caps",
    "11": "numbers",
    "1": "numbers",
}

# text-level patterns like ... and .. and quotes and equals
TEXT_PATTERNS = [
    ("ellipsis_3", re.compile(r"\.\.\.")),
    ("ellipsis_2", re.compile(r"\.\.")),
    ("quote_straight", re.compile(r"\"")),
    ("equals_sign", re.compile(r"=")),
]

def flag_low_confidence(words: List[Word], threshold: float = 0.90):
    return [w for w in words if w.confidence < threshold]

def flag_hyphenated(words: List[Word]):
    # token endswith '-' -> likely line-wrap hyphenation
    return [w for w in words if w.content.endswith("-") and len(w.content) >= 2]

def flag_small_tokens(words: List[Word], small_ratio: float = 0.65):
    heights = [(poly_to_bbox(w.polygon)[3] - poly_to_bbox(w.polygon)[1]) for w in words]
    if not heights:
        return []
    med = sorted(heights)[len(heights)//2]
    out = []
    for w in words:
        h = poly_to_bbox(w.polygon)[3] - poly_to_bbox(w.polygon)[1]
        if h < small_ratio * med:
            out.append(w)
    return out

def flag_hotwords(words: List[Word]):
    out = []
    lookup = {k.lower(): k for k in HOTWORDS_EXACT.keys()}
    for w in words:
        key = lookup.get(w.content.lower())
        if key:
            out.append(w)
    return out

# =========================
# Saving images + JSON entry
# =========================
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def save_error_images(error_id: int, page_img: Image.Image, context_poly: List[float], error_poly: List[float]) -> Dict[str, str]:
    ensure_dir(IMGS_ROOT)
    folder = os.path.join(IMGS_ROOT, f"{error_id}_images")
    ensure_dir(folder)

    context_path = os.path.join(folder, "context.png")
    error_path = os.path.join(folder, "error.png")

    crop_polygon(page_img, context_poly).save(context_path)
    crop_polygon(page_img, error_poly).save(error_path)

    # relative to CUR_DIR (as requested)
    return {
        "context": os.path.relpath(context_path, CUR_DIR),
        "error": os.path.relpath(error_path, CUR_DIR),
    }

def make_entry(
    error_id: int,
    error_type: str,
    source_document: str,
    page_number: int,
    line_number: int,
    word_number: int,
    character_number: int,
    line_text_str: str,
    error_text: str,
    context_poly: List[float],
    error_poly: List[float],
    images: Dict[str, str],
) -> Dict[str, Any]:
    return {
        "error_id": error_id,
        "error_type": error_type,
        "source_document": source_document,
        "page_number": page_number,
        "line_number": line_number,
        "word_number": word_number,
        "character_number": character_number,
        "context": {"line_text": line_text_str},
        "error": error_text,
        "bounding_boxes": {
            "context": context_poly,
            "error": error_poly,
        },
        "images": images,
    }

# =========================
# Mapping: output/page-N -> pdf-pages/<doc>/<doc>-page-N.pdf
# =========================
PAGE_DIR_RE = re.compile(r"^page-(\d+)$")
PDF_PAGE_RE = re.compile(r"^(?P<doc>.+)-page-(?P<n>\d+)\.pdf$")

def find_pdf_page(doc_name: str, page_n: int) -> Optional[str]:
    # pdf-pages/<doc_name>/<doc_name>-page-<n>.pdf
    cand = os.path.join(PDFPAGES_DIR, doc_name, f"{doc_name}-page-{page_n}.pdf")
    return cand if os.path.exists(cand) else None

# =========================
# Main scan
# =========================
def scan_all() -> Dict[str, Any]:
    ensure_dir(REVIEWS_DIR)
    ensure_dir(IMGS_ROOT)

    all_errors: List[Dict[str, Any]] = []
    error_id = 0

    # output/<doc>/
    doc_names = [d for d in os.listdir(OUTPUT_DIR) if os.path.isdir(os.path.join(OUTPUT_DIR, d))]
    # skip analysis folders if present
    doc_names = [d for d in doc_names if not d.startswith("_analysis")]

    for doc_name in sorted(doc_names):
        doc_out_dir = os.path.join(OUTPUT_DIR, doc_name)
        if not os.path.isdir(doc_out_dir):
            continue

        doc_errors: List[Dict[str, Any]] = []

        # output/<doc>/page-<n>/
        for entry in sorted(os.listdir(doc_out_dir)):
            m = PAGE_DIR_RE.match(entry)
            if not m:
                continue
            page_n = int(m.group(1))
            page_dir = os.path.join(doc_out_dir, entry)

            read_json = os.path.join(page_dir, "read.json")
            if not os.path.exists(read_json):
                continue

            pdf_page_path = find_pdf_page(doc_name, page_n)
            if not pdf_page_path:
                # If your naming ever differs, this is where to extend mapping logic
                continue

            # Load words + group lines
            page_number_from_read, words = load_words_from_read(read_json)
            lines = group_words_into_lines(words)

            # Render page once
            page_img = render_pdf_page(pdf_page_path, dpi=DEFAULT_DPI)

            # ----- word-based errors -----
            low_conf = flag_low_confidence(words, threshold=0.90)
            hyph = flag_hyphenated(words)
            small = flag_small_tokens(words, small_ratio=0.65)
            hot = flag_hotwords(words)

            def emit_word_error(w: Word, err_type: str):
                nonlocal error_id
                ln, wn = find_word_location(lines, w)
                if ln == -1:
                    return
                ctx_line = lines[ln-1]
                ctx_poly = context_poly_for_line(ctx_line)
                err_poly = w.polygon
                images = save_error_images(error_id, page_img, ctx_poly, err_poly)

                entry = make_entry(
                    error_id=error_id,
                    error_type=err_type,
                    source_document=f"{doc_name}.pdf",
                    page_number=page_n,  # page index in your split PDFs
                    line_number=ln,
                    word_number=wn,
                    character_number=w.offset,
                    line_text_str=line_text(ctx_line),
                    error_text=w.content,
                    context_poly=ctx_poly,
                    error_poly=err_poly,
                    images=images,
                )
                doc_errors.append(entry)
                all_errors.append(entry)
                error_id += 1

            for w in low_conf:
                emit_word_error(w, "low_confidence")
            for w in hyph:
                emit_word_error(w, "hyphenated_word")
            for w in small:
                emit_word_error(w, "small_token")
            for w in hot:
                # classify by your categories
                k = HOTWORDS_EXACT.get(w.content) or HOTWORDS_EXACT.get(w.content.upper()) or HOTWORDS_EXACT.get(w.content.lower())
                emit_word_error(w, f"hotword_{k or 'general'}")

            # ----- text-level patterns (optional; bbox approx to line bbox) -----
            # Use reconstructed.html if you want a closer “OCR line”, but line_text() is OK for flagging.
            page_text = "\n".join(line_text(lw) for lw in lines)

            for err_type, rx in TEXT_PATTERNS:
                for mt in rx.finditer(page_text):
                    # attach to nearest line by newline count
                    line_num = page_text[:mt.start()].count("\n") + 1
                    if 1 <= line_num <= len(lines):
                        ctx_line = lines[line_num-1]
                        ctx_poly = context_poly_for_line(ctx_line)
                        # error poly approximation = context poly (you can refine later)
                        images = save_error_images(error_id, page_img, ctx_poly, ctx_poly)
                        entry = make_entry(
                            error_id=error_id,
                            error_type=err_type,
                            source_document=f"{doc_name}.pdf",
                            page_number=page_n,
                            line_number=line_num,
                            word_number=-1,
                            character_number=mt.start(),
                            line_text_str=line_text(ctx_line),
                            error_text=mt.group(0),
                            context_poly=ctx_poly,
                            error_poly=ctx_poly,
                            images=images,
                        )
                        doc_errors.append(entry)
                        all_errors.append(entry)
                        error_id += 1

        # Optionally write per-document JSON
        if PER_DOC_JSON:
            out_doc = os.path.join(REVIEWS_DIR, f"errors_{doc_name}.json")
            with open(out_doc, "w", encoding="utf-8") as f:
                json.dump({"errors": doc_errors}, f, ensure_ascii=False, indent=2)

    return {"errors": all_errors}

if __name__ == "__main__":
    ensure_dir(REVIEWS_DIR)
    result = scan_all()
    out_path = os.path.join(REVIEWS_DIR, "errors.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(result['errors'])} errors to {os.path.relpath(out_path, CUR_DIR)}")
    print(f"Images saved under: {os.path.relpath(IMGS_ROOT, CUR_DIR)}/<error_id>_images/")
