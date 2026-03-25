#!/usr/bin/env python3
"""
Author: Nathan Lee, University of Chicago
Editor: David Falk, University of Chicago
Approach: Identify, Analyze, and extract low confidence tokens in OCR outputs for further review
Output: Standardized error entries for low-confidence OCR words.

Outputs:
- error_findings/low_confidence.json
"""
import os
import json
import logging
from time import time

# ── Script configuration variables ───────────────────────

# Threshold for confidence score
# If an OCR reading's confidence is below this threshold then it will consider it an error worthy of review
CONFIDENCE_THRESHOLD = 0.85

# Number of errors per file to extract
# Set to 0 to run the entire file
LIMIT = 5

# Counter for total number of errors written
ERROR_COUNT = 0

# Which files to search through to find low confidence tokens
SOURCE_FOLDERS = [
    "volume-1", "volume-2", "volume-3", "volume-4",
    "appendix-1", "appendix-2", "appendix-3",
]

# ── File paths (all relative to this script's directory) ───────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
OCR_DATA_DIR = os.path.join(SCRIPT_DIR, "..", "raw_data", "ocr_output")
OUTPUT_DIR   = os.path.join(SCRIPT_DIR, "output")
ERRORS_FILE  = os.path.join(OUTPUT_DIR, "low_confidence.json")


def source_document_name(volume_slug):
    return f"{volume_slug}.pdf"


def build_word_to_line_map(lines, words):
    """
    Build a mapping from each word → (line number, line object, word position in line).

    Output:
        word_to_line[id(word)] = (line_number, line, word_number)
    """

    # Dictionary to store mapping between each word and its corresponding line + position
    word_to_line = {}

    # Iterate over all lines on the page
    for line_index, line in enumerate(lines):

        # Extract the spans for the current line
        # Each span represents a contiguous block of text in the full OCR content string
        spans = line["spans"]

        # If the line has no spans, skip it
        if not spans:
            continue

        # Get the start and end character offsets for the line
        # NOTE:
        #   "offset" = starting character index in the full OCR text
        #   "length" = number of characters in the span
        start = spans[0]["offset"]
        end = start + spans[0]["length"]

        # Identify all words that fall within this line's span range
        # A word belongs to this line if its offset lies within [start, end)
        line_words = [word for word in words if start <= word["span"]["offset"] < end]

        # Sort the words in left-to-right reading order
        # We do this by sorting on the word's character offset
        line_words.sort(key = lambda word: word["span"]["offset"])

        # Assign each word its position within the line (1-indexed)
        # Example:
        #   first word → 1
        #   second word → 2
        for word_index, word in enumerate(line_words):

            # Store mapping using id(word) since dicts are not hashable
            # This ensures we can reliably look up the same object later
            word_to_line[id(word)] = (
                line,             # full line object (for text + polygon access)
                line_index + 1,   # line number on the page
                word_index + 1    # word position within the line (1-based index)
            )

    return word_to_line


def get_page(page_file):

    # Ensure it is a valid file and is the correct filetype
    # If it is not, then log a warning that we are skipping this file and return None
    if not (page_file.name.startswith("page_") and page_file.suffix == ".json"):
        logging.warning(f"Skipping {page_file.name}. Does not match required filename schema. Expected: page_{{i}}.json")
        return None
    
    # Open the file and load its data
    with open(page_file, "r", encoding = "utf-8") as f:
        page_file_data = json.load(f)

    # Extract the pages from the data
    # Although each pdf page is only one Azure OCR read page, it is still returned as a list object
    pages = page_file_data.get("pages", [])
    if len(pages) != 1:
        logging.warning(f"Skipping {page_file.name}. Expected 1 page. Got {len(pages)} pages.")
        return None

    # Get the first (and only) page
    page = pages[0]

    return page


def get_low_confidence_words(words):
    # Extract all words that are below the confidence threshold
    low_confidence_words = [w for w in words if w.get("confidence", 1.0) < CONFIDENCE_THRESHOLD]
    
    # If no low confidence words were identified, then proceed to next source folder
    if not low_confidence_words:
        logging.debug(f"No low confidence words identified at {CONFIDENCE_THRESHOLD} confidence threshold.")
        return []
    
    # Strip to max words to LIMIT
    if LIMIT > 0:
        low_confidence_words = low_confidence_words[:LIMIT]
    
    return low_confidence_words


def process_one_word(word, page_number, source_folder, word_to_line_map):
    
    # Declare ERROR_COUNT as a global
    global ERROR_COUNT

    # Extract line number, line object and
    line, line_number, word_number = word_to_line_map[id(word)]

    # Get the text on the line
    line_text = line["content"]

    # Get the bounding polygons for the word and the line
    word_polygon = word["polygon"]
    context_polygon = line["polygon"]

    entry = {
        "error_id":        int("1" + str(ERROR_COUNT)),
        "error_type":      "low_confidence",
        "source_document": f"{source_folder}.pdf",
        "page_number":     page_number,
        "line_number":     line_number,
        "word_number":     word_number,
        "error":           word["content"],
        "context":         line_text,
        "bounding_boxes": {
            "error": word_polygon,
            "context": context_polygon,
        },
    }
    return entry


def run_one_source_folder(source_folder):

    # Log the current source folder run
    logging.info(f"\n{'-' * 70}")
    logging.info(f"  CURRENT DOCUMENT: {source_folder}")
    logging.info(f"{'-' * 70}\n")

    # Define the path to the current source folder
    source_folder_path = os.path.join(OCR_DATA_DIR, source_folder)
    
    # Ensure folder exists, otherwise raise an error
    if not os.path.exists(source_folder_path):
        logging.error(f"{source_folder} folder does not exist at the specified location: {source_folder_path}")
        return []

    cur_errors = []
    
    # Iterate over the files in each folder in order of page number
    # key = lambda page_file: int(page_file.stem.split("_")[1]) is a little messy but let's break it down
    # from the inside out...
    #   1. ".split("_")[1]" extracts the page number from the filename 
    #           e.g. "page_532" --> "532"
    #
    #   2. ".stem." takes a path to a file and extracts the filename without the extension 
    #           e.g. "/C:/Users/Desktop/spanish_video.mp4" --> "spanish_video"
    #
    #   3. "int(...)" takes a string and casts it to an integer
    #           e.g. "532" --> 532
    for page_file in sorted(os.scandir(source_folder_path), key = lambda page_file: int(page_file.stem.split("_")[1])):
        
        # Get the page data from the file and check for proper formatting
        page = get_page(page_file)

        # Extract the page number if it was a valid page, skip to next page if not
        if page:
            page_number = page.get("pageNumber")
        else:
            continue
        
        # Extract the words and lines on the page (according to Azure OCR) as lists
        words = page["words"]
        lines = page["lines"]

        # Build a map that accepts a word and returns the line it is on
        word_to_line_map = build_word_to_line_map(lines, words)

        # Get the low confidence words, accounting for the LIMIT variable
        low_confidence_words = get_low_confidence_words(words)
        if not low_confidence_words:
            continue

        # Build a json entry for each word
        for word in low_confidence_words:
            entry = process_one_word(word, page_number, source_folder, word_to_line_map)
            if entry:
                cur_errors.append(entry)
                ERROR_COUNT += 1

    # Return the errors for this source folder    
    return cur_errors


def main():

    # Get start time so we can report total runtime
    start_time = time()

    # Log the current pipeline run
    logging.info(f"\n{'=' * 70}")
    logging.info(f"  IDENTIFYING LOW CONFIDENCE ERRORS")
    logging.info(f"{'=' * 70}\n")

    # List to store all of the low confidence errors
    errors = []

    # Iterate over all of the ocr output folders listed in the SOURCE_FOLDERS config variable
    for source_folder in SOURCE_FOLDERS:
        errors.extend(run_one_source_folder(source_folder))

    # Create the output directory if it does not exist
    os.makedirs(OUTPUT_DIR, exist_ok = True)

    # Write error to output directory
    with open(ERRORS_FILE, "w", encoding="utf-8") as f:
        json.dump({"errors": errors}, f, indent = 2, ensure_ascii = False)

    # Log number of errors, file lcoation, and total runtime
    logging.info(f"Wrote {len(errors)} errors to {ERRORS_FILE}")
    logging.info(f"Total Runtime: {time() - start_time} seconds")


if __name__ == "__main__":
    main()
