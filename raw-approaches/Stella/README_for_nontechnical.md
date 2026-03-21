
---

## File 2: `README_for_NonTechnical.md`

```markdown
# What This Tool Does (Non-Technical Explanation)

This system scans scanned historical documents and finds places where the computer likely misread the text.

It does not change the text automatically.  
It identifies suspicious areas and documents them carefully for review.

---

## Why This Was Needed

The Solanus documents are scanned PDFs.

Although the text can be extracted automatically, it often contains mistakes such as:
- Small fractions misread as letters or numbers
- Abbreviations interpreted incorrectly
- Random punctuation inserted
- Words broken incorrectly across lines
- Numbers mistaken for letters

Because the collection spans thousands of pages, manual review alone is not realistic.

---

## What This System Produces

For every suspicious word or symbol, the system creates:
1. A record in a master list (`errors.json`)
2. A zoomed image of the exact word
3. A wider image showing the full line

This allows humans to quickly verify whether the extracted text is correct.

---

## Where the Master List Is

There is a file called:
reviews/errors.json


It contains many entries like:

“On page 10 of appendix-1, line 8, the word ‘O.’ was flagged because the OCR confidence was low.”

Each entry includes:
- Which document
- Which page
- The exact line text (for context)
- The exact word or symbol that looks suspicious
- Image references showing the original scan

---

## Where the Images Are Stored

For every issue, two images are saved under:
reviews/imgs_SC/


Each error has its own folder:
0_images/
1_images/
2_images/
...


Inside each folder:
- `context.png` → the full line from the original scanned page
- `error.png` → a close-up of the specific word/symbol

This makes review transparent and visual.

---

## What This Enables

Instead of manually reading 1,000+ pages looking for OCR mistakes, we now have:
- a structured list of likely problems
- direct visual evidence for each one

This enables:
- faster quality control
- more accurate transcription
- systematic correction workflows
- auditability (every fix can be traced back to the scan image)

---

## In Plain Terms

The tool is like a spell-checker for scanned documents, but instead of guessing fixes, it carefully marks and documents possible problems so humans can review them quickly.

---

Author: Stella (Hyerin) Chae  
Solanus OCR Data Cleaning Project


