# Step 1 - Extracting Text From Scanned Documents

## The Premise
We will be using the writings of [The Blessed Solanus Casey] (https://www.solanuscasey.org/about-blessed-solanus-casey/writings-of-solanus/) to generate a conversational chatbot that can provide information and references for Solanus Casey. Although we focus on Solanus Casey, this pipeline is largely applicable to any set of scanned documents.

## What we are given

- PDFs of the writings of Solanus Casey (placed in the `source_materials` folder).
  - `Appendix_1.pdf`
  - `Appendix_2.pdf`
  - `Appendix_3.pdf`
  - `Volume_1.pdf`
  - `Volume_2.pdf`
  - `Volume_3.pdf`
  - `Volume_4.pdf`

## What we want to do in this step

- Extract text from the PDFs using Azure OCR Read text extraction.
