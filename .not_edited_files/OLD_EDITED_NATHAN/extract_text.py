import os
import json
import io
import logging
from time import time 
from pathlib import Path
from pypdf import PdfReader, PdfWriter
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from dotenv import load_dotenv
from config import FILES_TO_RUN, FILES_TO_EXCLUDE, FIRST_N_PAGES


def extract_text(endpoint, key, pdf_path, output_folder, first_n_pages):
    """
    Runs the Azure OCR prebuilt-read model once per document.
    """

    # Get the start time 
    start_time = time()

    # Extract document name and log it
    document_name = pdf_path.name
    logging.info(f"\n{'=' * 70}")
    logging.info(f"Extracting text for {document_name}")
    logging.info(f"{'=' * 70}\n")

    # Read the PDF into memory
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    # Extract first n pages if user specified FIRST_N_PAGES
    if first_n_pages > 0: 

        # Read the byte stream as a pdf
        # Initializes a reader and a writer
        reader = PdfReader(io.BytesIO(pdf_bytes))
        writer = PdfWriter()

        # Grab first n pages (assuming pdf has that many pages)
        for page in reader.pages[:min(first_n_pages, len(reader.pages))]:
            writer.add_page(page)

        # Create output buffer and write first n pages to that buffer
        output = io.BytesIO()
        writer.write(output)

        # Overwrite our initial variable with the first n pages buffer
        pdf_bytes = output.getvalue()


    # Initialize the Document Intelligence client
    client = DocumentIntelligenceClient(endpoint = endpoint, credential = AzureKeyCredential(key))

    # Send request to Azure OCR to analyze the document
    poller = client.begin_analyze_document(
        model_id = "prebuilt-read",
        body     = io.BytesIO(pdf_bytes),
    )

    # Grab the result once the extracted text is returned and then cast it to a Python Dict
    result = poller.result()
    result_dict = result.as_dict()

    # Save the extracted text document
    save_path = output_folder / f"{document_name}.json"
    with open(save_path, "w", encoding="utf-8") as output_file:
        json.dump(result_dict, output_file, ensure_ascii = False, indent = 2)
    
    # Log the results
    total_pages = len(result_dict.get("pages") or [])
    runtime = time() - start_time
    logging.info(f"Processed {total_pages} pages for {document_name} | Runtime: {runtime:.2f}s")
    logging.info(f"{'=' * 70}\n")
    logging.info(f"{'=' * 70}\n")


def get_pdf_paths(pdf_folder):
    """
    Returns a list of PDF paths to process, based on FILES_TO_RUN, and FILES_TO_EXCLUDE.
    """ 

    # If FILES_TO_RUN is specified, then grab those files. Otherwise, grab all pdfs in source data dir root
    pdfs = []
    if FILES_TO_RUN:
        pdfs = [pdf_folder / f for f in FILES_TO_RUN 
                if ((pdf_folder / f).exists() 
                and (pdf_folder / f).is_file() 
                and (pdf_folder / f).suffix.lower() == ".pdf" 
                and not f in FILES_TO_EXCLUDE)]
        
        if len(pdfs) != len(FILES_TO_RUN):
            logging.warning(f"We were unable to find all of the files that you requested in FILES_TO_RUN. If this is unexpected then please check:\n  1. Are all filenames spelled correctly?\n  2. Are all files in the correct folder?\n    Correct Folder: {pdf_folder}\n  3. Are all files PDFs?")
    else:
        pdfs = [f for f in pdf_folder.iterdir() 
                if (f.is_file() and f.suffix.lower() == ".pdf" and not f.name in FILES_TO_EXCLUDE)]
    
    return pdfs


def main():
    
    # Set logging config
    logging.basicConfig(level=logging.INFO)

    # Grab the environment variables necessary for connecting to the Azure OCR text extraction service
    # Code will fail with KeyError if either is missing
    load_dotenv()
    endpoint = os.environ["DOCUMENT_INTELLIGENCE_ENDPOINT"]
    api_key = os.environ["DOCUMENT_INTELLIGENCE_KEY"]

    # Construct the path to the data folder in the previous step
    pdf_folder = Path(__file__).resolve().parent.parent / "step_1" / "cleaned_pdfs" 

    # Get the paths to the pdfs
    pdf_paths = get_pdf_paths(pdf_folder)

    # Get the path to directory where we save outputs and create output folder if necessary
    output_folder = Path(__file__).parent / "ocr_output"
    os.makedirs(output_folder, exist_ok = True)

    # Iterate over all of the pdf files and use Azure OCR to extract
    for pdf_path in pdf_paths:
        extract_text(endpoint, api_key, pdf_path, output_folder, FIRST_N_PAGES)


if __name__ == "__main__":
    main()