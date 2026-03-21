# =====================================================
# GLOBAL SETTINGS
# =====================================================

# Name of the source data folder. This folder should be placed in the root of this entire project
# Please place all of your desired documents in this folder.
# NOTE: At present, the code only handles pdfs
# Assuming that SOURCE_DATA_FOLDER = "source_data", here are examples of acceptable and unacceptable setups:
#   Good: Solanus-Project-Pipeline/source_data/Volume_1.pdf
#   Good: Solanus-Project-Pipeline/source_data/Appendix_3.pdf
#   Bad: Solanus-Project-Pipeline/source_data/Volume_1.docx
#   Bad: Solanus-Project-Pipeline/step_1/source_data/Volume_1.pdf
SOURCE_DATA_FOLDER = "source_data"

# Choose whether or not to deskew the images. In general it is helpful, but run the pdf cleaner script once and check for warnings.
# Always manually review and if you notice that pages are tilted weirdly or something feels "clearly wrong", then set this to False.
DESKEW_FLAG = True


# -----------------------------------------------------
# FILE CHOICES
# -----------------------------------------------------

# NOTE: The code will first perform the "FILES_TO_RUN" logic and then perform the "FILES_TO_EXCLUDE" logic.
# Example: 
#   Assume you set the environment variables as follows:
#       FILES_TO_RUN = ["Volume_1.pdf", "Volume_2.pdf", "Volume_3.pdf"]
#       FILES_TO_EXCLUDE = ["Volume_2.pdf"]
#   
#   Files that will actually run:
#       1. Volume_1.pdf
#       2. Volume_3.pdf 

# Optional: If you would like to only run a subset of the files, then you can specify their filenames here
# If you leave it blank, then it will run all files in the source data folder
FILES_TO_RUN = []

# Optional: If you would like to run all but a subset of the files, then you can specify their filenames here
# If you leave it blank, then it will not exclude any files
FILES_TO_EXCLUDE = []