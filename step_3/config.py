# =====================================================
# GLOBAL SETTINGS
# =====================================================

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

# -----------------------------------------------------
# PAGE CHOICES
# -----------------------------------------------------
# 
# Example: 
#   Assume you set the environment variables as follows:
#       FILES_TO_RUN = ["Volume_1.pdf", "Volume_2.pdf", "Volume_3.pdf"]
#       FILES_TO_EXCLUDE = ["Volume_2.pdf"]
#   
#   Files that will actually run:
#       1. Volume_1.pdf
#       2. Volume_3.pdf 
# Optional: Process  the first N pages in each file.
# Use 0 to run all pages in the document
# Example: 
#   FIRST_N_PAGES = 5  ===> Pages 1-5 will be processed
FIRST_N_PAGES = 0

# -----------------------------------------------------
# -----------------------------------------------------