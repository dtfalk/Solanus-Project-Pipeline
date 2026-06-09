# Migrates DPI 300 polygons to DPI 150
# Basically a simple divide by 2
import json
from math import ceil
from pathlib import Path
from shutil import copy2

# Path variables
CUR_DIR     = Path(__file__).parent.resolve()
OUTPUT_PATH = CUR_DIR / "new_polygon_page_data"

def process_one_file(filepath: Path) -> None:
    """ 
    Accepts a path to an existing file, scales the polygon data in the file, and saves to new location
    """

    # Generally helpful things to have extracted 
    filename = filepath.name # "page_xyz.json"
    doc_folder_name = filepath.parent.parent.resolve().name # "Volume_x, Appendix_y"


    # Step 1: Find the size (pixels) of the current/old pdfs and derive scale factors 
    # ----------------------------------------------
    new_page_size_path = OUTPUT_PATH /  doc_folder_name / "page_sizes" / filename
    old_page_size_path = filepath.parent.parent.resolve() / "page_sizes" / filename

    with open(new_page_size_path, mode = "r", encoding = "utf8") as f:
        page_size_data  = json.load(f)
        new_page_width  = page_size_data["width_pixels"]
        new_page_height = page_size_data["height_pixels"]
    
    with open(old_page_size_path, mode = "r", encoding = "utf8") as f:
        page_size_data  = json.load(f)
        old_page_width  = page_size_data["width_pixels"]
        old_page_height = page_size_data["height_pixels"]

    width_scale_factor  = new_page_width  / old_page_width    
    height_scale_factor = new_page_height / old_page_height     
    
    # Step 2: Get old polygon data, rescale, and save 
    # ----------------------------------------------
    # Open the file and read into a dict
    with open(filepath, mode = "r") as f:
        
        # Load data from the input json
        file_data = json.load(f)
        
        # Extract the list of polygons, iterate over, and apply the scale factor
        polygon_list = file_data["polygon"]
        for polygon in polygon_list:
            polygon["x"] = ceil(polygon["x"] * width_scale_factor)
            polygon["y"] = ceil(polygon["y"] * height_scale_factor)
        
        # Overwrite the original json data in memory 
        # (Only things that we need to change)
        file_data["page_width"]   = new_page_width
        file_data["page_height"]  = new_page_height
        file_data["polygon"]      = polygon_list

        # Save to new directory
        destination = OUTPUT_PATH / doc_folder_name / "polygons" / filename
        with open(destination, mode = "w", encoding = "utf8") as f:
            json.dump(file_data, f, indent = 2)

    
def main():
    
    # Create output dir if is does not exist
    OUTPUT_PATH.mkdir(parents = True, exist_ok = True)

    # Path to the unscaled polygon data
    input_folder = CUR_DIR / "polygon_page_data"

    # Iterate over the folders in the dir
    for source_doc_dir in input_folder.iterdir():

        # Skip if not a folder
        if not source_doc_dir.is_dir():
            continue

        # Create save directory for each relevant dir
        save_sub_dir = OUTPUT_PATH / source_doc_dir.name
        save_sub_dir.mkdir(parents = True, exist_ok = True)
        
        # Find the subdirectory containing the jsons with polygon data
        polygon_json_dir = source_doc_dir / "polygons"
        for json_filepath in polygon_json_dir.iterdir():
            process_one_file(json_filepath)

if __name__ == "__main__":
    main()