from pathlib import Path
import json

# Source doc names and page to sample for each to get a sense of the schema I was using when I did that document
SRC_FILENAMES = ["Appendix_1", "Appendix_2", "Appendix_3", "Volume_1", "Volume_2", "Volume_3", "Volume_4"]
SAMPLE_PAGES   = [[1, 4, 6, 15, 19, 28, 42],
                  [1, 6, 13, 18, 19, 20, 50, 53, 65, 70],
                  [1, 4, 5, 6, 7, 9, 11, 19, 21],
                  [4, 8, 60, 63, 67, 82, 88, 92, 98, 116, 200, 244, 245, 270, 274], 
                  [1, 2, 59, 75, 84, 85, 135, 136, 165, 208, 261, 347],
                  [1, 7, 24, 53, 78, 119, 159, 218, 269, 310],
                  [1, 6, 7, 82, 135, 165, 218, 232]]

# Folder which this file is currently located in
CUR_DIR = Path(__file__).parent.resolve()

# Get the full paths to the JSON polygon data for each filename
FILEPATHS = []
for i, src_filename in enumerate(SRC_FILENAMES):
    for j, page in enumerate(SAMPLE_PAGES[i]):
        FILEPATHS.append(CUR_DIR / "step_3" / "extracted_samples" / src_filename / f'page_{page:03}' / f'page_{page:03}.json')


filekeys = []
for filepath in FILEPATHS:
    with open(filepath, mode = "r") as fp:
        cur_data = json.load(fp)
        cur_keys = (set(list(cur_data["documents"]["doc_1"].keys())), filepath.parent.parent.name)
        filekeys.append(cur_keys)

transition_files = []
for i, (f1_keys, f1_name) in enumerate(filekeys):
    
    # Skip the last index bc nothing comes after
    if i == len(filekeys) - 1:
        break

    # Grab the next file's keys
    f2_keys = filekeys[i + 1][0]
    f2_name = filekeys[i + 1][1]

    # Grab the keys in f1 and not f2 and vice versa
    only_in_f1 =  f1_keys.difference(f2_keys)
    only_in_f2 =  f2_keys.difference(f1_keys)

    if only_in_f1 or only_in_f2:
        print(f"  \nDifference between {f1_name} and {f2_name}:")
        transition_files.append(f2_name)
        
        # Print the keys present in file_i but not present in file_i+1
        if only_in_f1:
            print(f"    {f1_name} contains the following keys not present in {f2_name}:")
            for j, key in enumerate(only_in_f1):
                print(f"        {j + 1}. {key}")
        
        # Print the keys present in file_i+1 but not present in file_i
        if only_in_f2:
            print(f"    {f2_name} contains the following keys not present in {f1_name}:")
            for j, key in enumerate(only_in_f2):
                print(f"        {j + 1}. {key}")
    
print(f"\n\nTransition Files: {transition_files}")
