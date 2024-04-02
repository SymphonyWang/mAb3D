import pandas as pd
import subprocess
import os

ab3d_sheet = "/home/xiw4013/mAb3D/Ab3D-E-Screening - Sheet1.csv"
inputlist = "/home/xiw4013/mAb3D/inputlistV2.csv"
input_dir = "/home/xiw4013/mAb3D/input"
output_dir = "/home/xiw4013/mAb3D/zarr_upload/"

# Read the CSV file
df = pd.read_csv(ab3d_sheet)

# Define the mapping of columns to section and exposure
mapping = {
    'Ms1 Result': '0',
    'Ms2 Result': '1',
    'Hu1 (JC7) Result': '2',
    'Hu2 (JC8) Result': '3',
    'Hu3 (JC9) Result': '4',
    'Gut1 Result': '5',
    'Gut2 Result': '6',
}

# Create a new DataFrame with the required columns
new_df = pd.DataFrame(columns=['filename', 'section', 'exposure', 'transferflag', 'uploadflag'])

# Iterate over the rows of the DataFrame
for _, row in df.iterrows():
    for column, section in mapping.items():
        exposure = row[column]
        if pd.notna(exposure) and exposure.startswith('Exp'):
            exposure_number = exposure[3:]  # Remove 'Exp' prefix
            new_row = {
                'filename': row['Czi Name'],
                'section': section,
                'exposure': exposure_number,
                'transferflag': '0',
                'uploadflag': '0',
                'shortname': None,
                'orgURL': None,
            }
            new_df = new_df._append(new_row, ignore_index=True)

# Set each column as str type
new_df = new_df.astype(str)

# Write the new DataFrame to a CSV file
new_df.to_csv(inputlist, index=False)
print(f"Input Items List was Saved the new DataFrame to {inputlist}")

# def Convert czi to ome-zarr using bioformats2raw
def convert_to_ome_zarr(input_path, output_path): 
    
    # dimension-order Default ('XYZCT')
    command = ['bioformats2raw', input_path, output_path, '--compression', 'zlib', '--compression-properties', 'level=9', '--resolutions', '5']
    result = subprocess.run(command, stderr=subprocess.PIPE)

     # Check if the command was successful
    if result.returncode == 0:
        return True, "" # Return True for success and None for error detail
    else:
    # If there was an error, return False and the error details
        error_detail = result.stderr.decode('utf-8') if result.stderr else "Unknown error"
        return False, error_detail
    
def main():
  # Read the inputlist CSV file
  inputlist_df = pd.read_csv(inputlist, dtype={'filename': str, 'section': str, 'exposure': str, 'uploadflag': str})

  # Generate a list of .czi files from the inputlist DataFrame,If there are files with the same name, merge them into one
  czi_files = inputlist_df[inputlist_df['filename'].str.endswith('.czi')]['filename'].unique().tolist()
  
  # Iterate over the rows of the inputlist DataFrame
  for filename in czi_files:
    # Check if the file exists in the input directory
    input_path = os.path.join(input_dir, filename)
    if not os.path.exists(input_path):
      print(f"File {filename} does not exist in the input directory. Skipping to next file.")
      continue

    # Remove the .czi extension from the base filename
    base_name = os.path.splitext(filename)[0]

    # Construct the output path
    output_path = os.path.join(output_dir, f"{base_name}.zarr")
    
    # Check if the output directory already exists
    if os.path.exists(output_path):
      inputlist_df['transferflag'] = inputlist_df['transferflag'].astype(str)
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = '2'
      print(f"Output directory {output_path} already exists. Skipping file {filename}.")
      continue
    
    # Convert the file to OME-Zarr format
    success, error_detail = convert_to_ome_zarr(input_path, output_path)
    
    if not success:
      inputlist_df['transferflag'] = inputlist_df['transferflag'].astype(str)
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = 'e'
      print(f"Error converting file {filename}. Error details: {error_detail}. Skipping to next file.")
    else:
      print(f"File {filename} converted successfully.")
      # When the conversion is successful, update all 'transferflag' under this filename in inputlist_df to '1'
      inputlist_df['transferflag'] = inputlist_df['transferflag'].astype(str)
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = '1'
  
  # Write the updated inputlist DataFrame to the CSV file
  inputlist_df.to_csv(inputlist, index=False)
  
  print(f"All Files Conversion complete. CSV file written to {inputlist}.")

# Call the main function
main()