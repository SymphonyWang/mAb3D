import pandas as pd
import subprocess
import os
from czifile import CziFile
from pylibCZIrw import czi as pyczi
import json
import numpy as np

# Description ver7: add adjust bringtness to 1%~99% pixel value
ab3d_sheet = "/mnt/d/Xiaoman/001_mAb3D/05-E2/workfile/Ab3D-E-Screening - Ab3D-E2_BS1-27.csv"
inputlist = "/mnt/d/Xiaoman/001_mAb3D/05-E2/workfile/inputlist_E2_BS1-27.csv"
input_dir = "/mnt/d/Xiaoman/001_mAb3D/05-E2/input"
output_dir = "/mnt/d/Xiaoman/001_mAb3D/05-E2/zarr_upload"

# def Convert czi to ome-zarr using bioformats2raw
def convert_to_ome_zarr(input_path, output_path, secnum): 
    series = ','.join(str(i) for i in range(secnum))
    
    # dimension-order Default ('XYZCT')
    command = ['bioformats2raw', input_path, output_path, '--compression', 'zlib', '--compression-properties', 'level=9', '--resolutions', '5', '--series', series]
    result = subprocess.run(command, stderr=subprocess.PIPE)

     # Check if the command was successful
    if result.returncode == 0:
        return True, "" # Return True for success and None for error detail
    else:
    # If there was an error, return False and the error details
        error_detail = result.stderr.decode('utf-8') if result.stderr else "Unknown error"
        return False, error_detail
    
def read_czi_metadata(input_dir):
    with CziFile(input_dir) as czi:
        metadata = czi.metadata(raw=False)  # Get parsed metadata
        return metadata

def read_czidoc(input_dir):
    with pyczi.open_czi(input_dir) as czidoc:

        # get the bounding boxes for each individual scene
        scenes_bounding_rectangle = czidoc.scenes_bounding_rectangle
        # Extract width and height from each Rectangle object
        mapping = {k: (v.w, v.h) for k, v in scenes_bounding_rectangle.items()}
        return mapping

def calculate_histogram_and_percentiles(czi_path, percentiles=[1, 99], bins=16384):
    """
    Quickly calculate the 1% and 99% percentiles of non-zero pixels in a CZI file.

    Args:
        czi_path: Path to the CZI file.
        percentiles: List of percentiles to compute, default is [1, 99].
        bins: Number of bins for the histogram, default is 16384.

    Returns:
            dict: A dictionary containing min, max, percentiles, and bin size for each scene and channel.
                  Format: {scene_index: {channel_index: {'min': min_value, 'max': max_value, 'percentiles': [p1, p99], 'bin_size': bin_size}}}
    """

    if not os.path.isfile(czi_path):
        print(f"Error: File not found - {czi_path}")
        return None

    try:
        with CziFile(czi_path) as czi:
            # Get dimensions and axes information
            dims = czi.shape  # Example: (S, C, Y, X, ...)
            dim_order = czi.axes  # Example: 'SCYX0'
            print(f"Processing file with dimensions: {dims}")
            print(f"Dimension order of the CZI file: {dim_order}")

            # Read the data array
            data = czi.asarray()

            # Initialize results dictionary
            results = {}
            num_scenes = dims[0]  # S dimension
            num_channels = dims[1]  # C dimension

            for scene_idx in range(num_scenes):
                results[scene_idx] = {}
                for channel_idx in range(num_channels):
                    # Extract the data for the current scene and channel
                    channel_data = data[scene_idx, channel_idx, :, :, 0]

                    # Flatten the data to 1D for histogram calculation
                    flattened_data = channel_data.flatten()

                    # Filter out zero pixels
                    non_zero_data = flattened_data[flattened_data > 0]

                    if len(non_zero_data) > 0:
                        # Compute histogram
                        # hist, bin_edges = np.histogram(non_zero_data, bins=bins)

                        # Compute percentiles (1% and 99%)
                        p1 = np.percentile(non_zero_data, percentiles[0])
                        p99 = np.percentile(non_zero_data, percentiles[1])

                        # store the results
                        results[scene_idx][channel_idx] = {                           
                            'percentiles': [p1, p99], 
                        }

                    else:
                        print(f"No non-zero data for Scene {scene_idx}, Channel {channel_idx}.")

            return results

    except Exception as e:
        print(f"Error processing {czi_path}: {str(e)}")
        return None
    
def main():

  # Read the CSV file
  df = pd.read_csv(ab3d_sheet)

  # Create a new DataFrame with the required columns
  new_df = pd.DataFrame(columns=['filename', 'secnum', 'secname', 'transferflag', 'uploadflag', 'orgURL', 'shortname', 'markername', 'type', 'width', 'height', "1%_pixel_c0", "99%_pixel_c0", "1%_pixel_c1", "99%_pixel_c1"])

  scene_columns = ['Scene 1', 'Scene 2', 'Scene 3', 'Scene 4', 'Scene 5', 'Scene 6']

  for index, row in df.iterrows():
      if not pd.isnull(row['Czi Filename']):
          for i, scene in enumerate(scene_columns):
            if pd.notnull(row[scene]):  # Check if the 'Scene' value is not null          
              new_row = {
                'filename': row['Czi Filename'],             
                'secnum': i,
                'secname': row[scene],
                'transferflag': '0',
                'uploadflag': '0',
                'shortname': None,
                'orgURL': None,
                'markername': row['Official Symbol'],
                'type': None,
                'width': '0',
                'height': '0',
                '1%_pixel_c0': '0',
                '99%_pixel_c0': '0',
                '1%_pixel_c1': '0',
                '99%_pixel_c1': '0'
              }
              new_df = new_df._append(new_row, ignore_index=True)

  # Set each column as str type
  new_df = new_df.astype(str)

  # Write the new DataFrame to a CSV file
  new_df.to_csv(inputlist, index=False)
  print(f"Input Items List was Saved the new DataFrame to {inputlist}")
    
  # Read the inputlist CSV file
  inputlist_df = pd.read_csv(inputlist, dtype={'filename': str, 'transferflag': str, 'uploadflag': str, 'secnum': str, 'width': int, 'height': int, '1%_pixel_c0': int, '99%_pixel_c0': int, '1%_pixel_c1': int, '99%_pixel_c1': int})

  # Create a new DataFrame where the 'filename' column doesn't contain NaN values
  inputlist_df_no_nan = inputlist_df.dropna(subset=['filename'])

  # Generate a list of .czi files from the inputlist DataFrame
  czi_files = inputlist_df_no_nan[inputlist_df_no_nan['filename'].str.endswith('.czi')]['filename'].unique().tolist()

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
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = '2'
      inputlist_df.to_csv(inputlist, index=False)
      print(f"Output directory {output_path} already exists. Skipping file {filename}.")
      continue
    
    # Read czi_metadata
    metadata = read_czi_metadata(input_path)
    # Convert the metadata to a JSON string
    metadata_json = json.dumps(metadata, indent=4)
    # Write the metadata to a JSON file
    with open(f"{output_dir}/{base_name}_metadata.json", "w") as f:
        f.write(metadata_json)

    # Get the number of scenes of inputlist
    num_scenes = inputlist_df.groupby('filename')['secnum'].nunique()[filename]
    # Get the number of scenes in the metadata
    num_sc_meta = int(metadata["ImageDocument"]["Metadata"]["Information"]["Image"]["SizeS"])
    #num_ch_meta = int(metadata["ImageDocument"]["Metadata"]["Information"]["Image"]["SizeC"])


    if num_scenes != num_sc_meta:
      print(f"num_scenes {filename} and {num_scenes} and {num_sc_meta}")
      print(f"Warning converting file {filename}. Warning: Number of scenes in metadata is different from the secnum in inputlist")

    # Read czidoc
    czidoc = read_czidoc(input_path)

    # Update the width and height in the DataFrame
    for key, value in czidoc.items():
        key = str(key)
        inputlist_df.loc[(inputlist_df['filename'] == filename) & (inputlist_df['secnum'] == key), ['width', 'height']] = value

    # Calculate histogram and percentiles
    results = calculate_histogram_and_percentiles(input_path)

    # Update the DataFrame with the calculated percentiles
    for scene_idx, channels in results.items():
        for channel_idx, channel_data in channels.items():
            p1, p99 = channel_data['percentiles']

            # Update the DataFrame with the calculated percentiles
            # Cast to int before assignment
            inputlist_df.loc[(inputlist_df['filename'] == filename) & (inputlist_df['secnum'] == str(scene_idx)), [f'1%_pixel_c{channel_idx}', f'99%_pixel_c{channel_idx}']] = [int(p1), int(p99)]


    #Convert the file to OME-Zarr format
    success, error_detail = convert_to_ome_zarr(input_path, output_path, num_scenes)
    
    if not success:
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = 'e'
      print(f"Error converting file {filename}. Error details: {error_detail}. Skipping to next file.")
    else:
      print(f"File {filename} converted successfully.")
      # When the conversion is successful, update all 'transferflag' under this filename in inputlist_df to '1'
      inputlist_df.loc[inputlist_df['filename'] == filename, 'transferflag'] = '1'

    # Write the filtered DataFrame to a CSV file
    inputlist_df.to_csv(inputlist, index=False)
    print(f"Filtered Files Conversion complete. CSV file written to {inputlist}.")
  
  print(f"All Files Conversion complete. CSV file written to {inputlist}.")

# Call the main function
main()