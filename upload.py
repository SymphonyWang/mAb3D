import os
import subprocess
import pandas as pd
import shutil
from urllib.parse import quote

# Define the server name and upload path
upload_path = '/home/xiw4013/mAb3D/zarr_upload/'
inputlist = "/home/xiw4013/mAb3D/inputlistV2.csv"
s3_bucket = 'mAb3D'
resolutions = range(5)
awsbucket_path = 's3://mAb3D/test/'
zarr_upload_list = "/home/xiw4013/mAb3D/Zarr_upload_list.csv"

# Function to upload zarr file to AWS S3 bucket
def upload_to_s3(zarr_file, section_index):
    zarrpath = upload_path + zarr_file + '/' + str(section_index) + '/'
    awspath = awsbucket_path + zarr_file + '/' + str(section_index) + '/'
    # print(f"zarrpath:~~~~~~~~~~~~ {zarrpath}")
    # print(f"awspath:~~~~~~~~~~~~ {awspath}")

    aws_command = ['aws', '--endpoint-url', 'https://redcloud.cac.cornell.edu:8443/', '--no-verify', 's3', '--profile', 'CAC', 'cp',  '--recursive', zarrpath, awspath]
    result = subprocess.run(aws_command, stderr=subprocess.PIPE)

     # Check if the command was successful
    if result.returncode == 0:
        return True, "" # Return True for success and None for error detail
    else:
    # If there was an error, return False and the error details
        error_detail = result.stderr.decode('utf-8') if result.stderr else "Unknown error"
        return False, error_detail
    
# Function to delete a folder and all its contents    
def delete_folder(folder_path):
    try:
        # Use shutil.rmtree() to remove the directory and all its contents
        shutil.rmtree(folder_path)
        print(f"The folder '{folder_path}' and all its contents have been successfully deleted.")
    except Exception as e:
        print(f"Unable to delete the folder '{folder_path}': {e}")

def create_org_URL (zarr_file, section):
  try:     

      # encode zarr file URL
      zarr_file_encode = quote(zarr_file)
      orgURL = "https://neuroglancer-demo.appspot.com/#!%7B%22dimensions%22:%7B%22x%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22y%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22z%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22t%22:%5B1%2C%22%22%5D%7D%2C%22position%22:%5B6847%2C2872%2C0.5%2C0%5D%2C%22crossSectionScale%22:9.227814895369777%2C%22projectionOrientation%22:%5B-0.3254466950893402%2C0.8991797566413879%2C0.2924036383628845%2C0.007767873350530863%5D%2C%22projectionScale%22:12314.085503119006%2C%22layers%22:%5B%7B%22type%22:%22image%22%2C%22source%22:%22zarr2://https://redcloud.cac.cornell.edu:8443/swift/v1/mAb3D/test/{zarr_file_encode1}/{section1}/%22%2C%22localDimensions%22:%7B%22c%27%22:%5B1%2C%22%22%5D%7D%2C%22localPosition%22:%5B1%5D%2C%22tab%22:%22rendering%22%2C%22shaderControls%22:%7B%22normalized%22:%7B%22range%22:%5B0%2C700%5D%7D%7D%2C%22crossSectionRenderScale%22:0.14126447292866626%2C%22name%22:%22layer0%22%7D%5D%2C%22selectedLayer%22:%7B%22size%22:637%2C%22visible%22:true%2C%22layer%22:%22layer0%22%7D%2C%22layout%22:%22xy%22%7D".format(zarr_file_encode1=zarr_file_encode, section1=section)

      section_map = {
          '0': 'Ms1',
          '1': 'Ms2',
          '2': 'Hu1',
          '3': 'Hu2',
          '4': 'Hu3',
          '5': 'Gut1',
          '6': 'Gut2'
      }

      shortname = zarr_file.replace('.zarr', '') + '-' + section_map[section]
      # print(f"shortname     ] {shortname}")
      # print(f"orgURL     ] {orgURL}")
      print(f"creating org URL for {shortname} have been successfully completed.")
      return shortname, orgURL
  except Exception as e:
      print(f"Error creating org URL: {e}")
      return None, None

# Read the inputlist.csv using pandas
df = pd.read_csv(inputlist, dtype={'filename': str, 'section': str, 'exposure': str, 'uploadflag': str, 'orgURL': str, 'shortname': str})
print(f"df {df}")

# open a new csv file to write the updated information using pandas
# df[['filename', 'section']].to_csv(zarr_upload_list, index=False)

# Traverse filename in upload path
for index, row in df.iterrows():
  filename = row['filename']
  section = row['section']
  exposure = row['exposure']
  uploadflag = row['uploadflag']
  shortname = row['shortname']
  orgURL = row['orgURL']
  
  # Define the path to the zarr file
  zarr_path = os.path.join(upload_path, filename.replace('.czi', '.zarr'))
  zarr_file = os.path.basename(zarr_path)
  print(f"zarr_path ******* {zarr_path}")
  print(f"zarr_file ******* {zarr_file}")

  # Check if the file exists, the upload flag is f
  if uploadflag == '0' and os.path.exists(zarr_path):
    try:
      # total 4 exposures for each section: 0, 1, 2, 3
      # 0 : Green channel 1: Red channel 2: Far Red channel Exp1 3: Far Red channel Exp2
      # Here only consider Far Red channel
      exposure_delete_ranges = {
        # '1': [0, 1, 3],
        # '2': [0, 1, 2]   
        '1': [0, 2],
        '2': [0, 1]   
      }
      # loop through the resolutions,exposure_delete_ranges and delete the folders
      for resolution in resolutions:              
          for exposure_delete in exposure_delete_ranges.get(exposure, []):
            delete_path = os.path.join(upload_path, zarr_file, section, str(resolution), '0', str(exposure_delete))
            print(f"delete_path++++++++++: {delete_path}")
            delete_folder(delete_path)

      # Upload the file to the S3 bucket
      success, error_detail = upload_to_s3(zarr_file, section)

      if not success:
        raise Exception(f"Error uploading file {filename}: {error_detail}")
      else:  
        print(f"Uploaded file {filename} to S3 bucket {s3_bucket}.")
        # Update the upload flag to 1
        df.at[index, 'uploadflag'] = '1'
        df.to_csv(inputlist, index=False)

        shortname, orgURL = create_org_URL(zarr_file, section)
        df.at[index, 'shortname'] = shortname  # Updating shortname in DataFrame
        df.at[index, 'orgURL'] = orgURL  # Updating orgURL in DataFrame

    except Exception as e:
        print(f"Error uploading file {filename}: {e}")
        # Update the upload flag to 'e'
        df.at[index, 'uploadflag'] = 'e'

  # Write the updated DataFrame back to the input CSV file    
  df.to_csv(inputlist, index=False)

print(f"Updated inputlist CSV file written to {inputlist}.")
