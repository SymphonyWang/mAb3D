import os
import subprocess
import pandas as pd
from urllib.parse import quote

# Define the server name and upload path
upload_path = '/home/xiw4013/mAb3D/zarr_upload/'
inputlist = "/home/xiw4013/mAb3D/inputlistV4.csv"
s3_bucket = 'mAb3D'
awsbucket_path = 's3://mAb3D/'
zarr_upload_list = "/home/xiw4013/mAb3D/Zarr_upload_list.csv"

# Function to upload zarr file to AWS S3 bucket
def upload_to_s3(zarr_file, section_index):
    zarrpath = upload_path + zarr_file + '/' + str(section_index) + '/'
    awspath = awsbucket_path + zarr_file + '/' + str(section_index) + '/'

    aws_command = ['aws', '--endpoint-url', 'https://redcloud.cac.cornell.edu:8443/', '--no-verify', 's3', '--profile', 'CAC', 'cp',  '--recursive', zarrpath, awspath]
  
    result = subprocess.run(aws_command, stderr=subprocess.PIPE)

     # Check if the command was successful
    if result.returncode == 0:
        return True, "" # Return True for success and None for error detail
    else:
    # If there was an error, return False and the error details
        error_detail = result.stderr.decode('utf-8') if result.stderr else "Unknown error"
        return False, error_detail

def create_org_URL (zarr_file, section, secname, markername):
  try: 
      print(f"11111111111111111")    
      # encode zarr file URL
      zarr_file_encode = quote(zarr_file)
      print(f"zarr_file_encode {zarr_file_encode}")
      orgURL = "https://neuroglancer-demo.appspot.com/#!%7B%22dimensions%22:%7B%22x%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22y%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22z%22:%5B0.0000013759848761314483%2C%22m%22%5D%2C%22t%22:%5B1%2C%22%22%5D%7D%2C%22position%22:%5B6594.76806640625%2C3351.84765625%2C0.5%2C0%5D%2C%22crossSectionScale%22:9.227814895369777%2C%22projectionOrientation%22:%5B-0.3254466950893402%2C0.8991797566413879%2C0.2924036383628845%2C0.007767873350530863%5D%2C%22projectionScale%22:12314.085503119006%2C%22layers%22:%5B%7B%22type%22:%22image%22%2C%22source%22:%22zarr2://https://redcloud.cac.cornell.edu:8443/swift/v1/mAb3D/{zarr_file_encode1}/{section1}/%22%2C%22localDimensions%22:%7B%22c%27%22:%5B1%2C%22%22%5D%7D%2C%22localPosition%22:%5B2%5D%2C%22tab%22:%22rendering%22%2C%22shader%22:%22#define%20VOLUME_RENDERING%20false%5Cn%5Cn#uicontrol%20invlerp%20value%28range=%5B0%2C10000%5D%29%5Cn%5Cn#define%20WHITE%20%5C%22#FFFFFF%5C%22%5Cn#define%20RED%20%5C%22#FF0000%5C%22%5Cn#define%20GREEN%20%5C%22#00FF00%5C%22%5Cn#define%20BLUE%20%5C%22#0000FF%5C%22%5Cn#define%20YELLOW%20%5C%22#FFFF00%5C%22%5Cn#define%20MAGENTA%20%5C%22#FF00FF%5C%22%5Cn#define%20CYAN%20%5C%22#00FFFF%5C%22%5Cn%5Cn#uicontrol%20vec3%20display_color%20color%28default=%5C%22#FFFFFF%5C%22%29%3B%5Cn%5Cn%5Cnvoid%20main%28%29%20%7B%5Cn%20%20float%20f%20=%20value%28%29%3B%5Cn%20%20//emitGrayscale%28f%29%3B%5Cn%20%20//%2A%5Cn%20%20int%20as_alpha%20=%200%3B%5Cn%20%20if%20%28as_alpha==1%29%20%7B%5Cn%20%20%20%20%20%20emitRGBA%28vec4%28display_color%2C%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20f%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29%29%3B%5Cn%20%20%7D%20else%20if%20%28as_alpha==0%29%20%7B%5Cn%20%20%20%20%20%20emitRGB%28f%2Adisplay_color%29%3B%5Cn%20%20%7D%20%20//%2A/%5Cn%7D%5Cn%22%2C%22shaderControls%22:%7B%22value%22:%7B%22range%22:%5B0%2C1900%5D%7D%7D%2C%22crossSectionRenderScale%22:0.08202212110788706%2C%22name%22:%22Far%20red%20optimal%20%28{markername1}%29%22%7D%2C%7B%22type%22:%22image%22%2C%22source%22:%22zarr2://https://redcloud.cac.cornell.edu:8443/swift/v1/mAb3D/{zarr_file_encode1}/{section1}/%22%2C%22localDimensions%22:%7B%22c%27%22:%5B1%2C%22%22%5D%7D%2C%22localPosition%22:%5B0%5D%2C%22tab%22:%22source%22%2C%22shader%22:%22#define%20VOLUME_RENDERING%20false%5Cn%5Cn#uicontrol%20invlerp%20value%28range=%5B0%2C10000%5D%29%5Cn%5Cn#define%20WHITE%20%5C%22#FFFFFF%5C%22%5Cn#define%20RED%20%5C%22#FF0000%5C%22%5Cn#define%20GREEN%20%5C%22#00FF00%5C%22%5Cn#define%20BLUE%20%5C%22#0000FF%5C%22%5Cn#define%20YELLOW%20%5C%22#FFFF00%5C%22%5Cn#define%20MAGENTA%20%5C%22#FF00FF%5C%22%5Cn#define%20CYAN%20%5C%22#00FFFF%5C%22%5Cn%5Cn#uicontrol%20vec3%20display_color%20color%28default=%5C%22#FFFFFF%5C%22%29%3B%5Cn%5Cn%5Cnvoid%20main%28%29%20%7B%5Cn%20%20float%20f%20=%20value%28%29%3B%5Cn%20%20//emitGrayscale%28f%29%3B%5Cn%20%20//%2A%5Cn%20%20int%20as_alpha%20=%200%3B%5Cn%20%20if%20%28as_alpha==1%29%20%7B%5Cn%20%20%20%20%20%20emitRGBA%28vec4%28display_color%2C%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20f%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29%29%3B%5Cn%20%20%7D%20else%20if%20%28as_alpha==0%29%20%7B%5Cn%20%20%20%20%20%20emitRGB%28f%2Adisplay_color%29%3B%5Cn%20%20%7D%20%20//%2A/%5Cn%7D%5Cn%22%2C%22shaderControls%22:%7B%22value%22:%7B%22range%22:%5B0%2C3800%5D%7D%7D%2C%22crossSectionRenderScale%22:0.10404605121429011%2C%22name%22:%22Green%20%28nuclear%29%22%2C%22visible%22:false%7D%2C%7B%22type%22:%22image%22%2C%22source%22:%22zarr2://https://redcloud.cac.cornell.edu:8443/swift/v1/mAb3D/{zarr_file_encode1}/{section1}/%22%2C%22localDimensions%22:%7B%22c%27%22:%5B1%2C%22%22%5D%7D%2C%22localPosition%22:%5B1%5D%2C%22tab%22:%22rendering%22%2C%22shader%22:%22#define%20VOLUME_RENDERING%20false%5Cn%5Cn#uicontrol%20invlerp%20value%28range=%5B0%2C10000%5D%29%5Cn%5Cn#define%20WHITE%20%5C%22#FFFFFF%5C%22%5Cn#define%20RED%20%5C%22#FF0000%5C%22%5Cn#define%20GREEN%20%5C%22#00FF00%5C%22%5Cn#define%20BLUE%20%5C%22#0000FF%5C%22%5Cn#define%20YELLOW%20%5C%22#FFFF00%5C%22%5Cn#define%20MAGENTA%20%5C%22#FF00FF%5C%22%5Cn#define%20CYAN%20%5C%22#00FFFF%5C%22%5Cn%5Cn#uicontrol%20vec3%20display_color%20color%28default=%5C%22#FF0000%5C%22%29%3B%5Cn%5Cn%5Cnvoid%20main%28%29%20%7B%5Cn%20%20float%20f%20=%20value%28%29%3B%5Cn%20%20//emitGrayscale%28f%29%3B%5Cn%20%20//%2A%5Cn%20%20int%20as_alpha%20=%200%3B%5Cn%20%20if%20%28as_alpha==1%29%20%7B%5Cn%20%20%20%20%20%20emitRGBA%28vec4%28display_color%2C%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20f%5Cn%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%29%29%3B%5Cn%20%20%7D%20else%20if%20%28as_alpha==0%29%20%7B%5Cn%20%20%20%20%20%20emitRGB%28f%2Adisplay_color%29%3B%5Cn%20%20%7D%20%20//%2A/%5Cn%7D%22%2C%22shaderControls%22:%7B%22value%22:%7B%22range%22:%5B0%2C6500%5D%7D%7D%2C%22name%22:%22Red%20%28Vessel%29%22%2C%22visible%22:false%7D%5D%2C%22selectedLayer%22:%7B%22size%22:637%2C%22visible%22:true%2C%22layer%22:%22Far%20red%20optimal%20%28{markername1}%29%22%7D%2C%22layout%22:%22xy%22%7D".format(zarr_file_encode1=zarr_file_encode, section1=section, markername1=markername)

      shortname = zarr_file.replace('.zarr', '') + '-' + secname
      print(f"shortname {shortname}")
      print(f"creating org URL for {shortname} have been successfully completed.")
      return shortname, orgURL
  except Exception as e:
      print(f"Error creating org URL: {e}")
      return None, None

# Read the inputlist.csv using pandas
df = pd.read_csv(inputlist, dtype={'filename': str, 'secnum': str, 'secname': str, 'transferflag': str, 'uploadflag': str, 'orgURL': str, 'shortname': str, 'markername': str})

# Traverse filename in upload path
for index, row in df.iterrows():
  filename = row['filename']
  section = (row['secnum'])
  secname = row['secname']
  transferflag = row['transferflag']
  uploadflag = row['uploadflag']
  shortname = row['shortname']
  orgURL = row['orgURL']
  markername = row['markername']
  
  # Define the path to the zarr file
  if isinstance(filename, str):
      zarr_path = os.path.join(upload_path, filename.replace('.czi', '.zarr'))
      zarr_file = os.path.basename(zarr_path)

      # Check if the file exists, the upload flag is f
      if uploadflag == '0' and transferflag == '1' and section != '-' and os.path.exists(zarr_path):
        try:
          # Upload the file to the S3 bucket
          success, error_detail = upload_to_s3(zarr_file, section)

          if not success:
            raise Exception(f"Error uploading file {filename} section {section} : {error_detail}")
          else:  
            print(f"Uploaded file {filename} section {section} to S3 bucket {s3_bucket}.")
            # Update the upload flag to 1
            df.at[index, 'uploadflag'] = '1'
            df.to_csv(inputlist, index=False)

            shortname, orgURL = create_org_URL(zarr_file, section, secname, markername)
            df.at[index, 'shortname'] = shortname  # Updating shortname in DataFrame
            df.at[index, 'orgURL'] = orgURL  # Updating orgURL in DataFrame
            df.to_csv(inputlist, index=False)
        except Exception as e:
            print(f"Error uploading file {filename}: {e}")
            # Update the upload flag to 'e'
            df.at[index, 'uploadflag'] = 'e'

      # Write the updated DataFrame back to the input CSV file    
      df.to_csv(inputlist, index=False)

print(f"Updated inputlist CSV file written to {inputlist}.")
