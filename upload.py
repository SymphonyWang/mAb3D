import os
import csv
import subprocess

# Define the server name and upload path
upload_path = '/home/xiw4013/mAb3D/zarr_upload/'
s3_bucket = 'mAb3D'
inputlist = "/home/xiw4013/mAb3D/inputlist.csv"

# Function to upload zarr file to AWS S3 bucket
def upload_to_s3(zarr_file, zarr_path):
    aws_command = ['aws', '--endpoint-url', 'https://redcloud.cac.cornell.edu:8443/', '--no-verify', 's3', '--profile', 'CAC', 'cp',  '--recursive', zarr_path, 's3://mAb3D/test/' + zarr_file]
    result = subprocess.run(aws_command, stderr=subprocess.PIPE)

     # Check if the command was successful
    if result.returncode == 0:
        return True, "" # Return True for success and None for error detail
    else:
    # If there was an error, return False and the error details
        error_detail = result.stderr.decode('utf-8') if result.stderr else "Unknown error"
        return False, error_detail

# Open the CSV file
with open(inputlist, 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Open the output CSV file
with open('ZarrURL.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['filename', 'URL'])
    writer.writeheader()

    for row in rows:
        # Check if the file exists, the upload flag is 0, and the transfer flag is 1
        zarr_path = os.path.join(upload_path, row['filename'].replace('.czi', '.zarr'))
        print(f"zarr_path ******* {zarr_path}")
        zarr_file = os.path.basename(zarr_path)
        print(f"zarr_file ******* {zarr_file}")

        if os.path.exists(zarr_path) and row['uploadflag'] == '0' and row['transferflag'] in ['1']:
            try:
                # Upload the file to the S3 bucket
                success, error_detail = upload_to_s3(zarr_file, zarr_path)
                
                if not success:
                    raise Exception(f"Error uploading file {row['filename']}: {error_detail}")
                else:  
                  print(f"Uploaded file {row['filename']} to S3 bucket {s3_bucket}.")
                  # Write the filename and URL to the output CSV file
                  writer.writerow({'filename': row['filename'].replace('.czi', '.zarr'), 'URL': f"<a href='https://redcloud.cac.cornell.edu:8443/{s3_bucket}/test/{row['filename'].replace('.czi', '.zarr')}' target='_blank'>Link</a>"})
                  # Update the upload flag to 1
                  row['uploadflag'] = '1'
            except Exception as e:
                print(f"Error uploading file {row['filename']}: {e}")
                # Update the upload flag to 'e'
                row['uploadflag'] = 'e'

# Write the updated rows back to the input CSV file
with open(inputlist, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)