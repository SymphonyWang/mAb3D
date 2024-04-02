import os
import subprocess
import csv

path = "/home/xiw4013/mAb3D/input"
inputlist = "/home/xiw4013/mAb3D/inputlist.csv"
output_dir = "/home/xiw4013/mAb3D/zarr_upload/"

# def Convert czi to ome-zarr using bioformats2raw
def convert_to_ome_zarr(input_path, output_path):    

    # Run the bioformats2raw command
    # command = ['bioformats2raw', input_path, output_path, '--compression', 'zlib', '--compression-properties', 'level=9', '--dimension-order', 'XYZCT', '--resolutions', '3']
    # command = ['bioformats2raw', input_path, output_path, '--compression', 'zlib', '--compression-properties', 'level=9', '--dimension-order', 'XYCZT', '--resolutions', '3']
    # command = ['bioformats2raw', input_path, output_path, '--compression', 'zlib', '--compression-properties', 'level=9', '--dimension-order', 'XYTZC', '--resolutions', '3']
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


# Open the CSV file in write mode
with open(inputlist, 'w', newline='') as csvfile:
    # Define the fieldnames
    fieldnames = ['filename', 'section', 'exposure', 'transferflag', 'uploadflag']

    # Create a CSV DictWriter
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header to the CSV file
    writer.writeheader()

    # Use os.listdir to get all files in the directory
    for filename in os.listdir(path):
        # Use os.path.isfile to check if it's a file (not a directory)
        if os.path.isfile(os.path.join(path, filename)):
            # Check if the file has a .czi extension
            if filename.endswith('.czi'):
                # Extract the base filename without the path
                base_filename = os.path.basename(filename)

                # Remove the .czi extension from the base filename
                base_name = os.path.splitext(base_filename)[0]

                # Construct the input path
                input_path = os.path.join(path, filename)
                # Construct the output path
                output_path = os.path.join(output_dir, f"{base_name}.zarr")

                # Check if the output directory already exists
                if os.path.exists(output_path):
                    writer.writerow({'filename': filename, 'transferflag': '2', 'uploadflag': '0'})
                    print(f"Output directory {output_path} already exists. Skipping file {filename}.")
                    continue

                # Convert the file to OME-Zarr format
                success, error_detail = convert_to_ome_zarr(input_path, output_path)

                if not success:
                  writer.writerow({'filename': filename, 'transferflag': 'e', 'uploadflag': '0'})
                  print(f"Error converting file {filename}. Error details: {error_detail}. Skipping to next file.")
                else:
                    writer.writerow({'filename': filename, 'transferflag': '1', 'uploadflag': '0'})
                    print(f"Successfully converted file {filename} and wrote to {inputlist}.")


print(f"Conversion complete. CSV file written to {inputlist}.")