import pandas as pd
from urllib.parse import quote

inputlist = "/home/xiw4013/mAb3D/Ab3D-E-Screening - Sheet4.csv"
outputlist = "/home/xiw4013/mAb3D/Ab3D-E-Screening - Sheet4_ch.csv"

# Read the inputlist.csv using pandas
df = pd.read_csv(inputlist, dtype={'filename': str, 'orgURL_V1': str, 'orgURL_V2': str, 'markername': str})
print(f"df {df}")

# Create a new DataFrame with the required columns
new_df = pd.DataFrame(columns=['filename', 'orgURL_V1', 'orgURL_V2', 'markername'])

# Traverse filename in upload path
for index, row in df.iterrows():
  filename = row['filename']
  markername = (row['markername'])
  orgURL_V1 = row['orgURL_V1']
  orgURL_V2 = row['orgURL_V2']

  # replace the space with %20 in orgURL_V1 and save it to orgURL_V2
  orgURL_V2 = orgURL_V1.replace("%22green%22", "%22green%20%28nuclear%20dye%29%22").replace("%22red%22", "%22red%20%28autofluorescence%29%22").replace("%22far%20red%20long%20exp%22", "%22far%20red%20long%20exp%20%28{markername1}%29%22").replace("%22far%20red%20short%20exp%22", "%22far%20red%20short%20exp%20%28{markername1}%29%22").format(markername1=markername)
  print(f"orgURL_V2 {orgURL_V2}")

  # save csv file
  new_row = {
    'filename': filename,
    'orgURL_V1': orgURL_V1,
    'orgURL_V2': orgURL_V2,
    'markername': markername,
  }
  new_df = new_df._append(new_row, ignore_index=True)
  print(f"layername {filename} was saved to new_df")
  new_df.to_csv(outputlist, index=False)
  
  

