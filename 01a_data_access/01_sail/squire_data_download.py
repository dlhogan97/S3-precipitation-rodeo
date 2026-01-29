import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
import xarray as xr
import pandas as pd
# from utils.get_sail_data import get_sail_data
import json
import act

# Function to load ARM credentials
def load_arm_credentials(credential_path):
    with open(credential_path, 'r') as f:
        credentials = json.load(f)
    return credentials
# Location of ARM credentials
credential_path = '/home/dlhogan/tokens/.act_config.json' # CHANGE THIS TO YOUR LOCATION
credentials = load_arm_credentials(credential_path)
# api token and username for ARM
api_username = credentials.get('username')
api_token = credentials.get('token')
print("Loaded ARM credentials")

# Datastream to download
sail_datastream_dict = {
    "updated_squire": "gucxprecipradarsquireS2.c1",
}

# data time series
date_range = pd.date_range(start='2021-09-01', 
                           end='2023-06-16', 
                           freq='1D') # CHANGE THIS IF DESIRED
print(f"Set date range to {date_range[0]} to {date_range[-1]} with frequency of 1 day")

# change to location of data folder on your machine
storage_directory = f'/storage/dlhogan/precipitation-rodeo/data/raw/SAIL/' # CHANGE THIS TO YOUR DIRECTORY
print(f"Set storage directory to {storage_directory}")

# make the directory if it doesn't exist
for key in sail_datastream_dict.keys():
    if not os.path.exists(os.path.join(storage_directory, key)):
        os.makedirs(os.path.join(storage_directory, key))
        print(f"Created directory {os.path.join(storage_directory, key)}")
    else:
        print(f"Directory {os.path.join(storage_directory, key)} already exists. Skipping creation.")

# download the data 
# create empty data dictionary
data_loc_dict = {}
dates_to_download = []
# Check if the data already exists before downloading
for i, date in enumerate(date_range):
    if i == len(date_range) - 1:
        break
    for key in sail_datastream_dict.keys():
        # Check if the file already exists
        if (os.path.exists(f"{storage_directory}/{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}.000000.nc")):
            print(f"{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}.000000.nc already exists")
            print('-------------------')
            # add the filename to the dictionary which can be used if we want to load the data
            data_loc_dict[sail_datastream_dict[key]] = os.path.join(storage_directory,
                                                                    f"{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}.000000.nc")
            break
        else:
            if date.month in [1,2,3,12]:
                print(f"File {sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}.000000.nc not found. Downloading data...")
                dates_to_download.append(date)
# prompt user before downloading large amounts of data
if len(dates_to_download) > 0:
    print(f"About to download {len(dates_to_download)} days of data. This may take a while.")
    user_input = input("Do you want to proceed? (y/n): ")
    if user_input.lower() != 'y':
        print("Download cancelled by user.")
    else:
        for key in sail_datastream_dict.keys():
            act.discovery.download_arm_data(
                                            api_username, 
                                            api_token, 
                                            sail_datastream_dict[key],
                                            date_range[0].strftime('%Y%m%d'), date_range[-1].strftime('%Y%m%d'),
                                            output=os.path.join(storage_directory, key))