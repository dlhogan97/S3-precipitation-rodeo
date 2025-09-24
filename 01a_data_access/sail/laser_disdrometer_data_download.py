import os
# set working directory
os.chdir('/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/')

import xarray as xr
import pandas as pd
from utils.get_sail_data import get_sail_data
import json
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
    "laser_disdrometer_gothic": "gucldM1.b1",
    "laser_disdrometer_mtcb": "gucldS2.b1",
    "ldquats_gothic": "gucldquantsM1.c1",
    "ldquats_mtcb": "gucldquantsS2.c1",
}
print("Set datastream dictionary")
# data time series
date_range = pd.date_range(start='2021-09-01', 
                           end='2023-06-16', 
                           freq='5D') # CHANGE THIS IF DESIRED
print(f"Set date range to {date_range[0]} to {date_range[-1]} with frequency of 5 days")

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
# Iterate through the dictionary and pull the data for each datastream
for i, date in enumerate(date_range):
    if i == len(date_range) - 1:
        break
    for key in sail_datastream_dict.keys():
        # Check if the file already exists
        if (os.path.exists(f"{storage_directory}/{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")):
            print(f"{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc already exists")
            print('-------------------')
            # add the filename to the dictionary which can be used if we want to load the data
            data_loc_dict[sail_datastream_dict[key]] = os.path.join(storage_directory,f"{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
            continue
        else:
            ds = get_sail_data(api_username,
                        api_token,
                        sail_datastream_dict[key],
                        startdate=date.strftime('%Y%m%d'),
                        enddate=(date + pd.Timedelta('5D')).strftime('%Y%m%d'))
            if ds is None:
                print(f"No data for {sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}")
                print('-------------------')
                continue
            else:
                # save the dataset
                ds.to_netcdf(f"{storage_directory}/{key}/{sail_datastream_dict[key]}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
                # print that this file is completed
            
        print(f"File {i+1} of {len(date_range)} completed")
    print(f"Completed {key} data download for date range {date.strftime('%Y%m%d')} to {(date + pd.Timedelta('4D')).strftime('%Y%m%d')}")