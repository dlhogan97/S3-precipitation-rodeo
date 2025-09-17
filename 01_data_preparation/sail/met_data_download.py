import xarray as xr
import pandas as pd
from utils.get_sail_data import get_sail_data
import json
import os

# Function to load ARM credentials
def load_arm_credentials(credential_path):
    with open(credential_path, 'r') as f:
        credentials = json.load(f)
    return credentials
# Location of ARM credentials
credential_path = '/home/dlhogan/.act_config.json'
credentials = load_arm_credentials(credential_path)
# api token and username for ARM
api_username = credentials.get('username')
api_token = credentials.get('token')

# Datastream to download
sail_datastream_dict = {
    "met": "gucmetM1.b1",
}

# data time series
date_range = pd.date_range(start='2021-09-01', end='2023-06-16', freq='5D')

# change to location of data folder on your machine
storage_directory = f'/storage/dlhogan/precipitation-rodeo/data/raw/SAIL/'

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
    # Check if the file already exists
    if (os.path.exists(f"{storage_directory}/{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")): 
        print(f"{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc already exists")
        print('-------------------')
        # add the filename to the dictionary which can be used if we want to load the data
        data_loc_dict[sail_datastream_dict["squire_radar"]] = os.path.join(storage_directory,f"{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
        continue
    else:
        ds = get_sail_data(api_username,
                    api_token,
                    sail_datastream_dict["squire_radar"],
                    startdate=date.strftime('%Y%m%d'),
                    enddate=(date + pd.Timedelta('5D')).strftime('%Y%m%d'))
        if ds is None:
            print(f"No data for {sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}")
            print('-------------------')
            continue
        else:
            # resample to 1H mean
            ds = ds.resample(time='1H').mean()
            # drop lowest_height variable
            ds = ds.drop_vars('lowest_height')
            # save the dataset
            ds.to_netcdf(f"{storage_directory}/{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
            # print that this file is completed
    print(f"File {i+1} of {len(date_range)} completed")