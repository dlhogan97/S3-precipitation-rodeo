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
credential_path = '/home/dlhogan/.act_config.json'
credentials = load_arm_credentials(credential_path)
# api token and username for ARM
api_username = credentials.get('username')
api_token = credentials.get('token')

# Datastream to download
sail_datastream_dict = {
    "squire_radar": "gucxprecipradarsquireS2.c1",
}

# winter time data timeseries
winter_22 = ('20211201','20220331')
winter_23 = ('20221201','20230331')

# chop up data into 5-day chunks
winter_22_range = pd.date_range(start=winter_22[0], end=winter_22[1], freq='5D')
winter_23_range = pd.date_range(start=winter_23[0], end=winter_23[1], freq='5D')

# Set the location of the data folder where this data will be stored
winter_22_folder = 'winter_21_22'
winter_23_folder = 'winter_22_23'

# change to location of data folder on your machine
storage_directory = f'/storage/dlhogan/synoptic_sublimation/'

# download the data 
# load in the winter 22 data
sail_winter_22_folder = os.path.join(storage_directory,'sail_data',winter_22_folder)
# create empty data dictionary
w22_data_loc_dict = {}
# Iterate through the dictionary and pull the data for each datastream
for i, date in enumerate(winter_22_range):
    if i == len(winter_22_range) - 1:
        break
    # Check if the file already exists
    if (os.path.exists(f"{sail_winter_22_folder}/{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")): 
        print(f"{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc already exists")
        print('-------------------')
        # add the filename to the dictionary which can be used if we want to load the data
        w22_data_loc_dict[sail_datastream_dict["squire_radar"]] = os.path.join(sail_winter_22_folder,f"{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
        continue
    else:
        ds = get_sail_data(api_username,
                    api_token,
                    sail_datastream_dict["squire_radar"],
                    startdate=date.strftime('%Y%m%d'),
                    enddate=(date + pd.Timedelta('4D')).strftime('%Y%m%d'))
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
            ds.to_netcdf(f"{sail_winter_22_folder}/{sail_datastream_dict['squire_radar']}_{date.strftime('%Y%m%d')}_{(date + pd.Timedelta('4D')).strftime('%Y%m%d')}.nc")
            # print that this file is completed
    print(f"File {i+1} of {len(winter_22_range)} completed")