import xarray as xr
import pandas as pd
import numpy as np
import glob
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from utils.helper_funcs import convert_to_local_time, get_point_info

if __name__ == "__main__":
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    # check if ERA5-Land files are present
    if not os.path.exists(f"{data_dir}external/ERA5-Land/"):
        print("ERA5-Land data directory not found. Download the data before proceeding.")
    try:
        files = glob.glob(f"{data_dir}external/ERA5-Land/era5_land_20211001_20230930.nc")
    except Exception as e:
        print(f"Error finding ERA5-Land file: {e}")

    era5_land_ds = xr.open_dataset(files[0])
    # get the lon and lat values for gothic and kettle ponds
    try:
        example_gothic_ds = xr.open_dataset(f"{data_dir}processed/SAIL/met_30min.nc")
        gothic_point_info = get_point_info(example_gothic_ds)
        print('Got Gothic locations!')
    except Exception as e:
        print(f"Error opening example Gothic example dataset: {e}. Make sure the file exists.")
    try:
        example_kp_ds = xr.open_dataset(f"{data_dir}processed/SPLASH/asfs30_30min.nc")
        kettle_ponds_point_info = get_point_info(example_kp_ds)
        print('Got Kettle Ponds locations!')
    except Exception as e:
        print(f"Error opening example Kettle Ponds example dataset: {e}. Make sure the file exists.")

    era5_land_point = era5_land_ds.sel(longitude=gothic_point_info['longitude'].values, latitude=gothic_point_info['latitude'].values, method='nearest').squeeze()

    # change to local time
    era5_land_point = convert_to_local_time(era5_land_point, local_tz='America/Denver', time_variable='valid_time')

    # convert from m to mm for precipitation variables
    era5_land_point['tp'] = era5_land_point['tp'] * 1000
    era5_land_point['tp'].attrs['units'] = 'mm'
    # add longname
    era5_land_point['tp'].attrs['long_name'] = 'Total Precipitation'
    # add timezone attribute
    era5_land_point.attrs['timezone'] = 'America/Denver (MST/MDT)'

    # rename valid_time to time
    era5_land_point = era5_land_point.swap_dims({'valid_time': 'time'})

    # remove timezone
    era5_land_point['time'] = pd.to_datetime(era5_land_point['time'].values).tz_localize(None)

    # drop valid_time, number, exper 
    era5_land_point = era5_land_point.drop_vars(['valid_time', 'number', 'expver'])

    # save to netcdf
    os.makedirs(f"{data_dir}/processed/ERA5-Land/", exist_ok=True)
    output_path = f"{data_dir}/processed/ERA5-Land/era5_land_gothic_1hr.nc"
    era5_land_point.to_netcdf(output_path)
    print(f"Saved processed ERA5-Land data to {output_path}")