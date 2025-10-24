import xarray as xr
import pandas as pd
import numpy as np
import glob
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from utils.helper_funcs import convert_to_local_time, get_point_info
import time

def process_squire_data(files, resample_interval='30min', gothic_point_info=None, kettle_ponds_point_info=None):
    ds_list_gothic = []
    ds_list_kettle_ponds = []
    for i, file in enumerate(files):
        print(f"Processing file {i}/{len(files)}: {file}")
        start = time.time()
        ds = xr.open_dataset(file)

        # convert the ds to use lat and lon as the coordinates, replacing x and y
        ds['x'] = ds['x'].assign_coords({'x': ds['lon'].values})
        ds['y'] = ds['y'].assign_coords({'y': ds['lat'].values})
        ds = ds.swap_dims({'x': 'lon', 'y': 'lat'})

        # exclude dBZ and corrected_reflectivity variables
        ds = ds.drop_vars(['DBZ', 'corrected_reflectivity'])
        # convert to local time
        ds = convert_to_local_time(ds, local_tz='America/Denver')

        # grab the nearest point to gothic
        ds_gothic = ds.sel(lon=gothic_point_info['longitude'].values, lat=gothic_point_info['latitude'].values, method='nearest')
        ds_kettle_ponds = ds.sel(lon=kettle_ponds_point_info['longitude'].values, lat=kettle_ponds_point_info['latitude'].values, method='nearest')

        # resample to resample interval
        ds_gothic_resampled = ds_gothic.resample(time=resample_interval).mean().squeeze()
        ds_kettle_ponds_resampled = ds_kettle_ponds.resample(time=resample_interval).mean().squeeze()

        # caluclate resample interval total by converting from mm/hour to mm/30min
        ds_gothic_resampled_total = ds_gothic_resampled * 0.5
        ds_kettle_ponds_resampled_total = ds_kettle_ponds_resampled * 0.5

        # rename these variables to indicate total
        for var in ds_gothic_resampled_total.data_vars:
            ds_gothic_resampled_total = ds_gothic_resampled_total.rename({var: var + '_total'})
        for var in ds_kettle_ponds_resampled_total.data_vars:
            ds_kettle_ponds_resampled_total = ds_kettle_ponds_resampled_total.rename({var: var + '_total'})

        # add attributes from original dataset
        for var in ds.data_vars:
            ds_gothic_resampled[var].attrs = ds[var].attrs
            ds_kettle_ponds_resampled[var].attrs = ds[var].attrs

        # add the total to the original dataset
        ds_gothic_resampled = xr.concat([ds_gothic_resampled, ds_gothic_resampled_total], dim='time')
        ds_kettle_ponds_resampled = xr.concat([ds_kettle_ponds_resampled, ds_kettle_ponds_resampled_total], dim='time')
        # add unit and longname attributes for total variables
        for var in ds_gothic_resampled_total.data_vars:
            ds_gothic_resampled_total[var].attrs['units'] = 'mm'
            ds_kettle_ponds_resampled_total[var].attrs['units'] = 'mm'
            ds_gothic_resampled_total[var].attrs['long_name'] = ds_gothic_resampled[var.replace('_total', '')].attrs.get('long_name', '') + ' Total'
            ds_kettle_ponds_resampled_total[var].attrs['long_name'] = ds_kettle_ponds_resampled[var.replace('_total', '')].attrs.get('long_name', '') + ' Total'


        # add lat, lon, elev attributes
        ds_gothic_resampled_total = ds_gothic_resampled_total.assign_attrs({
            'latitude': gothic_point_info['latitude'].values,
            'longitude': gothic_point_info['longitude'].values,
            'elevation': gothic_point_info['elevation'].values,
            'time_zone': 'America/Denver',  
        })
        ds_kettle_ponds_resampled_total = ds_kettle_ponds_resampled_total.assign_attrs({
            'latitude': kettle_ponds_point_info['latitude'].values,
            'longitude': kettle_ponds_point_info['longitude'].values,
            'elevation': kettle_ponds_point_info['elevation'].values,
            'time_zone': 'America/Denver',
        })

        # remove timezone info from time coordinate
        ds_gothic_resampled_total['time'] = pd.to_datetime(ds_gothic_resampled_total['time'].values).tz_localize(None)
        ds_kettle_ponds_resampled_total['time'] = pd.to_datetime(ds_kettle_ponds_resampled_total['time'].values).tz_localize(None)

        ds_list_gothic.append(ds_gothic_resampled_total)
        ds_list_kettle_ponds.append(ds_kettle_ponds_resampled_total)

        end = time.time()
        print(f"Finished processing file {file} in {end - start:.2f} seconds.")
    combined_ds_gothic = xr.concat(ds_list_gothic, dim='time')
    combined_ds_gothic = combined_ds_gothic.sortby('time')
    combined_ds_kettle_ponds = xr.concat(ds_list_kettle_ponds, dim='time')
    combined_ds_kettle_ponds = combined_ds_kettle_ponds.sortby('time')
    return combined_ds_gothic, combined_ds_kettle_ponds

if __name__ == "__main__":

    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    # check if SQUIRE files are present
    if not os.path.exists(f"{data_dir}raw/SAIL/squire_radar/"):
        print("SQUIRE data directory not found. Download the data before proceeding.")
    try:
        files = glob.glob(f"{data_dir}raw/SAIL/squire_radar/gucxprecipradarsquireS2.c1*.nc")
    except Exception as e:
        print(f"Error finding SQUIRE files: {e}")

    RESAMPLE_INTERVAL = '30min'
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

    combined_ds_gothic, combined_ds_kettle_ponds = process_squire_data(
        files, resample_interval=RESAMPLE_INTERVAL,
        gothic_point_info=gothic_point_info,
        kettle_ponds_point_info=kettle_ponds_point_info
    )

    combined_ds_gothic.to_netcdf(f"{data_dir}processed/SAIL/squire_gothic_{RESAMPLE_INTERVAL}.nc")
    combined_ds_kettle_ponds.to_netcdf(f"{data_dir}processed/SAIL/squire_kettle_ponds_{RESAMPLE_INTERVAL}.nc")