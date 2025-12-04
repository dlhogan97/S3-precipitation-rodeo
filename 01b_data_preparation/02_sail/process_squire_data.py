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

def process_squire_data(files, resample_interval='30min', gothic_point_info=None, kettle_ponds_point_info=None, reasonable_threshold=None):
    ds_list_gothic = []
    ds_list_kettle_ponds = []
    for i, file in enumerate(files):
        print(f"Processing file {i+1}/{len(files)}: {file}")
        start = time.time()
        ds = xr.open_dataset(file)

        # convert the ds to use lat and lon as the coordinates, replacing x and y
        ds['x'] = ds['x'].assign_coords({'x': ds['lon'].values})
        ds['y'] = ds['y'].assign_coords({'y': ds['lat'].values})
        ds = ds.swap_dims({'x': 'lon', 'y': 'lat'})

        # exclude dBZ and corrected_reflectivity variables
        ds = ds.drop_vars(['DBZ', 'corrected_reflectivity', 'lowest_height'])
        # convert to local time
        ds = convert_to_local_time(ds, local_tz='America/Denver')

        # add a qc_missing and qc_bad flag for first squire variable
        first_var = list(ds.data_vars)[0]
        qc_missing = ds[first_var].isnull()
        qc_missing.name = 'squire_missing_flag'
        qc_missing.attrs['description'] = 'Quality flag for SQUIRE data: True = missing data, False = data present'
        qc_bad = (ds[first_var] < reasonable_threshold).isnull()
        qc_bad.name = 'squire_bad_flag'     
        qc_bad.attrs['description'] = 'Quality flag for SQUIRE data: True = bad data, False = good data'
        ds = xr.merge([ds, qc_missing, qc_bad], join='left')

        # grab the nearest point to gothic
        ds_gothic = ds.sel(lon=gothic_point_info['longitude'].values
                            , lat=gothic_point_info['latitude'].values, method='nearest').squeeze()
        ds_gothic = ds_gothic.expand_dims(site=['gothic'])
        ds_kettle_ponds = ds.sel(lon=kettle_ponds_point_info['longitude'].values
                            , lat=kettle_ponds_point_info['latitude'].values, method='nearest').squeeze()
        ds_kettle_ponds = ds_kettle_ponds.expand_dims(site=['kettle_ponds'])

        # apply reasonableness threshold
        if reasonable_threshold is not None:
            for var in ds_gothic.data_vars:
                ds_gothic[var] = ds_gothic[var].where(ds_gothic[var] <= reasonable_threshold, np.nan)
                # apply lower bound of 0.05 mm
                ds_gothic[var] = ds_gothic[var].where(ds_gothic[var] >= 0.05, 0)
            for var in ds_kettle_ponds.data_vars:
                ds_kettle_ponds[var] = ds_kettle_ponds[var].where(ds_kettle_ponds[var] <= reasonable_threshold, np.nan)
                ds_kettle_ponds[var] = ds_kettle_ponds[var].where(ds_kettle_ponds[var] >= 0.05, 0)

        # resample to resample interval
        ds_gothic_resampled = ds_gothic.resample(time=resample_interval).mean().squeeze()
        ds_kettle_ponds_resampled = ds_kettle_ponds.resample(time=resample_interval).mean().squeeze()

        # caluclate resample interval total by converting from mm/hour to mm/30min
        ds_gothic_resampled_total = ds_gothic_resampled * 0.5
        ds_kettle_ponds_resampled_total = ds_kettle_ponds_resampled * 0.5

        # rename these variables to indicate total
        for var in ds_gothic_resampled.data_vars:
            ds_gothic_resampled_total = ds_gothic_resampled_total.rename({var: var + '_total'})
        for var in ds_kettle_ponds_resampled.data_vars:
            ds_kettle_ponds_resampled_total = ds_kettle_ponds_resampled_total.rename({var: var + '_total'})

        # add unit and longname attributes for total variables
        for var in ds_gothic_resampled_total.data_vars:
            ds_gothic_resampled_total[var].attrs['units'] = 'mm'
            ds_kettle_ponds_resampled_total[var].attrs['units'] = 'mm'
            ds_gothic_resampled_total[var].attrs['long_name'] = ds_gothic_resampled[var.replace('_total', '')].attrs.get('long_name', '') + ' Total'
            ds_kettle_ponds_resampled_total[var].attrs['long_name'] = ds_kettle_ponds_resampled[var.replace('_total', '')].attrs.get('long_name', '') + ' Total'
            
        # add the total to the original dataset
        ds_gothic_resampled = ds_gothic_resampled.merge(ds_gothic_resampled_total, join='left')
        ds_kettle_ponds_resampled = ds_kettle_ponds_resampled.merge(ds_kettle_ponds_resampled_total, join='left')

        # add attributes from original dataset
        for var in ds.data_vars:
            ds_gothic_resampled[var].attrs = ds[var].attrs
            ds_kettle_ponds_resampled[var].attrs = ds[var].attrs

        # add lat, lon, elev attributes
        ds_gothic_resampled = ds_gothic_resampled.assign_attrs({
            'latitude': gothic_point_info['latitude'].astype('float32').values,
            'longitude': gothic_point_info['longitude'].astype('float32').values,
            'elevation': gothic_point_info['elevation'].astype('float32').values,
            'time_zone': 'America/Denver',  
        })
        ds_kettle_ponds_resampled = ds_kettle_ponds_resampled.assign_attrs({
            'latitude': kettle_ponds_point_info['latitude'].astype('float32').values,
            'longitude': kettle_ponds_point_info['longitude'].astype('float32').values,
            'elevation': kettle_ponds_point_info['elevation'].astype('float32').values,
            'time_zone': 'America/Denver',
        })

        # remove timezone info from time coordinate
        ds_gothic_resampled['time'] = pd.to_datetime(ds_gothic_resampled['time'].values).tz_localize(None)
        ds_kettle_ponds_resampled['time'] = pd.to_datetime(ds_kettle_ponds_resampled['time'].values).tz_localize(None)

        ds_list_gothic.append(ds_gothic_resampled)
        ds_list_kettle_ponds.append(ds_kettle_ponds_resampled)

        end = time.time()
        print(f"Finished processing file {file} in {end - start:.2f} seconds.")
    combined_ds_gothic = xr.concat(ds_list_gothic, dim='time')
    combined_ds_gothic = combined_ds_gothic.sortby('time')
    combined_ds_kettle_ponds = xr.concat(ds_list_kettle_ponds, dim='time')
    combined_ds_kettle_ponds = combined_ds_kettle_ponds.sortby('time')

    # concatenate along site dimension
    combined_ds = xr.concat([combined_ds_gothic, combined_ds_kettle_ponds], dim='site')
    return combined_ds

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
    REASONABLE_THRESHOLD = 0.522 * 25.4  # reasonable threshold for precipitation
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

    combined_ds = process_squire_data(
        files, resample_interval=RESAMPLE_INTERVAL,
        gothic_point_info=gothic_point_info,
        kettle_ponds_point_info=kettle_ponds_point_info,
        reasonable_threshold=REASONABLE_THRESHOLD
    )
    try:
        combined_ds.to_netcdf(f"{data_dir}processed/SAIL/squire_{RESAMPLE_INTERVAL}.nc")
        print(f"SQUIRE data successfully processed and saved to {data_dir}processed/SAIL/squire_{RESAMPLE_INTERVAL}.nc")
    except Exception as e:
        print(f"Error saving processed SQUIRE data: {e}")