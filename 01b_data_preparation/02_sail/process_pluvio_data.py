import glob
import os
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from utils import process_sail_data
from utils.helper_funcs import convert_to_local_time
import xarray as xr
import time
import numpy as np
from scipy import stats
import pandas as pd

# Assign data directory and get files
SITE_NAME = "mtcb"
data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
files = glob.glob(f"{data_dir}raw/SAIL/aos_{SITE_NAME}/*.nc")

# make sure files exist
if len(files) == 0:
    raise FileNotFoundError(f"No files found in {data_dir}/raw/SAIL/aos_{SITE_NAME}/")

def process_pluvio_data(file, vars_to_keep, resample_interval='30min', reasonableness_max=10):
    """
    Process a single Pluvio netCDF file.

    Parameters:
    file (str): Path to the netCDF file.
    vars_to_keep (list): List of variable names to retain in the processed dataset.

    Returns:
    xarray.Dataset: Processed dataset.
    """
    print(f"Processing file: {file}")
    ds = process_sail_data.initial_sail_processing(file, vars_to_keep=vars_to_keep)

    # fill times with 0 when maintenance_flag > 0
    ds = ds.where(
    (ds['maintenance_flag'] == 0)
    & (ds['reset_flag'] == 0)
    & (ds['pluvio_status'] == 0)
    & (ds['heater_status'] == 0)
    & (ds['accum_nrt'] < reasonableness_max), # 10-year threshold
    np.nan
    )
    # apply reasonableness check to intensity_rt
    ds['intensity_rt'] = ds['intensity_rt'].where(ds['intensity_rt'] <= reasonableness_max, np.nan)
    # accumulated variables: sum
    accum_rtnrt_da = ds['accum_rtnrt'].resample(time=resample_interval).sum()
    accum_nrt_da = ds['accum_nrt'].resample(time=resample_interval).sum()
    accum_total_nrt_da = ds['accum_total_nrt'].resample(time=resample_interval).sum()
    # rate variables: mean
    intensity_rt_da = ds['intensity_rt'].resample(time=resample_interval).mean()
    intensity_rtnrt_da = ds['intensity_rtnrt'].resample(time=resample_interval).mean()
    # first value for lat, lon, alt
    lat_da = ds['lat'].resample(time=resample_interval).first()
    lon_da = ds['lon'].resample(time=resample_interval).first()
    alt_da = ds['alt'].resample(time=resample_interval).first()

    ds.close()

    ds_merged = xr.merge([
        accum_rtnrt_da,
        accum_nrt_da,
        accum_total_nrt_da,
        intensity_rt_da,
        intensity_rtnrt_da,
        lat_da,
        lon_da,
        alt_da
    ])
    
    return ds_merged

if __name__ == "__main__":
    # Assign data directory and get files
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    files = glob.glob(f"{data_dir}raw/SAIL/pluvio/*.nc")
    RESAMPLE_INTERVAL = '30min'  # resample interval
    REASONABLENESS_MAX = 10  # maximum reasonable intensity in mm between 2 five minute observations

    vars_to_keep = [
    'intensity_rt',
    'accum_rtnrt',
    'accum_nrt',
    'accum_total_nrt',
    'maintenance_flag',
    'reset_flag',
    'intensity_rtnrt',
    'pluvio_status',
    'heater_status',
    'lat',
    'lon',
    'alt',
    ]

    print(f"Processing {len(files)} MET files from {data_dir}")
    print(f"Resampling to {RESAMPLE_INTERVAL} intervals")
    # create an empty list to hold processed datasets
    processed_datasets = []
    erroneous_files = []
    full_start = time.time()
    for i,file in enumerate(files):
        print("Processing file {}/{}: {}".format(i+1,len(files),file))
        start = time.time()
        try:
            ds_processed = process_pluvio_data(file, vars_to_keep, resample_interval=RESAMPLE_INTERVAL, reasonableness_max=REASONABLENESS_MAX)
        except Exception as e:
            print("Error processing file {}: {}".format(file, e))
            erroneous_files.append(file)
            continue
        processed_datasets.append(ds_processed)
        end = time.time()
        print("Processed in {:.2f} seconds".format(end-start))

    # concatenate all processed datasets along the time dimension
    ds_all = xr.concat(processed_datasets, dim='time')
    # Convert timezone-aware times to UTC and make them naive
    ds_all['time'] = ds_all.indexes['time'].tz_localize(None)
    # drop duplicate times if any
    _, index = np.unique(ds_all['time'], return_index=True)
    ds_all = ds_all.isel(time=index)

    # Build a complete half-hourly time index
    full_time_index = pd.date_range(
        start=ds_all['time'].min().item(),
        end=ds_all['time'].max().item(),
        freq='30min'
    )
    # Reindex to fill missing times with NaNs
    ds_all = ds_all.reindex(time=full_time_index)

    ds_all['time'].attrs['timezone'] = 'MST (UTC-6)'
    # sort by time
    ds_all = ds_all.sortby('time')
    # save to a new netcdf file
    output_filepath = f"{data_dir}/processed/SAIL/pluvio_{RESAMPLE_INTERVAL}.nc"
    print(f"Saving processed data to {output_filepath}")
    ds_all.to_netcdf(output_filepath)
    if len(erroneous_files) > 0:
        erroneous_df = pd.DataFrame(erroneous_files, columns=['erroneous_files'])
        erroneous_df.to_csv(f"{data_dir}/processed/SAIL/pluvio_{RESAMPLE_INTERVAL}_erroneous_files.csv", index=False)
    ds_all.close()
    end_full = time.time()

    print(f"Done! Total processing time: {end_full - full_start:.2f} seconds")