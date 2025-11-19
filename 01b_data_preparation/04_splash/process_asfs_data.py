import xarray as xr
import pandas as pd
import numpy as np
import glob
import os 
os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")
import sys
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
import utils.helper_funcs as hf
import time

def process_asfs_data(files, vars_to_keep=None, resample_interval='30min'):
    if vars_to_keep is None:
        print("No variables specified to keep. Exiting function.")
        return None
    
    example_ds = xr.open_dataset(files[0])
    qc_vars = [v + '_qc' for v in vars_to_keep if v + '_qc' in example_ds.data_vars]

    ds_list = []
    for i, file in enumerate(files):
        print(f"Processing file {i+1}/{len(files)}: {file}")
        start = time.time()
        ds = xr.open_dataset(file)

        sub_ds = ds[vars_to_keep + qc_vars]
        ds.close()

        # qc relevant data when qc vars are bad
        for var in vars_to_keep:    
            qc_var = var + '_qc'
            if qc_var in sub_ds.data_vars:
                sub_ds[var] = sub_ds[var].where(sub_ds[qc_var] == 0, other=np.nan)

        # drop qc vars
        sub_ds = sub_ds.drop_vars(qc_vars)

        # convert to local time
        sub_ds = hf.convert_to_local_time(sub_ds, local_tz='America/Denver')


        # resample to 30min
        sub_ds_30min = sub_ds.resample(time=resample_interval).mean() 
        
        # add timezones as global attribute
        sub_ds_30min.attrs['time_zone'] = 'America/Denver'

        ds_list.append(sub_ds_30min)
        end = time.time()
        print(f"Finished processing file {i+1}/{len(files)} in {end - start:.2f} seconds.")

    combined_ds = xr.concat(ds_list, dim='time')
    combined_ds = combined_ds.sortby('time')
    return combined_ds

if __name__ == "__main__":

    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"

    # check if ASFS-30 files are present
    if not os.path.exists(f"{data_dir}raw/SPLASH/ASFS-30_Level2_SPLASH2021-2023/"):
        print("ASFS-30 data directory not found. Download the data before proceeding.")
    try:
        files = glob.glob(f"{data_dir}raw/SPLASH/ASFS-30_Level2_SPLASH2021-2023/sledseb.asfs30.level2.0.10min*.nc")
    except Exception as e:
        print(f"Error finding ASFS-30 files: {e}")

    RESAMPLE_INTERVAL = '30min'
    VARS_TO_KEEP = [
                    'lat',
                    'lon',
                    'altitude',
                    'snow_depth',
                    'atmos_pressure',
                    'temp',
                    'rh',
                    'vapor_pressure',
                    'rhi',
                    'wspd_u_mean',
                    'wspd_v_mean',
                    'down_long_hemisp',
                    'down_short_hemisp',
                    'up_long_hemisp',
                    'up_short_hemisp',
                    ]
    
    asfs_ds_30min = process_asfs_data(files, vars_to_keep=VARS_TO_KEEP, resample_interval=RESAMPLE_INTERVAL)
    asfs_ds_30min['time'] = pd.to_datetime(asfs_ds_30min['time'].values).tz_localize(None)
    asfs_ds_30min.to_netcdf(f"{data_dir}processed/SPLASH/asfs30_30min.nc")
    print(f"ASFS-30 data processing complete. Saved to {data_dir}processed/SPLASH/asfs30_30min.nc")