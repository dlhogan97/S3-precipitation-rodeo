import xarray as xr
import pandas as pd
import numpy as np
import glob
import sys, os

if __name__ == "__main__":
    # set data directory
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"

    # add project root to sys.path for imports
    project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
    if project_root not in sys.path:
        sys.path.append(project_root)
    from utils.helper_funcs import convert_to_local_time, get_point_info

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

    # get the lon and lat values for gothic and kettle ponds
    example_gothic_ds = xr.open_dataset(f"{data_dir}processed/SAIL/met_30min.nc")
    example_kp_ds = xr.open_dataset(f"{data_dir}processed/SPLASH/asfs30_30min.nc")
    gothic_point_info = get_point_info(example_gothic_ds)
    kettle_ponds_point_info = get_point_info(example_kp_ds)

    # check if PRISM files are present
    if not os.path.exists(f"{data_dir}external/PRISM/processed/"):
        print("PRISM data directory not found. Download the data before proceeding.")
    try:
        files = glob.glob(f"{data_dir}external/PRISM/processed/prism_ppt_us_30s*.nc")
    except Exception as e:
        print(f"Error finding PRISM files: {e}")

    # extract the date from the filenames
    for file in files:
        date_str = file.split('_')[-2]
        date = pd.to_datetime(date_str, format='%Y%m%d')
        ds = xr.open_dataset(file)
        # add a time dimension
        ds = ds.expand_dims(time=[date])
        # select the nearest point to gothic
        ds_gothic = ds.sel(lon=gothic_point_info['longitude'].values
                            , lat=gothic_point_info['latitude'].values, method='nearest').squeeze()
        ds_gothic = ds_gothic.expand_dims(site=['gothic'])
        ds_kettle_ponds = ds.sel(lon=kettle_ponds_point_info['longitude'].values
                            , lat=kettle_ponds_point_info['latitude'].values, method='nearest').squeeze()
        ds_kettle_ponds = ds_kettle_ponds.expand_dims(site=['kettle_ponds'])

        # concatenate to single dataset
        ds_sites = xr.concat([ds_gothic, ds_kettle_ponds], dim='site')
        if 'ds_all' in locals():
            ds_all = xr.concat([ds_all, ds_sites], dim='time')
        else:
            ds_all = ds_sites

    # save the combined dataset
    ds_all.to_netcdf(f"{data_dir}processed/final/PRISM/prism_site_data.nc")
    print(f"Saved PRISM site data to netcdf: {data_dir}processed/final/PRISM/prism_site_data.nc")