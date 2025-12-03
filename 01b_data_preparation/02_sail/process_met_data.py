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


# -- functions used to process the data -- #
# calculate u and v from wind speed and direction
def xr_mode(x, axis=None):
    """Compute the statistical mode for an xarray reduce operation."""
    mode_result = stats.mode(x, nan_policy='omit', axis=axis)
    return xr.DataArray(mode_result.mode)

def calculate_wind_components(wind_speed, wind_direction):
    """
    Calculate the u and v components of wind from wind speed and direction.

    Parameters:
    wind_speed (float or np.ndarray): Wind speed in m/s.
    wind_direction (float or np.ndarray): Wind direction in degrees from north.

    Returns:
    tuple: A tuple containing the u and v components of the wind.
    """
    # Convert wind direction from degrees to radians
    wind_direction_rad = np.radians(wind_direction)

    # Calculate u and v components
    u = -wind_speed * np.sin(wind_direction_rad)  # East-West component
    v = -wind_speed * np.cos(wind_direction_rad)  # North-South component

    return u.data, v.data

def process_met_data(file, vars_to_keep, resample_interval='30min', reasonable_threshold=None):
    """
    Process a single MET netCDF file.

    Parameters:
    file (str): Path to the netCDF file.
    vars_to_keep (list): List of variable names to retain in the processed dataset.

    Returns:
    xarray.Dataset: Processed dataset.
    """
    print(f"Processing file: {file}")
    precip_vars_to_qc = [
        "qc_pwd_precip_rate_mean_1min",
        "qc_org_precip_rate_mean",
        "qc_tbrg_precip_total",
    ]
    qc_ds = xr.open_dataset(file)[precip_vars_to_qc]
    qc_pwd_missing = qc_ds['qc_pwd_precip_rate_mean_1min'].where(qc_ds['qc_pwd_precip_rate_mean_1min'].isin([0,2,3]), True).copy()
    qc_org_missing = qc_ds['qc_org_precip_rate_mean'].where(qc_ds['qc_org_precip_rate_mean'].isin([0,2,3]), True).copy()
    qc_tbrg_missing = qc_ds['qc_tbrg_precip_total'].where(qc_ds['qc_tbrg_precip_total'].isin([0,2,3]), True).copy()
    qc_pwd_bad = qc_ds['qc_pwd_precip_rate_mean_1min'].where(qc_ds['qc_pwd_precip_rate_mean_1min'].isin([0,1]), True).copy()
    qc_org_bad = qc_ds['qc_org_precip_rate_mean'].where(qc_ds['qc_org_precip_rate_mean'].isin([0,1]), True).copy()
    qc_tbrg_bad = qc_ds['qc_tbrg_precip_total'].where(qc_ds['qc_tbrg_precip_total'].isin([0,1]), True).copy()

    # fill all else with False
    qc_pwd_missing = qc_pwd_missing.where(qc_pwd_missing == True, False)
    qc_org_missing = qc_org_missing.where(qc_org_missing == True, False)
    qc_tbrg_missing = qc_tbrg_missing.where(qc_tbrg_missing == True, False)
    qc_pwd_bad = qc_pwd_bad.where(qc_pwd_bad == True, False)
    qc_org_bad = qc_org_bad.where(qc_org_bad == True, False)
    qc_tbrg_bad = qc_tbrg_bad.where(qc_tbrg_bad == True, False)

    # anywhere that precip_rate is NaN, set missing flag to True
    qc_pwd_missing = qc_pwd_missing.where(~qc_ds['qc_pwd_precip_rate_mean_1min'].isnull(), True)
    qc_org_missing = qc_org_missing.where(~qc_ds['qc_org_precip_rate_mean'].isnull(), True)
    qc_tbrg_missing = qc_tbrg_missing.where(~qc_ds['qc_tbrg_precip_total'].isnull(), True)

    # name the flags
    qc_pwd_missing.name = 'pwd_precip_missing_flag'
    qc_org_missing.name = 'org_precip_missing_flag'
    qc_tbrg_missing.name = 'tbrg_precip_missing_flag'
    qc_pwd_bad.name = 'pwd_precip_bad_flag'
    qc_org_bad.name = 'org_precip_bad_flag'
    qc_tbrg_bad.name = 'tbrg_precip_bad_flag'

    # convert all to local time and subset to vars to keep
    for d in [qc_pwd_bad, qc_org_bad, qc_tbrg_bad,
              qc_pwd_missing, qc_org_missing, qc_tbrg_missing]:
        try:
            d = convert_to_local_time(d, local_tz='America/Denver')
            # Convert timezone-aware times to UTC and make them naive
            d['time'] = d.indexes['time'].tz_localize(None)
        except Exception as e:
            print(f"Error converting to local time: {e}")

    # process sail data
    ds = process_sail_data.initial_sail_processing(file, vars_to_keep=vars_to_keep)
    # Convert timezone-aware times to UTC and make them naive
    ds['time'] = ds.indexes['time'].tz_localize(None)
    # add qc flags to dataset
    ds = xr.merge([ds, qc_pwd_missing, qc_org_missing, qc_tbrg_missing,
                   qc_pwd_bad, qc_org_bad, qc_tbrg_bad])
    # create accumulated variables
    org_precip_accum = ds["org_precip_rate_mean"] / 60
    org_precip_accum.name = "org_precip_accum"
    org_precip_accum.attrs["units"] = "mm"
    org_precip_accum.attrs["long_name"] = "Optical Rain Gauge Precipitation Accumulation"

    pwd_precip_total = ds["pwd_precip_rate_mean_1min"] / 60
    pwd_precip_total.name = "pwd_precip_total"
    pwd_precip_total.attrs["units"] = "mm"
    pwd_precip_total.attrs["long_name"] = "Precipitation accumulation from Present Weather Detector"

    if reasonable_threshold is not None:
        ds['org_precip_rate_mean'] = ds['org_precip_rate_mean'].where(ds['org_precip_rate_mean'] <= reasonable_threshold, np.nan)

    # calculate wind components
    # Calculate u and v components if both wind speed and direction are present
    if 'wspd_vec_mean' in ds and 'wdir_vec_mean' in ds:
        u, v = calculate_wind_components(ds['wspd_vec_mean'], ds['wdir_vec_mean'])
        ds['u'] = (('time',), u)
        ds['v'] = (('time',), v)
        ds['u'].attrs['units'] = 'm/s'
        ds['v'].attrs['units'] = 'm/s'
        ds['u'].attrs['long_name'] = 'East-West wind component'
        ds['v'].attrs['long_name'] = 'North-South wind component'

    # resample to desired length
    # accumulated variables: sum
    org_precip_accum_da = org_precip_accum.resample(time=resample_interval).sum()
    pwd_precip_total_da = pwd_precip_total.resample(time=resample_interval).sum()
    tbrg_precip_total_da = ds['tbrg_precip_total'].where(ds['tbrg_precip_total'] < reasonable_threshold, np.nan).resample(time=resample_interval).sum()

    # mean variables: mean
    atmos_pressure_da = ds['atmos_pressure'].resample(time=resample_interval).mean()
    temp_mean_da = ds['temp_mean'].resample(time=resample_interval).mean()
    rh_mean_da = ds['rh_mean'].resample(time=resample_interval).mean()
    vapor_pressure_mean_da = ds['vapor_pressure_mean'].resample(time=resample_interval).mean()
    wspd_vec_mean_da = ds['wspd_vec_mean'].resample(time=resample_interval).mean()
    u_mean_da = ds['u'].resample(time=resample_interval).mean()
    v_mean_da = ds['v'].resample(time=resample_interval).mean()
    pwd_precip_rate_mean_da = ds['pwd_precip_rate_mean_1min'].resample(time=resample_interval).mean()
    org_precip_rate_mean_da = ds['org_precip_rate_mean'].resample(time=resample_interval).mean()

    # mode variables: mode
    # silence small sample warning
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        wdir_vec_mean_da = ((10 * np.round(ds['wdir_vec_mean'] / 10)).astype(int)).resample(time=resample_interval).reduce(xr_mode)
        pwd_err_code_da = (ds['pwd_err_code'].fillna(0)).resample(time=resample_interval).reduce(xr_mode)

    # flag variables: if greater than 25% of data in interval is missing/bad, flag as True
    pwd_precip_missing_flag_da = ds['pwd_precip_missing_flag'].resample(time=resample_interval).sum()
    org_precip_missing_flag_da = ds['org_precip_missing_flag'].resample(time=resample_interval).sum()
    tbrg_precip_missing_flag_da = ds['tbrg_precip_missing_flag'].resample(time=resample_interval).sum()
    pwd_precip_bad_flag_da = ds['pwd_precip_bad_flag'].resample(time=resample_interval).sum()
    org_precip_bad_flag_da = ds['org_precip_bad_flag'].resample(time=resample_interval).sum()
    tbrg_precip_bad_flag_da = ds['tbrg_precip_bad_flag'].resample(time=resample_interval).sum()

    n_counts = ds['org_precip_rate_mean'].resample(time=resample_interval).count()
    pwd_precip_missing_flag_da = (pwd_precip_missing_flag_da / n_counts) > 0.25
    org_precip_missing_flag_da = (org_precip_missing_flag_da / n_counts) > 0.25
    tbrg_precip_missing_flag_da = (tbrg_precip_missing_flag_da / n_counts) > 0.25
    pwd_precip_bad_flag_da = (pwd_precip_bad_flag_da / n_counts) > 0.25
    org_precip_bad_flag_da = (org_precip_bad_flag_da / n_counts) > 0.25
    tbrg_precip_bad_flag_da = (tbrg_precip_bad_flag_da / n_counts) > 0.25

    pwd_precip_missing_flag_da.name = 'pwd_precip_missing_flag'
    org_precip_missing_flag_da.name = 'org_precip_missing_flag'
    tbrg_precip_missing_flag_da.name = 'tbrg_precip_missing_flag'
    pwd_precip_bad_flag_da.name = 'pwd_precip_bad_flag'
    org_precip_bad_flag_da.name = 'org_precip_bad_flag'
    tbrg_precip_bad_flag_da.name = 'tbrg_precip_bad_flag'

    # first value for lat, lon, alt
    lat_da = ds['lat'].resample(time=resample_interval).first()
    lon_da = ds['lon'].resample(time=resample_interval).first()
    alt_da = ds['alt'].resample(time=resample_interval).first()

    # merge all data arrays
    ds_merged = xr.merge([
        org_precip_accum_da,
        org_precip_missing_flag_da,
        org_precip_bad_flag_da,
        pwd_precip_total_da,
        pwd_precip_missing_flag_da,
        pwd_precip_bad_flag_da,
        tbrg_precip_total_da,
        tbrg_precip_missing_flag_da,
        tbrg_precip_bad_flag_da,
        atmos_pressure_da,
        temp_mean_da,
        rh_mean_da,
        vapor_pressure_mean_da,
        wspd_vec_mean_da,
        u_mean_da,
        v_mean_da,
        pwd_precip_rate_mean_da,
        org_precip_rate_mean_da,
        wdir_vec_mean_da,
        pwd_err_code_da,
        lat_da,
        lon_da,
        alt_da
    ])


    ds.close()
    return ds_merged


if __name__ == "__main__":
    # Assign data directory and get files
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    files = glob.glob(f"{data_dir}raw/SAIL/met/*.nc")
    RESAMPLE_INTERVAL = '30min'  # resample interval
    REASONABLE_THRESHOLD = 0.522 * 25.4  # reasonable threshold for precipitation
    vars_to_keep = [
    'atmos_pressure','temp_mean','rh_mean','vapor_pressure_mean',
    'wspd_vec_mean','wdir_vec_mean',
    'pwd_err_code','pwd_precip_rate_mean_1min',
    'org_precip_rate_mean','tbrg_precip_total',
    'lat','lon','alt',
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
            ds_processed = process_met_data(file, 
                                            vars_to_keep, 
                                            resample_interval=RESAMPLE_INTERVAL, 
                                            reasonable_threshold=REASONABLE_THRESHOLD)
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
    output_filepath = f"{data_dir}/processed/SAIL/met_{RESAMPLE_INTERVAL}.nc"
    print(f"Saving processed data to {output_filepath}")
    ds_all.to_netcdf(output_filepath)
    if len(erroneous_files) > 0:
        erroneous_df = pd.DataFrame(erroneous_files, columns=['erroneous_files'])
        erroneous_df.to_csv(f"{data_dir}/processed/SAIL/met_{RESAMPLE_INTERVAL}_erroneous_files.csv", index=False)
    ds_all.close()
    end_full = time.time()

    print(f"Done! Total processing time: {end_full - full_start:.2f} seconds")