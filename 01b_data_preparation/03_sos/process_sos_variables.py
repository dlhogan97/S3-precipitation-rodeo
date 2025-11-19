import numpy as np
import pandas as pd
import glob
import xarray as xr
import sys
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
import os
os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")
import utils.helper_funcs as hf

def fast_mode_rounded(x):
    arr = x.to_numpy()
    arr = arr[~np.isnan(arr)]  # drop NaNs fast
    if arr.size == 0:
        return np.nan
    rounded = (10 * np.round(arr / 10)).astype(int)
    vals, counts = np.unique(rounded, return_counts=True)
    return vals[np.argmax(counts)]

def process_sos_variables(ds, vars_to_keep, swe_vars, resample_interval='30min', reasonable_threshold=None):
    sub_ds = ds[vars_to_keep]
    ds.close()

    # for SWE_ variables, bfill NaN values
    for var in swe_vars:
        # backward fill values
        sub_ds[var] = sub_ds[var].bfill(dim='time')
        # remove any values with large absolute differences between time steps
        swe_diff = sub_ds[var].diff(dim='time')
        large_diff_mask = np.abs(swe_diff) > reasonable_threshold  # threshold of 30 mm
        # set the value after the large diff to NaN
        indices_to_nan = large_diff_mask.where(large_diff_mask, drop=True).time
        sub_ds[var] = sub_ds[var].where(~sub_ds['time'].isin(indices_to_nan), np.nan)
        # bfill again
        sub_ds[var] = sub_ds[var].bfill(dim='time')
        # create a new variable called var + '_max_accum_swe'
        sub_ds[var + '_max_accum'] = (('time',), np.maximum.accumulate(sub_ds[var].values))
        sub_ds[var + '_max_accum'].attrs['units'] = sub_ds[var].attrs['units']
        sub_ds[var + '_max_accum'].attrs['long_name'] = f"Accumulated {sub_ds[var].attrs.get('long_name', var)}"

    # convert to local time
    sub_ds = hf.convert_to_local_time(sub_ds, local_tz='America/Denver')

    # resample to 30min
    df = sub_ds.to_dataframe()
    df = df.resample(resample_interval).agg(
        {
            'SWE_p1_c': 'mean', 'SWE_p2_c': 'mean', 'SWE_p3_c': 'mean', 'SWE_p4_c': 'mean',
            'SWE_p1_c_max_accum': 'mean', 'SWE_p2_c_max_accum': 'mean', 'SWE_p3_c_max_accum': 'mean', 'SWE_p4_c_max_accum': 'mean',
            'spd_2m_c': 'mean', 'dir_2m_c': fast_mode_rounded, 'u_2m_c': 'mean', 'v_2m_c': 'mean',
            'spd_3m_c': 'mean', 'dir_3m_c': fast_mode_rounded, 'u_3m_c': 'mean', 'v_3m_c': 'mean',
            'spd_10m_c': 'mean', 'dir_10m_c': fast_mode_rounded, 'u_10m_c': 'mean', 'v_10m_c': 'mean',
            'h2o_2m_c': 'mean', 'h2o_3m_c': 'mean', 'h2o_10m_c': 'mean',
            'T_2m_c': 'mean', 'T_3m_c': 'mean', 'T_10m_c': 'mean',
            'RH_2m_c': 'mean', 'RH_3m_c': 'mean', 'RH_10m_c': 'mean',
            'P_10m_c': 'mean',
            'Rpile_out_9m_d': 'mean', 'Rpile_in_9m_d': 'mean', 'Rsw_in_9m_d': 'mean', 'Rsw_out_9m_d': 'mean',
        }
    )

    sub_ds_30min = df.to_xarray()

    # add attributes back
    for var in sub_ds_30min.data_vars:
        sub_ds_30min[var].attrs = sub_ds[var].attrs

    # add global attributes
    sub_ds_30min.attrs = sub_ds.attrs

    # add timezone to time dimension attribute
    sub_ds_30min['time'].attrs['tz'] = 'America/Denver'
    return sub_ds_30min

if __name__ == "__main__":
    storage_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    ds = xr.open_dataset(f"{storage_dir}processed/SOS/sos_ds_all_storage.nc")

    # change sites array to be ['d', 'ue', 'uw', 'c']
    sites = ['d', 'ue', 'uw', 'c']

    wind_vars = hf.WIND_VARIABLES
    wv_vars = hf.WATER_VAPOR_VARIABLES
    temp_vars = hf.TEMPERATURE_VARIABLES
    press_vars = hf.PRESSURE_VARIABLES
    swe_vars = hf.SWE_VARIABLES
    rad_vars = hf.RADIATION_VARIABLES
    RESAMPLE_INTERVAL = '30min'
    REASONABLE_THRESHOLD = 0.522 * 25.4  # reasonable threshold for precipitation

    vars_to_keep = wind_vars + wv_vars + temp_vars + press_vars + swe_vars

    # only variables at site _c
    vars_to_keep = [var for var in vars_to_keep if var.endswith('_c')]

    # only keep 2m, 3m, and 10m variables
    vars_to_keep = [var for var in vars_to_keep if any(f"_{h}" in var for h in ['2m', '3m', '10m', 'p1', 'p2', 'p3', 'p4'])]

    # remove vertical wind speed w_
    vars_to_keep = [var for var in vars_to_keep if not var.startswith('w_')] + rad_vars

    processed_ds = process_sos_variables(ds, vars_to_keep, swe_vars, RESAMPLE_INTERVAL, REASONABLE_THRESHOLD)
    processed_ds['time'] = pd.to_datetime(processed_ds['time'].values).tz_localize(None)
    processed_ds.to_netcdf(f"{storage_dir}processed/SOS/sos_ds_30min.nc")
    print(f"Processed SOS variables and saved to {storage_dir}processed/SOS/sos_ds_30min.nc")