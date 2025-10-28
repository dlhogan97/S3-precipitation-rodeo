SAIL_TEMPERATURE_VARS = {
    "mean":["temp_mean", "temperature_ambient"],
    "std":["temp_std"],
    "qc":["qc_temp_mean"],
}

SAIL_HUMIDITY_VARS = {
    "mean":["rh_mean", "vapor_pressure_mean", "rh_ambient"],
    "std":["rh_std", "vapor_pressure_std"],
    "qc":["qc_rh_mean", "qc_vapor_pressure_mean"],
}

SAIL_WIND_VARS = {
    "mean":["wspd_arith_mean", "wspd_vec_mean", "wind_speed", "u", "v"],
    "mode": ["wind_dir_mean", "wdir_vec_mean", "wind_direction"],
    "std":["wdir_vec_std"],
    "qc":["qc_wspd_arith_mean", "qc_wind_dir_mean", "qc_wspd_vec_mean", "qc_wdir_vec_mean"],
}

SAIL_PRECIPITATION_VARS = {
    "cumulative":["pwd_cumul_rain","tbrg_precip_total","tbrg_precip_total_corr", 
                  "rain_amount", "accum_rtnrt", "accum_nrt", "accum_total_nrt", "org_precip_accum",
                  'rain_rate_A_total','snow_rate_m2009_1_total','snow_rate_m2009_2_total','snow_rate_ws88diw_total','snow_rate_ws2012_total'],
    "duration": ["rain_duration"],
    "rate":["pwd_precip_rate_mean_1min","org_precip_rate_mean", "rain_intensity"],
    "qc":["qc_pwd_precip_rate_mean_1min","qc_pwd_cumul_rain","qc_pwd_cumul_snow","qc_org_precip_rate_mean","qc_tbrg_precip_total","qc_tbrg_precip_total_corr"],
}

SAIL_PRESSURE_VARS = {
    "mean":["atmos_pressure", "pressure_ambient"],
    "qc":["qc_atmos_pressure"],
}

import xarray as xr
import geopandas as gpd
from shapely.geometry import Point
from utils.helper_funcs import convert_to_local_time

def merge_sail_datasets(filepath):
    """Merge multiple SAIL datasets along the time dimension.

    Args:
        filepath (str): The file path to the SAIL NetCDF file.

    Returns:
        xarray.Dataset: The merged SAIL dataset.
    """
    # get all nc files in the directory
    filelist = [file for file in filepath if file.endswith(".nc")]
    dataset_list = []
    for file in filelist:
        ds = xr.open_dataset(file)
        dataset_list.append(ds)
        ds.close()
    ds = xr.concat(dataset_list, dim="time")
    return ds

def initial_sail_processing(file, vars_to_keep):
    ds = xr.open_dataset(file)
    ### 2. subset to only the variables we want
    for var in vars_to_keep:
        if "qc_" + var in ds.data_vars:
            vars_to_keep.append("qc_" + var)
    ds_sub = ds[vars_to_keep]
    # convert to local time
    try:
        ds_sub = convert_to_local_time(ds_sub, local_tz='America/Denver')
    except Exception as e:
        print(f"Error converting to local time: {e}")
    # close the original dataset
    ds.close()

    ### 3. filter out any bad data
    for var in ds_sub.data_vars:
        if 'qc' in var:
            data_var = var.replace('qc_', '')
            # drop replace values with NaN where qc is not 0
            ds_sub[data_var] = ds_sub[data_var].where(ds_sub[var] == 0)
    return ds_sub