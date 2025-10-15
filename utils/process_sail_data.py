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
    "cumulative":["pwd_cumul_rain", "pwd_cumul_snow","tbrg_precip_total","tbrg_precip_total_corr", "rain_amount"],
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

def get_sail_point_info(ds):
    """Extract the latitude, longitude, and elevation from the SAIL dataset.

    Args:
        ds (xarray.Dataset): The input SAIL dataset.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing the point information.
    """
    # save the merged dataset to a new NetCDF file
    lat = ds["lat"].values[0]
    lon = ds["lon"].values[0]
    elev = ds["alt"].values[0]

    # Create a GeoDataFrame and name after dataset.attrs['datastream']

    gdf = gpd.GeoDataFrame(
        {
            "datastream": [ds.attrs.get("datastream", "unknown")],
            "latitude": [lat],
            "longitude": [lon],
            "elevation": [elev],
        },
        geometry=[Point(lon, lat, elev)],
        crs="EPSG:4326",
    )
    return gdf

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