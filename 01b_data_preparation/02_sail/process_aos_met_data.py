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

# open a sample dataset to get variables so we only have to do this once
example_ds = xr.open_dataset(files[1]) 
precipitation_sum_vars = [v for v in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if v in example_ds.data_vars]
precipitation_duration_vars = [v for v in process_sail_data.SAIL_PRECIPITATION_VARS['duration'] if v in example_ds.data_vars]
precipitation_rate_vars = [v for v in process_sail_data.SAIL_PRECIPITATION_VARS['rate'] if v in example_ds.data_vars]
relative_humidity_vars = [v for v in process_sail_data.SAIL_HUMIDITY_VARS['mean'] if v in example_ds.data_vars]
wind_dir_vars = [v for v in process_sail_data.SAIL_WIND_VARS['mode'] if v in example_ds.data_vars]
wind_spd_vars = [v for v in process_sail_data.SAIL_WIND_VARS['mean'] if v in example_ds.data_vars] + ["u", "v"]
temperature_vars = [v for v in process_sail_data.SAIL_TEMPERATURE_VARS['mean'] if v in example_ds.data_vars]
pressure_vars = [v for v in process_sail_data.SAIL_PRESSURE_VARS['mean'] if v in example_ds.data_vars]
example_ds.close()

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

def process_aos_met_data(file, resample_interval='30min'):
    """Process AOS met data from SAIL dataset.

    Args:
        ds (xarray.Dataset): The input SAIL dataset.
    Returns:
        xarray.Dataset: The processed dataset with standardized variable names and added wind components.
    """
    ds = xr.open_dataset(file)

    # Calculate u and v components if both wind speed and direction are present
    if 'wind_speed' in ds and 'wind_direction' in ds:
        u, v = calculate_wind_components(ds['wind_speed'], ds['wind_direction'])
        ds['u'] = (('time',), u)
        ds['v'] = (('time',), v)
        ds['u'].attrs['units'] = 'm/s'
        ds['v'].attrs['units'] = 'm/s'
        ds['u'].attrs['long_name'] = 'East-West wind component'
        ds['v'].attrs['long_name'] = 'North-South wind component'

    # Convert time to local timezone (MDT)
    ds = convert_to_local_time(ds, local_tz='America/Denver')

    # Add global attributes
    ds.attrs['processed_by'] = 'process_aos_met_data.py'

    # Get variables to keep and resample to desired interval
    precipitation_sum_da      = ds[precipitation_sum_vars].resample(time=resample_interval).sum()
    precipitation_duration_da = ds[precipitation_duration_vars].resample(time=resample_interval).sum()
    precipitation_rate_da     = ds[precipitation_rate_vars].resample(time=resample_interval).mean()
    relative_humidity_da      = ds[relative_humidity_vars].resample(time=resample_interval).mean()
    wind_spd_da               = ds[wind_spd_vars].resample(time=resample_interval).mean()
    wind_dir_da               = ds[wind_dir_vars].resample(time=resample_interval).reduce(xr_mode)
    temperature_da            = ds[temperature_vars].resample(time=resample_interval).mean()
    pressure_da               = ds[pressure_vars].resample(time=resample_interval).mean()

    # assign units to the new data arrays
    for var in precipitation_sum_vars:
        precipitation_sum_da[var].attrs['units'] = 'mm'
    for var in precipitation_duration_vars:
        # convert from seconds to minutes if needed
        if precipitation_duration_da[var].attrs['units'] == 's':
            precipitation_duration_da[var] = precipitation_duration_da[var] / 60
            precipitation_duration_da[var].attrs['units'] = 'min'
    for var in precipitation_rate_vars:
        precipitation_rate_da[var].attrs['units'] = 'mm/hr'
    for var in relative_humidity_vars:
        relative_humidity_da[var].attrs['units'] = '%'
    for var in wind_spd_vars:
        wind_spd_da[var].attrs['units'] = 'm/s'
    for var in wind_dir_vars:
        wind_dir_da[var].attrs['units'] = 'deg'
    for var in temperature_vars:
        temperature_da[var].attrs['units'] = 'degC'
    for var in pressure_vars:
        pressure_da[var].attrs['units'] = 'hPa'

    # merge all the data arrays back into a single dataset
    ds_merged = xr.merge([
        precipitation_sum_da,
        precipitation_duration_da,
        precipitation_rate_da,
        relative_humidity_da,
        wind_spd_da,
        wind_dir_da,
        temperature_da,
        pressure_da
    ])

    # add global attributes to the merged dataset
    ds_merged.attrs = ds.attrs
    ds_merged.attrs['processing_steps'] = 'Resampled to 30min intervals; calculated u and v wind components; standardized variable names and units'
    ds_merged.attrs['original_file'] = os.path.basename(file)

    ds.close()

    return ds_merged

if __name__ == "__main__":
    os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")
    SITE_NAME = "mtcb"
    RESAMPLE_INTERVAL = '30min'
    print(f"Processing AOS met data for site: {SITE_NAME}")
    print(f"Resampling interval: {RESAMPLE_INTERVAL}, change if needed")
    # create an empty list to hold processed datasets
    processed_datasets = []
    erroneous_files = []
    for i,file in enumerate(files):
        print("Processing file {}/{}: {}".format(i+1,len(files),file))
        start = time.time()
        try:
            ds_processed = process_aos_met_data(file, resample_interval=RESAMPLE_INTERVAL)
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
    output_filepath = f"{data_dir}/processed/SAIL/aos_{SITE_NAME}_met_{RESAMPLE_INTERVAL}.nc"
    print(f"Saving processed data to {output_filepath}")
    ds_all.to_netcdf(output_filepath)
    if len(erroneous_files) > 0:
        erroneous_df = pd.DataFrame(erroneous_files, columns=['erroneous_files'])
        erroneous_df.to_csv(f"{data_dir}/processed/SAIL/aos_{SITE_NAME}_erroneous_files.csv", index=False)
    ds_all.close()