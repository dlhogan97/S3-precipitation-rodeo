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

parsivel_correction_dict = { 
    'holroyd1971': [0.17, -1],
    'brandes2007': [0.178, -0.922],
    'heymsfield2004': [0.104, -0.95],
    'huang2010' : [0.115, -1.188]
}

def correct_SAIL_parsivel_for_snow(ds, method='holroyd1971'):
    """
    Correct snowfall rate using a method discussed in Boudala et al. 2014
    """
    a = parsivel_correction_dict[method][0]
    b = parsivel_correction_dict[method][1]
    # Number density of particles
    N_D = ds['number_density_drops']
    # Fall velocity of particles summed over raw_fall_velocity
    V_D = ds['fall_velocity_calculated']
    # Class size width
    class_size_width = ds['class_size_width']

    # Apply the condition to include particle sizes from 3 to 31
    particle_size_indices = range(3, 32)
    raw_fall_velocity_indices = range(3, 32)

    # Select the relevant slices using isel
    N_D_masked = N_D.isel(particle_size=particle_size_indices)
    class_size_width_masked = class_size_width.isel(particle_size=particle_size_indices)
    V_D_masked = V_D.isel(raw_fall_velocity=raw_fall_velocity_indices)

    # Calculate the snowfall rate using vectorized operations
    result = (N_D_masked * V_D_masked * class_size_width_masked ** (3 + b)).sum(dim='particle_size').sum(dim='raw_fall_velocity')

    # Calculate the final result
    final_result = (6 * a * np.pi * 10e-4 * result)/60
    # filter to only include times with snowfall
    final_result = final_result.where(ds['weather_code'].isin([70,71,72,73,74,75,76,77,78,79,85,86,87]), ds['precip_rate'])
    return final_result

def xr_mode(x, axis=None):
    """Compute the statistical mode for an xarray reduce operation."""
    mode_result = stats.mode(x, nan_policy='omit', axis=axis)
    return xr.DataArray(mode_result.mode)

vars_to_keep = [
        'precip_rate', # mean
        'weather_code', # mode
        'equivalent_radar_reflectivity_ott', # mean or max
        'number_detected_particles', # sum
        'mor_visibility', # mode
        'class_size_width', # mean
        'fall_velocity_calculated', # mean
        'liquid_water_content', # mean
        'raw_spectrum', # mean?
        'median_volume_diameter', # median
        'snow_depth_intensity', # mean
        'number_density_drops', # mean
        'lon',
        'lat',
        'alt',
        ]
def process_disdrometer_data(file, resample_interval='30min', local_tz='America/Denver', reasonable_threshold=None):
    ### 1. open the dataset
    ds = xr.open_dataset(file)
    ### 2. subset to only the variables we want
    for var in vars_to_keep:
        if 'qc_' + var in ds.data_vars:
            vars_to_keep.append('qc_' + var)
    ds_sub = ds[vars_to_keep]
    # convert to local time
    try:
        ds_sub = convert_to_local_time(ds_sub, local_tz=local_tz)
    except Exception as e:
        print(f"Error converting to local time: {e}")
    # close the original dataset
    ds.close()

    ### 3. filter out any bad data
    # create precip_flag for data
    precip_missing_flag = ds_sub['qc_precip_rate'].where(ds_sub['qc_precip_rate'].isin([0,2,3]), True).copy()
    precip_bad_flag = ds_sub['qc_precip_rate'].where(ds_sub['qc_precip_rate'].isin([0,1]), True).copy()
    # fill all else with False
    precip_missing_flag = precip_missing_flag.where(precip_missing_flag == True, False)
    precip_bad_flag = precip_bad_flag.where(precip_bad_flag == True, False)
    # anywhere that precip_rate is NaN, set missing flag to True
    precip_missing_flag = precip_missing_flag.where(~ds_sub['precip_rate'].isnull(), True)
    # name the flags
    precip_missing_flag.name = 'precip_missing_flag'
    precip_bad_flag.name = 'precip_bad_flag'
    # add flags back to dataset
    ds_sub = xr.merge([ds_sub, precip_missing_flag, precip_bad_flag])
    
    # loop through variables and apply qc
    for var in ds_sub.data_vars:
        if 'qc' in var:
            data_var = var.replace('qc_', '')
            # drop replace values with NaN where qc is not 0
            ds_sub[data_var] = ds_sub[data_var].where(ds_sub[var] == 0, np.nan)

    # replace unreasonable precip rates with NaN
    if reasonable_threshold is not None:
        ds_sub['precip_rate'] = ds_sub['precip_rate'].where(ds_sub['precip_rate'] <= reasonable_threshold, np.nan)
        # update precip_flag accordingly
        ds_sub['precip_bad_flag'] = ds_sub['precip_bad_flag'].where(ds_sub['precip_rate'] <= reasonable_threshold, True)
    #  4. calculate correction for snow using all methods (save these as individual arrays)
    precip_rate_uncorrected = ds_sub['precip_rate']
    precip_rate_holroyd = correct_SAIL_parsivel_for_snow(ds_sub, 'holroyd1971')
    precip_rate_brandes = correct_SAIL_parsivel_for_snow(ds_sub, 'brandes2007')
    precip_rate_heymsfield = correct_SAIL_parsivel_for_snow(ds_sub, 'heymsfield2004')

    precip_rate_uncorrected.name = 'precip_rate_uncorrected'
    precip_rate_holroyd.name = 'precip_rate_holyroyd'
    precip_rate_brandes.name = 'precip_rate_brandes'
    precip_rate_heymsfield.name = 'precip_rate_heymsfield'

    # add unit and long_name attributes
    for precip_rate, method in zip([precip_rate_holroyd, precip_rate_brandes, precip_rate_heymsfield],
                                ['holroyd1971', 'brandes2007', 'heymsfield2004']):
        precip_rate.attrs['units'] = 'mm/hr'
        precip_rate.attrs['long_name'] = f'Precipitation rate corrected for snow using {method} method'

    # 5. create accumulated variable
    precip_accum_uncorrected = precip_rate_uncorrected/60 # convert from mm/hr to mm/min
    precip_accum_holroyd = precip_rate_holroyd/60
    precip_accum_brandes = precip_rate_brandes/60
    precip_accum_heymsfield = precip_rate_heymsfield/60

    precip_accum_uncorrected.name = 'precip_accum_uncorrected'
    precip_accum_holroyd.name = 'precip_accum_holyroyd'
    precip_accum_brandes.name = 'precip_accum_brandes'
    precip_accum_heymsfield.name = 'precip_accum_heymsfield'

    precip_accum_uncorrected.attrs['units'] = 'mm'
    precip_accum_holroyd.attrs['units'] = 'mm'
    precip_accum_brandes.attrs['units'] = 'mm'
    precip_accum_heymsfield.attrs['units'] = 'mm'

    precip_accum_uncorrected.attrs['long_name'] = 'Accumulated precipitation uncorrected'
    precip_accum_holroyd.attrs['long_name'] = 'Accumulated precipitation corrected using holroyd1971 method'
    precip_accum_brandes.attrs['long_name'] = 'Accumulated precipitation corrected using brandes2007 method'
    precip_accum_heymsfield.attrs['long_name'] = 'Accumulated precipitation corrected using heymsfield2004 method'

    # 6. resample to desired length using appropriate function for each variable
    
    # accumulated variables: sum
    precip_accum_uncorrected_da = precip_accum_uncorrected.resample(time=resample_interval).sum()
    precip_accum_holroyd_da = precip_accum_holroyd.resample(time=resample_interval).sum()
    precip_accum_brandes_da = precip_accum_brandes.resample(time=resample_interval).sum()
    precip_accum_heymsfield_da = precip_accum_heymsfield.resample(time=resample_interval).sum()
    number_detected_particles_da = ds_sub['number_detected_particles'].resample(time=resample_interval).sum()
   
    # mean variables: 
    precip_rate_da = ds_sub['precip_rate'].resample(time=resample_interval).mean()
    equivalent_radar_reflectivity_ott_da = ds_sub['equivalent_radar_reflectivity_ott'].resample(time=resample_interval).mean()
    class_size_width_da = ds_sub['class_size_width'].resample(time=resample_interval).mean()
    fall_velocity_calculated_da = ds_sub['fall_velocity_calculated'].resample(time=resample_interval).mean()
    liquid_water_content_da = ds_sub['liquid_water_content'].resample(time=resample_interval).mean()
    raw_spectrum_da = ds_sub['raw_spectrum'].resample(time=resample_interval).mean()
    snow_depth_intensity_da = ds_sub['snow_depth_intensity'].resample(time=resample_interval).mean()
    number_density_drops_da = ds_sub['number_density_drops'].resample(time=resample_interval).mean()

    # mode variables:
    weather_code_da = ds_sub['weather_code'].resample(time=resample_interval).reduce(xr_mode)
    mor_visibility_da = ds_sub['mor_visibility'].resample(time=resample_interval).reduce(xr_mode)

    # median variables:
    median_volume_diameter_da = ds_sub['median_volume_diameter'].resample(time=resample_interval).median()

    # # flag variables: if greater than 25% of data in interval is missing/bad, flag as True
    n_counts = ds_sub['precip_rate'].resample(time=resample_interval).count()
    precip_missing_flag_da = (ds_sub['precip_missing_flag'].resample(time=resample_interval).sum() / n_counts) > 0.25
    precip_bad_flag_da = (ds_sub['precip_bad_flag'].resample(time=resample_interval).sum() / n_counts) > 0.25
    # add name
    precip_missing_flag_da.name = 'precip_missing_flag'
    precip_bad_flag_da.name = 'precip_bad_flag'
    # add flag attributes
    precip_missing_flag_da.attrs['description'] = 'Quality flag for precipitation data: True = missing data, False = data present'
    precip_bad_flag_da.attrs['description'] = 'Quality flag for precipitation data: True = bad data, False = good data'

    # lon, lat, alt: take first value
    lon_da = ds_sub['lon'].resample(time=resample_interval).first()
    lat_da = ds_sub['lat'].resample(time=resample_interval).first()
    alt_da = ds_sub['alt'].resample(time=resample_interval).first()

    # 7. merge all variables back into a single dataset
    ds_resampled = xr.merge([
        precip_accum_uncorrected_da,
        precip_accum_holroyd_da,
        precip_accum_brandes_da,
        precip_accum_heymsfield_da,
        number_detected_particles_da,
        precip_rate_da,
        precip_missing_flag_da,
        precip_bad_flag_da,
        equivalent_radar_reflectivity_ott_da,
        class_size_width_da,
        fall_velocity_calculated_da,
        liquid_water_content_da,
        raw_spectrum_da,
        snow_depth_intensity_da,
        number_density_drops_da,
        weather_code_da,
        mor_visibility_da,
        median_volume_diameter_da,
        lon_da,
        lat_da,
        alt_da
    ])
    return ds_resampled

if __name__ == "__main__":
    os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")
    SITE_NAME = "gothic"  # change if needed
    RESAMPLE_INTERVAL = '30min'
    REASONABLE_THRESHOLD = 0.522*25.4  # set to None or 5-minute maximum precip rate in mm from NOAA Atlas 14
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
    files = glob.glob(f"{data_dir}raw/SAIL/laser_disdrometer_{SITE_NAME}/*.nc")
    print(f"Processing laser disdrometer data for site: {SITE_NAME}")
    print(f"Resampling interval: {RESAMPLE_INTERVAL}, change if needed")
    # create an empty list to hold processed datasets
    processed_datasets = []
    erroneous_files = []
    full_start = time.time()
    for i,file in enumerate(files):
        print("Processing file {}/{}: {}".format(i+1,len(files),file))
        start = time.time()
        try:
            ds_processed = process_disdrometer_data(file, 
                                                    resample_interval=RESAMPLE_INTERVAL, 
                                                    local_tz='America/Denver', 
                                                    reasonable_threshold=REASONABLE_THRESHOLD)
            processed_datasets.append(ds_processed)
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            erroneous_files.append(file)
        end = time.time()
        print(f"Time taken: {end - start:.2f} seconds")
    if erroneous_files:
        print("The following files could not be processed:")
        for ef in erroneous_files:
            print(ef)
    # concatenate all processed datasets along the time dimension
    if processed_datasets:
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
        # save to netcdf
        output_dir = f"{data_dir}processed/SAIL/"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"laser_disdrometer_{SITE_NAME}_processed_{RESAMPLE_INTERVAL}.nc")
        print(f"Saving processed data to {output_file}")
        ds_all.to_netcdf(output_file)
        end_full = time.time()
        print(f"Done! Total processing time: {end_full - full_start:.2f} seconds")