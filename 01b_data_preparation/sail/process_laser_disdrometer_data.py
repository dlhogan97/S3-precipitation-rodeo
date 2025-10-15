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
SITE_NAME = "gothic"
data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
files = glob.glob(f"{data_dir}/raw/SAIL/laser_disdrometer_{SITE_NAME}/*.nc")

parsivel_correction_dict = { 
    'holroyd1971': [0.17, -1],
    'brandes2007': [0.178, -0.922],
    'heymsfield2004': [0.104, -0.95]
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

    # Apply the condition to include particle sizes from 2 to 31
    particle_size_indices = range(2, 32)
    raw_fall_velocity_indices = range(2, 32)

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

if __name__ == "__main__":
    os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")
    SITE_NAME = "mtcb"
    RESAMPLE_INTERVAL = '30min'
    print(f"Processing laser disdrometer data for site: {SITE_NAME}")
    print(f"Resampling interval: {RESAMPLE_INTERVAL}, change if needed")
    # create an empty list to hold processed datasets
    processed_datasets = []
    erroneous_files = []
    for i,file in enumerate(files):
        print("Processing file {}/{}: {}".format(i+1,len(files),file))
        start = time.time()