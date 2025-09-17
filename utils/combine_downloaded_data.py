import pandas as pd
import datetime as dt
import xarray as xr
import numpy as np
import os 
from pathlib import Path

def combine_files(file_list, out_path, filename):
    """
    Combine multiple NetCDF files into a single NetCDF file.

    Parameters:
    file_list (list): List of paths to the NetCDF files to be combined.
    out_path (str): Path to save the combined NetCDF file.

    Returns:
    None
    """
    # open and merge datasets
    datasets = [xr.open_dataset(f) for f in file_list]
    combined_ds = xr.merge(datasets, compat='override')
    combined_ds = combined_ds.sortby('time')

    # ensure that missing values are consistently represented as NaN
    for var, da in combined_ds.data_vars.items():
        if da.dtype == np.float32 or da.dtype == np.int32:
            combined_ds[var] = da.fillna(np.nan)

    # Save the combined dataset to a new NetCDF file
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    out_file = os.path.join(out_path, filename)
    print(f"Saving combined dataset to {out_file}")
    if not os.path.exists(out_file):
        combined_ds.to_netcdf(out_file)
    else:
        print(f"File {out_file} already exists. Skipping save.")
    
    print(f"Combined {len(file_list)} files into {out_path}")
    return 

if __name__ == "__main__":
    combine_command_line = input("Do you want to combine files via command line input? (yes/no): ").strip().lower() == 'yes'
    if combine_command_line == 'yes':
        print("Provide a list of files to combine:")
        files = []
        while True:
            file_path = input("Enter file path (or 'done' to finish): ")
            if file_path.lower() == 'done':
                break
            files.append(file_path)
        output_directory = input("Enter output directory: ")
        output_filename = input("Enter output filename (with .nc extension): ")
        
        # provide check for user to make sure everything is correct
        print(f"Files to combine:")
        for f in files:
            print(f" - {f}\n")
        print(f"Output directory: {output_directory}")
        print(f"Output filename: {output_filename}")
        confirm = input("Is this correct? (yes/no): ")
        if confirm.lower() == 'yes':
            combine_files(files, output_directory, output_filename)
        else:
            print("Operation cancelled by user.")
    
    else:
        output_dir = Path("/storage/dlhogan/precipitation-rodeo/data/raw")
        
        # Laser disdrometer data from Gothic, CO
        laser_disdrometer_2022 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_21_22/laser_disdrometer_gothic_20211001_20220930.nc")
        laser_disdrometer_2023 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_22_23/laser_disdrometer_gothic_20221001_20230930.nc")
        laser_disdrometer_out = "SAIL_laserDisdrometerGothic_all.nc"
        # Combine files
        combine_files([laser_disdrometer_2022, laser_disdrometer_2023], output_dir, laser_disdrometer_out)
        print("Combined Gothic laser disdrometer data.")

        # Laser disdrometer data from Mt. Crested Butte, CO
        laser_disdrometer_mcb_2022 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_21_22/laser_disdrometer_mt_cb_20211001_20220930.nc")
        laser_disdrometer_mcb_2023 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_22_23/laser_disdrometer_mt_cb_20221001_20230930.nc")
        laser_disdrometer_mcb_out = "SAIL_laserDisdrometerMtCb_all.nc"
        combine_files([laser_disdrometer_mcb_2022, laser_disdrometer_mcb_2023], output_dir, laser_disdrometer_mcb_out)
        print("Combined Mt. Crested Butte laser disdrometer data.")

        # Gothic pluviometer data
        gothic_pluviometer_2022 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_21_22/pluvio2_20211001_20220930.nc")
        gothic_pluviometer_2023 = Path("/storage/dlhogan/synoptic_sublimation/sail_data/winter_22_23/pluviometer_gothic_20221001_20230930.nc")
        gothic_pluviometer_out = "SAIL_pluviometerGothic_all.nc"
        combine_files([gothic_pluviometer_2022, gothic_pluviometer_2023], output_dir, gothic_pluviometer_out)
        print("Combined Gothic pluviometer data.")
        print("All done!")

        
        
        