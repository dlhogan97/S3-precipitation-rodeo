""" 
Generated with the help of ChatGPT 4.0 at OpenAI. Link to prompts: https://chatgpt.com/share/52f25a1e-c008-43f0-80c2-7d44dde02cd7
"""
import pandas as pd
import numpy as np
import xarray as xr
import glob as glob
import os
import datetime as dt

os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")

original_storage_path = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/'
final_storage_path = '/storage/dlhogan/precipitation-rodeo/data/processed/SPLASH/'
if not os.path.exists(original_storage_path+'laser_disdrometer_stats_KP'):
    print("Download data using ~/01a_data_access/splash/laser_disdrometer_raw_data_download.py")
else:
    print('Data already downloaded')
    # we'll start by loading in one file and looking at the data
    original_storage_path = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/laser_disdrometer_stats_KP/*'
    files = glob.glob(original_storage_path)

def process_laser_disdrometer_file(file_path, reasonable_threshold=None):
    """
    Process a laser disdrometer file and return a xarray Dataset with the data.
    """
    # Define the size bins
    size_bins = [
        0.062, 0.187, 0.312, 0.437, 0.562, 0.687, 0.812, 0.937, 1.062, 1.187,
        1.375, 1.625, 1.875, 2.125, 2.375, 2.75, 3.25, 3.75, 4.25, 4.75,
        5.5, 6.5, 7.5, 8.5, 9.5, 11.0, 13.0, 15.0, 17.0, 19.0, 21.5, 24.5
    ]

    # Read the file
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Parse the header for metadata (assuming first line is header)
    header = lines[0].strip()
    instrument_info, time_info = header.split(" Time (YYJJJHH): ")
    year = int("20" + time_info[:2])
    day_of_year = int(time_info[2:5])
    start_hour = int(time_info[5:7])

    # Skip the first two lines
    lines = lines[2:]
    
    # Initialize a list to store parsed data
    data = []

    # Process each line in the file
    for i, line in enumerate(lines):
        fields = line.strip().split()
        
        # Skip lines that don't have the correct number of fields
        if len(fields) != 57:
            continue
        
        # Extract and convert data fields
        begin_time_str = fields[0].split('-')[0]
        end_time_str = fields[0].split('-')[1]
        particle_distribution = list(map(int, fields[1:33]))
        qc_data = list(map(int, fields[33:36]))
        precip_stats = list(map(float, fields[36:41]))
        laser_status = list(map(float, fields[41:47]))
        sensor_status = list(map(float, fields[47:53]))
        precip_partitioning = list(map(int, fields[53:57]))

        # Convert begin_time and end_time to timestamps
        begin_time = pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day_of_year-1, hours=start_hour,
                                                                minutes=int(begin_time_str[0:2]), 
                                                                seconds=int(begin_time_str[3:5]),
                                                                milliseconds=int(begin_time_str[6:9]))
        end_time = pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day_of_year-1, hours=start_hour,
                                                                minutes=int(end_time_str[0:2]), 
                                                                seconds=int(end_time_str[3:5]), 
                                                                milliseconds=int(end_time_str[6:9]))
        # If end time is before begin time, increment the hour by 1
        if end_time < begin_time:
            end_time += pd.Timedelta(hours=1)
        
        data.append({
            "time": begin_time,
            "particle_distribution": particle_distribution,
            "qc_data": qc_data,
            "precip_stats": precip_stats,
            "laser_status": laser_status,
            "sensor_status": sensor_status,
            "precip_partitioning": precip_partitioning
        })
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Ensure all lists in df have consistent lengths
    assert all(len(item) == 32 for item in df["particle_distribution"]), "Mismatch in particle_distribution length"
    assert all(len(item) == 3 for item in df["qc_data"]), "Mismatch in qc_data length"
    assert all(len(item) == 5 for item in df["precip_stats"]), "Mismatch in precip_stats length"
    assert all(len(item) == 6 for item in df["laser_status"]), "Mismatch in laser_status length"
    assert all(len(item) == 6 for item in df["sensor_status"]), "Mismatch in sensor_status length"
    assert all(len(item) == 4 for item in df["precip_partitioning"]), "Mismatch in precip_partitioning length"

    # Convert DataFrame to xarray Dataset
    ds = xr.Dataset(
        {
            "particle_distribution": (("time", "size_bins"), np.stack(df["particle_distribution"].values)),
            "Blackout": ("time", df["qc_data"].apply(lambda x: x[0])),
            "Good": ("time", df["qc_data"].apply(lambda x: x[1])),
            "Bad": ("time", df["qc_data"].apply(lambda x: x[2])),
            "NumParticle": ("time", df["precip_stats"].apply(lambda x: x[0])),
            "Rate": ("time", df["precip_stats"].apply(lambda x: x[1]), {"units": "mm/h", "descriptor": "Precipitation"}),
            "Amount": ("time", df["precip_stats"].apply(lambda x: x[2]), {"units": "mm", "descriptor": "Precipitation"}),
            "AmountSum": ("time", df["precip_stats"].apply(lambda x: x[3]), {"units": "mm", "descriptor": "Precipitation"}),
            "Z": ("time", df["precip_stats"].apply(lambda x: x[4]), {"units": "dB", "descriptor": "Precipitation"}),
            "NumError": ("time", df["laser_status"].apply(lambda x: x[0])),
            "Dirty": ("time", df["laser_status"].apply(lambda x: x[1])),
            "VeryDirty": ("time", df["laser_status"].apply(lambda x: x[2])),
            "Damaged": ("time", df["laser_status"].apply(lambda x: x[3])),
            "SignalAvg": ("time", df["laser_status"].apply(lambda x: x[4])),
            "SignalStdDev": ("time", df["laser_status"].apply(lambda x: x[5])),
            "TempAvg": ("time", df["sensor_status"].apply(lambda x: x[0]), {"units": "C", "descriptor": "Sensor Status"}),
            "TempStdDev": ("time", df["sensor_status"].apply(lambda x: x[1]), {"units": "C", "descriptor": "Sensor Status"}),
            "VoltAvg": ("time", df["sensor_status"].apply(lambda x: x[2]), {"units": "V", "descriptor": "Sensor Status"}),
            "VoltStdDev": ("time", df["sensor_status"].apply(lambda x: x[3]), {"units": "V", "descriptor": "Sensor Status"}),
            "HeatCurrentAvg": ("time", df["sensor_status"].apply(lambda x: x[4]), {"units": "A", "descriptor": "Sensor Status"}),
            "HeatCurrentStdDev": ("time", df["sensor_status"].apply(lambda x: x[5]), {"units": "A", "descriptor": "Sensor Status"}),
            "NumRain": ("time", df["precip_partitioning"].apply(lambda x: x[0])),
            "NumNoRain": ("time", df["precip_partitioning"].apply(lambda x: x[1])),
            "NumAmbig": ("time", df["precip_partitioning"].apply(lambda x: x[2])),
            "Type": ("time", df["precip_partitioning"].apply(lambda x: x[3]))
        },
        coords={
            "time": df["time"].values,
            "size_bins": size_bins,
        }
    )
    
    # apply reasonableness threshold
    if reasonable_threshold is not None:
        ds['Amount'] = ds['Amount'].where(ds['Amount'] <= reasonable_threshold, np.nan)
    
    # Add descriptor attributes
    ds["Blackout"].attrs["descriptor"] = "Samples"
    ds["Good"].attrs["descriptor"] = "Samples"
    ds["Bad"].attrs["descriptor"] = "Samples"
    ds["NumParticle"].attrs["descriptor"] = "Precipitation"
    ds["NumParticle"].attrs["units"] = ""
    ds["Rate"].attrs["descriptor"] = "Precipitation"
    ds["Amount"].attrs["descriptor"] = "Precipitation"
    ds["AmountSum"].attrs["descriptor"] = "Precipitation"
    ds["Z"].attrs["descriptor"] = "Precipitation"
    ds["NumError"].attrs["descriptor"] = "Laser Status"
    ds["Dirty"].attrs["descriptor"] = "Laser Status"
    ds["VeryDirty"].attrs["descriptor"] = "Laser Status"
    ds["Damaged"].attrs["descriptor"] = "Laser Status"
    ds["SignalAvg"].attrs["descriptor"] = "Laser Status"
    ds["SignalStdDev"].attrs["descriptor"] = "Laser Status"
    ds["TempAvg"].attrs["descriptor"] = "Sensor Status"
    ds["TempStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["VoltAvg"].attrs["descriptor"] = "Sensor Status"
    ds["VoltStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["HeatCurrentAvg"].attrs["descriptor"] = "Sensor Status"
    ds["HeatCurrentStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["NumRain"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["NumNoRain"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["NumAmbig"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["Type"].attrs["descriptor"] = "Precipitation Partitioning"


        # Particle distribution
    ds["particle_distribution"].attrs["long_name"] = "Partical distribution (count) binned by ClassNumber"

    # Data acquisition software quality control
    ds["Blackout"].attrs["long_name"] = "number of data samples excluded during PC clock synchronization"
    ds["Good"].attrs["long_name"] = "number of samples that passed the quality control checks, as performed by the data acquisition software"
    ds["Bad"].attrs["long_name"] = "number of samples that failed the quality control checks, as performed by the data acquisition software"

    # Precipitation statistics
    ds["NumParticle"].attrs["long_name"] = "total number of detected particles"
    ds["Rate"].attrs["long_name"] = "rain rate"
    ds["Amount"].attrs["long_name"] = "interval rain accumulation"
    ds["AmountSum"].attrs["long_name"] = "event rain accumulation"
    ds["Z"].attrs["long_name"] = "radar reflectivity factor"

    # Laser status
    ds["NumError"].attrs["long_name"] = "number of sample instances that were reported as dirty, very dirty, or damaged"
    ds["Dirty"].attrs["long_name"] = "laser protective glass is dirty, but measurements are still possible"
    ds["VeryDirty"].attrs["long_name"] = "laser protective glass is dirty, partially covered; no further usable measurements are possible"
    ds["Damaged"].attrs["long_name"] = "laser damaged"
    ds["SignalAvg"].attrs["long_name"] = "average signal amplitude of the laser strip"
    ds["SignalStdDev"].attrs["long_name"] = "standard deviation of the signal amplitude of the laser strip"

    # Sensor status
    ds["TempAvg"].attrs["long_name"] = "average sensor temperature"
    ds["TempStdDev"].attrs["long_name"] = "standard deviation of the sensor temperature"
    ds["VoltAvg"].attrs["long_name"] = "sensor power supply voltage"
    ds["VoltStdDev"].attrs["long_name"] = "standard deviation of the sensor power supply voltage"
    ds["HeatCurrentAvg"].attrs["long_name"] = "average heating system current"
    ds["HeatCurrentStdDev"].attrs["long_name"] = "standard deviation of the heating system current"

    # Precipitation partitioning
    ds["NumRain"].attrs["long_name"] = "number of particles detected as rain"
    ds["NumNoRain"].attrs["long_name"] = "number of particles detected not as rain"
    ds["NumAmbig"].attrs["long_name"] = "number of particles detected as ambiguous"
    ds["Type"].attrs["long_name"] = "precipitation type (1=rain; 2=mixed; 3=snow)"

    ds['time'].attrs['long_name'] = 'Time (UTC)'

    return ds

def mode_function(x):
        return x.mode().iloc[0] if not x.mode().empty else None
def only_one(df, resampling_interval):
     return(
          df
          .set_index("time")
          .resample(resampling_interval)
          .mean()
     )
def per_group(df, resampling_interval):
    return (
        df
        .set_index("time")
        .resample(resampling_interval)
        .agg({
            # 'particle_distribution': 'mean', 
            'Blackout': mode_function, 
            'Good': mode_function, 
            'Bad': mode_function, 
            'NumParticle': 'sum', 
            'Rate': 'mean', 
            'Amount': 'sum', 
            'AmountSum': 'sum', 
            'Z': 'mean', 
            'NumError': mode_function, 
            'Dirty': mode_function, 
            'VeryDirty': mode_function, 
            'Damaged': mode_function, 
            'SignalAvg': 'mean', 
            'SignalStdDev': 'mean', 
            'TempAvg': 'mean', 
            'TempStdDev': 'mean', 
            'VoltAvg': 'mean', 
            'VoltStdDev': 'mean', 
            'HeatCurrentAvg': 'mean', 
            'HeatCurrentStdDev': 'mean', 
            'NumRain': mode_function, 
            'NumNoRain': mode_function, 
            'NumAmbig': mode_function, 
            'Type': 'mean'})) 

def resample_xarray_dataset(ds, resampling_interval):
    """
    Resample an xarray Dataset along the 'time' dimension.
    Particle distribution variables are averaged, while 
    cumulative variables are summed, and categorical/mode-type 
    variables are handled appropriately.
    """

    attrs_dataset = ds.attrs.copy()
    attrs_vars = {var: ds[var].attrs.copy() for var in ds.variables}

    # Define how to handle each variable
    mean_vars = []
    sum_vars = ["Amount", "AmountSum", "NumParticle"]
    mode_vars = ["Blackout", "Good", "Bad", "NumError", "Dirty", "VeryDirty", 
                 "Damaged", "NumRain", "NumNoRain", "NumAmbig", "Type"]

    # Initialize dictionary for resampled data
    resampled_data = {}

    # 1️⃣ Mean variables
    for var in mean_vars:
        if var in ds:
            resampled_data[var] = ds[var].resample(time=resampling_interval).mean(keep_attrs=True)

    # 2️⃣ Sum variables
    for var in sum_vars:
        if var in ds:
            resampled_data[var] = ds[var].resample(time=resampling_interval).sum(keep_attrs=True)

    # 3️⃣ Mode variables
    def xr_mode(x, **kwargs):
        """Compute statistical mode along time."""
        vals, counts = np.unique(x[~np.isnan(x)], return_counts=True)
        if len(counts) == 0:
            return np.nan
        return vals[np.argmax(counts)]

    for var in mode_vars:
        if var in ds:
            resampled_data[var] = ds[var].resample(time=resampling_interval).reduce(xr_mode, keep_attrs=True)

    # Merge everything into one dataset
    ds_resampled = xr.Dataset(resampled_data)

    # Restore attributes
    ds_resampled.attrs.update(attrs_dataset)
    for var in ds_resampled.variables:
        if var in attrs_vars:
            ds_resampled[var].attrs.update(attrs_vars[var])

    return ds_resampled


# %%
# now we can process all the files
# lets start by creating a list of all the datasets
ds_list = []

for file in files:
    # print the run time every 100 files to keep track of progress
    if files.index(file) % 100 == 0:
        print(f"Processing file {files.index(file)} of {len(files)}")
    try:
        ds = process_laser_disdrometer_file(file, reasonable_threshold=0.522*25.44)  # defined as 100-year precip event over 10 minutes, converted to mm and sourced from NOAA Atlas 14
        # resample to 5-minutes
        # ds = resample_xarray_dataset(ds, '30min')
        ds_list.append(ds)
    except Exception as e:
        print(f"Error processing file {file}: {e}")
        continue

# then we can concatenate them all together
combined_ds = xr.concat(ds_list, dim="time")
# lastly we can make sure its sorted by time
combined_ds = combined_ds.sortby("time")

# %%
# calculate the 30minute resampled mean of the data by first
# saving all the variable and dataset attributes and then converting to pands to resample
# converting back to xarray and adding the attributes back
# resampled_30min mean
resampled_30min_ds = resample_xarray_dataset(combined_ds, '30min')

# %%
# save the dataset to a netcdf file
output = True
if output:
    resampled_30min_ds.to_netcdf(f'{final_storage_path}/SPLASH_kp_laser_disdrometer_30min.nc')

# %%
# close all open datasets
resampled_30min_ds.close()
