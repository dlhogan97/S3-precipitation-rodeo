import cdsapi
import pandas as pd
import xarray as xr
import zipfile
import os

# Set the time zone shift as a variable so it is easy to change
TIME_ZONE_SHIFT_HOURS = -7  # UTC-7 for MDT

# Calculate the time the TIME-ZONE midnight in UTC
LOCAL_MIDNIGHT_IN_UTC = (0-TIME_ZONE_SHIFT_HOURS) % 24
TIME_STEPS = ['00:00', f"{LOCAL_MIDNIGHT_IN_UTC:02d}:00"]

# Define the date range for data retrieval
DATES = ["20211001", "20230930"]  # YYYYMMDD format

# Set file path + name
data_path = "/storage/dlhogan/precipitation-rodeo/data/external/ERA5-Land/"
os.makedirs(data_path, exist_ok=True)

# Set the result file path
result_file = f"{data_path}era5_land_{DATES[0]}_{DATES[1]}.nc"

if not os.path.exists(result_file):
    print("Downloading ERA5-Land data...")
    # Create a CDS API client instance
    client = cdsapi.Client() 
    dataset = "reanalysis-era5-land"
    request = {
        'product_type': ['reanalysis'],
        'variable': ['total_precipitation'],
        'date': f"{DATES[0]}/{DATES[1]}",
        'time': TIME_STEPS,
        'area': [39, -108, 38, -106],  # North, West, South, East
        'grid': [1, 1],
        'data_format': 'netcdf',
    }
    result_file = client.retrieve(dataset, request).download(f"/storage/dlhogan/precipitation-rodeo/data/external/ERA5-Land/era5_land_{DATES[0]}_{DATES[1]}.zip")

    # unzip if needed
    if result_file.endswith(".zip"):
        with zipfile.ZipFile(result_file, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(result_file))
        # Remove the zip file after extraction
        os.remove(result_file)
        result_file = result_file.replace(".zip", ".nc")
        # rename the file from data_0.nc to era5_land_YYYYMMDD_YYYYMMDD.nc
        if os.path.exists(os.path.join(os.path.dirname(result_file), "data_0.nc")):
            new_file_path = result_file.replace("data_0.nc", f"era5_land_{DATES[0]}_{DATES[1]}.nc")
            os.rename(os.path.join(os.path.dirname(result_file), "data_0.nc"), new_file_path)
            result_file = new_file_path
ds = xr.open_dataset(
    result_file
).load()

#### From UTC to Local Time (MST/MDT) ####
# Example taken from https://github.com/ecmwf-projects/dss-notebooks/blob/main/documentation/daily_accumulation_for_era5_land.ipynb
# Group the data by hour
ds_grouped_by_hour = ds.groupby("valid_time.hour")

# Then create new datasets for the UTC midnight and the local midnight
i_UTC_midnight, i_local_midnight = ds_grouped_by_hour.groups
ds_UTC_midnight = ds.isel(valid_time=ds_grouped_by_hour.groups[i_UTC_midnight])
ds_local_midnight = ds.isel(valid_time=ds_grouped_by_hour.groups[i_local_midnight])

ds_local_midnight = ds_local_midnight.assign_coords(
    valid_time=ds_UTC_midnight.valid_time + pd.Timedelta(days=1)
)

# Subtract the UTC midnight data from the local midnight data
ds_local_to_utc_midnight = ds_UTC_midnight - ds_local_midnight
# Shift the time back one day
ds_local_to_utc_midnight = ds_local_to_utc_midnight.assign_coords(
    valid_time=ds_local_to_utc_midnight.valid_time - pd.Timedelta(days=1)
)

ds_accum_local = ds_local_midnight + ds_local_to_utc_midnight

shift = int(TIME_ZONE_SHIFT_HOURS < 0)
ds_accum_local = ds_accum_local.assign_coords(
    valid_time=ds_accum_local.valid_time + pd.Timedelta(days=shift)
)

# Save the adjusted dataset to a new NetCDF file
output_file = result_file.replace(".nc", "_local_time.nc")
ds_accum_local.to_netcdf(output_file)
print(f"Saved local time adjusted data to {output_file}")

# close all datasets
ds.close()
