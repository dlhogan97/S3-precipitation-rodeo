import pandas as pd
import glob
import xarray as xr

def process_lpdf_data(file, reasonable_threshold=None):
    # read in data
    df = pd.read_csv(file, sep=r'\s+', header=0,)
    # select columns
    sub_df = df[['date', 'time(MST)', 'Precip_mm']]
    sub_df = sub_df.assign(
        datetime=pd.to_datetime(sub_df['date'] + ' ' + sub_df['time(MST)'], format='%Y-%m-%d %H:%M:%S')
    )

    sub_df = sub_df.set_index('datetime')
    sub_df = sub_df.drop(columns=['date', 'time(MST)'])

    # Define the full expected range
    inferred_freq = pd.infer_freq(sub_df.index)
    full_index = pd.date_range(
        start=sub_df.index.min(),
        end=sub_df.index.max(),
        freq=inferred_freq,  # fallback if not inferred
        tz=sub_df.index.tz
    )

    # Reindex to fill any gaps
    sub_df = sub_df.reindex(full_index)

    # backfill missing values
    sub_df['Precip_mm'] = sub_df['Precip_mm'].bfill()

    # drop duplicates
    sub_df = sub_df[~sub_df.index.duplicated(keep='first')]

    # drop the hour values on daylight saving time transitions in 2021, 2022, or 2023
    dst_transition_times = [
        pd.Timestamp('2021-03-14 02:00:00'),
        pd.Timestamp('2021-11-07 01:00:00'),
        pd.Timestamp('2022-03-13 02:00:00'),
        pd.Timestamp('2022-11-06 01:00:00'),
        pd.Timestamp('2023-03-12 02:00:00'),
        pd.Timestamp('2023-11-05 01:00:00'),
    ]
    sub_df = sub_df[~sub_df.index.isin(dst_transition_times)]
    
    sub_df['qc_missing_precip'] = sub_df['Precip_mm'].isna().astype(int)
    sub_df['qc_bad_precip'] = 0  # placeholder for bad precip flag
    # apply reasonableness threshold
    if reasonable_threshold is not None:
        sub_df['Precip_mm'] = sub_df['Precip_mm'].where(sub_df['Precip_mm'] <= reasonable_threshold, float('nan'))
        sub_df['qc_bad_precip'] = ((sub_df['Precip_mm'] > reasonable_threshold)).astype(int)
    
    # set time zone
    # sub_df.index = sub_df.index.tz_localize('America/Denver', nonexistent='NaT', ambiguous='NaT')
    return sub_df



if __name__ == "__main__":
    data_dir = "/storage/dlhogan/precipitation-rodeo/data/"

    files = glob.glob(f"{data_dir}raw/SPLASH/LPDF_gauge/*.txt")
    output_path = f"{data_dir}/processed/SPLASH/lpdf_gauge_30min.nc"
    REASONABLE_THRESHOLD = 1.12 * 25.4  # reasonable threshold for precipitation
    if len(files) == 0:
        raise ValueError("No files found for processing. Get data from SPLASH LPDF gauge (refer to 01a_data_access README).")
    
    df_list = []
    for file in files:
        df = process_lpdf_data(file, reasonable_threshold=REASONABLE_THRESHOLD)
        df_list.append(df)

    # concatenate all dataframes
    combined_df = pd.concat(df_list).sort_index()

    # convert to xarray
    ds = combined_df.to_xarray()

    # rename variable to prcp
    ds = ds.rename({'Precip_mm': 'prcp',
                    'index':'time'})
    # add variable attributes
    ds['prcp'].attrs['units'] = 'mm'
    ds['prcp'].attrs['long_name'] = 'LPDF Gauge Precipitation'
    ds['qc_missing_precip'].attrs['long_name'] = 'Quality Control Flag for Missing Precipitation'
    ds['qc_bad_precip'].attrs['long_name'] = 'Quality Control Flag for Bad Precipitation (Exceeds Reasonable Threshold)'

    # add global attributes
    ds.attrs['timezone'] = 'America/Denver (MST/MDT)'
    ds.attrs['title'] = 'LPDF Gauge Precipitation at Kettle Ponds, CO'
    ds.attrs['source'] = 'SPLASH LPDF Gauge'
    ds.attrs['contact'] = 'Contact Tilden Meyers for dataset <tilden.meyers@noaa.gov>'

    # save to netcdf
    ds.to_netcdf(output_path)
    print(f"Saved processed LPDF gauge data to {output_path}")
    ds.close()