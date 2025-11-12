import pandas as pd
import xarray as xr
import numpy as np

if __name__ == "__main__":
    df = pd.read_csv("/storage/dlhogan/precipitation-rodeo/data/raw/billy-barr/billy-barr-snow-data-ColsAdjusted.csv",)


    # build Month map 
    month_map = {
        'Jan': 1,
        'Feb': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'Aug': 8,
        'Sept': 9,
        'Oct': 10,
        'Nov': 11,
        'Dec': 12
    }
    df['Month'] = df['Month'].map(month_map)

    df['datetime'] = pd.to_datetime(df['Year'].astype(str) + df['Month'].astype(str) + df['Day'].astype(str), format='%Y%m%d')
    df = df.set_index('datetime')
    df.drop(columns=['Year', 'Month', 'Day'], inplace=True)

    # adjust SWE column to be in cm from in
    df['SWE'] = df['SWE'] * 2.54
    df['water_total'] = df['water_total'] * 2.54
    df['rain'] = df['rain'] * 2.54

    ds = df.to_xarray()
    ds['T_min'].attrs['units'] = 'degC'
    ds['T_min'].attrs['long_name'] = 'Daily Minimum Air Temperature'
    ds['T_max'].attrs['units'] = 'degC'
    ds['T_max'].attrs['long_name'] = 'Daily Maximum Air Temperature'
    ds['snow'].attrs['units'] = 'cm'
    ds['snow'].attrs['long_name'] = 'Daily Snowfall'
    ds['SWE'].attrs['units'] = 'cm'
    ds['SWE'].attrs['long_name'] = 'Daily Snow Water Equivalent'
    ds['snow_total'].attrs['units'] = 'cm'
    ds['snow_total'].attrs['long_name'] = 'Total Snowfall -- do not use'
    ds['water_total'].attrs['units'] = 'cm'
    ds['water_total'].attrs['long_name'] = 'Total Water Equivalent on Ground'
    ds['snow_depth'].attrs['units'] = 'cm'
    ds['snow_depth'].attrs['long_name'] = 'Snow Depth on Ground'
    ds['rain'].attrs['units'] = 'cm'
    ds['rain'].attrs['long_name'] = 'Daily Rainfall'

    # save the dataset
    ds.to_netcdf("/storage/dlhogan/precipitation-rodeo/data/processed/billy_barr/billy_barr_snow_data.nc")
    print("Saved processed data to /storage/dlhogan/precipitation-rodeo/data/processed/billy_barr/billy_barr_snow_data.nc")