import pandas as pd
import xarray as xr
import numpy as np
import os

def fast_mode_rounded(x):
    arr = x.to_numpy()
    arr = arr[~np.isnan(arr)]  # drop NaNs fast
    if arr.size == 0:
        return np.nan
    rounded = (10 * np.round(arr / 10)).astype(int)
    vals, counts = np.unique(rounded, return_counts=True)
    return vals[np.argmax(counts)]

if __name__ == "__main__":
    # access the data file (will need to suppl this file)
    df = pd.read_csv("/storage/dlhogan/precipitation-rodeo/data/raw/billy-barr/billy-barr-20211001-20230930_colsAdjusted.csv",)

    # remove spaces from all column names
    df.columns = df.columns.str.replace(' ', '')

    vars_to_keep = [
        "date","time","windSpeed_m_per_s","flg_windSpeed","windVecMag_m_per_s","flg_windVecMag",
        "windDirec_Deg", "flg_windDirec", "mxAirTemp_Deg_C", "flg_mxAirTemp", "mnAirTemp_Deg_C", "flg_mnAirTemp",
        "avAirTemp_Deg_C", "flg_avAirTemp", "relHumidty_%", "flg_relHumidty", "baromPress_mbar", "flg_baromPress",
        "precip_mm", "flg_precip", 'accumPcpn_mm',  'flg_accumPcpn',
    ]
    flg_vars = [var for var in vars_to_keep if var.startswith("flg_")]
    non_flg_vars = [var for var in vars_to_keep if not var.startswith("flg_")]
    non_flg_vars.remove("date")
    non_flg_vars.remove("time")

    # zip flag and non-flag variables together
    for non_flg_var, flg_var in zip(non_flg_vars, flg_vars):
        # set non-flag variable to NaN where flag variable is not 0
        df[non_flg_var] = df[non_flg_var].where(df[flg_var].isin([0,'E','0']), np.nan)

    sub_df = df[vars_to_keep]
    # make the index the combination of date and time
    sub_df.index = pd.to_datetime(sub_df['date'] + ' ' + sub_df['time'])
    sub_df = sub_df.drop(columns=['date', 'time'])

    # remove spaces from all column names
    sub_df.columns = sub_df.columns.str.replace(' ', '')

    # replace -9999.9 with NaN in all columns except flag columns
    for col in sub_df.columns:
        if col not in flg_vars:
            sub_df[col] = sub_df[col].replace(-9999.0, np.nan)

    # drop flag columns
    sub_df = sub_df.drop(columns=flg_vars)

    # resample to 30 min intervals
    sub_df_30min = sub_df.resample('30min').agg(
        {
            'windSpeed_m_per_s': 'mean',
            'windVecMag_m_per_s': 'mean',
            'windDirec_Deg': fast_mode_rounded,
            'mxAirTemp_Deg_C': 'mean',
            'mnAirTemp_Deg_C': 'mean',
            'avAirTemp_Deg_C': 'mean',
            'relHumidty_%': 'mean',
            'baromPress_mbar': 'mean',
            'precip_mm': 'sum',
            'accumPcpn_mm': 'sum',
            # 'SnowDepth_mm': 'mean',
        }
    )

    # convert to xarray dataset
    ds_30min = sub_df_30min.to_xarray()

    # rename index to time
    ds_30min = ds_30min.rename({'index': 'time'})

    # rename variables by removing units after '_' from name, then save units in attrs
    var_rename_dict = {
        'windSpeed_m_per_s': 'windSpeed',
        'windVecMag_m_per_s': 'windVecMag',
        'windDirec_Deg': 'windDirec',
        'mxAirTemp_Deg_C': 'mxAirTemp',
        'mnAirTemp_Deg_C': 'mnAirTemp',
        'avAirTemp_Deg_C': 'avAirTemp',
        'relHumidty_%': 'relHumidty',
        'baromPress_mbar': 'baromPress',
        'precip_mm': 'precip',
        'accumPcpn_mm': 'accumPcpn',
        # 'SnowDepth_mm': 'SnowDepth',
    }
    for old_name, new_name in var_rename_dict.items():
        ds_30min = ds_30min.rename({old_name: new_name})
        # save units in attrs
        if old_name.endswith('_m_per_s'):
            ds_30min[new_name].attrs['units'] = 'm/s'
        elif old_name.endswith('_Deg'):
            ds_30min[new_name].attrs['units'] = 'degrees'
        elif old_name.endswith('_Deg_C'):
            ds_30min[new_name].attrs['units'] = 'degC'
        elif old_name.endswith('_mbar'):
            ds_30min[new_name].attrs['units'] = 'mbar'
        elif old_name.endswith('_mm'):
            ds_30min[new_name].attrs['units'] = 'mm'
        elif old_name.endswith('_%'):
            ds_30min[new_name].attrs['units'] = '%'

    # add lat, lon, alt variables
    ds_30min['lat'] = 38.963
    ds_30min['lat'].attrs['units'] = 'degrees_north'

    ds_30min['lon'] = -106.993
    ds_30min['lon'].attrs['units'] = 'degrees_east'

    ds_30min['alt'] = 2917.6
    ds_30min['alt'].attrs['units'] = 'meters'

    # add global attributes
    ds_30min.attrs['site_name'] = 'billy_barr'
    ds_30min.attrs['location'] = 'East River Valley, Gothic, Colorado, USA'
    ds_30min.attrs['data_source'] = 'Data provided by the billy barr meteorological station, data obtained from https://wrcc.dri.edu/cgi-bin/rawMAIN.pl?corbil'
    ds_30min.attrs['processing_notes'] = 'Quality control flags were used to remove erroneous data. Data were resampled to 30 minute intervals using mean for continuous ' \
                                            'variables and sum for precipitation variables. Wind direction was resampled using mode after rounding to nearest 10 degrees.'
    ds_30min.attrs['date_created'] = pd.Timestamp.now().isoformat()


    # save to netcdf file
    output_path = "/storage/dlhogan/precipitation-rodeo/data/processed/billy_barr/billy_barr_20211001_20230930_30min.nc"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    ds_30min.to_netcdf(output_path)
    ds_30min.close()