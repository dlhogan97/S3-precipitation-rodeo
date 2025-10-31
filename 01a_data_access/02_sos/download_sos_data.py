import pandas as pd
import xarray as xr
import os
import urllib
from urllib.error import URLError
import numpy as np
from metpy.units import units
import datetime as dt
from dateutil.relativedelta import relativedelta
import time

# Separate out the eddy covariance measurement variable names because they are very repetitive
ec_measurement_suffixes = [
    '1m_ue',    '2m_ue',    '3m_ue',    '10m_ue', 
    '1m_d',     '2m_d',     '3m_d',     '10m_d',
    '1m_uw',    '2m_uw',    '2_5m_uw',  '3m_uw',    '10m_uw', 
    '1m_c',     '2m_c',     '3m_c',     '5m_c',     '10m_c',    '15m_c',    '20m_c'
]

sonic_measurement_prefixes = [
    'u_', 'v_', 'w_', 'tc_', 'spd_', 'dir_', 
    'u_u__', 'v_v__', 'w_w__', 'tc_tc__', 
    'u_w__', 'v_w__', 'u_v__', 
    'u_tc__', 'v_tc__', 'w_tc__', 
    'u_u_u__', 'v_v_v__', 'w_w_w__', 
    'tc_tc_tc__', 
]
irga_measurement_prefixes = [
    'h2o_', 'h2o_h2o__', 'h2o_h2o_h2o__', 
]
sonic_plus_irga_measurement_prefixes = [
    'u_h2o__', 'v_h2o__', 'w_h2o__', 
]
ec_measurement_prefixes = sonic_measurement_prefixes + irga_measurement_prefixes + sonic_plus_irga_measurement_prefixes

ec_variable_names = [
    (prefix + suffix) for prefix in ec_measurement_prefixes for suffix in ec_measurement_suffixes
]

counts_vars = ['counts_' + suffix for suffix in ec_measurement_suffixes]
counts_1_vars = ['counts_' + suffix + '_1' for suffix in ec_measurement_suffixes]
counts_2_vars = ['counts_' + suffix + '_2' for suffix in ec_measurement_suffixes]
irgadiag_vars = ['irgadiag_' + suffix for suffix in ec_measurement_suffixes]
ldiag_vars = ['ldiag_' + suffix for suffix in ec_measurement_suffixes]

diagnostic_variable_names = counts_vars + counts_1_vars + counts_2_vars + irgadiag_vars + ldiag_vars

VARIABLE_NAMES = ec_variable_names + diagnostic_variable_names + [
    # Temperature & Relative Humidity Array 
    'T_1m_c', 'T_2m_c', 'T_3m_c', 'T_4m_c', 'T_5m_c', 'T_6m_c', 'T_7m_c', 'T_8m_c', 'T_9m_c', 'T_10m_c',
    'T_11m_c', 'T_12m_c', 'T_13m_c', 'T_14m_c', 'T_15m_c', 'T_16m_c', 'T_17m_c', 'T_18m_c', 'T_19m_c', 'T_20m_c',

    'RH_1m_c', 'RH_2m_c', 'RH_3m_c', 'RH_4m_c', 'RH_5m_c', 'RH_6m_c', 'RH_7m_c', 'RH_8m_c', 'RH_9m_c', 'RH_10m_c',
    'RH_11m_c','RH_12m_c','RH_13m_c','RH_14m_c','RH_15m_c','RH_16m_c','RH_17m_c','RH_18m_c','RH_19m_c','RH_20m_c',

    # Pressure Sensors
    'P_20m_c',
    'P_10m_c', 'P_10m_d', 'P_10m_uw', 'P_10m_ue',

    # Blowing snow/FlowCapt Sensors
    'SF_avg_1m_ue', 'SF_avg_2m_ue',

    # Apogee sensors
    "Vtherm_c", "Vtherm_d", "Vtherm_ue", "Vtherm_uw", 
    "Vpile_c", "Vpile_d", "Vpile_ue", "Vpile_uw",
    "IDir_c", "IDir_d", "IDir_ue", "IDir_uw",

    # Snow-level temperature arrays (towers D and UW)
    'Tsnow_0_4m_d', 'Tsnow_0_5m_d', 'Tsnow_0_6m_d', 'Tsnow_0_7m_d', 'Tsnow_0_8m_d', 'Tsnow_0_9m_d', 'Tsnow_1_0m_d', 'Tsnow_1_1m_d', 'Tsnow_1_2m_d', 'Tsnow_1_3m_d', 'Tsnow_1_4m_d', 'Tsnow_1_5m_d',
    'Tsnow_0_4m_uw', 'Tsnow_0_5m_uw', 'Tsnow_0_6m_uw', 'Tsnow_0_7m_uw', 'Tsnow_0_8m_uw', 'Tsnow_0_9m_uw', 'Tsnow_1_0m_uw', 'Tsnow_1_1m_uw', 'Tsnow_1_2m_uw', 'Tsnow_1_3m_uw', 'Tsnow_1_4m_uw', 'Tsnow_1_5m_uw',
    
    # Downward/Upward Facing Longwave Radiometers
    'Rpile_out_9m_d','Tcase_out_9m_d',    
    'Rpile_in_9m_d', 'Tcase_in_9m_d',
    'Tcase_uw', 'Rpile_in_uw', 'Rpile_out_uw',
    
    # Upward facing shortwave radiometer (tower D) - for measuring incoming solar radiation!
    'Rsw_in_9m_d', 'Rsw_out_9m_d', 'Rsw_in_uw', 'Rsw_out_uw',

    # Snow Pillow SWE
    'SWE_p1_c', 'SWE_p2_c', 'SWE_p3_c', 'SWE_p4_c',

    # Soil Moisture
    'Qsoil_d',

    # Ground Heat Flux
    'Gsoil_d',
    
    # Soil Temperature
    'Tsoil_0_6cm_d','Tsoil_3_1cm_d','Tsoil_1_9cm_d','Tsoil_4_4cm_d','Tsoil_8_1cm_d','Tsoil_9_4cm_d','Tsoil_10_6cm_d','Tsoil_11_9cm_d','Tsoil_18_1cm_d',
    'Tsoil_19_4cm_d','Tsoil_20_6cm_d','Tsoil_21_9cm_d','Tsoil_28_1cm_d','Tsoil_29_4cm_d','Tsoil_30_6cm_d','Tsoil_31_9cm_d'
    ]

def merge_datasets_with_different_variables(ds_list, dim='time'):
    """Take a list of datasets and merge them using xr.merge. First check that the two datasets
    have the same data vars. If they do not, missing data vars in each dataset are added with nan values
    so that the two datasets have the same set of data vars. NOTE: This gets slow with lots of datasets

    Args:
        ds_list (list(xr.Dataset)): list of xr.Dataset objects to merge.
        dim (string): dimension to merge datasets on. You probably want the default. Defaults to 'time'.
    Returns:
        xr.Dataset: Merged dataset.
    """
    def _merge_datasets_with_different_variables(ds1, ds2, dim):
        vars1 = set(ds1.data_vars)
        vars2 = set(ds2.data_vars)
        in1_notin2 = vars1.difference(vars2)
        in2_notin1 = vars2.difference(vars1)
        # add vars with NaN values to ds1
        for v in in2_notin1:
            ds1[v] = xr.DataArray(coords=ds1.coords, dims=ds1.dims)
        # add vars with NaN values to ds2
        for v in in1_notin2:
            ds2[v] = xr.DataArray(coords=ds2.coords, dims=ds2.dims)
        return xr.concat([ds1, ds2], dim=dim)

    new_ds = ds_list.pop(0)
    while ds_list:
        new_ds = _merge_datasets_with_different_variables(
            new_ds,
            ds_list.pop(0),
            dim=dim
        )
    return new_ds

def fill_missing_timestamps(ds):
    """Fills in missing timestamps in an xr.Dataset for all data variables with NaN values. This is
    particularly useful when multiple daily NetCDF files have been merged together. SoS NetCDF files 
    generally have data every 5 minutes. If data is missing from the beginning or end of the day,
    there may be missing timestamps (e.g. if the power was out at the study site between 12am and 2am
    on a day, the first timestamp in the dataset will be 02:02:30). This can be confusing when we want 
    to combine datasets from different days. This function fills in all missing timestamps between the 
    first timestamp and the last timestamp in the provided xr.Dataset. It makes sure there is one 
    timestamp every 5 minutes. 

    Args:
        ds (xr.Dataset): Dataset to be filled. 
    """
    def date_range(start_date, end_date, increment, period):
        result = []
        nxt = start_date
        delta = relativedelta(**{period:increment})
        while nxt <= end_date:
            result.append(nxt)
            nxt += delta

        return result
    dt_list = date_range(pd.to_datetime(ds.time.values[0]), pd.to_datetime(ds.time.values[-1]), 5, 'minutes')
    ds = ds.drop_duplicates(dim='time').reindex(time=dt_list)

    return ds

def download_sos_data_day(date = '20221101', local_download_dir = 'sosnoqc', cache=False,  planar_fit = False):
    """Download a netcdf file from the ftp url provided by the Earth Observing Laboratory at NCAR.
    Data is the daily data reynolds averaged to 5 minutes.

    Args:
        date (str, optional): Date to download data. in format '%Y%m%d', i.e. 20230101 for Jan 1, 2023. Defaults 
                to '20221101'.
        local_download_dir (str, optional): Directory to which files will be downloaded. Defaults to 'sosnoqc'; 
                this directory will be created if it does not already exist.
        cache (bool, optional): Whether or not to check the local_download_dir for the requested dataset. 
                If the file is already there, does not download it. Defaults to False.
        planar_fit (bool, optional): Whether or not to download data that has been planar fit by NCAR. These 
                datasets are not available for all dates. Defaults to False.

    Returns:
        str: file path to the downloaded file
    """
    base_url = 'ftp.eol.ucar.edu'
    if planar_fit:
        path = 'pub/archive/isfs/projects/SOS/netcdf/noqc_geo_tiltcor/'
    else:
        path = 'pub/archive/isfs/projects/SOS/netcdf/noqc_geo'
    
    if planar_fit:
        file_example =  f'isfs_sos_tiltcor_{date}.nc'

    else:
        file_example = f'isfs_{date}.nc'

    os.makedirs(local_download_dir, exist_ok=True)

    full_file_path = os.path.join('ftp://', base_url, path, file_example)
    if planar_fit:
        download_file_path = os.path.join(local_download_dir, 'planar_fit', file_example)
    else:
        download_file_path = os.path.join(local_download_dir, file_example)
    

    if cache and os.path.isfile(download_file_path):
        print(f"Caching...skipping download for {date}")
    else:
        urllib.request.urlretrieve(
            full_file_path,
            download_file_path   
        )

    return download_file_path

def download_sos_data(
    start_date,
    end_date,
    variable_names,
    local_download_dir = 'sosnoqc',
    cache = False,
    planar_fit = False
):
    """Download SoS datasets and perform a few preprocessing steps to clean up the data. 
    SoS datasets are NetCDF files from the ftp url provided by the Earth Observing Laboratory at NCAR.
    Data is the daily data reynolds averaged to 5 minutes. This function requires the caller to specify 
    the variables to be included in the output dataset because memory requirements are extensive if all 
    variables are included when merging datasets from many dates. 
    
    Specifically, this function:
    1. Downloads multiple netcdf files form the NCAR-EOL FTP server,
    2. Catches the URLERror thrown if the netcdf file for a specific date does not exist, and prints a 
        note that a failure occured,
    3. Merges the datasets into a single dataset, dealing with conflicts that arrise if some variables 
        are available in some datasets but not in others.
    4. Fills in missing timestamps so align with the 5 minute index that the datasets come in. 
        Timestamps may be missing in a single day's dataset if data-loss occured at the beginning 
        or end of a day.

    Args:
        start_date (str): first date to download data. in format '%Y%m%d', i.e. 20230101 for Jan 1, 2023/
        end_date (str): last date to download data. in format '%Y%m%d'.
        variable_names (list(str)): List of strings that represent NetCDF variable names to include in the
                combined dataset.
        local_download_dir (str, optional): Directory to which files will be downloaded. Defaults to 'sosnoqc'; 
                this directory will be created if it does not already exist.
        cache (bool, optional): Whether or not to check the local_download_dir for the requested dataset. 
                If the file is already there, does not download it. Defaults to False.
        planar_fit (bool, optional): Whether or not to download data that has been planar fit by NCAR. These 
                datasets are not available for all dates. Defaults to False.
    Returns:
        xr.Dataset: Merged and cleaned dataset with specified data variables between specified dates.
    """
    datelist = pd.date_range(
        dt.datetime.strptime(start_date, '%Y%m%d'),
        dt.datetime.strptime(end_date, '%Y%m%d'),
        freq='d'
    ).strftime('%Y%m%d').tolist()

    # We make sure that we aren't accessing variables that don't exist in the datasets
    # This is necessary because some daily NetCDF files don't have all the expected variables
    # (for example because an instrument was down). In that case, we want to add that variable
    # to the dataset, filled with nans, which sosmerge_datasets_with_different_variables
    # handles for us
    datasets = []
    for date in datelist:
        try:
            ds = xr.open_dataset(download_sos_data_day(date, local_download_dir, cache=cache, planar_fit=planar_fit))
        # Some dates are missing
        except URLError:
            print(f"failed on {date}, skipping")
        ds_new = ds[set(ds.data_vars).intersection(variable_names)]
        datasets.append(ds_new)
        
    sos_ds = merge_datasets_with_different_variables(datasets, dim='time')
    sos_ds = fill_missing_timestamps(sos_ds)
    return sos_ds

if __name__ == "__main__":
    # set up the data directory
    DATE_FORMAT_STR = '%Y%m%d'
    start_date = '20221130'
    # end_date = '20230509'
    end_date = '20230619'
    PLANAR_FIT = False

    datelist = pd.date_range(
        dt.datetime.strptime(start_date, DATE_FORMAT_STR),
        dt.datetime.strptime(end_date, DATE_FORMAT_STR),
        freq='d'
    ).strftime(DATE_FORMAT_STR).tolist()
    
    # Let's begin by downloading the SOS data and storing it in the /storage/ directory
    storage_dir = '/storage/dlhogan/precipitation-rodeo/data/raw/SOS/ISFS/'
    output_dir = '/storage/dlhogan/precipitation-rodeo/data/processed/SOS/'
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    # download data if storage dir is empty
    if os.listdir(storage_dir) == []:
        print("Downloading no qc data")
        sos_5min_ds = download_sos_data(
                                start_date=start_date,
                                end_date=end_date,
                                variable_names=VARIABLE_NAMES,
                                local_download_dir=storage_dir,
                                cache=True
                            );  
    else:
        if not os.path.exists(f"{output_dir}sos_ds_all_storage.nc"):
            print("Creating qc'd data file...")
            start = time.time()
            all_file_paths = [
            os.path.join(
                storage_dir,
                f'isfs_sos_qc_geo_tiltcor_5min_{date}.nc'
            ) for date in datelist
            ]
            datasets = []
            for i,file in enumerate(all_file_paths):
                ds = xr.open_dataset(file)
                # this ensures we don't access variables that aren't in this dataset, which would throw an error
                ds_new = ds[set(ds.data_vars).intersection(VARIABLE_NAMES)] # variables.DEFAULT_VARIABLES+hf.WATER_VAPOR_VARIABLES+hf.COUNT_VARIABLES
                datasets.append(ds_new)
                # for every 10th file, print the time between
                if i % 10 == 0:
                    print(f"Time elapsed for 10 files: {time.time()-start}")
                    start = time.time()
            sos_ds = xr.concat(datasets, dim='time')
            # ensure time index is evernly spaced by filling in missing times
            sos_ds = fill_missing_timestamps(sos_ds)
            sos_ds.to_netcdf(f"{output_dir}sos_ds_all_storage.nc")
            print(f"Download processed SOS data to {output_dir}sos_ds_all_storage.nc")
            sos_ds.close()