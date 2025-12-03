import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from utils import process_sail_data

DATA_PATH = '/storage/dlhogan/precipitation-rodeo/data/processed/'

if __name__ == "__main__":
    # Load datasets from Gothic
    billy_barr_ds = xr.open_dataset(f'{DATA_PATH}billy_barr/billy_barr_20211001-20230930_30min.nc')[['precip','precip_bad_flag','precip_missing_flag',]].sortby('time')

    # rename flags to qc_missing_billy_barr_precip and qc_bad_billy_barr_precip
    billy_barr_ds = billy_barr_ds.rename({'precip_missing_flag':'qc_missing_billy_barr_precip',
                                        'precip_bad_flag':'qc_bad_billy_barr_precip'})
    sail_ld_ds = xr.open_dataset(f'{DATA_PATH}SAIL/laser_disdrometer_gothic_processed_30min.nc')[['precip_accum_uncorrected','precip_accum_holyroyd',
                                                                                                'precip_accum_brandes','precip_accum_heymsfield',
                                                                                                'precip_missing_flag','precip_bad_flag',]].sortby('time')
    # rename variables to qc_missing_sail_ld and qc_bad_sail_ld
    sail_ld_ds = sail_ld_ds.rename({'precip_missing_flag':'qc_missing_sail_ld_uncorrected',
                                    'precip_bad_flag':'qc_bad_sail_ld_uncorrected'})
    sail_pluvio_ds = xr.open_dataset(f'{DATA_PATH}SAIL/pluvio_30min.nc')[['accum_nrt','pluvio_missing_flag','pluvio_bad_flag',]].sortby('time')
    # rename variables to qc_missing_sail_pluvio and qc_bad_sail_pluvio
    sail_pluvio_ds = sail_pluvio_ds.rename({'pluvio_missing_flag':'qc_missing_sail_pluvio',
                                            'pluvio_bad_flag':'qc_bad_sail_pluvio'})
    # load sail met dataset
    sail_met_ds = xr.open_dataset(f'{DATA_PATH}SAIL/met_30min.nc').sortby('time')

    # drop all vars in sail_met_ds not in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] 
    met_prcp_vars = [var for var in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if var in sail_met_ds.data_vars]
    # add qc variables for sail_met_ds
    met_prcp_vars += [f'{var}_missing_flag' for var in ['org_precip', 'pwd_precip', 'tbrg_precip']]
    met_prcp_vars += [f'{var}_bad_flag' for var in ['org_precip', 'pwd_precip', 'tbrg_precip']]
    sail_met_ds = sail_met_ds[met_prcp_vars]
    # rename sail met qc variables
    sail_met_ds = sail_met_ds.rename({'org_precip_missing_flag':'qc_missing_sail_org',
                                        'org_precip_bad_flag':'qc_bad_sail_org',
                                        'pwd_precip_missing_flag':'qc_missing_sail_pwd',
                                        'pwd_precip_bad_flag':'qc_bad_sail_pwd',
                                        'tbrg_precip_missing_flag':'qc_missing_sail_tbg',
                                        'tbrg_precip_bad_flag':'qc_bad_sail_tbg',})
    # load squire dataset 
    sail_squire_ds = xr.open_dataset(f'{DATA_PATH}SAIL/squire_30min.nc').sel(site='gothic').sortby('time').squeeze()
    squire_prcp_vars = [var for var in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if var in sail_squire_ds.data_vars]
    squire_prcp_vars += ["squire_missing_flag","squire_bad_flag"]
    sail_squire_ds = sail_squire_ds[squire_prcp_vars].drop_vars(['lat','lon','x','y','site'])
    # rename sail squire qc variables
    sail_squire_ds = sail_squire_ds.rename({'squire_missing_flag':'qc_missing_sail_squire_m2009_1',
                                            'squire_bad_flag':'qc_bad_sail_squire_m2009_1'})

    print('Merging datasets...')
    try:
        gothic_combined_ds = xr.merge([billy_barr_ds, sail_ld_ds, sail_pluvio_ds, sail_met_ds, sail_squire_ds], compat='override')
        print('Datasets merged successfully.')
    except Exception as e:
        print(f'Error merging datasets: {e}')

    # rename the variables
    # rename the variables
    variable_renames = {
        'precip': 'billy_barr_precip',
        'precip_accum_uncorrected': 'sail_ld_uncorrected',
        'precip_accum_holyroyd':'sail_ld_holyroyd',
        'precip_accum_brandes':'sail_ld_brandes',
        'precip_accum_heymsfield':'sail_ld_heymsfield',
        'accum_nrt':'sail_pluvio',
        'pwd_precip_total':'sail_pwd',
        'tbrg_precip_total':'sail_tbg',
        'org_precip_accum':'sail_org',
        'snow_rate_m2009_1_total':'sail_squire_m2009_1',
        'snow_rate_m2009_2_total':'sail_squire_m2009_2',
        'snow_rate_ws88diw_total':'sail_squire_ws88diw',
        'snow_rate_ws2012_total':'sail_squire_ws2012'
    }

    gothic_combined_ds = gothic_combined_ds.rename(variable_renames)

    # remove any obvious bad data: negative precipitation values or 30 minute precipitation > 50 mm
    MIN = 0.05  # minimum measurable precipitation
    MAX = 28.44  # 100-year event over 30 minutes
    for var in gothic_combined_ds.data_vars:
        if var == 'billy_barr_precip':
            gothic_combined_ds[var] = gothic_combined_ds[var].where((gothic_combined_ds[var] <= 15), np.nan)
            gothic_combined_ds[var] = gothic_combined_ds[var].where(gothic_combined_ds[var] >= MIN, 0)
        else:
            gothic_combined_ds[var] = gothic_combined_ds[var].where((gothic_combined_ds[var] <= MAX), np.nan)
            gothic_combined_ds[var] = gothic_combined_ds[var].where(gothic_combined_ds[var] >= MIN, 0)
    print('Bad data removed.')
    # update all bad flags to 1 where data is nan
    for var in gothic_combined_ds.data_vars:
        if 'qc_bad' in var:
            data_var = var.replace('qc_bad_', '')
            gothic_combined_ds[var] = gothic_combined_ds[var].where(~gothic_combined_ds[data_var].isnull(), 1)
    # convert all qc variables to int
    for var in gothic_combined_ds.data_vars:    
        if 'qc_' in var:
            gothic_combined_ds[var] = gothic_combined_ds[var].astype(int)

    # Save the merged dataset
    output_filepath = f'{DATA_PATH}final/gothic_precipitation_30min_with_flags.nc'
    try:
        gothic_combined_ds.to_netcdf(output_filepath)
        print(f'Merged dataset saved to {output_filepath}')
    except Exception as e:
        print(f'Error saving merged dataset: {e}')
    gothic_combined_ds.close()