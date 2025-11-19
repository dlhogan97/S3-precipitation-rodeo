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
    billy_barr_ds = xr.open_dataset(f'{DATA_PATH}billy_barr/billy_barr_20211001-20230930_30min.nc')['precip'].to_dataset().sortby('time')
    sail_ld_ds = xr.open_dataset(f'{DATA_PATH}SAIL/laser_disdrometer_gothic_processed_30min.nc')[['precip_accum_uncorrected','precip_accum_holyroyd',
                                                                                                'precip_accum_brandes','precip_accum_heymsfield',]].sortby('time')
    sail_pluvio_ds = xr.open_dataset(f'{DATA_PATH}SAIL/pluvio_30min.nc')['accum_nrt'].to_dataset().sortby('time')
    sail_met_ds = xr.open_dataset(f'{DATA_PATH}SAIL/met_30min.nc').sortby('time')
    sail_squire_ds = xr.open_dataset(f'{DATA_PATH}SAIL/squire_30min.nc').sel(site='gothic').sortby('time').squeeze()

    # drop all vars in sail_met_ds not in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] 
    met_prcp_vars = [var for var in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if var in sail_met_ds.data_vars]
    squire_prcp_vars = [var for var in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if var in sail_squire_ds.data_vars]
    sail_squire_ds = sail_squire_ds[squire_prcp_vars].drop_vars(['lat','lon','x','y','site'])
    sail_met_ds = sail_met_ds[met_prcp_vars]

    print('Merging datasets...')
    try:
        gothic_combined_ds = xr.merge([billy_barr_ds, sail_ld_ds, sail_pluvio_ds, sail_met_ds, sail_squire_ds], compat='override')
        print('Datasets merged successfully.')
    except Exception as e:
        print(f'Error merging datasets: {e}')

    # rename the variables
    variable_renames = {
        'precip': 'billy_barr_precip',
        'precip_accum_uncorrected': 'sail_ld_uncorrected',
        'precip_accum_holyroyd':'sail_ld_holyroyd',
        'precip_accum_brandes':'sail_ld_brandes',
        'precip_accum_heymsfield':'sail_ld_heymsfield',
        'accum_nrt':'sail_pluvio',
        'pwd_cumul_rain':'sail_pwd_rain',
        'tbrg_precip_total':'sail_tbg',
        'tbrg_precip_total_corr':'sail_tbg_corr',
        'org_precip_accum':'sail_org',
        'rain_rate_A_total':'sail_squire_rain',
        'snow_rate_m2009_1_total':'sail_squire_snow_m2009_1',
        'snow_rate_m2009_2_total':'sail_squire_snow_m2009_2',
        'snow_rate_ws88diw_total':'sail_squire_snow_ws88diw',
        'snow_rate_ws2012_total':'sail_squire_snow_ws2012'
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

    # Save the merged dataset
    output_filepath = f'{DATA_PATH}final/gothic_precipitation_30min.nc'
    try:
        gothic_combined_ds.to_netcdf(output_filepath)
        print(f'Merged dataset saved to {output_filepath}')
    except Exception as e:
        print(f'Error saving merged dataset: {e}')
    gothic_combined_ds.close()