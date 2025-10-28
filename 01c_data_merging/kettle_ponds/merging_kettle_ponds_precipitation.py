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
    # Load datasets from Kettle p\Ponds
    billy_barr_ds = xr.open_dataset(f'{DATA_PATH}billy_barr/billy_barr_20211001_20230930_30min.nc')[['precip']]
    splash_lpdf_ds = xr.open_dataset(f'{DATA_PATH}SPLASH/lpdf_gauge_30min.nc')
    splash_ld_ds = xr.open_dataset(f'{DATA_PATH}SPLASH/SPLASH_kp_laser_disdrometer_30min.nc')[['Amount']]
    sos_ds = xr.open_dataset(f'{DATA_PATH}SOS/sos_ds_30min.nc')[['SWE_p1_c_max_accum','SWE_p2_c_max_accum',
                                                                'SWE_p3_c_max_accum','SWE_p4_c_max_accum',]]
    sail_squire_ds = xr.open_dataset(f'{DATA_PATH}SAIL/squire_30min.nc').sel(site='kettle_ponds').sortby('time').squeeze()
    
    # merge kettle ponds datasets
    print('Merging Kettle Ponds datasets...')
    try:
        kettle_ponds_combined_ds = xr.merge([billy_barr_ds, splash_lpdf_ds, splash_ld_ds, sos_ds, sail_squire_ds], compat='override').sortby('time')
        print('Kettle Ponds datasets merged successfully.')
    except Exception as e:
        print(f'Error merging Kettle Ponds datasets: {e}')

    variable_renames_kp = {
                            'precip': 'billy_barr_precip',
                            'prcp':'splash_lpdf',
                            'Amount': 'splash_ld_uncorrected',
                            'SWE_p1_c_max_accum': "sos_swe_p1",
                            'SWE_p2_c_max_accum': "sos_swe_p2",
                            'SWE_p3_c_max_accum': "sos_swe_p3",
                            'SWE_p4_c_max_accum': "sos_swe_p4",
                            'rain_rate_A_total':'sail_squire_rain',
                            'snow_rate_m2009_1_total':'sail_squire_snow_m2009_1',
                            'snow_rate_m2009_2_total':'sail_squire_snow_m2009_2',
                            'snow_rate_ws88diw_total':'sail_squire_snow_ws88diw',
                            'snow_rate_ws2012_total':'sail_squire_snow_ws2012'
                          }

    kettle_ponds_combined_ds = kettle_ponds_combined_ds.rename(variable_renames_kp)

    # remove any obvious bad data: negative precipitation values or 30 minute precipitation > 50 mm
    for var in kettle_ponds_combined_ds.data_vars:
        kettle_ponds_combined_ds[var] = kettle_ponds_combined_ds[var].where((kettle_ponds_combined_ds[var] >= 0) & (kettle_ponds_combined_ds[var] <= 50), np.nan)
    print('Bad data removed.')

    # Save the merged dataset
    output_filepath = f'{DATA_PATH}final/kettle_ponds_precipitation_30min.nc'
    kettle_ponds_combined_ds.to_netcdf(output_filepath)
    print(f'Merged dataset saved to {output_filepath}')
    kettle_ponds_combined_ds.close()