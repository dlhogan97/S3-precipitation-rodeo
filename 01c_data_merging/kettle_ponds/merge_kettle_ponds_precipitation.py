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
    # Load billy barr data
    billy_barr_ds = xr.open_dataset(f'{DATA_PATH}billy_barr/billy_barr_20211001-20230930_30min.nc')[['precip','precip_bad_flag','precip_missing_flag',]].sortby('time')
    # rename flags to qc_missing_billy_barr_precip and qc_bad_billy_barr_precip
    billy_barr_ds = billy_barr_ds.rename({'precip_missing_flag':'qc_missing_billy_barr_precip',
                                        'precip_bad_flag':'qc_bad_billy_barr_precip'})
    
    # splash pluvio data
    splash_lpdf_ds = xr.open_dataset(f'{DATA_PATH}SPLASH/lpdf_gauge_30min.nc')
    # rename qc_variables
    splash_lpdf_ds = splash_lpdf_ds.rename({'qc_missing_precip':'qc_missing_splash_pluvio',
                                            'qc_bad_precip':'qc_bad_splash_pluvio'})
    # laser disdrometer data
    splash_ld_ds = xr.open_dataset(f'{DATA_PATH}SPLASH/SPLASH_kp_laser_disdrometer_30min.nc')[['Amount', 'qc_missing_precip', 'qc_bad_precip',
                                                                                               'precip_accum_holyroyd', 
                                                                                               'precip_accum_brandes', 
                                                                                               'precip_accum_heymsfield',
                                                                                               'precip_type']]
    # rename qc variables
    splash_ld_ds = splash_ld_ds.rename({'qc_missing_precip':'qc_missing_splash_ld_uncorrected',
                                        'qc_bad_precip':'qc_bad_splash_ld_uncorrected'})
    # sos data
    sos_ds = xr.open_dataset(f'{DATA_PATH}SOS/sos_ds_30min.nc')[['SWE_p1_c_max_accum', 'qc_SWE_p1_c_missing', 'qc_SWE_p1_c_bad',
                                                                'SWE_p2_c_max_accum', 'qc_SWE_p2_c_missing', 'qc_SWE_p2_c_bad',
                                                                'SWE_p3_c_max_accum', 'qc_SWE_p3_c_missing', 'qc_SWE_p3_c_bad',
                                                                'SWE_p4_c_max_accum', 'qc_SWE_p4_c_missing', 'qc_SWE_p4_c_bad']].sortby('time')
    # rename qc variable to remove _c
    sos_ds = sos_ds.rename({var: var.replace('_c_', '_') for var in sos_ds.data_vars if 'qc_' in var})
    # for non-qc variables, take the difference between time steps to get precipitation
    for var in ['SWE_p1_c_max_accum', 'SWE_p2_c_max_accum', 'SWE_p3_c_max_accum', 'SWE_p4_c_max_accum']:
        sos_ds[var] = sos_ds[var].diff(dim='time').fillna(0)

    # sail squire data
    sail_squire_ds = xr.open_dataset(f'{DATA_PATH}SAIL/squire_30min.nc').sel(site='kettle_ponds').sortby('time').squeeze()
    squire_prcp_vars = [var for var in process_sail_data.SAIL_PRECIPITATION_VARS['cumulative'] if var in sail_squire_ds.data_vars]
    squire_prcp_vars += ["squire_missing_flag","squire_bad_flag"]
    sail_squire_ds = sail_squire_ds[squire_prcp_vars].drop_vars(['lat','lon','x','y','site'])
    # rename sail squire qc variables
    sail_squire_ds = sail_squire_ds.rename({'squire_missing_flag':'qc_missing_sail_squire_m2009_1',
                                            'squire_bad_flag':'qc_bad_sail_squire_m2009_1'})
    
    MIN = 0.05  # minimum measurable precipitation
    MAX = 28.44  # 100-year event over 30 minutes
    # merge kettle ponds datasets
    print('Merging Kettle Ponds datasets...')
    try:
        kettle_ponds_combined_ds = xr.merge([billy_barr_ds, splash_lpdf_ds, splash_ld_ds, sos_ds, sail_squire_ds], compat='override').sortby('time')
        print('Kettle Ponds datasets merged successfully.')
    except Exception as e:
        print(f'Error merging Kettle Ponds datasets: {e}')

    variable_renames_kp = {
                            'precip': 'billy_barr_precip',
                            'prcp':'splash_pluvio',
                            'Amount': 'splash_ld_uncorrected',
                            'precip_accum_holyroyd' : 'splash_ld_holyroyd',
                            'precip_accum_brandes':'splash_ld_brandes',
                            'precip_accum_heymsfield':'splash_ld_heymsfield',
                            'SWE_p1_c_max_accum': "sos_SWE_p1",
                            'SWE_p2_c_max_accum': "sos_SWE_p2",
                            'SWE_p3_c_max_accum': "sos_SWE_p3",
                            'SWE_p4_c_max_accum': "sos_SWE_p4",
                            'snow_rate_m2009_1_total':'sail_squire_m2009_1',
                            'snow_rate_m2009_2_total':'sail_squire_m2009_2',
                            'snow_rate_ws88diw_total':'sail_squire_ws88diw',
                            'snow_rate_ws2012_total':'sail_squire_ws2012'
                          }

    kettle_ponds_combined_ds = kettle_ponds_combined_ds.rename(variable_renames_kp)
    # drop the following
    to_drop = ['snow_rate_ws88diw','snow_rate_m2009_1','snow_rate_m2009_2','snow_rate_ws2012','sail_squire_rain']
    kettle_ponds_combined_ds = kettle_ponds_combined_ds.drop_vars(to_drop, errors='ignore')

    # remove any obvious bad data: negative precipitation values or 30 minute precipitation > 28.44 mm (100-year event over 30 minutes)
    for var in kettle_ponds_combined_ds.data_vars:
        kettle_ponds_combined_ds[var] = kettle_ponds_combined_ds[var].where((kettle_ponds_combined_ds[var] <= MAX), np.nan)
        kettle_ponds_combined_ds[var] = kettle_ponds_combined_ds[var].where(kettle_ponds_combined_ds[var] >= MIN, 0)
    print('Maximum and minimum thresholds applied to Kettle Ponds data.')

    # update all bad flags to 1 where data is nan
    for var in kettle_ponds_combined_ds.data_vars:
        if 'qc_bad' in var:
            data_var = var.replace('qc_bad_', '')
            kettle_ponds_combined_ds[var] = kettle_ponds_combined_ds[var].where(~kettle_ponds_combined_ds[data_var].isnull(), 1)
    # convert all qc variables to int
    for var in kettle_ponds_combined_ds.data_vars:    
        if 'qc_' in var:
            kettle_ponds_combined_ds[var] = kettle_ponds_combined_ds[var].astype(int)

    # Save the merged dataset
    output_filepath = f'{DATA_PATH}final/kettle_ponds_precipitation_30min.nc'
    try:
        kettle_ponds_combined_ds.to_netcdf(output_filepath)
        print(f'Merged dataset saved to {output_filepath}')
    except Exception as e:
        print(f'Error saving merged dataset: {e}')
    kettle_ponds_combined_ds.close()