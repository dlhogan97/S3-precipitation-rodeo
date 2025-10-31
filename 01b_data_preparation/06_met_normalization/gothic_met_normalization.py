import glob
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
import pandas as pd
from utils.helper_funcs import calculate_wind_components
import numpy as np
import xarray as xr

def standardize_variables(df,bias_adj=None):
    # standardize the variables
    daily_means = df.groupby('doy').mean()
    daily_stds = df.groupby('doy').std()

    if bias_adj:
        for col, (scale, offset) in bias_adj.items():
            print(f"Applying bias adjustment to {col}: scale={scale}, offset={offset}")
            daily_means[col] = (daily_means[col] * scale) + offset
            daily_stds[col] = daily_stds[col] * scale
        daily_means['wind_speed'] = np.sqrt(daily_means['u_wind']**2 + daily_means['v_wind']**2)
        daily_stds['wind_speed'] = np.sqrt(daily_stds['u_wind']**2 + daily_stds['v_wind']**2)
    return daily_means, daily_stds

# subtract the daily climatological means
def anom_df(df, daily_means):
    df_anom = df.copy()
    for var in ['pressure', 'temperature', 'relative_humidity', 'u_wind', 'v_wind', 'wind_speed']:
        df_anom[var] = df_anom[var] - df_anom['doy'].map(daily_means[var])
    return df_anom

def compute_deltaD(df, anom_df, window_size):
    if not window_size:
        deltaD = anom_df.groupby('tod').mean()
    else:
        deltaD_raw = anom_df.groupby(['doy', 'tod']).mean().unstack()
        # add in wind speed climatology
        deltaD = deltaD_raw.rolling(window_size, center=True, min_periods=1).mean()
    # name the index levels to be variable and time of day
    deltaD.columns.set_names(['variable', 'tod'], inplace=True)
    # Stack the DataFrame for easier lookup
    deltaD_stacked = (
        deltaD
        .stack(level=1, future_stack=True)  # stack 'tod'
        .stack(level=0, future_stack=True)  # stack 'var'
    .to_frame('delta_d')
    )
    # create a multiindex from lookup made of day of year and time of day
    lookup_index = pd.MultiIndex.from_arrays(
    [df['doy'], df['tod']],
    names=['doy', 'tod']
    )

    # Broadcast lookup for all variables
    delta_vals = {
        var: deltaD_stacked.xs(var, level='variable').reindex(lookup_index).to_numpy().flatten()
        for var in ['pressure', 'temperature', 'relative_humidity', 'u_wind', 'v_wind', 'wind_speed']
    }

    # Build a DataFrame
    deltaD_df = pd.DataFrame(delta_vals, index=df.index)
    return deltaD_df

def calc_normalized_df(df, deltaD_df, daily_means, daily_stds):
    doy = df['doy']
    mu_daily = pd.DataFrame({v: doy.map(daily_means[v]) for v in deltaD_df.columns})
    sigma_daily = pd.DataFrame({v: doy.map(daily_stds[v]) for v in deltaD_df.columns})

    mu_clim = mu_daily + deltaD_df
    normalized_df = (df[deltaD_df.columns] - mu_clim) / sigma_daily
    return normalized_df

if __name__ == "__main__":
    DATA_DIR = "/storage/dlhogan/precipitation-rodeo/data"
    OUTPUT_DIR = f"{DATA_DIR}/processed/final"
    WINDOW_SIZE = 7  # days
    # Define constants
    billyBarrLongTermMet_ds = xr.open_dataset(f"{DATA_DIR}/processed/billy_barr/billy_barr_20011001-20251025_30min.nc")
    gothicMet_ds = xr.open_dataset(f"{DATA_DIR}/processed/SAIL/met_30min.nc")
    gothicBB_ds = xr.open_dataset(f"{DATA_DIR}/processed/billy_barr/billy_barr_20211001-20230930_30min.nc")

    # first separate wind speed and direction into u and v components
    u, v = calculate_wind_components(
        billyBarrLongTermMet_ds['windSpeed'],
        billyBarrLongTermMet_ds['windDirec']
    )
    billyBarrLongTermMet_ds['u_wind'] = (('time',), u)
    billyBarrLongTermMet_ds['v_wind'] = (('time',), v)

    u, v = calculate_wind_components(
        gothicBB_ds['windSpeed'],
        gothicBB_ds['windDirec']
    )
    gothicBB_ds['u_wind'] = (('time',), u)
    gothicBB_ds['v_wind'] = (('time',), v)


    VARS =['pressure', 'temperature', 'relative_humidity', 'u_wind', 'v_wind']

    # normalize the variable names
    gothicMet_ds = gothicMet_ds.rename({
        'atmos_pressure': 'pressure',
        'temp_mean': 'temperature',
        'rh_mean': 'relative_humidity',
        'wspd_vec_mean': 'wind_speed',
        'u': 'u_wind',
        'v': 'v_wind',
    })
    gothicBB_ds = gothicBB_ds.rename({
        'baromPress': 'pressure',
        'avAirTemp': 'temperature',
        'relHumidty': 'relative_humidity',
        'u_wind': 'u_wind',
        'v_wind': 'v_wind',
    })
    billyBarrLongTermMet_ds = billyBarrLongTermMet_ds.rename({
        'baromPress': 'pressure',
        'avAirTemp': 'temperature',
        'relHumidty': 'relative_humidity',
        'u_wind': 'u_wind',
        'v_wind': 'v_wind',
    })

    # subset datasets to only these variables
    gothicMet_df = gothicMet_ds[VARS].to_dataframe()
    gothicBB_df = gothicBB_ds[VARS].to_dataframe()

    # long term meteorology
    billyBarrLongTermMet_df = billyBarrLongTermMet_ds[VARS].to_dataframe()

        # add doy and tod volumns
    for df in [gothicMet_df, gothicBB_df]:
        df['doy'] = df.index.dayofyear
        df['tod'] = df.index.time
        df['wind_speed'] = np.sqrt(df['u_wind']**2 + df['v_wind']**2)

    billyBarrLongTermMet_df['doy'] = billyBarrLongTermMet_df.index.dayofyear

    bias_adj_dict = {
        'gothicMet': {
        'u_wind': [1.99, 0.78],
        'v_wind': [1.72, -0.62],
        },
        'gothicBB': {
            'u_wind': [1, 0],
            'v_wind': [1, 0],
        },
    }

    for site_name, site_df in zip(
        ['gothicMet', 'gothicBB'],
        [gothicMet_df, gothicBB_df]
    ):
        print(f"Processing site: {site_name}")
        print("Standardizing variables...")
        daily_means, daily_stds = standardize_variables(
            billyBarrLongTermMet_df,
            bias_adj=bias_adj_dict[site_name]
        )
        print("Calculating anomalies...")
        site_anom_df = anom_df(site_df, daily_means)
        print("Computing diurnal delta...")
        site_deltaD = compute_deltaD(site_df, site_anom_df, WINDOW_SIZE)
        print("Calculating normalized dataframe...")
        site_normalized = calc_normalized_df(site_df, site_deltaD, daily_means, daily_stds)
        site_normalized_ds = site_normalized.to_xarray()

        # add units and long_name attributes back
        for var in site_normalized[VARS].columns:
            site_normalized[var].attrs = site_df[var].attrs
        site_normalized['wind_speed'].attrs = {'units': 'm/s', 'long_name': 'Wind Speed'}
        # save to netcdf
        site_normalized_ds.to_netcdf(f"{OUTPUT_DIR}/{site_name}_met_normalized.nc")
        site_normalized_ds.close()
        print(f"Saved normalized data to {OUTPUT_DIR}/{site_name}_met_normalized.nc")