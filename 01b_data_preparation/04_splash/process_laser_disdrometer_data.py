import numpy as np
import pandas as pd
from scipy import stats
import xarray as xr
import glob
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from datetime import datetime

# -------------------------------------------------------
# Bin coordinate definitions
# -------------------------------------------------------
size_bins = np.array([
    0.062,0.187,0.312,0.437,0.562,0.687,0.812,0.937,
    1.062,1.187,1.375,1.625,1.875,2.125,2.375,2.750,
    3.250,3.750,4.250,4.750,5.500,6.500,7.500,8.500,
    9.500,11.000,13.000,15.000,17.000,19.000,21.500,24.500
])

velocity_bins = np.array([
    0.05,0.15,0.25,0.35,0.45,0.55,0.65,0.75,
    0.85,0.95,1.10,1.30,1.50,1.70,1.90,2.20,
    2.60,3.00,3.40,3.80,4.40,5.80,6.00,6.80,
    7.60,8.80,10.40,12.00,13.60,15.20,17.60,20.80
])

# filepath = raw_files[100]
def process_raw_laser_disdrometer_file(filepath):
    # -------------------------------------------------------
    # 1. Read header (date + hour)
    # -------------------------------------------------------
    with open(filepath, "r") as f:
        header = f.readline().strip()

    date_str = header.split()[0]        # "09/30/2021"
    time_str = header.split()[1]        # "21:00"

    file_date = datetime.strptime(date_str, "%m/%d/%Y").date()
    file_hour = int(time_str.split(":")[0])  # 21

    # -------------------------------------------------------
    # 2. Prepare lists
    # -------------------------------------------------------
    times = []
    precip_intensity = []
    accum_precip = []
    reflectivity = []
    particle_count_tot = []
    sensor_status = []
    error_code = []

    Nd_list = []
    vd_list = []
    PSD_list = []

    # -------------------------------------------------------
    # 3. Line-by-line parsing
    # -------------------------------------------------------
    with open(filepath, "r") as f:
        lines = f.readlines()[1:]   # skip header

    for line in lines:
        parts = line.strip().split()

        # Whitespace-delimited fields 1–7
        timestamp_raw = parts[0]
        precip_intensity.append(float(parts[1]))
        accum_precip.append(float(parts[2]))
        reflectivity.append(float(parts[3]))
        particle_count_tot.append(int(parts[4]))
        sensor_status.append(int(parts[5]))
        error_code.append(int(parts[6]))

        # Time parsing
        MMSSmmm = timestamp_raw.zfill(7)
        minute = int(MMSSmmm[0:2])
        second = int(MMSSmmm[2:4])

        times.append(
            datetime(file_date.year, file_date.month, file_date.day,
                    file_hour, minute, second)
        )

        # Everything after the first 7 fields:
        rest = " ".join(parts[7:])

        # Split *only* on commas → yields Nd + vd + PSD values
        vals = rest.replace(" ", ",").split(",")

        # Convert to floats, treating missing as NaN
        missing_vals = {"", "-9999", "-9", "-9.9", "-9.99", "-9.999"}

        vals = [float(v) if v not in missing_vals else np.nan for v in vals]

        # Required: 32 + 32 + 1024 = 1088 values
        if len(vals) != 1088:
            # Sometimes trailing commas produce 1089; trim
            vals = vals[:1088]

        # Extract Nd, vd, PSD
        Nd = np.array(vals[0:32])
        vd = np.array(vals[32:64])
        PSD_flat = np.array(vals[64:1088])      # 1024 values
        PSD = PSD_flat.reshape(32, 32)          # (velocity, size)

        Nd_list.append(Nd)
        vd_list.append(vd)
        PSD_list.append(PSD)

    # Stack arrays
    Nd_arr = np.stack(Nd_list)
    vd_arr = np.stack(vd_list)
    PSD_arr = np.stack(PSD_list)   # (time, velocity, size)
    
    class_size_width = np.array([0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125,
       0.125, 0.25 , 0.25 , 0.25 , 0.25 , 0.25 , 0.5  , 0.5  , 0.5  ,
       0.5  , 0.5  , 1.   , 1.   , 1.   , 1.   , 1.   , 2.   , 2.   ,
       2.   , 2.   , 2.   , 3.   , 3.   ])
    class_size_width = np.tile(class_size_width, (len(times), 1))

    # -------------------------------------------------------
    # 5. Build dataset
    # -------------------------------------------------------
    ds = xr.Dataset(
        data_vars={
            "precip_intensity":   ("time", np.array(precip_intensity)),
            "accum_precip":       ("time", np.array(accum_precip)),
            "reflectivity":       ("time", np.array(reflectivity)),
            "particle_count_tot": ("time", np.array(particle_count_tot)),
            "sensor_status":      ("time", np.array(sensor_status)),
            "error_code":         ("time", np.array(error_code)),

            "Nd":  (("time", "size_bin"), Nd_arr),
            "vd":  (("time", "velocity_bin"), vd_arr),
            "class_size_width": (("time","size_bin"), class_size_width),
            "PSD": (("time", "velocity_bin", "size_bin"), PSD_arr),
        },

        coords={
            "time": np.array(times),
            "size_bin": size_bins,
            "velocity_bin": velocity_bins,
        }
    )
    ds = ds.where(ds != -9.999)
    return ds

def process_stats_laser_disdrometer_file(filepath, reasonable_threshold=None):
    """
    Process a laser disdrometer file and return a xarray Dataset with the data.
    """
    # Read the file
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    # Parse the header for metadata (assuming first line is header)
    header = lines[0].strip()
    instrument_info, time_info = header.split(" Time (YYJJJHH): ")
    year = int("20" + time_info[:2])
    day_of_year = int(time_info[2:5])
    start_hour = int(time_info[5:7])

    # Skip the first two lines
    lines = lines[2:]
    
    # Initialize a list to store parsed data
    data = []

    # Process each line in the file
    for i, line in enumerate(lines):
        fields = line.strip().split()
        
        # Skip lines that don't have the correct number of fields
        if len(fields) != 57:
            continue
        
        # Extract and convert data fields
        begin_time_str = fields[0].split('-')[0]
        end_time_str = fields[0].split('-')[1]
        particle_distribution = list(map(int, fields[1:33]))
        qc_data = list(map(int, fields[33:36]))
        precip_stats = list(map(float, fields[36:41]))
        laser_status = list(map(float, fields[41:47]))
        sensor_status = list(map(float, fields[47:53]))
        precip_partitioning = list(map(int, fields[53:57]))

        # Convert begin_time and end_time to timestamps
        begin_time = pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day_of_year-1, hours=start_hour,
                                                                minutes=int(begin_time_str[0:2]), 
                                                                seconds=int(begin_time_str[3:5]),
                                                                milliseconds=int(begin_time_str[6:9]))
        end_time = pd.Timestamp(year, 1, 1) + pd.Timedelta(days=day_of_year-1, hours=start_hour,
                                                                minutes=int(end_time_str[0:2]), 
                                                                seconds=int(end_time_str[3:5]), 
                                                                milliseconds=int(end_time_str[6:9]))
        # If end time is before begin time, increment the hour by 1
        if end_time < begin_time:
            end_time += pd.Timedelta(hours=1)
        
        data.append({
            "time": begin_time,
            "particle_distribution": particle_distribution,
            "qc_data": qc_data,
            "precip_stats": precip_stats,
            "laser_status": laser_status,
            "sensor_status": sensor_status,
            "precip_partitioning": precip_partitioning
        })
    
    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Ensure all lists in df have consistent lengths
    assert all(len(item) == 32 for item in df["particle_distribution"]), "Mismatch in particle_distribution length"
    assert all(len(item) == 3 for item in df["qc_data"]), "Mismatch in qc_data length"
    assert all(len(item) == 5 for item in df["precip_stats"]), "Mismatch in precip_stats length"
    assert all(len(item) == 6 for item in df["laser_status"]), "Mismatch in laser_status length"
    assert all(len(item) == 6 for item in df["sensor_status"]), "Mismatch in sensor_status length"
    assert all(len(item) == 4 for item in df["precip_partitioning"]), "Mismatch in precip_partitioning length"

    # Convert DataFrame to xarray Dataset
    ds = xr.Dataset(
        {
            "particle_distribution": (("time", "size_bin"), np.stack(df["particle_distribution"].values)),
            "Blackout": ("time", df["qc_data"].apply(lambda x: x[0])),
            "Good": ("time", df["qc_data"].apply(lambda x: x[1])),
            "Bad": ("time", df["qc_data"].apply(lambda x: x[2])),
            "NumParticle": ("time", df["precip_stats"].apply(lambda x: x[0])),
            "Rate": ("time", df["precip_stats"].apply(lambda x: x[1]), {"units": "mm/h", "descriptor": "Precipitation"}),
            "Amount": ("time", df["precip_stats"].apply(lambda x: x[2]), {"units": "mm", "descriptor": "Precipitation"}),
            "AmountSum": ("time", df["precip_stats"].apply(lambda x: x[3]), {"units": "mm", "descriptor": "Precipitation"}),
            "Z": ("time", df["precip_stats"].apply(lambda x: x[4]), {"units": "dB", "descriptor": "Precipitation"}),
            "NumError": ("time", df["laser_status"].apply(lambda x: x[0])),
            "Dirty": ("time", df["laser_status"].apply(lambda x: x[1])),
            "VeryDirty": ("time", df["laser_status"].apply(lambda x: x[2])),
            "Damaged": ("time", df["laser_status"].apply(lambda x: x[3])),
            "SignalAvg": ("time", df["laser_status"].apply(lambda x: x[4])),
            "SignalStdDev": ("time", df["laser_status"].apply(lambda x: x[5])),
            "TempAvg": ("time", df["sensor_status"].apply(lambda x: x[0]), {"units": "C", "descriptor": "Sensor Status"}),
            "TempStdDev": ("time", df["sensor_status"].apply(lambda x: x[1]), {"units": "C", "descriptor": "Sensor Status"}),
            "VoltAvg": ("time", df["sensor_status"].apply(lambda x: x[2]), {"units": "V", "descriptor": "Sensor Status"}),
            "VoltStdDev": ("time", df["sensor_status"].apply(lambda x: x[3]), {"units": "V", "descriptor": "Sensor Status"}),
            "HeatCurrentAvg": ("time", df["sensor_status"].apply(lambda x: x[4]), {"units": "A", "descriptor": "Sensor Status"}),
            "HeatCurrentStdDev": ("time", df["sensor_status"].apply(lambda x: x[5]), {"units": "A", "descriptor": "Sensor Status"}),
            "NumRain": ("time", df["precip_partitioning"].apply(lambda x: x[0])),
            "NumNoRain": ("time", df["precip_partitioning"].apply(lambda x: x[1])),
            "NumAmbig": ("time", df["precip_partitioning"].apply(lambda x: x[2])),
            "Type": ("time", df["precip_partitioning"].apply(lambda x: x[3]))
        },
        coords={
            "time": df["time"].values,
            "size_bin": size_bins,
        }
    )
    
    # apply reasonableness threshold
    if reasonable_threshold is not None:
        ds['Amount'] = ds['Amount'].where(ds['Amount'] <= reasonable_threshold, np.nan)
    
    # Add descriptor attributes
    ds["Blackout"].attrs["descriptor"] = "Samples"
    ds["Good"].attrs["descriptor"] = "Samples"
    ds["Bad"].attrs["descriptor"] = "Samples"
    ds["NumParticle"].attrs["descriptor"] = "Precipitation"
    ds["NumParticle"].attrs["units"] = ""
    ds["Rate"].attrs["descriptor"] = "Precipitation"
    ds["Amount"].attrs["descriptor"] = "Precipitation"
    ds["AmountSum"].attrs["descriptor"] = "Precipitation"
    ds["Z"].attrs["descriptor"] = "Precipitation"
    ds["NumError"].attrs["descriptor"] = "Laser Status"
    ds["Dirty"].attrs["descriptor"] = "Laser Status"
    ds["VeryDirty"].attrs["descriptor"] = "Laser Status"
    ds["Damaged"].attrs["descriptor"] = "Laser Status"
    ds["SignalAvg"].attrs["descriptor"] = "Laser Status"
    ds["SignalStdDev"].attrs["descriptor"] = "Laser Status"
    ds["TempAvg"].attrs["descriptor"] = "Sensor Status"
    ds["TempStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["VoltAvg"].attrs["descriptor"] = "Sensor Status"
    ds["VoltStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["HeatCurrentAvg"].attrs["descriptor"] = "Sensor Status"
    ds["HeatCurrentStdDev"].attrs["descriptor"] = "Sensor Status"
    ds["NumRain"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["NumNoRain"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["NumAmbig"].attrs["descriptor"] = "Precipitation Partitioning"
    ds["Type"].attrs["descriptor"] = "Precipitation Partitioning"

    # Particle distribution
    ds["particle_distribution"].attrs["long_name"] = "Partical distribution (count) binned by ClassNumber"

    # Data acquisition software quality control
    ds["Blackout"].attrs["long_name"] = "number of data samples excluded during PC clock synchronization"
    ds["Good"].attrs["long_name"] = "number of samples that passed the quality control checks, as performed by the data acquisition software"
    ds["Bad"].attrs["long_name"] = "number of samples that failed the quality control checks, as performed by the data acquisition software"

    # Precipitation statistics
    ds["NumParticle"].attrs["long_name"] = "total number of detected particles"
    ds["Rate"].attrs["long_name"] = "rain rate"
    ds["Amount"].attrs["long_name"] = "interval rain accumulation"
    ds["AmountSum"].attrs["long_name"] = "event rain accumulation"
    ds["Z"].attrs["long_name"] = "radar reflectivity factor"

    # Laser status
    ds["NumError"].attrs["long_name"] = "number of sample instances that were reported as dirty, very dirty, or damaged"
    ds["Dirty"].attrs["long_name"] = "laser protective glass is dirty, but measurements are still possible"
    ds["VeryDirty"].attrs["long_name"] = "laser protective glass is dirty, partially covered; no further usable measurements are possible"
    ds["Damaged"].attrs["long_name"] = "laser damaged"
    ds["SignalAvg"].attrs["long_name"] = "average signal amplitude of the laser strip"
    ds["SignalStdDev"].attrs["long_name"] = "standard deviation of the signal amplitude of the laser strip"

    # Sensor status
    ds["TempAvg"].attrs["long_name"] = "average sensor temperature"
    ds["TempStdDev"].attrs["long_name"] = "standard deviation of the sensor temperature"
    ds["VoltAvg"].attrs["long_name"] = "sensor power supply voltage"
    ds["VoltStdDev"].attrs["long_name"] = "standard deviation of the sensor power supply voltage"
    ds["HeatCurrentAvg"].attrs["long_name"] = "average heating system current"
    ds["HeatCurrentStdDev"].attrs["long_name"] = "standard deviation of the heating system current"

    # Precipitation partitioning
    ds["NumRain"].attrs["long_name"] = "number of particles detected as rain"
    ds["NumNoRain"].attrs["long_name"] = "number of particles detected not as rain"
    ds["NumAmbig"].attrs["long_name"] = "number of particles detected as ambiguous"
    ds["Type"].attrs["long_name"] = "precipitation type (1=rain; 2=mixed; 3=snow)"

    ds['time'].attrs['long_name'] = 'Time (UTC)'

    return ds

# -------------------------------------------------------
# 4. Snowfall rate correction function
# -------------------------------------------------------

# -------------------------------------------------------
# 4. Snowfall rate correction function
# -------------------------------------------------------
parsivel_correction_dict = { 
    'holroyd1971': [0.17, -1],
    'brandes2007': [0.178, -0.922],
    'heymsfield2004': [0.104, -0.95],
    'huang2010' : [0.115, -1.188]
}
def correct_SPLASH_parsivel_for_snow(ds, method='holroyd1971'):
    """
    Correct snowfall rate using a method discussed in Boudala et al. 2014
    """
    a = parsivel_correction_dict[method][0]
    b = parsivel_correction_dict[method][1]
    # Number density of particles
    N_D = ds['Nd']
    # Fall velocity of particles summed over velocity_bin
    V_D = ds['vd']
    # Class size width
    class_size_width = ds['class_size_width']

    # Apply the condition to include particle sizes from 3 to 31
    size_bin_indices = range(3, 32)
    velocity_bin_indices = range(3, 32)

    # Select the relevant slices using isel
    N_D_masked = N_D.isel(size_bin=size_bin_indices)
    class_size_width_masked = class_size_width.isel(size_bin=size_bin_indices)
    V_D_masked = V_D.isel(velocity_bin=velocity_bin_indices)

    # Calculate the snowfall rate using vectorized operations
    result = (N_D_masked * V_D_masked * class_size_width_masked ** (3 + b)).sum(dim='size_bin').sum(dim='velocity_bin')

    # Calculate the final result
    final_result = (6 * a * np.pi * 10e-4 * result)
    return final_result


    return (
        df
        .set_index("time")
        .resample(resampling_interval)
        .agg({
            # 'particle_distribution': 'mean', 
            'Blackout': mode_function, 
            'Good': mode_function, 
            'Bad': mode_function, 
            'NumParticle': 'sum', 
            'Rate': 'mean', 
            'Amount': 'sum', 
            'AmountSum': 'sum', 
            'Z': 'mean', 
            'NumError': mode_function, 
            'Dirty': mode_function, 
            'VeryDirty': mode_function, 
            'Damaged': mode_function, 
            'SignalAvg': 'mean', 
            'SignalStdDev': 'mean', 
            'TempAvg': 'mean', 
            'TempStdDev': 'mean', 
            'VoltAvg': 'mean', 
            'VoltStdDev': 'mean', 
            'HeatCurrentAvg': 'mean', 
            'HeatCurrentStdDev': 'mean', 
            'NumRain': mode_function, 
            'NumNoRain': mode_function, 
            'NumAmbig': mode_function, 
            'Type': 'mean'}))



    raw_ds = process_laser_disdrometer_file(raw_file, reasonable_threshold=reasonable_threshold)
    stats_ds = process_laser_disdrometer_file(stats_file, reasonable_threshold=reasonable_threshold)
    
    tmin = pd.Timestamp(raw_ds.time.min().item())
    tmax = pd.Timestamp(raw_ds.time.max().item())
    # calculate corrected snowfall rate
    precip_rate_uncorrected = raw_ds['precip_intensity'].resample(time="2min").mean(skipna=True, keep_attrs=True) / 30  # convert from mm/hr to mm/min
    precip_rate_holyroyd = correct_SPLASH_parsivel_for_snow(raw_ds, method='holroyd1971').resample(time="2min").mean(skipna=True, keep_attrs=True) / 30
    precip_rate_brandes = correct_SPLASH_parsivel_for_snow(raw_ds, method='brandes2007').resample(time="2min").mean(skipna=True, keep_attrs=True) / 30
    precip_rate_heymsfield = correct_SPLASH_parsivel_for_snow(raw_ds, method='heymsfield2004').resample(time="2min").mean(skipna=True, keep_attrs=True) / 30

    precip_rate_uncorrected.name = 'precip_intensity'
    precip_rate_holyroyd.name = 'precip_intensity_holroyd1971'
    precip_rate_brandes.name = 'precip_intensity_brandes2007'
    precip_rate_heymsfield.name = 'precip_intensity_heymsfield2004'

    # For initial resampling to 2 min
    precip_accum = raw_ds['accum_precip'].resample(time="2min").sum(skipna=True, keep_attrs=True)
    sensor_status = raw_ds['sensor_status'].resample(time="2min").sum(skipna=True, keep_attrs=True)
    error_code = raw_ds['error_code'].resample(time="2min").sum(skipna=True, keep_attrs=True)

    for da in [precip_accum, sensor_status, error_code, 
               precip_rate_uncorrected, precip_rate_holyroyd, 
               precip_rate_brandes, precip_rate_heymsfield]:
        da.name = da.name
        da = da.reindex(time=pd.date_range(
            tmin.floor("2min"),
            tmax.ceil("2min"),
            freq="2min"
        ))
    raw_ds_resampled = xr.merge([
        precip_accum,
        sensor_status,
        error_code,
        precip_rate_uncorrected,
        precip_rate_holyroyd,
        precip_rate_brandes,
        precip_rate_heymsfield
    ])

    # merge with stats ds
    ds = xr.merge([raw_ds_resampled, stats_ds], join='left')

    # for all snow-corrected variables, set to precip_accum where Type!=3
    for var in ['precip_intensity_holroyd1971', 'precip_intensity_brandes2007', 'precip_intensity_heymsfield2004']:
        ds[var] = ds[var].where(ds['Type']==3, ds['precip_intensity'])
    
    # create qc_bad_precip variable
    ds['qc_bad_precip'] = ((ds['sensor_status'] != 0) | 
                           (ds['error_code'] != 0) | 
                           (ds['Bad'] != 0) |
                           (ds['Dirty'] != 0) |
                           (ds['VeryDirty'] != 0) |
                           (ds['Damaged'] != 0))
    # create qc_missing_precip variable
    ds['qc_missing_precip'] = ds['accum_precip'].isnull()

    # variables for resampling
    #  4. calculate correction for snow using all methods (save these as individual arrays)
    precip_rate_uncorrected = ds['precip_intensity']
    precip_rate_holroyd = ds['precip_intensity_holroyd1971']
    precip_rate_brandes = ds['precip_intensity_brandes2007']
    precip_rate_heymsfield = ds['precip_intensity_heymsfield2004']

    precip_rate_uncorrected.name = 'precip_rate_uncorrected'
    precip_rate_holroyd.name = 'precip_rate_holyroyd'
    precip_rate_brandes.name = 'precip_rate_brandes'
    precip_rate_heymsfield.name = 'precip_rate_heymsfield'

    # add unit and long_name attributes
    for precip_rate, method in zip([precip_rate_holroyd, precip_rate_brandes, precip_rate_heymsfield],
                                ['holroyd1971', 'brandes2007', 'heymsfield2004']):
        precip_rate.attrs['units'] = 'mm/hr'
        precip_rate.attrs['long_name'] = f'Precipitation rate corrected for snow using {method} method'

    # 5. create accumulated variable
    precip_accum_uncorrected = precip_rate_uncorrected/30 # convert from mm/hr to mm/min
    precip_accum_holroyd = precip_rate_holroyd/30
    precip_accum_brandes = precip_rate_brandes/30
    precip_accum_heymsfield = precip_rate_heymsfield/30

    precip_accum_uncorrected.name = 'precip_accum_uncorrected'
    precip_accum_holroyd.name = 'precip_accum_holyroyd'
    precip_accum_brandes.name = 'precip_accum_brandes'
    precip_accum_heymsfield.name = 'precip_accum_heymsfield'

    precip_accum_uncorrected.attrs['units'] = 'mm'
    precip_accum_holroyd.attrs['units'] = 'mm'
    precip_accum_brandes.attrs['units'] = 'mm'
    precip_accum_heymsfield.attrs['units'] = 'mm'

    precip_accum_uncorrected.attrs['long_name'] = 'Accumulated precipitation uncorrected'
    precip_accum_holroyd.attrs['long_name'] = 'Accumulated precipitation corrected using holroyd1971 method'
    precip_accum_brandes.attrs['long_name'] = 'Accumulated precipitation corrected using brandes2007 method'
    precip_accum_heymsfield.attrs['long_name'] = 'Accumulated precipitation corrected using heymsfield2004 method'

    # 6. resample to desired length using appropriate function for each variable
    
    # accumulated variables: sum
    precip_accum_uncorrected_da = precip_accum_uncorrected.resample(time=resample_interval).sum()
    precip_accum_holroyd_da = precip_accum_holroyd.resample(time=resample_interval).sum()
    precip_accum_brandes_da = precip_accum_brandes.resample(time=resample_interval).sum()
    precip_accum_heymsfield_da = precip_accum_heymsfield.resample(time=resample_interval).sum()
    precip_accum_raw_da = ds['accum_precip'].resample(time=resample_interval).sum()
    precip_accum_stats_da = ds['Amount'].resample(time=resample_interval).sum()
   
    # mean variables: 
    precip_rate_uncorrected_da = precip_rate_uncorrected.resample(time=resample_interval).mean()
    precip_rate_holroyd_da = precip_rate_holroyd.resample(time=resample_interval).mean()
    precip_rate_brandes_da = precip_rate_brandes.resample(time=resample_interval).mean()
    precip_rate_heymsfield_da = precip_rate_heymsfield.resample(time=resample_interval).mean()
    precip_rate_stats_da = ds['Rate'].resample(time=resample_interval).mean()

    # # flag variables: if greater than 25% of data in interval is missing/bad, flag as True
    n_counts = ds['accum_precip'].resample(time=resample_interval).count()
    precip_missing_flag_da = (ds['precip_missing_flag'].resample(time=resample_interval).sum() / n_counts) > 0.25
    precip_bad_flag_da = (ds['precip_bad_flag'].resample(time=resample_interval).sum() / n_counts) > 0.25
    # add name
    precip_missing_flag_da.name = 'precip_missing_flag'
    precip_bad_flag_da.name = 'precip_bad_flag'
    # add flag attributes
    precip_missing_flag_da.attrs['description'] = 'Quality flag for precipitation data: True = missing data, False = data present'
    precip_bad_flag_da.attrs['description'] = 'Quality flag for precipitation data: True = bad data, False = good data'

    ds_resampled = xr.merge([
        precip_accum_uncorrected_da,
        precip_accum_holroyd_da,
        precip_accum_brandes_da,
        precip_accum_heymsfield_da,
        precip_accum_raw_da,
        precip_accum_stats_da,
        precip_rate_uncorrected_da,
        precip_rate_holroyd_da,
        precip_rate_brandes_da,
        precip_rate_heymsfield_da,
        precip_rate_stats_da,
        precip_missing_flag_da,
        precip_bad_flag_da
    ])

    # make all values nan where precip_bad_flag is True
    for var in ds_resampled.data_vars:
        if var not in ['precip_missing_flag', 'precip_bad_flag']:
            ds_resampled[var] = ds_resampled[var].where(~ds_resampled['precip_bad_flag'] |
                                                        ~ds_resampled['precip_missing_flag'], np.nan)
    return ds_resampled 

def resample_raw_laser_disdrometer_data(file, resample_interval='30min', local_tz='America/Denver'):
    raw_ds = process_raw_laser_disdrometer_file(file,)

    # Resample everything using the controlled method:
    precip_rate_uncorrected = raw_ds['precip_intensity']
    precip_rate_holyroyd    = correct_SPLASH_parsivel_for_snow(raw_ds, method='holroyd1971')
    precip_rate_brandes     = correct_SPLASH_parsivel_for_snow(raw_ds, method='brandes2007')
    precip_rate_heymsfield  = correct_SPLASH_parsivel_for_snow(raw_ds, method='heymsfield2004')
    
    # name variables
    precip_rate_uncorrected.name = 'precip_rate_uncorrected'
    precip_rate_holyroyd.name = 'precip_rate_holyroyd'
    precip_rate_brandes.name = 'precip_rate_brandes'
    precip_rate_heymsfield.name = 'precip_rate_heymsfield'

    # add unit and long_name attributes
    for precip_rate, method in zip([precip_rate_holyroyd, precip_rate_brandes, precip_rate_heymsfield],
                                ['holroyd1971', 'brandes2007', 'heymsfield2004']):
        precip_rate.attrs['units'] = 'mm/hr'
        precip_rate.attrs['long_name'] = f'Precipitation rate corrected for snow using {method} method'

    # 5. create accumulated variable
    precip_accum_uncorrected = precip_rate_uncorrected/30 # convert from mm/hr to mm/min
    precip_accum_holyroyd = precip_rate_holyroyd/30
    precip_accum_brandes = precip_rate_brandes/30
    precip_accum_heymsfield = precip_rate_heymsfield/30

    # toss values less than 0.01
    precip_accum_uncorrected = precip_accum_uncorrected.where(precip_accum_uncorrected >= 0.001, 0)
    precip_accum_holyroyd = precip_accum_holyroyd.where(precip_accum_holyroyd >= 0.001, 0)
    precip_accum_brandes = precip_accum_brandes.where(precip_accum_brandes >= 0.001, 0)
    precip_accum_heymsfield = precip_accum_heymsfield.where(precip_accum_heymsfield >= 0.001, 0)

    precip_accum_uncorrected.name = 'precip_accum_uncorrected'
    precip_accum_holyroyd.name = 'precip_accum_holyroyd'
    precip_accum_brandes.name = 'precip_accum_brandes'
    precip_accum_heymsfield.name = 'precip_accum_heymsfield'

    precip_accum_uncorrected.attrs['units'] = 'mm'
    precip_accum_holyroyd.attrs['units'] = 'mm'

    precip_accum_brandes.attrs['units'] = 'mm'
    precip_accum_heymsfield.attrs['units'] = 'mm'

    precip_accum_uncorrected.attrs['long_name'] = 'Accumulated precipitation uncorrected'
    precip_accum_holyroyd.attrs['long_name'] = 'Accumulated precipitation corrected using holroyd1971 method'
    precip_accum_brandes.attrs['long_name'] = 'Accumulated precipitation corrected using brandes2007 method'
    precip_accum_heymsfield.attrs['long_name'] = 'Accumulated precipitation corrected using heymsfield2004 method'

    # accumulated variables: sum
    precip_accum_uncorrected_da = precip_accum_uncorrected.resample(time=resample_interval).sum()
    precip_accum_holyroyd_da = precip_accum_holyroyd.resample(time=resample_interval).sum()
    precip_accum_brandes_da = precip_accum_brandes.resample(time=resample_interval).sum()
    precip_accum_heymsfield_da = precip_accum_heymsfield.resample(time=resample_interval).sum()
   
    # mean variables: 
    precip_rate_uncorrected_da = precip_rate_uncorrected.resample(time=resample_interval).mean()
    precip_rate_holyroyd_da = precip_rate_holyroyd.resample(time=resample_interval).mean()
    precip_rate_brandes_da = precip_rate_brandes.resample(time=resample_interval).mean()
    precip_rate_heymsfield_da = precip_rate_heymsfield.resample(time=resample_interval).mean()

    raw_ds_resampled = xr.merge([
        precip_accum_uncorrected_da,
        precip_accum_brandes_da,
        precip_accum_heymsfield_da,
        precip_accum_holyroyd_da,
        precip_rate_holyroyd_da,
        precip_rate_uncorrected_da,
        precip_rate_brandes_da,
        precip_rate_heymsfield_da,
    ])
    return raw_ds_resampled

def resample_stats_laser_disdrometer_data(stats_file, resample_interval='30min', local_tz='America/Denver', reasonable_threshold=None):
    
    stats_ds = process_stats_laser_disdrometer_file(stats_file, reasonable_threshold=reasonable_threshold)
    # create qc_bad_precip variable
    stats_ds['qc_bad_precip'] = ((stats_ds['Bad'] != 0) |
                           (stats_ds['Damaged'] != 0)).astype(int).astype(bool)
    # create qc_missing_precip variable
    stats_ds['qc_missing_precip'] = stats_ds['Amount'].isnull().astype(bool)

    # 6. resample to desired length using appropriate function for each variable

    # # flag variables: if greater than 25% of data in interval is missing/bad, flag as True
    n_counts = stats_ds['Amount'].resample(time=resample_interval).count()
    qc_missing_precip_da = ((stats_ds['qc_missing_precip'].resample(time=resample_interval).sum() / n_counts) > 0.25).astype(bool)
    qc_bad_precip_da = ((stats_ds['qc_bad_precip'].resample(time=resample_interval).sum() / n_counts) > 0.25).astype(bool)
    # add name
    qc_missing_precip_da.name = 'qc_missing_precip'
    qc_bad_precip_da.name = 'qc_bad_precip'
    # add flag attributes
    qc_missing_precip_da.attrs['description'] = 'Quality flag for precipitation data: True = missing data, False = data present'
    qc_bad_precip_da.attrs['description'] = 'Quality flag for precipitation data: True = bad data, False = good data'

    # accumulated variables: sum
    precip_accum_stats_da = stats_ds['Amount'].resample(time=resample_interval).sum()
    precip_rate_stats_da = stats_ds['Rate'].resample(time=resample_interval).mean()
    
    # Type variables: mode
    precip_type_da = stats_ds['Type'].resample(time=resample_interval).mean()
    precip_type_da = precip_type_da.where((precip_type_da > 2) | (precip_type_da < 0.5), 1)
    precip_type_da = precip_type_da.where((precip_type_da < 2), 3)
    precip_type_da = precip_type_da.where((precip_type_da > 0.5), np.nan)

    precip_type_da.name = 'precip_type'
    precip_type_da.attrs['description'] = 'Precipitation type (1=rain; 2=mixed; 3=snow)'

    stats_ds_resampled = xr.merge([
        precip_accum_stats_da,
        precip_rate_stats_da,
        precip_type_da,
        qc_missing_precip_da,
        qc_bad_precip_da
    ])
    return stats_ds_resampled

if __name__ == "__main__":
    import time
    stats_files = glob.glob('/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/laser_disdrometer_stats_KP/*')
    raw_files = glob.glob('/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/laser_disdrometer_raw_KP/*')

    all_raw_datasets = []
    print("Processing raw laser disdrometer files...")
    start = time.time()
    for i,raw_file in enumerate(raw_files):
        # add percentage counter every 10 files
        if i % 10 == 0:
            elapsed = time.time() - start
            print(f"Processing file {i+1} of {len(raw_files)} ({(i+1)/len(raw_files)*100:.1f}%), elapsed time: {elapsed/60:.1f} min")
        try:
            ds = resample_raw_laser_disdrometer_data(raw_file, resample_interval='30min', local_tz='America/Denver')
            all_raw_datasets.append(ds)
        except Exception as e:
            print(f"Error processing {raw_file}: {e}")
            continue
    raw_ds = xr.concat(all_raw_datasets, dim='time')
    print("Done processing raw laser disdrometer files.")

    all_stats_datasets = []
    print("Processing stats laser disdrometer files...")
    start = time.time()
    for i,stats_file in enumerate(stats_files):
        # add percentage counter every 10 files
        if i % 10 == 0:
            elapsed = time.time() - start
            print(f"Processing file {i+1} of {len(stats_files)} ({(i+1)/len(stats_files)*100:.1f}%), elapsed time: {elapsed/60:.1f} min")
        try:
            ds = resample_stats_laser_disdrometer_data(stats_file, resample_interval='30min', local_tz='America/Denver', reasonable_threshold=50)
            all_stats_datasets.append(ds)
        except Exception as e:
            print(f"Error processing {stats_file}: {e}")
            continue
    stats_ds = xr.concat(all_stats_datasets, dim='time')
    print("Done processing stats laser disdrometer files.")

    print("Merging raw and stats datasets...")
    combined_ds = xr.merge([stats_ds, raw_ds], join='left')
    print("Done merging datasets.")
    
    # correct snowfall rates where Type!=3
    for var in ['precip_rate_holyroyd', 'precip_rate_brandes', 'precip_rate_heymsfield',
                'precip_accum_holyroyd', 'precip_accum_brandes', 'precip_accum_heymsfield']:
        combined_ds[var] = combined_ds[var].where(combined_ds['precip_type']==3, combined_ds['Rate'] if 'rate' in var else combined_ds['Amount'])
    
    # remove timezone info from time coordinate
    combined_ds['time'] = (
    pd.to_datetime(combined_ds['time'].values)
    .tz_localize('UTC')                         # ADD the UTC timezone
    .tz_convert('America/Denver')               # CONVERT to Denver time
    .tz_localize(None)                          # REMOVE timezone info
    )
    # sort by time
    combined_ds = combined_ds.sortby('time')
    # save to netcdf
    combined_ds.to_netcdf('/storage/dlhogan/precipitation-rodeo/data/processed/SPLASH/SPLASH_kp_laser_disdrometer.nc')
    print("Saved combined dataset to netcdf.")