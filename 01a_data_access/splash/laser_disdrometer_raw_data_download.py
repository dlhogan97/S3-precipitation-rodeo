# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import glob as glob
import os
from dask.distributed import LocalCluster, Client
import dask.dataframe as dd
import datetime as dt

# %% [markdown]
# # Clean SPLASH Laser Disdrometer data

# %%
# set the filepath
filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/'
if not os.path.exists(filepath+'laser_disdrometer_raw_KP'):
    print('Data not downlaoded yet. Would you like to download it?')
    download = input('y/n: ')
    if download == 'y':
        # downlaod the data from https://zenodo.org/records/10368926/files/NOAA_PSL_OttDisdrometerRaw_KettlePonds.zip using wget to the filepath
        os.system('wget https://zenodo.org/records/10368926/files/NOAA_PSL_OttDisdrometerRaw_KettlePonds.zip -P '+filepath)
        # unzip the file
        os.system('unzip '+filepath+'NOAA_PSL_OttDisdrometerRaw_KettlePonds.zip -d '+filepath)
        # remove the zip file
        os.system('rm '+filepath+'NOAA_PSL_OttDisdrometerRaw_KettlePonds.zip')
    else:
        print('Download the data from https://zenodo.org/records/10368926/files/NOAA_PSL_OttDisdrometerRaw_KettlePonds.zip')
else:
    print('Data already downloaded')
    # we'll start by loading in one file and looking at the data
    filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/laser_disdrometer_raw_KP/*'
    files = glob.glob(filepath)

# %%




