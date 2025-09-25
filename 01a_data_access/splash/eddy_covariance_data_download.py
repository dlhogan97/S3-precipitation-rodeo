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
import zipfile

# %% [markdown]
# # Clean SPLASH Laser Disdrometer data

# %%
# set the filepath
filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/'
if not os.path.exists(filepath+'eddy_covariance_data_KP'):
    print('Data not downloaded yet. Would you like to download it?')
    download = input('y/n: ')
    if download == 'y':
        # make eddy_covariance_data_raw_KP directory
        os.makedirs(filepath+'eddy_covariance_data_KP', exist_ok=True)
        # downlaod the data from https://zenodo.org/records/10558503
        zip_files = ["KP10mFLX-2021.zip", "KP10mFLX-2022.zip", "KP10mFLX-2023.zip"]
        for zip_file in zip_files:
            os.system('wget https://zenodo.org/records/10558503/files/'+zip_file+' -P '+filepath)
            with zipfile.ZipFile(filepath+zip_file, 'r') as zip_ref:
                zip_ref.extractall(filepath+'eddy_covariance_data_KP')
            os.system('rm '+filepath+zip_file)
    else:
        print('Download the data from https://zenodo.org/records/10558503/')
else:
    print('Data already downloaded')
    # we'll start by loading in one file and looking at the data
    filepath = '/storage/dlhogan/precipitation-rodeo/data/raw/SPLASH/eddy_covariance_data_KP/*'
    files = glob.glob(filepath)





