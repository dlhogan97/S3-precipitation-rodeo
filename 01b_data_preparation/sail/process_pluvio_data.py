import glob
import os
import sys, os
project_root = "/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo"
if project_root not in sys.path:
    sys.path.append(project_root)
from utils import process_sail_data
from utils.helper_funcs import convert_to_local_time
import xarray as xr
import time
import numpy as np
from scipy import stats
import pandas as pd

# Assign data directory and get files
SITE_NAME = "mtcb"
data_dir = "/storage/dlhogan/precipitation-rodeo/data/"
files = glob.glob(f"{data_dir}raw/SAIL/aos_{SITE_NAME}/*.nc")

# make sure files exist
if len(files) == 0:
    raise FileNotFoundError(f"No files found in {data_dir}/raw/SAIL/aos_{SITE_NAME}/")