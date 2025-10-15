import xarray as xr
import os
os.chdir("/home/dlhogan/projects/phd-repos/S3-precipitation-rodeo/")


if __name__ == "__main__":
    SITE_NAME = "mtcb"
    process_aos_met_data()