from __future__ import annotations
import numpy as np
import xarray as xr
import pandas as pd
from typing import Tuple


def nearest_grid_point(ds: xr.Dataset, lat: float, lon: float, lat_name: str = "lat", lon_name: str = "lon") -> xr.Dataset:
    lat_idx = np.abs(ds[lat_name] - lat).argmin()
    lon_idx = np.abs(ds[lon_name] - lon).argmin()
    return ds.isel({lat_name: lat_idx, lon_name: lon_idx})


def match_grid(source: xr.Dataset, target: xr.Dataset, method: str = "nearest", 
               lat_name: str = "lat", lon_name: str = "lon") -> xr.Dataset:
    # Regrid source to target grid using xarray interp
    return source.interp({lat_name: target[lat_name], lon_name: target[lon_name]}, method=method)


def coarsen_grid(ds: xr.Dataset, factor_lat: int, factor_lon: int, lat_name: str = "lat", lon_name: str = "lon", how: str = "mean") -> xr.Dataset:
    # Coarsen grid by integer factors
    coarsened = getattr(ds.coarsen({lat_name: factor_lat, lon_name: factor_lon}, boundary="trim"), how)()
    return coarsened


def to_common_grid(ds: xr.Dataset, ref: xr.Dataset, lat_name: str = "lat", lon_name: str = "lon") -> xr.Dataset:
    return ds.interp({lat_name: ref[lat_name], lon_name: ref[lon_name]})
