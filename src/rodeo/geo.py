from __future__ import annotations
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import xarray as xr

DEFAULT_CRS = "EPSG:4326"


def points_to_gdf(df: pd.DataFrame, lon_col: str = "lon", lat_col: str = "lat", crs: str = DEFAULT_CRS) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame(df.copy(), geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs=crs)
    return gdf


def ensure_crs(gdf: gpd.GeoDataFrame, crs: str = DEFAULT_CRS) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf = gdf.set_crs(crs)
    else:
        gdf = gdf.to_crs(crs)
    return gdf


def dataset_crs(ds: xr.Dataset, crs: str = DEFAULT_CRS) -> xr.Dataset:
    ds = ds.assign_attrs({"crs": crs})
    return ds
