from __future__ import annotations
from typing import Dict, Optional, Tuple
import pandas as pd
import xarray as xr
import numpy as np
import pint
from .variables import STD_VARIABLES

# Unit registry using pint
ureg = pint.UnitRegistry()
Q_ = ureg.Quantity

# Standard variable names for products
STD_VARS = {
    "precip_rate": ("mm / hour"),
    "temperature": ("degC"),
    "wind_speed": ("m / s"),
}


def convert_units_series(series: pd.Series, from_units: str, to_units: str) -> pd.Series:
    q = Q_(series.values, ureg(from_units))
    converted = q.to(ureg(to_units)).magnitude
    return pd.Series(converted, index=series.index)


def convert_units_dataarray(da: xr.DataArray, from_units: str, to_units: str) -> xr.DataArray:
    q = Q_(da.values, ureg(from_units))
    converted = q.to(ureg(to_units)).magnitude
    out = xr.DataArray(converted, coords=da.coords, dims=da.dims, attrs=da.attrs)
    out.attrs["units"] = to_units
    return out


def resample_time(obj, freq: str, how: str = "mean"):
    if isinstance(obj, pd.DataFrame):
        resampler = obj.resample(freq)
        return getattr(resampler, how)()
    elif isinstance(obj, xr.Dataset):
        resampler = obj.resample({"time": freq}) if "time" in obj.dims or "time" in obj.coords else obj.resample(time=freq)
        return getattr(resampler, how)()
    else:
        raise TypeError("Unsupported type for resample_time")


def ensure_timezone(obj, tz_from: str = "UTC", tz_to: str = "UTC"):
    if isinstance(obj, pd.DataFrame):
        idx = pd.to_datetime(obj.index)
        if idx.tz is None:
            idx = idx.tz_localize(tz_from)
        idx = idx.tz_convert(tz_to).tz_localize(None)
        obj.index = idx
        return obj
    elif isinstance(obj, xr.Dataset):
        t = pd.to_datetime(obj["time"].values)
        if getattr(t, "tz", None) is None:
            t = pd.DatetimeIndex(t).tz_localize(tz_from)
        t = t.tz_convert(tz_to).tz_localize(None)
        obj = obj.assign_coords(time=t)
        return obj
    else:
        raise TypeError("Unsupported type for ensure_timezone")


def standardize_dataset(ds: xr.Dataset, var_map: Dict[str, Dict[str, str]]) -> xr.Dataset:
    """Return a new dataset with standardized variable names and units.

    var_map: mapping from std name -> {name: raw_name, units: raw_units}
    Uses STD_VARIABLES for target units.
    """
    out = xr.Dataset(coords=ds.coords)
    for std_name, cfg in (var_map or {}).items():
        raw_name = cfg.get("name")
        raw_units = cfg.get("units")
        if raw_name is None or raw_name not in ds:
            continue
        da = ds[raw_name]
        target_units = STD_VARIABLES.get(std_name, {}).get("units")
        if target_units and raw_units and raw_units != target_units:
            da = convert_units_dataarray(da, raw_units, target_units)
        da = da.rename(std_name)
        da.attrs["long_name"] = STD_VARIABLES.get(std_name, {}).get("long_name", std_name)
        da.attrs["units"] = target_units or raw_units or da.attrs.get("units")
        out[std_name] = da
    # carry over time dim and any other coords
    for c in ds.coords:
        if c not in out.coords:
            out = out.assign_coords({c: ds[c]})
    return out
