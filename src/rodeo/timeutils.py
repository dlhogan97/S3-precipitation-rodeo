from __future__ import annotations
import pandas as pd
import xarray as xr


def to_timezone(obj, tz_from: str = "UTC", tz_to: str = "UTC"):
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
        raise TypeError("Unsupported type for to_timezone")
