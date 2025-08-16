from __future__ import annotations
from pathlib import Path
from typing import Optional, Union, Dict, Any
import json
import pandas as pd
import xarray as xr
import datetime as dt

from .registry import DatasetRegistry, DatasetEntry
from .paths import PROCESSED_DIR

# Simple IO layer that dispatches to different loaders based on registry

def load_dataset(key: str, registry: Optional[DatasetRegistry] = None):
    reg = registry or DatasetRegistry()
    entry: DatasetEntry = reg.get(key)
    if entry.loader == "snodgrass_csv":
        # utils is a top-level package in this repository
        from utils.get_snodgrass_data import get_snodgrass_data  # type: ignore
        if not entry.raw_path or not entry.meta_path:
            raise ValueError("snodgrass_csv requires raw_path and meta_path")
        return get_snodgrass_data(entry.raw_path, entry.meta_path)
    elif entry.loader == "xarray_nc":
        if entry.raw_path:
            return xr.open_dataset(entry.raw_path)
        elif entry.raw_glob:
            import glob
            files = sorted(glob.glob(entry.raw_glob))
            if not files:
                raise FileNotFoundError(f"No files matched {entry.raw_glob}")
            return xr.open_mfdataset(files, combine="by_coords")
        else:
            raise ValueError("xarray_nc requires raw_path or raw_glob")
    else:
        raise NotImplementedError(f"Unknown loader: {entry.loader}")


def save_processed(obj: Union[pd.DataFrame, xr.Dataset], name: str, meta: Optional[Dict[str, Any]] = None,
                   subdir: Optional[str] = None) -> Path:
    """Save DataFrame or Dataset to processed/ with a JSON sidecar carrying provenance.

    - DataFrame -> Parquet
    - Dataset  -> NetCDF
    """
    out_dir = PROCESSED_DIR / (subdir or "")
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{name}__{timestamp}"
    if isinstance(obj, pd.DataFrame):
        data_path = out_dir / f"{base}.parquet"
        obj.to_parquet(data_path)
    elif isinstance(obj, xr.Dataset):
        data_path = out_dir / f"{base}.nc"
        obj.to_netcdf(data_path)
    else:
        raise TypeError("Unsupported object type for save_processed")
    # write sidecar metadata
    sidecar = out_dir / f"{base}.json"
    with open(sidecar, "w") as f:
        json.dump(meta or {}, f, indent=2)
    return data_path
