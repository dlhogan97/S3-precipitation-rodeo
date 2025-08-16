"""Minimal example using the registry and IO helpers.

Run with: python -m examples.00_quickstart (after adding to PYTHONPATH) or open in a notebook.
"""
from rodeo import DatasetRegistry, load_dataset, resample_time, ensure_timezone, save_processed

reg = DatasetRegistry()
print("Datasets:", reg.list())

# Example: load GPM if configured
try:
    ds = load_dataset("gpm_imerg")
    ds = ensure_timezone(ds, tz_from="UTC", tz_to="UTC")
    ds_hourly = resample_time(ds, "1H", how="mean")
    out = save_processed(ds_hourly, "gpm_imerg_hourly", meta={"source": "gpm_imerg", "step": "resample to 1H"})
    print("Wrote:", out)
except Exception as e:
    print("Skipping example load:", e)
