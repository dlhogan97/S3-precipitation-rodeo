import os
import requests
import zipfile
import shutil
from pathlib import Path
import tempfile
import pandas as pd
import xarray as xr
import geopandas as gpd
import rasterio as rio

# ==========================
# USER CONFIGURATION
# ==========================

# PRISM API parameters
ELEMENTS = ["ppt"]  # e.g., ["ppt", "tmin", "tmax"]
REGION = "us"
RESOLUTION = "800m"
DATES = [d.strftime("%Y%m%d") for d in pd.date_range("2020-01-15", "2020-01-16")]

# Output directories
BASE_DIR = Path("/storage/dlhogan/precipitation-rodeo/data/external/PRISM")
ZIP_DIR = BASE_DIR / "raw_zip"
OUT_DIR = BASE_DIR / "raw"

# Ensure directories exist
ZIP_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Base URL template
BASE_URL = "https://services.nacse.org/prism/data/get/{region}/{res}/{element}/{date}?format=nc"

# ==========================
# FUNCTIONS
# ==========================

def download_prism_data(element: str, date: str):
    """Download PRISM .zip file for a given variable and date."""
    url = BASE_URL.format(region=REGION, res=RESOLUTION, element=element, date=date)
    zip_path = ZIP_DIR / f"prism_{element}_{REGION}_{RESOLUTION}_{date}.zip"

    if zip_path.exists():
        print(f"✅ {zip_path.name} already exists, skipping download.")
        return zip_path

    print(f"⬇️ Downloading {url}")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"❌ Failed to download {url} (status {response.status_code})")
        return None

    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"💾 Saved to {zip_path}")
    return zip_path


def extract_nc_from_zip(zip_path: Path, dest_dir: Path):
    """Extract only .nc files from a PRISM zip and clean up temp files."""
    expected_nc = dest_dir / (zip_path.stem + ".nc")
    if expected_nc.exists():
        print(f"✅ {zip_path.name} already processed.")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmp_path)
        except zipfile.BadZipFile:
            print(f"❌ {zip_path.name} is invalid or corrupted.")
            return

        nc_files = list(tmp_path.rglob("*.nc"))
        if not nc_files:
            print(f"⚠️ No .nc files found in {zip_path.name}")
            return

        for nc_file in nc_files:
            dest_file = dest_dir / nc_file.name
            shutil.copy2(nc_file, dest_file)
            print(f"→ Copied {nc_file.name} to {dest_dir}")

    # Remove original zip
    try:
        zip_path.unlink()
        print(f"🗑️ Deleted {zip_path.name}")
    except Exception as e:
        print(f"⚠️ Could not delete {zip_path.name}: {e}")
    # return path to extracted .nc
    return expected_nc

def clip_prism(raster_path, shape_path):
    # Prepare output path
    out_path = raster_path.replace("raw", "processed").replace(".nc", "_clipped.nc")
    print (f"📐 Clipping and saving to {out_path}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Open raster
    with xr.open_dataset(raster_path) as ds:
        # Read shapefile
        shape = gpd.read_file(shape_path)

        # Set CRS for raster
        crs = ds.crs.attrs["crs_wkt"]
        ds = ds.rio.write_crs(crs)

        # Transform shapefile to match raster CRS
        shape = shape.to_crs(ds.rio.crs)

        # Clip raster
        clipped = ds.rio.clip(shape.geometry, shape.crs, drop=True)

        # Rename variable to ppt and set attributes
        clipped = clipped.rename({"Band1": "ppt"})
        clipped["ppt"].attrs["units"] = "mm"
        clipped["ppt"].attrs["long_name"] = "PRISM daily precipitation"
        # Remove conflicting attribute before saving
        clipped["ppt"].attrs.pop("grid_mapping", None)

    # Write clipped file
    clipped.to_netcdf(out_path, mode='w')

    # Delete original only if the clipped file was successfully written
    if os.path.exists(out_path):
        os.remove(raster_path)
        print(f"✅ Original deleted: {raster_path}")

    return

# ==========================
# MAIN LOOP
# ==========================
if __name__ == "__main__":
    print("🚀 Starting PRISM download + extraction pipeline...")
    DATES = [d.strftime("%Y%m%d") for d in pd.date_range("2021-11-01", "2023-09-30")]
    # Define shapefile path
    SHAPE_PATH = "/storage/dlhogan/precipitation-rodeo/data/geographic/East_River_lumped_HRUs_GRUs.shp"
    RESOLUTION_MAP = {"800m": "30s", "4km":"120s"}
    for element in ELEMENTS:
        for date in DATES:
            zip_path = download_prism_data(element, date)
            if zip_path:
                extracted_nc = extract_nc_from_zip(zip_path, OUT_DIR)
            if extracted_nc:
                clip_prism(f"{OUT_DIR}/prism_{element}_{REGION}_{RESOLUTION_MAP[RESOLUTION]}_{date}.nc", SHAPE_PATH)

    print("\n✅ All downloads and extractions complete!")
