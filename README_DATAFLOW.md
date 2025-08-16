Data workflow overview

- Local data lives in data/ (ignored by git)
  - raw/: original downloads, never modified
  - external/: cached third-party assets
  - processed/: harmonized outputs for analysis

- Dataset registry: config/datasets.yml
  - Each dataset defines loader, paths, timezone, CRS, and variable mapping
  - Scripts call into src/rodeo/io.load_dataset(key)

- Harmonization utilities: src/rodeo/
  - harmonize.py: units conversion, time resampling, timezone handling
  - grid.py: nearest grid cell, regridding/interp, coarsening
  - geo.py: CRS handling for points and datasets
  - timeutils.py: timezone conversions

- Conventions
  - Use standardized variable names (see src/rodeo/variables.py)
  - Preserve raw files; write derived data and a JSON sidecar describing provenance, units, CRS, and processing
  - Always work in consistent CRS (EPSG:4326) and UTC unless analysis requires local time (convert back for outputs)
