# S3-precipitation-rodeo

Reproducible analysis of precipitation datasets from stations, gridded products, and models.

Setup

1) Create a virtual environment and install the package in editable mode.
2) Put raw data under `data/raw/...` (kept out of git).
3) Register datasets in `config/datasets.yml`.

Try it

- Run `examples/00_quickstart.py` to see the registry and a basic load/resample/save flow.

See `README_DATAFLOW.md` for the data layout and utility modules.