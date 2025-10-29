#!/bin/bash

# This script runs both merging data scripts

# activate data-and-plotting conda environment
conda activate data-and-plotting
# run merging scripts
python ~/projects/phd-repos/S3-precipitation-rodeo/01c_data_merging/gothic/merge_gothic_precipitation.py
echo "Finished merging Gothic precipitation data."
python ~/projects/phd-repos/S3-precipitation-rodeo/01c_data_merging/kettle_ponds/merge_kettle_ponds_precipitation.py
echo "Finished merging Kettle Ponds precipitation data."

echo "All data merging complete."