#!/bin/bash

# This script runs both preparation data scripts

# activate data-and-plotting conda environment
conda activate data-and-plotting
# run preparation scripts
python ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/
python ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/

echo "All data preparation complete."