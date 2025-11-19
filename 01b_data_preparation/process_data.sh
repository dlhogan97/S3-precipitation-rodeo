#!/bin/bash

# This script runs both preparation data scripts

# activate data-and-plotting conda environment
# Try to activate the conda environment if available
if command -v conda >/dev/null 2>&1; then
	# Source common shell rc so `conda activate` works in non-interactive shells
	if [ -f "$HOME/.bashrc" ]; then
		# shellcheck disable=SC1090
		source "$HOME/.bashrc" || true
	fi
	conda activate data-and-plotting || echo "Warning: could not activate 'data-and-plotting' environment"
else
	echo "Note: 'conda' not found in PATH. Proceeding without environment activation."
fi
# run preparation scripts
echo "Starting data preparation..."
# log start time
START_TIME=$(date +"%Y-%m-%d %H:%M:%S")
echo "Processing billy barr data..."
# run all python scripts in the 01_billy_barr directory and print the number of each script before running it
# create count variable to track script number
count=1
# get the total number of scripts to run
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/01_billy_barr/*.py | wc -l)
# loop through each script and run it
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/01_billy_barr/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

echo "Processing SAIL data..."
count=1
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/02_sail/*.py | wc -l)
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/02_sail/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

echo "Processing SOS data..."
count=1
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/03_sos/*.py | wc -l)
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/03_sos/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

echo "Processing SPLASH data..."
count=1
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/04_splash/*.py | wc -l)
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/04_splash/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

echo "Processing PRISM and ERA5-Land data..."
count=1
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/05_external/*.py | wc -l)
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/05_external/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

echo "Conducting met data normalization..."
count=1
total_scripts=$(ls ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/06_met_normalization/*.py | wc -l)
for script in ~/projects/phd-repos/S3-precipitation-rodeo/01b_data_preparation/06_met_normalization/*.py; do
    echo "Running script: $count/$total_scripts"
    ((count++))
    python "$script"
done

# log end time
END_TIME=$(date +"%Y-%m-%d %H:%M:%S")
# print how long the processing took in minutes and seconds
DURATION=$(( $(date -d "$END_TIME" +%s) - $(date -d "$START_TIME" +%s) ))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo "All data preparation for all sites complete. Took $MINUTES minutes and $SECONDS seconds."