
#!/bin/bash
set -euo pipefail

# This script runs all Python scripts in each subfolder of `01a_data_access`.
# It mirrors the behavior of `process_data.sh` in `01b_data_preparation`.

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

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Base dir: $BASE_DIR"
echo "Starting data access scripts..."

for subdir in "$BASE_DIR"/*; do
		if [ -d "$subdir" ]; then
				name=$(basename "$subdir")
				echo "Processing $name..."
				# gather python scripts in the subdir
				scripts=("$subdir"/*.py)
				if [ ! -e "${scripts[0]}" ]; then
						echo "  No python scripts found in $name"
						continue
				fi
				count=1
				total=${#scripts[@]}
				for script in "${scripts[@]}"; do
						echo "  Running script: $count/$total -> $(basename "$script")"
						((count++))
						python "$script"
				done
		fi
done

echo "All data access scripts complete."

exit 0

