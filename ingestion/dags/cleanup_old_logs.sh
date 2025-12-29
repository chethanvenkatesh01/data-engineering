#!/bin/bash

# Directory where the logs are stored
LOG_DIR="/opt/airflow/logs"

# Find all directories within the log directory
# Extract the date from the directory name, sort them, and get unique dates
# Keep only the top 7 unique dates, and remove the rest

find "$LOG_DIR" -type d \( -name 'run_id=manual__*' -or -name 'run_id=scheduled__*' \) | \
    sed 's/.*run_id=[^_]*__//' | \
    cut -d'T' -f1 | \
    sort -u | \
    tac | \
    tail -n +8 | \
    while read -r date; do
        # Convert the date back to the full directory path format and remove it
        find "$LOG_DIR" -type d -name "*$date*" -exec rm -rf {} +
    done
