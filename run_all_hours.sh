#!/bin/bash
# Generates tiles for all 18 forecast hours of an HRRR run.
# Usage: ./run_all_hours.sh <date> <run_hour>
# Example: ./run_all_hours.sh 20260418 0

set -e  # exit immediately if any command fails

DATE=$1
RUN_HOUR=$2

if [ -z "$DATE" ] || [ -z "$RUN_HOUR" ]; then
    echo "Usage: $0 <date> <run_hour>"
    echo "Example: $0 20260418 0"
    exit 1
fi

echo "=== Running pipeline for HRRR ${DATE} ${RUN_HOUR}Z — all forecast hours ==="

# Clean slate
rm -rf tiles
mkdir -p tiles

# Loop over forecast hours 0-18
for FH in $(seq 0 18); do
    # Pad to 2 digits: 0 → 00, 1 → 01, etc.
    FH_PADDED=$(printf "%02d" $FH)
    
    echo ""
    echo "──────── Forecast Hour ${FH_PADDED} ────────"
    
    python fetch_and_tile.py \
        "$DATE" \
        "$RUN_HOUR" \
        "$FH" \
        --output-dir "tiles/f${FH_PADDED}"
done

echo ""
echo "🎉 All 19 forecast hours complete!"
echo ""
echo "Tile folders generated:"
ls tiles/
