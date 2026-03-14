#!/bin/bash
set -e

LAKE_DIR="${ARADUNE_LAKE_DIR:-/app/data/lake}"

# Count pre-baked Parquet files (from Docker image)
PREBAKED_COUNT=0
if [ -d "$LAKE_DIR" ]; then
    PREBAKED_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
fi

if [ "$PREBAKED_COUNT" -gt 50 ]; then
    echo "Lake data pre-baked in image: $PREBAKED_COUNT Parquet files."
    # Start server immediately, sync new files in background
    if [ -n "$ARADUNE_S3_BUCKET" ]; then
        echo "Starting incremental R2 sync in background for new tables..."
        (
            python scripts/sync_lake.py download
            FINAL_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
            echo "Incremental sync complete. $FINAL_COUNT total Parquet files."
            # Send reload signal to pick up new tables
            for i in 1 2 3 4 5; do
                sleep 3
                python3 -c "
import urllib.request
try:
    urllib.request.urlopen(urllib.request.Request('http://localhost:8000/internal/reload-lake', method='POST'), timeout=5)
    print('Reload signal sent.')
    exit(0)
except: exit(1)
" 2>/dev/null && break
            done
        ) &
    fi
    exec uvicorn server.main:app --host 0.0.0.0 --port 8000
fi

# No pre-baked data — download from R2 BEFORE starting server.
# This takes 2-5 minutes but ensures all tables are available on first request.
if [ -n "$ARADUNE_S3_BUCKET" ]; then
    echo "Downloading data lake from R2 (this takes 2-5 minutes)..."
    python scripts/sync_lake.py download
    FINAL_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    echo "Download complete. $FINAL_COUNT Parquet files ready."
else
    echo "WARNING: No lake data and no S3 bucket configured. Starting with empty lake."
fi

exec uvicorn server.main:app --host 0.0.0.0 --port 8000
