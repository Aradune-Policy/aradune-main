#!/bin/bash
set -e

LAKE_DIR="${ARADUNE_LAKE_DIR:-/app/data/lake}"

# Count pre-baked Parquet files (from Docker image)
PREBAKED_COUNT=0
if [ -d "$LAKE_DIR" ]; then
    PREBAKED_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
fi

if [ "$PREBAKED_COUNT" -gt 0 ]; then
    echo "Lake data pre-baked in image: $PREBAKED_COUNT Parquet files. Skipping S3 download."
    exec uvicorn server.main:app --host 0.0.0.0 --port 8000
fi

# No pre-baked data — download from R2 in the background while server starts.
# The server starts immediately with an empty lake (health checks pass right away).
# db.py registers views in a background thread as Parquet files appear.
if [ -n "$ARADUNE_S3_BUCKET" ]; then
    echo "No pre-baked lake data. Starting server while syncing from R2 in background..."
    (
        python scripts/sync_lake.py download
        FINAL_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
        echo "Lake sync complete. $FINAL_COUNT Parquet files downloaded."
        # Wait for the server to be listening, then signal it to re-register views
        for i in 1 2 3 4 5 6 7 8 9 10; do
            sleep 3
            if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
                curl -s -X POST http://localhost:8000/internal/reload-lake > /dev/null 2>&1
                echo "Reload signal sent to server."
                break
            fi
        done
    ) &
else
    echo "WARNING: No lake data and no S3 bucket configured. Starting with empty lake."
fi

exec uvicorn server.main:app --host 0.0.0.0 --port 8000
