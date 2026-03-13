#!/bin/bash
set -e

LAKE_DIR="${ARADUNE_LAKE_DIR:-/app/data/lake}"

# Helper: send reload signal to server using Python (curl not available in slim image)
send_reload() {
    for i in 1 2 3 4 5 6 7 8 9 10; do
        sleep 3
        if python3 -c "
import urllib.request
try:
    urllib.request.urlopen('http://localhost:8000/health', timeout=2)
    urllib.request.urlopen(urllib.request.Request('http://localhost:8000/internal/reload-lake', method='POST'), timeout=5)
    print('Reload signal sent to server.')
    exit(0)
except Exception:
    exit(1)
" 2>/dev/null; then
            break
        fi
    done
}

# Count pre-baked Parquet files (from Docker image)
PREBAKED_COUNT=0
if [ -d "$LAKE_DIR" ]; then
    PREBAKED_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
fi

if [ "$PREBAKED_COUNT" -gt 0 ]; then
    echo "Lake data pre-baked in image: $PREBAKED_COUNT Parquet files."
    # Still sync from R2 in background to pick up any tables added after image build
    if [ -n "$ARADUNE_S3_BUCKET" ]; then
        echo "Starting incremental R2 sync in background for new tables..."
        (
            python scripts/sync_lake.py download
            FINAL_COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
            echo "Incremental sync complete. $FINAL_COUNT total Parquet files."
            send_reload
        ) &
    fi
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
        send_reload
    ) &
else
    echo "WARNING: No lake data and no S3 bucket configured. Starting with empty lake."
fi

exec uvicorn server.main:app --host 0.0.0.0 --port 8000
