#!/bin/bash
set -e

LAKE_DIR="${ARADUNE_LAKE_DIR:-/app/data/lake}"

# Download lake data from R2 in background, then signal reload.
# Server starts immediately for health checks.
if [ -n "$ARADUNE_S3_BUCKET" ]; then
    PREBAKED=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    if [ "$PREBAKED" -gt 50 ]; then
        echo "Pre-baked data: $PREBAKED files. Incremental sync in background."
    else
        echo "No pre-baked data. Full R2 sync starting in background..."
    fi
    (
        python scripts/sync_lake.py download
        COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
        echo "R2 sync complete: $COUNT Parquet files."
        # Signal server to re-register all views
        for attempt in $(seq 1 30); do
            sleep 5
            if python3 -c "
import urllib.request
try:
    urllib.request.urlopen(urllib.request.Request('http://localhost:8000/internal/reload-lake', method='POST'), timeout=10)
    print('Reload signal sent successfully.')
    exit(0)
except Exception as e:
    print(f'Reload attempt $attempt failed: {e}')
    exit(1)
" 2>&1; then
                break
            fi
        done
    ) &
else
    echo "WARNING: No S3 bucket configured."
fi

exec uvicorn server.main:app --host 0.0.0.0 --port 8000
