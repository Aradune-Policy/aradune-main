#!/bin/bash
set -e

LAKE_DIR="${ARADUNE_LAKE_DIR:-/data/lake}"

# Count existing Parquet files (persistent volume retains data across restarts)
EXISTING=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')

if [ "$EXISTING" -gt 100 ]; then
    echo "Persistent volume: $EXISTING Parquet files already on disk."
    echo "Starting server immediately. Incremental sync in background."

    # Incremental sync picks up any new tables added to R2 since last run
    if [ -n "$ARADUNE_S3_BUCKET" ]; then
        (
            python scripts/sync_lake.py download
            COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
            echo "Incremental sync complete: $COUNT files."
            # Signal reload for any new tables
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

    exec gunicorn server.main:app \
      --worker-class uvicorn.workers.UvicornWorker \
      --workers 2 \
      --bind 0.0.0.0:8000 \
      --timeout 120 \
      --max-requests 1000 \
      --max-requests-jitter 100 \
      --preload \
      --access-logfile - \
      --error-logfile -
fi

# First run on empty volume — download everything, then start
echo "Empty volume. Downloading full lake from R2..."
if [ -n "$ARADUNE_S3_BUCKET" ]; then
    python scripts/sync_lake.py download
    COUNT=$(find "$LAKE_DIR" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    echo "Download complete: $COUNT Parquet files."
else
    echo "WARNING: No S3 bucket configured."
fi

exec gunicorn server.main:app \
  --worker-class uvicorn.workers.UvicornWorker \
  --workers 2 \
  --bind 0.0.0.0:8000 \
  --timeout 120 \
  --max-requests 1000 \
  --max-requests-jitter 100 \
  --preload \
  --access-logfile - \
  --error-logfile -
