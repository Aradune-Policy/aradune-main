#!/bin/bash
set -e

# Download lake data from S3 if configured
if [ -n "$ARADUNE_S3_BUCKET" ]; then
    echo "Syncing lake data from S3..."
    python scripts/sync_lake.py download
    echo "Lake sync complete."
fi

exec uvicorn server.main:app --host 0.0.0.0 --port 8000
