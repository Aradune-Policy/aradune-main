FROM python:3.12-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY scripts/sync_lake.py ./scripts/sync_lake.py

# CPRA upload tool needs reference CSVs (68 E/M codes, code-category map, GPCI)
COPY data/reference/cpra/ ./data/reference/cpra/

# Pre-bake lake data (785MB of Parquet) — eliminates 60s cold start R2 sync
COPY data/lake/ ./data/lake/

COPY server/entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
