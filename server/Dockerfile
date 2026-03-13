FROM python:3.12-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY scripts/sync_lake.py ./scripts/sync_lake.py

# CPRA upload tool needs reference CSVs (68 E/M codes, code-category map, GPCI)
COPY data/reference/cpra/ ./data/reference/cpra/

COPY server/entrypoint.sh .
RUN chmod +x entrypoint.sh

# Lake data is NOT baked into the image -- entrypoint.sh downloads from R2
# on startup (adds ~60s cold start). To pre-bake for faster starts, build
# locally with: docker build --build-arg PREBAKE_LAKE=1 ...
RUN mkdir -p data/lake

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
