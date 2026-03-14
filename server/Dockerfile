FROM python:3.12-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ ./server/
COPY scripts/sync_lake.py ./scripts/sync_lake.py
COPY ontology/generated_prompt.md ./ontology/generated_prompt.md

# CPRA upload tool needs reference CSVs (68 E/M codes, code-category map, GPCI)
COPY data/reference/cpra/ ./data/reference/cpra/

COPY server/entrypoint.sh .
RUN chmod +x entrypoint.sh

# Lake data is downloaded from R2 on startup via entrypoint.sh.
# The download completes BEFORE the server starts to ensure all views are available.
RUN mkdir -p data/lake

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
