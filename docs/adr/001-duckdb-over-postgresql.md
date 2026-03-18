# ADR 001: DuckDB over PostgreSQL for the Data Lake

## Status
Accepted (2024-12)

## Context
Aradune ingests 400M+ rows of public Medicaid data across 750+ tables. The data is analytical (OLAP): columnar aggregations, cross-state comparisons, and ad-hoc joins across many tables. Write patterns are batch ETL (weekly to quarterly), not transactional. The deployment target is a single Fly.io instance with 2GB RAM.

PostgreSQL would require persistent storage, connection pooling, schema migrations for 750+ tables, and managed hosting ($50-200/month). It excels at transactional workloads but is suboptimal for analytical queries over wide tables with millions of rows.

## Decision
Use DuckDB as an embedded, in-process analytical database. Data is stored as Hive-partitioned Parquet files in Cloudflare R2. DuckDB creates views over these files at startup. Each FastAPI request gets a thread-safe cursor from a single shared connection.

## Consequences
- Positive: Zero infrastructure cost. Sub-millisecond warm reads. No connection pooling needed. Parquet files are portable, versionable, and work with any tool.
- Positive: DuckDB's columnar engine handles analytical queries (GROUP BY, MEDIAN, window functions) 10-100x faster than PostgreSQL for this workload.
- Negative: Single-writer limitation. No concurrent write transactions. DuckDB's Python binding has threadsafety=1.
- Negative: 2GB RAM ceiling constrains query complexity. Mitigated with memory_limit=900MB, disk spill, and object cache.
- Negative: No built-in replication or high availability. Acceptable for a pre-revenue product with a single operator.
