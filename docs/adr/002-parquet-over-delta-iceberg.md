# ADR 002: Hive-Partitioned Parquet over Delta Lake / Iceberg

## Status
Accepted (2024-12)

## Context
The data lake contains 750+ tables ranging from 50 rows (dim_state) to 28.3M rows (fact_sdud_combined). Tables are updated via batch ETL at varying frequencies (weekly to annually). The storage backend is Cloudflare R2 (S3-compatible).

Delta Lake and Apache Iceberg offer ACID transactions, time travel, and schema evolution. However, they add significant complexity: transaction logs, compaction jobs, and metadata management. DuckDB supports both formats via extensions, but the extensions are less mature than native Parquet support.

## Decision
Use Hive-partitioned Parquet files with a simple convention: `data/lake/fact/{table_name}/data.parquet` (or `snapshot={date}/data.parquet` for versioned tables). Schema evolution is handled by DuckDB's `UNION BY NAME` (fills NULLs for missing columns across file versions).

## Consequences
- Positive: Maximum simplicity. Any tool reads Parquet. No transaction log management.
- Positive: DuckDB's Parquet reader is highly optimized (zone maps, predicate pushdown, column pruning).
- Positive: R2 sync is trivial: upload Parquet files, download Parquet files. No metadata coordination.
- Negative: No ACID transactions. Concurrent ETL writes could produce inconsistent reads. Mitigated by running ETL offline and uploading complete files.
- Negative: No time travel. Mitigated by snapshot partitioning (`snapshot={date}/`) for tables that need historical versions.
- Negative: Schema changes require re-uploading files. Acceptable at current scale.
