# ADR 004: Data Partitioning and Lake Organization Strategy

## Status
Accepted (2025-01)

## Context
The data lake contains 750+ tables across 20 domains. Tables range from reference data (50 rows) to large fact tables (28.3M rows). The primary query pattern is state-level analysis: "Show me Florida's rates" or "Compare FL to GA enrollment." The storage backend is Cloudflare R2 with local disk as the primary query surface.

## Decision
Organize the lake using a medallion architecture (Bronze/Silver/Gold) with three table types:
- **fact/** tables (667): analytical data. One Parquet file per table. `state_code` is the universal join key.
- **dim/** tables (9): reference dimensions (state, procedure, occupation, locality).
- **ref/** tables (22): SCD Type 2 reference data with effective_date/termination_date.

Partitioning is by table, not within tables. Each table is a single Parquet file (or snapshot-versioned). DuckDB creates one view per table at startup. Cross-table joins happen at query time via state_code.

File naming: `data/lake/fact/{table_name}/data.parquet` (preferred) or `data/lake/fact/{table_name}/snapshot={YYYY-MM-DD}/data.parquet` (versioned).

## Consequences
- Positive: Simple. One file per table. No partition management complexity.
- Positive: DuckDB's zone maps and column pruning provide effective filtering without explicit partitioning.
- Negative: Large tables (28.3M rows SDUD) are stored as single files (~400MB). Could benefit from Hive partitioning by year for scan reduction.
- Negative: No automatic compaction or file size management. Current files average ~6.7MB; consolidation to 50-100MB targets would improve performance.
- Future: Consider Hive partitioning by state_code for the largest tables (>10M rows) to enable partition pruning.
