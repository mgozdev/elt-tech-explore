# N-PORT Bronze Layer Pipeline with dlt

This directory contains production-ready pipelines using [dlt (data load tool)](https://dlthub.com/) to ingest SEC N-PORT data into a lakehouse architecture.

## Overview

dlt is a Python-first data loading library with:
- Automatic schema inference and evolution
- Incremental loading with merge/upsert
- Multiple destination support (DuckLake, MotherDuck, Databricks)
- Built-in state management and metadata tracking

## Project Structure

```
nport-bronze/
├── Pipeline Scripts
│   ├── nport_extract_to_minio.py      # Extract single quarter from SEC.gov → MinIO
│   ├── backfill_extract_to_minio.py   # Batch: Extract all quarters → MinIO
│   ├── nport_load_from_minio.py       # Load from MinIO → Destination
│   ├── backfill_load_from_minio.py    # Batch: Load all data → Destination
│   ├── nport_pipeline_sec_api.py      # Alternative: SEC API → Destination (direct)
│   └── sec_api_source.py              # DLT source for SEC API
├── Utilities
│   └── config_helper.py               # Destination configuration manager
├── Configuration
│   ├── .dlt/config.toml               # Pipeline configuration
│   ├── .dlt/secrets.toml.example      # Credentials template
│   └── requirements.txt               # Python dependencies
└── Documentation
    └── docs/                          # Detailed guides
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Credentials

```bash
# Copy example secrets
cp .dlt/secrets.toml.example .dlt/secrets.toml

# Edit with your credentials
# - MinIO endpoint, access key, secret key
# - Databricks workspace URL, token (if using Databricks)
# - MotherDuck token (if using MotherDuck)
```

### 3. Choose Your Workflow

## Pipeline Workflows

### Workflow A: Two-Stage Pipeline (ZIP Files via MinIO)

**Use when:** Loading from SEC.gov quarterly ZIP files

**Stage 1: Extract & Stage**
```bash
# Single quarter
python nport_extract_to_minio.py --year 2024 --quarter 4

# OR backfill all quarters (2019-2025)
python backfill_extract_to_minio.py
```

**Stage 2: Load to Destination**
```bash
# Load specific quarter
python nport_load_from_minio.py --year 2024 --quarter 4 --destination ducklake

# OR load all data
python backfill_load_from_minio.py --destination databricks

# Load specific tables only
python nport_load_from_minio.py --tables submission registrant identifiers
```

**Supported destinations:**
- `ducklake` - DuckDB with Parquet files (local lakehouse)
- `motherduck` - Managed DuckDB cloud service
- `databricks` - Databricks lakehouse

---

### Workflow B: Direct API Pipeline

**Use when:** You have a SEC API key and want direct loading (no MinIO staging)

```bash
# Load specific quarter
python nport_pipeline_sec_api.py --quarter 2024 4

# Load specific month
python nport_pipeline_sec_api.py --month 2024 11

# Load full year
python nport_pipeline_sec_api.py --year 2024

# Load date range
python nport_pipeline_sec_api.py --range 2024 1 2024 12
```

**Note:** This workflow is untested (requires SEC API key)

---

## Pipeline Architecture

### Script Relationships

```
Two-Stage Pipeline (ZIP Files):
┌─────────────────────────┐     ┌──────────────────────────┐
│ SEC.gov Quarterly ZIPs  │────▶│ MinIO S3 Storage         │
│ (nport_extract_to_minio)│     │ (Hive partitioned TSVs)  │
└─────────────────────────┘     └──────────┬───────────────┘
                                           │
                                           ▼
                                ┌──────────────────────────┐
                                │ Destination              │
                                │ (nport_load_from_minio)  │
                                │ • DuckLake               │
                                │ • MotherDuck             │
                                │ • Databricks             │
                                └──────────────────────────┘

Direct API Pipeline:
┌─────────────────────────┐     ┌──────────────────────────┐
│ SEC API Bulk Downloads  │────▶│ Destination              │
│ (nport_pipeline_sec_api)│     │ (DuckLake/Databricks)    │
└─────────────────────────┘     └──────────────────────────┘
```

### Data Flow Details

**MinIO Staging Format:**
```
s3://nport-raw/files/
  ├── year=2024/
  │   └── quarter=4/
  │       ├── SUBMISSION.tsv
  │       ├── REGISTRANT.tsv
  │       └── ... (29 tables)
  └── year=2024/
      └── quarter=3/
          └── ...
```

**Destination Schema:**
- Dataset: `nport_bronze`
- Partitioned by: `_as_at_date` (quarter end date)
- 29 tables mapped from N-PORT TSV files
- Automatic dlt metadata columns: `_dlt_load_id`, `_dlt_id`

---

## Core Scripts Explained

### Extract Scripts (Source: SEC.gov ZIPs)

**nport_extract_to_minio.py** - Core single-quarter extractor
- Downloads one quarterly ZIP from SEC.gov
- Extracts 29 TSV files
- Uploads to MinIO in Hive-partitioned format
- CLI: `--year YYYY --quarter Q [--force]`

**backfill_extract_to_minio.py** - Batch wrapper
- Processes all quarters (2019-2025)
- Uses local ZIPs from `./downloaded_zips/` if available
- Skips quarters already in MinIO (unless `--force`)
- Interactive confirmation, progress tracking

### Load Scripts (MinIO → Destination)

**nport_load_from_minio.py** - Core incremental loader
- Reads TSV files from MinIO using DuckDB S3 integration
- Uses glob patterns for multi-quarter efficiency
- Handles schema evolution with `union_by_name`
- CLI: `[--year Y] [--quarter Q] [--tables T...] [--destination D]`

**backfill_load_from_minio.py** - Batch loader
- Loads ALL quarters in single operation per table
- Table-by-table sequential processing
- Shows progress and row counts
- CLI: `[--tables T...] [--destination D]`

### API Scripts (Alternative Source)

**sec_api_source.py** - DLT source library
- Custom dlt source for SEC API bulk downloads
- Downloads compressed JSONL files (`.jsonl.gz`)
- Maps `repPdEnd` → `_as_at_date` for partitioning
- Reusable across pipelines

**nport_pipeline_sec_api.py** - Direct API pipeline
- Loads directly from SEC API to destination
- No MinIO staging required
- Supports flexible date ranges
- Requires SEC API key in `.dlt/secrets.toml`

### Utilities

**config_helper.py** - Configuration manager
- Manages destination configuration (DuckDB, DuckLake, Databricks)
- Validates credentials
- CLI: `python config_helper.py show|set|test`

---

## Configuration Management

### View Current Configuration
```bash
python config_helper.py show
```

### Set Active Destination
```bash
python config_helper.py set ducklake
python config_helper.py set databricks
python config_helper.py set motherduck
```

### Test Destination Connection
```bash
python config_helper.py test
```

---

## Key dlt Concepts

### Write Dispositions

All pipelines use **merge** write disposition for production reliability:

```python
@dlt.resource(
    write_disposition="merge",
    primary_key=["accession_number", "table_specific_key"]
)
```

| Disposition | Behavior | Our Usage |
|------------|----------|-----------|
| `merge` | Upsert by primary key | ✅ All tables (handles re-runs, updates) |
| `append` | Add new rows | ⚠️ Not used (risk of duplicates) |
| `replace` | Drop and recreate | ⚠️ Not used (data loss risk) |

### Schema Evolution

dlt automatically handles:
- New columns added to source files
- Type inference and normalization
- Nested data flattening
- Metadata columns (`_dlt_load_id`, `_dlt_id`)

### Partitioning

All data is partitioned by `_as_at_date`:
- Derived from quarter end date (ZIP workflow)
- Derived from `repPdEnd` field (API workflow)
- Enables efficient time-based queries
- Supports incremental refreshes

---

## Data Exploration

### DuckLake (Local)
```bash
# Connect to DuckLake
python -c "import duckdb; conn = duckdb.connect(); \
  conn.execute(\"INSTALL 'ducklake.files' AS ducklake\"); \
  conn.execute('USE ducklake'); \
  print(conn.execute('SHOW TABLES').fetchall())"

# Query
python -c "import duckdb; conn = duckdb.connect(); \
  conn.execute(\"INSTALL 'ducklake.files' AS ducklake\"); \
  print(conn.execute('SELECT COUNT(*) FROM ducklake.nport_bronze.submission').fetchall())"
```

### Databricks
```sql
-- In Databricks SQL Editor
USE nport_bronze;
SHOW TABLES;
SELECT * FROM submission WHERE _as_at_date = '2024-12-31' LIMIT 10;
```

---

## Production Considerations

### Error Handling
- Pipelines use dlt's built-in error handling
- Failed rows are logged to `_dlt_loads` table
- Re-running pipelines is idempotent (merge disposition)

### Performance Tips
1. **Batch Operations**: Use `backfill_*` scripts for historical loads
2. **Parallel Loading**: DuckDB automatically parallelizes glob reads
3. **Incremental Updates**: Load only new quarters for regular updates
4. **Table Filtering**: Use `--tables` to load specific tables only

### Monitoring
- Check `_dlt_loads` table for load history
- Monitor MinIO storage usage
- Track row counts per `_as_at_date` partition

---

## Next Steps

### Immediate
1. Configure your destination in `.dlt/secrets.toml`
2. Run extract → load workflow for a single quarter
3. Verify data in destination
4. Set up backfill for historical data

### Silver Layer (Transform)
1. Data cleaning and standardization
2. Type casting and validation
3. Business rules application
4. Slowly changing dimensions (SCD Type 2)

### Gold Layer (Aggregations)
1. Summary tables and KPIs
2. Dimensional models (star schema)
3. Time series analytics
4. Data marts for business users

### Production Deployment
1. Orchestration (Airflow/Prefect/Dagster)
2. Monitoring and alerting
3. CI/CD pipelines
4. Data quality tests
5. Documentation and data catalog

---

## Common Patterns

### Incremental Quarter Load (Recommended)
```bash
# Monthly: Load latest quarter after SEC releases data
python nport_extract_to_minio.py --year 2024 --quarter 4
python nport_load_from_minio.py --year 2024 --quarter 4 --destination ducklake
```

### Full Historical Backfill (One-Time)
```bash
# Initial setup: Load all historical data
python backfill_extract_to_minio.py
python backfill_load_from_minio.py --destination databricks
```

### Selective Table Refresh
```bash
# Refresh specific tables only
python nport_load_from_minio.py --tables submission registrant --destination ducklake
```

---

## Troubleshooting

### MinIO Connection Errors
- Verify endpoint, access key, secret key in `.dlt/secrets.toml`
- Check MinIO is running: `curl http://localhost:9000`
- Test with: `python config_helper.py test`

### Schema Evolution Issues
- dlt automatically evolves schema (adds new columns)
- For breaking changes, use `--force` to re-extract
- Check `_dlt_loads` table for schema versions

### Memory Issues with Large Files
- Use batch scripts (`backfill_*`) which process sequentially
- DuckDB handles large files efficiently via streaming
- Reduce concurrent tables with `--tables` flag

### Databricks Connection
- Verify workspace URL and token in `.dlt/secrets.toml`
- Ensure cluster is running
- Check network connectivity to Databricks workspace

---

## Resources

### Documentation
- [dlt Documentation](https://dlthub.com/docs/)
- [DuckLake Destination Guide](./docs/dlt-DUCKLAKE.md)
- [SEC API Integration](./docs/QUICKSTART_SEC_API.md)
- [Full Dataset Guide](./docs/FULL_DATASET_GUIDE.md)

### dlt Resources
- [dlt General Usage](https://dlthub.com/docs/general-usage/pipeline)
- [dlt Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/)
- [dlt Community Slack](https://dlthub.com/community)

---

**Author:** Built as part of the ELT Technology Exploration project
**Status:** Production-ready for DuckLake and Databricks destinations
**Last Updated:** 2025-11-24
