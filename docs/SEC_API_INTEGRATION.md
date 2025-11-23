# SEC API Integration for N-PORT Data

This guide explains how to use the SEC API bulk download source to load N-PORT data directly into your DLT pipeline.

## Overview

The SEC API integration provides an automated way to fetch N-PORT filing data without manual file downloads. It uses the [SEC API bulk download endpoints](https://sec-api.io/docs/n-port-data-api) to retrieve compressed JSONL files.

### Key Features

- **Automated Downloads**: Fetch data directly from SEC API
- **Historical Data**: Access to all filings from 2019 to present
- **Partitioned Storage**: Automatically partitions by `_as_at_date`
- **Date Mapping**: Maps SEC API's `repPdDate` field to `_as_at_date`
- **Flexible Loading**: Load single months, quarters, years, or custom ranges
- **Same Schema**: Compatible with existing TSV-based pipeline

## Architecture

```
┌─────────────────┐
│   SEC API       │
│  Bulk Download  │
│                 │
│ /bulk/form-nport│
│ /YEAR/YEAR-     │
│ MONTH.jsonl.gz  │
└────────┬────────┘
         │
         │ HTTPS GET with API key
         ▼
┌─────────────────┐
│  sec_api_source │
│                 │
│ 1. Download .gz │
│ 2. Decompress   │
│ 3. Parse JSONL  │
│ 4. Map dates    │
└────────┬────────┘
         │
         │ DLT Resources
         ▼
┌─────────────────┐
│  DLT Pipeline   │
│                 │
│ - Partitioning  │
│ - Schema        │
│ - Destination   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DuckDB/       │
│   Databricks/   │
│   Other         │
└─────────────────┘
```

## Date Mapping Logic

The critical mapping for partitioning:

```
SEC API Field           DLT Field              Description
─────────────────────────────────────────────────────────────
repPdDate        ──►     _as_at_date           Reporting period date
                                              (portfolio holdings "as of" date)
```

### Why `repPdDate`?

- **`repPdDate`**: The date when the portfolio holdings snapshot was taken
- **`filedAt`**: When the filing was submitted to SEC (can be weeks later)
- **`_as_at_date`**: Your partition column representing the data snapshot date

The `repPdDate` field accurately represents the "as of" date for the portfolio holdings data, making it the correct choice for partitioning by data date.

## Setup

### 1. Get SEC API Key

1. Sign up at [https://sec-api.io](https://sec-api.io)
2. Navigate to your dashboard
3. Copy your API key

### 2. Configure Secrets

Edit [.dlt/secrets.toml](.dlt/secrets.toml) and add your API key:

```toml
[sources.sec_api]
api_key = "your-api-key-here"
```

**Important**: This file is gitignored - never commit it to version control.

### 3. Verify Configuration

```bash
# Show current DLT configuration
python config_helper.py --show

# List available bulk download files from SEC API
python nport_pipeline_sec_api.py --list-files
```

## Usage

### Load a Single Month

```bash
python nport_pipeline_sec_api.py --month 2024 10
```

This downloads and loads October 2024 N-PORT filings.

### Load a Quarter

```bash
python nport_pipeline_sec_api.py --quarter 2024 4
```

This loads Q4 2024 (October, November, December).

### Load a Full Year

```bash
python nport_pipeline_sec_api.py --year 2024
```

This loads all 12 months of 2024.

### Load a Custom Date Range

```bash
python nport_pipeline_sec_api.py --range 2023 6 2024 11
```

This loads from June 2023 through November 2024.

### Run Example Queries

```bash
python nport_pipeline_sec_api.py --month 2024 10 --query-examples
```

This loads the data and then runs example queries to verify.

## Python API Usage

You can also use the source programmatically:

```python
import dlt
from sec_api_source import sec_api_nport_source

# Define months to load
months = [(2024, 10), (2024, 11)]  # Oct and Nov 2024

# Create source
source = sec_api_nport_source(months=months)

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="nport_sec_api",
    destination="duckdb",
    dataset_name="nport_bronze"
)

# Run pipeline
load_info = pipeline.run(source)
print(load_info)
```

### Helper Functions

```python
from sec_api_source import (
    list_available_files,
    generate_month_range
)

# List all available files
list_available_files()

# Generate a range of months
months = generate_month_range(
    start_year=2023,
    start_month=6,
    end_year=2024,
    end_month=11
)
# Returns: [(2023, 6), (2023, 7), ..., (2024, 11)]
```

## Data Structure

### Source Data Format

The SEC API returns JSONL (JSON Lines) files compressed with gzip:

```
2024-10.jsonl.gz
├── Line 1: {"accessionNo": "...", "filedAt": "...", "repPdDate": "2024-10-31", ...}
├── Line 2: {"accessionNo": "...", "filedAt": "...", "repPdDate": "2024-10-31", ...}
└── Line N: {...}
```

Each line is a complete N-PORT filing in JSON format.

### DLT Resource Schema

The `nport_filings` resource has the following key fields:

| Field | Type | Description |
|-------|------|-------------|
| `_as_at_date` | DATE | Partition column (mapped from `repPdDate`) |
| `accessionNo` | TEXT | SEC accession number (unique filing ID) |
| `filedAt` | TEXT | Filing date and time |
| `repPdDate` | TEXT | Original reporting period end date |
| `fundInfo` | JSON | Fund information (assets, liabilities, etc.) |
| `invstOrSecs` | JSON | Investment holdings (portfolio securities) |
| `genInfo` | JSON | General filing information |

The schema is automatically inferred by DLT based on the JSON structure.

## Partitioning

### How It Works

The source automatically adds `_as_at_date` to each record:

```python
def add_partition_metadata(record: dict) -> dict:
    rep_pd_end = record.get("repPdDate")
    record["_as_at_date"] = rep_pd_end
    return record
```

DLT uses this field to create Hive-style partitions:

```
s3://elt/data/nport_bronze/nport_filings/
    _as_at_date=2024-10-31/
        data_xxxxx.parquet
    _as_at_date=2024-11-30/
        data_xxxxx.parquet
```

### Query Performance

Queries that filter by `_as_at_date` will only scan relevant partitions:

```sql
-- Efficient: Only scans Oct 2024 partition
SELECT COUNT(*)
FROM nport_bronze.nport_filings
WHERE _as_at_date = '2024-10-31';

-- Less efficient: Scans all partitions
SELECT COUNT(*)
FROM nport_bronze.nport_filings;
```

## Comparison: SEC API vs TSV Files

| Aspect | SEC API Source | TSV Files Source |
|--------|----------------|------------------|
| **Setup** | API key required | Files must be downloaded manually |
| **Automation** | Fully automated | Manual download step |
| **Data Format** | JSONL (nested JSON) | TSV (flat tables) |
| **Freshness** | Real-time access | Depends on manual updates |
| **Historical** | Complete archive | Only what you've downloaded |
| **Storage** | No local files needed | Requires local storage |
| **Speed** | Network dependent | Fast (local reads) |
| **Cost** | API usage costs | Free (after download) |

### When to Use Each

**Use SEC API Source When:**
- You want automated, scheduled updates
- You need the latest filings immediately
- You want to backfill historical data
- You prefer not to manage local files

**Use TSV Files Source When:**
- You already have local TSV files
- You're doing one-time historical loads
- You want to avoid API costs
- Network access is limited

## Troubleshooting

### API Key Not Found

```
Error: sources.sec_api.api_key configuration value is missing
```

**Solution**: Add your API key to `.dlt/secrets.toml`:

```toml
[sources.sec_api]
api_key = "your-api-key-here"
```

### HTTP 401 Unauthorized

```
requests.exceptions.HTTPError: 401 Client Error: Unauthorized
```

**Solution**: Your API key is invalid or expired. Check:
1. API key is correct in `.dlt/secrets.toml`
2. Account is active on SEC API portal
3. API key hasn't been revoked

### HTTP 404 Not Found

```
requests.exceptions.HTTPError: 404 Client Error: Not Found
```

**Solution**: The requested month doesn't exist. Use `--list-files` to see available data:

```bash
python nport_pipeline_sec_api.py --list-files
```

### No Data Found

```
Warning: No repPdDate found in record
```

**Solution**: Some filings may have different JSON structures. The source tries both:
1. Top-level `repPdDate` field
2. Nested `genInfo.repPdDate` field

If neither exists, `_as_at_date` will be `NULL` and the record will log a warning.

### Memory Issues

For large months with many filings, you might encounter memory issues.

**Solutions**:
1. Load smaller date ranges (e.g., one month at a time)
2. Increase system memory
3. Use streaming write disposition (future enhancement)

## Performance Tuning

### Download Speed

- SEC API bulk files are pre-compressed (typically 10-50 MB per month)
- Download time depends on your network speed
- Consider running during off-peak hours for large historical loads

### Processing Speed

The source uses efficient processing:
- **Decompression**: Python's built-in gzip (fast C implementation)
- **JSONL Parsing**: Line-by-line streaming (low memory)
- **DLT Loading**: Batch inserts with automatic optimization

Typical performance (varies by hardware and destination):
- **Download**: 5-30 seconds per month
- **Decompression**: 2-10 seconds per month
- **DLT Loading**: 10-60 seconds per month
- **Total**: ~1-2 minutes per month

### Parallel Loading

For multiple months, the pipeline processes them sequentially. For faster loading:

```python
# Future enhancement: parallel downloads
from concurrent.futures import ThreadPoolExecutor

def parallel_load(months):
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(lambda m: load_single_month(*m), months)
```

## Integration with Existing Pipeline

### Using Both Sources

You can use both the SEC API source and TSV file source:

```python
# Load historical data from TSV files
from nport_pipeline_partitioned import nport_source_partitioned

historical = nport_source_partitioned(data_dir="./historical")
pipeline.run(historical)

# Load recent data from SEC API
from sec_api_source import sec_api_nport_source

recent = sec_api_nport_source(months=[(2024, 11)])
pipeline.run(recent)
```

Both sources use the same partitioning scheme (`_as_at_date`), so data integrates seamlessly.

### Incremental Loading Pattern

For ongoing updates, use append mode:

```python
# In sec_api_source.py, change write_disposition
@dlt.resource(
    name="nport_filings",
    write_disposition="append",  # Changed from "replace"
    ...
)
```

Then load new months as they become available:

```bash
# Monthly cron job
python nport_pipeline_sec_api.py --month $(date +%Y) $(date +%m)
```

## Advanced Configuration

### Custom Destination

Override the destination configuration:

```python
pipeline = dlt.pipeline(
    pipeline_name="nport_sec_api",
    destination="databricks",  # or "postgres", "bigquery", etc.
    dataset_name="nport_bronze"
)
```

### Custom Schema Transformation

Add custom transformations before loading:

```python
@dlt.resource
def transformed_nport_filings(year, month, api_key):
    for record in nport_bulk_resource(year, month, api_key):
        # Custom transformation
        record["fund_name"] = record.get("genInfo", {}).get("regName", "")
        record["total_assets"] = record.get("fundInfo", {}).get("totAssets", 0)
        yield record
```

### Rate Limiting

If you're making many API calls, implement rate limiting:

```python
import time
from functools import wraps

def rate_limit(calls_per_second=2):
    def decorator(func):
        last_called = [0.0]
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            wait_time = (1.0 / calls_per_second) - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result
        return wrapper
    return decorator

@rate_limit(calls_per_second=2)
def download_bulk_file(year, month, api_key):
    # ... existing code
```

## Next Steps

1. **Configure Your API Key**: Add it to `.dlt/secrets.toml`
2. **Test the Connection**: Run `--list-files` to verify
3. **Load Sample Data**: Try loading a single month
4. **Validate Results**: Run example queries to inspect data
5. **Automate**: Set up scheduled runs for regular updates

## Additional Resources

- [SEC API Documentation](https://sec-api.io/docs/n-port-data-api)
- [DLT Documentation](https://dlthub.com/docs)
- [N-PORT Filing Format](https://www.sec.gov/files/form-n-port-popular.pdf)

## Support

For issues or questions:
1. Check this documentation
2. Review the inline code comments in `sec_api_source.py`
3. Consult SEC API documentation
4. Check DLT documentation for destination-specific issues
