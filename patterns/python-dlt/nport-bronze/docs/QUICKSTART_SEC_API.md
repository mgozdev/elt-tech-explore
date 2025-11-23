# SEC API Quick Start Guide

Get started with the SEC API N-PORT integration in 5 minutes.

## Prerequisites

- Python environment with DLT installed
- SEC API account and API key from [https://sec-api.io](https://sec-api.io)

## Setup (One-Time)

### 1. Add Your API Key

Edit `.dlt/secrets.toml` and add:

```toml
[sources.sec_api]
api_key = "your-sec-api-key-here"
```

### 2. Verify Setup

```bash
# Check configuration
python config_helper.py --show

# Test API connection
python nport_pipeline_sec_api.py --list-files
```

You should see a list of available bulk download files.

## Quick Usage Examples

### Load One Month

```bash
python nport_pipeline_sec_api.py --month 2024 10
```

### Load a Quarter

```bash
python nport_pipeline_sec_api.py --quarter 2024 4
```

### Load a Year

```bash
python nport_pipeline_sec_api.py --year 2024
```

### Load Custom Range

```bash
python nport_pipeline_sec_api.py --range 2023 6 2024 11
```

## Key Concepts

### Date Mapping

The source automatically maps:
- **SEC API field**: `repPdEnd` (reporting period end)
- **Your partition column**: `_as_at_date`

This ensures data is correctly partitioned by the portfolio holdings date.

### File Structure

Created files:
- [sec_api_source.py](sec_api_source.py) - Custom DLT source for SEC API
- [nport_pipeline_sec_api.py](nport_pipeline_sec_api.py) - Pipeline execution script
- [SEC_API_INTEGRATION.md](SEC_API_INTEGRATION.md) - Comprehensive documentation

### How It Works

```
1. Downloads .jsonl.gz file from SEC API
2. Decompresses the file
3. Parses JSONL (one JSON object per line)
4. Maps repPdEnd → _as_at_date
5. Loads into your configured destination
```

## Query Your Data

After loading, query with SQL:

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="nport_sec_api_pipeline",
    destination="duckdb",
    dataset_name="nport_bronze"
)

with pipeline.sql_client() as client:
    result = client.execute_sql("""
        SELECT _as_at_date, COUNT(*) as filing_count
        FROM nport_bronze.nport_filings
        GROUP BY _as_at_date
        ORDER BY _as_at_date DESC;
    """)

    for row in result:
        print(f"{row[0]}: {row[1]:,} filings")
```

## Troubleshooting

**API key not found?**
- Check `.dlt/secrets.toml` has the correct format
- Ensure the file is in the `.dlt` directory (not `.dlt/secrets.toml.example`)

**401 Unauthorized?**
- Verify your API key is correct
- Check your SEC API account is active

**404 Not Found?**
- Use `--list-files` to see available months
- Not all months may be available yet

## Next Steps

1. Load a sample month to test
2. Verify data with example queries
3. Set up automated scheduled runs
4. Read [SEC_API_INTEGRATION.md](SEC_API_INTEGRATION.md) for advanced usage

## Comparison with TSV Files

| Feature | SEC API | TSV Files |
|---------|---------|-----------|
| Setup | API key | Manual download |
| Automation | ✅ Yes | ❌ Manual |
| Latest data | ✅ Real-time | ⏳ When you download |
| Historical | ✅ Complete archive | Only what you have |

Both sources use the same `_as_at_date` partitioning, so you can use them interchangeably.
