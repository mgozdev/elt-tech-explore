# Running with Full N-PORT Dataset

This guide walks you through running the dlt pipelines with the full quarterly N-PORT dataset.

## Prerequisites

- Full N-PORT dataset downloaded to `data/nport/2024q4_nport/` (or similar)
- At least 8GB RAM available
- ~10GB free disk space for DuckDB files

## Dataset Size Expectations

### Sample Data (Current)
- 13 tables × 10 rows each = 130 rows total
- Load time: ~1 second
- DuckDB file: <1MB

### Full Quarterly Dataset (Typical)
- Varies by quarter, but expect:
  - **SUBMISSION**: 10,000-50,000 rows
  - **IDENTIFIERS**: 100,000-500,000 rows
  - **Other tables**: Varies widely
- Total rows: Millions
- Load time: 30 seconds - 5 minutes (depends on hardware)
- DuckDB file: 500MB - 5GB

## Step-by-Step Guide

### 1. Verify Data Location

```bash
# Check what data you have
ls -lh data/nport/2024q4_nport/*.tsv

# Check row counts
wc -l data/nport/2024q4_nport/*.tsv
```

### 2. Update Pipeline Configuration

Each pipeline file has a `DATA_DIR` constant at the top. Make sure it points to your full dataset:

```python
# In nport_pipeline.py, nport_pipeline_incremental.py, nport_pipeline_validated.py
DATA_DIR = Path("../../../data/nport/2024q4_nport")  # Update this path if needed
```

### 3. Run Basic Pipeline First

Start with the basic pipeline to ensure everything works:

```bash
cd patterns/python-dlt/nport-bronze

# Run basic pipeline
time python nport_pipeline.py
```

**What to watch:**
- Load time (should be seconds to minutes, not hours)
- Memory usage (should stay under available RAM)
- Any error messages
- Final row counts

### 4. Run Incremental Pipeline

```bash
# First run - loads all data
time python nport_pipeline_incremental.py
```

**Observe:**
- Initial load time
- DuckDB file size
- Row counts per table

### 5. Test Incremental Behavior

Run the incremental pipeline again:

```bash
# Second run - should be faster (merge mode)
time python nport_pipeline_incremental.py
```

**Should see:**
- Faster execution (merge of same data)
- Same row counts (no duplicates)
- Primary key deduplication working

### 6. Run Validated Pipeline

```bash
time python nport_pipeline_validated.py
```

**Look for:**
- Validation errors (if any)
- Invalid row counts
- Validation metadata in output

## Performance Optimization Tips

### If Load is Slow

1. **Increase batch size** (dlt default is good, but can tune):
   ```python
   @dlt.resource(max_table_nesting=0)  # Disable nesting for flat data
   def load_data():
       ...
   ```

2. **Use parallel loading** (dlt can load tables in parallel):
   ```python
   # dlt automatically parallelizes when yielding multiple resources
   ```

3. **Partition large tables**:
   ```python
   @dlt.resource(
       table_name=lambda item: f"submission_{item['filing_year']}_q{item['filing_quarter']}"
   )
   ```

### If Memory is Low

1. **Process in batches**:
   ```python
   def load_data(file_path):
       with open(file_path) as f:
           reader = csv.DictReader(f, delimiter='\t')
           batch = []
           for row in reader:
               batch.append(row)
               if len(batch) >= 10000:
                   yield batch
                   batch = []
           if batch:
               yield batch
   ```

2. **Use generator pattern** (already implemented):
   ```python
   # Good - yields one row at a time
   for row in reader:
       yield row
   ```

### If Disk Space is Limited

1. **Use compression**:
   ```python
   pipeline = dlt.pipeline(
       destination='duckdb',
       destination_config={'compression': 'gzip'}
   )
   ```

2. **Archive old loads**:
   ```bash
   # Compress old DuckDB files
   gzip nport_bronze_*.duckdb
   ```

## Monitoring the Load

### Real-Time Monitoring

```bash
# In another terminal, watch disk usage
watch -n 1 du -sh patterns/python-dlt/nport-bronze/*.duckdb

# Monitor memory
watch -n 1 free -h
```

### Post-Load Analysis

```python
# Add to any pipeline
import time

start_time = time.time()
load_info = pipeline.run(source())
duration = time.time() - start_time

print(f"Load completed in {duration:.2f} seconds")
print(f"Rows per second: {total_rows / duration:.2f}")
```

## Querying Full Dataset

### Connect to DuckDB

```bash
duckdb patterns/python-dlt/nport-bronze/nport_bronze_pipeline.duckdb
```

### Useful Queries

```sql
-- Check table sizes
SELECT
    table_name,
    (SELECT COUNT(*) FROM nport_bronze.|| table_name) as row_count
FROM information_schema.tables
WHERE table_schema = 'nport_bronze'
    AND table_name NOT LIKE '_dlt%'
ORDER BY table_name;

-- Check load metadata
SELECT * FROM nport_bronze._dlt_loads ORDER BY inserted_at DESC;

-- Analyze submission data
SELECT
    sub_type,
    COUNT(*) as count,
    MIN(filing_date) as first_filing,
    MAX(filing_date) as last_filing
FROM nport_bronze.submission
GROUP BY sub_type;

-- Check for duplicates (should be none with merge mode)
SELECT
    accession_number,
    COUNT(*) as count
FROM nport_bronze.submission
GROUP BY accession_number
HAVING COUNT(*) > 1;

-- Database file size
SELECT pg_size_pretty(pg_database_size(current_database()));
```

## Expected Results

### Basic Pipeline (Replace Mode)
- **First run**: Loads all data, creates tables
- **Second run**: Drops and recreates tables, same data
- **Row counts**: Should match source TSV files

### Incremental Pipeline (Merge Mode)
- **First run**: Loads all data
- **Second run**: Merges same data, no duplicates
- **Row counts**: Stable across runs (primary key deduplication)

### Validated Pipeline
- **Validation errors**: Depends on data quality
- **Valid rows**: Should be majority
- **Metadata**: _validated_at, _validation_passed columns

## Troubleshooting Full Dataset

### Issue: "Out of Memory"

**Solution 1**: Increase batch processing
```python
# Process fewer rows at once
@dlt.resource(write_disposition="append")
def load_in_chunks(file_path):
    chunk_size = 10000
    # ... yield in chunks
```

**Solution 2**: Use streaming mode (already default)

**Solution 3**: Increase system swap space

### Issue: "Disk Full"

**Solution 1**: Clean up old DuckDB files
```bash
rm patterns/python-dlt/nport-bronze/*.duckdb
```

**Solution 2**: Move DuckDB to larger disk
```python
pipeline = dlt.pipeline(
    destination='duckdb',
    pipelines_dir='/path/to/larger/disk'
)
```

### Issue: Load Takes Too Long (> 10 minutes)

**Check:**
1. Is data on network drive? (Copy to local SSD)
2. Are you running other heavy processes?
3. Is antivirus scanning the files?

**Optimize:**
1. Use local SSD storage
2. Disable antivirus for data directory (temporarily)
3. Close other applications

### Issue: Validation Errors

**Investigate:**
```python
# Add detailed logging
if validation_errors:
    with open('validation_errors.json', 'w') as f:
        json.dump(validation_errors, f, indent=2)
```

**Common causes:**
- Changed SEC date format
- New submission types
- Data corruption in source files
- Encoding issues

## Next Steps After Full Load

### 1. Data Profiling

```bash
# Install ydata-profiling
pip install ydata-profiling

# Profile a table
python -c "
import pandas as pd
import duckdb
from ydata_profiling import ProfileReport

con = duckdb.connect('nport_bronze_pipeline.duckdb')
df = con.execute('SELECT * FROM nport_bronze.submission').df()
profile = ProfileReport(df, title='N-PORT Submission Profile')
profile.to_file('submission_profile.html')
"
```

### 2. Performance Benchmarking

Document:
- Load time
- Rows per second
- Memory peak usage
- Disk space used
- Compare across pipeline variants

### 3. Silver Layer Development

Start building transformation logic:
- Data type conversions
- Business rule validation
- Derived columns
- Aggregations

### 4. Production Preparation

- Add error handling
- Implement retry logic
- Add monitoring/alerting
- Schedule with orchestrator (Airflow/Prefect)
- Set up CI/CD

## Resources

- [dlt Performance Optimization](https://dlthub.com/docs/reference/performance)
- [DuckDB Performance Guide](https://duckdb.org/docs/guides/performance)
- [N-PORT Technical Spec](../../../data/nport/nport_readme.htm)

---

**Pro Tip:** Always test with sample data first, then scale to full dataset. Start simple, add complexity incrementally.
