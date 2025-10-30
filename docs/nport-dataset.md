# N-PORT Dataset Documentation

## Overview

Form N-PORT is a monthly portfolio holdings report filed by registered investment companies (mutual funds and ETFs). This dataset provides detailed information about fund holdings, securities, and risk metrics.

## Data Source

**Official SEC Page:** [Form N-PORT Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets)

## Dataset Characteristics

### Format
- **File Format**: Tab-Separated Values (TSV)
- **Metadata Format**: JSON-LD (CSV on the Web)
- **Encoding**: UTF-8
- **Data Structure**: Relational (multiple related tables)

### Update Frequency
- Monthly filings
- Quarterly aggregated datasets available
- Current data: Q4 2024 (as of this repository setup)

## Data Schema

The N-PORT dataset consists of multiple related tables. The metadata is defined in [`nport_metadata.json`](../data/nport/nport_metadata.json).

### Core Tables

#### 1. SUBMISSION Table
**Primary Key:** ACCESSION_NUMBER

Key fields:
- `ACCESSION_NUMBER`: Unique 20-character EDGAR submission identifier
- `FILING_DATE`: Date the form was filed
- `FILE_NUM`: SEC file number
- `SUB_TYPE`: Submission type (NPORT-P, NPORT-P/A, NT NPORT-P)

#### 2. Additional Tables
The complete schema includes tables for:
- Portfolio holdings details
- Security identifiers
- Investment classifications
- Risk metrics
- Counterparty information
- Returns and flows

For detailed schema information, refer to:
- [`nport_metadata.json`](../data/nport/nport_metadata.json) - Complete field definitions
- [`nport_readme.htm`](../data/nport/nport_readme.htm) - Human-readable documentation
- [`edgar-form-n-port-xml-tech-spec-113/`](../data/nport/edgar-form-n-port-xml-tech-spec-113/) - Technical specifications

## Data Volume Estimates

### Typical Quarterly Dataset
- **Size**: Several GB compressed, 10-50+ GB uncompressed
- **Records**: Millions of rows across tables
- **Complexity**: Normalized relational structure with foreign keys

### Growth Rate
- Approximately 4 quarterly releases per year
- Growing number of registered funds filing N-PORT

## Use Cases for Lakehouse Patterns

This dataset is ideal for exploring lakehouse patterns because it:

1. **Has Complex Schema**: Multiple related tables requiring joins
2. **Is Large Scale**: Sufficient data volume to test performance
3. **Requires Updates**: New filings arrive monthly (incremental loading)
4. **Needs Historical Analysis**: Trend analysis across quarters
5. **Has Data Quality Needs**: Validation and cleaning requirements
6. **Supports Various Queries**: Both analytical and operational patterns

## Data Quality Considerations

### Common Issues
- Missing values in optional fields
- Inconsistent formatting across periods
- Late amendments (NPORT-P/A filings)
- Complex nested relationships

### Validation Points
- Primary key uniqueness
- Referential integrity across tables
- Date range validation
- Required vs. optional fields
- Data type compliance

## ETL/ELT Patterns to Test

### Extraction
- Bulk download vs. incremental
- Change data capture strategies
- API vs. file-based ingestion

### Loading
- Full refresh vs. upsert patterns
- Partitioning strategies (by filing date, quarter, fund type)
- File format optimization (Parquet, Delta, Iceberg)

### Transformation
- Schema evolution handling
- Data normalization/denormalization
- Calculated fields and aggregations
- Slowly Changing Dimensions (SCD) for fund information

## Getting Started with the Data

### Quick Exploration

```bash
# List available data files
ls -la data/nport/2024q4_nport/

# View metadata schema
cat data/nport/nport_metadata.json | jq .

# Sample first few rows of a table
head -n 20 data/nport/2024q4_nport/SUBMISSION.tsv
```

### Python Quick Start

```python
import polars as pl

# Read a table
df = pl.read_csv(
    "data/nport/2024q4_nport/SUBMISSION.tsv",
    separator="\t"
)

print(df.shape)
print(df.head())
```

### DuckDB Quick Start

```sql
-- Query directly from TSV files
SELECT COUNT(*)
FROM read_csv_auto('data/nport/2024q4_nport/SUBMISSION.tsv',
                   delim='\t',
                   header=true);
```

## Additional Resources

- [SEC Investment Management N-PORT Page](https://www.sec.gov/divisions/investment/imnport.shtml)
- [N-PORT Filing Instructions](https://www.sec.gov/files/formn-port.pdf)
- Technical specification: [`edgar-form-n-port-xml-tech-spec-113/`](../data/nport/edgar-form-n-port-xml-tech-spec-113/)

## Analysis Ideas

1. **Portfolio Analysis**: Track holdings changes over time
2. **Risk Metrics**: Analyze fund risk profiles
3. **Market Trends**: Identify investment patterns
4. **Compliance**: Monitor filing timeliness
5. **Performance Benchmarking**: Compare ETL approaches for processing speed
