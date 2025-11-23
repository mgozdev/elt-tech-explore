"""
SEC API Bulk Download Source for DLT
=====================================

Custom DLT source to download N-PORT bulk data files from SEC API.

Key features:
- Downloads compressed JSONL files (.jsonl.gz) from SEC API
- Decompresses and parses JSONL format
- Maps repPdEnd → _as_at_date for partitioning
- Handles authentication via API key
- Supports selecting specific months or date ranges

API Documentation: https://sec-api.io/docs/n-port-data-api#/bulk/form-nport/index.json
"""

import gzip
import json
from pathlib import Path
from typing import Iterator, Optional, List
from datetime import datetime
import requests
import dlt
from dlt.sources import DltResource


# =============================================================================
# CONFIGURATION
# =============================================================================

# SEC API base URL for bulk downloads
BULK_API_BASE_URL = "https://api.sec-api.io/bulk/form-nport"

# Field mapping: SEC API → DLT schema
# The SEC API uses 'repPdEnd' (reporting period end) as the "as of" date
AS_AT_DATE_FIELD = "repPdEnd"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def download_bulk_file(year: int, month: int, api_key: str) -> bytes:
    """
    Download a bulk JSONL.gz file for a specific year and month.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        api_key: SEC API authentication key

    Returns:
        Compressed file content as bytes

    Raises:
        requests.HTTPError: If download fails
    """
    # Format: /bulk/form-nport/YEAR/YEAR-MONTH.jsonl.gz
    # Example: /bulk/form-nport/2024/2024-10.jsonl.gz
    url = f"{BULK_API_BASE_URL}/{year}/{year}-{month:02d}.jsonl.gz"

    print(f"[*] Downloading: {url}")

    # API key can be passed as query parameter or header
    params = {"token": api_key}

    response = requests.get(url, params=params, stream=True)
    response.raise_for_status()

    # Read the entire compressed content
    content = response.content
    print(f"[+] Downloaded {len(content):,} bytes")

    return content


def parse_jsonl_gz(compressed_data: bytes) -> Iterator[dict]:
    """
    Decompress and parse JSONL.gz content.

    Args:
        compressed_data: Gzip-compressed JSONL data

    Yields:
        Parsed JSON objects (one per line)
    """
    # Decompress
    decompressed = gzip.decompress(compressed_data)

    # Parse JSONL (one JSON object per line)
    lines = decompressed.decode('utf-8').strip().split('\n')

    print(f"[+] Decompressed {len(lines):,} records")

    for line_num, line in enumerate(lines, 1):
        if not line.strip():
            continue

        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            print(f"[!] Warning: Failed to parse line {line_num}: {e}")
            continue


def add_partition_metadata(record: dict) -> dict:
    """
    Add _as_at_date field to record for partitioning.

    Maps the SEC API's repPdEnd field to our _as_at_date partition column.

    Args:
        record: Original SEC API record

    Returns:
        Record with _as_at_date field added
    """
    # Extract the reporting period end date
    rep_pd_end = record.get(AS_AT_DATE_FIELD)

    if not rep_pd_end:
        # Fallback: try to get from genInfo.repPdEnd (nested location)
        rep_pd_end = record.get("genInfo", {}).get("repPdEnd")

    if rep_pd_end:
        # Parse the date and format as YYYY-MM-DD
        # SEC API format: "2024-10-31" (already in correct format)
        record["_as_at_date"] = rep_pd_end
    else:
        # If no date found, log warning and use None
        print(f"[!] Warning: No repPdEnd found in record: {record.get('accessionNo', 'UNKNOWN')}")
        record["_as_at_date"] = None

    return record


def get_available_months(api_key: str) -> List[dict]:
    """
    Fetch the index of available bulk download files.

    Args:
        api_key: SEC API authentication key

    Returns:
        List of available files with metadata (key, updatedAt, size)
    """
    url = f"{BULK_API_BASE_URL}/index.json"
    params = {"token": api_key}

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()


# =============================================================================
# DLT SOURCE
# =============================================================================

@dlt.resource(
    name="nport_filings",
    write_disposition="append",  # Append mode for incremental loads
    columns={
        "_as_at_date": {
            "data_type": "date",
            "partition": True,
            "nullable": False,
        }
    }
)
def nport_bulk_resource(
    year: int,
    month: int,
    api_key: str = dlt.secrets.value
) -> Iterator[dict]:
    """
    DLT resource to load N-PORT bulk data for a specific month.

    This resource:
    1. Downloads the compressed JSONL file for the specified month
    2. Decompresses and parses the JSONL format
    3. Adds _as_at_date field for partitioning
    4. Yields records for DLT to load

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        api_key: SEC API key (loaded from secrets by default)

    Yields:
        N-PORT filing records with _as_at_date field
    """
    print(f"\n[*] Loading N-PORT bulk data for {year}-{month:02d}")

    # Download the file
    compressed_data = download_bulk_file(year, month, api_key)

    # Parse JSONL
    records = parse_jsonl_gz(compressed_data)

    # Add partition metadata and yield
    record_count = 0
    for record in records:
        record_with_metadata = add_partition_metadata(record)
        yield record_with_metadata
        record_count += 1

    print(f"[+] Processed {record_count:,} N-PORT filings")


@dlt.source
def sec_api_nport_source(
    months: List[tuple],  # List of (year, month) tuples
    api_key: str = dlt.secrets.value
):
    """
    DLT source for SEC API N-PORT bulk downloads.

    This source creates a resource for each month to download.

    Args:
        months: List of (year, month) tuples to download
                Example: [(2024, 10), (2024, 11)]
        api_key: SEC API key (loaded from secrets by default)

    Yields:
        DLT resources for each month

    Example:
        ```python
        # Download October and November 2024
        source = sec_api_nport_source(
            months=[(2024, 10), (2024, 11)]
        )

        pipeline = dlt.pipeline(
            pipeline_name="nport_sec_api",
            destination="duckdb",
            dataset_name="nport_bronze"
        )

        load_info = pipeline.run(source)
        ```
    """
    for year, month in months:
        # Create a unique resource for each month
        yield nport_bulk_resource(year=year, month=month, api_key=api_key)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def list_available_files(api_key: Optional[str] = None) -> None:
    """
    Display all available bulk download files.

    Args:
        api_key: SEC API key (optional, loads from secrets if not provided)
    """
    if api_key is None:
        api_key = dlt.secrets.get("sources.sec_api.api_key")

    print("\n" + "="*80)
    print("AVAILABLE SEC API BULK DOWNLOAD FILES")
    print("="*80)

    files = get_available_months(api_key)

    print(f"\nFound {len(files)} available files:\n")

    for file_info in files:
        key = file_info.get("key", "")
        size_mb = file_info.get("size", 0) / (1024 * 1024)
        updated = file_info.get("updatedAt", "")

        print(f"  {key:<40} {size_mb:>8.1f} MB    Updated: {updated}")

    print("\n" + "="*80)


def generate_month_range(start_year: int, start_month: int,
                        end_year: int, end_month: int) -> List[tuple]:
    """
    Generate a list of (year, month) tuples for a date range.

    Args:
        start_year: Starting year
        start_month: Starting month (1-12)
        end_year: Ending year
        end_month: Ending month (1-12)

    Returns:
        List of (year, month) tuples

    Example:
        >>> generate_month_range(2024, 10, 2024, 12)
        [(2024, 10), (2024, 11), (2024, 12)]
    """
    months = []

    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append((current_year, current_month))

        # Increment month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    return months


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    """
    Test the SEC API source by listing available files.

    To test downloads, run the pipeline script instead.
    """
    import sys

    # Try to load API key from secrets
    try:
        api_key = dlt.secrets.get("sources.sec_api.api_key")
        list_available_files(api_key)
    except Exception as e:
        print(f"\n[!] Error: {e}")
        print("\n[*] Please configure your SEC API key in .dlt/secrets.toml:")
        print("    [sources.sec_api]")
        print("    api_key = \"your-api-key-here\"")
        sys.exit(1)
