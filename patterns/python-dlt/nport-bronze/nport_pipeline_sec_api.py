"""
N-PORT Bronze Layer Pipeline - SEC API Bulk Download
=====================================================

This pipeline loads N-PORT data directly from the SEC API bulk download
endpoints, eliminating the need for manual file downloads.

Key differences from the TSV-based pipeline:
- Fetches data directly from SEC API (no local files needed)
- Downloads compressed JSONL files
- Maps repPdEnd → _as_at_date for partitioning
- Supports selecting specific months or date ranges

Prerequisites:
1. SEC API key configured in .dlt/secrets.toml
2. Active destination configured (duckdb or databricks)
"""

from pathlib import Path
import dlt
from config_helper import ConfigManager
from sec_api_source import (
    sec_api_nport_source,
    generate_month_range,
    list_available_files
)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Pipeline configuration
PIPELINE_NAME = "nport_sec_api_pipeline"

# Load destination from config file
config_mgr = ConfigManager()
dest_config = config_mgr.get_destination_config()
DESTINATION = dest_config["name"]
DATASET_NAME = dest_config["dataset"]


# =============================================================================
# PIPELINE EXECUTION
# =============================================================================

def run_sec_api_pipeline(months: list) -> None:
    """
    Execute the N-PORT pipeline using SEC API bulk downloads.

    Args:
        months: List of (year, month) tuples to download
                Example: [(2024, 10), (2024, 11)]

    The pipeline will:
    1. Download compressed JSONL files for each month
    2. Decompress and parse the data
    3. Map repPdEnd → _as_at_date for partitioning
    4. Load into the configured destination
    """
    import time

    print("\n" + "="*80)
    print("N-PORT BRONZE LAYER PIPELINE (SEC API)")
    print("="*80)

    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=DESTINATION,
        dataset_name=DATASET_NAME,
    )

    print(f"\n[*] Pipeline: {PIPELINE_NAME}")
    print(f"[*] Destination: {DESTINATION}")
    print(f"[*] Dataset: {DATASET_NAME}")
    print(f"[*] Months to load: {months}")
    print(f"[*] Partition column: _as_at_date (mapped from repPdEnd)")

    # Create the source
    source = sec_api_nport_source(months=months)

    # Run the pipeline
    start_time = time.time()
    print("\n[*] Starting SEC API bulk download...")
    load_info = pipeline.run(source)
    elapsed = time.time() - start_time

    # Display results
    print("\n" + "="*80)
    print("LOAD SUMMARY")
    print("="*80)
    print(load_info)

    print(f"\n[+] Load completed in {elapsed:.1f} seconds")

    # Show detailed metrics
    print("\n[*] Load Metrics:")
    print(f"  - Pipeline name: {load_info.pipeline.pipeline_name}")
    print(f"  - Dataset: {load_info.pipeline.dataset_name}")
    print(f"  - Destination: {load_info.pipeline.destination.destination_name}")

    print("\n[*] Date Mapping:")
    print(f"  - SEC API field: repPdEnd (reporting period end)")
    print(f"  - DLT field: _as_at_date (partition column)")
    print(f"  - This represents the 'as of' date for portfolio holdings")

    print("\n[+] Pipeline completed successfully!")


# =============================================================================
# QUERY EXAMPLES
# =============================================================================

def query_examples() -> None:
    """
    Show example queries for the loaded data.
    """
    print("\n" + "="*80)
    print("EXAMPLE QUERIES")
    print("="*80)

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=DESTINATION,
        dataset_name=DATASET_NAME,
    )

    with pipeline.sql_client() as client:
        # Example 1: Count filings by date
        print("\n[*] Example 1: Count filings by reporting period date")
        result = client.execute_sql(f"""
            SELECT
                _as_at_date,
                COUNT(*) as filing_count
            FROM {DATASET_NAME}.nport_filings
            GROUP BY _as_at_date
            ORDER BY _as_at_date DESC
            LIMIT 10;
        """)

        for row in result:
            print(f"  {row[0]}: {row[1]:,} filings")

        # Example 2: Sample filing data
        print("\n[*] Example 2: Sample filing data")
        result = client.execute_sql(f"""
            SELECT
                _as_at_date,
                accessionNo,
                filedAt
            FROM {DATASET_NAME}.nport_filings
            LIMIT 5;
        """)

        for row in result:
            print(f"  {row[0]} | {row[1]} | {row[2]}")


# =============================================================================
# USAGE SCENARIOS
# =============================================================================

def load_single_month(year: int, month: int):
    """
    Load a single month of N-PORT data.

    Example:
        load_single_month(2024, 10)  # October 2024
    """
    months = [(year, month)]
    run_sec_api_pipeline(months)


def load_quarter(year: int, quarter: int):
    """
    Load a full quarter of N-PORT data.

    Args:
        year: Year (e.g., 2024)
        quarter: Quarter number (1-4)

    Example:
        load_quarter(2024, 4)  # Q4 2024 (Oct, Nov, Dec)
    """
    quarter_months = {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [7, 8, 9],
        4: [10, 11, 12],
    }

    if quarter not in quarter_months:
        raise ValueError(f"Invalid quarter: {quarter}. Must be 1-4.")

    months = [(year, m) for m in quarter_months[quarter]]
    run_sec_api_pipeline(months)


def load_year(year: int):
    """
    Load a full year of N-PORT data.

    Example:
        load_year(2024)
    """
    months = [(year, m) for m in range(1, 13)]
    run_sec_api_pipeline(months)


def load_date_range(start_year: int, start_month: int,
                   end_year: int, end_month: int):
    """
    Load a custom date range of N-PORT data.

    Example:
        load_date_range(2023, 6, 2024, 11)  # June 2023 to Nov 2024
    """
    months = generate_month_range(start_year, start_month, end_year, end_month)
    run_sec_api_pipeline(months)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load N-PORT data from SEC API bulk downloads"
    )

    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List all available files from SEC API"
    )

    parser.add_argument(
        "--month",
        nargs=2,
        type=int,
        metavar=("YEAR", "MONTH"),
        help="Load a single month (e.g., --month 2024 10)"
    )

    parser.add_argument(
        "--quarter",
        nargs=2,
        type=int,
        metavar=("YEAR", "QUARTER"),
        help="Load a quarter (e.g., --quarter 2024 4)"
    )

    parser.add_argument(
        "--year",
        type=int,
        help="Load a full year (e.g., --year 2024)"
    )

    parser.add_argument(
        "--range",
        nargs=4,
        type=int,
        metavar=("START_YEAR", "START_MONTH", "END_YEAR", "END_MONTH"),
        help="Load a date range (e.g., --range 2023 6 2024 11)"
    )

    parser.add_argument(
        "--query-examples",
        action="store_true",
        help="Show example queries after loading"
    )

    args = parser.parse_args()

    # Show current configuration
    config_mgr.show_config()

    # List available files
    if args.list_files:
        list_available_files()
        exit(0)

    # Determine what to load
    if args.month:
        year, month = args.month
        load_single_month(year, month)
    elif args.quarter:
        year, quarter = args.quarter
        load_quarter(year, quarter)
    elif args.year:
        load_year(args.year)
    elif args.range:
        start_year, start_month, end_year, end_month = args.range
        load_date_range(start_year, start_month, end_year, end_month)
    else:
        # Default: Load Q4 2024 as example
        print("\n[*] No date range specified. Loading Q4 2024 as example.")
        print("[*] Use --help to see all options.")
        load_quarter(2024, 4)

    # Run example queries if requested
    if args.query_examples:
        query_examples()

    print("\n" + "="*80)
    print("SEC API PIPELINE BENEFITS")
    print("="*80)
    print("""
    Benefits of SEC API integration:

    1. AUTOMATION:
       - No manual file downloads needed
       - Scheduled pipelines can fetch latest data automatically
       - Reduces operational overhead

    2. DATA FRESHNESS:
       - Access to latest filings as soon as they're published
       - Real-time updates within 300ms of SEC publication
       - No lag from manual download processes

    3. HISTORICAL ACCESS:
       - Complete archive from 2019 to present
       - Easy to backfill historical data
       - No local storage requirements for raw files

    4. CONSISTENCY:
       - Same data format as your existing pipeline
       - repPdEnd → _as_at_date mapping maintains partitioning
       - Compatible with existing downstream queries

    5. SCALABILITY:
       - Download only the months you need
       - Incremental loading pattern
       - Efficient compression reduces bandwidth

    Next Steps:
    - Add your SEC API key to .dlt/secrets.toml
    - Run --list-files to see available data
    - Load specific months/quarters as needed
    """)
