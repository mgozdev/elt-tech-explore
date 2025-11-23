"""
Backfill N-PORT Load from MinIO
================================

Loads all N-PORT data from MinIO to ducklake.

This script loads ALL available data from MinIO at once using DuckDB's
glob patterns and union_by_name to handle schema evolution.

Key features:
- Loads all quarters in a single operation per table (very efficient)
- Handles schema evolution automatically with union_by_name
- Skips tables that don't exist in any quarters
- Shows progress and row counts
- Can optionally filter by year/quarter range

Usage:
    python backfill_load_from_minio.py              # Load all data
    python backfill_load_from_minio.py --tables submission registrant  # Specific tables only
"""

import sys
import time
import dlt
from pathlib import Path
from config_helper import ConfigManager

# Import from the load script
from nport_load_from_minio import (
    nport_minio_source,
    PIPELINE_NAME,
    DESTINATION,
    DATASET_NAME,
    TABLE_MAPPINGS,
    MINIO_ENDPOINT,
    MINIO_BUCKET
)


def backfill_load_from_minio(
    tables: list[str] = None,
    year: int = None,
    quarter: int = None,
    destination: str = None,
    dataset: str = None,
    skip_confirmation: bool = False
):
    """
    Backfill load all N-PORT data from MinIO to destination.

    Args:
        tables: List of table names to load (default: all tables)
        year: Optional year filter
        quarter: Optional quarter filter
        destination: DLT destination (default: from DESTINATION constant)
        dataset: Dataset name (default: from DATASET_NAME constant)
        skip_confirmation: Skip the yes/no confirmation prompt
    """
    print("\n" + "="*80)
    print("N-PORT LOAD BACKFILL FROM MINIO")
    print("="*80)

    # Determine which tables to load
    if tables:
        # Validate table names
        valid_tables = []
        for table in tables:
            # Check if it's a table name or TSV filename
            if table in TABLE_MAPPINGS.values():
                # It's a table name - find the TSV filename
                tsv_file = [k for k, v in TABLE_MAPPINGS.items() if v == table][0]
                valid_tables.append(tsv_file)
            elif table in TABLE_MAPPINGS.keys():
                # It's a TSV filename
                valid_tables.append(table)
            else:
                print(f"[!] WARNING: Unknown table '{table}', skipping")

        if not valid_tables:
            print("[!] No valid tables specified. Exiting.")
            sys.exit(1)

        tables_to_load = valid_tables
        table_names = [TABLE_MAPPINGS[t] for t in valid_tables]
    else:
        tables_to_load = None
        table_names = list(TABLE_MAPPINGS.values())

    # Use provided destination or default
    dest = destination if destination else DESTINATION
    ds = dataset if dataset else DATASET_NAME

    print(f"\nMinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Destination: {dest}")
    print(f"Dataset: {ds}")
    print(f"Year filter: {year if year else 'ALL'}")
    print(f"Quarter filter: Q{quarter if quarter else 'ALL'}")
    print(f"Tables to load: {len(table_names)}")
    print("="*80)

    # Show tables
    print("\nTables to load:")
    for i, name in enumerate(table_names, 1):
        print(f"  {i}. {name}")

    # Confirm
    print("\n" + "="*80)
    print(f"This will load ALL matching data from MinIO to {dest}")
    print("Using DuckDB's union_by_name for schema evolution handling")
    print("="*80)

    if not skip_confirmation:
        response = input("\nProceed with load? (yes/no): ")
        if response.lower() != 'yes':
            print("\n[*] Load cancelled.")
            sys.exit(0)
    else:
        print("\n[*] Auto-confirming load (--yes flag provided)")

    # Create pipeline
    print("\n[*] Creating DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dest,
        dataset_name=ds
    )

    # Run load - ONE TABLE AT A TIME to ensure commits
    print("\n" + "="*80)
    print("STARTING LOAD (Sequential table-by-table)")
    print("="*80)
    print()

    start_time = time.time()
    loaded_tables = []
    failed_tables = []

    try:
        # Load each table individually to ensure proper commits
        for idx, table_file in enumerate(tables_to_load if tables_to_load else list(TABLE_MAPPINGS.keys()), 1):
            table_name = TABLE_MAPPINGS.get(table_file, table_file)

            print(f"\n[{idx}/{len(table_names)}] Loading {table_name}...")
            print("-" * 80)

            try:
                # Create source for just this ONE table
                source = nport_minio_source(
                    year=year,
                    quarter=quarter,
                    tables=[table_file]
                )

                # Load this table
                load_info = pipeline.run(source)

                # Verify it succeeded
                if load_info.has_failed_jobs:
                    print(f"[!] FAILED: {table_name}")
                    failed_tables.append(table_name)
                else:
                    print(f"[+] SUCCESS: {table_name}")
                    loaded_tables.append(table_name)

            except Exception as e:
                print(f"[!] ERROR loading {table_name}: {e}")
                failed_tables.append(table_name)
                # Continue with next table instead of failing entire job
                import traceback
                traceback.print_exc()

        elapsed = time.time() - start_time

        # Summary
        print("\n" + "="*80)
        print("LOAD COMPLETE")
        print("="*80)
        print(f"\nTotal time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        print(f"Successfully loaded: {len(loaded_tables)}/{len(table_names)} tables")
        print(f"Failed: {len(failed_tables)}/{len(table_names)} tables")

        if loaded_tables:
            print("\n[+] Successfully loaded tables:")
            for table in loaded_tables:
                print(f"    - {table}")

        if failed_tables:
            print("\n[!] Failed tables:")
            for table in failed_tables:
                print(f"    - {table}")

        # Show load details
        print("\n[*] Pipeline Details:")
        print(f"  - Pipeline: {PIPELINE_NAME}")
        print(f"  - Dataset: {ds}")
        print(f"  - Destination: {dest}")

        # Try to get row counts per table
        print("\n[*] Attempting to verify data...")
        try:
            with pipeline.sql_client() as client:
                print("\n" + "="*80)
                print("DATA VERIFICATION")
                print("="*80)

                for table_name in table_names[:5]:  # Show first 5 tables
                    try:
                        result = client.execute_sql(f"""
                            SELECT
                                COUNT(*) as total_rows,
                                COUNT(DISTINCT _as_at_date) as unique_quarters
                            FROM {ds}.{table_name}
                        """)

                        for row in result:
                            total_rows, unique_quarters = row
                            print(f"\n{table_name}:")
                            print(f"  Total rows: {total_rows:,}")
                            print(f"  Quarters: {unique_quarters}")
                    except Exception as e:
                        print(f"\n{table_name}: Could not verify ({e})")

                if len(table_names) > 5:
                    print(f"\n... and {len(table_names) - 5} more tables")

                # Show quarters loaded
                print("\n" + "="*80)
                print("QUARTERS LOADED")
                print("="*80)

                result = client.execute_sql(f"""
                    SELECT
                        _as_at_date,
                        COUNT(*) as record_count
                    FROM {ds}.submission
                    GROUP BY _as_at_date
                    ORDER BY _as_at_date
                """)

                print(f"\n{'Quarter End':<15} {'Submissions':>15}")
                print("-" * 35)
                for row in result:
                    print(f"{str(row[0]):<15} {row[1]:>15,}")

        except Exception as e:
            print(f"\n[!] Could not verify data: {e}")

        print("\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print(f"""
        Data is now loaded in {dest}!

        Next steps:
        1. Query the data:
           SELECT * FROM {ds}.submission LIMIT 10;

        2. Check data quality:
           SELECT _as_at_date, COUNT(*)
           FROM {ds}.submission
           GROUP BY _as_at_date
           ORDER BY _as_at_date;

        3. Build silver/gold layers

        4. Set up incremental loads for new quarters
        """)

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n[!] LOAD FAILED after {elapsed:.1f}s")
        print(f"[!] Error: {e}")

        import traceback
        traceback.print_exc()

        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill N-PORT data load from MinIO to destination"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific table names or TSV filenames to load (default: all tables)"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Load specific year only (default: all years)"
    )
    parser.add_argument(
        "--quarter",
        type=int,
        choices=[1, 2, 3, 4],
        help="Load specific quarter only (default: all quarters)"
    )
    parser.add_argument(
        "--destination",
        type=str,
        help=f"DLT destination (default: {DESTINATION})"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help=f"Dataset name (default: {DATASET_NAME})"
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed with load"
    )

    args = parser.parse_args()

    try:
        backfill_load_from_minio(
            tables=args.tables,
            year=args.year,
            quarter=args.quarter,
            destination=args.destination,
            dataset=args.dataset,
            skip_confirmation=args.yes
        )
    except KeyboardInterrupt:
        print("\n\n[!] Load interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[!] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
