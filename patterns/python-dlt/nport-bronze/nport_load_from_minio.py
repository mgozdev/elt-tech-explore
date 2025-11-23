"""
N-PORT Data Load Pipeline - Load from MinIO
===========================================

Pipeline 2 of 2: Loads N-PORT data from MinIO S3-compatible storage using
DuckDB bulk reads with glob patterns.

This pipeline:
1. Reads TSV files from MinIO using DuckDB's S3 integration
2. Uses glob patterns to read multiple quarters efficiently
3. Applies store_rejects pattern for production error handling
4. Adds _as_at_date partition column based on Hive partitions

The data must first be staged by the companion pipeline:
nport_extract_to_minio.py
"""

import dlt
import duckdb
from pathlib import Path
from typing import Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

PIPELINE_NAME = "nport_load_from_minio"
DESTINATION = "ducklake"
DATASET_NAME = "nport_bronze"

# MinIO Configuration - loaded from .dlt/secrets.toml [sources.minio]
# Defaults provided for backward compatibility
try:
    MINIO_ENDPOINT = dlt.secrets["sources.minio.endpoint"]
except KeyError:
    MINIO_ENDPOINT = "localhost:9000"

try:
    MINIO_ACCESS_KEY = dlt.secrets["sources.minio.access_key"]
except KeyError:
    MINIO_ACCESS_KEY = "minioadmin"

try:
    MINIO_SECRET_KEY = dlt.secrets["sources.minio.secret_key"]
except KeyError:
    MINIO_SECRET_KEY = "minioadmin"

try:
    MINIO_BUCKET = dlt.secrets["sources.minio.bucket"]
except KeyError:
    MINIO_BUCKET = "nport-raw"

try:
    MINIO_BASE_PATH = dlt.secrets["sources.minio.base_path"]
except KeyError:
    MINIO_BASE_PATH = "files"

try:
    MINIO_USE_SSL = dlt.secrets["sources.minio.use_ssl"]
except KeyError:
    MINIO_USE_SSL = False

try:
    MINIO_REGION = dlt.secrets["sources.minio.region"]
except KeyError:
    MINIO_REGION = "us-east-1"

# Table mappings (TSV filename -> DLT table name)
TABLE_MAPPINGS = {
    "SUBMISSION.tsv": "submission",
    "REGISTRANT.tsv": "registrant",
    "FUND_REPORTED_INFO.tsv": "fund_reported_info",
    "INTEREST_RATE_RISK.tsv": "interest_rate_risk",
    "BORROWER.tsv": "borrower",
    "BORROW_AGGREGATE.tsv": "borrow_aggregate",
    "MONTHLY_TOTAL_RETURN.tsv": "monthly_total_return",
    "MONTHLY_RETURN_CAT_INSTRUMENT.tsv": "monthly_return_cat_instrument",
    "FUND_REPORTED_HOLDING.tsv": "fund_reported_holding",
    "IDENTIFIERS.tsv": "identifiers",
    "DEBT_SECURITY.tsv": "debt_security",
    "DEBT_SECURITY_REF_INSTRUMENT.tsv": "debt_security_ref_instrument",
    "CONVERTIBLE_SECURITY_CURRENCY.tsv": "convertible_security_currency",
    "REPURCHASE_AGREEMENT.tsv": "repurchase_agreement",
    "REPURCHASE_COUNTERPARTY.tsv": "repurchase_counterparty",
    "REPURCHASE_COLLATERAL.tsv": "repurchase_collateral",
    "DERIVATIVE_COUNTERPARTY.tsv": "derivative_counterparty",
    "SWAPTION_OPTION_WARNT_DERIV.tsv": "swaption_option_warnt_deriv",
    "DESC_REF_INDEX_BASKET.tsv": "desc_ref_index_basket",
    "DESC_REF_INDEX_COMPONENT.tsv": "desc_ref_index_component",
    "DESC_REF_OTHER.tsv": "desc_ref_other",
    "FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv": "fut_fwd_nonforeigncur_contract",
    "FWD_FOREIGNCUR_CONTRACT_SWAP.tsv": "fwd_foreigncur_contract_swap",
    "NONFOREIGN_EXCHANGE_SWAP.tsv": "nonforeign_exchange_swap",
    "FLOATING_RATE_RESET_TENOR.tsv": "floating_rate_reset_tenor",
    "OTHER_DERIV.tsv": "other_deriv",
    "OTHER_DERIV_NOTIONAL_AMOUNT.tsv": "other_deriv_notional_amount",
    "SECURITIES_LENDING.tsv": "securities_lending",
    "EXPLANATORY_NOTE.tsv": "explanatory_note",
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def configure_duckdb_for_minio(con: duckdb.DuckDBPyConnection):
    """
    Configure DuckDB connection for MinIO S3 access.

    Args:
        con: DuckDB connection to configure
    """
    # Install and load httpfs extension for S3 access
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure S3 settings for MinIO
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute(f"SET s3_use_ssl={'true' if MINIO_USE_SSL else 'false'};")
    con.execute(f"SET s3_url_style='path';")
    con.execute(f"SET s3_region='{MINIO_REGION}';")

    print(f"[+] DuckDB configured for MinIO at {MINIO_ENDPOINT}")


def quarter_to_date(year: int, quarter: int) -> str:
    """Convert year/quarter to quarter-end date string."""
    quarter_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    return f"{year}-{quarter_ends[quarter]}"


def get_s3_path_for_table(
    table_file: str,
    year: Optional[int] = None,
    quarter: Optional[int] = None
) -> str:
    """
    Construct S3 path with glob pattern for a table.

    Args:
        table_file: TSV filename (e.g., "SUBMISSION.tsv")
        year: Specific year, or None for all years
        quarter: Specific quarter, or None for all quarters

    Returns:
        S3 path with glob pattern
    """
    year_part = f"year={year}" if year else "year=*"
    quarter_part = f"quarter={quarter}" if quarter else "quarter=*"

    return f"s3://{MINIO_BUCKET}/{MINIO_BASE_PATH}/{year_part}/{quarter_part}/{table_file}"


# =============================================================================
# DLT SOURCES
# =============================================================================

@dlt.source
def nport_minio_source(
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    tables: Optional[list[str]] = None
):
    """
    DLT source that loads N-PORT data from MinIO using DuckDB bulk reads.

    Args:
        year: Specific year to load, or None for all years
        quarter: Specific quarter to load, or None for all quarters
        tables: List of table names to load, or None for all tables

    Returns:
        DLT resources for each table
    """
    # Determine which tables to load
    tables_to_load = tables if tables else list(TABLE_MAPPINGS.keys())

    resources = []

    for table_file in tables_to_load:
        if table_file not in TABLE_MAPPINGS:
            print(f"[!] WARNING: Unknown table {table_file}, skipping")
            continue

        table_name = TABLE_MAPPINGS[table_file]
        resources.append(create_table_resource(table_file, table_name, year, quarter))

    return resources


def create_table_resource(
    table_file: str,
    table_name: str,
    year: Optional[int],
    quarter: Optional[int]
):
    """
    Create a DLT resource for loading a specific table from MinIO.

    Args:
        table_file: TSV filename (e.g., "SUBMISSION.tsv")
        table_name: DLT table name
        year: Specific year or None
        quarter: Specific quarter or None

    Returns:
        DLT resource
    """

    @dlt.resource(
        name=table_name,
        write_disposition="append",
        columns={
            "_as_at_date": {
                "data_type": "date",
                "partition": True,
                "nullable": False,
            }
        }
    )
    def _resource():
        """
        Load table from MinIO using DuckDB with glob patterns.
        Uses store_rejects for production-ready error handling.
        """
        # Construct S3 path with glob pattern
        s3_path = get_s3_path_for_table(table_file, year, quarter)

        print(f"[*] Loading {table_name} from {s3_path}")

        # Create DuckDB connection
        con = duckdb.connect(':memory:')

        # Configure for MinIO
        configure_duckdb_for_minio(con)

        # rejects_table = f"{table_name}_rejects"

        # Read from S3 using DuckDB with Hive partitioning
        # DuckDB will automatically extract year= and quarter= from paths
        # union_by_name handles schema evolution across quarters
        # NOTE: store_rejects is NOT compatible with union_by_name in DuckDB
        #       If you encounter file errors, you may need to disable union_by_name
        #       and process quarters separately with store_rejects enabled
        #
        # IMPORTANT: sample_size=20000 (not -1)
        #   - sample_size=-1 means "sample entire dataset for schema detection"
        #   - When reading multiple files with glob patterns, sample_size=-1 causes
        #     DuckDB to incorrectly concatenate file samples, breaking newline detection
        #   - This resulted in "Maximum line size exceeded" errors (147MB "line")
        #   - sample_size=20000 provides sufficient sampling for accurate schema detection
        #     while avoiding the multi-file concatenation bug
        #   - Tested successfully with 23 quarters (149M+ rows) of IDENTIFIERS data
        query = f"""
            SELECT
                *,
                CAST(
                    CASE quarter
                        WHEN 1 THEN year || '-03-31'
                        WHEN 2 THEN year || '-06-30'
                        WHEN 3 THEN year || '-09-30'
                        WHEN 4 THEN year || '-12-31'
                    END AS DATE
                ) as _as_at_date
            FROM read_csv_auto(
                '{s3_path}',
                delim='\\t',
                header=true,
                sample_size=20000,
                new_line='\\n',
                max_line_size=10000000,
                hive_partitioning=true,
                union_by_name=true
                -- store_rejects NOT compatible with union_by_name
                -- store_rejects=true,
                -- rejects_table='table_name_rejects',
                -- rejects_limit=1000000
            )
        """

        try:
            # Check if any files match the pattern first
            check_query = f"""
                SELECT COUNT(*) as file_count
                FROM glob('{s3_path}')
            """
            file_count_result = con.execute(check_query).fetchone()
            file_count = file_count_result[0] if file_count_result else 0

            if file_count == 0:
                print(f"[*] No files found for {table_name} at {s3_path} - table may not exist in these quarters")
                print(f"[*] Skipping {table_name} (this is normal for tables added in later quarters)")
                return

            print(f"[*] Found {file_count} file(s) matching pattern")

            arrow_table = con.execute(query).fetch_arrow_table()
            row_count = len(arrow_table)

            # Reject checking disabled when using union_by_name
            # (store_rejects is incompatible with union_by_name)
            # If you encounter errors, consider disabling union_by_name and
            # processing quarters individually with store_rejects enabled

            print(f"[+] Loaded {row_count:,} rows from {table_name}")

            # Note: Any row-level errors will cause the entire load to fail
            # This is a tradeoff for supporting schema evolution with union_by_name

            yield arrow_table

        except Exception as e:
            print(f"[!] ERROR loading {table_name}: {e}")
            raise

        finally:
            con.close()

    return _resource


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load N-PORT data from MinIO to destination"
    )
    parser.add_argument("--year", type=int, help="Specific year to load (default: all years)")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], help="Specific quarter to load (default: all quarters)")
    parser.add_argument("--tables", nargs="+", help="Specific tables to load (default: all tables)")
    parser.add_argument("--destination", type=str, default=DESTINATION, help=f"DLT destination (default: {DESTINATION})")
    parser.add_argument("--dataset", type=str, default=DATASET_NAME, help=f"Dataset name (default: {DATASET_NAME})")

    args = parser.parse_args()

    print("="*80)
    print("N-PORT LOAD FROM MINIO")
    print("="*80)
    print(f"Year filter: {args.year if args.year else 'ALL'}")
    print(f"Quarter filter: Q{args.quarter if args.quarter else 'ALL'}")
    print(f"Tables: {args.tables if args.tables else 'ALL'}")
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Destination: {args.destination}")
    print(f"Dataset: {args.dataset}")
    print("="*80)

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=args.destination,
        dataset_name=args.dataset
    )

    # Create source
    source = nport_minio_source(
        year=args.year,
        quarter=args.quarter,
        tables=args.tables
    )

    # Run pipeline
    load_info = pipeline.run(source)

    print("\n" + "="*80)
    print("LOAD COMPLETE")
    print("="*80)
    print(load_info)
