"""
N-PORT Data Extraction Pipeline - Stage to MinIO
=================================================

Pipeline 1 of 2: Downloads quarterly N-PORT data from SEC.gov and stages
to MinIO S3-compatible storage in Hive-partitioned structure.

This pipeline:
1. Downloads quarterly zip files from SEC.gov
2. Extracts TSV files from the zip
3. Uploads TSVs to MinIO in Hive partition format:
   s3://nport-raw/files/year=YYYY/quarter=Q/TABLE_NAME.tsv

The staged files can then be loaded by the companion pipeline:
nport_load_from_minio.py
"""

import dlt
import tempfile
import zipfile
import requests
from pathlib import Path
from typing import Iterator, Tuple
import boto3
from botocore.client import Config
from config_helper import ConfigManager

# =============================================================================
# CONFIGURATION
# =============================================================================

PIPELINE_NAME = "nport_extract_to_minio"
SEC_BASE_URL = "https://www.sec.gov/files/dera/data/form-n-port-data-sets"
USER_AGENT = "YourName your.email@example.com"  # SEC requires identification

# MinIO Configuration - loaded from .dlt/secrets.toml [sources.minio]
# Defaults provided for backward compatibility
MINIO_ENDPOINT = dlt.secrets.get("sources.minio.endpoint", "localhost:9000")
MINIO_ACCESS_KEY = dlt.secrets.get("sources.minio.access_key", "minioadmin")
MINIO_SECRET_KEY = dlt.secrets.get("sources.minio.secret_key", "minioadmin")
MINIO_BUCKET = dlt.secrets.get("sources.minio.bucket", "nport-raw")
MINIO_BASE_PATH = dlt.secrets.get("sources.minio.base_path", "files")
MINIO_USE_SSL = dlt.secrets.get("sources.minio.use_ssl", False)
MINIO_REGION = dlt.secrets.get("sources.minio.region", "us-east-1")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_s3_client():
    """Create boto3 S3 client configured for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}" if not MINIO_USE_SSL else f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(signature_version='s3v4'),
    )


def quarter_to_date(year: int, quarter: int) -> str:
    """Convert year/quarter to quarter-end date string."""
    quarter_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    return f"{year}-{quarter_ends[quarter]}"


def download_quarterly_zip(year: int, quarter: int, download_dir: Path) -> Path:
    """
    Download N-PORT quarterly zip file from SEC.gov.

    Args:
        year: Year (e.g., 2024)
        quarter: Quarter (1-4)
        download_dir: Directory to save the zip file

    Returns:
        Path to downloaded zip file
    """
    url = f"{SEC_BASE_URL}/{year}q{quarter}_nport.zip"
    zip_path = download_dir / f"{year}q{quarter}_nport.zip"

    # SEC.gov requires User-Agent header
    headers = {"User-Agent": USER_AGENT}

    print(f"[*] Downloading: {url}")
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    # Save zip file
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    file_size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"[+] Downloaded: {zip_path.name} ({file_size_mb:.1f} MB)")

    return zip_path


def extract_tsv_files(zip_path: Path, extract_dir: Path) -> list[Path]:
    """
    Extract all TSV files from a zip archive.

    Args:
        zip_path: Path to zip file
        extract_dir: Directory to extract files to

    Returns:
        List of extracted TSV file paths
    """
    print(f"[*] Extracting: {zip_path.name}")

    extracted_files = []

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            if member.upper().endswith('.TSV'):
                # Extract the file
                zip_ref.extract(member, extract_dir)
                file_path = extract_dir / member

                # Get file size
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"[+] Extracted: {member} ({file_size_mb:.1f} MB)")

                extracted_files.append(file_path)

    return extracted_files


def upload_to_minio(
    file_path: Path,
    year: int,
    quarter: int,
    s3_client
) -> str:
    """
    Upload TSV file to MinIO in Hive-partitioned structure.

    Args:
        file_path: Local path to TSV file
        year: Year for Hive partition
        quarter: Quarter for Hive partition
        s3_client: Boto3 S3 client

    Returns:
        S3 key where file was uploaded
    """
    # Construct S3 key with Hive partitioning
    # Format: files/year=YYYY/quarter=Q/TABLE_NAME.tsv
    table_name = file_path.name
    s3_key = f"{MINIO_BASE_PATH}/year={year}/quarter={quarter}/{table_name}"

    # Upload file
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"[*] Uploading {table_name} to s3://{MINIO_BUCKET}/{s3_key} ({file_size_mb:.1f} MB)")

    s3_client.upload_file(
        str(file_path),
        MINIO_BUCKET,
        s3_key
    )

    print(f"[+] Uploaded: {s3_key}")

    return s3_key


def check_quarter_exists_in_minio(year: int, quarter: int, s3_client) -> bool:
    """
    Check if a quarter's data already exists in MinIO.

    Args:
        year: Year to check
        quarter: Quarter to check
        s3_client: Boto3 S3 client

    Returns:
        True if quarter data exists, False otherwise
    """
    prefix = f"{MINIO_BASE_PATH}/year={year}/quarter={quarter}/"

    try:
        response = s3_client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=prefix,
            MaxKeys=1
        )
        return 'Contents' in response and len(response['Contents']) > 0
    except Exception as e:
        print(f"[!] Error checking MinIO: {e}")
        return False


# =============================================================================
# DLT SOURCES
# =============================================================================

@dlt.source
def nport_sec_gov_extract_source(year: int, quarter: int, skip_if_exists: bool = True):
    """
    DLT source that extracts N-PORT data for a quarter and stages to MinIO.

    Args:
        year: Year (e.g., 2024)
        quarter: Quarter (1-4)
        skip_if_exists: If True, skip if quarter already exists in MinIO

    Returns:
        DLT resource that yields upload results
    """

    @dlt.resource(
        name="extract_metadata",
        write_disposition="append",
    )
    def extract_quarter():
        """
        Downloads quarter data and uploads to MinIO.
        Yields metadata about the extraction.
        """
        s3_client = get_s3_client()

        # Check if already exists
        if skip_if_exists and check_quarter_exists_in_minio(year, quarter, s3_client):
            print(f"[*] Quarter {year} Q{quarter} already exists in MinIO - skipping")
            yield {
                "year": year,
                "quarter": quarter,
                "as_at_date": quarter_to_date(year, quarter),
                "status": "skipped",
                "reason": "already_exists",
                "files_uploaded": 0
            }
            return

        # Create temp directories
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            download_dir = temp_path / "downloads"
            extract_dir = temp_path / "extracted"
            download_dir.mkdir()
            extract_dir.mkdir()

            try:
                # Step 1: Download zip
                print(f"\n[1/3] Downloading {year} Q{quarter}")
                zip_path = download_quarterly_zip(year, quarter, download_dir)

                # Step 2: Extract TSV files
                print(f"\n[2/3] Extracting TSV files")
                tsv_files = extract_tsv_files(zip_path, extract_dir)

                # Step 3: Upload to MinIO
                print(f"\n[3/3] Uploading to MinIO")
                uploaded_keys = []
                for tsv_file in tsv_files:
                    s3_key = upload_to_minio(tsv_file, year, quarter, s3_client)
                    uploaded_keys.append(s3_key)

                print(f"\n[+] Successfully staged {len(uploaded_keys)} files for {year} Q{quarter}")

                # Yield metadata
                yield {
                    "year": year,
                    "quarter": quarter,
                    "as_at_date": quarter_to_date(year, quarter),
                    "status": "success",
                    "files_uploaded": len(uploaded_keys),
                    "s3_keys": uploaded_keys
                }

            except Exception as e:
                print(f"\n[!] ERROR: Failed to extract {year} Q{quarter}: {e}")
                yield {
                    "year": year,
                    "quarter": quarter,
                    "as_at_date": quarter_to_date(year, quarter),
                    "status": "failed",
                    "error": str(e),
                    "files_uploaded": 0
                }
                raise

    return extract_quarter()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract N-PORT data from SEC.gov and stage to MinIO"
    )
    parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2024)")
    parser.add_argument("--quarter", type=int, required=True, choices=[1, 2, 3, 4], help="Quarter (1-4)")
    parser.add_argument("--force", action="store_true", help="Force re-extraction even if data exists")

    args = parser.parse_args()

    # Load destination config (for metadata tracking only)
    config_mgr = ConfigManager()
    dest_config = config_mgr.get_destination_config()

    print("="*80)
    print("N-PORT EXTRACTION TO MINIO")
    print("="*80)
    print(f"Year: {args.year}")
    print(f"Quarter: Q{args.quarter}")
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Base Path: {MINIO_BASE_PATH}")
    print(f"Metadata tracking: {dest_config['name']}")
    print("="*80)

    # Create pipeline for metadata tracking (not for actual data)
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dest_config["name"],
        dataset_name="nport_extraction_metadata"
    )

    # Run extraction
    source = nport_sec_gov_extract_source(
        year=args.year,
        quarter=args.quarter,
        skip_if_exists=not args.force
    )

    load_info = pipeline.run(source)

    print("\n" + "="*80)
    print("EXTRACTION COMPLETE")
    print("="*80)
    print(load_info)
