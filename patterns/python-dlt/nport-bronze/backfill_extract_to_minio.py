"""
Backfill N-PORT Extraction to MinIO
====================================

Backfill script that extracts all available N-PORT quarters to MinIO.

Smart features:
1. Uses already-downloaded zips from ./downloaded_zips if they exist
2. Only downloads from SEC.gov if zip doesn't exist locally
3. Skips quarters already in MinIO (unless --force)
4. Processes quarters sequentially with progress tracking
5. Stops on errors for manual intervention

Usage:
    python backfill_extract_to_minio.py
    python backfill_extract_to_minio.py --force  # Re-upload everything
    python backfill_extract_to_minio.py --zip-dir ./my_zips  # Use different zip directory
"""

import sys
import time
from pathlib import Path
import tempfile
import zipfile
import dlt
from config_helper import ConfigManager
from nport_extract_to_minio import (
    get_s3_client,
    quarter_to_date,
    download_quarterly_zip,
    extract_tsv_files,
    upload_to_minio,
    check_quarter_exists_in_minio,
    MINIO_BUCKET,
    MINIO_BASE_PATH,
    MINIO_ENDPOINT
)

# Import check_available_quarters from existing script
import requests
SEC_GOV_BASE_URL = "https://www.sec.gov/files/dera/data/form-n-port-data-sets"
USER_AGENT = "YourName your.email@example.com"


def check_available_quarters(start_year: int = 2019, end_year: int = 2025):
    """
    Check which quarterly datasets are available on SEC.gov.

    Returns:
        List of (year, quarter) tuples for available datasets
    """
    available = []

    print("\n" + "="*80)
    print("CHECKING AVAILABLE QUARTERS")
    print("="*80)

    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            url = f"{SEC_GOV_BASE_URL}/{year}q{quarter}_nport.zip"

            try:
                # HEAD request to check if file exists (faster than GET)
                response = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=10)

                if response.status_code == 200:
                    # Get file size if available
                    size_mb = int(response.headers.get('Content-Length', 0)) / (1024 * 1024)
                    available.append((year, quarter))
                    print(f"  [OK] {year} Q{quarter}: Available ({size_mb:.1f} MB)")
                else:
                    print(f"  [--] {year} Q{quarter}: Not available")

            except Exception as e:
                print(f"  [!!] {year} Q{quarter}: Error checking ({e})")

    return available


def backfill_extract_to_minio(
    zip_dir: Path = None,
    force: bool = False,
    start_year: int = 2019,
    end_year: int = 2025
):
    """
    Backfill extraction of N-PORT data to MinIO.

    Args:
        zip_dir: Directory containing already-downloaded zips (default: ./downloaded_zips)
        force: If True, re-upload even if quarter exists in MinIO
        start_year: First year to check (default: 2019)
        end_year: Last year to check (default: 2025)
    """
    # Set default zip directory
    if zip_dir is None:
        zip_dir = Path("./downloaded_zips")

    zip_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*80)
    print("N-PORT EXTRACTION BACKFILL TO MINIO")
    print("="*80)
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Base Path: {MINIO_BASE_PATH}")
    print(f"Zip Directory: {zip_dir.absolute()}")
    print(f"Force Re-upload: {force}")
    print("="*80)

    # Get S3 client
    s3_client = get_s3_client()

    # Check available quarters on SEC.gov
    print("\n[*] Checking available quarters from SEC.gov...")
    available = check_available_quarters(start_year, end_year)

    if not available:
        print("\n[!] No quarters available. Exiting.")
        sys.exit(1)

    # Sort by date (oldest first)
    available.sort()

    print(f"\n[+] Found {len(available)} available quarters")

    # Display plan
    print("\n" + "="*80)
    print("EXTRACTION PLAN")
    print("="*80)
    print(f"\nWill process {len(available)} quarters:\n")

    for year, quarter in available:
        as_at_date = quarter_to_date(year, quarter)
        zip_filename = f"{year}q{quarter}_nport.zip"
        zip_path = zip_dir / zip_filename

        # Check status
        exists_locally = zip_path.exists()
        exists_minio = check_quarter_exists_in_minio(year, quarter, s3_client)

        status = ""
        if exists_minio and not force:
            status = "[SKIP - Already in MinIO]"
        elif exists_locally:
            status = "[USE LOCAL ZIP]"
        else:
            status = "[DOWNLOAD FROM SEC.gov]"

        print(f"  {year} Q{quarter} -> {as_at_date}: {status}")

    # Confirm
    print("\n" + "="*80)
    response = input("\nProceed with extraction? (yes/no): ")
    if response.lower() != 'yes':
        print("\n[*] Extraction cancelled.")
        sys.exit(0)

    # Load DLT config for metadata tracking
    config_mgr = ConfigManager()
    dest_config = config_mgr.get_destination_config()

    # Create pipeline for metadata tracking
    pipeline = dlt.pipeline(
        pipeline_name="nport_extract_to_minio",
        destination=dest_config["name"],
        dataset_name="nport_extraction_metadata"
    )

    # Process each quarter
    print("\n" + "="*80)
    print("STARTING EXTRACTION")
    print("="*80)

    start_time = time.time()
    processed = []
    skipped = []
    failed = []

    for idx, (year, quarter) in enumerate(available, 1):
        as_at_date = quarter_to_date(year, quarter)

        print(f"\n{'='*80}")
        print(f"[{idx}/{len(available)}] Processing {year} Q{quarter}")
        print(f"_as_at_date: {as_at_date}")
        print(f"{'='*80}")

        quarter_start = time.time()

        # Check if already in MinIO
        if not force and check_quarter_exists_in_minio(year, quarter, s3_client):
            print(f"[*] Quarter {year} Q{quarter} already exists in MinIO - skipping")
            skipped.append((year, quarter))
            continue

        zip_filename = f"{year}q{quarter}_nport.zip"
        zip_path = zip_dir / zip_filename

        try:
            # Step 1: Get or download zip
            if zip_path.exists():
                print(f"[*] Using existing zip: {zip_path.name}")
                file_size_mb = zip_path.stat().st_size / (1024 * 1024)
                print(f"[+] Zip size: {file_size_mb:.1f} MB")
            else:
                print(f"[*] Downloading {zip_filename} from SEC.gov...")
                zip_path = download_quarterly_zip(year, quarter, zip_dir)

            # Step 2: Extract TSV files to temp directory
            print(f"\n[*] Extracting TSV files...")
            with tempfile.TemporaryDirectory() as temp_dir:
                extract_dir = Path(temp_dir)
                tsv_files = extract_tsv_files(zip_path, extract_dir)

                # Step 3: Upload to MinIO
                print(f"\n[*] Uploading {len(tsv_files)} files to MinIO...")
                uploaded_keys = []
                for tsv_file in tsv_files:
                    s3_key = upload_to_minio(tsv_file, year, quarter, s3_client)
                    uploaded_keys.append(s3_key)

                print(f"\n[+] Successfully uploaded {len(uploaded_keys)} files for {year} Q{quarter}")

            # Track in DLT metadata
            @dlt.resource(name="extract_metadata", write_disposition="append")
            def metadata():
                yield {
                    "year": year,
                    "quarter": quarter,
                    "as_at_date": as_at_date,
                    "status": "success",
                    "files_uploaded": len(uploaded_keys),
                    "used_local_zip": zip_path.exists()
                }

            pipeline.run(metadata())

            quarter_elapsed = time.time() - quarter_start
            processed.append((year, quarter))

            print(f"\n[+] {year} Q{quarter} completed in {quarter_elapsed:.1f}s")
            print(f"[*] Progress: {len(processed)}/{len(available)} quarters processed, {len(skipped)} skipped")

            # Estimate remaining time
            if processed:
                avg_time = (time.time() - start_time) / len(processed)
                remaining = len(available) - len(processed) - len(skipped)
                est_remaining_mins = (remaining * avg_time) / 60
                print(f"[*] Estimated time remaining: {est_remaining_mins:.1f} minutes")

        except Exception as e:
            quarter_elapsed = time.time() - quarter_start
            print(f"\n[!] FATAL ERROR: Failed to process {year} Q{quarter}")
            print(f"[!] Error: {e}")
            print(f"[!] Time spent: {quarter_elapsed:.1f}s")
            print(f"\n[!] Stopping extraction. Fix the issue and re-run to continue.")
            print(f"[!] Successfully processed: {len(processed)} quarters")
            print(f"[!] Skipped: {len(skipped)} quarters")

            import traceback
            traceback.print_exc()

            sys.exit(1)

    # Summary
    total_elapsed = time.time() - start_time

    print("\n" + "="*80)
    print("EXTRACTION BACKFILL COMPLETE")
    print("="*80)

    print(f"\nTotal time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
    print(f"Processed: {len(processed)}/{len(available)} quarters")
    print(f"Skipped: {len(skipped)}/{len(available)} quarters (already in MinIO)")
    print(f"Failed: {len(failed)}/{len(available)} quarters")

    if processed:
        print("\n[+] Successfully processed quarters:")
        for year, quarter in processed:
            as_at_date = quarter_to_date(year, quarter)
            print(f"    {year} Q{quarter} (_as_at_date: {as_at_date})")

    if skipped:
        print("\n[*] Skipped quarters (already in MinIO):")
        for year, quarter in skipped:
            as_at_date = quarter_to_date(year, quarter)
            print(f"    {year} Q{quarter} (_as_at_date: {as_at_date})")

    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("""
    Data is now staged in MinIO!

    Next steps:
    1. Load data from MinIO to ducklake:
       python nport_load_from_minio.py --year 2023 --quarter 4  # Test single quarter
       python backfill_load_from_minio.py  # Load all quarters

    2. Verify data in MinIO with boto3 or mc client

    3. Set up incremental loads for new quarters
    """)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill N-PORT extraction to MinIO"
    )
    parser.add_argument(
        "--zip-dir",
        type=Path,
        help="Directory containing downloaded zip files (default: ./downloaded_zips)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-upload even if quarter exists in MinIO"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2019,
        help="First year to check (default: 2019)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Last year to check (default: 2025)"
    )

    args = parser.parse_args()

    try:
        backfill_extract_to_minio(
            zip_dir=args.zip_dir,
            force=args.force,
            start_year=args.start_year,
            end_year=args.end_year
        )
    except KeyboardInterrupt:
        print("\n\n[!] Extraction interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[!] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
