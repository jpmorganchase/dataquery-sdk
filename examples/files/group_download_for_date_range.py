#!/usr/bin/env python3
"""
Example: Group Download for Date Range (Sync)

This example demonstrates how to download all files in a group for a date range
using the DATAQUERY SDK with synchronous methods.

Usage:
    python examples/08_group_download_for_date_range.py --group-id <GROUP_ID> --start-date <YYYYMMDD> --end-date <YYYYMMDD>
"""
import sys
from pathlib import Path
import argparse

# Add the parent directory to the path so we can import the SDK
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataquery import DataQuery, setup_logging

def main():
    parser = argparse.ArgumentParser(description="Group Download for Date Range Example (Sync)")
    parser.add_argument("--group-id", required=False, help="Group ID")
    parser.add_argument("--start-date", required=False, help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", required=False, help="End date (YYYYMMDD)")
    parser.add_argument("--destination", default="./downloads", help="Destination directory")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Maximum concurrent downloads")
    args = parser.parse_args()

    # Setup logging
    setup_logging("INFO")

    # Prompt if not provided
    group_id = args.group_id or input("Enter group ID: ").strip()
    start_date = args.start_date or input("Enter start date (YYYYMMDD): ").strip()
    end_date = args.end_date or input("Enter end date (YYYYMMDD): ").strip()
    destination = Path(args.destination)
    max_concurrent = args.max_concurrent

    try:
        # Create DataQuery instance (automatically loads config from .env)
        dataquery = DataQuery()

        print(f"\n🚀 Downloading all files for group '{group_id}' from {start_date} to {end_date}...")
        report = dataquery.run_group_download(
            group_id,
            start_date=start_date,
            end_date=end_date,
            destination_dir=destination,
            max_concurrent=max_concurrent
        )
        print("\n✅ Group download for date range completed!")
        print(f"Summary: {report}")

        # Cleanup resources
        dataquery.cleanup()

        return 0

    except Exception as e:
        print(f"FAILED: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)