#!/usr/bin/env python3
"""
Example: List Available Files by Date Range

This example demonstrates how to list available files within a specific
date range. This is useful for discovering what data is available for
download within a given time period.

Usage:
    python list_available_files.py --group-id <group_id> --start-date <YYYYMMDD> --end-date <YYYYMMDD>
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to import dataquery
sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import AuthenticationError  # noqa: E402


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="List available files by date range")
    parser.add_argument(
        "--group-id", required=True, help="ID of the group to list files from"
    )
    parser.add_argument("--file-group-id", help="Optional file group ID to filter by")
    parser.add_argument(
        "--start-date", help="Start date (YYYYMMDD). Defaults to 7 days ago."
    )
    parser.add_argument("--end-date", help="End date (YYYYMMDD). Defaults to today.")
    return parser.parse_args()


async def main():
    args = parse_args()

    # Default dates if not provided
    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y%m%d")
    if not args.start_date:
        args.start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

    print(f"[Info] Listing files for Group: {args.group_id}")
    print(f"[Info] Date Range: {args.start_date} to {args.end_date}")
    if args.file_group_id:
        print(f"[Info] Filter by File Group: {args.file_group_id}")

    try:
        async with DataQuery() as dq:
            files = await dq.list_available_files_async(
                group_id=args.group_id,
                file_group_id=args.file_group_id,
                start_date=args.start_date,
                end_date=args.end_date,
            )

            if not files:
                print("[Info] No files found for the given criteria.")
                return

            print(f"[Success] Found {len(files)} available files")
            print("-" * 60)
            print(f"{'File Name':<40} {'Date':<10} {'Size':<15}")
            print("-" * 60)

            if len(files) > 20:
                print(f"... and {len(files) - 20} more")

    except AuthenticationError:
        print("[Error] Authentication failed. Please check your credentials.")
    except Exception as e:
        print(f"[Error] An unexpected error occurred: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Info] Operation cancelled by user.")
