#!/usr/bin/env python3
"""
Example: Download Group Files by Date Range

Downloads all files for a group between a from-date and to-date.

Usage:
    python download_group_by_date.py GROUP_ID FROM_DATE TO_DATE DESTINATION
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data

Options:
    --max-concurrent-files: Max concurrent file downloads (default: 5)
    --num-parts: Number of parallel parts per file download (default: 1)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402


async def main():
    parser = argparse.ArgumentParser(
        description="Download Group Files by Date Range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download January 2025
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data

    # Download a single day
    python download_group_by_date.py SPECIALIST_SALES 20250115 20250115 ./data

    # Download with higher concurrency
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data --max-concurrent-files 10
        """,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument("from_date", help="Start date in YYYYMMDD format (e.g. 20250101)")
    parser.add_argument("to_date", help="End date in YYYYMMDD format (e.g. 20250131)")
    parser.add_argument("destination", help="Destination directory for downloads")
    parser.add_argument(
        "--max-concurrent-files",
        type=int,
        default=5,
        help="Max concurrent file downloads (default: 5)",
    )
    parser.add_argument(
        "--num-parts",
        type=int,
        default=1,
        help="Number of parallel parts per file download (default: 1)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.04,
        help="Delay between file downloads in seconds (default: 0.04 for 25 TPS)",
    )

    args = parser.parse_args()

    dest_path = Path(args.destination)
    dest_path.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Group Download by Date Range")
    print("=" * 60)
    print(f"Group ID:       {args.group_id}")
    print(f"From Date:      {args.from_date}")
    print(f"To Date:        {args.to_date}")
    print(f"Destination:    {dest_path}")
    print(f"Concurrent:     {args.max_concurrent_files}")
    print(f"Parts per File: {args.num_parts}")
    print("=" * 60)

    start_time = time.time()

    try:
        async with DataQuery() as dq:
            report = await dq.run_group_download_async(
                group_id=args.group_id,
                start_date=args.from_date,
                end_date=args.to_date,
                destination_dir=dest_path,
                max_concurrent=args.max_concurrent_files,
                num_parts=args.num_parts,
                delay_between_downloads=args.delay,
                max_retries=3,
            )

        elapsed = time.time() - start_time
        total = report.get("total_files", 0)
        success = report.get("successful_downloads", 0)
        failed = report.get("failed_downloads", 0)

        print(f"\n{'=' * 60}")
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Total Files:    {total}")
        print(f"Successful:     {success}")
        print(f"Failed:         {failed}")
        if total > 0:
            print(f"Success Rate:   {(success / total) * 100:.1f}%")
        print(f"Total Time:     {elapsed:.1f}s ({elapsed / 60:.1f} minutes)")
        print("=" * 60)

    except Exception as e:
        print(f"\n[Error] {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[Info] Operation cancelled by user.")
