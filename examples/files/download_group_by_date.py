#!/usr/bin/env python3
"""
Example: Parallel Group Download for 12 Months

This script downloads files for a group across 12 months,
with each month running as a separate download task.

The script processes months sequentially (one at a time) to avoid
authentication token conflicts and rate limiting issues that can occur
when multiple parallel downloads share the same OAuth session.

Usage:
    python download_group_by_date.py GROUP_ID YEAR DESTINATION
    python download_group_by_date.py SPECIALIST_SALES 2025 ./data

Options:
    --max-concurrent-files: Max concurrent file downloads per month (default: 5)
    --num-parts: Number of parallel parts per file download (default: 1)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
    --start-month: Starting month (1-12, default: 1)
    --end-month: Ending month (1-12, default: 12)

Rate Limiting:
    The SDK defaults to full 25 TPS capacity (1500 requests/minute).
    This can be adjusted via environment variables:
    - DATAQUERY_REQUESTS_PER_MINUTE (default: 1500)
    - DATAQUERY_BURST_CAPACITY (default: 25)
"""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from datetime import datetime
from calendar import monthrange

sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402


def get_month_range(year: int, month: int) -> tuple[str, str]:
    """Get start and end date for a given month in YYYYMMDD format."""
    start_date = f"{year}{month:02d}01"
    last_day = monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{last_day:02d}"
    return start_date, end_date


async def download_month(
    dq: DataQuery,
    group_id: str,
    year: int,
    month: int,
    destination_dir: Path,
    max_concurrent: int,
    num_parts: int,
    delay_between_downloads: float,
) -> dict:
    """Download files for a single month."""
    start_date, end_date = get_month_range(year, month)
    month_name = datetime(year, month, 1).strftime("%B")

    print(f"\n[Starting] {month_name} {year}: {start_date} to {end_date}")
    month_start_time = time.time()

    try:
        report = await dq.run_group_download_async(
            group_id=group_id,
            start_date=start_date,
            end_date=end_date,
            destination_dir=destination_dir / f"{year}_{month:02d}",
            max_concurrent=max_concurrent,
            num_parts=num_parts,
            delay_between_downloads=delay_between_downloads,
            max_retries=3,
        )

        elapsed = time.time() - month_start_time
        success_count = report.get("successful_downloads", 0)
        failed_count = report.get("failed_downloads", 0)
        total_count = report.get("total_files", 0)

        print(
            f"[Done] {month_name}: {success_count}/{total_count} files "
            f"({failed_count} failed) in {elapsed:.1f}s"
        )

        return {
            "month": month,
            "month_name": month_name,
            "report": report,
            "elapsed_seconds": elapsed,
        }

    except Exception as e:
        elapsed = time.time() - month_start_time
        print(f"[Error] {month_name}: {e} (after {elapsed:.1f}s)")
        return {
            "month": month,
            "month_name": month_name,
            "error": str(e),
            "elapsed_seconds": elapsed,
        }


async def run_sequential_downloads(
    dq: DataQuery,
    group_id: str,
    year: int,
    start_month: int,
    end_month: int,
    destination_dir: Path,
    max_concurrent_files: int,
    num_parts: int,
    delay_between_downloads: float,
) -> list[dict]:
    """Run downloads for each month sequentially to avoid token conflicts."""
    results = []

    for month in range(start_month, end_month + 1):
        result = await download_month(
            dq=dq,
            group_id=group_id,
            year=year,
            month=month,
            destination_dir=destination_dir,
            max_concurrent=max_concurrent_files,
            num_parts=num_parts,
            delay_between_downloads=delay_between_downloads,
        )
        results.append(result)

        # Small delay between months to allow rate limits to recover
        if month < end_month:
            await asyncio.sleep(2.0)

    return results


async def main():
    parser = argparse.ArgumentParser(
        description="Sequential Group Download for Multiple Months",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all 12 months of 2025
    python download_group_by_date.py SPECIALIST_SALES 2025 ./data

    # Download Q1 only (Jan-Mar)
    python download_group_by_date.py SPECIALIST_SALES 2025 ./data --start-month 1 --end-month 3

    # Download with higher concurrency per month
    python download_group_by_date.py SPECIALIST_SALES 2025 ./data --max-concurrent-files 10
        """,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument("year", type=int, help="Year to download (e.g., 2025)")
    parser.add_argument("destination", help="Destination directory for downloads")
    parser.add_argument(
        "--max-concurrent-files",
        type=int,
        default=5,
        help="Max concurrent file downloads per month (default: 5)",
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
    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
        choices=range(1, 13),
        metavar="1-12",
        help="Starting month (1-12, default: 1)",
    )
    parser.add_argument(
        "--end-month",
        type=int,
        default=12,
        choices=range(1, 13),
        metavar="1-12",
        help="Ending month (1-12, default: 12)",
    )

    args = parser.parse_args()

    # Validate month range
    if args.start_month > args.end_month:
        print("[Error] start-month must be <= end-month")
        sys.exit(1)

    dest_path = Path(args.destination)
    dest_path.mkdir(parents=True, exist_ok=True)

    num_months = args.end_month - args.start_month + 1
    total_start_time = time.time()

    print("=" * 60)
    print("Group Download - Sequential by Month")
    print("=" * 60)
    print(f"Group ID:            {args.group_id}")
    print(f"Year:                {args.year}")
    print(
        f"Months:              {args.start_month} to {args.end_month} ({num_months} months)"
    )
    print(f"Destination:         {dest_path}")
    print(f"Concurrent Files:    {args.max_concurrent_files}")
    print(f"Parts per File:      {args.num_parts}")
    print(f"Delay between files: {args.delay}s")

    # Show rate limit configuration
    rpm = os.getenv("DATAQUERY_REQUESTS_PER_MINUTE", "1500")
    burst = os.getenv("DATAQUERY_BURST_CAPACITY", "25")
    tps = int(rpm) / 60
    print(f"Rate Limit:          {rpm} RPM / {tps:.1f} TPS (burst: {burst})")
    print("=" * 60)

    try:
        async with DataQuery() as dq:
            results = await run_sequential_downloads(
                dq=dq,
                group_id=args.group_id,
                year=args.year,
                start_month=args.start_month,
                end_month=args.end_month,
                destination_dir=dest_path,
                max_concurrent_files=args.max_concurrent_files,
                num_parts=args.num_parts,
                delay_between_downloads=args.delay,
            )

        # Calculate summary
        total_elapsed = time.time() - total_start_time
        total_files = 0
        total_success = 0
        total_failed = 0
        months_with_errors = []

        for result in results:
            if "report" in result:
                report = result["report"]
                total_files += report.get("total_files", 0)
                total_success += report.get("successful_downloads", 0)
                total_failed += report.get("failed_downloads", 0)
            if "error" in result:
                months_with_errors.append(result["month_name"])

        # Print summary
        print("\n" + "=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Total Files Found:   {total_files}")
        print(f"Successful:          {total_success}")
        print(f"Failed:              {total_failed}")
        if total_files > 0:
            success_rate = (total_success / total_files) * 100
            print(f"Success Rate:        {success_rate:.1f}%")
        print(
            f"Total Time:          {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)"
        )

        if months_with_errors:
            print(f"\nMonths with errors:  {', '.join(months_with_errors)}")

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
