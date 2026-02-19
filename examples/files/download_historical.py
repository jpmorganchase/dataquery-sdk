#!/usr/bin/env python3
"""
Example: Historical Download with Monthly Chunking

Downloads files for a group across a large date range by splitting
into monthly chunks. This avoids timeouts and memory issues when
downloading many months of data at once.

If the date range is within a single month, it downloads directly.
If the range spans multiple months, it splits into monthly chunks
and downloads each month sequentially.

Usage:
    python download_historical.py GROUP_ID FROM_DATE TO_DATE DESTINATION
    python download_historical.py SPECIALIST_SALES 20240101 20251231 ./data

Options:
    --max-concurrent-files: Max concurrent file downloads per month (default: 5)
    --num-parts: Number of parallel parts per file download (default: 1)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
"""

import argparse
import asyncio
import sys
import time
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402


def split_into_monthly_ranges(from_date: str, to_date: str) -> list[tuple[str, str]]:
    """Split a date range into monthly chunks.

    Args:
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format

    Returns:
        List of (start_date, end_date) tuples, each covering at most one month.
    """
    start = datetime.strptime(from_date, "%Y%m%d").date()
    end = datetime.strptime(to_date, "%Y%m%d").date()

    ranges = []
    current = start

    while current <= end:
        # End of current month
        last_day = monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)

        # Clamp to the overall end date
        chunk_end = min(month_end, end)

        ranges.append((current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))

        # Move to first day of next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return ranges


async def download_chunk(
    dq: DataQuery,
    group_id: str,
    start_date: str,
    end_date: str,
    destination_dir: Path,
    max_concurrent: int,
    num_parts: int,
    delay: float,
) -> dict:
    """Download files for a single monthly chunk."""
    print(f"\n[Starting] {start_date} to {end_date}")
    chunk_start = time.time()

    try:
        report = await dq.run_group_download_async(
            group_id=group_id,
            start_date=start_date,
            end_date=end_date,
            destination_dir=destination_dir,
            max_concurrent=max_concurrent,
            num_parts=num_parts,
            delay_between_downloads=delay,
            max_retries=3,
        )

        elapsed = time.time() - chunk_start
        success = report.get("successful_downloads", 0)
        failed = report.get("failed_downloads", 0)
        total = report.get("total_files", 0)

        print(f"[Done] {start_date}-{end_date}: {success}/{total} files ({failed} failed) in {elapsed:.1f}s")

        return {
            "start_date": start_date,
            "end_date": end_date,
            "report": report,
            "elapsed_seconds": elapsed,
        }

    except Exception as e:
        elapsed = time.time() - chunk_start
        print(f"[Error] {start_date}-{end_date}: {e} (after {elapsed:.1f}s)")
        return {
            "start_date": start_date,
            "end_date": end_date,
            "error": str(e),
            "elapsed_seconds": elapsed,
        }


async def main():
    parser = argparse.ArgumentParser(
        description="Historical Download with Monthly Chunking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download full year 2025
    python download_historical.py SPECIALIST_SALES 20250101 20251231 ./data

    # Download 6 months across year boundary
    python download_historical.py SPECIALIST_SALES 20241001 20250331 ./data

    # Download with higher concurrency per chunk
    python download_historical.py SPECIALIST_SALES 20240101 20251231 ./data --max-concurrent-files 10
        """,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument("from_date", help="Start date in YYYYMMDD format (e.g. 20240101)")
    parser.add_argument("to_date", help="End date in YYYYMMDD format (e.g. 20251231)")
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

    args = parser.parse_args()

    # Validate dates
    try:
        start = datetime.strptime(args.from_date, "%Y%m%d")
        end = datetime.strptime(args.to_date, "%Y%m%d")
    except ValueError:
        print("[Error] Dates must be in YYYYMMDD format")
        sys.exit(1)

    if start > end:
        print("[Error] from_date must be before or equal to to_date")
        sys.exit(1)

    dest_path = Path(args.destination)
    dest_path.mkdir(parents=True, exist_ok=True)

    # Split into monthly chunks
    monthly_ranges = split_into_monthly_ranges(args.from_date, args.to_date)

    total_start = time.time()

    print("=" * 60)
    print("Historical Download - Monthly Chunking")
    print("=" * 60)
    print(f"Group ID:       {args.group_id}")
    print(f"From Date:      {args.from_date}")
    print(f"To Date:        {args.to_date}")
    print(f"Monthly Chunks: {len(monthly_ranges)}")
    print(f"Destination:    {dest_path}")
    print(f"Concurrent:     {args.max_concurrent_files}")
    print(f"Parts per File: {args.num_parts}")
    print("=" * 60)

    for i, (s, e) in enumerate(monthly_ranges, 1):
        print(f"  Chunk {i}: {s} -> {e}")

    try:
        async with DataQuery() as dq:
            results = []
            for i, (chunk_start, chunk_end) in enumerate(monthly_ranges):
                result = await download_chunk(
                    dq=dq,
                    group_id=args.group_id,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    destination_dir=dest_path,
                    max_concurrent=args.max_concurrent_files,
                    num_parts=args.num_parts,
                    delay=args.delay,
                )
                results.append(result)

                # Small delay between chunks to let rate limits recover
                if i < len(monthly_ranges) - 1:
                    await asyncio.sleep(2.0)

        # Summary
        total_elapsed = time.time() - total_start
        total_files = 0
        total_success = 0
        total_failed = 0
        chunks_with_errors = []

        for result in results:
            if "report" in result:
                report = result["report"]
                total_files += report.get("total_files", 0)
                total_success += report.get("successful_downloads", 0)
                total_failed += report.get("failed_downloads", 0)
            if "error" in result:
                chunks_with_errors.append(f"{result['start_date']}-{result['end_date']}")

        print(f"\n{'=' * 60}")
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Chunks:         {len(monthly_ranges)}")
        print(f"Total Files:    {total_files}")
        print(f"Successful:     {total_success}")
        print(f"Failed:         {total_failed}")
        if total_files > 0:
            print(f"Success Rate:   {(total_success / total_files) * 100:.1f}%")
        print(f"Total Time:     {total_elapsed:.1f}s ({total_elapsed / 60:.1f} minutes)")

        if chunks_with_errors:
            print(f"\nChunks with errors: {', '.join(chunks_with_errors)}")

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
