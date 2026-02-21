#!/usr/bin/env python3
"""
Example: Historical Download with Monthly Chunking

Downloads files for a group across a large date range by splitting
into monthly chunks. This avoids timeouts and memory issues when
downloading many months of data at once.

Uses the built-in DataQuery.download_historical_async() method which
handles monthly chunking automatically.

Usage:
    python download_historical.py GROUP_ID FROM_DATE TO_DATE DESTINATION
    python download_historical.py SPECIALIST_SALES 20240101 20251231 ./data

Options:
    --max-concurrent-files: Max concurrent file downloads per month (default: 5)
    --num-parts: Number of parallel parts per file download (default: 1)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
    --chunk-delay: Delay between monthly chunks in seconds (default: 2.0)
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402


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
    parser.add_argument(
        "--chunk-delay",
        type=float,
        default=2.0,
        help="Delay between monthly chunks in seconds (default: 2.0)",
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

    print("=" * 60)
    print("Historical Download - Monthly Chunking")
    print("=" * 60)
    print(f"Group ID:       {args.group_id}")
    print(f"From Date:      {args.from_date}")
    print(f"To Date:        {args.to_date}")
    print(f"Destination:    {dest_path}")
    print(f"Concurrent:     {args.max_concurrent_files}")
    print(f"Parts per File: {args.num_parts}")
    print(f"Chunk Delay:    {args.chunk_delay}s")
    print("=" * 60)

    try:
        async with DataQuery() as dq:
            summary = await dq.download_historical_async(
                group_id=args.group_id,
                start_date=args.from_date,
                end_date=args.to_date,
                destination_dir=dest_path,
                max_concurrent=args.max_concurrent_files,
                num_parts=args.num_parts,
                delay_between_downloads=args.delay,
                max_retries=3,
                chunk_delay=args.chunk_delay,
            )

        # Print per-chunk results
        for chunk in summary.get("chunk_results", []):
            status = "Error" if "error" in chunk else "Done"
            s = chunk.get("successful_downloads", 0)
            t = chunk.get("total_files", 0)
            f = chunk.get("failed_downloads", 0)
            e = chunk.get("elapsed_seconds", 0)
            if "error" in chunk:
                print(f"  [{status}] {chunk['start_date']}-{chunk['end_date']}: {chunk['error']} ({e:.1f}s)")
            else:
                print(f"  [{status}] {chunk['start_date']}-{chunk['end_date']}: {s}/{t} files ({f} failed) in {e:.1f}s")

        # Print summary
        total_files = summary.get("total_files", 0)
        total_success = summary.get("successful_downloads", 0)
        total_failed = summary.get("failed_downloads", 0)
        total_time = summary.get("total_time_formatted", "N/A")
        chunks = summary.get("monthly_chunks", 0)
        chunks_with_errors = summary.get("chunks_with_errors", [])

        print(f"\n{'=' * 60}")
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        print(f"Chunks:         {chunks}")
        print(f"Total Files:    {total_files}")
        print(f"Successful:     {total_success}")
        print(f"Failed:         {total_failed}")
        if total_files > 0:
            print(f"Success Rate:   {(total_success / total_files) * 100:.1f}%")
        print(f"Total Time:     {total_time}")

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
