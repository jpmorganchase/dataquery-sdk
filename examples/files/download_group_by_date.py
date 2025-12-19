#!/usr/bin/env python3
"""
Example: Group Parallel Download by Date Range

This example demonstrates how to download all files for a specific group within
a date range. It uses the SDK's parallel download capabilities for maximum
performance.

Usage:
    python download_group_by_date.py GROUP_ID START_DATE END_DATE DESTINATION
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402
from dataquery.models import DownloadProgress  # noqa: E402


def progress_callback(progress: DownloadProgress):
    """Callback to display download progress for the current file."""
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    # Use \r to overwrite the line for a clean progress bar
    print(f"\r[Downloading] {progress.file_group_id}: {pct}", end="", flush=True)


async def main():
    parser = argparse.ArgumentParser(
        description="Group Parallel Download by Date Range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument("start_date", help="Start date in YYYYMMDD format")
    parser.add_argument("end_date", help="End date in YYYYMMDD format")
    parser.add_argument("destination", help="Destination directory for downloads")
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="Max concurrent file downloads (default: 5)",
    )
    parser.add_argument(
        "--num-parts",
        type=int,
        default=5,
        help="Number of parallel parts per file (default: 5)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between downloads in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retry attempts for failed downloads (default: 3)",
    )

    args = parser.parse_args()

    # Basic date validation
    if not (len(args.start_date) == 8 and args.start_date.isdigit()):
        print(f"[Error] Start date must be YYYYMMDD. Got: {args.start_date}")
        return
    if not (len(args.end_date) == 8 and args.end_date.isdigit()):
        print(f"[Error] End date must be YYYYMMDD. Got: {args.end_date}")
        return

    dest_path = Path(args.destination)
    if not dest_path.exists():
        try:
            dest_path.mkdir(parents=True, exist_ok=True)
            print(f"[Info] Created directory: {dest_path}")
        except Exception as e:
            print(f"[Error] Could not create directory: {e}")
            return

    print("=" * 60)
    print(f"Group Download: {args.group_id}")
    print(f"Range: {args.start_date} to {args.end_date}")
    print(f"Destination: {dest_path}")
    print(f"Parallel: {args.max_concurrent} files × {args.num_parts} parts = {args.max_concurrent * args.num_parts} concurrent requests")
    print(f"Max Retries: {args.max_retries}")
    print("=" * 60)

    try:
        async with DataQuery() as dq:
            report = await dq.run_group_download_async(
                group_id=args.group_id,
                start_date=args.start_date,
                end_date=args.end_date,
                destination_dir=dest_path,
                max_concurrent=args.max_concurrent,
                num_parts=args.num_parts,
                delay_between_downloads=args.delay,
                max_retries=args.max_retries,
                progress_callback=progress_callback,
            )

            print()  # newline after progress

            if report.get("error"):
                print(f"\n[Warning] {report.get('error')}")
            else:
                print("\n[Success] Download Batch Completed")

            # Summary
            print("-" * 60)
            print(f"Total Files: {report.get('total_files', 0)}")
            print(f"Successful:  {report.get('successful_downloads', 0)}")
            print(f"Failed:      {report.get('failed_downloads', 0)}")

            if report.get("retries_attempted", 0) > 0:
                print(f"Retries:     {report.get('retries_attempted', 0)}/{report.get('max_retries', 3)}")

            if report.get("total_time_seconds"):
                print(f"Total Time:  {report.get('total_time_seconds'):.2f}s")

            # Failed files details
            if report.get("failed_files"):
                print("\n[Error] Failed Files:")
                for f in report.get("failed_files", []):
                    print(f"  - {f}")

            # Per-file timing (top 5 slowest)
            timing = report.get("per_file_timing", {})
            if timing and timing.get("file_times"):
                print("\n[Info] Top 5 Slowest Downloads:")
                sorted_files = sorted(
                    timing.get("file_times", []),
                    key=lambda x: x.get("download_time_seconds", 0),
                    reverse=True,
                )
                for f in sorted_files[:5]:
                    fid = f.get("file_group_id")
                    time_s = f.get("download_time_seconds", 0)
                    size = f.get("file_size_bytes", 0) / (1024 * 1024)
                    print(f"  - {fid}: {time_s:.2f}s ({size:.2f} MB)")

    except Exception as e:
        print(f"\n[Error] An unexpected error occurred: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Info] Operation cancelled.")
