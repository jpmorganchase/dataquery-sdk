#!/usr/bin/env python3
"""
Example: Download File

This example demonstrates how to download a specific file using its file group ID
and optional date. It supports parallel downloading for faster performance.

Usage:
    python download_file.py --file-group-id ID [--date DATE] [--dest DIR] [--overwrite]
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402
from dataquery.models import DownloadOptions, DownloadProgress  # noqa: E402


def simple_progress_callback(progress: DownloadProgress):
    """Callback to display download progress."""
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    # Use \r to overwrite the line for a clean progress bar
    print(f"\r[Progress] {pct} ({progress.bytes_downloaded:,} bytes)", end="", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Download File Example")
    parser.add_argument(
        "--file-group-id",
        required=True,
        help="ID of the file group to download"
    )
    parser.add_argument(
        "--date",
        help="File date (YYYYMMDD, YYYYMMDDTHHMM, etc.). Optional."
    )
    parser.add_argument(
        "--dest",
        default="./downloads",
        help="Destination directory. Defaults to ./downloads"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing file if it exists"
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    print(f"[Start] Download File: {args.file_group_id}")
    if args.date:
        print(f"[Info] Date: {args.date}")
    print(f"[Info] Destination: {args.dest}")

    options = DownloadOptions(overwrite_existing=args.overwrite)
    destination_path = Path(args.dest)

    try:
        async with DataQuery() as dq:
            # Use the SDK download method
            result = await dq.download_file_async(
                file_group_id=args.file_group_id,
                file_datetime=args.date,
                destination_path=destination_path,
                options=options,
                progress_callback=simple_progress_callback,
            )

            print()  # newline after progress

            if result and result.status == "completed":
                print("[Success] Download completed")
                print(f"[File] Saved to: {result.local_path}")
                print(f"[Info] Size: {result.file_size:,} bytes")
                if result.download_time:
                    print(f"[Time] Duration: {result.download_time:.2f}s")
                if result.speed_mbps:
                    print(f"[Speed] Speed: {result.speed_mbps:.2f} MB/s")
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error')
                print(f"[Error] Download failed: {error_msg}")

    except Exception as e:
        print(f"\n[Error] An unexpected error occurred: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Info] Operation cancelled.")
