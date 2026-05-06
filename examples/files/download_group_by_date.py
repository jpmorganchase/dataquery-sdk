#!/usr/bin/env python3
"""
Example: Download Group Files by Date Range

Downloads files for a group between a from-date and to-date.
Optionally filter to one or more file-group-ids.

Usage:
    python download_group_by_date.py GROUP_ID FROM_DATE TO_DATE DESTINATION
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data

    # Filter to specific file-group-ids
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data \
        --file-group-id FG_ABC FG_DEF

Options:
    --file-group-id: One or more file-group-ids to download (omit for all)
    --max-concurrent-files: Max concurrent file downloads (default: 5)
    --num-parts: Parallel range parts per file (default: 1, single stream)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv  # noqa: E402

from dataquery import DataQuery  # noqa: E402

load_dotenv()


async def main():
    parser = argparse.ArgumentParser(
        description="Download Group Files by Date Range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all files in January 2025
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data

    # Download a single day
    python download_group_by_date.py SPECIALIST_SALES 20250115 20250115 ./data

    # Download only specific file-group-ids
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data \\
        --file-group-id FG_ABC FG_DEF FG_XYZ

    # Download with parallel range requests per file
    python download_group_by_date.py SPECIALIST_SALES 20250101 20250131 ./data \\
        --num-parts 4 --max-concurrent-files 5
        """,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument("from_date", help="Start date in YYYYMMDD format (e.g. 20250101)")
    parser.add_argument("to_date", help="End date in YYYYMMDD format (e.g. 20250131)")
    parser.add_argument("destination", help="Destination directory for downloads")
    parser.add_argument(
        "--file-group-id",
        nargs="+",
        default=None,
        help="One or more file-group-ids to download (omit for all files in the group)",
    )
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
        help="Parallel range parts per file (default: 1, single stream). Set >1 for large files.",
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

    # Normalise file_group_id: single string if one value, list if many, None if omitted.
    file_group_id = args.file_group_id
    if file_group_id and len(file_group_id) == 1:
        file_group_id = file_group_id[0]

    print("=" * 60)
    print("Group Download by Date Range")
    print("=" * 60)
    print(f"Group ID:        {args.group_id}")
    print(f"File Group IDs:  {file_group_id or '(all)'}")
    print(f"From Date:       {args.from_date}")
    print(f"To Date:         {args.to_date}")
    print(f"Destination:     {dest_path}")
    print(f"Concurrent:      {args.max_concurrent_files}")
    print(f"Parts per File:  {args.num_parts}")
    print("=" * 60)

    start_time = time.time()

    try:
        async with DataQuery() as dq:
            report = await dq.run_group_download_async(
                group_id=args.group_id,
                start_date=args.from_date,
                end_date=args.to_date,
                destination_dir=dest_path,
                file_group_id=file_group_id,
                max_concurrent=args.max_concurrent_files,
                num_parts=args.num_parts,
                delay_between_downloads=args.delay,
                max_retries=3,
            )

        elapsed = time.time() - start_time
        total = report.counts.get("total_files", 0)
        success = report.counts.get("successful_downloads", 0)
        failed = report.counts.get("failed_downloads", 0)

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
