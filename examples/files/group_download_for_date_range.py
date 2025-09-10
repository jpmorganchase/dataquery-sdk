import sys

import argparse
from pathlib import Path

from dataquery import DataQuery

import asyncio


async def main():
    parser = argparse.ArgumentParser(description="Group Download for Date Range Example (Async)")
    parser.add_argument("--group-id", required=False, help="Group ID")
    parser.add_argument("--start-date", required=False, help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", required=False, help="End date (YYYYMMDD)")
    parser.add_argument("--destination", default="./downloads", help="Destination directory")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Maximum concurrent downloads")
    args = parser.parse_args()

    group_id = args.group_id or input("Enter group ID: ").strip()
    start_date = args.start_date or input("Enter start date (YYYYMMDD): ").strip()
    end_date = args.end_date or input("Enter end date (YYYYMMDD): ").strip()
    destination = Path(args.destination)
    max_concurrent = args.max_concurrent

    try:
        async with DataQuery() as dq:

            report = await dq.run_group_download_async(
                group_id,
                start_date=start_date,
                end_date=end_date,
                destination_dir=destination,
                max_concurrent=max_concurrent
            )
        print("\n✅ Group download for date range completed!")
        print(f"Summary: {report}")
        return 0

    except Exception as e:
        print(f"FAILED: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
