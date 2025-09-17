"""Command Line Interface for DataQuery SDK."""
import asyncio
import argparse
import sys
from pathlib import Path

from dataquery import DataQuery
from dataquery.models import DownloadOptions


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="DataQuery SDK CLI")
    subparsers = parser.add_subparsers(dest="command")
    
    # Download group command
    group_parser = subparsers.add_parser("download-group")
    group_parser.add_argument("group_id")
    group_parser.add_argument("start_date")
    group_parser.add_argument("end_date")
    group_parser.add_argument("destination")
    group_parser.add_argument("--max-concurrent", type=int, default=3)
    group_parser.add_argument("--num-parts", type=int, default=5)
    
    args = parser.parse_args()
    
    if args.command == "download-group":
        asyncio.run(download_group(args))
    else:
        parser.print_help()


async def download_group(args):
    """Download group files."""
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id=args.group_id,
            start_date=args.start_date,
            end_date=args.end_date,
            destination_dir=Path(args.destination),
            max_concurrent=args.max_concurrent,
            num_parts=args.num_parts
        )
        print(f"Downloaded {report['successful_downloads']} files")


if __name__ == "__main__":
    main()