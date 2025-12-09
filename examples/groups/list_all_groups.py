#!/usr/bin/env python3
"""
Lean example: list all groups with minimal code.

Defaults:
- unlimited (SDK paginates)

Usage:
  python examples/groups/list_all_groups.py [--limit 100]
"""

import argparse
import asyncio
import sys
from pathlib import Path

import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402


async def main():
    parser = argparse.ArgumentParser(description="List all groups (lean)")
    parser.add_argument(
        "--limit", type=int, default=None, help="Max groups to fetch (default: all)"
    )
    args = parser.parse_args()

    try:
        async with DataQuery() as dq:
            start_time = time.monotonic()  # Start timer
            if args.limit is not None:
                groups = await dq.list_groups_async(limit=args.limit)
            else:
                groups = await dq.list_groups_async()
            end_time = time.monotonic()  # End timer
            elapsed = end_time - start_time  # Calculate elapsed time
            print(f"Total groups: {len(groups)} (fetched in {elapsed:.2f}s)")
            for i, g in enumerate(groups[:20], 1):
                print("-" * 80)
                print(
                    f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}"
                )
            if len(groups) > 20:
                print(f"... and {len(groups) - 20} more")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
