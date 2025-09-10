#!/usr/bin/env python3
"""
Lean example: list all groups with minimal code.

Defaults:
- unlimited (SDK paginates)

Usage:
  python examples/groups/list_all_groups.py [--limit 100]
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery


async def main():
    parser = argparse.ArgumentParser(description="List all groups (lean)")
    parser.add_argument("--limit", type=int, default=None, help="Max groups to fetch (default: all)")
    args = parser.parse_args()

    try:
        async with DataQuery() as dq:
            if args.limit is not None:
                groups = await dq.list_groups_async(limit=args.limit)
            else:
                groups = await dq.list_groups_async()
            print(f"Total groups: {len(groups)}")
            for i, g in enumerate(groups[:20], 1):
                print(f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}")
            if len(groups) > 20:
                print(f"... and {len(groups)-20} more")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
