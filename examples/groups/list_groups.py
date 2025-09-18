#!/usr/bin/env python3
"""
Lean example: list groups using core API with simple CLI args.

Defaults:
- limit: 10

Usage:
  python examples/groups/list_groups.py --limit 20
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery


async def main():
    parser = argparse.ArgumentParser(description="List groups (lean)")
    parser.add_argument(
        "--limit", type=int, default=10, help="Max groups to list (default: 10)"
    )
    args = parser.parse_args()

    try:
        async with DataQuery() as dq:
            groups = await dq.list_groups_async(limit=args.limit)
            print(f"Found {len(groups)} groups")
            for i, g in enumerate(groups, 1):
                print(
                    f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}"
                )
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
