#!/usr/bin/env python3
"""
Lean example: search groups using core API with simple CLI args.

Defaults:
- keyword: "economy"
- limit: 10

Usage:
  python examples/groups/search_groups.py --keyword rates --limit 5
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402


async def main():
    parser = argparse.ArgumentParser(description="Search groups (lean)")
    parser.add_argument(
        "--keyword", default="economy", help="Search keyword (default: economy)"
    )
    parser.add_argument(
        "--limit", type=int, default=10, help="Max results (default: 10)"
    )
    args = parser.parse_args()

    try:
        async with DataQuery() as dq:
            results = await dq.search_groups_async(args.keyword, limit=args.limit)
            print(f"Found {len(results)} groups")
            for i, g in enumerate(results, 1):
                print(
                    f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}"
                )
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
