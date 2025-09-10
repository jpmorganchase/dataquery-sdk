#!/usr/bin/env python3
"""
Lean example: get group filters using core API with simple CLI args.

Defaults:
- If --group-id is not provided, the first available group is used
- --show controls how many filters to print (default: 5)

Usage:
  python examples/groups_advanced/get_group_filters.py [--group-id <id>] [--show 5]
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError


async def main() -> None:
    parser = argparse.ArgumentParser(description="Get group filters (lean)")
    parser.add_argument("--group-id", help="Group ID. If omitted, uses the first available group")
    parser.add_argument("--show", type=int, default=5, help="How many filters to print (default: 5)")
    args = parser.parse_args()

    try:
        async with DataQuery() as dq:
            group_id = args.group_id
            if not group_id:
                groups = await dq.list_groups_async(limit=1)
                if not groups:
                    print("No groups available")
                    return
                group_id = groups[0].group_id

            resp = await dq.get_group_filters_async(group_id)

            filters = getattr(resp, "filters", []) or []
            print(f"Filters: {len(filters)}")
            for i, f in enumerate(filters[: args.show], 1):
                name = f.get("name", "")
                ftype = f.get("type", "")
                print(f"{i}. {name} ({ftype})")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
