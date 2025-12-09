#!/usr/bin/env python3
"""
Lean example: search instruments with defaults and simple CLI args.

Defaults:
- If --group-id is not provided, the first available group is used
- --keyword defaults to "market"
- --show controls how many results to print (default: 10)

Usage:
  python examples/instruments/search_instruments.py [--group-id <id>] [--keyword market] [--show 10]
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import DataQueryError  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Search instruments (lean)")
    parser.add_argument(
        "--group-id", help="Group ID. If omitted, uses the first available group"
    )
    parser.add_argument(
        "--keyword", default="market", help="Search keyword (default: market)"
    )
    parser.add_argument(
        "--show", type=int, default=10, help="How many results to print (default: 10)"
    )
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

            resp = await dq.search_instruments_async(
                group_id=group_id, keywords=args.keyword
            )

            instruments = getattr(resp, "instruments", []) or []
            print(f"Found: {len(instruments)} instruments for '{args.keyword}'")
            for i, inst in enumerate(instruments[: args.show], 1):
                name = inst.get("name", "")
                inst_id = inst.get("instrument_id", "")
                print(f"{i}. {name} ({inst_id})")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
