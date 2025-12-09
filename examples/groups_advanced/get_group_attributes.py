#!/usr/bin/env python3
"""
Lean example: get group attributes using core API with simple CLI args.

Defaults:
- If --group-id is not provided, the first available group is used
- --show controls how many attributes to print (default: 5)

Usage:
  python examples/groups_advanced/get_group_attributes.py \
    [--group-id <id>] [--instrument-id <instrument>] [--page <token>] [--show 5]
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
    parser = argparse.ArgumentParser(description="Get group attributes (lean)")
    parser.add_argument(
        "--group-id", help="Group ID. If omitted, uses the first available group"
    )
    parser.add_argument(
        "--instrument-id", help="Instrument ID to filter results (optional)"
    )
    parser.add_argument("--page", help="Pagination token (optional)")
    parser.add_argument(
        "--show", type=int, default=5, help="How many attributes to print (default: 5)"
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

            resp = await dq.get_group_attributes_async(
                group_id=group_id,
                instrument_id=getattr(args, "instrument_id", None),
                page=args.page,
            )

            attributes = getattr(resp, "attributes", []) or []
            print(f"Attributes: {len(attributes)}")
            for i, attr in enumerate(attributes[: args.show], 1):
                name = attr.get("name", "")
                attr_type = attr.get("type", "")
                print(f"{i}. {name} ({attr_type})")

            if hasattr(resp, "pagination") and resp.pagination:
                next_page = resp.pagination.get("next_page")
                if next_page:
                    print(f"Next page token: {next_page}")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
