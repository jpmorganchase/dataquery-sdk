#!/usr/bin/env python3
"""
Lean example: list instruments with defaults and simple CLI args.

Defaults:
- If --group-id is not provided, the first available group is used
- --limit defaults to 10, --offset defaults to 0

Usage:
  python examples/instruments/list_instruments.py [--group-id <id>] [--limit 10] [--offset 0]
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
    parser = argparse.ArgumentParser(description="List instruments (lean)")
    parser.add_argument("--group-id", help="Group ID. If omitted, uses the first available group")
    parser.add_argument("--limit", type=int, default=10, help="Max instruments to list (default: 10)")
    parser.add_argument("--offset", type=int, default=0, help="Offset for pagination (default: 0)")
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

            resp = await dq.list_instruments_async(group_id=group_id, limit=args.limit, offset=args.offset)

            items = getattr(resp, "items", 0) or 0
            instruments = getattr(resp, "instruments", []) or []
            print(f"Instruments: {items}")
            for i, inst in enumerate(instruments[: min(args.limit, 10)], 1):
                inst_id = getattr(inst, "instrument_id", None) or inst.get("instrument_id", "")
                name = getattr(inst, "instrument_name", None) or inst.get("name", "")
                print(f"{i}. {inst_id} {('- ' + name) if name else ''}")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
