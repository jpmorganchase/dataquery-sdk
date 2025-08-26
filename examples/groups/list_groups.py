#!/usr/bin/env python3
"""
Lean example: interactively list groups.

Prompts for limit and prints basic info. Press Ctrl+C to exit.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError


async def main():
    print("🚀 List Groups (lean)")
    try:
        limit_raw = input("Enter limit [10]: ").strip() or "10"
        limit = int(limit_raw)
    except ValueError:
        print("❌ Invalid limit")
        return

    try:
        async with DataQuery() as dq:
            groups = await dq.list_groups_async(limit=limit)
            print(f"✅ Found {len(groups)} groups")
            for i, g in enumerate(groups, 1):
                print(f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())


 
