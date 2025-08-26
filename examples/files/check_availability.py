#!/usr/bin/env python3
"""
Lean example: interactively check file availability for a given file_group_id and date.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError, NotFoundError


async def main():
    print("🚀 Check Availability (lean)")
    file_group_id = input("Enter file_group_id: ").strip()
    if not file_group_id:
        print("❌ file_group_id is required")
        return
    date = input("Enter date (YYYYMMDD): ").strip()
    if not date:
        print("❌ date is required")
        return
    try:
        async with DataQuery() as dq:
            try:
                availability = await dq.check_availability_async(file_group_id, date)
            except NotFoundError:
                print("📭 Not found")
                return
            print(availability)
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
