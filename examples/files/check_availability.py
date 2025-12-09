#!/usr/bin/env python3
"""
Lean example: interactively check file availability for a given file_group_id and date.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import AuthenticationError, NotFoundError  # noqa: E402, NotFoundError


async def main():
    print("[Start] Check Availability (lean)")
    file_group_id = input("Enter file_group_id: ").strip()
    if not file_group_id:
        print("[Error] file_group_id is required")
        return
    date = input("Enter date (YYYYMMDD): ").strip()
    if not date:
        print("[Error] date is required")
        return
    try:
        async with DataQuery() as dq:
            try:
                availability = await dq.check_availability_async(file_group_id, date)
            except NotFoundError:
                print("[Info] Not found")
                return
            if availability:
                has = bool(getattr(availability, "is_available", False))
                print("[Success] Available" if has else "[Error] Not available")
            else:
                print("[Error] Not available")
    except AuthenticationError as e:
        print(f"[Error] Authentication failed: {e}")
    except Exception as e:
        print(f"[Error] Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
