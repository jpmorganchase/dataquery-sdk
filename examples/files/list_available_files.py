#!/usr/bin/env python3
"""List files available for a group within a date range."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "JPMAQS_GENERIC_RETURNS"
START_DATE = "20250101"
END_DATE = "20250131"


async def main():
    async with DataQuery() as dq:
        files = await dq.list_available_files_async(
            group_id=GROUP_ID,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        for entry in files:
            print(entry)


if __name__ == "__main__":
    asyncio.run(main())
