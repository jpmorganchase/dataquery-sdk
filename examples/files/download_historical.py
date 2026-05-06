#!/usr/bin/env python3
"""Download a historical date range, chunked monthly by the SDK."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "JPMAQS_GENERIC_RETURNS"
START_DATE = "20240101"
END_DATE = "20241231"


async def main():
    async with DataQuery() as dq:
        summary = await dq.download_historical_async(
            group_id=GROUP_ID,
            start_date=START_DATE,
            end_date=END_DATE,
            destination_dir=Path("./downloads"),
        )
        print(summary)


if __name__ == "__main__":
    asyncio.run(main())
