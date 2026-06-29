#!/usr/bin/env python3
"""Download every file in a group between two dates, chunking calls to the
available-files endpoint by ``DEFAULT_WRITTEN_RESEARCH_CHUNK_DAYS`` days."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.utils import run_group_download_chunked_async  # noqa: E402

GROUP_ID = "RESEARCH_EQUITY_ALL"
START_DATE = "20260616" # 14 days before current date
END_DATE = "20260629"


async def main():
    async with DataQuery() as dq:
        result = await run_group_download_chunked_async(
            dq,
            group_id=GROUP_ID,
            start_date=START_DATE,
            end_date=END_DATE,
            destination_dir=Path("./downloads"),
        )
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
