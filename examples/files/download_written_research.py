#!/usr/bin/env python3
"""Download every file in a group between two dates and unzip any ZIP archives
as they are downloaded."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.utils import download_zip_async  # noqa: E402

GROUP_ID = "RESEARCH_EQUITY_ALL"
# Trailing two weeks up to yesterday. Current-day archives are skipped by the
# unzip step anyway, since their content may still be updating.
END_DATE = "20161031"
START_DATE = "20161001"


async def main():
    async with DataQuery() as dq:
        result = await download_zip_async(
            dq,
            group_id=GROUP_ID,
            start_date=START_DATE,
            end_date=END_DATE,
            destination_dir=Path("./downloads"),
        )
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
