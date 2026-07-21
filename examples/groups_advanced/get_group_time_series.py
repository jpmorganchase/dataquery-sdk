#!/usr/bin/env python3
"""Get a time series for every instrument in a group (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

GROUP_ID = "FI_GO_BO_EA"
ATTRIBUTES = ["MIDPRC"]
START_DATE = "20240101"
END_DATE = "20240131"


async def main():
    async with DataQuery() as dq:
        page = await dq.get_group_time_series_async(
            group_id=GROUP_ID,
            attributes=ATTRIBUTES,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        while page is not None:
            for instrument in page.instruments or []:
                print(instrument)
            page = await dq.get_next_page_async(page)


if __name__ == "__main__":
    asyncio.run(main())
