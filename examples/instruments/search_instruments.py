#!/usr/bin/env python3
"""Search instruments by keyword within a group."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "FI_GO_BO_EA"
KEYWORD = "market"


async def main():
    async with DataQuery() as dq:
        response = await dq.search_instruments_async(group_id=GROUP_ID, keywords=KEYWORD)
        for instrument in response.instruments or []:
            print(instrument)


if __name__ == "__main__":
    asyncio.run(main())
