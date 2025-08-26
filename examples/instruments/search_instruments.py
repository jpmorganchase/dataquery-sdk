#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def search_instruments_async():
    async with DataQuery() as dq:
        response = await dq.search_instruments_async(
            group_id="JPMAQS",
            keywords="Construction",
            page=None
        )
        print(response)

if __name__ == "__main__":
    asyncio.run(search_instruments_async())