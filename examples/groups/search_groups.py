#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def search_groups_async():
    async with DataQuery() as dq:
        results = await dq.search_groups_async("example_keyword", limit=10)
        print(results)

if __name__ == "__main__":
    asyncio.run(search_groups_async())