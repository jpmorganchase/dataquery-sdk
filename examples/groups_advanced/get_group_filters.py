#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def get_group_filters_async():
    async with DataQuery() as dq:
        filters_response = await dq.get_group_filters_async("EXAMPLE_GROUP_ID")
        print(filters_response)

if __name__ == "__main__":
    asyncio.run(get_group_filters_async())