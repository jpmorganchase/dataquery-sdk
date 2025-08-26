#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def get_group_attributes_async():
    async with DataQuery() as dq:
        attributes_response = await dq.get_group_attributes_async(
            group_id="JPMAQS",
            page=None
        )
        print(attributes_response)

if __name__ == "__main__":
    asyncio.run(get_group_attributes_async())