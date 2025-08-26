#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def list_instruments_async():
    async with DataQuery() as dq:
        instruments = await dq.list_instruments_async(
            group_id="JPMAQS",
            instrument_id="23446c3f4c05f8024ba172a9c6a16fdb-DQQMIDJMQQFM"
        )
        print(instruments)

if __name__ == "__main__":
    asyncio.run(list_instruments_async())