#!/usr/bin/env python3

import asyncio
from dataquery import DataQuery

async def get_group_time_series_async():
    async with DataQuery() as dq:
        response = await dq.get_group_time_series_async(
            group_id="JPMAQS",
            attributes=["value", "grading"],
            data="ALL",
            format="JSON",
            start_date="20240101",
            end_date="20240401",
            calendar="CAL_USBANK",
            frequency="FREQ_DAY",
            conversion="CONV_LASTBUS_ABS",
            nan_treatment="NA_FILL_FORWARD",
            page=None
        )
        print(response)

if __name__ == "__main__":
    asyncio.run(get_group_time_series_async())