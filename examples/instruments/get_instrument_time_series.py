#!/usr/bin/env python3
"""Get a time series for specific instruments and attributes."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

INSTRUMENTS = ["IE00BH3SQ895"]
ATTRIBUTES = ["MIDPRC"]
START_DATE = "20240101"
END_DATE = "20240131"


async def main():
    async with DataQuery() as dq:
        response = await dq.get_instrument_time_series_async(
            instruments=INSTRUMENTS,
            attributes=ATTRIBUTES,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
