#!/usr/bin/env python3
"""Get a time series for one or more DataQuery expressions (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

EXPRESSIONS = ["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"]
START_DATE = "20240101"
END_DATE = "20240131"


async def main():
    async with DataQuery() as dq:
        instruments = []
        page = await dq.get_expressions_time_series_async(
            expressions=EXPRESSIONS,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        while page is not None:
            instruments.extend(page.instruments or [])
            page = await dq.get_next_page_async(page)

        print(f"Instruments returned: {len(instruments)}")

        # Convert to a tidy DataFrame: one row per (instrument, attribute, date).
        try:
            df = dq.time_series_to_dataframe(instruments)
            print(df.head(10))
        except ImportError:
            print("pandas not installed — run: pip install 'dataquery-sdk[pandas]'")


if __name__ == "__main__":
    asyncio.run(main())
