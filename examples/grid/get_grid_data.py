#!/usr/bin/env python3
"""Fetch a grid for a single expression."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

EXPRESSION = "DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"


async def main():
    async with DataQuery() as dq:
        response = await dq.get_grid_data_async(
            expr=EXPRESSION,
            grid_id=None,
            date=datetime.now().strftime("%Y%m%d"),
        )
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
