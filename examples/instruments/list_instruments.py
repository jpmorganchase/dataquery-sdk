#!/usr/bin/env python3
"""List instruments in a group."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "FI_GO_BO_EA"


async def main():
    async with DataQuery() as dq:
        response = await dq.list_instruments_async(group_id=GROUP_ID, limit=10)
        for instrument in response.instruments or []:
            print(instrument.instrument_id, "—", instrument.instrument_name)


if __name__ == "__main__":
    asyncio.run(main())
