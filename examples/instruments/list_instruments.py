#!/usr/bin/env python3
"""List instruments in a group (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

GROUP_ID = "FI_GO_BO_EA"


async def main():
    async with DataQuery() as dq:
        # Each response is one page; get_next_page_async follows ``next``.
        page = await dq.list_instruments_async(group_id=GROUP_ID)
        while page is not None:
            for instrument in page.instruments or []:
                print(instrument.instrument_id, "—", instrument.instrument_name)
            page = await dq.get_next_page_async(page)


if __name__ == "__main__":
    asyncio.run(main())
