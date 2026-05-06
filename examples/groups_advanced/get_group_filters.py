#!/usr/bin/env python3
"""List the filters available on a group."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "FI_GO_BO_EA"


async def main():
    async with DataQuery() as dq:
        response = await dq.get_group_filters_async(GROUP_ID)
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
