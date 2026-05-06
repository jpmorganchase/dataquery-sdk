#!/usr/bin/env python3
"""List files in a group."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_ID = "JPMAQS_GENERIC_RETURNS"


async def main():
    async with DataQuery() as dq:
        files = await dq.list_files_async(GROUP_ID)
        for file_info in files:
            print(file_info.file_group_id)


if __name__ == "__main__":
    asyncio.run(main())
