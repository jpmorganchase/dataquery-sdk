#!/usr/bin/env python3
"""List files in a group (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

GROUP_ID = "JPMAQS_GENERIC_RETURNS"


async def main():
    async with DataQuery() as dq:
        # list_files_page_async returns a full FileList page; follow ``next``
        # with get_next_page_async, just like every other paged endpoint.
        page = await dq.list_files_page_async(GROUP_ID)
        while page is not None:
            for file_info in page.file_group_ids:
                print(file_info.file_group_id)
            page = await dq.get_next_page_async(page)


if __name__ == "__main__":
    asyncio.run(main())
