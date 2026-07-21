#!/usr/bin/env python3
"""Search groups by keyword (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

KEYWORD = "economy"


async def main():
    async with DataQuery() as dq:
        # search_groups_page_async returns a full page; follow ``next`` with
        # get_next_page_async to walk the rest.
        page = await dq.search_groups_page_async(KEYWORD)
        while page is not None:
            for group in page.groups:
                print(group.group_id, "—", group.group_name)
            page = await dq.get_next_page_async(page)


if __name__ == "__main__":
    asyncio.run(main())
