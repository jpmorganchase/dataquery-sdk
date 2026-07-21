#!/usr/bin/env python3
"""List groups one page at a time (client-driven pagination)."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")


async def main():
    async with DataQuery() as dq:
        # Fetch the first page, then follow ``next`` yourself with
        # get_next_page_async. Each page carries items / page_size / next_link.
        page = await dq.list_groups_page_async()
        page_no = 1
        while page is not None:
            print(f"page {page_no}: {len(page.groups)} groups (items={page.items}, page-size={page.page_size})")
            for group in page.groups:
                print("  ", group.group_id, "—", group.group_name)
            page = await dq.get_next_page_async(page)
            page_no += 1


if __name__ == "__main__":
    asyncio.run(main())
