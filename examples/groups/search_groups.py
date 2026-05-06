#!/usr/bin/env python3
"""Search groups by keyword."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

KEYWORD = "economy"


async def main():
    async with DataQuery() as dq:
        results = await dq.search_groups_async(KEYWORD, limit=10)
        for group in results:
            print(group.group_id, "—", group.group_name)


if __name__ == "__main__":
    asyncio.run(main())
