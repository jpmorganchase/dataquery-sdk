#!/usr/bin/env python3
"""List groups."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402


async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async(limit=10)
        for group in groups:
            print(group.group_id, "—", group.group_name)


if __name__ == "__main__":
    asyncio.run(main())
