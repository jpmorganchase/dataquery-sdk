#!/usr/bin/env python3
"""List every group (SDK paginates internally)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402


async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async()
        print(f"Total groups: {len(groups)}")


if __name__ == "__main__":
    asyncio.run(main())
