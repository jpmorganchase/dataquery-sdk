#!/usr/bin/env python3
"""
Multi-group notification-driven download example.

Subscribes to several DataQuery groups concurrently — one SSE connection per
group, sharing auth and rate limiting through a single ``DataQuery`` instance.
Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

GROUP_IDS = ["JPMAQS", "MARKETS", "ECON"]


async def main():
    async with DataQuery() as dq:
        managers = await asyncio.gather(*(dq.auto_download_async(group_id=group_id) for group_id in GROUP_IDS))
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            await asyncio.gather(
                *(manager.stop() for manager in managers),
                return_exceptions=True,
            )
            for group_id, manager in zip(GROUP_IDS, managers):
                print(group_id, manager.get_stats())


if __name__ == "__main__":
    asyncio.run(main())
