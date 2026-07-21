#!/usr/bin/env python3
"""
Notification-driven download example.

Subscribes to the DataQuery SSE notification stream and downloads files as
they are announced. Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")


async def main():
    group_id = input("Group ID to watch: ").strip()
    if not group_id:
        print("[Error] Group ID is required")
        return

    async with DataQuery() as dq:
        manager = await dq.auto_download_async(group_id=group_id)
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            await manager.stop()
            print(manager.get_stats())


if __name__ == "__main__":
    asyncio.run(main())
