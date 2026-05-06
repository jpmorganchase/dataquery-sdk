#!/usr/bin/env python3
"""Download today's CATALOG file for every group that has one."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

TODAY = datetime.now().strftime("%Y%m%d")
DESTINATION = Path("./downloads")


async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async()
        for group in groups:
            files = await dq.list_available_files_async(
                group_id=group.group_id,
                start_date=TODAY,
                end_date=TODAY,
            )
            for entry in files:
                fgid = entry.get("file-group-id") or ""
                if "CATALOG" in fgid.upper() and entry.get("is-available"):
                    await dq.download_file_async(
                        file_group_id=fgid,
                        file_datetime=TODAY,
                        destination_path=DESTINATION,
                    )
                    print(fgid)


if __name__ == "__main__":
    asyncio.run(main())
