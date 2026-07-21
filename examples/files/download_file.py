#!/usr/bin/env python3
"""Download a single file."""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")

FILE_GROUP_ID = "JPMAQS_GENERIC_RETURNS"
FILE_DATETIME = "20250115"


async def main():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id=FILE_GROUP_ID,
            file_datetime=FILE_DATETIME,
            destination_path=Path("./downloads"),
        )
        print(result.local_path, result.file_size)


if __name__ == "__main__":
    asyncio.run(main())
