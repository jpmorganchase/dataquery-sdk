#!/usr/bin/env python3
"""Check whether a single file is available."""

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
        availability = await dq.check_availability_async(FILE_GROUP_ID, FILE_DATETIME)
        print("available" if availability and availability.is_available else "not available")


if __name__ == "__main__":
    asyncio.run(main())
