#!/usr/bin/env python3
"""
Example: Download catalog files for all groups.

Lists all groups via the /groups API, checks available files for each group,
and downloads any CATALOG file that is available for today's date.

Usage:
    python download_all_catalogs.py
"""

import asyncio
from datetime import datetime
from pathlib import Path

from dataquery import DataQuery

TODAY = datetime.now().strftime("%Y%m%d")
DESTINATION = Path("./downloads")


async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async(limit=700)
        print(f"Found {len(groups)} groups. Checking catalogs for {TODAY}...\n")

        downloaded = 0
        skipped = 0

        for g in groups:
            group_id = g.group_id
            try:
                files = await dq.list_available_files_async(
                    group_id=group_id,
                    start_date=TODAY,
                    end_date=TODAY,
                )
            except Exception as e:
                print(f"  {group_id}: could not list files ({e})")
                continue

            catalogs = [
                f for f in files
                if "CATALOG" in (f.get("file-group-id") or "").upper()
                and f.get("is-available") is True
            ]

            if not catalogs:
                skipped += 1
                continue

            for cat in catalogs:
                fgid = cat["file-group-id"]
                print(f"  {fgid} ... ", end="", flush=True)
                try:
                    result = await dq.download_file_async(
                        file_group_id=fgid,
                        file_datetime=TODAY,
                        destination_path=DESTINATION,
                        num_parts=1,
                    )
                    if result and result.status == "completed":
                        print(f"OK ({result.file_size:,} bytes)")
                        downloaded += 1
                    else:
                        print(f"FAILED: {getattr(result, 'error_message', 'unknown')}")
                except Exception as e:
                    print(f"ERROR: {e}")

    print(f"\nDone. Downloaded: {downloaded}, Skipped (no catalog available): {skipped}")


if __name__ == "__main__":
    asyncio.run(main())
