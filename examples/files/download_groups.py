#!/usr/bin/env python3
"""Download every available file for all file-delivery-enabled groups, over a date range.

Workflow:
  1. GET /groups                         -> list every group
  2. keep the groups where `is-file-delivery-enabled` is True
  3. GET .../available-files per group   -> for the time period (start..end date)
  4. download every entry whose `is-available` is True
  5. write a "missing data" report for anything the API did NOT make available
     (is-available == False, an empty result, or a download that errored out)
"""

import asyncio
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402

# Time period to pull (inclusive, YYYYMMDD).
START_DATE = "20250101"
END_DATE = "20250131"

DESTINATION = Path("./downloads")
REPORT_PATH = DESTINATION / "missing_data_report.json"


def is_delivery_enabled(group) -> bool:
    """Read the `is-file-delivery-enabled` flag off a group.

    `/groups` returns it as an extra (hyphenated) field, so it lands in
    ``model_extra`` rather than as a typed attribute. Tolerate a bool or a
    "true"/"false" string.
    """
    value = (group.model_extra or {}).get("is-file-delivery-enabled")
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


async def main():
    downloaded = 0
    # group_id -> list of {file_group_id, file_datetime, reason} entries
    missing: dict[str, list[dict]] = defaultdict(list)

    async with DataQuery() as dq:
        groups = await dq.list_groups_async(limit=None)
        delivery_groups = [g for g in groups if is_delivery_enabled(g)]
        print(
            f"{len(delivery_groups)}/{len(groups)} groups have file delivery enabled — pulling {START_DATE}..{END_DATE}"
        )

        for group in delivery_groups:
            gid = group.group_id or ""

            # 3. file-available endpoint for the time period
            try:
                entries = await dq.list_available_files_async(
                    group_id=gid,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            except Exception as exc:  # group-level availability lookup failed
                missing[gid].append(
                    {
                        "file_group_id": "*",
                        "file_datetime": f"{START_DATE}..{END_DATE}",
                        "reason": f"availability lookup failed: {exc}",
                    }
                )
                continue

            if not entries:
                missing[gid].append(
                    {
                        "file_group_id": "*",
                        "file_datetime": f"{START_DATE}..{END_DATE}",
                        "reason": "no files returned for date range",
                    }
                )
                continue

            for entry in entries:
                fgid = entry.get("file-group-id") or ""
                fdate = entry.get("file-datetime") or ""

                if not entry.get("is-available"):
                    # 5. published-but-not-available == missing data
                    missing[gid].append({"file_group_id": fgid, "file_datetime": fdate, "reason": "is-available=false"})
                    continue

                # 4. download every available file
                try:
                    await dq.download_file_async(
                        file_group_id=fgid,
                        file_datetime=fdate,
                        destination_path=DESTINATION,
                    )
                    downloaded += 1
                    print(f"  ✓ {gid}  {fgid}  {fdate}")
                except Exception as exc:
                    missing[gid].append(
                        {"file_group_id": fgid, "file_datetime": fdate, "reason": f"download failed: {exc}"}
                    )
                    print(f"  ✗ {gid}  {fgid}  {fdate}  ({exc})")

    # --- Report ---
    missing_count = sum(len(rows) for rows in missing.values())
    print("\n=== Summary ===")
    print(f"Downloaded files     : {downloaded}")
    print(f"Groups with gaps     : {len(missing)}")
    print(f"Missing/failed entries: {missing_count}")

    if missing:
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "date_range": {"start": START_DATE, "end": END_DATE},
            "downloaded": downloaded,
            "groups_with_missing_data": missing,
        }
        DESTINATION.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, indent=2))
        print(f"\nMissing-data report written to {REPORT_PATH}")
        for gid, rows in sorted(missing.items()):
            print(f"\n{gid}  ({len(rows)} missing)")
            for row in rows:
                print(f"  - {row['file_group_id']}  {row['file_datetime']}  ({row['reason']})")


if __name__ == "__main__":
    asyncio.run(main())
