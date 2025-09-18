#!/usr/bin/env python3
"""
Lean example: get group time series using core API with simple CLI args.

Defaults:
- If --group-id is not provided, the first available group is used
- Date range: last 90 days
- Attributes: CLOSE
- Output format: JSON
- Prints a small summary

Usage:
  python examples/groups_advanced/get_group_time_series.py \
    [--group-id <id>] [--attributes CLOSE,VOLUME] [--start YYYYMMDD] [--end YYYYMMDD] \
    [--frequency FREQ_DAY] [--calendar CAL_USBANK] [--conversion CONV_LASTBUS_ABS] \
    [--nan NA_FILL_FORWARD] [--page <token>] [--show 3]
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError


async def main() -> None:
    parser = argparse.ArgumentParser(description="Get group time series (lean)")
    parser.add_argument(
        "--group-id", help="Group ID. If omitted, uses the first available group"
    )
    parser.add_argument(
        "--attributes",
        default="CLOSE",
        help="Comma-separated attributes (default: CLOSE)",
    )
    parser.add_argument("--start", help="Start date YYYYMMDD (default: 90 days ago)")
    parser.add_argument("--end", help="End date YYYYMMDD (default: today)")
    parser.add_argument(
        "--frequency", default="FREQ_DAY", help="Frequency (default: FREQ_DAY)"
    )
    parser.add_argument(
        "--calendar", default="CAL_USBANK", help="Calendar (default: CAL_USBANK)"
    )
    parser.add_argument(
        "--conversion",
        default="CONV_LASTBUS_ABS",
        help="Conversion (default: CONV_LASTBUS_ABS)",
    )
    parser.add_argument(
        "--nan",
        dest="nan_treatment",
        default="NA_FILL_FORWARD",
        help="NaN treatment (default: NA_FILL_FORWARD)",
    )
    parser.add_argument("--page", help="Pagination token (optional)")
    parser.add_argument(
        "--show", type=int, default=3, help="How many series to print (default: 3)"
    )
    args = parser.parse_args()

    # Dates
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=90)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    attributes_list = [a.strip() for a in args.attributes.split(",") if a.strip()]

    try:
        async with DataQuery() as dq:
            group_id = args.group_id
            if not group_id:
                groups = await dq.list_groups_async(limit=1)
                if not groups:
                    print("No groups available")
                    return
                group_id = groups[0].group_id

            resp = await dq.get_group_time_series_async(
                group_id=group_id,
                instruments=None,
                attributes=attributes_list,
                filters=None,
                data="REFERENCE_DATA",
                format="JSON",
                start_date=start_date,
                end_date=end_date,
                calendar=args.calendar,
                frequency=args.frequency,
                conversion=args.conversion,
                nan_treatment=args.nan_treatment,
                page=args.page,
            )

            series = getattr(resp, "series", []) or []
            print(f"Series: {len(series)}")
            for i, s in enumerate(series[: args.show], 1):
                instrument = s.get("instrument", "")
                attribute = s.get("attribute", "")
                points = s.get("data", [])
                print(f"{i}. {instrument} - {attribute} (points: {len(points)})")

            if hasattr(resp, "pagination") and resp.pagination:
                next_page = resp.pagination.get("next_page")
                if next_page:
                    print(f"Next page token: {next_page}")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
