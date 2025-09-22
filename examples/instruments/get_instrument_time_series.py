#!/usr/bin/env python3
"""
Lean example: get instrument time series with defaults and simple CLI args.

Defaults:
- If --instruments not provided, fetch first 2 from first group
- Date range: last 60 days
- Attributes: CLOSE
- Output format: JSON

Usage:
  python examples/instruments/get_instrument_time_series.py \
    [--instruments ID1,ID2] [--attributes CLOSE,VOLUME] [--start YYYYMMDD] [--end YYYYMMDD] \
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
    parser = argparse.ArgumentParser(description="Get instrument time series (lean)")
    parser.add_argument(
        "--instruments",
        help="Comma-separated instrument IDs. If omitted, auto-detect first 2 from first group",
        required=True,
    )
    parser.add_argument(
        "--attributes",
        default="CLOSE",
        help="Comma-separated attributes (default: CLOSE)",
        required=True,
    )
    parser.add_argument(
        "--start",
        help="Start date YYYYMMDD",
        required=True,
    )
    parser.add_argument(
        "--end",
        help="End date YYYYMMDD (default: today)",
        required=True,
    )
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
        start_dt = end_dt - timedelta(days=60)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    attributes_list = [a.strip() for a in args.attributes.split(",") if a.strip()]

    # Instruments
    instrument_ids = None
    if args.instruments:
        instrument_ids = [i.strip() for i in args.instruments.split(",") if i.strip()]

    try:
        async with DataQuery() as dq:
            if not instrument_ids:
                groups = await dq.list_groups_async(limit=100)
                if not groups:
                    print("No groups available")
                    return
                instruments_resp = await dq.list_instruments_async(groups[0].group_id)
                print(instruments_resp)
                instruments = getattr(instruments_resp, "instruments", []) or []
                if not instruments:
                    print("No instruments found in the first group")
                    return
                instrument_ids = [
                    getattr(inst, "instrument_id", None)
                    or inst.get("instrument_id", "")
                    for inst in instruments
                ]

            resp = await dq.get_instrument_time_series_async(
                instruments=instrument_ids[0:10],
                attributes=attributes_list,
                data="ALL",
                format="JSON",
                start_date=start_date,
                end_date=end_date,
                calendar=args.calendar,
                frequency=args.frequency,
                conversion=args.conversion,
                nan_treatment=args.nan_treatment,
                page=args.page,
            )

            instruments = getattr(resp, "instruments", []) or []
            print(f"Series: {instruments}")
            attributes = [
                attr
                for instrument in instruments
                for attr in getattr(instrument, "attributes", [])
            ]
            print(f"attributes: {attributes}")
            time_series_list = [attr.time_series for attr in attributes]
            print(f"time_series: {time_series_list}")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
