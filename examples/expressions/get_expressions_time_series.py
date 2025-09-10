#!/usr/bin/env python3
"""
Lean example: get expressions time series using core API with simple CLI args.

Defaults:
- Expressions: GDP_US_REAL
- Date range: last 30 days
- Output format: JSON

Usage:
  python examples/expressions/get_expressions_time_series.py \
    [--expressions GDP_US_REAL,CPI_US_CORE] [--start YYYYMMDD] [--end YYYYMMDD] \
    [--frequency FREQ_DAY] [--calendar CAL_USBANK] [--conversion CONV_LASTBUS_ABS] \
    [--nan NA_NOTHING] [--data REFERENCE_DATA] [--show 3]
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError


async def main() -> None:
    parser = argparse.ArgumentParser(description="Get expressions time series (lean)")
    parser.add_argument("--expressions", default="GDP_US_REAL", help="Comma-separated expressions (default: GDP_US_REAL)")
    parser.add_argument("--start", help="Start date YYYYMMDD (default: 30 days ago)")
    parser.add_argument("--end", help="End date YYYYMMDD (default: today)")
    parser.add_argument("--frequency", default="FREQ_DAY", help="Frequency (default: FREQ_DAY)")
    parser.add_argument("--calendar", default="CAL_USBANK", help="Calendar (default: CAL_USBANK)")
    parser.add_argument("--conversion", default="CONV_LASTBUS_ABS", help="Conversion (default: CONV_LASTBUS_ABS)")
    parser.add_argument("--nan", dest="nan_treatment", default="NA_NOTHING", help="NaN treatment (default: NA_NOTHING)")
    parser.add_argument("--data", default="REFERENCE_DATA", help="Data flag (default: REFERENCE_DATA)")
    parser.add_argument("--show", type=int, default=3, help="How many series to print (default: 3)")
    args = parser.parse_args()

    # Dates
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    expressions_list = [e.strip() for e in args.expressions.split(",") if e.strip()]

    try:
        async with DataQuery() as dq:
            resp = await dq.get_expressions_time_series_async(
                expressions=expressions_list,
                data=args.data,
                format="JSON",
                start_date=start_date,
                end_date=end_date,
                calendar=args.calendar,
                frequency=args.frequency,
                conversion=args.conversion,
                nan_treatment=args.nan_treatment,
                page=None,
            )

            series = getattr(resp, "series", []) or []
            print(f"Series: {len(series)}")
            for i, s in enumerate(series[: args.show], 1):
                expr = s.get("expression", "")
                points = s.get("data", [])
                print(f"{i}. {expr} (points: {len(points)})")

    except DataQueryError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
