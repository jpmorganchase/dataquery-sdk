#!/usr/bin/env python3
"""
Example: Get Grid Data

This example demonstrates how to retrieve grid data using either expressions
or grid IDs. Grid data provides structured, tabular data that can be used
for analysis and reporting.

Usage:
    python get_grid_data.py [--mode {async,sync,compare}] [--expression EXPR] [--grid-id ID]
"""

import argparse
import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import AuthenticationError, NotFoundError  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="Get Grid Data Example")
    parser.add_argument(
        "--mode",
        choices=["async", "sync", "compare"],
        default="async",
        help="Execution mode",
    )
    parser.add_argument(
        "--expression",
        default="GDP_QUARTERLY_COMPARISON",
        help="Expression to query (for async mode)",
    )
    parser.add_argument(
        "--grid-id",
        default="ECONOMIC_INDICATORS_GRID",
        help="Grid ID to query (for sync mode)",
    )
    return parser.parse_args()


async def async_example(expression: str):
    """Demonstrate async grid data retrieval using expressions."""
    print(f"\n[Async] Getting Grid Data for Expression: {expression}")

    try:
        async with DataQuery() as dq:
            # Calculate date range
            end_dt = datetime.now()

            # Get grid data using expression
            start_time = time.time()
            grid_response = await dq.get_grid_data_async(expr=expression, grid_id=None, date=end_dt.strftime("%Y%m%d"))
            elapsed = time.time() - start_time

            print(f"[Success] Retrieved data in {elapsed:.2f}s")

            # Display grid data information
            if hasattr(grid_response, "series") and grid_response.series:
                print(f"[Info] Series count: {len(grid_response.series)}")
                first = grid_response.series[0]
                print(f"   Expression: {first.expr}")
                if first.records:
                    print(f"   Records: {len(first.records)} (showing up to 3)")
                    for rec in first.records[:3]:
                        # Format record for display
                        formatted_rec = {k: v for k, v in list(rec.items())[:3]}
                        print(f"     - {formatted_rec}...")
            else:
                print("[Info] No grid data available for this expression")

            return grid_response

    except AuthenticationError:
        print("[Error] Authentication failed. Check your credentials.")
    except NotFoundError:
        print(f"[Error] Expression '{expression}' not found.")
    except Exception as e:
        print(f"[Error] Unexpected error: {e}")


def sync_example(grid_id: str):
    """Demonstrate synchronous grid data retrieval using grid ID."""
    print(f"\n[Sync] Getting Grid Data for Grid ID: {grid_id}")

    try:
        dq = DataQuery()

        start_time = time.time()
        grid_response = dq.get_grid_data(expr=None, grid_id=grid_id, date="20240630")
        elapsed = time.time() - start_time

        print(f"[Success] Retrieved data in {elapsed:.2f}s")

        if hasattr(grid_response, "series") and grid_response.series:
            print(f"[Info] Series count: {len(grid_response.series)}")
            first = grid_response.series[0]
            print(f"   First series expr: {first.expr}")
            print(f"   Records: {len(first.records or [])}")
        else:
            print("[Info] No grid data available for this grid ID")

        return grid_response

    except Exception as e:
        print(f"[Error] Error: {e}")
    finally:
        dq.cleanup()


def comparison_example():
    """Demonstrate comparison between expression and grid ID approaches."""
    print("\n[Compare] Expression vs Grid ID Comparison")

    test_cases = [
        {"name": "Expression", "expr": "MARKET_OVERVIEW_DAILY", "grid_id": None},
        {"name": "Grid ID", "expr": None, "grid_id": "MARKET_OVERVIEW_GRID"},
    ]

    dq = DataQuery()
    try:
        print(f"{'Type':<15} {'Status':<10} {'Time':<10} {'Rows':<10}")
        print("-" * 50)

        for case in test_cases:
            try:
                start_time = time.time()
                resp = dq.get_grid_data(
                    expr=case["expr"],
                    grid_id=case["grid_id"],
                    data="REFERENCE_DATA",
                    format="JSON",
                    end_date="20240131",
                    calendar="CAL_USBANK",
                    frequency="FREQ_DAY",
                    conversion="CONV_LASTBUS_ABS",
                )
                elapsed = time.time() - start_time

                rows = 0
                if hasattr(resp, "grid_data") and resp.grid_data:
                    rows = len(resp.grid_data.get("rows", []))

                print(f"{case['name']:<15} {'Success':<10} {elapsed:<10.2f} {rows:<10}")

            except Exception as e:
                print(f"{case['name']:<15} {'Failed':<10} {'-':<10} {str(e)[:20]}")

    finally:
        dq.cleanup()


async def main():
    args = parse_args()

    print("=" * 60)
    print("DataQuery SDK - Grid Data Example")
    print("=" * 60)

    if args.mode == "async":
        await async_example(args.expression)
    elif args.mode == "sync":
        # Run sync example in a thread to avoid blocking if this were a larger app
        # But for this script, direct call is fine, or wrap in to_thread
        await asyncio.to_thread(sync_example, args.grid_id)
    elif args.mode == "compare":
        await asyncio.to_thread(comparison_example)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Info] Operation cancelled.")
