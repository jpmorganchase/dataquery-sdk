#!/usr/bin/env python3
"""
Example: List Available Files by Date Range

This example demonstrates how to list available files within a specific
date range. This is useful for discovering what data is available for
download within a given time period.

Key features demonstrated:
- Listing files by date range
- Filtering by specific file groups
- Date validation and formatting
- File availability analysis
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError


async def main():
    print("🚀 List Available Files (lean)")
    group_id = input("Enter group ID: ").strip()
    if not group_id:
        print("❌ group_id is required")
        return
    file_group_id = input("Filter by file_group_id (optional): ").strip() or None
    start = input("Start date (YYYYMMDD): ").strip()
    end = input("End date (YYYYMMDD): ").strip()
    if not start or not end:
        print("❌ Both start and end dates are required")
        return
    try:
        async with DataQuery() as dq:
            files = await dq.list_available_files_async(
                group_id=group_id,
                file_group_id=file_group_id,
                start_date=start,
                end_date=end,
            )
            print(f"✅ Found {len(files)} available files")
            for i, f in enumerate(files[:20], 1):
                name = f.get("filename") or f.get("file_group_id") or "unknown"
                date = f.get("file_datetime", "")
                size = f.get("file_size", 0)
                print(f"{i}. {name} ({date}) - {size:,} bytes")
            if len(files) > 20:
                print(f"... and {len(files)-20} more")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    try:
        import nest_asyncio  # type: ignore

        nest_asyncio.apply()
    except Exception:
        pass
    asyncio.run(main())


def date_range_analysis_example():
    """Demonstrate comprehensive date range analysis."""
    print("\n🔄 Advanced Example: Comprehensive Date Range Analysis")

    try:
        dq = DataQuery()

        # Get a group for analysis
        groups = dq.list_groups(limit=1)

        if not groups:
            print("❌ No groups available")
            return None

        group = groups[0]
        print(f"📊 Performing date range analysis for: {group.group_name}")

        # Test different date ranges
        date_ranges = [
            {
                "name": "Last 7 days",
                "start": (datetime.now() - timedelta(days=7)).strftime("%Y%m%d"),
                "end": datetime.now().strftime("%Y%m%d"),
            },
            {
                "name": "Last 30 days",
                "start": (datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                "end": datetime.now().strftime("%Y%m%d"),
            },
            {"name": "Q1 2024", "start": "20240101", "end": "20240331"},
        ]

        analysis_results = {}

        for date_range in date_ranges:
            name = date_range["name"]
            start_date = date_range["start"]
            end_date = date_range["end"]

            print(f"\n📅 Analyzing {name} ({start_date} to {end_date})...")

            try:
                available_files = dq.list_available_files(
                    group_id=group.group_id,
                    file_group_id=None,
                    start_date=start_date,
                    end_date=end_date,
                )

                file_count = len(available_files)
                total_size = sum(f.get("file_size", 0) for f in available_files)

                # Count unique dates
                unique_dates = set(
                    f.get("file_datetime")
                    for f in available_files
                    if f.get("file_datetime")
                )

                analysis_results[name] = {
                    "file_count": file_count,
                    "total_size": total_size,
                    "unique_dates": len(unique_dates),
                    "avg_size": total_size / file_count if file_count > 0 else 0,
                }

                print(f"   ✅ {file_count} files, {total_size:,} bytes")
                print(f"   📅 {len(unique_dates)} unique dates")

            except Exception as e:
                analysis_results[name] = {"error": str(e)}
                print(f"   ❌ Error: {e}")

        # Compare results
        print("\n📊 Date Range Comparison:")
        print(
            f"{'Range':<15} {'Files':<8} {'Size (MB)':<12} {'Dates':<8} {'Avg Size':<12}"
        )
        print("-" * 60)

        for name, result in analysis_results.items():
            if "error" not in result:
                size_mb = result["total_size"] / (1024**2)
                avg_size = result["avg_size"]
                print(
                    f"{name:<15} {result['file_count']:<8} {size_mb:<12.2f} {result['unique_dates']:<8} {avg_size:<12,.0f}"
                )
            else:
                print(f"{name:<15} Error: {result['error']}")

        return analysis_results

    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        dq.cleanup()
