#!/usr/bin/env python3
"""
Example: Get Grid Data

This example demonstrates how to retrieve grid data using either expressions
or grid IDs. Grid data provides structured, tabular data that can be used
for analysis and reporting.

Key features demonstrated:
- Grid data retrieval using expressions
- Grid data retrieval using grid IDs
- Parameter configuration for grid queries
- Data formatting and display options
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError, DataQueryError, NotFoundError


async def async_example():
    """Demonstrate async grid data retrieval using expressions."""
    print("🔄 Async Example: Getting Grid Data with Expressions")

    try:
        async with DataQuery() as dq:
            # Example expression for grid data
            expression = "GDP_QUARTERLY_COMPARISON"  # Replace with actual expression

            print(f"📊 Requesting grid data for expression: {expression}")

            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)  # Last year

            # Get grid data using expression
            grid_response = await dq.get_grid_data_async(
                expr=expression, grid_id=None, date=end_date.strftime("%Y%m%d")
            )

            print(f"✅ Retrieved grid data successfully")
            print(f"📊 Response type: {type(grid_response)}")

            # Display grid data information
            if hasattr(grid_response, "series") and grid_response.series:
                print(f"📋 Grid series: {len(grid_response.series)}")
                first = grid_response.series[0]
                print(f"   Expression: {first.expr}")
                if first.records:
                    print(f"   Records: {len(first.records)} (showing up to 3)")
                    for rec in first.records[:3]:
                        print(f"     - {list(rec.items())[:5]}")
            else:
                print("ℹ️  No grid data available for this expression")

            # Check for pagination
            if hasattr(grid_response, "pagination") and grid_response.pagination:
                pagination = grid_response.pagination
                print(f"\n📄 Pagination:")
                print(f"   Has more data: {pagination.get('has_next', False)}")
                print(f"   Next page: {pagination.get('next_page', 'None')}")

            return grid_response

    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Please check your API credentials in the .env file")
        return None
    except NotFoundError as e:
        print(f"❌ Expression not found: {e}")
        print("💡 Please verify the expression name is correct")
        return None
    except DataQueryError as e:
        print(f"❌ API error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None


def sync_example():
    """Demonstrate synchronous grid data retrieval using grid ID."""
    print("\n🔄 Sync Example: Getting Grid Data with Grid ID")

    try:
        dq = DataQuery()

        # Example grid ID (replace with actual grid ID from your API)
        grid_id = "ECONOMIC_INDICATORS_GRID"

        print(f"📊 Requesting grid data for Grid ID: {grid_id}")

        # Get grid data using grid ID
        grid_response = dq.get_grid_data(expr=None, grid_id=grid_id, date="20240630")

        if hasattr(grid_response, "series") and grid_response.series:
            print(f"✅ Retrieved grid data successfully")
            print(f"📋 Series count: {len(grid_response.series)}")
            first = grid_response.series[0]
            print(f"   First series expr: {first.expr}")
            print(f"   Records: {len(first.records or [])}")
        else:
            print("ℹ️  No grid data available for this grid ID")

        return grid_response

    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        dq.cleanup()


def comparison_example():
    """Demonstrate comparison between expression and grid ID approaches."""
    print("\n🔄 Advanced Example: Expression vs Grid ID Comparison")

    try:
        dq = DataQuery()

        # Test both expression and grid ID approaches
        test_cases = [
            {
                "name": "Expression Approach",
                "expr": "MARKET_OVERVIEW_DAILY",
                "grid_id": None,
            },
            {
                "name": "Grid ID Approach",
                "expr": None,
                "grid_id": "MARKET_OVERVIEW_GRID",
            },
        ]

        results = {}

        for test_case in test_cases:
            name = test_case["name"]
            print(f"\n🧪 Testing {name}...")

            try:
                start_time = datetime.now()

                grid_response = dq.get_grid_data(
                    expr=test_case["expr"],
                    grid_id=test_case["grid_id"],
                    data="REFERENCE_DATA",
                    format="JSON",
                    start_date="20240101",
                    end_date="20240131",  # Smaller date range for comparison
                    calendar="CAL_USBANK",
                    frequency="FREQ_DAY",
                    conversion="CONV_LASTBUS_ABS",
                )

                end_time = datetime.now()
                response_time = (end_time - start_time).total_seconds()

                # Analyze response
                if hasattr(grid_response, "grid_data") and grid_response.grid_data:
                    grid_data = grid_response.grid_data
                    column_count = len(grid_data.get("columns", []))
                    row_count = len(grid_data.get("rows", []))

                    results[name] = {
                        "success": True,
                        "response_time": response_time,
                        "columns": column_count,
                        "rows": row_count,
                        "total_cells": column_count * row_count,
                    }

                    print(f"   ✅ Success in {response_time:.2f}s")
                    print(f"   📊 {column_count} columns, {row_count} rows")
                else:
                    results[name] = {"success": False, "error": "No data returned"}
                    print(f"   ℹ️  No data returned")

            except Exception as e:
                results[name] = {"success": False, "error": str(e)}
                print(f"   ❌ Error: {e}")

        # Compare results
        print(f"\n📊 Comparison Results:")
        for name, result in results.items():
            print(f"\n   {name}:")
            if result["success"]:
                print(f"      ✅ Successful")
                print(f"      ⏱️  Response time: {result['response_time']:.2f}s")
                print(
                    f"      📊 Data: {result['columns']} cols × {result['rows']} rows"
                )
                print(f"      📈 Total cells: {result['total_cells']}")
            else:
                print(f"      ❌ Failed: {result['error']}")

        return results

    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        dq.cleanup()


async def main():
    """Run all examples."""
    print("=" * 60)
    print("📊 DataQuery SDK - Grid Data Examples")
    print("=" * 60)

    # Run async example with expressions
    await async_example()

    # Run sync example with grid ID
    sync_example()

    # Run comparison example
    comparison_example()

    print("\n" + "=" * 60)
    print("✅ All examples completed!")
    print(
        "💡 Tip: Use expressions for dynamic queries, grid IDs for predefined data sets"
    )
    print("=" * 60)


if __name__ == "__main__":
    # Check if we're in an async context
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            # We're already in an async context, just await
            import nest_asyncio

            nest_asyncio.apply()
            asyncio.run(main())
        else:
            raise
