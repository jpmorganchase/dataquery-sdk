#!/usr/bin/env python3
"""
Example: Get Expressions Time Series Data

This example demonstrates how to use DataQuery expressions to retrieve
time series data. Expressions allow you to create complex calculations
and transformations on the data.

Key features demonstrated:
- Using DataQuery expressions for time series
- Date range specification
- Format and calendar options
- Error handling and validation
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError, AuthenticationError, NotFoundError


async def async_example():
    """Demonstrate async expressions time series retrieval."""
    print("🔄 Async Example: Getting Expressions Time Series Data")
    
    try:
        async with DataQuery() as dq:
            # Example expressions - replace with actual expressions from your API
            expressions = [
                "GDP_US_REAL",  # Example expression for US Real GDP
                "CPI_US_CORE",  # Example expression for US Core CPI
            ]
            
            # Calculate date range (last 30 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            print(f"📊 Requesting time series for expressions: {', '.join(expressions)}")
            print(f"📅 Date range: {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}")
            
            # Get expressions time series data
            response = await dq.get_expressions_time_series_async(
                expressions=expressions,
                data="REFERENCE_DATA",  # or "HISTORICAL_DATA"
                format="JSON",  # or "CSV"
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d'),
                calendar="CAL_USBANK",  # US Banking calendar
                frequency="FREQ_DAY",  # Daily frequency
                conversion="CONV_LASTBUS_ABS",  # Last business day, absolute
                nan_treatment="NA_NOTHING",  # How to handle NaN values
                page=None  # Optional pagination token
            )
            
            print(f"✅ Retrieved time series data successfully")
            print(f"📈 Response type: {type(response)}")
            
            # Display response details
            if hasattr(response, 'series') and response.series:
                print(f"📊 Number of time series: {len(response.series)}")
                for i, series in enumerate(response.series[:3]):  # Show first 3
                    print(f"   {i+1}. Expression: {series.get('expression', 'N/A')}")
                    print(f"      Description: {series.get('description', 'N/A')}")
                    if 'data' in series and series['data']:
                        print(f"      Data points: {len(series['data'])}")
                        # Show first few data points
                        for j, point in enumerate(series['data'][:3]):
                            print(f"        {j+1}. Date: {point.get('date', 'N/A')}, Value: {point.get('value', 'N/A')}")
                    print()
            
            # Check for pagination
            if hasattr(response, 'pagination') and response.pagination:
                print(f"📄 Pagination available:")
                print(f"   Next page: {response.pagination.get('next_page', 'None')}")
                print(f"   Total pages: {response.pagination.get('total_pages', 'Unknown')}")
            
            return response
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Please check your API credentials in the .env file")
        return None
    except NotFoundError as e:
        print(f"❌ Expressions not found: {e}")
        print("💡 Please verify the expression names are correct")
        return None
    except DataQueryError as e:
        print(f"❌ API error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None


def sync_example():
    """Demonstrate synchronous expressions time series retrieval."""
    print("\n🔄 Sync Example: Getting Expressions Time Series Data")
    
    try:
        dq = DataQuery()
        
        # Example with different expressions and parameters
        expressions = [
            "UNEMPLOYMENT_RATE_US",  # Example expression
            "INFLATION_RATE_US",     # Example expression
        ]
        
        print(f"📊 Requesting time series for expressions: {', '.join(expressions)}")
        
        # Get expressions time series data (synchronous)
        response = dq.get_expressions_time_series(
            expressions=expressions,
            data="HISTORICAL_DATA",
            format="JSON",
            start_date="20240101",  # Fixed date range
            end_date="20240331",
            calendar="CAL_USBANK",
            frequency="FREQ_MONTH",  # Monthly frequency
            conversion="CONV_LASTBUS_ABS"
        )
        
        print(f"✅ Retrieved time series data successfully")
        
        # Display basic information
        if hasattr(response, 'series') and response.series:
            print(f"📈 Retrieved {len(response.series)} time series")
            for series in response.series:
                expression = series.get('expression', 'Unknown')
                data_count = len(series.get('data', []))
                print(f"   • {expression}: {data_count} data points")
        
        return response
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        # Always cleanup in synchronous mode
        dq.cleanup()


def advanced_example():
    """Demonstrate advanced expressions usage with custom parameters."""
    print("\n🔄 Advanced Example: Custom Expression Parameters")
    
    try:
        dq = DataQuery()
        
        # Complex expressions with calculations
        expressions = [
            "GDP_US_REAL / GDP_US_NOMINAL * 100",  # GDP deflator calculation
            "LOG(SP500_INDEX)",  # Logarithmic transformation
        ]
        
        print(f"📊 Using advanced expressions:")
        for i, expr in enumerate(expressions, 1):
            print(f"   {i}. {expr}")
        
        # Get with specific parameters for calculations
        response = dq.get_expressions_time_series(
            expressions=expressions,
            data="REFERENCE_DATA",
            format="JSON",
            start_date="20230101",
            end_date="20231231",
            calendar="CAL_USBANK",
            frequency="FREQ_QUARTER",  # Quarterly data
            conversion="CONV_LASTBUS_REL",  # Relative conversion
            nan_treatment="NA_FILL_FORWARD"  # Forward fill NaN values
        )
        
        print(f"✅ Advanced expressions processed successfully")
        
        # Analysis of results
        if hasattr(response, 'series') and response.series:
            for series in response.series:
                expression = series.get('expression', 'Unknown')
                data_points = series.get('data', [])
                
                if data_points:
                    values = [float(point.get('value', 0)) for point in data_points if point.get('value') is not None]
                    if values:
                        avg_value = sum(values) / len(values)
                        min_value = min(values)
                        max_value = max(values)
                        
                        print(f"📈 {expression}:")
                        print(f"   Data points: {len(data_points)}")
                        print(f"   Average: {avg_value:.4f}")
                        print(f"   Range: {min_value:.4f} to {max_value:.4f}")
        
        return response
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        dq.cleanup()


async def main():
    """Run all examples."""
    print("=" * 60)
    print("📊 DataQuery SDK - Expressions Time Series Examples")
    print("=" * 60)
    
    # Run async example
    await async_example()
    
    # Run sync example
    sync_example()
    
    # Run advanced example
    advanced_example()
    
    print("\n" + "=" * 60)
    print("✅ All examples completed!")
    print("💡 Tip: Modify the expressions to match your actual data requirements")
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
