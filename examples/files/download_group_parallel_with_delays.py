#!/usr/bin/env python3
"""
Example: Download group files with parallel processing and configurable delays.

This example demonstrates the new delay functionality that helps manage API load
by staggering the start of each file download.
"""

import asyncio
from pathlib import Path
from dataquery import DataQuery

async def main():
    """Download group files with delays between downloads."""
    print("🚀 Group Parallel Download with Delays Example")
    print("=" * 50)
    
    # Initialize DataQuery
    dq = DataQuery()
    
    # Get user input
    group_id = input("Enter group ID: ").strip()
    start_date = input("Enter start date (YYYYMMDD): ").strip()
    end_date = input("Enter end date (YYYYMMDD): ").strip()
    
    # Get delay settings
    delay_input = input("Delay between downloads in seconds [1.0]: ").strip()
    delay_between_downloads = float(delay_input) if delay_input else 1.0
    
    # Get concurrency settings
    max_concurrent = int(input("Max concurrent files [3]: ").strip() or "3")
    num_parts = int(input("Number of parallel parts per file [10]: ").strip() or "10")
    
    destination_dir = Path(input("Destination directory [./downloads]: ").strip() or "./downloads")
    
    print(f"\n📊 Download Configuration:")
    print(f"   Group ID: {group_id}")
    print(f"   Date Range: {start_date} to {end_date}")
    print(f"   Max Concurrent Files: {max_concurrent}")
    print(f"   Parts per File: {num_parts}")
    print(f"   Delay Between Downloads: {delay_between_downloads}s")
    print(f"   Destination: {destination_dir}")
    print(f"   Total Concurrent Requests: {max_concurrent * num_parts}")
    print(f"   Expected Delay Range: 0-{(max_concurrent-1) * delay_between_downloads:.1f}s")
    
    try:
        print(f"\n🔄 Starting download with delays...")
        
        # Download with delays
        result = await dq.run_group_download_parallel_async(
            group_id=group_id,
            start_date=start_date,
            end_date=end_date,
            destination_dir=destination_dir,
            max_concurrent=max_concurrent,
            num_parts=num_parts,
            delay_between_downloads=delay_between_downloads
        )
        
        # Display results
        print(f"\n✅ Download completed!")
        print(f"📊 Results:")
        print(f"   Total Files: {result.get('total_files', 0)}")
        print(f"   Successful: {result.get('successful_downloads', 0)}")
        print(f"   Failed: {result.get('failed_downloads', 0)}")
        print(f"   Success Rate: {result.get('success_rate', 0):.1f}%")
        print(f"   Concurrency Model: {result.get('concurrency_model', 'unknown')}")
        print(f"   Delay Applied: {result.get('delay_between_downloads', 0)}s between downloads")
        print(f"   Delay Range: {result.get('total_delay_range', 'N/A')}")
        
        if result.get('rate_limit_applied'):
            print(f"   ⚠️  Rate limiting was applied")
            recommendations = result.get('rate_limit_recommendations', {})
            if recommendations.get('recommendations'):
                print(f"   💡 Recommendations: {len(recommendations['recommendations'])} suggestions available")
        
        if result.get('downloaded_files'):
            print(f"\n📁 Downloaded Files:")
            for file_id in result['downloaded_files'][:5]:  # Show first 5
                print(f"   ✅ {file_id}")
            if len(result['downloaded_files']) > 5:
                print(f"   ... and {len(result['downloaded_files']) - 5} more")
        
        if result.get('failed_files'):
            print(f"\n❌ Failed Files:")
            for file_id in result['failed_files'][:5]:  # Show first 5
                print(f"   ❌ {file_id}")
            if len(result['failed_files']) > 5:
                print(f"   ... and {len(result['failed_files']) - 5} more")
                
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
