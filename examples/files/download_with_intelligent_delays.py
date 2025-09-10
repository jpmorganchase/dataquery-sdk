#!/usr/bin/env python3
"""
Example: Download with Intelligent Delay-Based Rate Limit Protection.

This example demonstrates the new approach that maintains full concurrency
while using intelligent delays to ensure rate limit compliance.
"""

import asyncio
from pathlib import Path
from dataquery import DataQuery

async def main():
    """Download with intelligent delay-based rate limit protection."""
    print("🧠 Intelligent Delay-Based Rate Limit Protection Example")
    print("=" * 60)
    
    # Initialize DataQuery
    dq = DataQuery()
    
    # Get user input
    group_id = input("Enter group ID: ").strip()
    start_date = input("Enter start date (YYYYMMDD): ").strip()
    end_date = input("Enter end date (YYYYMMDD): ").strip()
    
    # Get concurrency settings
    max_concurrent = int(input("Max concurrent files [4]: ").strip() or "4")
    num_parts = int(input("Number of parallel parts per file [10]: ").strip() or "10")
    base_delay = float(input("Base delay in seconds [1.0]: ").strip() or "1.0")
    
    destination_dir = Path(input("Destination directory [./downloads]: ").strip() or "./downloads")
    
    total_requests = max_concurrent * num_parts
    
    print(f"\n📊 Download Configuration:")
    print(f"   Group ID: {group_id}")
    print(f"   Date Range: {start_date} to {end_date}")
    print(f"   Max Concurrent Files: {max_concurrent}")
    print(f"   Parts per File: {num_parts}")
    print(f"   Total Concurrent Requests: {total_requests}")
    print(f"   Base Delay: {base_delay}s")
    print(f"   Destination: {destination_dir}")
    
    # Show rate limit analysis
    print(f"\n🔍 Rate Limit Analysis:")
    rate_capacity = dq._calculate_rate_limit_capacity()
    print(f"   Requests per minute: {rate_capacity['requests_per_minute']}")
    print(f"   Burst capacity: {rate_capacity['burst_capacity']}")
    print(f"   Safe interval: {rate_capacity['safe_interval']:.3f}s")
    
    # Calculate intelligent delay
    intelligent_delay = dq._calculate_intelligent_delay(
        total_requests, 
        rate_capacity, 
        base_delay
    )
    
    print(f"\n🧠 Intelligent Delay Calculation:")
    print(f"   Base delay: {base_delay}s")
    print(f"   Intelligent delay: {intelligent_delay:.3f}s")
    print(f"   Delay range: 0-{(max_concurrent-1) * intelligent_delay:.1f}s")
    
    # Show delay pattern
    print(f"\n⏱️  File Start Pattern:")
    for i in range(min(5, max_concurrent)):
        delay = i * intelligent_delay
        print(f"   File {i+1}: starts after {delay:.3f}s delay")
    if max_concurrent > 5:
        print(f"   ... and {max_concurrent - 5} more files with increasing delays")
    
    # Show benefits
    print(f"\n🚀 Benefits of This Approach:")
    print(f"   ✅ Maintains full concurrency ({total_requests} requests)")
    print(f"   ✅ Automatic rate limit protection")
    print(f"   ✅ No performance reduction")
    print(f"   ✅ Intelligent delay calculation")
    print(f"   ✅ Graceful load management")
    
    try:
        print(f"\n🔄 Starting download with intelligent delays...")
        
        # Download with intelligent delay-based protection
        result = await dq.run_group_download_parallel_async(
            group_id=group_id,
            start_date=start_date,
            end_date=end_date,
            destination_dir=destination_dir,
            max_concurrent=max_concurrent,
            num_parts=num_parts,
            delay_between_downloads=base_delay
        )
        
        # Display results
        print(f"\n✅ Download completed!")
        print(f"📊 Results:")
        print(f"   Concurrency Model: {result.get('concurrency_model', 'unknown')}")
        print(f"   Rate Limit Protection: {result.get('rate_limit_protection', 'unknown')}")
        print(f"   Total Files: {result.get('total_files', 0)}")
        print(f"   Successful: {result.get('successful_downloads', 0)}")
        print(f"   Failed: {result.get('failed_downloads', 0)}")
        print(f"   Success Rate: {result.get('success_rate', 0):.1f}%")
        print(f"   Total Concurrent Requests: {result.get('total_concurrent_requests', 0)}")
        print(f"   Base Delay: {result.get('base_delay', 0)}s")
        print(f"   Intelligent Delay: {result.get('intelligent_delay', 0):.3f}s")
        print(f"   Delay Range: {result.get('delay_range', 'N/A')}")
        
        # Show rate limit capacity info
        rate_capacity = result.get('rate_limit_capacity', {})
        if rate_capacity:
            print(f"\n📊 Rate Limit Capacity Used:")
            print(f"   Requests per minute: {rate_capacity.get('requests_per_minute', 'N/A')}")
            print(f"   Burst capacity: {rate_capacity.get('burst_capacity', 'N/A')}")
            print(f"   Safe interval: {rate_capacity.get('safe_interval', 'N/A')}s")
        
        if result.get('downloaded_files'):
            print(f"\n📁 Downloaded Files:")
            for file_id in result['downloaded_files'][:5]:
                print(f"   ✅ {file_id}")
            if len(result['downloaded_files']) > 5:
                print(f"   ... and {len(result['downloaded_files']) - 5} more")
        
        if result.get('failed_files'):
            print(f"\n❌ Failed Files:")
            for file_id in result['failed_files'][:5]:
                print(f"   ❌ {file_id}")
            if len(result['failed_files']) > 5:
                print(f"   ... and {len(result['failed_files']) - 5} more")
                
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
     asyncio.run(main())
 