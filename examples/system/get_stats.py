#!/usr/bin/env python3
"""
Example: Get Client Statistics

This example demonstrates how to get comprehensive client and connection statistics.

Features demonstrated:
- Get overall client statistics
- Get connection pool statistics
- Monitor performance metrics
- Track usage patterns
- System health monitoring
"""

import asyncio
import sys
from pathlib import Path
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError, AuthenticationError


async def async_example():
    """Demonstrate async statistics gathering."""
    print("🔄 Async Example: Getting Client Statistics")
    print("=" * 60)
    
    try:
        async with DataQuery() as dq:
            # Perform some operations to generate statistics
            print("🔄 Performing operations to generate statistics...")
            
            # Make several API calls
            groups = await dq.list_groups_async(limit=5)
            print(f"   📋 Listed {len(groups)} groups")
            
            # Check health
            health = await dq.health_check_async()
            print(f"   🏥 Health check: {'✅ Healthy' if health else '❌ Unhealthy'}")
            
            if groups:
                # Try to list files for first group
                try:
                    files = await dq.list_files_async(groups[0].group_id)
                    if files and hasattr(files, 'file_group_ids'):
                        print(f"   📁 Listed {len(files.file_group_ids)} files")
                except:
                    print("   📁 No files accessible in first group")
            
            # Get comprehensive statistics
            print(f"\n📊 Getting comprehensive client statistics...")
            stats = dq.get_stats()
            
            print(f"✅ Statistics retrieved successfully!")
            print(f"\n📈 Client Statistics Overview:")
            
            # Show different statistics categories
            for category, data in stats.items():
                if isinstance(data, dict):
                    print(f"\n🔸 {category.replace('_', ' ').title()}:")
                    
                    # Show key metrics for each category
                    for key, value in data.items():
                        if isinstance(value, (int, float, str, bool)):
                            if isinstance(value, float):
                                print(f"   • {key.replace('_', ' ').title()}: {value:.2f}")
                            else:
                                print(f"   • {key.replace('_', ' ').title()}: {value}")
                        elif isinstance(value, dict):
                            print(f"   • {key.replace('_', ' ').title()}: {len(value)} items")
                        elif isinstance(value, list):
                            print(f"   • {key.replace('_', ' ').title()}: {len(value)} entries")
                else:
                    print(f"🔸 {category.replace('_', ' ').title()}: {data}")
            
            # Get connection pool statistics
            print(f"\n🔗 Getting connection pool statistics...")
            try:
                pool_stats = dq.get_pool_stats()
                
                print(f"✅ Pool statistics retrieved!")
                print(f"\n🏊 Connection Pool Details:")
                
                for key, value in pool_stats.items():
                    if isinstance(value, (int, float)):
                        if isinstance(value, float):
                            print(f"   • {key.replace('_', ' ').title()}: {value:.2f}")
                        else:
                            print(f"   • {key.replace('_', ' ').title()}: {value}")
                    else:
                        print(f"   • {key.replace('_', ' ').title()}: {value}")
                        
            except Exception as e:
                print(f"⚠️  Pool statistics not available: {e}")
            
            # Demonstrate monitoring over time
            print(f"\n📊 Monitoring statistics over time...")
            
            initial_stats = dq.get_stats()
            
            # Perform more operations
            print("🔄 Performing additional operations...")
            for i in range(3):
                try:
                    await dq.health_check_async()
                    if groups and i < len(groups):
                        await dq.list_files_async(groups[i].group_id)
                except:
                    pass
                await asyncio.sleep(0.5)
            
            final_stats = dq.get_stats()
            
            # Compare statistics
            print(f"\n📈 Statistics Comparison:")
            print(f"   🔍 Comparing initial vs final statistics...")
            
            # Compare specific metrics if available
            for category in ['rate_limiter', 'retry_manager', 'auth_info']:
                if category in initial_stats and category in final_stats:
                    initial = initial_stats[category]
                    final = final_stats[category]
                    
                    if isinstance(initial, dict) and isinstance(final, dict):
                        print(f"\n   📊 {category.replace('_', ' ').title()} Changes:")
                        for key in initial.keys():
                            if key in final:
                                init_val = initial[key]
                                final_val = final[key]
                                if isinstance(init_val, (int, float)) and isinstance(final_val, (int, float)):
                                    change = final_val - init_val
                                    if change != 0:
                                        print(f"     • {key}: {init_val} → {final_val} ({change:+})")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def sync_example():
    """Demonstrate sync statistics gathering."""
    print("\n🔄 Sync Example: Getting Client Statistics")
    print("=" * 60)
    
    dq = DataQuery()
    try:
        # Perform some operations
        print("🔄 Performing operations...")
        
        groups = dq.list_groups(limit=3)
        health = dq.health_check()
        
        print(f"   📋 Listed {len(groups)} groups")
        print(f"   🏥 Health: {'✅' if health else '❌'}")
        
        # Get statistics
        print(f"\n📊 Getting client statistics...")
        stats = dq.get_stats()
        
        print(f"✅ Statistics available!")
        
        # Show summary statistics
        print(f"\n📈 Quick Statistics Summary:")
        
        # Count total statistics
        total_categories = len(stats)
        total_metrics = 0
        
        for category, data in stats.items():
            if isinstance(data, dict):
                total_metrics += len(data)
            else:
                total_metrics += 1
        
        print(f"   • Total categories: {total_categories}")
        print(f"   • Total metrics: {total_metrics}")
        
        # Show key categories
        key_categories = ['rate_limiter', 'auth_info', 'client_config']
        for category in key_categories:
            if category in stats:
                print(f"   • {category.replace('_', ' ').title()}: ✅ Available")
            else:
                print(f"   • {category.replace('_', ' ').title()}: ❌ Not available")
        
        # Get pool statistics
        print(f"\n🔗 Getting pool statistics...")
        try:
            pool_stats = dq.get_pool_stats()
            print(f"✅ Pool statistics: {len(pool_stats)} metrics")
        except Exception as e:
            print(f"⚠️  Pool statistics not available: {e}")
        
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        dq.cleanup()


def demonstrate_monitoring_patterns():
    """Show patterns for monitoring and alerting."""
    print("\n📡 Monitoring and Alerting Patterns")
    print("=" * 50)
    
    print("1. 🚨 Performance monitoring:")
    print("   stats = dq.get_stats()")
    print("   ")
    print("   # Check response times")
    print("   if 'rate_limiter' in stats:")
    print("       rate_data = stats['rate_limiter']")
    print("       if 'average_response_time' in rate_data:")
    print("           if rate_data['average_response_time'] > 5.0:")
    print("               send_alert('High response time detected')")
    print()
    
    print("2. 📊 Usage tracking:")
    print("   stats = dq.get_stats()")
    print("   ")
    print("   # Track API usage")
    print("   if 'rate_limiter' in stats:")
    print("       requests_made = stats['rate_limiter'].get('requests_made', 0)")
    print("       print(f'Total requests: {requests_made}')")
    print("       ")
    print("       # Log usage for billing/monitoring")
    print("       log_usage_metric('api_requests', requests_made)")
    print()
    
    print("3. 🔗 Connection monitoring:")
    print("   pool_stats = dq.get_pool_stats()")
    print("   ")
    print("   # Monitor connection health")
    print("   active_connections = pool_stats.get('active_connections', 0)")
    print("   max_connections = pool_stats.get('max_connections', 100)")
    print("   ")
    print("   utilization = active_connections / max_connections")
    print("   if utilization > 0.8:")
    print("       send_alert('High connection pool utilization')")
    print()
    
    print("4. 🔐 Authentication monitoring:")
    print("   stats = dq.get_stats()")
    print("   ")
    print("   # Check auth status")
    print("   if 'auth_info' in stats:")
    print("       auth_data = stats['auth_info']")
    print("       if not auth_data.get('authenticated', False):")
    print("           send_alert('Authentication failure detected')")
    print("       ")
    print("       # Check token expiry")
    print("       if 'token_expires_in' in auth_data:")
    print("           expires_in = auth_data['token_expires_in']")
    print("           if expires_in < 300:  # Less than 5 minutes")
    print("               print('Token expiring soon - refresh needed')")


def performance_analysis_example():
    """Show how to analyze performance from statistics."""
    print("\n⚡ Performance Analysis Example")
    print("=" * 50)
    
    print("📊 Key performance metrics to monitor:")
    print()
    
    print("1. 🕒 Response Times:")
    print("   • Average response time < 2 seconds (good)")
    print("   • Average response time 2-5 seconds (acceptable)")
    print("   • Average response time > 5 seconds (poor)")
    print()
    
    print("2. 📈 Request Success Rate:")
    print("   • Success rate > 99% (excellent)")
    print("   • Success rate 95-99% (good)")
    print("   • Success rate < 95% (needs attention)")
    print()
    
    print("3. 🔄 Rate Limiting:")
    print("   • Rate limit hits = 0 (optimal)")
    print("   • Occasional rate limit hits (normal)")
    print("   • Frequent rate limit hits (reduce request rate)")
    print()
    
    print("4. 🔗 Connection Pool:")
    print("   • Pool utilization < 70% (healthy)")
    print("   • Pool utilization 70-90% (monitor)")
    print("   • Pool utilization > 90% (increase pool size)")
    print()
    
    print("💡 Performance optimization tips:")
    print("   • Use async methods for concurrent operations")
    print("   • Implement proper error handling and retries")
    print("   • Monitor and adjust rate limiting")
    print("   • Use connection pooling effectively")
    print("   • Cache frequently accessed data")


def main():
    """Run all examples."""
    print("🚀 PyDataQuery SDK - Get Statistics Example")
    print("=" * 60)
    
    # Run async example
    asyncio.run(async_example())
    
    # Run sync example
    sync_example()
    
    # Show monitoring patterns
    demonstrate_monitoring_patterns()
    
    # Show performance analysis
    performance_analysis_example()
    
    print("\n✨ Example completed!")
    print("💡 Use statistics for monitoring and optimization.")
    print("💡 Implement alerting based on key metrics.")
    print("💡 Regular monitoring helps maintain system health.")


if __name__ == "__main__":
    main()
