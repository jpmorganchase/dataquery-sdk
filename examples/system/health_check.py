#!/usr/bin/env python3
"""
Example: Health Check

This example demonstrates how to check the health status of the DataQuery service.

Features demonstrated:
- Service health verification
- Async and sync health checks
- Error handling for service issues
- Connection testing
- System status monitoring
"""

import asyncio
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError, AuthenticationError, NetworkError


async def async_example():
    """Demonstrate async health check."""
    print("🔄 Async Example: Health Check")
    print("=" * 40)
    
    try:
        async with DataQuery() as dq:
            print("🏥 Checking DataQuery service health...")
            
            # Perform health check
            start_time = time.time()
            is_healthy = await dq.health_check_async()
            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            if is_healthy:
                print(f"✅ Service is healthy!")
                print(f"📶 Response time: {response_time:.2f}ms")
            else:
                print(f"⚠️  Service is not healthy")
                print(f"📶 Response time: {response_time:.2f}ms")
            
            # Perform multiple health checks to test consistency
            print(f"\n🔄 Performing multiple health checks...")
            results = []
            times = []
            
            for i in range(3):
                start = time.time()
                try:
                    result = await dq.health_check_async()
                    duration = (time.time() - start) * 1000
                    results.append(result)
                    times.append(duration)
                    print(f"  Check {i+1}: {'✅ Healthy' if result else '❌ Unhealthy'} ({duration:.2f}ms)")
                except Exception as e:
                    print(f"  Check {i+1}: ❌ Error - {e}")
                    results.append(False)
                    times.append(0)
                
                # Small delay between checks
                await asyncio.sleep(0.5)
            
            # Show statistics
            if times:
                avg_time = sum(times) / len(times)
                healthy_count = sum(1 for r in results if r)
                print(f"\n📊 Health Check Statistics:")
                print(f"   • Successful checks: {healthy_count}/{len(results)}")
                print(f"   • Average response time: {avg_time:.2f}ms")
                print(f"   • Min response time: {min(times):.2f}ms")
                print(f"   • Max response time: {max(times):.2f}ms")
                
                if healthy_count == len(results):
                    print(f"   🎯 Service status: EXCELLENT")
                elif healthy_count > len(results) / 2:
                    print(f"   ⚠️  Service status: UNSTABLE")
                else:
                    print(f"   ❌ Service status: UNHEALTHY")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Health check failed due to authentication issues")
    except NetworkError as e:
        print(f"❌ Network error: {e}")
        print("💡 Unable to reach DataQuery service")
    except DataQueryError as e:
        print(f"❌ Service error: {e}")
        print("💡 DataQuery service returned an error")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def sync_example():
    """Demonstrate sync health check."""
    print("\n🔄 Sync Example: Health Check")
    print("=" * 40)
    
    dq = DataQuery()
    try:
        print("🏥 Checking DataQuery service health...")
        
        start_time = time.time()
        is_healthy = dq.health_check()
        response_time = (time.time() - start_time) * 1000
        
        if is_healthy:
            print(f"✅ Service is healthy!")
            print(f"📶 Response time: {response_time:.2f}ms")
            
            # Additional status information
            print(f"\n📋 Service Status:")
            print(f"   • Status: OPERATIONAL")
            print(f"   • Latency: {'LOW' if response_time < 1000 else 'HIGH'}")
            print(f"   • Connection: STABLE")
            
        else:
            print(f"⚠️  Service is not healthy")
            print(f"📶 Response time: {response_time:.2f}ms")
            print(f"\n📋 Service Status:")
            print(f"   • Status: DOWN or MAINTENANCE")
            print(f"   • Recommended action: Retry later")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials")
    except NetworkError as e:
        print(f"❌ Network error: {e}")
        print("💡 Check your internet connection")
    except DataQueryError as e:
        print(f"❌ Service error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        dq.cleanup()


def monitoring_simulation():
    """Simulate continuous health monitoring."""
    print("\n📡 Continuous Health Monitoring Simulation")
    print("=" * 60)
    
    print("💡 Example of how to implement continuous monitoring:")
    print()
    
    print("🔄 Monitoring loop pseudocode:")
    print("   while monitoring_active:")
    print("       try:")
    print("           is_healthy = await dq.health_check_async()")
    print("           if not is_healthy:")
    print("               send_alert('Service is down!')")
    print("           log_health_status(is_healthy)")
    print("       except Exception as e:")
    print("           log_error(e)")
    print("       await asyncio.sleep(30)  # Check every 30 seconds")
    print()
    
    print("🔔 Alert conditions:")
    print("   • Service returns unhealthy status")
    print("   • Response time exceeds threshold (e.g., 5 seconds)")
    print("   • Multiple consecutive failures")
    print("   • Network connectivity issues")
    print()
    
    print("📊 Monitoring metrics to track:")
    print("   • Uptime percentage")
    print("   • Average response time")
    print("   • Error rate")
    print("   • Service availability windows")


def troubleshooting_guide():
    """Show troubleshooting steps for health check issues."""
    print("\n🔧 Health Check Troubleshooting Guide")
    print("=" * 50)
    
    print("❌ If health check fails:")
    print()
    
    print("1. 🔐 Authentication Issues:")
    print("   • Verify credentials in .env file")
    print("   • Check if tokens are expired")
    print("   • Validate client_id and client_secret")
    print()
    
    print("2. 🌐 Network Issues:")
    print("   • Check internet connectivity")
    print("   • Verify firewall settings")
    print("   • Test DNS resolution")
    print("   • Try different network (e.g., mobile hotspot)")
    print()
    
    print("3. ⚙️  Service Issues:")
    print("   • Check DataQuery service status page")
    print("   • Verify API endpoint URL")
    print("   • Look for maintenance announcements")
    print("   • Contact support if persistent")
    print()
    
    print("4. 🐛 Code Issues:")
    print("   • Check for typos in configuration")
    print("   • Verify SDK version compatibility")
    print("   • Review error logs for details")
    print("   • Test with minimal example")


def main():
    """Run all examples."""
    print("🚀 dataquery-sdk - Health Check Example")
    print("=" * 60)
    
    # Run async example
    asyncio.run(async_example())
    
    # Run sync example
    sync_example()
    
    # Show monitoring concepts
    monitoring_simulation()
    
    # Show troubleshooting guide
    troubleshooting_guide()
    
    print("\n✨ Example completed!")
    print("💡 Use health checks before making API calls in production.")
    print("💡 Implement monitoring for production systems.")
    print("💡 Check get_stats.py for detailed client statistics.")


if __name__ == "__main__":
    main()
