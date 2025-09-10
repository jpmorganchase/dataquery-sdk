#!/usr/bin/env python3
"""
Example: Complete Workflow

This example demonstrates a complete workflow using multiple dataquery-sdk endpoints.

Features demonstrated:
- Complete end-to-end workflow
- Health checking before operations
- Group discovery and selection
- File listing and availability checking
- File download with progress tracking
- Error handling throughout the process
- Statistics monitoring
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataquery import DataQuery
from dataquery.models import DownloadOptions, DownloadProgress
from dataquery.exceptions import (
    DataQueryError, AuthenticationError, NotFoundError, 
    NetworkError, RateLimitError
)


def progress_callback(progress: DownloadProgress):
    """Progress callback for downloads."""
    if progress.total_bytes > 0:
        percent = (progress.bytes_downloaded / progress.total_bytes) * 100
        mb_downloaded = progress.bytes_downloaded / (1024 * 1024)
        mb_total = progress.total_bytes / (1024 * 1024)
        print(f"    📥 Progress: {percent:.1f}% ({mb_downloaded:.2f}MB / {mb_total:.2f}MB)")
    else:
        mb_downloaded = progress.bytes_downloaded / (1024 * 1024)
        print(f"    📥 Downloaded: {mb_downloaded:.2f}MB")


async def complete_workflow():
    """Demonstrate a complete workflow."""
    print("🚀 dataquery-sdk - Complete Workflow Example")
    print("=" * 70)
    
    workflow_start = datetime.now()
    
    try:
        async with DataQuery() as dq:
            # Step 1: Health Check
            print("\n🏥 Step 1: Checking DataQuery Service Health")
            print("-" * 50)
            
            try:
                is_healthy = await dq.health_check_async()
                if is_healthy:
                    print("✅ DataQuery service is healthy and ready")
                else:
                    print("⚠️  DataQuery service is not responding optimally")
                    print("   Continuing with workflow...")
            except NetworkError:
                print("❌ Cannot reach DataQuery service")
                print("💡 Check your internet connection and try again")
                return
            
            # Step 2: Group Discovery
            print("\n📋 Step 2: Discovering Available Data Groups")
            print("-" * 50)
            
            try:
                groups = await dq.list_groups_async(limit=10)
                print(f"✅ Found {len(groups)} data groups:")
                
                for i, group in enumerate(groups[:5], 1):  # Show first 5
                    print(f"   {i}. {group.group_id}")
                
                if len(groups) > 5:
                    print(f"   ... and {len(groups) - 5} more groups")
                
            except AuthenticationError:
                print("❌ Authentication failed")
                print("💡 Check your credentials in .env file")
                return
            except DataQueryError as e:
                print(f"❌ Error listing groups: {e}")
                return
            
            # Step 3: Group Selection and File Discovery
            print("\n📁 Step 3: Exploring Files in Groups")
            print("-" * 50)
            
            target_group = None
            target_files = []
            
            for group in groups[:3]:  # Check first 3 groups
                try:
                    print(f"🔍 Checking group: {group.group_id}")
                    files = await dq.list_files_async(group.group_id)
                    
                    if files:
                        file_count = len(files)
                        print(f"   ✅ Found {file_count} files")
                        
                        target_group = group.group_id
                        target_files = files[:3]  # Take first 3 files
                        break
                    else:
                        print(f"   📭 No files found")
                        
                except NotFoundError:
                    print(f"   ❌ Access denied or group not found")
                except DataQueryError as e:
                    print(f"   ⚠️  Error: {e}")
            
            if not target_group or not target_files:
                print("\n⚠️  No accessible files found in available groups")
                print("💡 This workflow requires groups with downloadable files")
                
                # Show statistics anyway
                await show_statistics(dq)
                return
            
            print(f"\n🎯 Selected group: {target_group}")
            print(f"📄 Files to process: {len(target_files)}")
            
            # Step 4: File Information and Availability
            print("\n🔍 Step 4: Getting File Information and Availability")
            print("-" * 50)
            
            available_files = []
            
            for file_ref in target_files:
                file_id = file_ref.file_group_id
                print(f"\n📄 Processing file: {file_id}")
                
                try:
                    # Get file information
                    file_info = await dq.get_file_info_async(target_group, file_id)
                    
                    if file_info:
                        print(f"   📊 File info retrieved")
                        if hasattr(file_info, 'file_size') and file_info.file_size:
                            size_mb = file_info.file_size / (1024 * 1024)
                            print(f"   📏 Size: {size_mb:.2f} MB")
                    
                    # Check availability for recent dates
                    test_dates = []
                    today = datetime.now()
                    for i in range(5):  # Check last 5 days
                        date = today - timedelta(days=i)
                        test_dates.append(date.strftime("%Y%m%d"))
                    
                    available_date = None
                    for date in test_dates:
                        try:
                            availability = await dq.check_availability_async(file_id, date)
                            if availability and hasattr(availability, 'available') and availability.available:
                                available_date = date
                                print(f"   ✅ Available for date: {date}")
                                break
                        except NotFoundError:
                            continue
                    
                    if available_date:
                        available_files.append((file_id, available_date, file_info))
                    else:
                        print(f"   📭 No recent availability found")
                        
                except Exception as e:
                    print(f"   ❌ Error processing file: {e}")
            
            # Step 5: File Download
            print(f"\n📥 Step 5: Downloading Available Files")
            print("-" * 50)
            
            if not available_files:
                print("❌ No files available for download")
            else:
                # Setup download directory
                download_dir = Path("downloads")
                download_dir.mkdir(exist_ok=True)
                
                options = DownloadOptions(
                    destination_path=str(download_dir),
                    overwrite_existing=True,
                    chunk_size_setting=8192
                )
                
                successful_downloads = 0
                
                for file_id, date, file_info in available_files[:2]:  # Download first 2 files
                    print(f"\n📥 Downloading: {file_id} for {date}")
                    
                    try:
                        result = await dq.download_file_async(
                            file_group_id=file_id,
                            file_datetime=date,
                            destination_path=download_dir,
                            options=options,
                            progress_callback=progress_callback
                        )
                        
                        if result and hasattr(result, 'success') and result.success:
                            successful_downloads += 1
                            print(f"    ✅ Download completed successfully!")
                            
                            if hasattr(result, 'file_path'):
                                print(f"    📂 Saved to: {result.file_path}")
                        else:
                            print(f"    ❌ Download failed")
                            if hasattr(result, 'error_message'):
                                print(f"    📝 Error: {result.error_message}")
                                
                    except RateLimitError:
                        print(f"    ⏱️  Rate limit reached - pausing...")
                        await asyncio.sleep(2)
                    except DataQueryError as e:
                        print(f"    ❌ Download error: {e}")
                
                print(f"\n📊 Download Summary:")
                print(f"   • Files attempted: {len(available_files[:2])}")
                print(f"   • Successful downloads: {successful_downloads}")
                print(f"   • Download directory: {download_dir.absolute()}")
            
            # Step 6: Statistics and Summary
            print(f"\n📈 Step 6: Workflow Statistics and Summary")
            print("-" * 50)
            
            await show_statistics(dq)
            
            # Workflow summary
            workflow_duration = datetime.now() - workflow_start
            print(f"\n🎯 Workflow Summary:")
            print(f"   • Total duration: {workflow_duration.total_seconds():.2f} seconds")
            print(f"   • Groups explored: {len(groups)}")
            print(f"   • Files discovered: {len(target_files) if target_files else 0}")
            print(f"   • Files available: {len(available_files) if available_files else 0}")
            print(f"   • Downloads completed: {successful_downloads if 'successful_downloads' in locals() else 0}")
            
            print(f"\n✨ Workflow completed successfully! 🎉")
            
    except AuthenticationError as e:
        print(f"\n❌ Authentication Error: {e}")
        print("💡 Verify your credentials in the .env file")
    except NetworkError as e:
        print(f"\n❌ Network Error: {e}")
        print("💡 Check your internet connection")
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        print("💡 Check the logs for more details")


async def show_statistics(dq: DataQuery):
    """Show client statistics."""
    try:
        stats = dq.get_stats()
        
        print(f"\n📊 Client Statistics:")
        
        # Show key statistics
        if 'rate_limiter' in stats:
            rate_stats = stats['rate_limiter']
            if isinstance(rate_stats, dict):
                requests = rate_stats.get('requests_made', 'N/A')
                print(f"   • Total requests made: {requests}")
        
        if 'auth_info' in stats:
            auth_stats = stats['auth_info']
            if isinstance(auth_stats, dict):
                authenticated = auth_stats.get('authenticated', False)
                print(f"   • Authentication status: {'✅ Active' if authenticated else '❌ Inactive'}")
        
        # Pool statistics
        try:
            pool_stats = dq.get_pool_stats()
            if pool_stats:
                print(f"   • Connection pool: ✅ Active")
                active_connections = pool_stats.get('active_connections', 0)
                if active_connections:
                    print(f"   • Active connections: {active_connections}")
        except:
            print(f"   • Connection pool: ⚠️  Not available")
        
    except Exception as e:
        print(f"   ⚠️  Statistics not available: {e}")


def main():
    """Run the complete workflow."""
    print("🔄 Starting complete dataquery-sdk workflow...")
    print("💡 This example demonstrates all major SDK features")
    print()
    
    # Run the complete workflow
    asyncio.run(complete_workflow())
    
    print("\n💡 Next Steps:")
    print("   • Explore individual endpoint examples in subdirectories")
    print("   • Check the downloads/ directory for any downloaded files")
    print("   • Review the SDK documentation for advanced features")
    print("   • Implement error handling and monitoring for production use")


if __name__ == "__main__":
    main()
