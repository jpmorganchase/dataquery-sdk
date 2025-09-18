import argparse
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from dataquery.dataquery import DataQuery
from dataquery.models import DownloadProgress


async def main():
    # Load environment variables from local .env (if present)
    load_dotenv()

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Group Parallel Download by Date Range",
        epilog="""
Examples:
  %(prog)s GROUP123 20240101 20240131 ./downloads
  %(prog)s GROUP123 20240101 20240131 ./downloads
  %(prog)s GROUP123 20240101 20240131 ./downloads

Note: This script uses default concurrency settings (5 concurrent files, 5 parts per file).
To customize these settings, modify the script or use the DataQuery SDK directly.

Required Environment Variables:
  DATAQUERY_BASE_URL     - Base URL of the DataQuery API
  DATAQUERY_CLIENT_ID    - OAuth client ID (if using OAuth)
  DATAQUERY_CLIENT_SECRET - OAuth client secret (if using OAuth)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("group_id", help="Group ID to download files from")
    parser.add_argument(
        "start_date", help="Start date in YYYYMMDD format (e.g., 20240101)"
    )
    parser.add_argument("end_date", help="End date in YYYYMMDD format (e.g., 20240131)")
    parser.add_argument("destination", help="Destination directory for downloads")

    args = parser.parse_args()

    group_id = args.group_id
    start_date = args.start_date
    end_date = args.end_date
    dest = args.destination

    # Validate required environment variables
    required_env_vars = ["DATAQUERY_BASE_URL"]
    missing_vars = []
    for var in required_env_vars:
        value = os.getenv(var)
        if not value or value.strip() == "":
            missing_vars.append(var)

    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   {var}")
        print("\nPlease set these environment variables before running the script.")
        print("Example:")
        print("   export DATAQUERY_BASE_URL='https://api-developer.jpmorgan.com'")
        print("   export DATAQUERY_CLIENT_ID='your_client_id'")
        print("   export DATAQUERY_CLIENT_SECRET='your_client_secret'")
        return

    # Validate date format
    def validate_date_format(date_str, name):
        if len(date_str) != 8 or not date_str.isdigit():
            print(f"❌ Error: {name} must be in YYYYMMDD format (e.g., 20240101)")
            print(f"   Received: {date_str}")
            return False
        return True

    if not validate_date_format(start_date, "start_date"):
        return
    if not validate_date_format(end_date, "end_date"):
        return

    # Validate destination directory
    dest_path = Path(dest)
    if not dest_path.exists():
        try:
            dest_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Created destination directory: {dest_path.absolute()}")
        except Exception as e:
            print(f"❌ Error: Cannot create destination directory '{dest}': {e}")
            return

    # Use default concurrency settings from the SDK
    max_concurrent = 5  # Default from SDK
    num_parts = 5  # Default from SDK

    print("🚀 Group Parallel Download by Date Range")
    print(f"   Group ID: {group_id}")
    print(f"   Date Range: {start_date} to {end_date}")
    print(f"   Destination: {dest}")
    print(f"   Max Concurrent: {max_concurrent} (default)")
    print(f"   Parts per File: {num_parts} (default)")
    print()

    try:
        # Enable additional logs by default unless already set via environment
        os.environ.setdefault("DATAQUERY_ENABLE_DEBUG_LOGGING", "true")
        os.environ.setdefault("DATAQUERY_LOG_LEVEL", "DEBUG")

        async with DataQuery(enable_debug_logging=True, log_level="DEBUG") as dq:
            # Per-file progress callback (invoked for each file part update)
            def progress_callback(progress: DownloadProgress):
                pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
                print(
                    f"\rDownloading {progress.file_group_id}: {pct}", end="", flush=True
                )

            report = await dq.run_group_download_async(
                group_id=group_id,
                start_date=start_date,
                end_date=end_date,
                destination_dir=Path(dest),
                progress_callback=progress_callback,
            )

            print()  # newline after progress line

            # Check if there was an error (e.g., no files found)
            if report.get("error"):
                print(f"\n⚠️  {report.get('error')}")
            else:
                print("\n✅ Completed")

            # Display timing information
            print("\n⏱️  Timing Information:")
            print(f"   Total time: {report.get('total_time_formatted', 'N/A')}")
            print(f"   Total time (seconds): {report.get('total_time_seconds', 'N/A')}")
            if report.get("total_time_minutes", 0) >= 1:
                print(
                    f"   Total time (minutes): {report.get('total_time_minutes', 'N/A')}"
                )

            # Optional: show rate limiter / client stats
            try:
                stats = dq.get_stats()
                print(f"\n📊 Rate Limiter Stats: {stats.get('rate_limiter')}")
            except Exception:
                pass

            print("\n📁 Download Summary:")
            print(f"   Group: {report.get('group_id')}")
            print(f"   Range: {report.get('start_date')} to {report.get('end_date')}")
            print(f"   Total files: {report.get('total_files')}")
            print(f"   Successful: {report.get('successful_downloads')}")
            print(f"   Failed: {report.get('failed_downloads')}")
            print(f"   Success rate: {report.get('success_rate'):.2f}%")

            # Display concurrency and timing details (only if not an error case)
            if not report.get("error"):
                print("\n🚀 Performance Details:")
                print(f"   Max concurrent files: {report.get('max_concurrent')}")
                print(f"   Parts per file: {report.get('num_parts')}")
                print(
                    f"   Total concurrent requests: {report.get('total_concurrent_requests')}"
                )
                print(f"   Concurrency model: {report.get('concurrency_model', 'N/A')}")
                print(
                    f"   Rate limit protection: {report.get('rate_limit_protection', 'N/A')}"
                )

                if report.get("intelligent_delay"):
                    print(
                        f"   Intelligent delay: {report.get('intelligent_delay'):.3f}s"
                    )
                    print(f"   Delay range: {report.get('delay_range', 'N/A')}")

            if report.get("failed_files"):
                print(f"\n❌ Failed files: {report.get('failed_files')}")

            # Display per-file timing information
            per_file_timing = report.get("per_file_timing", {})
            if per_file_timing and per_file_timing.get("file_times"):
                print("\n⏱️  Per-File Timing Details:")
                print(
                    f"   Total download time: {per_file_timing.get('total_download_time_formatted', 'N/A')}"
                )
                print(
                    f"   Average file time: {per_file_timing.get('average_file_time_seconds', 0):.2f}s"
                )
                print(
                    f"   Fastest file: {per_file_timing.get('min_file_time_seconds', 0):.2f}s"
                )
                print(
                    f"   Slowest file: {per_file_timing.get('max_file_time_seconds', 0):.2f}s"
                )

                print("\n📁 Individual File Times:")
                file_times = per_file_timing.get("file_times", [])
                for i, file_info in enumerate(
                    file_times[:10], 1
                ):  # Show first 10 files
                    file_id = file_info.get("file_group_id", "unknown")
                    download_time = file_info.get("download_time_seconds", 0)
                    file_size = file_info.get("file_size_bytes", 0)
                    speed = file_info.get("speed_mbps", 0)

                    # Format file size
                    if file_size >= 1024 * 1024:
                        size_str = f"{file_size/(1024*1024):.1f}MB"
                    elif file_size >= 1024:
                        size_str = f"{file_size/1024:.1f}KB"
                    else:
                        size_str = f"{file_size}B"

                    print(
                        f"   {i:2d}. {file_id}: {download_time:.2f}s ({size_str}, {speed:.1f}MB/s)"
                    )

                if len(file_times) > 10:
                    print(f"   ... and {len(file_times) - 10} more files")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
