import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

from dataquery.dataquery import DataQuery
from dataquery.models import DownloadProgress


async def main():
    # Load environment variables from local .env (if present)
    load_dotenv()

    print("🚀 Group Parallel Download by Date Range")
    group_id = input("Enter group_id: ").strip()
    if not group_id:
        print("❌ group_id is required")
        return

    start_date = input("Enter start date (YYYYMMDD): ").strip()
    end_date = input("Enter end date (YYYYMMDD): ").strip()
    if not start_date or not end_date:
        print("❌ start_date and end_date are required")
        return

    dest = input("Destination directory [./downloads]: ").strip() or "./downloads"

    try:

        async with DataQuery(enable_debug_logging=True, log_level="DEBUG") as dq:
            # Per-file progress callback (invoked for each file part update)
            def progress_callback(progress: DownloadProgress):
                pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
                print(f"\rDownloading {progress.file_group_id}: {pct}", end="", flush=True)

            report = await dq.run_group_download_parallel_async(
                group_id=group_id,
                start_date=start_date,
                end_date=end_date,
                destination_dir=Path(dest),
                progress_callback=progress_callback,
            )

            print()  # newline after progress line

            # Check if there was an error (e.g., no files found)
            if report.get('error'):
                print(f"\n⚠️  {report.get('error')}")
            else:
                print("\n✅ Completed")

            # Display timing information
            print(f"\n⏱️  Timing Information:")
            print(f"   Total time: {report.get('total_time_formatted', 'N/A')}")
            print(f"   Total time (seconds): {report.get('total_time_seconds', 'N/A')}")
            if report.get('total_time_minutes', 0) >= 1:
                print(f"   Total time (minutes): {report.get('total_time_minutes', 'N/A')}")

            # Optional: show rate limiter / client stats
            try:
                stats = dq.get_stats()
                print(f"\n📊 Rate Limiter Stats: {stats.get('rate_limiter')}")
            except Exception:
                pass

            print(f"\n📁 Download Summary:")
            print(f"   Group: {report.get('group_id')}")
            print(f"   Range: {report.get('start_date')} to {report.get('end_date')}")
            print(f"   Total files: {report.get('total_files')}")
            print(f"   Successful: {report.get('successful_downloads')}")
            print(f"   Failed: {report.get('failed_downloads')}")
            print(f"   Success rate: {report.get('success_rate'):.2f}%")

            # Display concurrency and timing details (only if not an error case)
            if not report.get('error'):
                print(f"\n🚀 Performance Details:")
                print(f"   Max concurrent files: {report.get('max_concurrent')}")
                print(f"   Parts per file: {report.get('num_parts')}")
                print(f"   Total concurrent requests: {report.get('total_concurrent_requests')}")
                print(f"   Concurrency model: {report.get('concurrency_model', 'N/A')}")
                print(f"   Rate limit protection: {report.get('rate_limit_protection', 'N/A')}")

                if report.get('intelligent_delay'):
                    print(f"   Intelligent delay: {report.get('intelligent_delay'):.3f}s")
                    print(f"   Delay range: {report.get('delay_range', 'N/A')}")

            if report.get('failed_files'):
                print(f"\n❌ Failed files: {report.get('failed_files')}")

            # Display per-file timing information
            per_file_timing = report.get('per_file_timing', {})
            if per_file_timing and per_file_timing.get('file_times'):
                print(f"\n⏱️  Per-File Timing Details:")
                print(f"   Total download time: {per_file_timing.get('total_download_time_formatted', 'N/A')}")
                print(f"   Average file time: {per_file_timing.get('average_file_time_seconds', 0):.2f}s")
                print(f"   Fastest file: {per_file_timing.get('min_file_time_seconds', 0):.2f}s")
                print(f"   Slowest file: {per_file_timing.get('max_file_time_seconds', 0):.2f}s")

                print(f"\n📁 Individual File Times:")
                file_times = per_file_timing.get('file_times', [])
                for i, file_info in enumerate(file_times[:10], 1):  # Show first 10 files
                    file_id = file_info.get('file_group_id', 'unknown')
                    download_time = file_info.get('download_time_seconds', 0)
                    file_size = file_info.get('file_size_bytes', 0)
                    speed = file_info.get('speed_mbps', 0)

                    # Format file size
                    if file_size >= 1024 * 1024:
                        size_str = f"{file_size / (1024 * 1024):.1f}MB"
                    elif file_size >= 1024:
                        size_str = f"{file_size / 1024:.1f}KB"
                    else:
                        size_str = f"{file_size}B"

                    print(f"   {i:2d}. {file_id}: {download_time:.2f}s ({size_str}, {speed:.1f}MB/s)")

                if len(file_times) > 10:
                    print(f"   ... and {len(file_times) - 10} more files")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
