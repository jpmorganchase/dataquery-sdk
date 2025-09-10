import argparse
import asyncio
from pathlib import Path
from dotenv import load_dotenv

from dataquery.dataquery import DataQuery
from dataquery.models import DownloadOptions, DownloadProgress


def simple_progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\rProgress: {pct}", end="", flush=True)


async def main():
    # Load environment variables from a local .env if present
    load_dotenv()
    parser = argparse.ArgumentParser(description="Parallel Range Download Example")
    parser.add_argument("--file-group-id", required=True, help="File group ID")
    parser.add_argument("--file-datetime", default=None, help="File date (YYYYMMDD/THHMM/THHMMSS) [optional]")
    parser.add_argument("--dest", default="./downloads", help="Destination directory")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--num-parts", type=int, default=5, help="Number of parallel parts")
    args = parser.parse_args()

    file_group_id = args.file_group_id
    file_datetime = args.file_datetime
    dest = args.dest
    overwrite = args.overwrite
    num_parts = args.num_parts

    options = DownloadOptions(destination_path=dest, overwrite_existing=overwrite)

    try:
        async with DataQuery() as dq:
            # Use the client-level parallel downloader
            result = await dq._client.download_file_parallel_async(
                file_group_id=file_group_id,
                file_datetime=file_datetime,
                options=options,
                num_parts=num_parts,
                progress_callback=simple_progress_callback,
            )

            print()  # newline after progress
            if result and getattr(result, 'status', None) and result.status.value == "completed":
                print("✅ Download completed")
                print(f"📁 File saved to: {result.local_path}")
                print(f"📊 File size: {result.file_size:,} bytes")
                print(f"⏱️  Download time: {result.download_time:.2f} seconds")
                print(f"🚀 Speed: {result.speed_mbps:.2f} MB/s")
            else:
                print(f"❌ Download failed: {getattr(result, 'error_message', 'unknown error')}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())