import asyncio
from pathlib import Path

from dotenv import load_dotenv

from dataquery import DataQuery
from dataquery.models import DownloadOptions, DownloadProgress


def simple_progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\rProgress: {pct}", end="", flush=True)


async def main():
    # Load environment variables from a local .env if present
    load_dotenv()
    print("🚀 Parallel Range Download Example")
    file_group_id = input("Enter file_group_id: ").strip()
    if not file_group_id:
        print("❌ file_group_id is required")
        return

    file_datetime = (
        input("Enter file date (YYYYMMDD/THHMM/THHMMSS) [optional]: ").strip() or None
    )
    dest = input("Destination directory [./downloads]: ").strip() or "./downloads"
    overwrite = input("Overwrite existing? (y/N) [N]: ").strip().lower() in ("y", "yes")

    options = DownloadOptions(overwrite_existing=overwrite)

    try:
        async with DataQuery() as dq:
            # Use the SDK download method
            result = await dq.download_file_async(
                file_group_id=file_group_id,
                file_datetime=file_datetime,
                destination_path=Path(dest),
                options=options,
                progress_callback=simple_progress_callback,
            )

            print()  # newline after progress
            if (
                result
                and getattr(result, "status", None)
                and result.status.value == "completed"
            ):
                print("✅ Download completed")
                print(f"📁 File saved to: {result.local_path}")
                print(f"📊 File size: {result.file_size:,} bytes")
                print(f"⏱️  Download time: {result.download_time:.2f} seconds")
                print(f"🚀 Speed: {result.speed_mbps:.2f} MB/s")
            else:
                print(
                    f"❌ Download failed: {getattr(result, 'error_message', 'unknown error')}"
                )
                dt = getattr(result, "download_time", None)
                if dt is not None:
                    print(f"⏱️  Download time: {dt:.2f} seconds")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
