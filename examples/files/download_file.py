#!/usr/bin/env python3
"""
Lean example: interactively download a file.
"""

import asyncio
import sys
from pathlib import Path
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.models import DownloadOptions, DownloadProgress
from dataquery.exceptions import AuthenticationError, NotFoundError


def simple_progress_callback(progress: DownloadProgress):
    """Simple progress callback showing percentage."""
    if progress.total_bytes > 0:
        percent = (progress.bytes_downloaded / progress.total_bytes) * 100
        print(f"📥 Download progress: {percent:.1f}% ({progress.bytes_downloaded}/{progress.total_bytes} bytes)")
    else:
        print(f"📥 Downloaded: {progress.bytes_downloaded} bytes")


def detailed_progress_callback(progress: DownloadProgress):
    """Detailed progress callback with more information."""
    if progress.total_bytes > 0:
        percent = (progress.bytes_downloaded / progress.total_bytes) * 100
        mb_downloaded = progress.bytes_downloaded / (1024 * 1024)
        mb_total = progress.total_bytes / (1024 * 1024)
        
        print(f"📊 Progress: {percent:.1f}% | {mb_downloaded:.2f}MB / {mb_total:.2f}MB")
        
        # Show download speed if available
        if hasattr(progress, 'download_speed') and progress.download_speed:
            speed_mbps = progress.download_speed / (1024 * 1024)
            print(f"    Speed: {speed_mbps:.2f} MB/s")
    else:
        mb_downloaded = progress.bytes_downloaded / (1024 * 1024)
        print(f"📊 Downloaded: {mb_downloaded:.2f}MB")


async def main():
    print("🚀 Download File (lean)")
    file_group_id = input("Enter file_group_id: ").strip()
    if not file_group_id:
        print("❌ file_group_id is required")
        return
    file_datetime = input("Enter file date (YYYYMMDD) [optional]: ").strip() or None
    dest = input("Destination directory [./downloads]: ").strip() or "./downloads"
    overwrite = (input("Overwrite existing? (y/N) [N]: ").strip().lower() in ("y","yes"))

    options = DownloadOptions(destination_path=dest, overwrite_existing=overwrite)
    try:
        async with DataQuery() as dq:
            try:
                result = await dq.download_file_async(
                    file_group_id=file_group_id,
                    file_datetime=file_datetime,
                    options=options,
                    progress_callback=simple_progress_callback
                )
            except NotFoundError:
                print("📭 Not available for that date")
                return
            if getattr(result, 'status', None) and result.status.value == "completed":
                print(f"✅ Downloaded to: {result.local_path}")
            elif getattr(result, 'success', False):
                print("✅ Download completed")
            else:
                print(f"❌ Download failed: {getattr(result, 'error_message', 'unknown error')}")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())


 
