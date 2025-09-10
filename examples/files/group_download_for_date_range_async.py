#!/usr/bin/env python3
"""
Example: Group Download for Date Range with Progress Callbacks (Async)

This example demonstrates how to download all files in a group for a date range
using the DATAQUERY SDK with asynchronous methods and progress callbacks.

Usage:
    python examples/group_download_for_date_range_async.py --group-id <GROUP_ID> --start-date <YYYYMMDD> --end-date <YYYYMMDD> [--progress-type TYPE]
"""
import asyncio
import sys
import time
from pathlib import Path
import argparse
from typing import Dict, List, Optional, Callable

# Add the parent directory to the path so we can import the SDK
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataquery import DataQuery, setup_logging
from dataquery.models import DownloadStatus, DownloadProgress, DownloadOptions


class GroupDownloadProgressTracker:
    """Tracks progress for group downloads."""

    def __init__(self, total_files: int):
        self.total_files = total_files
        self.completed_files = 0
        self.failed_files = 0
        self.current_file_progress = 0.0
        self.start_time = time.time()
        self.file_results = {}

    def update_file_progress(self, file_group_id: str, progress: DownloadProgress):
        """Update progress for a specific file."""
        self.current_file_progress = progress.percentage
        self.file_results[file_group_id] = progress

    def mark_file_completed(self, file_group_id: str, success: bool):
        """Mark a file as completed."""
        if success:
            self.completed_files += 1
        else:
            self.failed_files += 1

    @property
    def overall_progress(self) -> float:
        """Calculate overall progress percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.completed_files / self.total_files) * 100

    @property
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self.start_time

    def get_summary(self) -> Dict:
        """Get a summary of the download progress."""
        return {
            "total_files": self.total_files,
            "completed_files": self.completed_files,
            "failed_files": self.failed_files,
            "overall_progress": self.overall_progress,
            "current_file_progress": self.current_file_progress,
            "elapsed_time": self.elapsed_time,
            "success_rate": (self.completed_files / self.total_files) * 100 if self.total_files > 0 else 0
        }


def simple_group_progress_callback(tracker: GroupDownloadProgressTracker):
    """Simple progress callback for group downloads."""
    summary = tracker.get_summary()
    print(f"📊 Group Progress: {summary['overall_progress']:.1f}% | "
          f"Completed: {summary['completed_files']}/{summary['total_files']} | "
          f"Current File: {summary['current_file_progress']:.1f}%")


def detailed_group_progress_callback(tracker: GroupDownloadProgressTracker):
    """Detailed progress callback for group downloads."""
    summary = tracker.get_summary()
    eta_str = ""
    if summary['completed_files'] > 0:
        avg_time_per_file = summary['elapsed_time'] / summary['completed_files']
        remaining_files = summary['total_files'] - summary['completed_files']
        eta_seconds = remaining_files * avg_time_per_file
        if eta_seconds < 60:
            eta_str = f"ETA: {eta_seconds:.1f}s"
        elif eta_seconds < 3600:
            eta_str = f"ETA: {eta_seconds/60:.1f}m"
        else:
            eta_str = f"ETA: {eta_seconds/3600:.1f}h"

    print(f"📊 Group Progress: {summary['overall_progress']:.1f}% | "
          f"Completed: {summary['completed_files']}/{summary['total_files']} | "
          f"Failed: {summary['failed_files']} | "
          f"Current File: {summary['current_file_progress']:.1f}% | "
          f"Time: {summary['elapsed_time']:.1f}s | {eta_str}")


def tqdm_style_group_progress_callback(tracker: GroupDownloadProgressTracker):
    """TQDM-style progress callback for group downloads."""
    summary = tracker.get_summary()
    bar_length = 40
    filled_length = int(bar_length * summary['overall_progress'] / 100)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    eta_str = ""
    if summary['completed_files'] > 0:
        avg_time_per_file = summary['elapsed_time'] / summary['completed_files']
        remaining_files = summary['total_files'] - summary['completed_files']
        eta_seconds = remaining_files * avg_time_per_file
        eta_str = f"ETA: {eta_seconds:.1f}s" if eta_seconds < 60 else f"ETA: {eta_seconds/60:.1f}m"

    print(f"\r[{bar}] {summary['overall_progress']:.1f}% | "
          f"{summary['completed_files']}/{summary['total_files']} files | "
          f"Current: {summary['current_file_progress']:.1f}% | {eta_str}", end='', flush=True)

    if summary['overall_progress'] >= 100:
        print()  # New line when complete


def file_progress_callback(tracker: GroupDownloadProgressTracker, file_group_id: str):
    """Create a progress callback for individual file downloads."""
    def callback(progress: DownloadProgress):
        tracker.update_file_progress(file_group_id, progress)
        # You can add file-specific progress display here if needed
    return callback


# Progress callback registry
GROUP_PROGRESS_CALLBACKS = {
    'simple': simple_group_progress_callback,
    'detailed': detailed_group_progress_callback,
    'tqdm': tqdm_style_group_progress_callback,
    'none': None
}


async def download_group_with_progress(
        dataquery: DataQuery,
        group_id: str,
        start_date: str,
        end_date: str,
        destination_dir: Path,
        max_concurrent: int,
        progress_callback: Optional[Callable] = None
) -> Dict:
    """Download group files with progress tracking."""

    # Get available files
    print(f"🔍 Finding available files for group '{group_id}' from {start_date} to {end_date}...")
    files = await dataquery.list_available_files_async(
        group_id=group_id,
        start_date=start_date,
        end_date=end_date
    )
    available_files = [file for file in files if file.get('is-available') is True]

    if not available_files:
        print("❌ No available files found for the specified date range")
        return {"error": "No available files found for date range"}

    print(f"✅ Found {len(available_files)} files to download")

    # Initialize progress tracker
    tracker = GroupDownloadProgressTracker(len(available_files))

    # Create destination directory
    dest_dir = destination_dir / group_id
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Download files with progress tracking
    print(f"🚀 Starting download of {len(available_files)} files...")
    print("-" * 60)

    results = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async def download_file_with_progress(file_info):
        async with semaphore:
            file_group_id = file_info.get('file-group-id', file_info.get('file_group_id'))
            file_datetime = file_info.get('file-datetime', file_info.get('file_datetime'))

            if not file_group_id:
                print(f"❌ File info missing file-group-id: {file_info}")
                tracker.mark_file_completed(file_group_id, False)
                return None

            # Generate filename
            if file_datetime:
                filename = f"{file_group_id}_{file_datetime}"
            else:
                filename = file_group_id

            # Get file extension
            file_extension = file_info.get('extension', '.bin')
            if not file_extension.startswith('.'):
                file_extension = f".{file_extension}"

            dest_path = dest_dir / f"{filename}{file_extension}"

            # Create download options with progress callback
            download_options = DownloadOptions(
                destination_path=dest_path,
                show_progress=False,  # We'll handle progress display ourselves
                progress_callback=file_progress_callback(tracker, file_group_id)
            )

            try:
                result = await dataquery.download_file_async(
                    file_group_id,
                    file_datetime=file_datetime,
                    options=download_options
                )

                success = result.status == DownloadStatus.COMPLETED
                tracker.mark_file_completed(file_group_id, success)

                # Call group progress callback
                if progress_callback:
                    progress_callback(tracker)

                if success:
                    print(f"✅ Downloaded: {file_group_id}")
                else:
                    print(f"❌ Failed: {file_group_id} - {result.error_message}")

                return result

            except Exception as e:
                tracker.mark_file_completed(file_group_id, False)
                print(f"❌ Error downloading {file_group_id}: {e}")

                # Call group progress callback
                if progress_callback:
                    progress_callback(tracker)

                return None

    # Start all download tasks
    download_tasks = [download_file_with_progress(f) for f in available_files]
    download_results = await asyncio.gather(*download_tasks, return_exceptions=True)

    # Process results
    successful = []
    failed = []

    for r in download_results:
        if isinstance(r, Exception):
            failed.append(r)
        elif r and hasattr(r, 'status') and getattr(r, 'status', None) == DownloadStatus.COMPLETED:
            successful.append(r)
        else:
            failed.append(r)

    # Final progress update
    if progress_callback:
        progress_callback(tracker)

    print("-" * 60)
    print("📊 Download Summary:")
    print(f"   Total files: {len(available_files)}")
    print(f"   Successful: {len(successful)}")
    print(f"   Failed: {len(failed)}")
    print(f"   Success rate: {(len(successful) / len(available_files)) * 100:.1f}%")
    print(f"   Total time: {tracker.elapsed_time:.2f} seconds")

    return {
        "group_id": group_id,
        "start_date": start_date,
        "end_date": end_date,
        "total_files": len(available_files),
        "successful_downloads": len(successful),
        "failed_downloads": len(failed),
        "success_rate": (len(successful) / len(available_files)) * 100 if available_files else 0,
        "downloaded_files": [r.file_group_id for r in successful if hasattr(r, 'file_group_id')],
        "failed_files": [str(f) if isinstance(f, Exception) else f.get('file-group-id', f.get('file_group_id', 'unknown')) for f in failed if isinstance(f, dict)],
        "total_time": tracker.elapsed_time,
        "destination": str(dest_dir)
    }


async def main():
    parser = argparse.ArgumentParser(description="Group Download for Date Range with Progress Callbacks Example (Async)")
    parser.add_argument("--group-id", required=False, help="Group ID")
    parser.add_argument("--start-date", required=False, help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", required=False, help="End date (YYYYMMDD)")
    parser.add_argument("--destination", default="./downloads", help="Destination directory")
    parser.add_argument("--max-concurrent", type=int, default=6, help="Maximum concurrent downloads")
    parser.add_argument("--progress-type", choices=list(GROUP_PROGRESS_CALLBACKS.keys()),
                        default="detailed", help="Type of progress callback to use")
    args = parser.parse_args()

    # Setup logging
    setup_logging("DEBUG")

    # Prompt if not provided
    group_id = args.group_id or input("Enter group ID: ").strip()
    start_date = args.start_date or input("Enter start date (YYYYMMDD): ").strip()
    end_date = args.end_date or input("Enter end date (YYYYMMDD): ").strip()
    destination = Path(args.destination)
    max_concurrent = args.max_concurrent
    progress_callback = GROUP_PROGRESS_CALLBACKS[args.progress_type]

    try:
        # Use async context manager for automatic cleanup
        async with DataQuery() as dataquery:
            print(f"🚀 Starting group download for '{group_id}' from {start_date} to {end_date}")
            print(f"📁 Destination: {destination}")
            print(f"⚡ Max concurrent: {max_concurrent}")
            print(f"📊 Progress type: {args.progress_type}")
            print("-" * 60)

            report = await download_group_with_progress(
                dataquery,
                group_id,
                start_date,
                end_date,
                destination,
                max_concurrent,
                progress_callback
            )

            if "error" not in report:
                print("\n✅ Group download completed successfully!")
                print(f"📁 Files saved to: {report['destination']}")
            else:
                print(f"\n❌ Group download failed: {report['error']}")
                return 1

        return 0

    except Exception as e:
        print(f"❌ FAILED: {e}")
        return 1

if __name__ == "__main__":
    setup_logging("INFO")
    exit_code = asyncio.run(main())
    sys.exit(exit_code)