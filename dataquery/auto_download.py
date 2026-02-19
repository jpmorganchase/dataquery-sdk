"""
Auto-Download Manager for dataquery-sdk

This module provides automatic file download functionality that continuously
monitors data groups and downloads new files when they become available.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .config import EnvConfig
from .models import DownloadOptions, DownloadProgress


class AutoDownloadManager:
    """
    Manages automatic file download monitoring and downloading.

    This class continuously monitors a data group for new files and automatically
    downloads them if they don't already exist in the destination folder.

    Features:
    - Continuous monitoring with configurable intervals
    - File filtering capabilities
    - Progress tracking and callbacks
    - Error handling and retry logic
    - Statistics and monitoring
    - Graceful shutdown
    """

    def __init__(
        self,
        client,  # DataQueryClient instance
        group_id: str,
        destination_dir: str = "./downloads",
        interval_minutes: int = 30,
        file_filter: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
        max_retries: int = 3,
        check_current_date_only: bool = True,
        max_concurrent_downloads: Optional[int] = None,
    ):
        """
        Initialize the AutoDownloadManager.

        Args:
            client: DataQueryClient instance
            group_id: ID of the data group to monitor
            destination_dir: Directory to download files to
            interval_minutes: Check interval in minutes
            file_filter: Optional function to filter files
            progress_callback: Optional callback for download progress
            error_callback: Optional callback for errors
            max_retries: Maximum retry attempts for failed downloads
            check_current_date_only: If True, only check files for current date
            max_concurrent_downloads: Maximum concurrent downloads (uses SDK default if None)
        """
        self.client = client
        self.group_id = group_id
        self.destination_dir = Path(destination_dir)
        self.interval_minutes = interval_minutes
        self.file_filter = file_filter
        self.progress_callback = progress_callback
        self.error_callback = error_callback
        self.max_retries = max_retries
        self.check_current_date_only = check_current_date_only
        # Use SDK default if not specified
        self.max_concurrent_downloads = (
            max_concurrent_downloads
            if max_concurrent_downloads is not None
            else EnvConfig.get_int("MAX_CONCURRENT_DOWNLOADS", "5")
        )

        # Create destination directory if it doesn't exist
        self.destination_dir.mkdir(parents=True, exist_ok=True)

        # Internal state
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._downloaded_files: Set[str] = set()
        self._failed_files: Dict[str, int] = {}  # file_group_id -> retry_count

        # Statistics
        self.stats: Dict[str, Any] = {
            "start_time": None,
            "last_check_time": None,
            "total_checks": 0,
            "files_discovered": 0,
            "files_downloaded": 0,
            "files_skipped": 0,
            "download_failures": 0,
            "total_bytes_downloaded": 0,
            "errors": [],
        }

        # Setup logging
        self.logger = logging.getLogger(f"AutoDownloadManager.{group_id}")

        # Download options
        # Note: destination_path is used by the client to treat this as a directory target
        self.download_options = DownloadOptions(
            destination_path=self.destination_dir,
            overwrite_existing=False,  # Don't overwrite existing files
            chunk_size=8192,
        )
        # Backward-compat extra attribute used by tests; harmless extra field
        setattr(self.download_options, "output_dir", str(self.destination_dir))

    async def start(self):
        """Start the auto-download monitoring."""
        if self._running:
            raise RuntimeError("AutoDownloadManager is already running")

        self._running = True
        self.stats["start_time"] = datetime.now()
        self._stop_event.clear()

        self.logger.info(
            f"Starting auto-download for group '{self.group_id}' (interval: {self.interval_minutes} minutes)"
        )

        # Start the monitoring task
        self._task = asyncio.create_task(self._monitoring_loop())

        return self

    async def stop(self):
        """Stop the auto-download monitoring."""
        if not self._running:
            return

        self.logger.info("Stopping auto-download monitoring...")

        self._running = False
        self._stop_event.set()

        # Wait for the monitoring task to complete
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("Monitoring task did not stop gracefully, cancelling...")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

        self.logger.info("Auto-download monitoring stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop that runs continuously."""
        try:
            while self._running:
                try:
                    await self._check_and_download_files()
                    self.stats["total_checks"] += 1
                    self.stats["last_check_time"] = datetime.now()

                except Exception as e:
                    self.logger.error("Error in monitoring loop: %s", e)
                    self.stats["errors"].append(
                        {
                            "timestamp": datetime.now().isoformat(),
                            "error": str(e),
                            "type": "monitoring_loop",
                        }
                    )

                    # Call error callback if provided
                    if self.error_callback:
                        try:
                            await self._safe_call_callback(self.error_callback, e)
                        except Exception as cb_error:
                            self.logger.error("Error in error callback: %s", cb_error)

                # Wait for the specified interval or until stopped
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_minutes * 60)
                    # If we reach here, stop was requested
                    break
                except asyncio.TimeoutError:
                    # Timeout is normal, continue to next iteration
                    continue

        except asyncio.CancelledError:
            self.logger.info("Monitoring loop cancelled")
        except Exception as e:
            self.logger.error("Fatal error in monitoring loop: %s", e)
        finally:
            self._running = False

    async def _check_and_download_files(self):
        """Use available-files to find and download files if needed."""
        try:
            if not self._running:
                return

            # Determine date window to check
            dates_to_check = self._get_dates_to_check()
            start_date = min(dates_to_check)
            end_date = max(dates_to_check)

            # Query available files for the date window
            available_files = await self.client.list_available_files_async(
                group_id=self.group_id,
                file_group_id=None,
                start_date=start_date,
                end_date=end_date,
            )

            if not isinstance(available_files, list) or not available_files:
                self.logger.debug(f"No available files for group '{self.group_id}' between {start_date} and {end_date}")
                return

            self.logger.debug(f"Found {len(available_files)} available file entries for group '{self.group_id}'")

            # Build eligible downloads
            eligible: List[tuple[str, str, str]] = []  # (file_group_id, date_str, file_key)
            for item in available_files:
                if not self._running:
                    break
                file_group_id = item.get("file-group-id")
                date_str = item.get("file-datetime")
                avail_flag = item.get("is-available")
                is_available = bool(avail_flag)
                if not file_group_id or not date_str or not is_available:
                    continue
                if self.file_filter and not self.file_filter(item):
                    continue
                file_key = f"{file_group_id}_{date_str}"
                if file_key in self._downloaded_files:
                    continue
                if self._failed_files.get(file_key, 0) >= self.max_retries:
                    continue
                if self._file_exists_locally(file_group_id, date_str):
                    self.stats["files_skipped"] += 1
                    self._downloaded_files.add(file_key)
                    continue
                eligible.append((file_group_id, date_str, file_key))

            if not eligible:
                return

            # Parallel downloads with concurrency limit
            semaphore = asyncio.Semaphore(max(1, int(self.max_concurrent_downloads)))

            async def worker(fid: str, dstr: str, fkey: str):
                if not self._running:
                    return
                async with semaphore:
                    await self._download_file(fid, dstr, fkey)

            tasks = [asyncio.create_task(worker(fid, dstr, fkey)) for fid, dstr, fkey in eligible]
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            self.logger.error("Error checking available files: %s", e)
            raise

    def _get_dates_to_check(self) -> List[str]:
        """Get list of dates to check for files."""
        if self.check_current_date_only:
            # Only check current date
            return [datetime.now().strftime("%Y%m%d")]
        else:
            # Check current date and previous few days
            dates: List[str] = []
            today = datetime.now()
            for i in range(3):  # Check today and last 2 days
                date = today - timedelta(days=i)
                dates.append(date.strftime("%Y%m%d"))
            return dates

    def _file_exists_locally(self, file_group_id: str, date_str: str) -> bool:
        """Check if file already exists in the destination directory."""
        # Look for files that might match this file_group_id and date
        # This is a heuristic approach since we don't know the exact filename

        for file_path in self.destination_dir.glob("*"):
            if file_path.is_file():
                filename = file_path.name
                # Check if filename contains the file_group_id and date
                if file_group_id in filename and date_str in filename:
                    return True

        return False

    async def _download_file(self, file_group_id: str, date_str: str, file_key: str):
        """Download a file."""
        self.logger.info("Downloading '%s' for %s", file_group_id, date_str)

        # Create progress callback that updates statistics
        def progress_wrapper(progress: DownloadProgress):
            # Update statistics
            current_total = int(self.stats.get("total_bytes_downloaded", 0) or 0)
            self.stats["total_bytes_downloaded"] = max(
                current_total, int(getattr(progress, "bytes_downloaded", 0) or 0)
            )

            # Call user callback if provided
            if self.progress_callback:
                try:
                    if asyncio.iscoroutinefunction(self.progress_callback):
                        asyncio.create_task(self.progress_callback(progress))
                    else:
                        self.progress_callback(progress)
                except Exception as e:
                    self.logger.error("Error in progress callback: %s", e)

        try:
            result = await self.client.download_file_async(
                file_group_id=file_group_id,
                file_datetime=date_str,
                options=self.download_options,
                progress_callback=progress_wrapper,
            )

            succeeded = False
            already_exists = False
            if result is not None and getattr(result, "status", None) is not None:
                try:
                    from .models import (  # import inside to avoid cycles in some tools
                        DownloadStatus,
                    )

                    succeeded = result.status == DownloadStatus.COMPLETED
                    already_exists = result.status == DownloadStatus.ALREADY_EXISTS
                except Exception:
                    succeeded = False

            if already_exists:
                self.logger.info(
                    "File already exists for '%s' for %s, skipping",
                    file_group_id,
                    date_str,
                )
                self.stats["files_skipped"] += 1
                self._downloaded_files.add(file_key)
                if file_key in self._failed_files:
                    del self._failed_files[file_key]

            elif succeeded:
                self.logger.info("Successfully downloaded '%s' for %s", file_group_id, date_str)
                self.stats["files_downloaded"] += 1
                self._downloaded_files.add(file_key)
                # Accumulate total bytes if provided
                try:
                    if getattr(result, "file_size", None):
                        self.stats["total_bytes_downloaded"] += int(result.file_size) or 0
                except Exception:
                    pass

                # Reset failure count on success
                if file_key in self._failed_files:
                    del self._failed_files[file_key]

            else:
                error_msg = getattr(result, "error_message", "Unknown error") if result else "No result"
                self.logger.error(
                    "Download failed for '%s' for %s: %s",
                    file_group_id,
                    date_str,
                    error_msg,
                )
                self._failed_files[file_key] = self._failed_files.get(file_key, 0) + 1
                self.stats["download_failures"] += 1

        except Exception as e:
            self.logger.error(
                "Exception downloading '%s' for %s: %s",
                file_group_id,
                date_str,
                e,
            )
            self._failed_files[file_key] = self._failed_files.get(file_key, 0) + 1
            self.stats["download_failures"] += 1
            raise

    async def _safe_call_callback(self, callback: Callable, *args, **kwargs):
        """Safely call a callback function."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args, **kwargs)
            else:
                callback(*args, **kwargs)
        except Exception as e:
            self.logger.error("Error in callback: %s", e)

    def get_stats(self) -> Dict[str, Any]:
        """Get auto-download statistics."""
        runtime = None
        if self.stats["start_time"]:
            try:
                start = self.stats["start_time"]
                if isinstance(start, datetime):
                    runtime = (datetime.now() - start).total_seconds()
            except Exception:
                runtime = None

        return {
            **self.stats,
            "runtime_seconds": runtime,
            "is_running": self._running,
            "downloaded_files_count": len(self._downloaded_files),
            "failed_files_count": len(self._failed_files),
            "group_id": self.group_id,
            "destination_dir": str(self.destination_dir),
            "interval_minutes": self.interval_minutes,
            "check_current_date_only": self.check_current_date_only,
        }

    def get_downloaded_files(self) -> Set[str]:
        """Get set of successfully downloaded file keys."""
        return self._downloaded_files.copy()

    def get_failed_files(self) -> Dict[str, int]:
        """Get dictionary of failed file keys and their retry counts."""
        return self._failed_files.copy()

    @property
    def is_running(self) -> bool:
        """Check if the auto-download manager is running."""
        return self._running

    def __str__(self) -> str:
        """String representation of the manager."""
        status = "running" if self._running else "stopped"
        return f"AutoDownloadManager(group='{self.group_id}', status={status}, interval={self.interval_minutes}min)"

    def __repr__(self) -> str:
        """Detailed representation of the manager."""
        return (
            f"AutoDownloadManager(group_id='{self.group_id}', "
            f"destination_dir='{self.destination_dir}', "
            f"interval_minutes={self.interval_minutes}, "
            f"running={self._running})"
        )
