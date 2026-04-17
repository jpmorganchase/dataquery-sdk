"""
Shared download utilities for the SSE-driven NotificationDownloadManager.

Helpers for the per-event download path: progress wrappers and the
`download_and_track` coroutine that records stats and updates the success /
failure tracking sets after each download attempt.
"""

import asyncio
import inspect
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

from .models import DownloadOptions, DownloadProgress, DownloadStatus

logger = logging.getLogger(__name__)


def file_exists_locally(destination_dir: Path, file_group_id: str, date_str: str) -> bool:
    """Check if a file already exists in the destination directory (heuristic match)."""
    for file_path in destination_dir.glob("*"):
        if file_path.is_file() and file_group_id in file_path.name and date_str in file_path.name:
            return True
    return False


def create_progress_wrapper(
    stats: Dict[str, Any],
    user_callback: Optional[Callable] = None,
    logger_instance: Optional[logging.Logger] = None,
) -> Callable[[DownloadProgress], None]:
    """Create a progress callback that updates stats and optionally delegates to a user callback."""
    _logger = logger_instance or logger

    def wrapper(progress: DownloadProgress) -> None:
        current_total = int(stats.get("total_bytes_downloaded", 0) or 0)
        stats["total_bytes_downloaded"] = max(current_total, int(getattr(progress, "bytes_downloaded", 0) or 0))
        if user_callback:
            try:
                if inspect.iscoroutinefunction(user_callback):
                    asyncio.create_task(user_callback(progress))
                else:
                    user_callback(progress)
            except Exception as e:
                _logger.error("Error in progress callback: %s", e)

    return wrapper


async def download_and_track(
    client: Any,
    file_group_id: str,
    date_str: str,
    file_key: str,
    download_options: DownloadOptions,
    stats: Dict[str, Any],
    downloaded_files: Set[str],
    failed_files: Dict[str, int],
    progress_callback: Optional[Callable] = None,
    logger_instance: Optional[logging.Logger] = None,
) -> None:
    """Download a file, updating stats and tracking sets on success/failure."""
    _logger = logger_instance or logger
    _logger.info("Downloading '%s' for %s", file_group_id, date_str)

    wrapper = create_progress_wrapper(stats, progress_callback, _logger)

    try:
        result = await client.download_file_async(
            file_group_id=file_group_id,
            file_datetime=date_str,
            options=download_options,
            progress_callback=wrapper,
        )

        succeeded = False
        already_exists = False
        if result is not None and getattr(result, "status", None) is not None:
            succeeded = result.status == DownloadStatus.COMPLETED
            already_exists = result.status == DownloadStatus.ALREADY_EXISTS

        if already_exists:
            _logger.info("File already exists: '%s' for %s — skipping", file_group_id, date_str)
            stats["files_skipped"] += 1
            downloaded_files.add(file_key)
            failed_files.pop(file_key, None)

        elif succeeded:
            _logger.info("Downloaded '%s' for %s", file_group_id, date_str)
            stats["files_downloaded"] += 1
            downloaded_files.add(file_key)
            failed_files.pop(file_key, None)
            try:
                if getattr(result, "file_size", None):
                    stats["total_bytes_downloaded"] += int(result.file_size or 0)
            except Exception:
                pass

        else:
            error_msg = getattr(result, "error_message", "Unknown") if result else "No result"
            _logger.error("Download failed for '%s' for %s: %s", file_group_id, date_str, error_msg)
            failed_files[file_key] = failed_files.get(file_key, 0) + 1
            stats["download_failures"] += 1

    except Exception as exc:
        _logger.error("Exception downloading '%s' for %s: %s", file_group_id, date_str, exc)
        failed_files[file_key] = failed_files.get(file_key, 0) + 1
        stats["download_failures"] += 1
        raise
