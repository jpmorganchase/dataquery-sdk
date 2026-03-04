"""
Notification-driven download manager for the DataQuery SDK.

Subscribes to the DataQuery /notification SSE endpoint and, whenever a
notification arrives, fetches the list of available files for the configured
group and downloads any files that are not already present locally.
"""

import asyncio
import inspect
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .models import DownloadOptions, DownloadProgress, DownloadStatus
from .sse_client import SSEClient, SSEEvent

logger = logging.getLogger(__name__)


class NotificationDownloadManager:
    """
    Downloads files in response to SSE notifications from the DataQuery API.

    When a notification is received on the /notification endpoint the manager:

    1. Calls ``list_available_files_async`` for the configured group.
    2. Skips files that are already present in the destination directory.
    3. Downloads all remaining files concurrently (up to *max_concurrent*).

    The manager also performs an initial availability check on start so that
    any files that became available before the SSE connection was established
    are not missed.

    Usage::

        async with DataQueryClient(config) as client:
            manager = NotificationDownloadManager(
                client=client,
                group_id="my-group",
                destination_dir="./downloads",
            )
            await manager.start()
            # keep the process alive ...
            await manager.stop()
    """

    def __init__(
        self,
        client,  # DataQueryClient
        group_id: str,
        destination_dir: str = "./downloads",
        file_filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
        max_retries: int = 3,
        max_concurrent_downloads: int = 5,
        initial_check: bool = True,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
    ):
        """
        Initialise the manager.

        Args:
            client: An initialised ``DataQueryClient`` instance.
            group_id: The data group to watch.
            destination_dir: Directory where files will be saved.
            file_filter: Optional predicate called with each available-file
                         dict; return ``False`` to skip that file.
            progress_callback: Called with a ``DownloadProgress`` object during
                               downloads.  May be sync or async.
            error_callback: Called with an ``Exception`` when a connection or
                            download error occurs.  May be sync or async.
            max_retries: Maximum download retry attempts per file.
            max_concurrent_downloads: Concurrency limit for parallel downloads.
            initial_check: If ``True`` (default) perform a file-availability
                           check immediately on start before SSE events arrive.
            reconnect_delay: Initial SSE reconnection delay in seconds.
            max_reconnect_delay: Maximum SSE reconnection delay in seconds.
        """
        self.client = client
        self.group_id = group_id
        self.destination_dir = Path(destination_dir)
        self.file_filter = file_filter
        self.progress_callback = progress_callback
        self.error_callback = error_callback
        self.max_retries = max_retries
        self.max_concurrent_downloads = max_concurrent_downloads
        self.initial_check = initial_check

        self.destination_dir.mkdir(parents=True, exist_ok=True)

        # SSE client (created in start())
        self._sse_client: Optional[SSEClient] = None
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        # State
        self._running = False
        self._downloaded_files: Set[str] = set()
        self._failed_files: Dict[str, int] = {}  # file_key -> retry_count
        self._download_lock = asyncio.Lock()

        # Statistics
        self.stats: Dict[str, Any] = {
            "start_time": None,
            "notifications_received": 0,
            "checks_triggered": 0,
            "files_discovered": 0,
            "files_downloaded": 0,
            "files_skipped": 0,
            "download_failures": 0,
            "total_bytes_downloaded": 0,
            "errors": [],
        }

        self._download_options = DownloadOptions(
            destination_path=self.destination_dir,
            overwrite_existing=False,
            chunk_size=8192,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> "NotificationDownloadManager":
        """Start the SSE subscription and (optionally) an initial check."""
        if self._running:
            raise RuntimeError("NotificationDownloadManager is already running")

        self._running = True
        self.stats["start_time"] = datetime.now()

        logger.info(
            "Starting NotificationDownloadManager for group '%s' -> %s",
            self.group_id,
            self.destination_dir,
        )

        # Perform initial availability check before SSE events arrive.
        if self.initial_check:
            try:
                await self._check_and_download()
            except Exception as exc:
                logger.warning("Initial availability check failed: %s", exc)

        # Start the SSE client.
        self._sse_client = SSEClient(
            config=self.client.config,
            auth_manager=self.client.auth_manager,
            on_event=self._on_sse_event,
            on_error=self._on_sse_error,
            reconnect_delay=self._reconnect_delay,
            max_reconnect_delay=self._max_reconnect_delay,
        )
        await self._sse_client.start()

        return self

    async def stop(self) -> None:
        """Stop the SSE subscription and wait for in-flight downloads."""
        if not self._running:
            return
        self._running = False
        if self._sse_client:
            await self._sse_client.stop()
            self._sse_client = None
        logger.info("NotificationDownloadManager stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of runtime statistics."""
        runtime = None
        start = self.stats.get("start_time")
        if isinstance(start, datetime):
            runtime = (datetime.now() - start).total_seconds()
        return {
            **self.stats,
            "runtime_seconds": runtime,
            "is_running": self._running,
            "group_id": self.group_id,
            "destination_dir": str(self.destination_dir),
            "downloaded_file_keys": len(self._downloaded_files),
            "failed_file_keys": len(self._failed_files),
        }

    # ------------------------------------------------------------------
    # SSE callbacks
    # ------------------------------------------------------------------

    async def _on_sse_event(self, event: SSEEvent) -> None:
        """Called by SSEClient for every received event."""
        self.stats["notifications_received"] += 1
        logger.info(
            "SSE notification received (event=%s id=%s): %s",
            event.event,
            event.id,
            event.data[:200] if event.data else "",
        )
        if not self._running:
            return
        try:
            await self._check_and_download()
        except Exception as exc:
            logger.error("Error handling notification: %s", exc)
            await self._dispatch_error(exc)

    async def _on_sse_error(self, exc: Exception) -> None:
        """Called by SSEClient on connection errors."""
        logger.warning("SSE connection error: %s", exc)
        await self._dispatch_error(exc)

    # ------------------------------------------------------------------
    # Download logic (shared with the initial check)
    # ------------------------------------------------------------------

    async def _check_and_download(self) -> None:
        """Fetch available files and download any that are new."""
        self.stats["checks_triggered"] += 1
        today = datetime.now().strftime("%Y%m%d")

        available = await self.client.list_available_files_async(
            group_id=self.group_id,
            start_date=today,
            end_date=today,
        )

        if not available:
            logger.debug("No available files for group '%s' on %s", self.group_id, today)
            return

        logger.debug("Available files for '%s': %d entries", self.group_id, len(available))

        eligible: List[tuple] = []
        for item in available:
            file_group_id: Optional[str] = item.get("file-group-id")
            date_str: Optional[str] = item.get("file-datetime")
            if not file_group_id or not date_str:
                continue
            if not item.get("is-available"):
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

        self.stats["files_discovered"] += len(eligible)

        if not eligible:
            return

        semaphore = asyncio.Semaphore(max(1, self.max_concurrent_downloads))

        async def worker(fid: str, dstr: str, fkey: str) -> None:
            async with semaphore:
                await self._download_file(fid, dstr, fkey)

        await asyncio.gather(
            *(asyncio.create_task(worker(fid, dstr, fkey)) for fid, dstr, fkey in eligible),
            return_exceptions=True,
        )

    def _file_exists_locally(self, file_group_id: str, date_str: str) -> bool:
        """Heuristic check: does a local file contain both the id and date?"""
        for p in self.destination_dir.glob("*"):
            if p.is_file() and file_group_id in p.name and date_str in p.name:
                return True
        return False

    async def _download_file(self, file_group_id: str, date_str: str, file_key: str) -> None:
        logger.info("Downloading '%s' for %s", file_group_id, date_str)

        def progress_wrapper(progress: DownloadProgress) -> None:
            self.stats["total_bytes_downloaded"] = max(
                int(self.stats.get("total_bytes_downloaded", 0) or 0),
                int(getattr(progress, "bytes_downloaded", 0) or 0),
            )
            if self.progress_callback:
                try:
                    if inspect.iscoroutinefunction(self.progress_callback):
                        asyncio.create_task(self.progress_callback(progress))
                    else:
                        self.progress_callback(progress)
                except Exception as exc:
                    logger.error("Error in progress callback: %s", exc)

        try:
            result = await self.client.download_file_async(
                file_group_id=file_group_id,
                file_datetime=date_str,
                options=self._download_options,
                progress_callback=progress_wrapper,
            )

            succeeded = False
            already_exists = False
            if result is not None and getattr(result, "status", None) is not None:
                succeeded = result.status == DownloadStatus.COMPLETED
                already_exists = result.status == DownloadStatus.ALREADY_EXISTS

            if already_exists:
                logger.info("File already exists: '%s' for %s — skipping", file_group_id, date_str)
                self.stats["files_skipped"] += 1
                self._downloaded_files.add(file_key)
                self._failed_files.pop(file_key, None)
            elif succeeded:
                logger.info("Downloaded '%s' for %s", file_group_id, date_str)
                self.stats["files_downloaded"] += 1
                self._downloaded_files.add(file_key)
                self._failed_files.pop(file_key, None)
                try:
                    if getattr(result, "file_size", None):
                        self.stats["total_bytes_downloaded"] += int(result.file_size or 0)
                except Exception:
                    pass
            else:
                error_msg = getattr(result, "error_message", "Unknown") if result else "No result"
                logger.error("Download failed for '%s' for %s: %s", file_group_id, date_str, error_msg)
                self._failed_files[file_key] = self._failed_files.get(file_key, 0) + 1
                self.stats["download_failures"] += 1

        except Exception as exc:
            logger.error("Exception downloading '%s' for %s: %s", file_group_id, date_str, exc)
            self._failed_files[file_key] = self._failed_files.get(file_key, 0) + 1
            self.stats["download_failures"] += 1
            raise

    async def _dispatch_error(self, exc: Exception) -> None:
        self.stats["errors"].append({"timestamp": datetime.now().isoformat(), "error": str(exc)})
        if not self.error_callback:
            return
        try:
            if inspect.iscoroutinefunction(self.error_callback):
                await self.error_callback(exc)
            else:
                self.error_callback(exc)
        except Exception as cb_exc:
            logger.error("Error in error_callback: %s", cb_exc)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        status = "running" if self._running else "stopped"
        return f"NotificationDownloadManager(group='{self.group_id}', status={status})"

    def __repr__(self) -> str:
        return (
            f"NotificationDownloadManager(group_id={self.group_id!r}, "
            f"destination_dir={str(self.destination_dir)!r}, "
            f"running={self._running})"
        )
