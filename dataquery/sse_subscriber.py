"""
Notification-driven download manager for the DataQuery SDK.

Subscribes to the DataQuery /sse/event/notification SSE endpoint and
downloads files as notifications arrive. Each SSE event carries the
``fileGroupId`` and ``fileDateTime`` directly, so the manager calls the
file-availability endpoint for that specific file and downloads it if
``is-available`` is true — no need to list all available files.

On startup an initial bulk availability check is performed so that files
published before the SSE connection was established are not missed.
"""

import asyncio
import inspect
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from ._download_utils import download_and_track, file_exists_locally
from .models import DownloadOptions, DownloadProgress
from .sse_client import SSEClient, SSEEvent

logger = logging.getLogger(__name__)


class NotificationDownloadManager:
    """
    Downloads files in response to SSE notifications from the DataQuery API.

    Each SSE event from ``/sse/event/notification`` carries::

        { "eventId": "...",
          "data": { "fileGroupId": "...", "fileDateTime": "..." },
          "timestamp": "..." }

    When a notification arrives the manager:

    1. Parses ``fileGroupId`` and ``fileDateTime`` from the event payload.
    2. Calls ``check_availability_async`` for that specific file.
    3. If ``is-available`` is true and the file is not already local,
       downloads it directly.

    On startup an initial bulk availability check is performed so that
    files published before the SSE connection was established are not
    missed.

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
        """Parse the SSE event payload and download the referenced file.

        Expected event structure::

            { "eventId": "...",
              "data": { "fileGroupId": "...", "fileDateTime": "..." },
              "timestamp": "..." }
        """
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
            await self._handle_notification(event)
        except Exception as exc:
            logger.error("Error handling notification: %s", exc)
            await self._dispatch_error(exc)

    async def _on_sse_error(self, exc: Exception) -> None:
        """Called by SSEClient on connection errors."""
        logger.warning("SSE connection error: %s", exc)
        await self._dispatch_error(exc)

    # ------------------------------------------------------------------
    # Event-driven download (single file per notification)
    # ------------------------------------------------------------------

    async def _handle_notification(self, event: SSEEvent) -> None:
        """Extract fileGroupId/fileDateTime from the event, check availability, download."""
        if not event.data:
            logger.debug("SSE event has no data payload — skipping")
            return

        # Parse the JSON payload
        try:
            payload = json.loads(event.data)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Could not parse SSE event data as JSON: %s", exc)
            return

        # The file info lives under the "data" key of the payload
        data = payload.get("data") or payload  # fall back to top-level if "data" is absent
        file_group_id: Optional[str] = data.get("fileGroupId")
        file_date_time: Optional[str] = data.get("fileDateTime")

        if not file_group_id or not file_date_time:
            logger.warning(
                "SSE event missing fileGroupId or fileDateTime: %s",
                event.data[:200],
            )
            return

        file_key = f"{file_group_id}_{file_date_time}"
        self.stats["checks_triggered"] += 1

        # Already handled?
        if file_key in self._downloaded_files:
            logger.debug("Already downloaded %s — skipping", file_key)
            return
        if self._failed_files.get(file_key, 0) >= self.max_retries:
            logger.debug("Max retries reached for %s — skipping", file_key)
            return
        if self._file_exists_locally(file_group_id, file_date_time):
            logger.debug("File already exists locally for %s — skipping", file_key)
            self.stats["files_skipped"] += 1
            self._downloaded_files.add(file_key)
            return

        # Optional user filter
        if self.file_filter and not self.file_filter({"file-group-id": file_group_id, "file-datetime": file_date_time}):
            logger.debug("File %s filtered out by user predicate", file_key)
            return

        # Check availability via the API
        try:
            availability = await self.client.check_availability_async(file_group_id, file_date_time)
        except Exception as exc:
            logger.error("Availability check failed for %s: %s", file_key, exc)
            await self._dispatch_error(exc)
            return

        is_available = getattr(availability, "is_available", False)
        if not is_available:
            logger.debug("File %s is not available yet", file_key)
            return

        self.stats["files_discovered"] += 1

        # Download
        await self._download_file(file_group_id, file_date_time, file_key)

    # ------------------------------------------------------------------
    # Initial bulk check (startup only)
    # ------------------------------------------------------------------

    async def _check_and_download(self) -> None:
        """Bulk-fetch available files for today and download any that are new.

        Used only for the initial startup check so files published before
        the SSE connection was established are not missed.
        """
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
            fid: Optional[str] = item.get("file-group-id")
            dstr: Optional[str] = item.get("file-datetime")
            if not fid or not dstr:
                continue
            if not item.get("is-available"):
                continue
            if self.file_filter and not self.file_filter(item):
                continue
            file_key = f"{fid}_{dstr}"
            if file_key in self._downloaded_files:
                continue
            if self._failed_files.get(file_key, 0) >= self.max_retries:
                continue
            if self._file_exists_locally(fid, dstr):
                self.stats["files_skipped"] += 1
                self._downloaded_files.add(file_key)
                continue
            eligible.append((fid, dstr, file_key))

        self.stats["files_discovered"] += len(eligible)

        if not eligible:
            return

        semaphore = asyncio.Semaphore(max(1, self.max_concurrent_downloads))

        async def worker(fid: str, dstr: str, fkey: str) -> None:
            async with semaphore:
                await self._download_file(fid, dstr, fkey)

        await asyncio.gather(
            *(asyncio.create_task(worker(f, d, k)) for f, d, k in eligible),
            return_exceptions=True,
        )

    def _file_exists_locally(self, file_group_id: str, date_str: str) -> bool:
        """Heuristic check: does a local file contain both the id and date?"""
        return file_exists_locally(self.destination_dir, file_group_id, date_str)

    async def _download_file(self, file_group_id: str, date_str: str, file_key: str) -> None:
        await download_and_track(
            client=self.client,
            file_group_id=file_group_id,
            date_str=date_str,
            file_key=file_key,
            download_options=self._download_options,
            stats=self.stats,
            downloaded_files=self._downloaded_files,
            failed_files=self._failed_files,
            progress_callback=self.progress_callback,
            logger_instance=logger,
        )

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
