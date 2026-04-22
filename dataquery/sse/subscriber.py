"""
Notification-driven download manager for the DataQuery SDK.

Subscribes to the DataQuery /sse/event/notification SSE endpoint and
downloads files as notifications arrive. Each SSE event carries
``file-group-id``, ``file-datetime``, and ``file-action`` in the JSON
payload. Only events with ``file-action == "CREATED-UPDATED"`` trigger a
download.

The ``event-id`` field from the JSON payload is persisted to disk so that
cross-process replay via the ``last-event-id`` query parameter works
correctly on reconnection.
"""

import asyncio
import inspect
import json
import logging
from collections import OrderedDict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from ..download.utils import download_and_track, file_exists_locally
from ..types.models import DownloadOptions, DownloadProgress
from .client import SSEClient, SSEEvent
from .event_store import SSEEventIdStore, Subscription, build_event_id_store

logger = logging.getLogger(__name__)


class _BoundedKeySet:
    """Set-like LRU container used to remember already-downloaded file keys.

    Bounded so the manager can run indefinitely (24/7) without unbounded
    memory growth. Touching a key on access (``__contains__``) keeps the
    "hot" keys alive so a duplicate-event burst won't re-trigger downloads.
    """

    __slots__ = ("_data", "_maxsize")

    def __init__(self, maxsize: int) -> None:
        self._maxsize = max(1, int(maxsize))
        self._data: "OrderedDict[str, None]" = OrderedDict()

    def add(self, key: str) -> None:
        if key in self._data:
            self._data.move_to_end(key)
            return
        self._data[key] = None
        if len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def __contains__(self, key: object) -> bool:
        if key in self._data:
            self._data.move_to_end(key)  # type: ignore[arg-type]
            return True
        return False

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)


class _BoundedRetryMap:
    """Dict-like LRU container used to remember per-file retry counts.

    Mirrors :class:`_BoundedKeySet` but stores integer values instead of a
    set membership marker. ``__setitem__`` touches the key (LRU on write).
    """

    __slots__ = ("_data", "_maxsize")

    def __init__(self, maxsize: int) -> None:
        self._maxsize = max(1, int(maxsize))
        self._data: "OrderedDict[str, int]" = OrderedDict()

    def get(self, key: str, default: int = 0) -> int:
        return self._data.get(key, default)

    def pop(self, key: str, default: Any = None) -> Any:
        return self._data.pop(key, default)

    def __setitem__(self, key: str, value: int) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def __getitem__(self, key: str) -> int:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)


class NotificationDownloadManager:
    """
    Downloads files in response to SSE notifications from the DataQuery API.

    Each SSE event from ``/sse/event/notification`` carries::

        { "event-id": "unique-event-identifier",
          "data": { "file-group-id": "...", "file-datetime": "...", "file-action": "..." },
          "timestamp": "..." }

    When a notification arrives the manager:

    1. Parses ``event-id`` for replay, and ``file-group-id``, ``file-datetime``, ``file-action`` from the event payload.
    2. Only proceeds if ``file-action`` equals ``"CREATED-UPDATED"``.
    3. Checks if the file is not already local.
    4. Downloads the file.

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
        initial_check: bool = False,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        file_group_id: Optional[Union[str, List[str]]] = None,
        show_progress: bool = True,
        enable_event_replay: bool = True,
        heartbeat_timeout: float = 0.0,
        max_tracked_files: int = 10_000,
        max_tracked_errors: int = 1_000,
    ):
        """
        Initialise the manager.

        Args:
            client: An initialised ``DataQueryClient`` instance.
            group_id: The data group to watch. Sent to the server as the
                      ``group-id`` query parameter on the SSE subscription so
                      only notifications for this group are delivered.
            destination_dir: Directory where files will be saved.
            file_filter: Optional predicate called with each available-file
                         dict; return ``False`` to skip that file.
            progress_callback: Called with a ``DownloadProgress`` object during
                               downloads.  May be sync or async.
            error_callback: Called with an ``Exception`` when a connection or
                            download error occurs.  May be sync or async.
            max_retries: Maximum download retry attempts per file.
            max_concurrent_downloads: Concurrency limit for parallel downloads.
            initial_check: If ``True`` perform a file-availability check
                           immediately on start before SSE events arrive.
            reconnect_delay: Initial SSE reconnection delay in seconds.
            max_reconnect_delay: Maximum SSE reconnection delay in seconds.
            file_group_id: Optional restriction to one or more file-group-ids.
                           Sent to the server as the ``file-group-id`` query
                           parameter (comma-separated when a list) so filtering
                           happens at the source — the client no longer receives
                           events for other files.
            show_progress: If ``True`` (default), log download progress at
                           DEBUG level when no ``progress_callback`` is set.
            enable_event_replay: If ``True`` (default), persist the last seen
                           SSE event id to disk and, on subsequent runs, send
                           it as the ``last-event-id`` query parameter so the
                           server replays any events published while the
                           process was down. When a stored id is found the
                           initial bulk availability check is skipped because
                           replay covers that gap precisely. Set ``False`` to
                           restore the legacy bulk-check-every-startup
                           behaviour.
            heartbeat_timeout: When > 0, force-reconnect the SSE stream if no
                           bytes (events or comment heartbeats) arrive within
                           this many seconds. Detects half-open TCP /
                           stalled-proxy hangs that the server's clean-close
                           recycle wouldn't otherwise surface. Must be larger
                           than the server's keep-alive interval. ``0`` (the
                           default) disables the watchdog.
            max_tracked_files: Maximum number of file keys to remember in the
                           in-memory dedup / retry maps. Bounded so the manager
                           can run indefinitely without unbounded memory growth.
                           Eviction is LRU — keys still seeing traffic stay hot.
                           Default 10,000 ≈ a few MB, sufficient for years of
                           realistic SSE volume.
            max_tracked_errors: Maximum number of recent errors retained in
                           ``stats["errors"]``. Implemented as a ring buffer.
                           Default 1,000.
        """
        self.client = client
        self.subscription = Subscription.from_user(group_id, file_group_id)
        # Preserve the original kwargs as attributes for backward-compat with
        # callers that read them off the manager (CLI, tests, get_stats).
        self.group_id = group_id
        self.file_group_id = file_group_id
        self.destination_dir = Path(destination_dir)
        self.file_filter = file_filter
        self.progress_callback = progress_callback
        self.error_callback = error_callback
        self.max_retries = max_retries
        self.max_concurrent_downloads = max_concurrent_downloads
        self.initial_check = initial_check
        self.enable_event_replay = enable_event_replay

        self.destination_dir.mkdir(parents=True, exist_ok=True)

        # SSE client (created in start())
        self._sse_client: Optional[SSEClient] = None
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._heartbeat_timeout = heartbeat_timeout
        self._event_id_store: Optional[SSEEventIdStore] = None

        # State — bounded so the manager can run 24/7 without leaking.
        self._running = False
        self._downloaded_files: _BoundedKeySet = _BoundedKeySet(max_tracked_files)
        self._failed_files: _BoundedRetryMap = _BoundedRetryMap(max_tracked_files)
        # Lazy-init: ``asyncio.Lock()`` on Python 3.9 eagerly binds to the
        # running loop, which breaks construction from sync code. Mirrors
        # the same pattern used in :class:`SSEEventIdStore`.
        self._download_lock: Optional[asyncio.Lock] = None

        # Statistics — ``errors`` is a ring buffer for the same reason.
        self.stats: Dict[str, Any] = {
            "start_time": None,
            "notifications_received": 0,
            "checks_triggered": 0,
            "files_discovered": 0,
            "files_downloaded": 0,
            "files_skipped": 0,
            "download_failures": 0,
            "total_bytes_downloaded": 0,
            "errors": deque(maxlen=max(1, int(max_tracked_errors))),
        }

        self._download_options = DownloadOptions(
            destination_path=self.destination_dir,
            overwrite_existing=False,
            chunk_size=8192,
            show_progress=show_progress,
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

        # Resolve the persistent event-id store and decide whether to replay.
        # When a stored event id exists, the SSE server replays missed events
        # so the bulk initial-check is unnecessary (and would re-check files
        # that the replay is about to deliver).
        stored_event_id: Optional[str] = None
        if self.enable_event_replay:
            self._event_id_store = build_event_id_store(self.client.config, self.subscription)
            if self._event_id_store is not None:
                stored_event_id = self._event_id_store.load()

        if stored_event_id is not None:
            logger.info(
                "Event replay enabled — resuming from event-id %s; skipping initial bulk check.",
                stored_event_id,
            )
        elif self.initial_check:
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
            params=self.subscription.query_params(),
            event_id_store=self._event_id_store,
            heartbeat_timeout=self._heartbeat_timeout,
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
        last_event_id = None
        if self._sse_client is not None:
            last_event_id = self._sse_client._last_event_id
        snapshot = {
            **self.stats,
            "runtime_seconds": runtime,
            "is_running": self._running,
            "group_id": self.group_id,
            "destination_dir": str(self.destination_dir),
            "downloaded_file_keys": len(self._downloaded_files),
            "failed_file_keys": len(self._failed_files),
            "last_event_id": last_event_id,
        }
        # Convert the bounded ring buffer to a plain list so callers that
        # JSON-serialise the snapshot (CLI --watch, get_stats consumers) do
        # not need to know about the underlying deque.
        snapshot["errors"] = list(self.stats["errors"])
        return snapshot

    def clear_event_id(self) -> None:
        """Delete the persisted SSE event id so the next start() replays from
        scratch (or runs the legacy initial bulk check, depending on flags)."""
        if self._event_id_store is not None:
            self._event_id_store.clear()

    # ------------------------------------------------------------------
    # SSE callbacks
    # ------------------------------------------------------------------

    async def _on_sse_event(self, event: SSEEvent) -> None:
        """Parse the SSE event payload and download the referenced file.

        Expected event structure::

            { "event-id": "unique-event-identifier",
              "data": { "file-group-id": "...", "file-datetime": "...", "file-action": "CREATED-UPDATED" },
              "timestamp": "..." }
        
        Note: Only files with file-action="CREATED-UPDATED" will be downloaded.
        The event-id field is persisted for cross-process event replay.
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
        """Extract file-group-id/file-datetime/file-action from the event, check availability, download."""
        if not event.data:
            logger.debug("SSE event has no data payload — skipping")
            return

        # Parse the JSON payload
        try:
            payload = json.loads(event.data)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Could not parse SSE event data as JSON: %s", exc)
            return

        # The file info lives under the "data" key of the payload, but some
        # event variants nest it directly. Fall back only when "data" is
        # missing or not a dict — `or` would also fall back on `{}`/`None`,
        # which is wrong (an empty dict is a real, well-formed value).
        nested = payload.get("data") if isinstance(payload, dict) else None
        data = nested if isinstance(nested, dict) else (payload if isinstance(payload, dict) else {})
        
        # Extract event-id from JSON payload for replay support
        event_id_raw = payload.get("event-id") if isinstance(payload, dict) else None
        event_id: Optional[str] = str(event_id_raw) if event_id_raw is not None else None
        if event_id:
            # Update the SSE client's last event ID so it's sent on reconnection
            if self._sse_client is not None:
                self._sse_client._last_event_id = event_id
            # Persist the event ID for cross-process replay
            if self._event_id_store is not None:
                asyncio.create_task(self._event_id_store.save(event_id))
            logger.debug("Captured event-id: %s", event_id)
        
        file_group_id: Optional[str] = data.get("file-group-id")
        file_date_time: Optional[str] = data.get("file-datetime")
        file_action: Optional[str] = data.get("file-action")

        if not file_group_id or not file_date_time:
            logger.warning(
                "SSE event missing file-group-id or file-datetime: %s",
                event.data[:200],
            )
            return

        file_key = f"{file_group_id}_{file_date_time}"
        self.stats["checks_triggered"] += 1

        logger.debug(
            "Processing notification: file-group-id=%s, file-datetime=%s, file-action=%s",
            file_group_id,
            file_date_time,
            file_action or "(none)",
        )

        # Only download files with CREATED-UPDATED action
        if file_action != "CREATED-UPDATED":
            logger.debug(
                "Skipping file %s — file-action '%s' is not 'CREATED-UPDATED'",
                file_key,
                file_action or "(none)",
            )
            return

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
        filter_data = {
            "file-group-id": file_group_id,
            "file-datetime": file_date_time,
        }
        if file_action is not None:
            filter_data["file-action"] = file_action
        if self.file_filter and not self.file_filter(filter_data):
            logger.debug("File %s filtered out by user predicate", file_key)
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
