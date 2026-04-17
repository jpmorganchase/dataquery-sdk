"""
Server-Sent Events (SSE) client for the DataQuery notification endpoint.

Connects to the /notification SSE endpoint and dispatches events to registered
handlers. Automatically reconnects on connection loss using exponential backoff.
"""

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Union

import aiohttp

from .auth import OAuthManager
from .models import ClientConfig
from .sse_event_store import SSEEventIdStore

logger = logging.getLogger(__name__)

# A connection that stays open for at least this many seconds is considered
# "healthy" — the next disconnect resets the exponential backoff so that the
# expected periodic server-side recycle (e.g. a 5-minute idle timeout) doesn't
# inflate the reconnect delay over time.
_HEALTHY_CONNECTION_SECONDS = 30.0


@dataclass
class SSEEvent:
    """A parsed Server-Sent Event."""

    event: str = "message"
    data: str = ""
    id: Optional[str] = None
    retry: Optional[int] = None


class SSEClient:
    """
    Server-Sent Events client for the DataQuery /notification endpoint.

    Maintains a persistent streaming connection and dispatches SSE events to
    registered callbacks. Reconnects automatically with exponential backoff when
    the connection is lost.

    Usage::

        sse = SSEClient(
            config=config,
            auth_manager=auth_manager,
            on_event=lambda event: print(event),
        )
        await sse.start()
        # ... later ...
        await sse.stop()
    """

    def __init__(
        self,
        config: ClientConfig,
        auth_manager: OAuthManager,
        on_event: Optional[Callable[[SSEEvent], Union[None, Awaitable[None]]]] = None,
        on_error: Optional[Callable[[Exception], Union[None, Awaitable[None]]]] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        sse_timeout: float = 0,  # 0 = no timeout on the streaming read
        params: Optional[dict] = None,
        event_id_store: Optional[SSEEventIdStore] = None,
        heartbeat_timeout: float = 0.0,
    ):
        """
        Initialise the SSE client.

        Args:
            config: DataQuery client configuration.
            auth_manager: OAuthManager used to obtain Bearer tokens.
            on_event: Callback invoked for every received SSEEvent.
                      May be a regular function or a coroutine function.
            on_error: Callback invoked when a (re)connection error occurs.
                      May be a regular function or a coroutine function.
            reconnect_delay: Initial delay in seconds before the first
                             reconnection attempt.
            max_reconnect_delay: Maximum delay between reconnection attempts.
            sse_timeout: Total seconds to wait while reading from the stream
                         before treating the connection as stale (0 = unlimited).
            params: Optional query string parameters appended to the notification
                    URL. Used to subscribe to a filtered notification stream —
                    e.g. ``{"group-id": "G", "file-group-id": "FG"}`` tells the
                    server to only emit events for that group/file-group so no
                    client-side filtering is needed.
            event_id_store: Optional persistent store for the last seen event
                    id. When provided, the stored id seeds ``_last_event_id``
                    on construction (so the very first connection includes it
                    as ``last-event-id`` query param + ``Last-Event-ID`` header
                    for cross-process replay) and every received event id is
                    written back to the store.
            heartbeat_timeout: When > 0, force-reconnect if no bytes (events
                    OR comment heartbeats) are received from the server within
                    this many seconds. Protects against silent half-open TCP
                    connections that wouldn't otherwise be detected. Must be
                    larger than the server's keep-alive interval. ``0`` (the
                    default) disables the watchdog and relies on the server
                    closing the stream cleanly.
        """
        self.config = config
        self.auth_manager = auth_manager
        self.on_event = on_event
        self.on_error = on_error
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.sse_timeout = sse_timeout
        self.params = dict(params) if params else None
        self.event_id_store = event_id_store
        self.heartbeat_timeout = heartbeat_timeout

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_event_id: Optional[str] = None
        if event_id_store is not None:
            stored = event_id_store.load()
            if stored:
                self._last_event_id = stored
                logger.info("Seeded SSE last-event-id from store: %s", stored)
        self._save_tasks: set[asyncio.Task] = set()
        self._last_connection_duration: float = 0.0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> "SSEClient":
        """Start the SSE connection loop in a background task."""
        if self._running:
            raise RuntimeError("SSEClient is already running")
        self._running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="sse-client")
        return self

    async def stop(self) -> None:
        """Signal the connection loop to stop and wait for it to finish."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        if self._save_tasks:
            await asyncio.gather(*self._save_tasks, return_exceptions=True)
        logger.info("SSEClient stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_notification_url(self) -> str:
        base = self.config.api_base_url.rstrip("/")
        return f"{base}/sse/event/notification"

    async def _get_headers(self) -> dict:
        headers = await self.auth_manager.get_headers()
        headers["Accept"] = "text/event-stream"
        headers["Cache-Control"] = "no-cache"
        if self._last_event_id is not None:
            headers["Last-Event-ID"] = self._last_event_id
        return headers

    async def _run_loop(self) -> None:
        """Outer loop: reconnect with exponential backoff on failure.

        The backoff is reset whenever the preceding connection lasted long
        enough to count as "healthy" (see ``_HEALTHY_CONNECTION_SECONDS``) so
        an expected periodic server recycle — e.g. a 5-minute idle timeout —
        doesn't grow the reconnect delay across cycles.
        """
        delay = self.reconnect_delay
        while self._running:
            self._last_connection_duration = 0.0
            try:
                await self._connect_and_listen()
                # If _connect_and_listen returned cleanly (stop requested),
                # exit the loop.
                if not self._running:
                    break
                # Server closed the connection — reconnect after a short pause.
                logger.info(
                    "SSE connection closed by server after %.1fs; reconnecting in %.1fs",
                    self._last_connection_duration,
                    delay,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "SSE connection error after %.1fs: %s; reconnecting in %.1fs",
                    self._last_connection_duration,
                    exc,
                    delay,
                )
                await self._dispatch_error(exc)

            # Wait for `delay` seconds or until stop() is called.
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                break  # stop() was called during the wait
            except asyncio.TimeoutError:
                pass

            if self._last_connection_duration >= _HEALTHY_CONNECTION_SECONDS:
                # The previous connection lasted long enough to be considered
                # successful, so the next hiccup should start from a fresh
                # short delay rather than inheriting the last backoff value.
                delay = self.reconnect_delay
            else:
                # Exponential backoff, capped at max_reconnect_delay.
                delay = min(delay * 2, self.max_reconnect_delay)

        self._running = False

    def _build_request_params(self) -> Optional[dict]:
        """Combine the static subscription params with the current
        ``last-event-id`` (when known) so server-side replay can resume from
        the last persisted event on every (re)connection.
        """
        effective: dict = dict(self.params) if self.params else {}
        if self._last_event_id is not None:
            effective["last-event-id"] = self._last_event_id
        return effective or None

    async def _connect_and_listen(self) -> float:
        """Open a single SSE connection and read events until disconnected.

        Returns the number of seconds the connection stayed open (used by
        ``_run_loop`` to decide whether to reset the backoff).
        """
        url = self._build_notification_url()
        headers = await self._get_headers()
        request_params = self._build_request_params()

        # Use a fresh session per connection to avoid header/state bleed.
        timeout = aiohttp.ClientTimeout(total=None, connect=30.0, sock_read=self.sse_timeout or None)

        proxy_kwargs = self.config.get_proxy_kwargs()

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, params=request_params, **proxy_kwargs) as response:
                if response.status != 200:
                    raise ConnectionError(f"SSE endpoint returned HTTP {response.status} for {url}")
                logger.info("SSE connection established to %s (params=%s)", url, request_params)
                started_at = time.monotonic()
                # Any exception from _parse_sse_stream propagates with the
                # connection duration recorded on `self` so callers can read
                # it after the exception bubbles up.
                self._last_connection_duration = 0.0
                try:
                    await self._parse_sse_stream(response)
                finally:
                    self._last_connection_duration = time.monotonic() - started_at
                return self._last_connection_duration

    async def _parse_sse_stream(self, response: aiohttp.ClientResponse) -> None:
        """
        Read and parse an SSE stream line-by-line, dispatching events.

        SSE wire format (per spec)::

            event: <type>\\n
            data: <payload>\\n
            id: <id>\\n
            retry: <ms>\\n
            \\n          ← blank line dispatches the buffered event

        When ``self.heartbeat_timeout > 0`` the loop enforces that at least one
        byte arrives within the timeout window between lines; if the server
        goes silent (half-open TCP / stalled proxy) a ``ConnectionError`` is
        raised so the outer loop reconnects. Any line — including SSE
        comments (``:keepalive``) — counts as activity.
        """
        event_type = "message"
        data_parts: list[str] = []
        event_id: Optional[str] = None
        retry_ms: Optional[int] = None

        content_iter = response.content.__aiter__()
        while True:
            if not self._running:
                break

            try:
                if self.heartbeat_timeout > 0:
                    raw_line = await asyncio.wait_for(
                        content_iter.__anext__(),
                        timeout=self.heartbeat_timeout,
                    )
                else:
                    raw_line = await content_iter.__anext__()
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    f"No SSE data received within {self.heartbeat_timeout:.1f}s "
                    "(heartbeat watchdog triggered) — forcing reconnect"
                ) from exc

            line = raw_line.decode("utf-8").rstrip("\r\n")

            if not line:
                # Blank line — dispatch the buffered event if there is data.
                if data_parts:
                    event = SSEEvent(
                        event=event_type,
                        data="\n".join(data_parts),
                        id=event_id,
                        retry=retry_ms,
                    )
                    if event.id is not None:
                        self._last_event_id = event.id
                        self._persist_event_id(event.id)
                    await self._dispatch_event(event)
                # Reset buffers for the next event.
                event_type = "message"
                data_parts = []
                event_id = None
                retry_ms = None
                continue

            if line.startswith(":"):
                # Comment line — ignore.
                continue

            if ":" in line:
                field_name, _, field_value = line.partition(":")
                field_value = field_value.lstrip(" ")
            else:
                field_name = line
                field_value = ""

            if field_name == "event":
                event_type = field_value
            elif field_name == "data":
                data_parts.append(field_value)
            elif field_name == "id":
                event_id = field_value
            elif field_name == "retry":
                try:
                    retry_ms = int(field_value)
                except ValueError:
                    pass

    def _persist_event_id(self, event_id: str) -> None:
        """Fire-and-forget save of the latest event id to the persistent store.

        We don't ``await`` the save inside the parse loop — a slow disk would
        block event dispatch and grow the read buffer. The save task is tracked
        so it can be awaited on stop().
        """
        if self.event_id_store is None:
            return
        try:
            task = asyncio.create_task(self.event_id_store.save(event_id))
        except RuntimeError:
            # No running loop (shouldn't happen inside _parse_sse_stream, but
            # be defensive): fall back to scheduling on the current loop.
            return
        self._save_tasks.add(task)
        task.add_done_callback(self._save_tasks.discard)

    async def _dispatch_event(self, event: SSEEvent) -> None:
        if not self.on_event:
            return
        try:
            if inspect.iscoroutinefunction(self.on_event):
                await self.on_event(event)
            else:
                self.on_event(event)
        except Exception as exc:
            logger.error("Error in SSE on_event callback: %s", exc)

    async def _dispatch_error(self, exc: Exception) -> None:
        if not self.on_error:
            return
        try:
            if inspect.iscoroutinefunction(self.on_error):
                await self.on_error(exc)
            else:
                self.on_error(exc)
        except Exception as cb_exc:
            logger.error("Error in SSE on_error callback: %s", cb_exc)
