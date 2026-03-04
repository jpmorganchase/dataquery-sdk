"""
Server-Sent Events (SSE) client for the DataQuery notification endpoint.

Connects to the /notification SSE endpoint and dispatches events to registered
handlers. Automatically reconnects on connection loss using exponential backoff.
"""

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Callable, Optional

import aiohttp

from .auth import OAuthManager
from .models import ClientConfig

logger = logging.getLogger(__name__)


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
        on_event: Optional[Callable[[SSEEvent], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        sse_timeout: float = 0,  # 0 = no timeout on the streaming read
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
        """
        self.config = config
        self.auth_manager = auth_manager
        self.on_event = on_event
        self.on_error = on_error
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.sse_timeout = sse_timeout

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_event_id: Optional[str] = None

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
        logger.info("SSEClient stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_notification_url(self) -> str:
        base = self.config.api_base_url.rstrip("/")
        return f"{base}/notification"

    async def _get_headers(self) -> dict:
        headers = await self.auth_manager.get_headers()
        headers["Accept"] = "text/event-stream"
        headers["Cache-Control"] = "no-cache"
        if self._last_event_id is not None:
            headers["Last-Event-ID"] = self._last_event_id
        return headers

    async def _run_loop(self) -> None:
        """Outer loop: reconnect with exponential backoff on failure."""
        delay = self.reconnect_delay
        while self._running:
            try:
                await self._connect_and_listen()
                # If _connect_and_listen returned cleanly (stop requested),
                # exit the loop.
                if not self._running:
                    break
                # Server closed the connection — reconnect after a short pause.
                logger.info("SSE connection closed by server; reconnecting in %.1fs", delay)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("SSE connection error: %s; reconnecting in %.1fs", exc, delay)
                await self._dispatch_error(exc)

            # Wait for `delay` seconds or until stop() is called.
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                break  # stop() was called during the wait
            except asyncio.TimeoutError:
                pass

            # Exponential backoff, capped at max_reconnect_delay.
            delay = min(delay * 2, self.max_reconnect_delay)

        self._running = False

    async def _connect_and_listen(self) -> None:
        """Open a single SSE connection and read events until disconnected."""
        url = self._build_notification_url()
        headers = await self._get_headers()

        # Use a fresh session per connection to avoid header/state bleed.
        timeout = aiohttp.ClientTimeout(total=None, connect=30.0, sock_read=self.sse_timeout or None)

        proxy = self.config.proxy_url if self.config.proxy_enabled else None

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, proxy=proxy) as response:
                if response.status != 200:
                    raise ConnectionError(
                        f"SSE endpoint returned HTTP {response.status} for {url}"
                    )
                logger.info("SSE connection established to %s", url)
                await self._parse_sse_stream(response)

    async def _parse_sse_stream(self, response: aiohttp.ClientResponse) -> None:
        """
        Read and parse an SSE stream line-by-line, dispatching events.

        SSE wire format (per spec)::

            event: <type>\\n
            data: <payload>\\n
            id: <id>\\n
            retry: <ms>\\n
            \\n          ← blank line dispatches the buffered event
        """
        event_type = "message"
        data_parts: list[str] = []
        event_id: Optional[str] = None
        retry_ms: Optional[int] = None

        async for raw_line in response.content:
            if not self._running:
                break

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
