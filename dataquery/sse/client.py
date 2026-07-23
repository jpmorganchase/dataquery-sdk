"""Server-Sent Events (SSE) client for the DataQuery notification endpoint."""

import asyncio
import inspect
import logging
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Union

import aiohttp

from .. import constants as C
from ..transport.auth import OAuthManager
from ..types.models import ClientConfig
from .event_store import SSEEventIdStore

logger = logging.getLogger(__name__)


def is_expected_disconnect(exc: BaseException) -> bool:
    """True for connection-close exceptions raised during normal server recycles."""
    return isinstance(
        exc,
        (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, aiohttp.ServerTimeoutError),
    )


def _with_jitter(delay: float) -> float:
    """Apply equal jitter: a random point in ``[delay/2, delay]``."""
    if delay <= 0:
        return delay
    return random.uniform(delay / 2.0, delay)


class _SSEFatalError(Exception):
    """Non-retryable SSE failure (403/404 or exhausted auth) — stop reconnecting."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


class _SSEAuthError(Exception):
    """HTTP 401 on connect — retryable a bounded number of times, then escalated to a fatal error."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


@dataclass
class SSEEvent:
    """A parsed Server-Sent Event."""

    event: str = "message"
    data: str = ""
    id: Optional[str] = None
    retry: Optional[int] = None


class SSEClient:
    """Server-Sent Events client for the DataQuery /notification endpoint."""

    def __init__(
        self,
        config: ClientConfig,
        auth_manager: OAuthManager,
        on_event: Optional[Callable[[SSEEvent], Union[None, Awaitable[None]]]] = None,
        on_error: Optional[Callable[[Exception], Union[None, Awaitable[None]]]] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        sse_timeout: float = 90.0,
        params: Optional[dict] = None,
        event_id_store: Optional[SSEEventIdStore] = None,
        heartbeat_timeout: float = 0.0,
        defer_event_id_persistence: bool = False,
    ):
        """Initialise the SSE client."""
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
        self._defer_event_id_persistence = defer_event_id_persistence

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_event_id: Optional[str] = None
        if event_id_store is not None:
            stored = event_id_store.load()
            if stored:
                self._last_event_id = stored
                logger.info("Seeded SSE last-event-id from store: %s", stored)
        self._save_tasks: set[asyncio.Task] = set()
        self._last_connection_duration: float = 0.0
        self._consecutive_auth_failures: int = 0
        self._server_retry_delay: Optional[float] = None
        self._stop_lock: Optional[asyncio.Lock] = None

    async def start(self) -> "SSEClient":
        """Start the SSE connection loop in a background task."""
        if self._running:
            raise RuntimeError("SSEClient is already running")
        self._running = True
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        else:
            self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="sse-client")
        return self

    async def stop(self) -> None:
        """Signal the connection loop to stop and wait for it to finish."""
        if self._stop_lock is None:
            self._stop_lock = asyncio.Lock()
        async with self._stop_lock:
            if not self._running and self._task is None:
                return
            self._running = False
            if self._stop_event is not None:
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
                self._task = None
            if self._save_tasks:
                await asyncio.gather(*self._save_tasks, return_exceptions=True)
                self._save_tasks.clear()
            logger.info("SSEClient stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_event_id(self) -> Optional[str]:
        """The most recent SSE event id seen on the wire, or ``None``."""
        return self._last_event_id

    def _build_notification_url(self) -> str:
        base = self.config.api_base_url.rstrip("/")
        return f"{base}{C.SSE_NOTIFICATION_PATH}"

    async def _get_headers(self) -> dict:
        headers = await self.auth_manager.get_headers()
        headers["Accept"] = "text/event-stream"
        if self._last_event_id is not None:
            headers["Last-Event-ID"] = self._last_event_id
        return headers

    def _base_delay(self) -> float:
        """The reconnect floor: the server-supplied ``retry:`` hint, otherwise the configured ``reconnect_delay``."""
        if self._server_retry_delay is not None:
            return self._server_retry_delay
        return self.reconnect_delay

    async def _run_loop(self) -> None:
        """Outer loop: reconnect with exponential backoff on failure."""
        delay = self._base_delay()
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        stop_event = self._stop_event
        while self._running:
            self._last_connection_duration = 0.0
            try:
                await self._connect_and_listen()
                self._consecutive_auth_failures = 0
                if not self._running:
                    break
                logger.info(
                    "SSE connection closed by server after %.1fs; reconnecting in ~%.1fs",
                    self._last_connection_duration,
                    delay,
                )
            except asyncio.CancelledError:
                break
            except _SSEFatalError as exc:
                logger.error("SSE fatal error (HTTP %s): %s; not reconnecting", exc.status, exc)
                await self._dispatch_error(exc)
                break
            except Exception as exc:
                if isinstance(exc, _SSEAuthError):
                    self._consecutive_auth_failures += 1
                    if self._consecutive_auth_failures > C.SSE_MAX_AUTH_RETRIES:
                        logger.error(
                            "SSE authentication failed %d consecutive times (HTTP 401); not reconnecting",
                            self._consecutive_auth_failures,
                        )
                        await self._dispatch_error(exc)
                        break
                    logger.warning(
                        "SSE auth failed (HTTP 401), attempt %d/%d; retrying after token refresh",
                        self._consecutive_auth_failures,
                        C.SSE_MAX_AUTH_RETRIES,
                    )
                else:
                    self._consecutive_auth_failures = 0
                    if is_expected_disconnect(exc):
                        logger.debug(
                            "SSE connection closed after %.1fs: %s; reconnecting in ~%.1fs",
                            self._last_connection_duration,
                            exc,
                            delay,
                        )
                    else:
                        logger.warning(
                            "SSE connection error after %.1fs: %s; reconnecting in ~%.1fs",
                            self._last_connection_duration,
                            exc,
                            delay,
                        )
                await self._dispatch_error(exc)
                next_delay = min(delay * 2, self.max_reconnect_delay)

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=_with_jitter(delay))
                    break
                except asyncio.TimeoutError:
                    pass
                delay = next_delay
                continue

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=_with_jitter(delay))
                break
            except asyncio.TimeoutError:
                pass

            if self._last_connection_duration >= C.SSE_HEALTHY_CONNECTION_SECONDS:
                delay = self._base_delay()
            else:
                delay = min(delay * 2, self.max_reconnect_delay)

        self._running = False

    def _build_request_params(self) -> Optional[dict]:
        """Combine the static subscription params with the current ``last-event-id`` when known."""
        effective: dict = dict(self.params) if self.params else {}
        if self._last_event_id is not None:
            effective["last-event-id"] = self._last_event_id
        return effective or None

    async def _connect_and_listen(self) -> float:
        """Open a single SSE connection and read events until disconnected."""
        url = self._build_notification_url()
        headers = await self._get_headers()
        request_params = self._build_request_params()

        timeout = aiohttp.ClientTimeout(total=None, connect=30.0, sock_read=self.sse_timeout or None)

        proxy_kwargs = self.config.get_proxy_kwargs()

        # read_bufsize raises the per-line limit so a large frame can't trip LineTooLong.
        async with aiohttp.ClientSession(timeout=timeout, read_bufsize=C.SSE_READ_BUFSIZE) as session:
            async with session.get(url, headers=headers, params=request_params, **proxy_kwargs) as response:
                if response.status != 200:
                    body = ""
                    try:
                        body = (await response.text())[:200]
                    except Exception:
                        pass
                    detail = f": {body}" if body else ""
                    if response.status in (403, 404):
                        raise _SSEFatalError(
                            response.status,
                            f"SSE endpoint returned HTTP {response.status} for {url}{detail}",
                        )
                    if response.status == 401:
                        raise _SSEAuthError(
                            response.status,
                            f"SSE authentication failed (HTTP 401) for {url}{detail}",
                        )
                    raise ConnectionError(f"SSE endpoint returned HTTP {response.status} for {url}{detail}")
                logger.info("SSE connection established to %s (params=%s)", url, request_params)
                started_at = time.monotonic()
                self._last_connection_duration = 0.0
                try:
                    await self._parse_sse_stream(response)
                finally:
                    self._last_connection_duration = time.monotonic() - started_at
                return self._last_connection_duration

    async def _parse_sse_stream(self, response: aiohttp.ClientResponse) -> None:
        """Read and parse an SSE stream line-by-line, dispatching events."""
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
            except aiohttp.ServerTimeoutError:
                # sock_read idle timeout; SocketTimeoutError subclasses asyncio.TimeoutError, so handle first.
                raise
            except asyncio.TimeoutError as exc:
                raise ConnectionError(
                    f"No SSE data within {self.heartbeat_timeout:.1f}s (heartbeat watchdog) — forcing reconnect"
                ) from exc

            # utf-8-sig strips a leading UTF-8 BOM (SSE spec); strip exactly the terminator.
            line = raw_line.decode("utf-8-sig")
            if line.endswith("\n"):
                line = line[:-1]
            if line.endswith("\r"):
                line = line[:-1]

            if not line:
                if data_parts:
                    event = SSEEvent(
                        event=event_type,
                        data="\n".join(data_parts),
                        id=event_id,
                        retry=retry_ms,
                    )
                    await self._dispatch_event(event)
                event_type = "message"
                data_parts = []
                event_id = None
                retry_ms = None
                continue

            if line.startswith(":"):
                continue

            if ":" in line:
                field_name, _, field_value = line.partition(":")
                # SSE spec: strip exactly one leading space from the value.
                if field_value.startswith(" "):
                    field_value = field_value[1:]
            else:
                field_name = line
                field_value = ""

            if field_name == "event":
                event_type = field_value
            elif field_name == "data":
                data_parts.append(field_value)
            elif field_name == "id":
                event_id = field_value
                if field_value and field_value.isdigit():
                    self._last_event_id = field_value
                    if not self._defer_event_id_persistence:
                        self._persist_event_id(field_value)
            elif field_name == "retry":
                try:
                    retry_ms = int(field_value)
                except ValueError:
                    pass
                else:
                    # SSE spec: 'retry' sets the reconnect time; track separately, clamp to max.
                    self._server_retry_delay = min(retry_ms / 1000.0, self.max_reconnect_delay)

    async def _dispatch_event(self, event: SSEEvent) -> None:
        if not self.on_event:
            return
        try:
            if inspect.iscoroutinefunction(self.on_event):
                await self.on_event(event)
            else:
                self.on_event(event)
        except Exception:
            logger.exception("Error in SSE on_event callback (event=%s id=%s)", event.event, event.id)

    async def _dispatch_error(self, exc: Exception) -> None:
        if not self.on_error:
            return
        try:
            if inspect.iscoroutinefunction(self.on_error):
                await self.on_error(exc)
            else:
                self.on_error(exc)
        except Exception:
            logger.exception("Error in SSE on_error callback")

    def _persist_event_id(self, event_id: str) -> None:
        """Fire-and-forget save of the event id to the persistent store."""
        store = self.event_id_store
        if store is None:
            return

        async def _save() -> None:
            try:
                await store.save(event_id)
            except Exception as exc:
                logger.warning("Failed to persist SSE event id: %s", exc)

        task = asyncio.create_task(_save())
        self._save_tasks.add(task)
        task.add_done_callback(self._save_tasks.discard)
