"""Shared synchronous-execution helper.

Both the high-level :class:`~dataquery.dataquery.DataQuery` facade and the
lower-level :class:`~dataquery.core.client.DataQueryClient` expose synchronous
wrappers around their async methods. They must not use ``asyncio.run`` per call:
that opens and closes a fresh event loop each time, but an
``aiohttp.ClientSession`` is bound to the loop that created it. A second sync
call would then reuse a session whose loop is already closed and raise
``RuntimeError: Event loop is closed``.

:class:`SyncRunner` funnels every sync call through one persistent background
loop running in a daemon thread, so the session stays valid for the lifetime of
the client.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Optional


class SyncRunner:
    """Runs coroutines on a single persistent event loop in a daemon thread.

    The loop is created lazily on first use and torn down by :meth:`close`.
    After teardown the runner is reusable — the next :meth:`run` starts a new
    loop — so a client can be closed and used again.
    """

    __slots__ = ("_loop", "_thread", "_lock")

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @staticmethod
    def _loop_main(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            return loop
        with self._lock:
            loop = self._loop
            if loop is not None and not loop.is_closed():
                return loop
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=self._loop_main,
                args=(loop,),
                name="dataquery-sync-loop",
                daemon=True,
            )
            thread.start()
            self._loop = loop
            self._thread = thread
            return loop

    def run(self, coro: Any) -> Any:
        """Submit ``coro`` to the background loop and block for its result."""
        # Refuse to block the calling thread's own running loop — would deadlock.
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            coro.close()
            raise RuntimeError(
                "Cannot run a synchronous DataQuery method from within a running "
                "asyncio event loop. Use the async (*_async) version instead."
            )

        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    def close(self) -> None:
        """Stop the background loop and join its thread (idempotent)."""
        with self._lock:
            loop, thread = self._loop, self._thread
            self._loop, self._thread = None, None
        if loop is None:
            return
        if not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread.is_alive():
            thread.join(timeout=10.0)
