"""Regression tests for the synchronous API's event-loop handling.

Historically each sync call ran on its own throwaway loop via ``asyncio.run``.
The ``aiohttp.ClientSession`` created on the first call was bound to that loop,
which was then closed, so the *second* sync call reused a session whose loop was
already gone and raised ``RuntimeError: Event loop is closed``. These tests pin
the fix: every sync call now shares one persistent background loop, so a real
session survives across calls.
"""

import asyncio
import http.server
import threading

import pytest

from dataquery.core.client import DataQueryClient
from dataquery.dataquery import DataQuery
from dataquery.types.models import ClientConfig


def _make_dq(base_url: str = "https://api.example.com") -> DataQuery:
    """Build a DataQuery that passes config validation without OAuth/network."""
    cfg = ClientConfig(base_url=base_url, oauth_enabled=False, bearer_token="test-token")
    return DataQuery(cfg)


def test_sync_calls_share_single_live_loop():
    """Two sync calls must run on the *same*, still-open event loop.

    This is the root cause in isolation: pre-fix, ``asyncio.run`` gave each call
    a distinct loop and closed the first one.
    """
    dq = _make_dq()

    async def _running_loop():
        return asyncio.get_running_loop()

    try:
        loop1 = dq._run_sync(_running_loop())
        loop2 = dq._run_sync(_running_loop())

        assert loop1 is loop2, "sync calls should share one persistent loop"
        assert not loop1.is_closed(), "the shared loop must stay open between calls"
    finally:
        dq.close()


def test_sync_runner_loop_stops_after_close():
    """``close()`` tears down the background loop and its daemon thread."""
    dq = _make_dq()

    async def _noop():
        return True

    assert dq._run_sync(_noop()) is True
    loop = dq._sync_runner._loop
    assert loop is not None

    dq.close()

    assert dq._sync_runner._loop is None
    assert loop.is_closed()


def test_sync_runner_restarts_after_close():
    """A client is reusable after ``close()`` — the runner starts a fresh loop."""
    dq = _make_dq()

    async def _running_loop():
        return asyncio.get_running_loop()

    try:
        loop1 = dq._run_sync(_running_loop())
        dq.close()
        loop2 = dq._run_sync(_running_loop())

        assert loop1 is not loop2
        assert loop1.is_closed()
        assert not loop2.is_closed()
    finally:
        dq.close()


@pytest.mark.asyncio
async def test_sync_call_inside_running_loop_raises():
    """Calling a sync method from inside an event loop raises a clear error."""
    dq = _make_dq()
    with pytest.raises(RuntimeError, match="running\\s+asyncio event loop"):
        # We are inside the test's event loop here.
        dq.list_groups()


# ---------------------------------------------------------------------------
# Faithful end-to-end regression: a real aiohttp session must survive a second
# sync call. Pre-fix this raised "RuntimeError: Event loop is closed".
# ---------------------------------------------------------------------------


class _OkHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server API
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silence per-request stderr logging
        pass


@pytest.fixture
def local_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _OkHandler)
    thread = threading.Thread(target=server.serve_forever, name="test-http", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def test_sync_real_session_survives_across_calls(local_server):
    """Open a real session via ``connect()``, then reuse it on a later sync call.

    The second sync call runs on a different ``asyncio.run`` loop pre-fix, so the
    real session (bound to the first loop) blows up. Post-fix it shares the loop
    and the request completes.
    """
    dq = _make_dq(base_url=local_server)
    try:
        # First sync round-trip: creates the real aiohttp session.
        dq.connect()
        session = dq._client.session
        assert not session.closed

        # Second sync round-trip: actually drives the same session. This is the
        # exact operation that used to raise "Event loop is closed".
        async def _get_status():
            # The call must run on the loop the session is bound to.
            assert asyncio.get_running_loop() is session._loop
            async with dq._client.session.get(f"{local_server}/ping") as resp:
                return resp.status

        status = dq._run_sync(_get_status())
        assert status == 200
        assert not session.closed
    finally:
        dq.close()


def test_sync_wrapper_two_real_calls_no_loop_error(local_server):
    """Two consecutive public sync calls on a live session don't error out."""
    dq = _make_dq(base_url=local_server)
    try:
        dq.connect()
        url = f"{local_server}/ping"

        async def _get():
            async with dq._client.session.get(url) as resp:
                return resp.status

        # Several sequential sync calls — the pre-fix bug surfaced on the 2nd.
        assert dq._run_sync(_get()) == 200
        assert dq._run_sync(_get()) == 200
        assert dq._run_sync(_get()) == 200
    finally:
        dq.close()


def test_client_sync_calls_share_single_live_loop():
    """The lower-level DataQueryClient must also share one persistent loop.

    DataQueryClient is a public export with its own ``_run_sync``; pre-fix it had
    the same ``asyncio.run``-per-call loop-binding bug as the facade.
    """
    cfg = ClientConfig(base_url="https://api.example.com", oauth_enabled=False, bearer_token="t")
    client = DataQueryClient(cfg)

    async def _running_loop():
        return asyncio.get_running_loop()

    try:
        loop1 = client._run_sync(_running_loop())
        loop2 = client._run_sync(_running_loop())

        assert loop1 is loop2
        assert not loop1.is_closed()
    finally:
        client._sync_runner.close()
