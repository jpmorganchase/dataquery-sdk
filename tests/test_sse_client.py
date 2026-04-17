"""Tests for ``dataquery.sse_client.SSEClient``.

Covers SSE wire-format parsing, event/error dispatch (sync + async callbacks),
URL construction, start/stop lifecycle, and exponential reconnection backoff.
The network layer is faked — no real HTTP is performed.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import AsyncMock

import pytest

from dataquery.models import ClientConfig
from dataquery.sse_client import SSEClient, SSEEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> ClientConfig:
    return ClientConfig(
        base_url="https://api.example.com",
        context_path="/api/v2",
        oauth_enabled=False,
        bearer_token="T",
    )


def _make_auth_manager(headers: dict | None = None):
    headers = headers or {"Authorization": "Bearer T"}
    mgr = SimpleNamespace()
    mgr.get_headers = AsyncMock(return_value=dict(headers))
    return mgr


class _FakeContent:
    """Async iterator of pre-canned SSE byte lines."""

    def __init__(self, lines: List[bytes]):
        self._lines = iter(lines)

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._lines)
        except StopIteration:
            raise StopAsyncIteration


class _FakeResponse:
    def __init__(self, content: _FakeContent):
        self.content = content


# ---------------------------------------------------------------------------
# Unit tests for internals
# ---------------------------------------------------------------------------


def test_params_default_none_when_not_provided():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    assert client.params is None


def test_params_copied_defensively_from_constructor():
    src = {"group-id": "G", "file-group-id": "FG"}
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), params=src)
    # Mutating the original must not affect the client.
    src["group-id"] = "OTHER"
    assert client.params == {"group-id": "G", "file-group-id": "FG"}


def test_build_notification_url_strips_trailing_slash():
    cfg = _make_config()
    client = SSEClient(config=cfg, auth_manager=_make_auth_manager())
    url = client._build_notification_url()
    assert url.endswith("/sse/event/notification")
    assert "//sse" not in url


@pytest.mark.asyncio
async def test_get_headers_sets_sse_fields_and_last_event_id():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    client._last_event_id = "evt-42"
    headers = await client._get_headers()
    assert headers["Accept"] == "text/event-stream"
    assert headers["Cache-Control"] == "no-cache"
    assert headers["Last-Event-ID"] == "evt-42"
    assert headers["Authorization"] == "Bearer T"


@pytest.mark.asyncio
async def test_get_headers_omits_last_event_id_when_unset():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    headers = await client._get_headers()
    assert "Last-Event-ID" not in headers


# ---------------------------------------------------------------------------
# SSE wire-format parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_sse_stream_dispatches_single_event():
    received: list[SSEEvent] = []

    def on_event(evt: SSEEvent) -> None:
        received.append(evt)

    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), on_event=on_event)
    client._running = True

    content = _FakeContent(
        [
            b"event: message\n",
            b"data: hello\n",
            b"id: 7\n",
            b"\n",
        ]
    )
    await client._parse_sse_stream(_FakeResponse(content))

    assert len(received) == 1
    assert received[0].event == "message"
    assert received[0].data == "hello"
    assert received[0].id == "7"
    # Last-event-id should be captured for reconnection.
    assert client._last_event_id == "7"


@pytest.mark.asyncio
async def test_parse_sse_stream_multiline_data_joined_with_newline():
    received: list[SSEEvent] = []
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        on_event=lambda e: received.append(e),
    )
    client._running = True

    content = _FakeContent(
        [
            b"data: line1\n",
            b"data: line2\n",
            b"\n",
        ]
    )
    await client._parse_sse_stream(_FakeResponse(content))

    assert received[0].data == "line1\nline2"


@pytest.mark.asyncio
async def test_parse_sse_stream_ignores_comments_and_default_event_type():
    received: list[SSEEvent] = []
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        on_event=lambda e: received.append(e),
    )
    client._running = True

    content = _FakeContent(
        [
            b": keepalive comment\n",
            b"data: x\n",
            b"\n",
        ]
    )
    await client._parse_sse_stream(_FakeResponse(content))

    assert len(received) == 1
    assert received[0].event == "message"
    assert received[0].data == "x"


@pytest.mark.asyncio
async def test_parse_sse_stream_parses_retry_hint_and_ignores_garbage():
    received: list[SSEEvent] = []
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        on_event=lambda e: received.append(e),
    )
    client._running = True

    content = _FakeContent(
        [
            b"retry: 2500\n",
            b"retry: not-a-number\n",
            b"data: ping\n",
            b"\n",
        ]
    )
    await client._parse_sse_stream(_FakeResponse(content))

    # Later invalid retry preserves the previously parsed value.
    assert received[0].retry == 2500


@pytest.mark.asyncio
async def test_parse_sse_stream_multiple_events_reset_buffers():
    received: list[SSEEvent] = []
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        on_event=lambda e: received.append(e),
    )
    client._running = True

    content = _FakeContent(
        [
            b"event: one\n",
            b"data: a\n",
            b"id: 1\n",
            b"\n",
            b"data: b\n",
            b"\n",
        ]
    )
    await client._parse_sse_stream(_FakeResponse(content))

    assert len(received) == 2
    assert received[0].event == "one"
    assert received[0].id == "1"
    assert received[1].event == "message"  # reset to default
    assert received[1].id is None
    assert received[1].data == "b"


@pytest.mark.asyncio
async def test_parse_sse_stream_stops_mid_stream_when_running_false():
    received: list[SSEEvent] = []
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        on_event=lambda e: received.append(e),
    )
    client._running = True

    lines = iter(
        [
            b"data: first\n",
            b"\n",
            b"data: second\n",
            b"\n",
        ]
    )

    class _StoppingContent:
        def __aiter__(self):
            return self

        async def __anext__(self) -> bytes:
            try:
                line = next(lines)
            except StopIteration:
                raise StopAsyncIteration
            # Flip the flag just before returning the second event's data line.
            if line == b"data: second\n":
                client._running = False
            return line

    await client._parse_sse_stream(_FakeResponse(_StoppingContent()))
    # Only the first event should have been dispatched before stop took effect.
    assert [e.data for e in received] == ["first"]


# ---------------------------------------------------------------------------
# Callback dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_event_supports_async_callback():
    calls: list[SSEEvent] = []

    async def on_event(evt: SSEEvent) -> None:
        calls.append(evt)

    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), on_event=on_event)
    await client._dispatch_event(SSEEvent(data="x"))
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_dispatch_event_swallows_callback_exceptions():
    def boom(_evt: Any) -> None:
        raise RuntimeError("callback failed")

    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), on_event=boom)
    # Must not raise.
    await client._dispatch_event(SSEEvent(data="x"))


@pytest.mark.asyncio
async def test_dispatch_error_supports_async_callback():
    seen: list[Exception] = []

    async def on_error(exc: Exception) -> None:
        seen.append(exc)

    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), on_error=on_error)
    await client._dispatch_error(RuntimeError("x"))
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_dispatch_error_no_op_without_callback():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    await client._dispatch_error(RuntimeError("x"))


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_then_stop_cleanly_exits_loop():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    # Replace the inner connect to do nothing and let the loop idle.
    client._connect_and_listen = AsyncMock(return_value=None)  # type: ignore[assignment]

    await client.start()
    assert client.is_running
    await client.stop()
    assert not client.is_running


@pytest.mark.asyncio
async def test_start_twice_raises():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    client._connect_and_listen = AsyncMock(return_value=None)  # type: ignore[assignment]
    await client.start()
    try:
        with pytest.raises(RuntimeError):
            await client.start()
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_stop_without_start_is_noop():
    client = SSEClient(config=_make_config(), auth_manager=_make_auth_manager())
    await client.stop()  # should simply return


@pytest.mark.asyncio
async def test_run_loop_reconnects_with_exponential_backoff_then_stops():
    """After a failure the outer loop waits ``delay`` then doubles it.

    We trigger two failures, inspect the growing delay via a patched wait_for
    that records the requested timeout, then stop the client.
    """
    client = SSEClient(
        config=_make_config(),
        auth_manager=_make_auth_manager(),
        reconnect_delay=1.0,
        max_reconnect_delay=8.0,
    )

    attempts = {"n": 0}

    async def fake_connect() -> None:
        attempts["n"] += 1
        raise ConnectionError("boom")

    client._connect_and_listen = fake_connect  # type: ignore[assignment]

    delays: list[float] = []
    orig_wait_for = asyncio.wait_for

    async def capturing_wait_for(coro: Any, timeout: float) -> Any:
        delays.append(timeout)
        # After two backoff slots let the stop event fire to exit the loop.
        if len(delays) >= 2:
            client._stop_event.set()
        return await orig_wait_for(coro, timeout=0.01)

    client._stop_event = asyncio.Event()
    # Patch asyncio.wait_for only within the sse_client module.
    import dataquery.sse_client as sse_mod

    sse_mod.asyncio.wait_for = capturing_wait_for  # type: ignore[assignment]
    try:
        client._running = True
        await client._run_loop()
    finally:
        sse_mod.asyncio.wait_for = orig_wait_for  # type: ignore[assignment]

    assert attempts["n"] >= 2
    # Delays should grow: 1.0, 2.0 ... (capped at 8.0).
    assert delays[0] == pytest.approx(1.0)
    assert delays[1] == pytest.approx(2.0)
