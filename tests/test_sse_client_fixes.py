"""Tests for the SSE client robustness fixes:

- equal-jitter backoff
- sock_read vs heartbeat timeout distinction + is_expected_disconnect membership
- fatal (403/404) vs bounded-retry (401) vs transient HTTP classification
- server ``retry:`` hint tracked separately from configured reconnect_delay
- UTF-8 BOM stripping
"""

import asyncio
from types import SimpleNamespace
from typing import List
from unittest.mock import AsyncMock

import aiohttp
import pytest

from dataquery import constants as C
from dataquery.sse.client import (
    SSEClient,
    SSEEvent,
    _SSEAuthError,
    _SSEFatalError,
    _with_jitter,
    is_expected_disconnect,
)
from dataquery.types.models import ClientConfig


def _make_config() -> ClientConfig:
    return ClientConfig(
        base_url="https://api.example.com",
        context_path="/api/v2",
        oauth_enabled=False,
        bearer_token="T",
    )


def _make_auth_manager():
    mgr = SimpleNamespace()
    mgr.get_headers = AsyncMock(return_value={"Authorization": "Bearer T"})
    mgr.force_refresh = AsyncMock(return_value=None)
    mgr.token_manager = SimpleNamespace(current_token=None)
    return mgr


class _FakeContent:
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
    def __init__(self, content):
        self.content = content


def _client(**kwargs) -> SSEClient:
    return SSEClient(config=_make_config(), auth_manager=_make_auth_manager(), **kwargs)


# --------------------------------------------------------------------------- #
# Jitter
# --------------------------------------------------------------------------- #
def test_with_jitter_within_equal_jitter_range():
    for base in (1.0, 2.0, 8.0, 60.0):
        for _ in range(50):
            assert base / 2.0 <= _with_jitter(base) <= base
    assert _with_jitter(0.0) == 0.0


# --------------------------------------------------------------------------- #
# Timeout classification
# --------------------------------------------------------------------------- #
def test_is_expected_disconnect_includes_server_timeout():
    assert is_expected_disconnect(aiohttp.ServerTimeoutError()) is True
    assert is_expected_disconnect(aiohttp.SocketTimeoutError()) is True
    assert is_expected_disconnect(aiohttp.ServerDisconnectedError()) is True
    assert is_expected_disconnect(ValueError()) is False


@pytest.mark.asyncio
async def test_sock_read_timeout_propagates_unwrapped():
    # heartbeat disabled (default): a sock_read ServerTimeoutError must propagate
    # as-is (not be re-wrapped as a "heartbeat watchdog" ConnectionError) so the
    # outer loop classifies it as an expected idle recycle.
    client = _client()
    client._running = True

    class _TimeoutContent:
        def __aiter__(self):
            return self

        async def __anext__(self) -> bytes:
            raise aiohttp.ServerTimeoutError("sock read timed out")

    with pytest.raises(aiohttp.ServerTimeoutError):
        await client._parse_sse_stream(_FakeResponse(_TimeoutContent()))


# --------------------------------------------------------------------------- #
# Fatal vs bounded-retry vs transient HTTP
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_run_loop_stops_on_fatal_status():
    errors: list = []
    client = _client(on_error=errors.append)
    attempts = {"n": 0}

    async def fatal():
        attempts["n"] += 1
        raise _SSEFatalError(403, "forbidden")

    client._connect_and_listen = fatal  # type: ignore[assignment]
    client._running = True
    client._stop_event = asyncio.Event()

    await client._run_loop()

    assert attempts["n"] == 1  # no reconnect on a fatal status
    assert any(isinstance(e, _SSEFatalError) for e in errors)
    assert not client._running


@pytest.mark.asyncio
async def test_run_loop_bounded_retries_on_401_then_stops(monkeypatch):
    errors: list = []
    client = _client(on_error=errors.append, reconnect_delay=0.01, max_reconnect_delay=0.02)
    attempts = {"n": 0}

    async def always_401():
        attempts["n"] += 1
        raise _SSEAuthError(401, "unauthorized")

    client._connect_and_listen = always_401  # type: ignore[assignment]

    import dataquery.sse.client as sse_mod

    orig_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await orig_wait_for(coro, timeout=0.001)

    monkeypatch.setattr(sse_mod.asyncio, "wait_for", fast_wait_for)

    client._running = True
    client._stop_event = asyncio.Event()
    await client._run_loop()

    # SSE_MAX_AUTH_RETRIES retries + the initial attempt, then give up.
    assert attempts["n"] == C.SSE_MAX_AUTH_RETRIES + 1
    assert any(isinstance(e, _SSEAuthError) for e in errors)
    assert not client._running


# --------------------------------------------------------------------------- #
# Server retry: hint
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_retry_hint_tracked_separately_from_reconnect_delay():
    client = _client(reconnect_delay=5.0, max_reconnect_delay=60.0)
    client._running = True
    content = _FakeContent([b"retry: 2500\n", b"data: x\n", b"\n"])
    await client._parse_sse_stream(_FakeResponse(content))

    assert client._server_retry_delay == 2.5
    assert client.reconnect_delay == 5.0  # configured floor untouched
    assert client._base_delay() == 2.5  # hint preferred for the reconnect base


@pytest.mark.asyncio
async def test_retry_hint_clamped_to_max():
    client = _client(reconnect_delay=5.0, max_reconnect_delay=10.0)
    client._running = True
    content = _FakeContent([b"retry: 999000\n", b"\n"])
    await client._parse_sse_stream(_FakeResponse(content))
    assert client._server_retry_delay == 10.0


# --------------------------------------------------------------------------- #
# BOM stripping
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_parse_strips_utf8_bom_on_first_line():
    received: list[SSEEvent] = []
    client = _client(on_event=received.append)
    client._running = True
    # First line carries a UTF-8 BOM; without stripping, the field name becomes
    # "﻿data" and the line is ignored (no event dispatched).
    content = _FakeContent([b"\xef\xbb\xbfdata: hi\n", b"\n"])
    await client._parse_sse_stream(_FakeResponse(content))

    assert len(received) == 1
    assert received[0].data == "hi"
