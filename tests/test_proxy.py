"""Proxy plumbing: ``ClientConfig.get_proxy_kwargs()`` + auth + SSE threading.

Regression guard for the bug where ``TokenManager._get_new_token`` /
``_refresh_token`` created an ``aiohttp.ClientSession`` without honoring any
proxy options from ``ClientConfig``. The same helper is now used by the SSE
client and the main API request path, so those are exercised here too.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import BasicAuth

from dataquery.sse.client import SSEClient
from dataquery.transport.auth import TokenManager
from dataquery.types.models import ClientConfig, OAuthToken, TokenResponse

# ---------------------------------------------------------------------------
# ClientConfig.get_proxy_kwargs — the single source of truth
# ---------------------------------------------------------------------------


def test_proxy_kwargs_empty_when_disabled():
    cfg = ClientConfig(base_url="https://api.example.com", proxy_enabled=False, proxy_url="http://p:8080")
    assert cfg.get_proxy_kwargs() == {}


def test_proxy_kwargs_empty_when_enabled_but_url_missing():
    cfg = ClientConfig(base_url="https://api.example.com", proxy_enabled=True, proxy_url=None)
    assert cfg.get_proxy_kwargs() == {}


def test_proxy_kwargs_url_only_when_no_credentials():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        proxy_enabled=True,
        proxy_url="http://proxy:8080",
    )
    kwargs = cfg.get_proxy_kwargs()
    assert kwargs == {"proxy": "http://proxy:8080"}
    assert "proxy_auth" not in kwargs


def test_proxy_kwargs_includes_basic_auth_when_credentials_set():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        proxy_enabled=True,
        proxy_url="http://proxy:8080",
        proxy_username="alice",
        proxy_password="s3cret",
    )
    kwargs = cfg.get_proxy_kwargs()
    assert kwargs["proxy"] == "http://proxy:8080"
    assert isinstance(kwargs["proxy_auth"], BasicAuth)
    assert kwargs["proxy_auth"].login == "alice"
    assert kwargs["proxy_auth"].password == "s3cret"


# ---------------------------------------------------------------------------
# TokenManager: the original bug — POSTs to the OAuth token URL must route
# through the proxy.
# ---------------------------------------------------------------------------


def _proxy_cfg(with_auth: bool = False) -> ClientConfig:
    return ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=True,
        client_id="cid",
        client_secret="csecret",
        oauth_token_url="https://authe.example.com/oauth/token",
        proxy_enabled=True,
        proxy_url="http://proxy:8080",
        proxy_username="u" if with_auth else None,
        proxy_password="p" if with_auth else None,
    )


class _StubPostCtx:
    """Async context manager that captures the kwargs passed to ``session.post``
    and returns a canned JSON response."""

    def __init__(self, payload: dict, recorder: dict):
        self._payload = payload
        self._recorder = recorder

    async def __aenter__(self):
        resp = MagicMock()
        resp.status = 200
        resp.json = AsyncMock(return_value=self._payload)
        return resp

    async def __aexit__(self, *_a):
        return False


class _StubSessionCtx:
    """Stubs aiohttp.ClientSession() → returns a session whose ``.post`` records
    every kwarg it's called with."""

    def __init__(self, recorder: dict, payload: dict):
        self._recorder = recorder
        self._payload = payload

    async def __aenter__(self):
        session = MagicMock()

        def post(url, **kwargs):
            self._recorder.clear()
            self._recorder.update(kwargs)
            self._recorder["_url"] = url
            return _StubPostCtx(self._payload, self._recorder)

        session.post = post
        return session

    async def __aexit__(self, *_a):
        return False


_FAKE_TOKEN_PAYLOAD = {
    "access_token": "tok",
    "token_type": "Bearer",
    "expires_in": 3600,
}


@pytest.mark.asyncio
async def test_get_new_token_applies_proxy_without_auth():
    cfg = _proxy_cfg(with_auth=False)
    recorder: dict = {}

    with patch(
        "dataquery.transport.auth.aiohttp.ClientSession", return_value=_StubSessionCtx(recorder, _FAKE_TOKEN_PAYLOAD)
    ):
        mgr = TokenManager(cfg)
        token = await mgr._get_new_token()

    assert isinstance(token, OAuthToken)
    assert recorder["_url"] == cfg.oauth_token_url
    assert recorder["proxy"] == "http://proxy:8080"
    assert "proxy_auth" not in recorder


@pytest.mark.asyncio
async def test_get_new_token_applies_proxy_with_basic_auth():
    cfg = _proxy_cfg(with_auth=True)
    recorder: dict = {}

    with patch(
        "dataquery.transport.auth.aiohttp.ClientSession", return_value=_StubSessionCtx(recorder, _FAKE_TOKEN_PAYLOAD)
    ):
        mgr = TokenManager(cfg)
        await mgr._get_new_token()

    assert recorder["proxy"] == "http://proxy:8080"
    assert isinstance(recorder["proxy_auth"], BasicAuth)
    assert recorder["proxy_auth"].login == "u"
    assert recorder["proxy_auth"].password == "p"


@pytest.mark.asyncio
async def test_get_new_token_omits_proxy_kwargs_when_disabled():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=True,
        client_id="cid",
        client_secret="csecret",
        oauth_token_url="https://authe.example.com/oauth/token",
        proxy_enabled=False,
    )
    recorder: dict = {}

    with patch(
        "dataquery.transport.auth.aiohttp.ClientSession", return_value=_StubSessionCtx(recorder, _FAKE_TOKEN_PAYLOAD)
    ):
        mgr = TokenManager(cfg)
        await mgr._get_new_token()

    assert "proxy" not in recorder
    assert "proxy_auth" not in recorder


@pytest.mark.asyncio
async def test_refresh_token_applies_proxy():
    cfg = _proxy_cfg(with_auth=True)
    recorder: dict = {}

    mgr = TokenManager(cfg)
    mgr.current_token = OAuthToken(
        access_token="old",
        token_type="Bearer",
        expires_in=3600,
        refresh_token="r123",
    )

    with patch(
        "dataquery.transport.auth.aiohttp.ClientSession", return_value=_StubSessionCtx(recorder, _FAKE_TOKEN_PAYLOAD)
    ):
        await mgr._refresh_token()

    assert recorder["proxy"] == "http://proxy:8080"
    assert isinstance(recorder["proxy_auth"], BasicAuth)


# ---------------------------------------------------------------------------
# SSE client: the notification GET must also include proxy auth (previously
# it only passed the URL).
# ---------------------------------------------------------------------------


class _StubGetCtx:
    def __init__(self, recorder: dict):
        self._recorder = recorder

    async def __aenter__(self):
        resp = MagicMock()
        resp.status = 200

        async def _noop(*_a, **_kw):
            return None

        # Make content an empty async iterator so _parse_sse_stream exits cleanly.
        class _EmptyContent:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        resp.content = _EmptyContent()
        return resp

    async def __aexit__(self, *_a):
        return False


class _StubSSESessionCtx:
    def __init__(self, recorder: dict):
        self._recorder = recorder

    async def __aenter__(self):
        session = MagicMock()

        def get(url, **kwargs):
            self._recorder.clear()
            self._recorder.update(kwargs)
            self._recorder["_url"] = url
            return _StubGetCtx(self._recorder)

        session.get = get
        return session

    async def __aexit__(self, *_a):
        return False


def _sse_auth_manager():
    mgr = SimpleNamespace()
    mgr.get_headers = AsyncMock(return_value={"Authorization": "Bearer T"})
    return mgr


@pytest.mark.asyncio
async def test_sse_connect_includes_proxy_and_auth():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=False,
        bearer_token="T",
        proxy_enabled=True,
        proxy_url="http://proxy:8080",
        proxy_username="u",
        proxy_password="p",
    )
    recorder: dict = {}

    client = SSEClient(config=cfg, auth_manager=_sse_auth_manager())
    client._running = True

    with patch("dataquery.sse.client.aiohttp.ClientSession", return_value=_StubSSESessionCtx(recorder)):
        await client._connect_and_listen()

    assert recorder["proxy"] == "http://proxy:8080"
    assert isinstance(recorder["proxy_auth"], BasicAuth)
    assert recorder["proxy_auth"].login == "u"
