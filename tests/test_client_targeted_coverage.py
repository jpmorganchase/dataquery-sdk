import asyncio
import time
from unittest.mock import patch, AsyncMock

import pytest

from dataquery.client import DataQueryClient
from dataquery.models import ClientConfig


def make_cfg(**over):
    base = dict(
        base_url="https://api.example.com",
        context_path="/api/v1",
        oauth_enabled=False,
        bearer_token="t",
        timeout=600.0,
        pool_connections=2,
        pool_maxsize=4,
    )
    base.update(over)
    return ClientConfig(**base)


def test_build_api_url_and_files_api_url_variants():
    cfg = make_cfg()
    c = DataQueryClient(cfg)
    # api url uses api_base_url (base_url + context_path)
    assert c._build_api_url("groups") == "https://api.example.com/api/v1/groups"
    # leading slash should be normalized
    assert c._build_api_url("/groups") == "https://api.example.com/api/v1/groups"

    # files url falls back to api_base when files_base not set
    assert c._build_files_api_url("group/file/download") == "https://api.example.com/api/v1/group/file/download"

    # when files_base/context configured, use them
    cfg2 = make_cfg(files_base_url="https://files.example.com", files_context_path="/files")
    c2 = DataQueryClient(cfg2)
    assert c2._build_files_api_url("group/file/download") == "https://files.example.com/files/group/file/download"


def test_build_api_url_length_validation():
    cfg = make_cfg()
    c = DataQueryClient(cfg)
    too_long = "a" * 2100
    with pytest.raises(Exception):
        c._build_api_url(too_long)


def test_response_cache_set_get_and_expiry(monkeypatch):
    cfg = make_cfg()
    c = DataQueryClient(cfg)

    key = c._get_cache_key("endpoint", {"b":2, "a":1})
    # order-insensitive key
    assert key == "endpoint?a=1&b=2"

    # set and get
    c._set_cache(key, {"ok": True})
    assert c._get_from_cache(key) == {"ok": True}

    # expire by mocking time
    now = time.time()
    monkeypatch.setattr('time.time', lambda: now + c._cache_ttl + 1)
    assert c._get_from_cache(key) is None

    # clear_cache should not fail
    c.clear_cache()


@pytest.mark.asyncio
async def test_connect_and_close_create_session_and_cleanup(monkeypatch):
    cfg = make_cfg(timeout=600.0, pool_connections=3, pool_maxsize=6)
    c = DataQueryClient(cfg)

    created = {}

    class DummySession:
        def __init__(self, *args, **kwargs):
            created['kwargs'] = kwargs
        async def close(self):
            created['closed'] = True

    class DummyConnector:
        def __init__(self, **kwargs):
            created['connector'] = kwargs

    # Patch aiohttp objects used inside connect
    monkeypatch.setattr('dataquery.client.aiohttp.ClientSession', DummySession)
    monkeypatch.setattr('dataquery.client.aiohttp.TCPConnector', DummyConnector)
    class DummyTimeout:
        def __init__(self, **kwargs):
            created['timeout'] = kwargs
    monkeypatch.setattr('dataquery.client.aiohttp.ClientTimeout', DummyTimeout)

    # Avoid monitoring side-effects
    with patch.object(c.pool_monitor, 'start_monitoring', return_value=None), \
         patch.object(c.pool_monitor, 'stop_monitoring', return_value=None):
        await c.connect()
        # verify timeout fields derived from config
        assert created['timeout']['total'] == 600.0
        assert created['timeout']['connect'] == 300.0
        assert created['timeout']['sock_read'] == 300.0
        # verify connector config uses pool sizes and keepalive
        assert created['connector']['limit'] == 6
        assert created['connector']['limit_per_host'] == 3
        assert created['connector']['keepalive_timeout'] == 300

        await c.close()
        assert created.get('closed') is True


@pytest.mark.asyncio
async def test_async_context_manager_uses_connect_and_close(monkeypatch):
    cfg = make_cfg()
    c = DataQueryClient(cfg)
    with patch.object(c, 'connect', new=AsyncMock(return_value=None)) as m_conn, \
         patch.object(c, 'close', new=AsyncMock(return_value=None)) as m_close:
        async with c as ctx:
            assert ctx is c
        m_conn.assert_called_once()
        m_close.assert_called_once()


class DummyResp:
    def __init__(self, status=200, headers=None, url="https://api.example.com/x", text_body=""):
        self.status = status
        self.headers = headers or {}
        self.url = url
        self._text = text_body
    async def text(self):
        return self._text


@pytest.mark.asyncio
async def test_handle_response_success_updates_rate_limiter(monkeypatch):
    cfg = make_cfg()
    c = DataQueryClient(cfg)
    called = {"ok": False}
    monkeypatch.setattr(c.rate_limiter, 'handle_successful_request', lambda: called.__setitem__('ok', True))
    await c._handle_response(DummyResp(status=200, headers={}))
    assert called['ok'] is True


@pytest.mark.asyncio
async def test_handle_response_4xx_and_5xx_and_401_403_404(monkeypatch):
    cfg = make_cfg()
    c = DataQueryClient(cfg)

    # 401
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=401))
    # 403
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=403))
    # 404
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=404))
    # 500
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=500))
    # 400 generic client error
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=400))


@pytest.mark.asyncio
async def test_handle_response_429_invokes_rate_limit_handler(monkeypatch):
    cfg = make_cfg()
    c = DataQueryClient(cfg)
    called = {"rate": False}
    def fake_handle(headers):
        called['rate'] = True
    monkeypatch.setattr(c.rate_limiter, 'handle_rate_limit_response', fake_handle)
    with pytest.raises(Exception):
        await c._handle_response(DummyResp(status=429, headers={"Retry-After": "1"}))
    assert called['rate'] is True


@pytest.mark.asyncio
async def test_enter_request_cm_accepts_direct_cm_and_coroutine_cm():
    cfg = make_cfg()
    c = DataQueryClient(cfg)

    class AsyncCM:
        async def __aenter__(self):
            return "ok"
        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def coro_returning_cm():
        return AsyncCM()

    # Case 1: direct CM
    c._make_authenticated_request = lambda *a, **k: AsyncCM()
    cm1 = await c._enter_request_cm('GET', 'u')
    async with cm1 as v1:
        assert v1 == "ok"

    # Case 2: coroutine returning CM
    c._make_authenticated_request = lambda *a, **k: coro_returning_cm()
    cm2 = await c._enter_request_cm('GET', 'u')
    async with cm2 as v2:
        assert v2 == "ok"
