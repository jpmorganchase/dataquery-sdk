"""Custom headers on the wire, against an in-process aiohttp server.

Covers what the mocked unit tests can't: aiohttp's own merge of session
defaults with per-request headers, the headers of the SSE stream, and two
clients in one process each sending only their own headers.
"""

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer

from dataquery import constants as C
from dataquery.core.client import DataQueryClient
from dataquery.sse.client import SSEClient
from dataquery.transport.auth import OAuthManager
from dataquery.types.models import ClientConfig

CONTEXT_PATH = "/api/v2"


@pytest_asyncio.fixture
async def server():
    """(base_url, seen): the headers of every request, in arrival order."""
    seen: list = []

    async def groups(request: web.Request) -> web.Response:
        seen.append(request.headers)
        return web.json_response({"groups": []})

    async def notification(request: web.Request) -> web.StreamResponse:
        seen.append(request.headers)
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"id: 1\ndata: {}\n\n")
        return response

    app = web.Application()
    app.router.add_get(f"{CONTEXT_PATH}/{C.API_GROUPS}", groups)
    app.router.add_get(f"{CONTEXT_PATH}{C.SSE_NOTIFICATION_PATH}", notification)
    test_server = TestServer(app)
    await test_server.start_server()
    yield str(test_server.make_url("")).rstrip("/"), seen
    await test_server.close()


def _config(base_url: str, tmp_path, custom_headers=None) -> ClientConfig:
    return ClientConfig(
        base_url=base_url,
        context_path=CONTEXT_PATH,
        files_base_url=base_url,
        files_context_path=CONTEXT_PATH,
        oauth_enabled=False,
        bearer_token="T",
        download_dir=str(tmp_path),
        custom_headers=custom_headers or {},
    )


@pytest.mark.asyncio
async def test_api_request_carries_custom_headers(server, tmp_path):
    base_url, seen = server
    config = _config(
        base_url,
        tmp_path,
        {"X-User-Agent": "MyApp/1.0", "X-Team": "rates", "user-agent": "MyApp-Agent/2.0"},
    )
    async with DataQueryClient(config) as client:
        await client.list_groups_async()

    (headers,) = seen
    assert headers["X-Team"] == "rates"
    assert headers["X-User-Agent"] == "MyApp/1.0"
    # The custom user-agent replaced the SDK's rather than joining it.
    assert headers.getall("User-Agent") == ["MyApp-Agent/2.0"]
    # Auth is still the SDK's own, added per request.
    assert headers["Authorization"] == "Bearer T"


@pytest.mark.asyncio
async def test_sse_stream_carries_custom_headers_under_its_own(server, tmp_path):
    base_url, seen = server
    config = _config(
        base_url,
        tmp_path,
        {"X-User-Agent": "MyApp/1.0", "X-Team": "rates", "accept": "application/json"},
    )
    client = SSEClient(config, OAuthManager(config))
    client._running = True  # the stream is only read while the client is running
    await client._connect_and_listen()

    (headers,) = seen
    assert headers["X-Team"] == "rates"
    assert headers["X-User-Agent"] == "MyApp/1.0"
    assert headers.getall("Accept") == ["text/event-stream"]
    assert headers["Authorization"] == "Bearer T"
    assert client.last_event_id == "1"


@pytest.mark.asyncio
async def test_each_client_sends_only_its_own_headers(server, tmp_path):
    base_url, seen = server
    risk = DataQueryClient(_config(base_url, tmp_path, {"X-User-Agent": "RiskEngine/2.1", "X-Team": "risk"}))
    plain = DataQueryClient(_config(base_url, tmp_path))
    async with risk, plain:
        await risk.list_groups_async()
        await plain.list_groups_async()

    risk_headers, plain_headers = seen
    assert risk_headers["X-User-Agent"] == "RiskEngine/2.1"
    assert risk_headers["X-Team"] == "risk"
    assert "X-User-Agent" not in plain_headers
    assert "X-Team" not in plain_headers
