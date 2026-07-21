"""Coverage for the stdio <-> Streamable HTTP MCP proxy.

Runs the proxy against an in-process aiohttp server that plays the remote MCP
endpoint (plus its OAuth token endpoint), so no real network is needed.
"""

import json

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer

from dataquery.mcp_proxy import StreamableHttpProxy
from dataquery.transport.auth import OAuthManager
from dataquery.types.models import ClientConfig

SESSION_ID = "sess-1"
PROTOCOL_VERSION = "2025-06-18"


def _make_app(state: dict) -> web.Application:
    """Fake remote MCP server + OAuth token endpoint."""

    async def token(request: web.Request) -> web.Response:
        state["token_calls"] += 1
        return web.json_response(
            {
                "access_token": f"tok{state['token_calls']}",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )

    async def mcp_post(request: web.Request) -> web.StreamResponse:
        auth = request.headers.get("Authorization", "")
        state["seen_auth"].append(auth)
        if auth in state["revoked"]:
            return web.Response(status=401)

        message = await request.json()
        state["seen_session"].append(request.headers.get("Mcp-Session-Id"))

        if state.get("force_status"):
            return web.Response(status=state["force_status"])

        if message.get("id") is None:
            return web.Response(status=202)  # notification

        if message.get("method") == "initialize":
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}},
                },
                headers={"Mcp-Session-Id": SESSION_ID},
            )

        if message.get("method") == "tools/list":
            resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await resp.prepare(request)
            note = {"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progress": 1}}
            reply = {"jsonrpc": "2.0", "id": message["id"], "result": {"tools": []}}
            await resp.write(f"data: {json.dumps(note)}\n\n".encode())
            await resp.write(f"data: {json.dumps(reply)}\n\n".encode())
            await resp.write_eof()
            return resp

        return web.json_response({"jsonrpc": "2.0", "id": message["id"], "result": {"echo": message.get("method")}})

    async def mcp_get(request: web.Request) -> web.Response:
        return web.Response(status=405)

    async def mcp_delete(request: web.Request) -> web.Response:
        state["deleted"] = True
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/oauth/token", token)
    app.router.add_post("/mcp", mcp_post)
    app.router.add_get("/mcp", mcp_get)
    app.router.add_delete("/mcp", mcp_delete)
    return app


@pytest_asyncio.fixture
async def rig(tmp_path):
    """(proxy, state, emitted) wired to an in-process fake MCP server."""
    state = {
        "token_calls": 0,
        "revoked": set(),
        "seen_auth": [],
        "seen_session": [],
        "deleted": False,
    }
    server = TestServer(_make_app(state))
    await server.start_server()
    base_url = str(server.make_url("")).rstrip("/")

    config = ClientConfig(
        base_url=base_url,
        oauth_token_url=f"{base_url}/oauth/token",
        client_id="cid",
        client_secret="secret",
        download_dir=str(tmp_path),
    )
    oauth = OAuthManager(config)
    emitted: list = []
    async with aiohttp.ClientSession() as session:
        proxy = StreamableHttpProxy(f"{base_url}/mcp", oauth, session, emitted.append)
        yield proxy, state, emitted
    await server.close()


def _initialize_msg(msg_id: int = 1) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "method": "initialize",
        "params": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}},
    }


@pytest.mark.asyncio
async def test_initialize_captures_session_and_protocol(rig):
    proxy, state, emitted = rig
    await proxy.handle_message(_initialize_msg())

    assert len(emitted) == 1
    assert emitted[0]["result"]["protocolVersion"] == PROTOCOL_VERSION
    assert proxy.session_id == SESSION_ID
    assert proxy.protocol_version == PROTOCOL_VERSION
    assert state["seen_auth"] == ["Bearer tok1"]


@pytest.mark.asyncio
async def test_sse_response_emits_all_events_with_session_header(rig):
    proxy, state, emitted = rig
    await proxy.handle_message(_initialize_msg())
    await proxy.handle_message({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})

    assert [m.get("method", "response") for m in emitted[1:]] == [
        "notifications/progress",
        "response",
    ]
    assert emitted[-1]["id"] == 2
    assert state["seen_session"][-1] == SESSION_ID  # session echoed after init


@pytest.mark.asyncio
async def test_notification_emits_nothing(rig):
    proxy, _state, emitted = rig
    await proxy.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert emitted == []


@pytest.mark.asyncio
async def test_401_refreshes_token_and_retries(rig):
    proxy, state, emitted = rig
    await proxy.handle_message(_initialize_msg())

    state["revoked"].add("Bearer tok1")  # simulate expiry server-side
    await proxy.handle_message({"jsonrpc": "2.0", "id": 2, "method": "ping"})

    assert state["token_calls"] == 2  # initial token + forced refresh
    assert state["seen_auth"][-1] == "Bearer tok2"
    assert emitted[-1] == {"jsonrpc": "2.0", "id": 2, "result": {"echo": "ping"}}


@pytest.mark.asyncio
async def test_server_error_becomes_jsonrpc_error(rig):
    proxy, state, emitted = rig
    await proxy.handle_message(_initialize_msg())

    state["force_status"] = 500
    await proxy.handle_message({"jsonrpc": "2.0", "id": 7, "method": "ping"})

    err = emitted[-1]
    assert err["id"] == 7
    assert err["error"]["code"] == -32001
    assert "500" in err["error"]["message"]


@pytest.mark.asyncio
async def test_expired_session_404_surfaces_error(rig):
    proxy, state, emitted = rig
    await proxy.handle_message(_initialize_msg())

    state["force_status"] = 404
    await proxy.handle_message({"jsonrpc": "2.0", "id": 8, "method": "ping"})

    err = emitted[-1]
    assert err["id"] == 8
    assert "session expired" in err["error"]["message"].lower()


@pytest.mark.asyncio
async def test_get_stream_not_offered_gives_up(rig):
    proxy, _state, _emitted = rig
    await proxy.listen_get_stream()  # server answers 405; must return, not loop


@pytest.mark.asyncio
async def test_close_deletes_session(rig):
    proxy, state, _emitted = rig
    await proxy.handle_message(_initialize_msg())
    await proxy.close()
    assert state["deleted"] is True


@pytest.mark.asyncio
async def test_close_without_session_is_noop(rig):
    proxy, state, _emitted = rig
    await proxy.close()
    assert state["deleted"] is False
