"""Local stdio <-> Streamable HTTP proxy for a remote MCP server."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional
from urllib.parse import urlsplit

import aiohttp
import structlog

from .config import EnvConfig
from .transport.auth import OAuthManager
from .types.exceptions import AuthenticationError, ConfigurationError

logger = structlog.get_logger(__name__)

_SESSION_HEADER = "Mcp-Session-Id"
_PROTOCOL_HEADER = "MCP-Protocol-Version"

_GET_BACKOFF_INITIAL = 1.0
_GET_BACKOFF_MAX = 30.0


class StreamableHttpProxy:
    """Forward JSON-RPC messages between a stdio client and a remote endpoint."""

    def __init__(
        self,
        url: str,
        oauth: OAuthManager,
        session: aiohttp.ClientSession,
        emit: Callable[[Dict[str, Any]], None],
    ):
        self.url = url
        self.oauth = oauth
        self.session = session
        self.emit = emit
        self.session_id: Optional[str] = None
        self.protocol_version: Optional[str] = None

    async def _headers(self) -> Dict[str, str]:
        headers = await self.oauth.get_headers()
        headers["Accept"] = "application/json, text/event-stream"
        if self.session_id:
            headers[_SESSION_HEADER] = self.session_id
        if self.protocol_version:
            headers[_PROTOCOL_HEADER] = self.protocol_version
        return headers

    async def handle_message(self, message: Dict[str, Any]) -> None:
        """POST one client message to the endpoint and emit whatever comes back."""
        for attempt in (1, 2):
            headers = await self._headers()
            headers["Content-Type"] = "application/json"
            try:
                resp = await self.session.post(self.url, json=message, headers=headers)
            except aiohttp.ClientError as exc:
                logger.error("MCP endpoint unreachable", url=self.url, error=str(exc))
                self._emit_transport_error(message, f"MCP endpoint unreachable: {exc}")
                return

            async with resp:
                if resp.status == 401 and attempt == 1:
                    # Token likely expired between refresh checks; force a
                    # fresh one and retry once.
                    logger.info("401 from MCP endpoint, refreshing token")
                    await self.oauth.force_refresh()
                    continue

                if resp.status in (202, 204):
                    return

                if resp.status == 404 and self.session_id:
                    # Session expired server-side. A transparent re-init is
                    # not possible mid-conversation; surface it so the MCP
                    # client restarts the proxy and re-initializes.
                    logger.error("MCP session expired (404), restart the connection")
                    self._emit_transport_error(message, "MCP session expired; restart the connection")
                    return

                if resp.status >= 400:
                    body = (await resp.text())[:500]
                    logger.error("MCP endpoint error", status=resp.status, body=body)
                    self._emit_transport_error(message, f"MCP endpoint returned {resp.status}")
                    return

                self._capture_session(resp)

                content_type = resp.headers.get("Content-Type", "")
                if content_type.startswith("text/event-stream"):
                    await self._pump_sse(resp)
                else:
                    payload = await resp.json(content_type=None)
                    if payload is not None:
                        self._emit_message(payload)
                return

    def _capture_session(self, resp: aiohttp.ClientResponse) -> None:
        session_id = resp.headers.get(_SESSION_HEADER)
        if session_id and session_id != self.session_id:
            self.session_id = session_id
            logger.info("MCP session established")

    def _emit_message(self, payload: Any) -> None:
        if isinstance(payload, dict):
            result = payload.get("result")
            if isinstance(result, dict) and result.get("protocolVersion"):
                self.protocol_version = result["protocolVersion"]
        self.emit(payload)

    def _emit_transport_error(self, request: Dict[str, Any], text: str) -> None:
        """Answer a request the endpoint never processed with a JSON-RPC error."""
        request_id = request.get("id")
        if request_id is None:
            return
        self.emit(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32001, "message": text},
            }
        )

    async def _pump_sse(self, resp: aiohttp.ClientResponse) -> None:
        """Emit each ``data:`` event of an SSE stream as a JSON-RPC message."""
        data_lines: list[str] = []
        async for raw in resp.content:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif line == "" and data_lines:
                data = "\n".join(data_lines)
                data_lines = []
                try:
                    self._emit_message(json.loads(data))
                except json.JSONDecodeError:
                    logger.warning("Skipping non-JSON SSE event", data=data[:200])

    async def listen_get_stream(self) -> None:
        """Pump the optional server-initiated GET stream, reconnecting politely."""
        backoff = _GET_BACKOFF_INITIAL
        while True:
            headers = await self._headers()
            try:
                async with self.session.get(self.url, headers=headers) as resp:
                    if resp.status in (404, 405, 501):
                        logger.debug("Server offers no GET stream", status=resp.status)
                        return
                    if resp.status >= 400:
                        logger.debug("GET stream rejected", status=resp.status)
                        return
                    backoff = _GET_BACKOFF_INITIAL
                    await self._pump_sse(resp)
            except aiohttp.ClientError as exc:
                logger.debug("GET stream dropped", error=str(exc))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _GET_BACKOFF_MAX)

    async def close(self) -> None:
        """Best-effort session teardown (spec: DELETE with the session id)."""
        if not self.session_id:
            return
        try:
            headers = await self._headers()
            async with self.session.delete(self.url, headers=headers) as resp:
                logger.debug("MCP session closed", status=resp.status)
        except Exception as exc:
            logger.debug("MCP session close failed", error=str(exc))


def _configure_stderr_logging() -> None:
    """Route all structlog output to stderr; stdout is the JSON-RPC channel."""
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _build_oauth_manager(url: str, env_file: Optional[Path]) -> OAuthManager:
    """Build the OAuth manager from env config."""
    if env_file is not None:
        EnvConfig.load_env_file(env_file)
    if not EnvConfig.get_env_var("BASE_URL"):
        parts = urlsplit(url)
        os.environ[f"{EnvConfig.PREFIX}BASE_URL"] = f"{parts.scheme}://{parts.netloc}"
    config = EnvConfig.create_client_config()
    oauth = OAuthManager(config)
    if not oauth.is_authenticated():
        raise ConfigurationError(
            "No credentials configured: set DATAQUERY_CLIENT_ID and DATAQUERY_CLIENT_SECRET (or DATAQUERY_BEARER_TOKEN)"
        )
    return oauth


async def _read_stdin_lines() -> "asyncio.StreamReader":
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    return reader


def _emit_stdout(message: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


async def run_mcp_proxy(url: str, env_file: Optional[Path] = None) -> int:
    """Run the stdio <-> Streamable HTTP proxy until stdin closes."""
    _configure_stderr_logging()
    try:
        oauth = _build_oauth_manager(url, env_file)
        await oauth.authenticate()
    except (ConfigurationError, AuthenticationError) as exc:
        print(f"dataquery mcp connect: {exc}", file=sys.stderr)
        return 1

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        proxy = StreamableHttpProxy(url, oauth, session, _emit_stdout)
        logger.info("MCP proxy connected", url=url)

        get_task: Optional[asyncio.Task] = None
        pending: set[asyncio.Task] = set()

        def _track(coro: Awaitable[None]) -> None:
            task = asyncio.ensure_future(coro)
            pending.add(task)
            task.add_done_callback(pending.discard)

        try:
            reader = await _read_stdin_lines()
            while True:
                line = await reader.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON-RPC line", line=text[:200])
                    continue
                # Each message runs as its own task so a long streamed
                # response doesn't block the next client message.
                _track(proxy.handle_message(message))
                if get_task is None and message.get("method") == "notifications/initialized":
                    get_task = asyncio.ensure_future(proxy.listen_get_stream())
        finally:
            if get_task is not None:
                get_task.cancel()
            for task in list(pending):
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await proxy.close()

    return 0
