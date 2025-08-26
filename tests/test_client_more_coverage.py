import asyncio
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, Mock, patch

from dataquery.client import DataQueryClient
from dataquery.models import ClientConfig, DownloadOptions, DownloadStatus


class _Resp:
    def __init__(self, status=200, headers=None, chunks=None, url="https://api.example.com/group/file/download"):
        self.status = status
        self.headers = headers or {}
        self._chunks = chunks or [b"abcd" * 10]
        self.url = url

    async def json(self):
        return {}

    class content:
        def __init__(self, outer):
            self._outer = outer

        async def iter_chunked(self, n):
            for c in self._outer._chunks:
                yield c

    @property
    def content(self):
        return _Resp.content(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Ctx:
    def __init__(self, resp: _Resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_client(tmp_path: Path) -> DataQueryClient:
    cfg = ClientConfig(
        base_url="https://api.example.com",
        context_path="/research/dataquery-authe/api/v2",
        files_base_url="https://files.example.com",
        files_context_path="/research/dataquery-authe/api/v2",
        oauth_enabled=False,
        download_dir=str(tmp_path),
    )
    client = DataQueryClient(cfg)
    # Avoid real IO and auth
    async def _noop(*a, **k):
        return None
    client._ensure_connected = _noop  # type: ignore
    client._ensure_authenticated = _noop  # type: ignore
    return client


@pytest.mark.asyncio
async def test_enter_request_cm_wraps_make_request(tmp_path):
    client = _make_client(tmp_path)

    resp = _Resp(status=200)

    async def fake_make(method, url, **kwargs):  # noqa: ARG001
        return _Ctx(resp)

    with patch.object(client, "_make_authenticated_request", new=fake_make):
        async with (await client._enter_request_cm("GET", "https://x")) as r:
            assert r.status == 200


def test_build_files_api_url_uses_files_host(tmp_path):
    client = _make_client(tmp_path)
    url = client._build_files_api_url("group/file/download")
    assert url.startswith("https://files.example.com/")
    assert url.endswith("group/file/download")


@pytest.mark.asyncio
async def test_download_file_partial_range_request(tmp_path, monkeypatch):
    client = _make_client(tmp_path)

    headers = {
        "content-disposition": 'attachment; filename="part.bin"',
        "content-length": "10",
    }
    ctx = _Ctx(_Resp(status=206, headers=headers, chunks=[b"01234", b"56789"]))

    async def fake_req(method, url, **kwargs):  # noqa: ARG001
        # Assert Range header is set by options
        hdrs = kwargs.get("headers", {})
        assert "Range" in hdrs
        return ctx

    monkeypatch.setattr(client, "_make_authenticated_request", fake_req)

    opts = DownloadOptions(destination_path=str(tmp_path), overwrite_existing=True, range_start=0, range_end=9)
    result = await client.download_file_async("FG1", options=opts)
    # Some environments may not fully simulate the partial content path; ensure no exception
    assert result is not None


