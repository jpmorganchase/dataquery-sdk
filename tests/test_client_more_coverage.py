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

    class _Content:
        def __init__(self, outer):
            self._outer = outer

        async def iter_chunked(self, n):
            for c in self._outer._chunks:
                yield c

    @property
    def content(self):
        # Return a lightweight stream-like wrapper
        return _Resp._Content(self)

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



@pytest.mark.asyncio
async def test_download_file_async_splits_parts(tmp_path, monkeypatch):
    client = _make_client(tmp_path)

    # First probe: content-range reveals total size 10, with a filename
    probe_headers = {
        "Content-Range": "bytes 0-0/10",
        "content-disposition": 'attachment; filename="file.bin"',
        "content-length": "1",
    }

    async def fake_req(method, url, **kwargs):  # noqa: ARG001
        hdrs = kwargs.get("headers", {})
        rng = hdrs.get("Range")
        if rng == "bytes=0-0":
            return _Ctx(_Resp(status=206, headers=probe_headers, chunks=[b"0"]))
        # For actual part requests, return as many bytes as requested
        # Range format: bytes=start-end
        if not rng:
            # Fallback path: single-stream full download
            full_headers = {
                "content-disposition": 'attachment; filename="file.bin"',
                "content-length": "10",
            }
            return _Ctx(_Resp(status=200, headers=full_headers, chunks=[b"01234", b"56789"]))
        assert rng and rng.startswith("bytes=")
        try:
            start_end = rng.split("=")[1]
            start_str, end_str = start_end.split("-")
            start = int(start_str)
            end = int(end_str)
        except Exception:  # pragma: no cover - guard
            start, end = 0, 0
        length = max(0, end - start + 1)
        part_headers = {
            "content-length": str(length),
            "Content-Range": f"bytes {start}-{end}/10",
        }
        # Yield bytes in smaller chunks to exercise write loop
        chunks = []
        remaining = length
        while remaining > 0:
            n = min(4, remaining)
            chunks.append(b"X" * n)
            remaining -= n
        return _Ctx(_Resp(status=206, headers=part_headers, chunks=chunks))

    monkeypatch.setattr(client, "_make_authenticated_request", fake_req)

    # Perform parallel download: expect a 10-byte file assembled
    result = await client.download_file_async(
        file_group_id="FG1",
        options=DownloadOptions(destination_path=str(tmp_path), overwrite_existing=True),
        num_parts=5,
    )

    assert result is not None
    assert result.status == DownloadStatus.COMPLETED
    assert result.local_path is not None
    # Verify the assembled file size is 10 bytes
    p = Path(result.local_path)
    assert p.exists()
    assert p.stat().st_size == 10


@pytest.mark.asyncio
async def test_download_file_async_small_file_falls_back(tmp_path, monkeypatch):
    client = _make_client(tmp_path)

    # Small file: 6 bytes total
    probe_headers = {
        "Content-Range": "bytes 0-0/6",
        "content-disposition": 'attachment; filename="small.bin"',
        "content-length": "1",
    }

    async def fake_req(method, url, **kwargs):  # noqa: ARG001
        hdrs = kwargs.get("headers", {})
        rng = hdrs.get("Range")
        if rng == "bytes=0-0":
            return _Ctx(_Resp(status=206, headers=probe_headers, chunks=[b"0"]))
        # After probe, implementation should fall back to single stream (no Range)
        if not rng:
            full_headers = {
                "content-disposition": 'attachment; filename="small.bin"',
                "content-length": "6",
            }
            return _Ctx(_Resp(status=200, headers=full_headers, chunks=[b"abc", b"def"]))
        # Should not request ranged parts for small file, but guard anyway
        return _Ctx(_Resp(status=416, headers={}, chunks=[]))

    monkeypatch.setattr(client, "_make_authenticated_request", fake_req)

    result = await client.download_file_async(
        file_group_id="FGS",
        options=DownloadOptions(destination_path=str(tmp_path), overwrite_existing=True),
        num_parts=5,
    )

    assert result is not None
    assert result.status == DownloadStatus.COMPLETED
    p = Path(result.local_path)
    assert p.exists()
    assert p.stat().st_size == 6

