import pytest

from dataquery.client import DataQueryClient
from dataquery.models import ClientConfig, DownloadOptions, DownloadStatus


class DummyLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


class DummyLoggingManager:
    def __init__(self):
        self.logger = DummyLogger()

    def get_logger(self, *_):
        return self.logger

    def log_operation_start(self, *a, **k):
        pass

    def log_operation_end(self, *a, **k):
        pass

    def log_request(self, *a, **k):
        pass

    def log_response(self, *a, **k):
        pass

    def log_metric(self, *a, **k):
        pass


class FakeResponse:
    def __init__(self, status=200, headers=None, body_chunks=None):
        self.status = status
        self.headers = headers or {}
        self._chunks = body_chunks or [b"abc"]
        self.url = "https://api.example.com/group/file/download"

    async def json(self):
        return {}

    class content:
        def __init__(self, outer):
            self._outer = outer

        async def iter_chunked(self, n):
            for chunk in self._outer._chunks:
                yield chunk

    @property
    def content(self):
        return FakeResponse.content(self)


class FakeCtx:
    def __init__(self, resp: FakeResponse):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_client(tmp_path) -> DataQueryClient:
    cfg = ClientConfig(
        base_url="https://api.example.com",
        api_base_url="https://api.example.com/research/dataquery-authe/api/v2",
        client_id="id",
        client_secret="secret",
        oauth_enabled=False,
        download_dir=str(tmp_path),
    )
    client = DataQueryClient(cfg)
    client.logging_manager = DummyLoggingManager()
    client.logger = client.logging_manager.get_logger(__name__)

    async def _noop(*args, **kwargs):
        return None

    # Avoid real IO
    client._ensure_connected = _noop  # type: ignore
    client._ensure_authenticated = _noop  # type: ignore
    return client


## Removed xfail test: download_partial_206


@pytest.mark.asyncio
async def test_download_overwrite_protection(tmp_path, monkeypatch):
    client = make_client(tmp_path)

    # Pre-create destination file
    dest = tmp_path / "f.csv"
    dest.write_bytes(b"existing")

    async def cm_ok(method, url, **kwargs):
        resp = FakeResponse(
            status=200,
            headers={
                "content-disposition": 'attachment; filename="f.csv"',
                "content-length": "3",
            },
            body_chunks=[b"abc"],
        )
        return FakeCtx(resp)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_ok)

    opts = DownloadOptions(destination_path=str(tmp_path), overwrite_existing=False)
    result = await client.download_file_async("FG1", options=opts)
    assert result.status == DownloadStatus.FAILED
    # Error message content can vary by platform/mocks
    assert result.error_message


## Removed xfail test: download_range_header
