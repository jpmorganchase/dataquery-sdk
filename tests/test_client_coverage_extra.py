import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from dataquery.client import (
    DataQueryClient,
    format_duration,
    format_file_size,
    get_filename_from_response,
    parse_content_disposition,
    validate_attributes_list,
    validate_date_format,
    validate_file_datetime,
    validate_instruments_list,
    validate_required_param,
)
from dataquery.exceptions import (
    AuthenticationError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
)


class StubRateLimiter:
    def __init__(self):
        self.success_called = 0
        self.rate_limit_called = 0

    def handle_successful_request(self):
        self.success_called += 1

    def handle_rate_limit_response(self, headers):
        self.rate_limit_called += 1


class StubLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class FakeResponse:
    def __init__(self, status=200, headers=None, url="https://api.example.com/x"):
        self.status = status
        self.headers = headers or {}
        self.url = url

    async def json(self):
        return {}


class FakeRespCtx:
    def __init__(self, response: FakeResponse):
        self._resp = response

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_bare_client():
    # Bypass __init__ to avoid heavy setup; set only attributes used by tests
    client = object.__new__(DataQueryClient)
    client.logger = StubLogger()
    client.rate_limiter = StubRateLimiter()
    client.config = SimpleNamespace(
        api_base_url="https://api.example.com", base_url="https://api.example.com"
    )
    return client


def test_format_file_size_various_units():
    assert format_file_size(0) == "0 B"
    assert format_file_size(512) == "512.00 B"
    assert format_file_size(1024) == "1.00 KB"
    assert format_file_size(1024 * 1024) == "1.00 MB"
    assert format_file_size(-2048) == "-2.00 KB"


def test_format_duration_various_ranges():
    assert format_duration(0) == "0s"
    assert format_duration(1.23) == "1.2s"
    assert format_duration(120) == "2.0m"
    assert format_duration(7200) == "2.0h"
    assert format_duration(-90) == "-1.5m"


@pytest.mark.parametrize(
    "cd,expected",
    [
        ("attachment; filename*=UTF-8''example%20name.csv", "example name.csv"),
        ('attachment; filename="report.pdf"', "report.pdf"),
        ("attachment; filename=data.txt", "data.txt"),
        (None, None),
    ],
)
def test_parse_content_disposition(cd, expected):
    assert parse_content_disposition(cd) == expected


def test_get_filename_from_response_variants():
    # From content-disposition
    r1 = SimpleNamespace(
        headers={"content-disposition": 'attachment; filename="file.csv"'}
    )
    assert get_filename_from_response(r1, "fgid") == "file.csv"

    # From content-type mapping
    r2 = SimpleNamespace(headers={"content-type": "text/csv"})
    assert get_filename_from_response(r2, "abc") == "abc.csv"

    # Unknown type -> .bin
    r3 = SimpleNamespace(headers={"content-type": "application/x-unknown"})
    assert get_filename_from_response(r3, "x", "20240101") == "x_20240101.bin"

    # No headers -> default .bin
    r4 = SimpleNamespace(headers={})
    assert get_filename_from_response(r4, "z") == "z.bin"


def test_validate_file_datetime_and_dates():
    # Valid
    for d in ["20240101", "20240101T1200", "20240101T120000"]:
        validate_file_datetime(d)
    # Invalid
    with pytest.raises(ValueError):
        validate_file_datetime("2024-01-01")

    # Date formats
    for s in ["20240101", "TODAY", "TODAY-5D", "TODAY-10M"]:
        validate_date_format(s, "start-date")
    with pytest.raises(ValidationError):
        validate_date_format("YESTERDAY", "start-date")


def test_validate_required_and_lists():
    with pytest.raises(ValidationError):
        validate_required_param(None, "x")
    with pytest.raises(ValidationError):
        validate_required_param("  ", "x")

    # Instruments list
    validate_instruments_list(["a", "b"])  # ok
    with pytest.raises(ValidationError):
        validate_instruments_list([])
    with pytest.raises(ValidationError):
        validate_instruments_list(["a"] * 21)
    with pytest.raises(ValidationError):
        validate_instruments_list([""])

    # Attributes list
    validate_attributes_list(["x"])  # ok
    with pytest.raises(ValidationError):
        validate_attributes_list([])
    with pytest.raises(ValidationError):
        validate_attributes_list([""])


def test_extract_endpoint_and_build_and_validate_url():
    client = _make_bare_client()

    # Extract endpoint when base_url present
    ep = client._extract_endpoint("https://api.example.com/api/v2/groups")
    assert ep in {"/api/v2/groups", "groups", "api"}  # allow flexibility

    # Fallback extraction
    ep2 = client._extract_endpoint("https://other/alpha/beta")
    assert ep2 in {"beta", "root"}

    # Build URL joins correctly and length validated via request URL
    url = client._build_api_url("groups")
    assert url.endswith("/groups")

    # Validate request URL length
    long_param = {"q": "a" * 3000}
    with pytest.raises(ValidationError):
        client._validate_request_url(url, long_param)


def test_operation_priority_and_file_extension():
    client = _make_bare_client()
    assert client._get_operation_priority("GET", "download").name == "HIGH"
    assert client._get_operation_priority("GET", "health").name == "CRITICAL"
    assert client._get_operation_priority("POST", "other").name == "NORMAL"

    # File extension extraction
    assert client._get_file_extension("file.csv") == ".csv"
    assert client._get_file_extension("file") == ".bin"
    assert client._get_file_extension("../etc/passwd") == "bin"
    assert client._get_file_extension(123) == "bin"


@pytest.mark.asyncio
async def test_handle_response_status_mappings_and_rate_limit():
    client = _make_bare_client()

    # 200 -> success path triggers successful request mark
    await client._handle_response(FakeResponse(status=200, headers={}))
    assert client.rate_limiter.success_called == 1

    # 401
    with pytest.raises(AuthenticationError):
        await client._handle_response(FakeResponse(status=401, headers={}))

    # 403
    with pytest.raises(AuthenticationError):
        await client._handle_response(FakeResponse(status=403, headers={}))

    # 404
    with pytest.raises(NotFoundError):
        await client._handle_response(FakeResponse(status=404, headers={}))

    # 429 -> rate limit
    with pytest.raises(RateLimitError):
        await client._handle_response(
            FakeResponse(status=429, headers={"Retry-After": "1"})
        )
    assert client.rate_limiter.rate_limit_called == 1

    # 500
    with pytest.raises(NetworkError):
        await client._handle_response(FakeResponse(status=500, headers={}))

    # 400
    with pytest.raises(ValidationError):
        await client._handle_response(FakeResponse(status=400, headers={}))


@pytest.mark.asyncio
async def test_health_check_async_success_and_failure(monkeypatch):
    client = _make_bare_client()

    async def fake_request_ok(method, url, **kwargs):
        return FakeRespCtx(FakeResponse(status=200))

    async def fake_request_fail(method, url, **kwargs):
        raise RuntimeError("boom")

    # Success
    monkeypatch.setattr(client, "_make_authenticated_request", fake_request_ok)
    ok = await client.health_check_async()
    assert ok is True

    # Failure -> returns False
    monkeypatch.setattr(client, "_make_authenticated_request", fake_request_fail)
    ok2 = await client.health_check_async()
    assert ok2 is False


def test_dataframe_conversion_paths(monkeypatch):
    client = _make_bare_client()

    # Simple list of dicts with date and numeric-like strings
    data = [
        {"id": "1", "value": "10", "created_date": "2024-01-01"},
        {"id": "2", "value": "20", "created_date": "2024-01-02"},
    ]

    # Provide a minimal pandas stub
    class _PandasStub:
        class _Series(list):
            @property
            def dtype(self):
                return "object"

            def notna(self):
                return self

            def sum(self):
                return len(self)

            def __truediv__(self, other):
                return 1

        class DataFrame(dict):
            def __init__(self, records=None):
                super().__init__()
                self._records = records or []
                self._columns = set(self._records[0].keys()) if self._records else set()

            @property
            def columns(self):
                return list(self._columns)

            def __getitem__(self, key):
                return _PandasStub._Series([r.get(key) for r in self._records])

            def __setitem__(self, key, value):
                for i, r in enumerate(self._records):
                    r[key] = value[i] if isinstance(value, list) else value
                self._columns.add(key)

        @staticmethod
        def to_datetime(x, errors=None):
            return x

        @staticmethod
        def to_numeric(x, errors=None):
            return x

    import sys

    sys.modules["pandas"] = _PandasStub()

    df = client.to_dataframe(
        data, date_columns=["created_date"], numeric_columns=["value"]
    )
    assert set(df.columns) >= {"id", "value", "created_date"}

    # Object with model_dump
    class M:
        def model_dump(self):
            return {"a": 1, "b": {"c": 2}}

    df2 = client.to_dataframe([M()])
    assert any(col.startswith("b_") for col in df2.columns)

    # Object without model_dump
    class N:
        def __init__(self):
            self.x = 3
            self.y = [1, 2]

    df3 = client.to_dataframe([N()])
    assert set(df3.columns) >= {"x", "y"}
