import pytest

from dataquery.client import DataQueryClient
from dataquery.exceptions import AuthenticationError, FileNotFoundError, ValidationError
from dataquery.models import ClientConfig


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


class DummyRetryManager:
    async def execute_with_retry(self, func, *args, **kwargs):
        return await func(*args, **kwargs)

    def get_stats(self):
        return {}


class DummyRateLimiter:
    async def shutdown(self):
        pass

    def get_stats(self):
        return {}


class DummyPoolMonitor:
    def __init__(self):
        self.started = False

    def start_monitoring(self, *_):
        self.started = True

    def stop_monitoring(self):
        self.started = False

    def get_stats(self):
        return {"connections": {"idle": 0}}

    def get_pool_summary(self):
        return {"connections": {"idle": 0}}


class DummyAuth:
    def __init__(self, ok=True):
        self.ok = ok

    def is_authenticated(self):
        return self.ok

    async def authenticate(self):
        if not self.ok:
            raise RuntimeError("auth fail")

    async def get_headers(self):
        return {"Authorization": "Bearer X"}

    def get_auth_info(self):
        return {"mode": "dummy"}


class DummySession:
    closed = False

    async def request(self, method, url, **kwargs):
        return DummyResponse()

    def close(self):
        self.closed = True


class DummyResponse:
    status = 200
    headers = {"content-length": "0"}
    url = "https://api.example.com/x"

    async def json(self):
        return {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    class content:
        @staticmethod
        async def iter_chunked(n):
            for _ in range(2):
                yield b"x" * n


class FakeCtx:
    def __init__(self, resp: DummyResponse):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_client(monkeypatch, auth_ok=True):
    cfg = ClientConfig(
        base_url="https://api.example.com",
        api_base_url="https://api.example.com/research/dataquery-authe/api/v2",
        client_id="id",
        client_secret="secret",
        oauth_enabled=False,
    )
    client = DataQueryClient(cfg)
    # Replace heavy components with dummies
    client.logging_manager = DummyLoggingManager()
    client.logger = client.logging_manager.get_logger(__name__)
    client.retry_manager = DummyRetryManager()
    client.rate_limiter = DummyRateLimiter()
    client.pool_monitor = DummyPoolMonitor()
    client.auth_manager = DummyAuth(ok=auth_ok)
    client.session = DummySession()

    # Don’t actually connect
    async def _noop():
        return None

    monkeypatch.setattr(client, "_ensure_connected", _noop)

    # Bypass response handler by default (covered elsewhere)
    async def ok_handle(resp):
        return None

    monkeypatch.setattr(client, "_handle_response", ok_handle)
    return client


@pytest.mark.asyncio
async def test_groups_apis(monkeypatch):
    client = make_client(monkeypatch)

    # list_groups_async
    async def req_groups(method, url, **kwargs):
        data = {"groups": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_groups)
    groups = await client.list_groups_async(limit=5)
    assert isinstance(groups, list)

    # list_all_groups_async with two pages (relative next link)
    calls = {"n": 0}

    async def req_groups_paged(method, url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            data = {
                "groups": [
                    {
                        "group-id": "G1",
                        "group-name": "Name",
                        "description": "d",
                        "taxonomy-node1": "t1",
                        "taxonomy-node2": "t2",
                        "taxonomy-node3": "t3",
                        "taxonomy-node4": "t4",
                        "taxonomy-node5": "t5",
                        "premium": False,
                        "population": {
                            "attributes": 0,
                            "instruments": 0,
                            "time-series": 0,
                        },
                        "attributes": [],
                        "top-instruments": [],
                    }
                ],
                "links": [{"self": "/groups", "next": "groups?page=2"}],
            }
        else:
            data = {"groups": [], "links": [{"self": "/groups", "next": None}]}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_groups_paged)
    all_groups = await client.list_all_groups_async()
    assert len(all_groups) >= 1


@pytest.mark.asyncio
async def test_files_and_availability(monkeypatch):
    client = make_client(monkeypatch)

    # list_files_async
    async def req_files(method, url, **kwargs):
        data = {
            "group-id": "G",
            "file-group-ids": [{"file-group-id": "F1", "file-type": "csv"}],
        }
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_files)
    fl = await client.list_files_async("G")
    assert fl.file_count == 1

    # get_file_info_async success
    fi = await client.get_file_info_async("G", "F1")
    assert fi.file_group_id == "F1"

    # get_file_info_async not found
    async def req_files_empty(method, url, **kwargs):
        data = {"group-id": "G", "file-group-ids": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_files_empty)
    with pytest.raises(FileNotFoundError):
        await client.get_file_info_async("G", "F1")

    # check_availability_async
    async def req_avail(method, url, **kwargs):
        data = {
            "group-id": "G",
            "file-group-id": "F1",
            "date-range": {"earliest": "20240101", "latest": "20240102"},
            "availability": [
                {"file-datetime": "20240101", "is-available": True},
                {"file-datetime": "20240102", "is-available": False},
            ],
        }
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_avail)
    ar = await client.check_availability_async("F1", "20240101")
    assert getattr(ar, "is_available", False) is True

    # list_available_files_async
    async def req_avail_list(method, url, **kwargs):
        data = {"available-files": [{"file-datetime": "20240101"}]}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req_avail_list)
    lst = await client.list_available_files_async("G")
    assert lst and lst[0]["file-datetime"] == "20240101"


@pytest.mark.asyncio
async def test_instruments_and_time_series(monkeypatch):
    client = make_client(monkeypatch)

    # list_instruments_async
    async def req_inst(method, url, **kwargs):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_inst(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_inst)
    res = await client.list_instruments_async("G")
    assert res.items == 0

    # get_instrument_time_series_async
    async def req_ts(method, url, **kwargs):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_ts(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_ts)
    with pytest.raises(ValidationError):
        await client.get_instrument_time_series_async([], ["A"])  # invalid instruments
    ts = await client.get_instrument_time_series_async(["I"], ["A"])  # ok params
    assert ts.items == 0

    # expressions time series
    def cm_ts2(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_ts2)
    ts2 = await client.get_expressions_time_series_async(["DB(X)"])
    assert ts2.items == 0


@pytest.mark.asyncio
async def test_group_filters_attributes_ts(monkeypatch):
    client = make_client(monkeypatch)

    # filters
    async def req_filters(method, url, **kwargs):
        data = {"items": 0, "page-size": 50, "links": [], "filters": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_filters(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "filters": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_filters)
    fr = await client.get_group_filters_async("G")
    assert fr.items == 0

    # attributes
    async def req_attrs(method, url, **kwargs):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_attrs(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_attrs)
    ar = await client.get_group_attributes_async("G")
    assert ar.items == 0

    # group time series
    async def req_gts(method, url, **kwargs):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_gts(*a, **k):
        data = {"items": 0, "page-size": 50, "links": [], "instruments": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_gts)
    gts = await client.get_group_time_series_async("G", ["A"])  # minimal valid
    assert gts.items == 0


@pytest.mark.asyncio
async def test_grid_api(monkeypatch):
    client = make_client(monkeypatch)

    with pytest.raises(ValueError):
        await client.get_grid_data_async()
    with pytest.raises(ValueError):
        await client.get_grid_data_async(expr="X", grid_id="Y")

    async def req_grid(method, url, **kwargs):
        data = {"series": []}
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    def cm_grid(*a, **k):
        data = {"series": []}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_grid)
    gr = await client.get_grid_data_async(expr="DBGRID(X)")
    assert isinstance(gr.series, list)


@pytest.mark.asyncio
async def test_auth_and_connect_close_paths(monkeypatch):
    # _ensure_authenticated raises when not authenticated
    client = make_client(monkeypatch, auth_ok=False)
    with pytest.raises(AuthenticationError):
        await client._ensure_authenticated()

    # close non-async close path
    await client.close()
    assert client.session is None
