import asyncio
from types import SimpleNamespace

import pytest

from dataquery.core.client import DataQueryClient
from dataquery.types.exceptions import (
    APIResponseError,
    AuthenticationError,
    FileNotFoundInGroupError,
    PaginationError,
    ValidationError,
)
from dataquery.types.models import ClientConfig, FileList, GroupList, InstrumentsResponse, TimeSeriesResponse


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
    with pytest.raises(FileNotFoundInGroupError):
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
async def test_search_api(monkeypatch):
    client = make_client(monkeypatch)

    with pytest.raises(ValueError):
        await client.search_async("")
    with pytest.raises(ValueError):
        await client.search_async("   ")

    captured: dict = {}

    def cm_search(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        data = {"results": [{"id": "G1", "name": "Group One"}]}
        r = DummyResponse()

        async def json():
            return data

        r.json = json
        return FakeCtx(r)

    monkeypatch.setattr(client, "_make_authenticated_request", cm_search)
    result = await client.search_async("10y treasury")
    assert result == {"results": [{"id": "G1", "name": "Group One"}]}
    assert captured["method"] == "POST"
    assert captured["json"] == {"query": "10y treasury"}
    assert captured["url"].endswith("/search")


# ---------------------------------------------------------------------------
# Validation symmetry — non-file query endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expressions_time_series_rejects_empty_list(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValueError):
        await client.get_expressions_time_series_async([])


@pytest.mark.asyncio
async def test_expressions_time_series_rejects_blank_entry(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValueError):
        await client.get_expressions_time_series_async(["DB(X)", "  "])


@pytest.mark.asyncio
async def test_expressions_time_series_rejects_bad_date(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValidationError):
        await client.get_expressions_time_series_async(["DB(X)"], start_date="not-a-date")


@pytest.mark.asyncio
async def test_group_time_series_rejects_empty_attributes(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValidationError):
        await client.get_group_time_series_async("G", [])


@pytest.mark.asyncio
async def test_group_time_series_rejects_bad_date(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValidationError):
        await client.get_group_time_series_async("G", ["A"], end_date="bogus")


@pytest.mark.asyncio
async def test_grid_data_rejects_bad_date(monkeypatch):
    client = make_client(monkeypatch)
    with pytest.raises(ValidationError):
        await client.get_grid_data_async(expr="X", date="2026/01/01")


# ---------------------------------------------------------------------------
# list_all_groups_async — pagination loop guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_all_groups_breaks_on_repeated_next_link(monkeypatch):
    """Pathological server returning the same next link forever must not hang."""
    client = make_client(monkeypatch)
    calls = {"n": 0}

    async def looping_pager(method, url, **kwargs):
        calls["n"] += 1
        # Always advertise the same next link → would loop without the guard.
        data = {
            "groups": [],
            "links": [{"self": "/groups", "next": "groups?page=looping"}],
        }
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", looping_pager)
    with pytest.raises(PaginationError) as ei:
        await client.list_all_groups_async()
    # First call fetches the seed URL, second sees the loop and raises.
    assert calls["n"] == 2
    assert ei.value.details["pages_fetched"] == 2
    assert "looping" in ei.value.details["url"]


@pytest.mark.asyncio
async def test_list_all_groups_max_pages_raises(monkeypatch):
    """Hitting the max_pages cap raises PaginationError by default."""
    client = make_client(monkeypatch)
    counter = {"n": 0}

    async def unbounded_pager(method, url, **kwargs):
        counter["n"] += 1
        # Each page advertises a different next link → no loop, just unbounded.
        data = {
            "groups": [{"group-id": f"G{counter['n']}"}],
            "links": [{"self": "/groups", "next": f"groups?page={counter['n']}"}],
        }
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", unbounded_pager)
    with pytest.raises(PaginationError) as ei:
        await client.list_all_groups_async(max_pages=3)
    assert ei.value.details["max_pages"] == 3
    assert ei.value.details["pages_fetched"] == 3


@pytest.mark.asyncio
async def test_list_all_groups_max_pages_silent(monkeypatch):
    """raise_on_cap=False truncates silently instead of raising."""
    client = make_client(monkeypatch)
    counter = {"n": 0}

    async def unbounded_pager(method, url, **kwargs):
        counter["n"] += 1
        data = {
            "groups": [{"group-id": f"G{counter['n']}"}],
            "links": [{"self": "/groups", "next": f"groups?page={counter['n']}"}],
        }
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", unbounded_pager)
    result = await client.list_all_groups_async(max_pages=2, raise_on_cap=False)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_search_all_groups_walks_cursor_pagination(monkeypatch):
    """search_all_groups_async follows links[].next across pages and aggregates."""
    client = make_client(monkeypatch)
    pages = [
        {"groups": [{"group-id": "M1"}], "links": [{"next": "groups/search?p=2"}]},
        {"groups": [{"group-id": "M2"}, {"group-id": "M3"}], "links": [{"next": None}]},
    ]
    idx = {"i": 0}

    async def search_pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", search_pager)
    result = await client.search_all_groups_async("macro")
    assert [g.group_id for g in result] == ["M1", "M2", "M3"]
    assert idx["i"] == 2


@pytest.mark.asyncio
async def test_iter_groups_async_yields_lazily(monkeypatch):
    """iter_groups_async yields each Group across pages."""
    client = make_client(monkeypatch)
    pages = [
        {"groups": [{"group-id": "A"}, {"group-id": "B"}], "links": [{"next": "groups?p=2"}]},
        {"groups": [{"group-id": "C"}], "links": [{"next": None}]},
    ]
    idx = {"i": 0}

    async def pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    seen = []
    async for g in client.iter_groups_async():
        seen.append(g.group_id)
    assert seen == ["A", "B", "C"]


@pytest.mark.asyncio
async def test_get_next_page_async_client_driven(monkeypatch):
    """Client owns the loop: list_groups_page_async + get_next_page_async.

    The SDK fetches exactly one page per call and surfaces links / items /
    page_size / next_link so the caller decides when to continue.
    """
    client = make_client(monkeypatch)
    pages = [
        {
            "items": 3,
            "page-size": 2,
            "groups": [{"group-id": "A"}, {"group-id": "B"}],
            "links": [{"self": "/groups", "next": "groups?page=2"}],
        },
        {
            "items": 3,
            "page-size": 2,
            "groups": [{"group-id": "C"}],
            "links": [{"self": "groups?page=2", "next": None}],
        },
    ]
    idx = {"i": 0}

    async def pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)

    page = await client.list_groups_page_async(limit=2)
    # Pagination metadata is surfaced to the caller.
    assert page.items == 3
    assert page.page_size == 2
    assert page.next_link == "groups?page=2"
    assert page.get_self_link() == "/groups"
    assert page.has_next_page() is True

    seen = [g.group_id for g in page.groups]
    page = await client.get_next_page_async(page)
    assert page is not None
    seen += [g.group_id for g in page.groups]

    # Last page has no next link, so the caller stops here.
    assert page.has_next_page() is False
    assert await client.get_next_page_async(page) is None

    assert seen == ["A", "B", "C"]
    # Exactly two HTTP calls — the SDK never walked ahead on its own.
    assert idx["i"] == 2


@pytest.mark.asyncio
async def test_get_next_page_async_returns_none_without_fetching(monkeypatch):
    """A page with no next link yields None and makes no request."""
    client = make_client(monkeypatch)
    calls = {"n": 0}

    async def boom(method, url, **kwargs):
        calls["n"] += 1
        raise AssertionError("should not fetch when there is no next link")

    monkeypatch.setattr(client, "_make_authenticated_request", boom)
    last = GroupList(**{"groups": [], "links": [{"self": "/groups", "next": None}]})
    assert await client.get_next_page_async(last) is None
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_get_next_page_async_preserves_response_type(monkeypatch):
    """get_next_page_async returns the same response type as the page passed in."""
    client = make_client(monkeypatch)

    async def pager(method, url, **kwargs):
        resp = DummyResponse()

        async def json():
            return {"items": 2, "page-size": 1, "links": [{"next": None}], "instruments": []}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    first = InstrumentsResponse(
        **{"items": 2, "page-size": 1, "links": [{"next": "grp/instruments?page=2"}], "instruments": []}
    )
    nxt = await client.get_next_page_async(first)
    assert isinstance(nxt, InstrumentsResponse)
    assert nxt.page_size == 1
    assert nxt.has_next_page() is False


@pytest.mark.asyncio
async def test_list_files_client_driven_pagination(monkeypatch):
    """Files paginate like every other endpoint: FileList + get_next_page_async."""
    client = make_client(monkeypatch)
    pages = [
        {
            "group-id": "G",
            "items": 3,
            "page-size": 2,
            "file-group-ids": [{"file-group-id": "F1"}, {"file-group-id": "F2"}],
            "links": [{"self": "/group/files", "next": "group/files?page=2"}],
        },
        {
            "group-id": "G",
            "items": 3,
            "page-size": 2,
            "file-group-ids": [{"file-group-id": "F3"}],
            "links": [{"self": "group/files?page=2", "next": None}],
        },
    ]
    idx = {"i": 0}

    async def pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)

    page = await client.list_files_async("G")
    assert isinstance(page, FileList)
    assert page.items == 3
    assert page.page_size == 2
    assert page.next_link == "group/files?page=2"
    assert page.has_next_page() is True

    seen = [f.file_group_id for f in page.file_group_ids]
    page = await client.get_next_page_async(page)
    assert page is not None and isinstance(page, FileList)
    seen += [f.file_group_id for f in page.file_group_ids]
    assert page.has_next_page() is False
    assert await client.get_next_page_async(page) is None

    assert seen == ["F1", "F2", "F3"]
    assert idx["i"] == 2


@pytest.mark.asyncio
async def test_get_next_page_uses_surface_base_url(monkeypatch):
    """FileList next links resolve against the files base; others against the JSON base."""
    client = make_client(monkeypatch)
    client.config = client.config.model_copy(
        update={"files_base_url": "https://files.example.com", "files_context_path": "/api/v2"}
    )
    captured = {}

    async def file_pager(method, url, **kwargs):
        captured["url"] = url
        resp = DummyResponse()

        async def json():
            return {"group-id": "G", "file-group-ids": [], "links": [{"next": None}]}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", file_pager)
    file_page = FileList(**{"group-id": "G", "file-group-ids": [], "links": [{"next": "group/files?page=2"}]})
    await client.get_next_page_async(file_page)
    assert captured["url"] == "https://files.example.com/api/v2/group/files?page=2"

    async def group_pager(method, url, **kwargs):
        captured["url"] = url
        resp = DummyResponse()

        async def json():
            return {"groups": [], "links": [{"next": None}]}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", group_pager)
    group_page = GroupList(**{"groups": [], "links": [{"next": "groups?page=2"}]})
    await client.get_next_page_async(group_page)
    assert captured["url"] == "https://api.example.com/research/dataquery-authe/api/v2/groups?page=2"


@pytest.mark.asyncio
async def test_get_next_page_resolves_host_absolute_links(monkeypatch):
    """A host-absolute next path must not double the API base path."""
    client = make_client(monkeypatch)
    captured = {}

    async def pager(method, url, **kwargs):
        captured["url"] = url
        resp = DummyResponse()

        async def json():
            return {"groups": [], "links": [{"next": None}]}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    page = GroupList(
        **{"groups": [], "links": [{"next": "/research/dataquery-authe/api/v2/groups?page=2"}]}
    )
    await client.get_next_page_async(page)
    assert captured["url"] == "https://api.example.com/research/dataquery-authe/api/v2/groups?page=2"


@pytest.mark.asyncio
async def test_get_next_page_resolves_slash_links_under_context_path(monkeypatch):
    """Regression: the live API returns leading-slash links WITHOUT the context path.

    A next link like ``/group/time-series?...&page=...`` must resolve under the
    configured API base (including ``research/dataquery-authe/api/v2``), not at
    the host root — resolving at the root produced a 404
    ('no Route matched with those values').
    """
    client = make_client(monkeypatch)
    captured = {}

    async def pager(method, url, **kwargs):
        captured["url"] = url
        resp = DummyResponse()

        async def json():
            return {"items": 0, "page-size": 1, "links": [{"next": None}], "instruments": []}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    page = TimeSeriesResponse(
        **{
            "items": 1,
            "page-size": 1,
            "links": [{"next": "/group/time-series?group-id=G&page=tok"}],
            "instruments": [],
        }
    )
    await client.get_next_page_async(page)
    assert captured["url"] == (
        "https://api.example.com/research/dataquery-authe/api/v2/group/time-series?group-id=G&page=tok"
    )


@pytest.mark.asyncio
async def test_get_next_page_refuses_foreign_host(monkeypatch):
    """An absolute next link to a different host is never followed with credentials."""
    client = make_client(monkeypatch)
    calls = {"n": 0}

    async def boom(method, url, **kwargs):
        calls["n"] += 1
        raise AssertionError("must not follow a foreign-host link")

    monkeypatch.setattr(client, "_make_authenticated_request", boom)
    page = GroupList(**{"groups": [], "links": [{"next": "https://evil.example.org/groups?page=2"}]})
    with pytest.raises(PaginationError):
        await client.get_next_page_async(page)
    assert calls["n"] == 0


def test_build_page_rejects_unrecognized_shapes():
    """A 2xx body carrying none of the model's fields fails loudly, not as an empty page."""
    from dataquery.core._mixins import PaginationMixin

    with pytest.raises(APIResponseError):
        PaginationMixin._build_page(GroupList, {})
    with pytest.raises(APIResponseError) as ei:
        PaginationMixin._build_page(GroupList, {"message": "Unauthorized"})
    assert ei.value.details["keys"] == ["message"]


def test_build_page_keeps_data_with_extra_errors_field():
    """A data payload that merely carries an extra errors field is not an envelope."""
    from dataquery.core._mixins import PaginationMixin

    page = PaginationMixin._build_page(
        InstrumentsResponse,
        {"items": 0, "page-size": 1, "links": [], "instruments": [], "errors": [{"code": "X"}]},
    )
    assert isinstance(page, InstrumentsResponse)
    assert page.instruments == []


@pytest.mark.asyncio
async def test_list_all_files_walks_pages(monkeypatch):
    """list_all_files_async aggregates every page of the file catalog."""
    client = make_client(monkeypatch)
    pages = [
        {
            "group-id": "G",
            "file-group-ids": [{"file-group-id": "F1"}, {"file-group-id": "F2"}],
            "links": [{"next": "group/files?page=2"}],
        },
        {
            "group-id": "G",
            "file-group-ids": [{"file-group-id": "F3"}],
            "links": [{"next": None}],
        },
    ]
    idx = {"i": 0}

    async def pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    files = await client.list_all_files_async("G")
    assert [f.file_group_id for f in files] == ["F1", "F2", "F3"]
    assert idx["i"] == 2


@pytest.mark.asyncio
async def test_no_content_body_builds_empty_page(monkeypatch):
    """A 204/info 'no content' body builds an empty page instead of raising."""
    client = make_client(monkeypatch)

    async def req(method, url, **kwargs):
        resp = DummyResponse()

        async def json():
            return {"info": {"code": "204", "description": "There is no content available."}}

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req)
    page = await client.list_instruments_async("G")
    assert isinstance(page, InstrumentsResponse)
    assert page.instruments == []
    assert page.info and page.info["code"] == "204"
    assert page.has_next_page() is False
    # Nothing more to fetch — the client-driven loop terminates.
    assert await client.get_next_page_async(page) is None


@pytest.mark.asyncio
async def test_error_envelope_raises_api_response_error(monkeypatch):
    """An ``errors`` envelope (e.g. invalid page token) raises APIResponseError."""
    client = make_client(monkeypatch)

    async def req(method, url, **kwargs):
        resp = DummyResponse()

        async def json():
            return {
                "errors": [
                    {
                        "code": "498",
                        "message": "Unrecognized Page Token.",
                        "description": "The page token provided is invalid.",
                    }
                ]
            }

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", req)
    first = InstrumentsResponse(**{"links": [{"next": "grp/instruments?page=bad"}], "instruments": []})
    with pytest.raises(APIResponseError) as ei:
        await client.get_next_page_async(first)
    assert ei.value.code == "498"
    assert "invalid" in str(ei.value).lower()


@pytest.mark.asyncio
async def test_client_driven_pagination_stops_on_no_content(monkeypatch):
    """Client-driven loop terminates cleanly when the next page is a 204 no-content."""
    client = make_client(monkeypatch)
    pages = [
        {
            "items": 3,
            "page-size": 2,
            "instruments": [{"item": 1, "instrument-id": "I1", "instrument-name": "A"}],
            "links": [{"self": "/i", "next": "grp/instruments?page=2"}],
        },
        {"info": {"code": "204", "description": "There is no content available."}},
    ]
    idx = {"i": 0}

    async def pager(method, url, **kwargs):
        data = pages[idx["i"]]
        idx["i"] += 1
        resp = DummyResponse()

        async def json():
            return data

        resp.json = json
        return resp

    monkeypatch.setattr(client, "_make_authenticated_request", pager)
    page = await client.list_instruments_async("G")
    seen = []
    while page is not None:
        seen += [i.instrument_id for i in page.instruments]
        page = await client.get_next_page_async(page)
    assert seen == ["I1"]
    assert idx["i"] == 2


@pytest.mark.asyncio
async def test_auth_and_connect_close_paths(monkeypatch):
    # _ensure_authenticated raises when not authenticated
    client = make_client(monkeypatch, auth_ok=False)
    with pytest.raises(AuthenticationError):
        await client._ensure_authenticated()

    # close non-async close path
    await client.close()
    assert client.session is None
