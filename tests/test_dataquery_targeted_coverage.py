import asyncio
from pathlib import Path
from typing import List
from unittest.mock import AsyncMock, patch

import pytest

from dataquery.dataquery import DataQuery
from dataquery.exceptions import ConfigurationError
from dataquery.models import (
    AvailabilityInfo,
    ClientConfig,
    DownloadResult,
    DownloadStatus,
    FileInfo,
    Group,
)


@pytest.mark.asyncio
async def test_run_groups_async_empty():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    with patch.object(dq, "list_groups_async", new=AsyncMock(return_value=[])):
        result = await dq.run_groups_async()
        assert result["error"] == "No groups found"


@pytest.mark.asyncio
async def test_run_groups_async_success():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    groups: List[Group] = [
        Group(provider="A"),
        Group(provider="B"),
        Group(provider="A"),
    ]
    with patch.object(dq, "list_groups_async", new=AsyncMock(return_value=groups)):
        result = await dq.run_groups_async()
        assert result["total_groups"] == 3
        assert set(result["providers"]) == {"A", "B"}


@pytest.mark.asyncio
async def test_run_group_files_async_empty():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    with patch.object(dq, "list_files_async", new=AsyncMock(return_value=[])):
        result = await dq.run_group_files_async("grp")
        assert result["error"] == "No files found"


@pytest.mark.asyncio
async def test_run_group_files_async_success_types_and_dump():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    files = [
        FileInfo(file_group_id="f1", file_type=["JSON", "CSV"]),
        FileInfo(file_group_id="f2", file_type=["CSV"]),
    ]
    with patch.object(dq, "list_files_async", new=AsyncMock(return_value=files)):
        result = await dq.run_group_files_async("grp")
        assert result["group_id"] == "grp"
        assert result["total_files"] == 2
        assert set(result["file_types"]) == {"JSON", "CSV"}
        assert isinstance(result["files"], list)


@pytest.mark.asyncio
async def test_health_check_async_delegates_to_client():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    mock_client = type("C", (), {"health_check_async": AsyncMock(return_value=True)})()
    dq._client = mock_client  # Bypass connect
    with patch.object(dq, "connect_async", new=AsyncMock(return_value=None)):
        ok = await dq.health_check_async()
        assert ok is True
        mock_client.health_check_async.assert_called_once_with()


@pytest.mark.asyncio
async def test_run_availability_async_report():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    avail = AvailabilityInfo(file_date="20240101", is_available=True, file_name="a.txt")
    with patch.object(
        dq, "check_availability_async", new=AsyncMock(return_value=avail)
    ):
        report = await dq.run_availability_async("fg", "20240101")
        assert report["file_group_id"] == "fg"
        assert report["file_datetime"] == "20240101"
        assert report["is_available"] is True


@pytest.mark.asyncio
async def test_run_download_async_report_from_result():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    result = DownloadResult(
        file_group_id="fg",
        local_path=Path("/tmp/x"),
        file_size=1024 * 1024,
        download_time=1.0,
        status=DownloadStatus.COMPLETED,
    )
    with patch.object(dq, "download_file_async", new=AsyncMock(return_value=result)):
        report = await dq.run_download_async("fg", "20240101")
        assert report["download_successful"] is True
        assert report["local_path"].endswith("/tmp/x")
        assert report["file_size"] == result.file_size


@pytest.mark.asyncio
async def test_run_group_download_async_no_available_files():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    with patch.object(dq, "list_available_files_async", new=AsyncMock(return_value=[])):
        out = await dq.run_group_download_async("grp", "20240101", "20240102")
        assert out["error"] == "No available files found for date range"


def test_init_raises_configurationerror_on_validate_failure():
    cfg = ClientConfig(
        base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
    )
    with patch(
        "dataquery.dataquery.EnvConfig.validate_config", side_effect=Exception("boom")
    ):
        with pytest.raises(ConfigurationError):
            DataQuery(config_or_env_file=cfg)


def test_token_url_autoderive_when_client_id_given():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=False,
        bearer_token="t",
        oauth_token_url=None,
    )
    with patch("dataquery.dataquery.EnvConfig.validate_config", return_value=None):
        dq = DataQuery(config_or_env_file=cfg, client_id="id")
        assert dq.client_config.oauth_token_url == "https://api.example.com/oauth/token"


def test_overrides_applied_basic():
    cfg = ClientConfig(
        base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
    )
    with patch("dataquery.dataquery.EnvConfig.validate_config", return_value=None):
        dq = DataQuery(config_or_env_file=cfg, timeout=123.0, max_retries=5)
        assert dq.client_config.timeout == 123.0
        assert dq.client_config.max_retries == 5


@pytest.mark.asyncio
async def test_async_context_manager_calls_connect_and_close():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    with patch.object(
        dq, "connect_async", new=AsyncMock(return_value=None)
    ) as m_connect:
        with patch.object(
            dq, "close_async", new=AsyncMock(return_value=None)
        ) as m_close:
            async with dq as ctx:
                assert ctx is dq
            m_connect.assert_called_once()
            m_close.assert_called_once()


@pytest.mark.asyncio
async def test_wrapper_methods_delegate_to_client():
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )

    # Prepare a fake client with async methods
    class FakeClient:
        async def list_all_groups_async(self):
            return [Group(provider="X")]

        async def list_groups_async(self, limit=None):
            return [Group(provider="Y")]

        async def search_groups_async(self, keywords, limit=None, offset=None):
            return [Group(provider="Z")]

        async def list_files_async(self, group_id, file_group_id=None):
            return FileList(
                group_id=group_id, file_group_ids=[FileInfo(file_group_id="f1")]
            )

        async def check_availability_async(self, file_group_id, file_datetime):
            return AvailabilityInfo(file_date=file_datetime, is_available=True)

        async def list_available_files_async(
            self, group_id, file_group_id=None, start_date=None, end_date=None
        ):
            return [{"is-available": True}]

        async def health_check_async(self):
            return True

        async def list_instruments_async(self, group_id, instrument_id=None, page=None):
            return InstrumentsResponse(items=1, page_size=1, instruments=[], links=None)

        async def search_instruments_async(self, group_id, keywords, page=None):
            return InstrumentsResponse(items=0, page_size=1, instruments=[], links=None)

        async def get_instrument_time_series_async(self, *args, **kwargs):
            return TimeSeriesResponse(items=0, page_size=1, instruments=[], links=None)

        async def get_expressions_time_series_async(self, *args, **kwargs):
            return TimeSeriesResponse(items=0, page_size=1, instruments=[], links=None)

        async def get_group_filters_async(self, group_id, page=None):
            return FiltersResponse(items=0, page_size=1, filters=[], links=None)

        async def get_group_attributes_async(
            self, group_id, instrument_id=None, page=None
        ):
            return AttributesResponse(items=0, page_size=1, instruments=[], links=None)

        async def get_group_time_series_async(self, *args, **kwargs):
            return TimeSeriesResponse(items=0, page_size=1, instruments=[], links=None)

        async def get_grid_data_async(self, *args, **kwargs):
            return GridDataResponse(series=[], error_code=None, error_message=None)

        async def connect(self):
            return None

        async def close(self):
            return None

    from dataquery.models import (
        AttributesResponse,
        FileList,
        FiltersResponse,
        GridDataResponse,
        InstrumentsResponse,
        TimeSeriesResponse,
    )

    dq._client = FakeClient()
    with patch.object(dq, "connect_async", new=AsyncMock(return_value=None)):
        # list_groups_async both branches
        out_all = await dq.list_groups_async()
        assert isinstance(out_all, list)
        out_lim = await dq.list_groups_async(limit=1)
        assert isinstance(out_lim, list)
        # search groups
        sg = await dq.search_groups_async("kw")
        assert isinstance(sg, list)
        # files listing (unwrap FileList to list)
        files = await dq.list_files_async("grp")
        assert isinstance(files, list)
        # availability
        av = await dq.check_availability_async("fg", "20240101")
        assert getattr(av, "is_available", False) is True
        # list available files
        laf = await dq.list_available_files_async("grp")
        assert isinstance(laf, list)
        # health
        assert await dq.health_check_async() is True
        # instruments and time series
        assert (await dq.list_instruments_async("g")).items in (0, 1)
        assert (await dq.search_instruments_async("g", "k")).items in (0, 1)
        assert (await dq.get_instrument_time_series_async(["i"], ["a"])) is not None
        assert (await dq.get_expressions_time_series_async(["e"])) is not None
        assert (await dq.get_group_filters_async("g")).filters == []
        ar = await dq.get_group_attributes_async("g")
        assert isinstance(ar.instruments, list)
        assert (await dq.get_group_time_series_async(["g"], ["a"])) is not None
        assert (await dq.get_grid_data_async(expr="x")) is not None


@pytest.mark.asyncio
async def test_connect_async_and_close_async_create_and_cleanup_client(monkeypatch):
    created = {}

    class DummyClient:
        def __init__(self, cfg):
            created["cfg"] = cfg

        async def connect(self):
            return None

        async def close(self):
            created["closed"] = True
            return None

    monkeypatch.setattr("dataquery.dataquery.DataQueryClient", DummyClient)

    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    assert dq._client is None

    await dq.connect_async()
    assert dq._client is not None

    await dq.close_async()
    assert dq._client is None
    assert created.get("closed") is True


@pytest.mark.asyncio
async def test_cleanup_async_calls_close_and_gc(monkeypatch):
    dq = DataQuery(
        config_or_env_file=ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token="t"
        )
    )
    with patch.object(dq, "close_async", new=AsyncMock(return_value=None)) as m_close:
        await dq.cleanup_async()
        m_close.assert_called_once()
