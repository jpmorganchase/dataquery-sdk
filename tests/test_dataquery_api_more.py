from unittest.mock import AsyncMock, patch

import pytest

from dataquery.dataquery import DataQuery
from dataquery.models import ClientConfig


def _dq(monkeypatch) -> DataQuery:
    cfg = ClientConfig(base_url="https://api.example.com")
    monkeypatch.setattr(
        "dataquery.dataquery.EnvConfig.validate_config", lambda _cfg: None
    )
    return DataQuery(cfg)


@pytest.mark.asyncio
async def test_more_async_endpoints(monkeypatch):
    dq = _dq(monkeypatch)
    with patch("dataquery.dataquery.DataQueryClient") as Fake:
        inst = Fake.return_value
        inst.connect = AsyncMock()
        inst.close = AsyncMock()
        await dq.connect_async()
        # Instruments and filters/attributes/grid
        inst.list_instruments_async = AsyncMock(return_value={"items": []})
        inst.search_instruments_async = AsyncMock(return_value={"items": []})
        inst.get_instrument_time_series_async = AsyncMock(return_value={"series": []})
        inst.get_expressions_time_series_async = AsyncMock(return_value={"series": []})
        inst.get_group_filters_async = AsyncMock(return_value={"filters": []})
        inst.get_group_attributes_async = AsyncMock(return_value={"attributes": []})
        inst.get_group_time_series_async = AsyncMock(return_value={"series": []})
        inst.get_grid_data_async = AsyncMock(return_value={"grid": {}})

        assert await dq.list_instruments_async("G") == {"items": []}
        assert await dq.search_instruments_async("G", "k") == {"items": []}
        assert await dq.get_instrument_time_series_async(["i"], ["a"]) == {"series": []}
        assert await dq.get_expressions_time_series_async(["e"]) == {"series": []}
        assert await dq.get_group_filters_async("G") == {"filters": []}
        assert await dq.get_group_attributes_async("G") == {"attributes": []}
        assert await dq.get_group_time_series_async("G", ["a"]) == {"series": []}
        assert await dq.get_grid_data_async(expr="x") == {"grid": {}}


def test_sync_proxies_more(monkeypatch):
    dq = _dq(monkeypatch)
    with patch(
        "dataquery.dataquery.DataQuery._run_async", return_value={"ok": True}
    ) as ra:
        assert dq.get_grid_data(expr="x") == {"ok": True}
        assert ra.called
