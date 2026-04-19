from unittest.mock import AsyncMock, patch

from dataquery.core.dataquery import DataQuery
from dataquery.types.models import ClientConfig


def test_sync_wrapper_calls_run_async(monkeypatch):
    cfg = ClientConfig(base_url="https://api.example.com")
    monkeypatch.setattr("dataquery.core.dataquery.EnvConfig.validate_config", lambda _cfg: None)
    dq = DataQuery(cfg)
    with patch("dataquery.core.dataquery.DataQuery._run_sync", return_value=[]) as ra:
        dq.list_groups()
        ra.assert_called()
