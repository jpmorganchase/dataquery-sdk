import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dataquery.core.dataquery import DataQuery
from dataquery.types.models import ClientConfig


@pytest.mark.asyncio
async def test_run_async_in_existing_loop(monkeypatch):
    cfg = ClientConfig(base_url="https://api.example.com")
    # Avoid validation complexity
    monkeypatch.setattr("dataquery.core.dataquery.EnvConfig.validate_config", lambda _cfg: None)
    dq = DataQuery(cfg)
    # Patch client and connect
    with patch("dataquery.core.dataquery.DataQueryClient") as Fake:
        inst = Fake.return_value
        inst.connect = AsyncMock()
        inst.close = AsyncMock()
        await dq.connect_async()
        # Call a method that uses existing loop path
        inst.list_groups_async = AsyncMock(return_value=[])
        groups = await dq.list_groups_async()
        assert groups == []
