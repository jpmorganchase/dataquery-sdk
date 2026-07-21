from unittest.mock import AsyncMock, patch

import pytest

from dataquery.dataquery import DataQuery
from dataquery.types.models import ClientConfig, FileList, GroupList


def test_sync_wrapper_calls_run_async(monkeypatch):
    cfg = ClientConfig(base_url="https://api.example.com")
    monkeypatch.setattr("dataquery.dataquery.EnvConfig.validate_config", lambda _cfg: None)
    dq = DataQuery(cfg)
    with patch("dataquery.dataquery.DataQuery._run_sync", return_value=[]) as ra:
        dq.list_groups()
        ra.assert_called()


def test_page_sync_wrappers_call_run_sync(monkeypatch):
    """The client-driven pagination sync wrappers all route through _run_sync."""
    cfg = ClientConfig(base_url="https://api.example.com")
    monkeypatch.setattr("dataquery.dataquery.EnvConfig.validate_config", lambda _cfg: None)
    dq = DataQuery(cfg)
    page = GroupList(groups=[])
    with patch("dataquery.dataquery.DataQuery._run_sync", return_value=page) as ra:
        dq.list_groups_page(limit=5)
        dq.search_groups_page("kw", limit=5)
        dq.list_files_page("G")
        dq.get_next_page(page)
        assert ra.call_count == 4


@pytest.mark.asyncio
async def test_facade_page_methods_delegate(monkeypatch):
    """Facade page methods forward args to the client and return its pages."""
    cfg = ClientConfig(base_url="https://api.example.com")
    monkeypatch.setattr("dataquery.dataquery.EnvConfig.validate_config", lambda _cfg: None)
    dq = DataQuery(cfg)
    calls = {}

    class FakeClient:
        async def list_groups_page_async(self, limit=None, page=None):
            calls["groups"] = (limit, page)
            return GroupList(groups=[])

        async def search_groups_page_async(self, keywords, limit=None, page=None):
            calls["search"] = (keywords, limit, page)
            return GroupList(groups=[])

        async def list_files_async(self, group_id, file_group_id=None, page=None):
            calls["files_page"] = (group_id, file_group_id, page)
            return FileList(**{"group-id": group_id, "file-group-ids": []})

        async def list_all_files_async(self, group_id, file_group_id=None):
            calls["files_all"] = (group_id, file_group_id)
            return []

        async def get_next_page_async(self, page):
            calls["next"] = type(page).__name__
            return None

    dq._client = FakeClient()
    with patch.object(dq, "connect_async", new=AsyncMock(return_value=None)):
        page = await dq.list_groups_page_async(limit=3)
        assert calls["groups"] == (3, None)

        await dq.search_groups_page_async("macro", limit=2, page="tok")
        assert calls["search"] == ("macro", 2, "tok")

        file_page = await dq.list_files_page_async("G", page="tok2")
        assert calls["files_page"] == ("G", None, "tok2")
        assert file_page.file_group_ids == []

        assert await dq.list_files_async("G") == []
        assert calls["files_all"] == ("G", None)

        assert await dq.get_next_page_async(page) is None
        assert calls["next"] == "GroupList"
