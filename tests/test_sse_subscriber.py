"""Tests for ``dataquery.sse_subscriber.NotificationDownloadManager``.

Exercises notification parsing, availability gating, de-dup, the user file
filter, the initial bulk check, stop/start, and error dispatch. The DataQuery
client is a fake that returns whatever the test expects.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from dataquery.models import DownloadResult, DownloadStatus
from dataquery.sse_client import SSEEvent
from dataquery.sse_subscriber import NotificationDownloadManager

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeClient:
    """Minimal stand-in for ``DataQueryClient`` that the subscriber uses."""

    def __init__(self):
        self.config = SimpleNamespace()
        self.auth_manager = SimpleNamespace()
        self.check_availability_async = AsyncMock()
        self.list_available_files_async = AsyncMock(return_value=[])
        self.download_file_async = AsyncMock()


def _availability(is_available: bool = True):
    return SimpleNamespace(is_available=is_available)


def _download_result(status: DownloadStatus, size: int = 123) -> DownloadResult:
    return DownloadResult(
        file_group_id="FG",
        group_id="G",
        local_path=Path("/tmp/FG_20240101.csv"),
        file_size=size,
        status=status,
    )


def _event(
    file_group_id: Optional[str] = "FG",
    file_date_time: Optional[str] = "20240101",
    event_id: str = "e1",
    *,
    top_level: bool = False,
    raw_data: Optional[str] = None,
) -> SSEEvent:
    if raw_data is not None:
        return SSEEvent(event="message", data=raw_data, id=event_id)
    body: Dict[str, Any] = {}
    if file_group_id is not None:
        body["fileGroupId"] = file_group_id
    if file_date_time is not None:
        body["fileDateTime"] = file_date_time
    payload = body if top_level else {"data": body}
    return SSEEvent(event="message", data=json.dumps(payload), id=event_id)


# ---------------------------------------------------------------------------
# Notification handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_notification_triggers_download_and_updates_stats(tmp_path):
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(True)
    client.download_file_async.return_value = _download_result(DownloadStatus.COMPLETED, size=500)

    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
    )
    mgr._running = True

    await mgr._on_sse_event(_event())

    assert mgr.stats["notifications_received"] == 1
    assert mgr.stats["files_discovered"] == 1
    assert mgr.stats["files_downloaded"] == 1
    assert mgr.stats["download_failures"] == 0
    # The download bookkeeping key is "<fg>_<date>".
    assert "FG_20240101" in mgr._downloaded_files
    client.check_availability_async.assert_awaited_once_with("FG", "20240101")
    client.download_file_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_accepts_top_level_payload(tmp_path):
    """Some deployments flatten the payload — fields at the top level."""
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(True)
    client.download_file_async.return_value = _download_result(DownloadStatus.COMPLETED)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = True
    await mgr._on_sse_event(_event(top_level=True))
    client.check_availability_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_notification_skips_when_availability_false(tmp_path):
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(False)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    await mgr._on_sse_event(_event())

    assert mgr.stats["files_discovered"] == 0
    client.download_file_async.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_notification_is_deduped(tmp_path):
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(True)
    client.download_file_async.return_value = _download_result(DownloadStatus.COMPLETED)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = True
    await mgr._on_sse_event(_event())
    await mgr._on_sse_event(_event(event_id="e2"))

    assert client.download_file_async.await_count == 1
    assert mgr.stats["notifications_received"] == 2


@pytest.mark.asyncio
async def test_notification_with_missing_fields_is_logged_and_skipped(tmp_path):
    client = _FakeClient()
    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    # Missing fileDateTime.
    await mgr._on_sse_event(_event(file_date_time=None))
    client.check_availability_async.assert_not_called()


@pytest.mark.asyncio
async def test_notification_with_non_json_data_is_ignored(tmp_path):
    client = _FakeClient()
    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    await mgr._on_sse_event(_event(raw_data="not-json"))
    client.check_availability_async.assert_not_called()


@pytest.mark.asyncio
async def test_file_filter_excludes_notification(tmp_path):
    client = _FakeClient()

    def only_other(f: Dict[str, Any]) -> bool:
        return f["file-group-id"] != "FG"

    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        file_filter=only_other,
    )
    await mgr._on_sse_event(_event())
    client.check_availability_async.assert_not_called()


@pytest.mark.asyncio
async def test_availability_error_goes_to_error_callback(tmp_path):
    client = _FakeClient()
    client.check_availability_async.side_effect = RuntimeError("no network")

    seen: List[Exception] = []

    async def on_err(exc: Exception) -> None:
        seen.append(exc)

    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        error_callback=on_err,
    )
    mgr._running = True
    await mgr._on_sse_event(_event())

    assert len(seen) == 1
    assert mgr.stats["errors"] and "no network" in mgr.stats["errors"][0]["error"]


@pytest.mark.asyncio
async def test_notification_ignored_when_not_running(tmp_path):
    client = _FakeClient()
    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = False
    await mgr._on_sse_event(_event())
    client.check_availability_async.assert_not_called()


@pytest.mark.asyncio
async def test_failed_download_marks_file_and_bumps_counter(tmp_path):
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(True)
    client.download_file_async.return_value = _download_result(DownloadStatus.FAILED)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = True
    await mgr._on_sse_event(_event())

    assert mgr.stats["download_failures"] == 1
    assert mgr._failed_files.get("FG_20240101", 0) == 1
    assert "FG_20240101" not in mgr._downloaded_files


@pytest.mark.asyncio
async def test_already_exists_counts_as_skip(tmp_path):
    client = _FakeClient()
    client.check_availability_async.return_value = _availability(True)
    client.download_file_async.return_value = _download_result(DownloadStatus.ALREADY_EXISTS)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = True
    await mgr._on_sse_event(_event())

    assert mgr.stats["files_skipped"] == 1
    assert mgr.stats["files_downloaded"] == 0
    assert "FG_20240101" in mgr._downloaded_files


# ---------------------------------------------------------------------------
# Initial bulk check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initial_check_downloads_only_available_files(tmp_path):
    client = _FakeClient()
    client.list_available_files_async.return_value = [
        {"file-group-id": "A", "file-datetime": "20240101", "is-available": True},
        {"file-group-id": "B", "file-datetime": "20240101", "is-available": False},
        # Missing fields should be ignored.
        {"file-group-id": None, "file-datetime": "20240101", "is-available": True},
    ]
    client.download_file_async.return_value = _download_result(DownloadStatus.COMPLETED)

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)
    mgr._running = True

    await mgr._check_and_download()

    assert mgr.stats["files_discovered"] == 1
    assert client.download_file_async.await_count == 1


@pytest.mark.asyncio
async def test_initial_check_skips_files_already_local(tmp_path):
    # Create a file on disk that the heuristic should consider "already there".
    (tmp_path / "A_20240101.csv").write_text("data")

    client = _FakeClient()
    client.list_available_files_async.return_value = [
        {"file-group-id": "A", "file-datetime": "20240101", "is-available": True},
    ]

    mgr = NotificationDownloadManager(client=client, group_id="G", destination_dir=str(tmp_path), initial_check=False)

    await mgr._check_and_download()

    assert mgr.stats["files_skipped"] == 1
    client.download_file_async.assert_not_called()


# ---------------------------------------------------------------------------
# Lifecycle + stats snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_without_start_is_noop(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(), group_id="G", destination_dir=str(tmp_path), initial_check=False
    )
    await mgr.stop()
    assert not mgr.is_running


@pytest.mark.asyncio
async def test_stop_shuts_down_sse_client(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(), group_id="G", destination_dir=str(tmp_path), initial_check=False
    )
    fake_sse = SimpleNamespace(stop=AsyncMock())
    mgr._sse_client = fake_sse  # type: ignore[assignment]
    mgr._running = True

    await mgr.stop()

    fake_sse.stop.assert_awaited_once()
    assert mgr._sse_client is None
    assert not mgr._running


def test_get_stats_returns_snapshot_with_derived_fields(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(), group_id="G", destination_dir=str(tmp_path), initial_check=False
    )
    snap = mgr.get_stats()
    assert snap["group_id"] == "G"
    assert snap["is_running"] is False
    assert snap["destination_dir"] == str(tmp_path)
    assert "downloaded_file_keys" in snap and "failed_file_keys" in snap


def test_repr_and_str(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(), group_id="G", destination_dir=str(tmp_path), initial_check=False
    )
    assert "G" in str(mgr)
    assert "NotificationDownloadManager" in repr(mgr)


# ---------------------------------------------------------------------------
# SSE error dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sse_error_records_error_and_invokes_callback(tmp_path):
    seen: List[Exception] = []

    def on_err(exc: Exception) -> None:
        seen.append(exc)

    mgr = NotificationDownloadManager(
        client=_FakeClient(),
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        error_callback=on_err,
    )
    await mgr._on_sse_error(RuntimeError("sse boom"))

    assert len(seen) == 1
    assert mgr.stats["errors"]
