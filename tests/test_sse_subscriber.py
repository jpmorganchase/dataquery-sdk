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

    def __init__(self, download_dir: str = ""):
        # Replay-related code reads ``download_dir`` / ``token_storage_*`` on
        # ``client.config`` to find the persistence directory; provide them so
        # build_event_id_store() doesn't blow up on missing attrs.
        self.config = SimpleNamespace(
            download_dir=download_dir,
            token_storage_enabled=False,
            token_storage_dir=None,
        )
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
# SSE subscription params (server-side filter)
# ---------------------------------------------------------------------------


async def _sse_params_from_start(mgr: NotificationDownloadManager) -> dict:
    """Call mgr.start() with SSEClient stubbed; return the params it received."""
    captured: dict = {}

    class _FakeSSE:
        def __init__(self, *_args, **kwargs):
            captured.update(kwargs.get("params") or {})
            self._started = False

        async def start(self) -> "_FakeSSE":
            self._started = True
            return self

        async def stop(self) -> None:
            self._started = False

    import dataquery.sse_subscriber as sse_sub

    original = sse_sub.SSEClient
    sse_sub.SSEClient = _FakeSSE  # type: ignore[assignment]
    try:
        await mgr.start()
    finally:
        sse_sub.SSEClient = original  # type: ignore[assignment]
        await mgr.stop()
    return captured


@pytest.mark.asyncio
async def test_sse_params_group_only(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(),
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
    )
    params = await _sse_params_from_start(mgr)
    assert params == {"group-id": "G"}


@pytest.mark.asyncio
async def test_sse_params_with_single_file_group_id(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(),
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        file_group_id="FG_ABC",
    )
    params = await _sse_params_from_start(mgr)
    assert params == {"group-id": "G", "file-group-id": "FG_ABC"}


@pytest.mark.asyncio
async def test_sse_params_with_multiple_file_group_ids(tmp_path):
    mgr = NotificationDownloadManager(
        client=_FakeClient(),
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        file_group_id=["FG1", "FG2", "FG3"],
    )
    params = await _sse_params_from_start(mgr)
    assert params == {"group-id": "G", "file-group-id": "FG1,FG2,FG3"}


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


# ---------------------------------------------------------------------------
# Cross-process event replay
# ---------------------------------------------------------------------------


async def _run_start_with_fake_sse(mgr: NotificationDownloadManager):
    """Stub SSEClient out so start() can run without a real connection.

    Returns the kwargs the SSEClient constructor was called with.
    """
    captured: dict = {}

    class _FakeSSE:
        def __init__(self, *_args, **kwargs):
            captured.update(kwargs)
            self._started = False

        async def start(self) -> "_FakeSSE":
            self._started = True
            return self

        async def stop(self) -> None:
            self._started = False

    import dataquery.sse_subscriber as sse_sub

    original = sse_sub.SSEClient
    sse_sub.SSEClient = _FakeSSE  # type: ignore[assignment]
    try:
        await mgr.start()
    finally:
        sse_sub.SSEClient = original  # type: ignore[assignment]
        await mgr.stop()
    return captured


@pytest.mark.asyncio
async def test_replay_skips_initial_check_when_event_id_persisted(tmp_path):
    """If a stored last-event-id exists, the bulk initial check must be
    skipped — replay handles that gap precisely."""
    # Pre-seed the on-disk store as if a previous run had saved an event id.
    state_dir = tmp_path / ".sse_state"
    state_dir.mkdir()
    from dataquery.sse_event_store import _fingerprint_subscription

    fingerprint = _fingerprint_subscription("G", None)
    (state_dir / f"sse_{fingerprint}.json").write_text('{"last_event_id": "evt-prev"}')

    client = _FakeClient(download_dir=str(tmp_path))
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=True,  # would normally bulk-check; replay should override.
    )

    captured = await _run_start_with_fake_sse(mgr)

    # The bulk listing API must NOT have been called.
    client.list_available_files_async.assert_not_called()
    # The store must have been wired into the SSEClient.
    assert captured.get("event_id_store") is not None


@pytest.mark.asyncio
async def test_replay_disabled_runs_legacy_initial_check(tmp_path):
    """``enable_event_replay=False`` must restore the legacy bulk-check path."""
    # Seed a stored id — it must be ignored when replay is disabled.
    state_dir = tmp_path / ".sse_state"
    state_dir.mkdir()
    from dataquery.sse_event_store import _fingerprint_subscription

    fingerprint = _fingerprint_subscription("G", None)
    (state_dir / f"sse_{fingerprint}.json").write_text('{"last_event_id": "evt-prev"}')

    client = _FakeClient(download_dir=str(tmp_path))
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=True,
        enable_event_replay=False,
    )

    captured = await _run_start_with_fake_sse(mgr)

    client.list_available_files_async.assert_called_once()
    assert captured.get("event_id_store") is None


@pytest.mark.asyncio
async def test_replay_runs_initial_check_on_first_run(tmp_path):
    """No stored id ⇒ legacy bulk check still runs on the very first start."""
    client = _FakeClient(download_dir=str(tmp_path))
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=True,
    )

    captured = await _run_start_with_fake_sse(mgr)

    client.list_available_files_async.assert_called_once()
    # Store still attached so future event ids are persisted.
    assert captured.get("event_id_store") is not None


@pytest.mark.asyncio
async def test_clear_event_id_removes_store_file(tmp_path):
    state_dir = tmp_path / ".sse_state"
    state_dir.mkdir()
    from dataquery.sse_event_store import _fingerprint_subscription

    fingerprint = _fingerprint_subscription("G", None)
    state_file = state_dir / f"sse_{fingerprint}.json"
    state_file.write_text('{"last_event_id": "evt-x"}')

    client = _FakeClient(download_dir=str(tmp_path))
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
    )
    await _run_start_with_fake_sse(mgr)

    assert state_file.exists()
    mgr.clear_event_id()
    assert not state_file.exists()


# ---------------------------------------------------------------------------
# Bounded structures — required for true 24/7 operation
# ---------------------------------------------------------------------------


def test_downloaded_files_evicts_oldest_when_over_cap(tmp_path):
    """LRU cap means an unbounded event stream cannot leak memory."""
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        max_tracked_files=3,
    )
    for i in range(5):
        mgr._downloaded_files.add(f"k{i}")
    # Only the 3 most recently added survive.
    assert len(mgr._downloaded_files) == 3
    assert "k0" not in mgr._downloaded_files
    assert "k1" not in mgr._downloaded_files
    assert "k2" in mgr._downloaded_files
    assert "k3" in mgr._downloaded_files
    assert "k4" in mgr._downloaded_files


def test_downloaded_files_membership_check_touches_lru(tmp_path):
    """A `key in set` check refreshes the key so duplicate-event spam keeps
    the entry hot — the manager won't drop and re-download a file that
    notifications keep referencing."""
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        max_tracked_files=3,
    )
    mgr._downloaded_files.add("hot")
    mgr._downloaded_files.add("k1")
    mgr._downloaded_files.add("k2")
    # Touch "hot" then add three more — "hot" should survive while k1/k2 don't.
    assert "hot" in mgr._downloaded_files
    mgr._downloaded_files.add("k3")
    mgr._downloaded_files.add("k4")
    assert "hot" in mgr._downloaded_files
    assert "k1" not in mgr._downloaded_files


def test_failed_files_evicts_oldest_when_over_cap(tmp_path):
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        max_tracked_files=3,
    )
    for i in range(5):
        mgr._failed_files[f"k{i}"] = 1
    assert len(mgr._failed_files) == 3
    assert mgr._failed_files.get("k0", 0) == 0  # evicted
    assert mgr._failed_files.get("k4", 0) == 1


def test_failed_files_setitem_touches_lru(tmp_path):
    """Re-assigning a key (incrementing its retry count) keeps it hot."""
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        max_tracked_files=3,
    )
    mgr._failed_files["a"] = 1
    mgr._failed_files["b"] = 1
    mgr._failed_files["c"] = 1
    mgr._failed_files["a"] = 2  # touch
    mgr._failed_files["d"] = 1  # forces eviction of LRU — should be "b"
    assert "a" in mgr._failed_files
    assert "b" not in mgr._failed_files
    assert mgr._failed_files.get("a", 0) == 2


def test_failed_files_pop_removes_entry(tmp_path):
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
    )
    mgr._failed_files["k"] = 5
    assert mgr._failed_files.pop("k", None) == 5
    assert "k" not in mgr._failed_files
    # Pop of missing key returns the default — no exception.
    assert mgr._failed_files.pop("missing", "sentinel") == "sentinel"


@pytest.mark.asyncio
async def test_errors_are_a_bounded_ring_buffer(tmp_path):
    """`stats["errors"]` must not grow unboundedly across long-running sessions."""
    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
        max_tracked_errors=5,
    )
    for i in range(20):
        await mgr._dispatch_error(RuntimeError(f"err-{i}"))
    assert len(mgr.stats["errors"]) == 5
    # Ring buffer keeps the most recent — first surviving entry is err-15.
    first = mgr.stats["errors"][0]
    last = mgr.stats["errors"][-1]
    assert "err-15" in first["error"]
    assert "err-19" in last["error"]


@pytest.mark.asyncio
async def test_get_stats_serialises_errors_as_plain_list(tmp_path):
    """get_stats() must return a JSON-serialisable snapshot — the deque is
    converted to a list so callers (CLI --watch, json.dumps) just work."""
    import json

    client = _FakeClient()
    mgr = NotificationDownloadManager(
        client=client,
        group_id="G",
        destination_dir=str(tmp_path),
        initial_check=False,
    )
    await mgr._dispatch_error(RuntimeError("boom"))
    snap = mgr.get_stats()
    assert isinstance(snap["errors"], list)
    # Round-trips through json.dumps without a TypeError.
    json.dumps({k: v for k, v in snap.items() if k not in ("start_time",)})
