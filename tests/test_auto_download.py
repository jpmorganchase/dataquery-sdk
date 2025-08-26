import asyncio
from types import SimpleNamespace
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, Mock

from dataquery.auto_download import AutoDownloadManager
from dataquery.models import FileList, FileInfo, DownloadProgress, DownloadResult, DownloadStatus
from dataquery.exceptions import NotFoundError


@pytest.mark.asyncio
async def test_init_creates_directory_and_defaults(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="group1", destination_dir=str(temp_download_dir))

    assert manager.destination_dir.exists()
    assert manager.download_options.output_dir == str(temp_download_dir)
    assert manager.max_retries == 3
    assert manager.check_current_date_only is True
    assert manager.is_running is False


def test_get_dates_to_check_modes(temp_download_dir):
    client = Mock()
    m1 = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), check_current_date_only=True)
    dates1 = m1._get_dates_to_check()
    assert isinstance(dates1, list) and len(dates1) == 1 and len(dates1[0]) == 8

    m2 = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), check_current_date_only=False)
    dates2 = m2._get_dates_to_check()
    assert isinstance(dates2, list) and len(dates2) == 3
    assert all(len(d) == 8 for d in dates2)


def test_file_exists_locally(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))

    file_id = "fileA"
    date_str = "20240115"
    # Initially no file present
    assert manager._file_exists_locally(file_id, date_str) is False

    # Create a dummy file matching the heuristic
    matching = Path(temp_download_dir) / f"prefix_{file_id}_{date_str}_suffix.csv"
    matching.write_text("dummy")
    assert manager._file_exists_locally(file_id, date_str) is True


@pytest.mark.asyncio
async def test_check_and_download_skips_when_already_downloaded(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"
    manager._downloaded_files.add(f"{file_id}_{date_str}")

    # Should silently return without calling availability or download
    client.check_availability_async = AsyncMock()
    manager._download_file = AsyncMock()
    await manager._check_and_download_file_for_date(file_id, date_str)
    client.check_availability_async.assert_not_called()
    manager._download_file.assert_not_called()


@pytest.mark.asyncio
async def test_check_and_download_skips_when_exceeded_retries(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), max_retries=2)
    file_id = "fid"
    date_str = "20240115"
    manager._failed_files[f"{file_id}_{date_str}"] = 2

    client.check_availability_async = AsyncMock()
    manager._download_file = AsyncMock()
    await manager._check_and_download_file_for_date(file_id, date_str)
    client.check_availability_async.assert_not_called()
    manager._download_file.assert_not_called()


@pytest.mark.asyncio
async def test_check_and_download_skips_when_exists_locally(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"
    # Create a file that should match
    Path(temp_download_dir, f"{file_id}_{date_str}.csv").write_text("x")

    await manager._check_and_download_file_for_date(file_id, date_str)
    assert manager.stats["files_skipped"] == 1
    assert f"{file_id}_{date_str}" in manager.get_downloaded_files()


@pytest.mark.asyncio
async def test_check_and_download_handles_not_found_and_unavailable(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"

    # NotFoundError path
    client.check_availability_async = AsyncMock(side_effect=NotFoundError("File", file_id))
    await manager._check_and_download_file_for_date(file_id, date_str)

    # Unavailable path
    client.check_availability_async = AsyncMock(return_value=SimpleNamespace(available=False))
    await manager._check_and_download_file_for_date(file_id, date_str)

    # Neither should trigger download
    manager._download_file = AsyncMock()
    manager._download_file.assert_not_called()


@pytest.mark.asyncio
async def test_check_and_download_triggers_download_on_available(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"

    client.check_availability_async = AsyncMock(return_value=SimpleNamespace(available=True))
    manager._download_file = AsyncMock()
    await manager._check_and_download_file_for_date(file_id, date_str)
    manager._download_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_download_file_success_updates_stats_and_progress_callback(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(
        client,
        group_id="g",
        destination_dir=str(temp_download_dir),
        progress_callback=Mock(),
    )

    file_id = "fid"
    date_str = "20240115"
    file_key = f"{file_id}_{date_str}"

    progress_seen = {"called": False}

    async def side_effect_download(file_group_id, file_datetime, options, progress_callback):  # noqa: ARG001
        # Simulate progress callback invocation
        progress = DownloadProgress(file_group_id=file_group_id, bytes_downloaded=1024, total_bytes=4096)
        progress_callback(progress)
        # Return a successful result
        return DownloadResult(file_group_id=file_group_id, status=DownloadStatus.COMPLETED, success=True, file_size=1024)

    client.download_file_async = AsyncMock(side_effect=side_effect_download)

    await manager._download_file(file_id, date_str, file_key)

    assert manager.stats["files_downloaded"] == 1
    assert file_key in manager.get_downloaded_files()
    # Total bytes should capture max seen via progress
    assert manager.stats["total_bytes_downloaded"] >= 1024


@pytest.mark.asyncio
async def test_download_file_failure_increments_failed(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"
    file_key = f"{file_id}_{date_str}"

    # Return an unsuccessful result
    client.download_file_async = AsyncMock(return_value=SimpleNamespace(success=False, error_message="oops"))

    await manager._download_file(file_id, date_str, file_key)
    assert manager.stats["download_failures"] == 1
    assert manager.get_failed_files().get(file_key, 0) == 1


@pytest.mark.asyncio
async def test_download_file_exception_increments_and_raises(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir))
    file_id = "fid"
    date_str = "20240115"
    file_key = f"{file_id}_{date_str}"

    client.download_file_async = AsyncMock(side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError):
        await manager._download_file(file_id, date_str, file_key)

    assert manager.stats["download_failures"] == 1
    assert manager.get_failed_files().get(file_key, 0) == 1


@pytest.mark.asyncio
async def test_process_file_respects_filter(temp_download_dir):
    client = Mock()
    # Filter out all files
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), file_filter=lambda f: False)

    class F:
        file_group_id = "fg1"

    manager._check_and_download_file_for_date = AsyncMock()
    await manager._process_file(F())
    manager._check_and_download_file_for_date.assert_not_called()


@pytest.mark.asyncio
async def test_monitoring_loop_calls_error_callback(temp_download_dir):
    client = Mock()
    error_cb = AsyncMock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), interval_minutes=0, error_callback=error_cb)

    # Force an error inside the monitoring loop
    manager._check_and_download_files = AsyncMock(side_effect=RuntimeError("loop error"))

    await manager.start()
    # Let the loop run one iteration
    await asyncio.sleep(0)
    # Stop will set the event and wait for the task
    await manager.stop()

    error_cb.assert_awaited()
    assert manager.is_running is False


@pytest.mark.asyncio
async def test_start_and_stop_state_transitions(temp_download_dir):
    client = Mock()
    manager = AutoDownloadManager(client, group_id="g", destination_dir=str(temp_download_dir), interval_minutes=0)

    # Provide a minimal list_files_async response for the loop
    # Use FileList/FileInfo to satisfy attribute checks
    file_list = FileList(group_id="g", file_group_ids=[])
    client.list_files_async = AsyncMock(return_value=file_list)

    await manager.start()
    assert manager.is_running is True
    await asyncio.sleep(0)
    await manager.stop()
    assert manager.is_running is False


