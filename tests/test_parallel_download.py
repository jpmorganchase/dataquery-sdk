"""Coverage for dataquery.download.parallel.

Targets the pure helpers (range math, classification, salvage, progress
reporting) and the orchestration/fallback paths via lightweight mocks, so no
real network or large files are needed.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dataquery.core.client import DataQueryClient
from dataquery.download import parallel
from dataquery.types.models import ClientConfig, DownloadProgress, DownloadStatus


def _real_client() -> DataQueryClient:
    return DataQueryClient(ClientConfig(base_url="https://api.example.com", context_path="/v2"))


# --------------------------------------------------------------------------- #
# _compute_ranges
# --------------------------------------------------------------------------- #
def _assert_covers(ranges, total):
    assert ranges[0][0] == 0
    assert ranges[-1][1] == total - 1
    for (_s1, e1), (s2, _e2) in zip(ranges, ranges[1:]):
        assert s2 == e1 + 1  # contiguous, no gaps/overlaps


def test_compute_ranges_even_split():
    ranges = parallel._compute_ranges(100, 4)
    assert len(ranges) == 4
    _assert_covers(ranges, 100)


def test_compute_ranges_remainder_goes_to_last():
    ranges = parallel._compute_ranges(10, 3)
    _assert_covers(ranges, 10)
    assert ranges[-1] == (6, 9)  # last part absorbs the remainder


def test_compute_ranges_single_part():
    assert parallel._compute_ranges(100, 1) == [(0, 99)]


# --------------------------------------------------------------------------- #
# _file_id / _file_dt (both key spellings)
# --------------------------------------------------------------------------- #
def test_file_id_and_dt_key_variants():
    assert parallel._file_id({"file-group-id": "a"}) == "a"
    assert parallel._file_id({"file_group_id": "b"}) == "b"
    assert parallel._file_id({}) is None
    assert parallel._file_dt({"file-datetime": "20240101"}) == "20240101"
    assert parallel._file_dt({"file_datetime": "20240102"}) == "20240102"
    assert parallel._file_dt({}) is None


# --------------------------------------------------------------------------- #
# _classify
# --------------------------------------------------------------------------- #
def test_classify_sorts_results():
    files = [{"file-group-id": "ok"}, {"file-group-id": "err"}, {"file-group-id": "none"}, {"file-group-id": "bad"}]
    results = [
        SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id="ok"),
        RuntimeError("boom"),
        None,
        SimpleNamespace(status=DownloadStatus.FAILED, file_group_id="bad"),
    ]
    succeeded, failed = parallel._classify(files, results)
    assert [r.file_group_id for r in succeeded] == ["ok"]
    assert {f["file-group-id"] for f in failed} == {"err", "none", "bad"}


# --------------------------------------------------------------------------- #
# _ProgressReporter
# --------------------------------------------------------------------------- #
def test_progress_reporter_dispatch_rewind_flush():
    progress = DownloadProgress(file_group_id="f", total_bytes=100, start_time=datetime.now())
    seen = []
    reporter = parallel._ProgressReporter(
        progress=progress,
        total_bytes=100,
        progress_callback=seen.append,
        show_progress=False,
        file_group_id="f",
    )
    reporter.add_bytes(100)  # reaching total forces a dispatch
    assert reporter.bytes_downloaded == 100
    assert len(seen) == 1
    reporter.rewind(40)
    assert reporter.bytes_downloaded == 60
    reporter.flush()  # should not raise


# --------------------------------------------------------------------------- #
# _preallocate_file + _seek_write
# --------------------------------------------------------------------------- #
def test_preallocate_and_seek_write(tmp_path):
    target = tmp_path / "blob.bin"
    parallel._preallocate_file(target, 8)
    assert target.stat().st_size == 8
    with open(target, "r+b") as fh:
        parallel._seek_write(fh, 0, b"AAAA")
        parallel._seek_write(fh, 4, b"BBBB")
    assert target.read_bytes() == b"AAAABBBB"


# --------------------------------------------------------------------------- #
# _salvage
# --------------------------------------------------------------------------- #
def test_salvage_promotes_when_all_bytes_present(tmp_path):
    client = _real_client()
    dest = tmp_path / "out.csv"
    temp = tmp_path / "out.csv.tmp"
    temp.write_bytes(b"0123456789")  # 10 bytes, complete
    result = parallel._salvage(client, "fg", dest, temp, total_bytes=10, bytes_downloaded=10, start_time=0.0)
    assert result is not None
    assert result.status == DownloadStatus.COMPLETED
    assert dest.exists() and not temp.exists()


def test_salvage_discards_partial(tmp_path):
    client = _real_client()
    dest = tmp_path / "out.csv"
    temp = tmp_path / "out.csv.tmp"
    temp.write_bytes(b"012")  # 3 of 10 bytes
    result = parallel._salvage(client, "fg", dest, temp, total_bytes=10, bytes_downloaded=3, start_time=0.0)
    assert result is None
    assert not temp.exists()  # partial cleaned up


def test_salvage_no_temp_returns_none(tmp_path):
    client = _real_client()
    result = parallel._salvage(client, "fg", tmp_path / "x", tmp_path / "missing.tmp", 10, 10, 0.0)
    assert result is None


# --------------------------------------------------------------------------- #
# download_file_parallel — fallback paths
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_parallel_falls_back_to_single_stream_when_ranges_disabled(tmp_path):
    client = SimpleNamespace(
        config=SimpleNamespace(enable_range_downloads=False, overwrite_existing=False, timeout=60.0),
        download_file_async=AsyncMock(return_value="single"),
    )
    out = await parallel.download_file_parallel(
        client=client,
        file_group_id="fg",
        file_datetime=None,
        destination_path=tmp_path,
        num_parts=4,
        global_semaphore=asyncio.Semaphore(2),
    )
    assert out == "single"
    client.download_file_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_parallel_single_part_uses_single_stream(tmp_path):
    client = SimpleNamespace(
        config=SimpleNamespace(enable_range_downloads=True, overwrite_existing=False, timeout=60.0),
        download_file_async=AsyncMock(return_value="single"),
    )
    out = await parallel.download_file_parallel(
        client=client,
        file_group_id="fg",
        file_datetime=None,
        destination_path=tmp_path,
        num_parts=1,
        global_semaphore=asyncio.Semaphore(2),
    )
    assert out == "single"


# --------------------------------------------------------------------------- #
# _download_one_with_stagger
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_stagger_missing_id_returns_none():
    out = await parallel._download_one_with_stagger(
        client=object(),
        file_info={},
        destination_dir=Path("/tmp"),
        num_parts=1,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
    )
    assert out is None


@pytest.mark.asyncio
async def test_stagger_success(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)
    out = await parallel._download_one_with_stagger(
        client=object(),
        file_info={"file-group-id": "fg"},
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
    )
    assert out.file_group_id == "fg"


@pytest.mark.asyncio
async def test_stagger_swallows_exception(monkeypatch):
    async def boom(**kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(parallel, "download_file_parallel", boom)
    out = await parallel._download_one_with_stagger(
        client=object(),
        file_info={"file-group-id": "fg"},
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
    )
    assert out is None


@pytest.mark.asyncio
async def test_stagger_awaits_on_file_complete_for_success(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)
    seen: list = []

    async def on_done(result):
        seen.append(result.file_group_id)

    out = await parallel._download_one_with_stagger(
        client=object(),
        file_info={"file-group-id": "fg"},
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
        on_file_complete=on_done,
    )
    assert out.file_group_id == "fg"
    assert seen == ["fg"]


@pytest.mark.asyncio
async def test_stagger_skips_on_file_complete_for_failure(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.FAILED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)
    seen: list = []

    async def on_done(result):
        seen.append(result.file_group_id)

    await parallel._download_one_with_stagger(
        client=object(),
        file_info={"file-group-id": "fg"},
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
        on_file_complete=on_done,
    )
    assert seen == []


@pytest.mark.asyncio
async def test_stagger_on_file_complete_error_does_not_fail_download(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)

    async def on_done(result):
        raise RuntimeError("unzip blew up")

    out = await parallel._download_one_with_stagger(
        client=object(),
        file_info={"file-group-id": "fg"},
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        delay_seconds=0.0,
        progress_callback=None,
        on_file_complete=on_done,
    )
    assert out.status is DownloadStatus.COMPLETED


# --------------------------------------------------------------------------- #
# download_files_with_retry
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_download_files_with_retry_recovers_on_second_attempt(monkeypatch):
    attempts: dict = {}

    async def fake_parallel(**kwargs):
        fgid = kwargs["file_group_id"]
        attempts[fgid] = attempts.get(fgid, 0) + 1
        if fgid == "f2" and attempts[fgid] == 1:
            return None  # transient failure on first try
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=fgid)

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)

    succeeded, failed, retry_count = await parallel.download_files_with_retry(
        client=object(),
        files=[{"file-group-id": "f1"}, {"file-group-id": "f2"}],
        destination_dir=Path("/tmp"),
        num_parts=4,
        global_semaphore=asyncio.Semaphore(2),
        intelligent_delay=0.0,
        base_retry_delay=0.0,
        max_retries=2,
    )
    assert retry_count == 1
    assert {r.file_group_id for r in succeeded} == {"f1", "f2"}
    assert failed == []


@pytest.mark.asyncio
async def test_download_files_with_retry_all_succeed_first_pass(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)

    succeeded, failed, retry_count = await parallel.download_files_with_retry(
        client=object(),
        files=[{"file-group-id": "f1"}],
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        intelligent_delay=0.0,
        base_retry_delay=0.0,
        max_retries=2,
    )
    assert retry_count == 0
    assert len(succeeded) == 1 and failed == []


@pytest.mark.asyncio
async def test_download_files_with_retry_exhausts(monkeypatch):
    async def always_fail(**kwargs):
        return None

    monkeypatch.setattr(parallel, "download_file_parallel", always_fail)

    succeeded, failed, retry_count = await parallel.download_files_with_retry(
        client=object(),
        files=[{"file-group-id": "f1"}],
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(1),
        intelligent_delay=0.0,
        base_retry_delay=0.0,
        max_retries=2,
    )
    assert retry_count == 2
    assert succeeded == []
    assert [f["file-group-id"] for f in failed] == ["f1"]


@pytest.mark.asyncio
async def test_download_files_with_retry_forwards_on_file_complete(monkeypatch):
    async def fake_parallel(**kwargs):
        return SimpleNamespace(status=DownloadStatus.COMPLETED, file_group_id=kwargs["file_group_id"])

    monkeypatch.setattr(parallel, "download_file_parallel", fake_parallel)
    seen: list = []

    async def on_done(result):
        seen.append(result.file_group_id)

    succeeded, failed, _ = await parallel.download_files_with_retry(
        client=object(),
        files=[{"file-group-id": "f1"}, {"file-group-id": "f2"}],
        destination_dir=Path("/tmp"),
        num_parts=2,
        global_semaphore=asyncio.Semaphore(2),
        intelligent_delay=0.0,
        base_retry_delay=0.0,
        max_retries=1,
        on_file_complete=on_done,
    )
    assert failed == []
    assert sorted(seen) == ["f1", "f2"]  # exactly once per successful file
