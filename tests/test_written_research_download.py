"""Coverage for the written-research download/unzip workflow in dataquery.utils.

Targets the pure helpers (monthly range math, safe extraction, status
aggregation) and the download_zip_async orchestration via a fake DataQuery, so
no real network is needed.
"""

import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pytest

from dataquery import utils
from dataquery.constants.download import NO_FILES_FOUND_ERROR
from dataquery.types.exceptions import ValidationError
from dataquery.types.models import DownloadResult, DownloadStatus, OperationReport

TODAY = datetime.now().strftime("%Y%m%d")
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")


def _make_zip(path: Path, members: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return path


# --------------------------------------------------------------------------- #
# _split_into_monthly_ranges
# --------------------------------------------------------------------------- #
def _assert_covers(ranges, start, end):
    assert ranges[0][0] == start
    assert ranges[-1][1] == end
    for (_s1, e1), (s2, _e2) in zip(ranges, ranges[1:]):
        prev = datetime.strptime(e1, "%Y%m%d") + timedelta(days=1)
        assert prev.strftime("%Y%m%d") == s2  # contiguous, no gaps/overlaps


def test_monthly_ranges_single_window():
    assert utils._split_into_monthly_ranges("20260701", "20260707") == [("20260701", "20260707")]


def test_monthly_ranges_same_day():
    assert utils._split_into_monthly_ranges("20260714", "20260714") == [("20260714", "20260714")]


def test_monthly_ranges_cross_month():
    ranges = utils._split_into_monthly_ranges("20260115", "20260310")
    assert ranges == [("20260115", "20260131"), ("20260201", "20260228"), ("20260301", "20260310")]
    _assert_covers(ranges, "20260115", "20260310")


def test_monthly_ranges_cross_year():
    ranges = utils._split_into_monthly_ranges("20251215", "20260105")
    assert ranges == [("20251215", "20251231"), ("20260101", "20260105")]
    _assert_covers(ranges, "20251215", "20260105")


def test_monthly_ranges_invalid_format():
    with pytest.raises(ValidationError):
        utils._split_into_monthly_ranges("2026-07-01", "20260707")


def test_monthly_ranges_end_before_start():
    with pytest.raises(ValidationError):
        utils._split_into_monthly_ranges("20260707", "20260701")


# --------------------------------------------------------------------------- #
# _aggregate_chunk_status
# --------------------------------------------------------------------------- #
def _chunk(status: str, error: Optional[str] = None) -> dict:
    return {"start_date": "20260701", "end_date": "20260707", "status": status, "error": error, "counts": {}}


def test_aggregate_status_all_success():
    assert utils._aggregate_chunk_status([_chunk("success"), _chunk("success")]) == "success"


def test_aggregate_status_quiet_windows_do_not_poison():
    chunks = [_chunk("success"), _chunk("error", NO_FILES_FOUND_ERROR)]
    assert utils._aggregate_chunk_status(chunks) == "success"


def test_aggregate_status_all_quiet_is_success():
    assert utils._aggregate_chunk_status([_chunk("error", NO_FILES_FOUND_ERROR)]) == "success"


def test_aggregate_status_real_error_mixed():
    chunks = [_chunk("success"), _chunk("error", "boom")]
    assert utils._aggregate_chunk_status(chunks) == "partial"


def test_aggregate_status_all_real_errors():
    assert utils._aggregate_chunk_status([_chunk("error", "boom"), _chunk("error", "bang")]) == "error"


# --------------------------------------------------------------------------- #
# _extract_zip_safely
# --------------------------------------------------------------------------- #
def test_extract_zip_safely_extracts_members(tmp_path):
    zip_path = _make_zip(tmp_path / "a.zip", {"report.txt": "hello", "sub/nested.txt": "world"})
    members = utils._extract_zip_safely(zip_path, tmp_path)
    assert sorted(members) == ["report.txt", "sub/nested.txt"]
    assert (tmp_path / "report.txt").read_text() == "hello"
    assert (tmp_path / "sub" / "nested.txt").read_text() == "world"


def test_extract_zip_safely_rejects_traversal(tmp_path):
    target = tmp_path / "target"
    zip_path = _make_zip(tmp_path / "evil.zip", {"../escape.txt": "pwned"})
    with pytest.raises(ValidationError):
        utils._extract_zip_safely(zip_path, target)
    assert not (tmp_path / "escape.txt").exists()  # nothing written outside target


# --------------------------------------------------------------------------- #
# _extract_single_zip
# --------------------------------------------------------------------------- #
def test_extract_single_zip_skips_current_day(tmp_path):
    zip_path = _make_zip(tmp_path / f"research_{TODAY}T0930.zip", {"a.txt": "x"})
    assert utils._extract_single_zip(zip_path, TODAY, True) is None
    assert zip_path.exists()  # left untouched
    assert not (tmp_path / "a.txt").exists()


def test_extract_single_zip_uses_last_datetime_token(tmp_path):
    # An earlier digit run (e.g. a start-date token) must not shadow the
    # file-datetime, which is the last YYYYMMDDThhmm token in the name.
    zip_path = _make_zip(tmp_path / f"research_{TODAY}T0000_{YESTERDAY}T0930.zip", {"a.txt": "x"})
    record = utils._extract_single_zip(zip_path, TODAY, True)
    assert record is not None
    assert (tmp_path / "a.txt").exists()


def test_extract_single_zip_skips_invalid_zip(tmp_path):
    bad = tmp_path / f"research_{YESTERDAY}T0930.zip"
    bad.write_bytes(b"this is not a zip")
    assert utils._extract_single_zip(bad, TODAY, True) is None
    assert bad.exists()


def test_extract_single_zip_remove_flag(tmp_path):
    kept = _make_zip(tmp_path / f"keep_{YESTERDAY}T0930.zip", {"k.txt": "x"})
    removed = _make_zip(tmp_path / f"drop_{YESTERDAY}T0931.zip", {"d.txt": "x"})
    assert utils._extract_single_zip(kept, TODAY, False) is not None
    assert utils._extract_single_zip(removed, TODAY, True) is not None
    assert kept.exists()
    assert not removed.exists()
    assert (tmp_path / "k.txt").exists() and (tmp_path / "d.txt").exists()


# --------------------------------------------------------------------------- #
# download_zip_async (orchestration, via a fake DataQuery)
# --------------------------------------------------------------------------- #
def _report(status="success", error=None, files=1) -> OperationReport:
    return OperationReport(
        operation="group_download",
        status=status,
        error=error,
        counts={"total_files": files, "successful_downloads": files, "failed_downloads": 0},
    )


class FakeDQ:
    """Stands in for DataQuery; `behavior` runs once per date window."""

    def __init__(self, behavior):
        self._behavior = behavior
        self.calls = []

    async def run_group_download_async(
        self, *, group_id, start_date, end_date, destination_dir, on_file_complete=None, **kwargs
    ):
        self.calls.append((start_date, end_date))
        return await self._behavior(destination_dir / group_id, start_date, end_date, on_file_complete)


@pytest.mark.asyncio
async def test_download_zip_async_extracts_via_callback(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        zip_path = _make_zip(group_dir / f"research_{YESTERDAY}T0930.zip", {"doc.txt": "hi"})
        await on_file_complete(
            DownloadResult(file_group_id="fg1", local_path=zip_path, status=DownloadStatus.COMPLETED)
        )
        return _report()

    dq = FakeDQ(behavior)
    result = await utils.download_zip_async(dq, "GRP", "20260701", "20260707", destination_dir=tmp_path)

    assert result["status"] == "success"
    assert result["counts"] == {"total_files": 1, "successful_downloads": 1, "failed_downloads": 0}
    assert len(result["extracted"]) == 1
    assert result["extraction_errors"] == []
    assert (tmp_path / "GRP" / "doc.txt").read_text() == "hi"
    assert not (tmp_path / "GRP" / f"research_{YESTERDAY}T0930.zip").exists()  # removed after extract


@pytest.mark.asyncio
async def test_download_zip_async_ignores_non_zip_results(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        group_dir.mkdir(parents=True, exist_ok=True)
        csv = group_dir / "data.csv"
        csv.write_text("a,b")
        await on_file_complete(DownloadResult(file_group_id="fg1", local_path=csv, status=DownloadStatus.COMPLETED))
        return _report()

    result = await utils.download_zip_async(FakeDQ(behavior), "GRP", "20260701", "20260707", destination_dir=tmp_path)
    assert result["extracted"] == []
    assert (tmp_path / "GRP" / "data.csv").exists()


@pytest.mark.asyncio
async def test_download_zip_async_fallback_sweep(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        # Zip lands on disk but the hook never hears about it (e.g. missing
        # local_path); the final sweep must pick it up.
        _make_zip(group_dir / f"research_{YESTERDAY}T0930.zip", {"doc.txt": "hi"})
        return _report()

    result = await utils.download_zip_async(FakeDQ(behavior), "GRP", "20260701", "20260707", destination_dir=tmp_path)
    assert len(result["extracted"]) == 1
    assert (tmp_path / "GRP" / "doc.txt").exists()


@pytest.mark.asyncio
async def test_download_zip_async_reports_extraction_errors(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        zip_path = _make_zip(group_dir / f"research_{YESTERDAY}T0930.zip", {"../escape.txt": "pwned"})
        await on_file_complete(
            DownloadResult(file_group_id="fg1", local_path=zip_path, status=DownloadStatus.COMPLETED)
        )
        return _report()

    result = await utils.download_zip_async(FakeDQ(behavior), "GRP", "20260701", "20260707", destination_dir=tmp_path)
    # Downloads succeeded, but the archive could not be extracted safely: that
    # must be visible in the summary, not just a log line.
    assert result["status"] == "partial"
    assert len(result["extraction_errors"]) == 1
    assert "escape.txt" in result["extraction_errors"][0]["error"]
    assert result["extracted"] == []


@pytest.mark.asyncio
async def test_download_zip_async_splits_months_and_tolerates_quiet_windows(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        if start.startswith("202606"):
            return _report(status="error", error=NO_FILES_FOUND_ERROR, files=0)
        zip_path = _make_zip(group_dir / f"research_{YESTERDAY}T0930.zip", {"doc.txt": "hi"})
        await on_file_complete(
            DownloadResult(file_group_id="fg1", local_path=zip_path, status=DownloadStatus.COMPLETED)
        )
        return _report()

    dq = FakeDQ(behavior)
    result = await utils.download_zip_async(dq, "GRP", "20260615", "20260714", destination_dir=tmp_path)

    assert dq.calls == [("20260615", "20260630"), ("20260701", "20260714")]
    assert result["status"] == "success"  # quiet June window is not a failure
    assert len(result["chunks"]) == 2
    assert result["counts"]["successful_downloads"] == 1


@pytest.mark.asyncio
async def test_download_zip_async_leaves_current_day_zip(tmp_path):
    async def behavior(group_dir, start, end, on_file_complete):
        zip_path = _make_zip(group_dir / f"research_{TODAY}T0930.zip", {"doc.txt": "hi"})
        await on_file_complete(
            DownloadResult(file_group_id="fg1", local_path=zip_path, status=DownloadStatus.COMPLETED)
        )
        return _report()

    result = await utils.download_zip_async(FakeDQ(behavior), "GRP", "20260701", "20260707", destination_dir=tmp_path)
    assert result["extracted"] == []
    assert result["extraction_errors"] == []
    assert (tmp_path / "GRP" / f"research_{TODAY}T0930.zip").exists()


# --------------------------------------------------------------------------- #
# run_group_download_chunked_async (shared range runner)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_run_group_download_chunked_async_aggregates(tmp_path):
    class ChunkedFakeDQ:
        def __init__(self):
            self.calls = []

        async def run_group_download_async(self, *, group_id, start_date, end_date, **kwargs):
            self.calls.append((start_date, end_date))
            return _report(files=2)

    dq = ChunkedFakeDQ()
    result = await utils.run_group_download_chunked_async(dq, "GRP", "20260701", "20260710", chunk_days=5)
    assert dq.calls == [("20260701", "20260705"), ("20260706", "20260710")]
    assert result["totals"]["total_files"] == 4
    assert [c["status"] for c in result["chunks"]] == ["success", "success"]
