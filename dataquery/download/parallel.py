"""
Parallel range downloader.

Downloads one file as ``num_parts`` parallel HTTP range requests, where each
range competes for a single ``global_semaphore`` shared across all files being
downloaded concurrently. Total in-flight HTTP requests =
``max_concurrent × num_parts`` rather than a hierarchical file-then-parts model.

Used by ``DataQuery.run_group_download_async`` for bulk date-range downloads.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Callable, Optional

import aiohttp
import structlog

from .. import constants as C
from ..core.client import get_filename_from_response, validate_file_datetime
from ..types.models import (
    BandwidthThrottler,
    DownloadOptions,
    DownloadProgress,
    DownloadResult,
    DownloadStatus,
)
from ..utils import format_file_size as _format_file_size

if TYPE_CHECKING:
    from ..core.client import DataQueryClient

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _seek_write(fh: IO[bytes], pos: int, data: bytes) -> None:
    """Sync seek+write; runs in the default thread executor."""
    fh.seek(pos)
    fh.write(data)


def _preallocate_file(path: Path, size: int) -> None:
    """Sync truncate to ``size`` bytes; runs in the default thread executor."""
    with open(path, "wb", buffering=C.PREALLOC_BUFFER_SIZE) as f:
        f.truncate(size)


def _compute_ranges(total_bytes: int, num_parts: int) -> list[tuple[int, int]]:
    """Split ``total_bytes`` into ``num_parts`` inclusive byte ranges."""
    part_size = total_bytes // num_parts
    ranges: list[tuple[int, int]] = []
    cursor = 0
    for i in range(num_parts):
        end = (cursor + part_size - 1) if i < num_parts - 1 else (total_bytes - 1)
        if cursor > end:
            break
        ranges.append((cursor, end))
        cursor = end + 1
    return ranges


# ---------------------------------------------------------------------------
# Size probe
# ---------------------------------------------------------------------------


@dataclass
class _ProbeResult:
    total_bytes: int
    filename: str


async def _probe_size(
    client: "DataQueryClient",
    url: str,
    params: dict,
    file_group_id: str,
    file_datetime: Optional[str],
    global_semaphore: asyncio.Semaphore,
) -> Optional[_ProbeResult]:
    """Issue a 1-byte range probe to learn file size and filename.

    Returns ``None`` when the server doesn't honor ranges or the file is below
    the small-file threshold — caller should fall back to single-stream.
    """
    async with global_semaphore:
        async with await client._enter_request_cm("GET", url, params=params, headers=C.PROBE_HEADERS) as probe_resp:
            await client._handle_response(probe_resp)

            content_range = probe_resp.headers.get("Content-Range")
            if not (content_range and "/" in content_range):
                return None

            try:
                total_bytes = int(content_range.rsplit("/", 1)[1])
            except ValueError:
                total_bytes = int(probe_resp.headers.get("Content-Length", "0"))

            if total_bytes and total_bytes < C.SMALL_FILE_THRESHOLD:
                return None

            filename = get_filename_from_response(probe_resp, file_group_id, file_datetime)
            return _ProbeResult(total_bytes=total_bytes, filename=filename)


# ---------------------------------------------------------------------------
# Progress reporting
# ---------------------------------------------------------------------------


class _ProgressReporter:
    """Tracks total bytes downloaded and throttles callback/log dispatch.

    Safe to mutate from concurrent range workers because all increments happen
    on the event loop thread (chunks are read via ``async for``, so there are no
    preemption points inside ``add_bytes``).
    """

    def __init__(
        self,
        progress: DownloadProgress,
        total_bytes: int,
        progress_callback: Optional[Callable],
        show_progress: bool,
        file_group_id: str,
    ) -> None:
        self._progress = progress
        self._total_bytes = total_bytes
        self._callback = progress_callback
        self._show_progress = show_progress
        self._file_group_id = file_group_id
        self.bytes_downloaded = 0
        self._last_callback_bytes = 0
        self._last_callback_time = time.time()

    def add_bytes(self, n: int) -> None:
        self.bytes_downloaded += n
        self._maybe_dispatch()

    def rewind(self, n: int) -> None:
        """Subtract bytes written by a part that will be retried."""
        self.bytes_downloaded -= n

    def flush(self) -> None:
        self._progress.update_progress(self.bytes_downloaded)

    def _maybe_dispatch(self) -> None:
        # Byte threshold checked first to avoid time.time() syscall on every chunk.
        if not (
            self.bytes_downloaded - self._last_callback_bytes >= C.CALLBACK_BYTE_THRESHOLD
            or self.bytes_downloaded == self._total_bytes
            or time.time() - self._last_callback_time >= C.CALLBACK_TIME_THRESHOLD
        ):
            return

        self._last_callback_bytes = self.bytes_downloaded
        self._last_callback_time = time.time()
        self._progress.update_progress(self.bytes_downloaded)

        if self._callback:
            self._callback(self._progress)
        elif self._show_progress:
            logger.debug(
                "Download progress (parallel)",
                file=self._file_group_id,
                percentage=f"{self._progress.percentage:.1f}%",
                downloaded=_format_file_size(self.bytes_downloaded, precision=2, strict=True),
            )


# ---------------------------------------------------------------------------
# Range worker
# ---------------------------------------------------------------------------


@dataclass
class _RangeContext:
    """Immutable shared state passed to every range worker."""

    client: "DataQueryClient"
    url: str
    params: dict
    temp_path: Path
    chunk_size: int
    semaphore: asyncio.Semaphore
    timeout: aiohttp.ClientTimeout
    throttler: Optional[BandwidthThrottler]
    reporter: _ProgressReporter
    max_retries: int
    retry_delay: float


async def _download_range(ctx: _RangeContext, start_byte: int, end_byte: int) -> None:
    """Download one byte range with retries, writing into the preallocated temp file."""
    loop = asyncio.get_running_loop()
    range_headers = {"Range": f"bytes={start_byte}-{end_byte}"}
    part_bytes_written = 0

    for attempt in range(ctx.max_retries + 1):
        try:
            if attempt > 0 and part_bytes_written > 0:
                ctx.reporter.rewind(part_bytes_written)
                part_bytes_written = 0
                await asyncio.sleep(ctx.retry_delay * (2 ** (attempt - 1)))

            with open(ctx.temp_path, "r+b") as part_fh:
                async with ctx.semaphore:
                    async with await ctx.client._enter_request_cm(
                        "GET",
                        ctx.url,
                        params=ctx.params,
                        headers=range_headers,
                        timeout=ctx.timeout,
                    ) as resp:
                        await ctx.client._handle_response(resp)
                        current_pos = start_byte

                        async for chunk in resp.content.iter_chunked(ctx.chunk_size):
                            if ctx.throttler:
                                await ctx.throttler.throttle(len(chunk))

                            await loop.run_in_executor(None, _seek_write, part_fh, current_pos, chunk)

                            chunk_len = len(chunk)
                            current_pos += chunk_len
                            part_bytes_written += chunk_len
                            ctx.reporter.add_bytes(chunk_len)
            return
        except Exception:
            if attempt == ctx.max_retries:
                raise


# ---------------------------------------------------------------------------
# Finalization / salvage
# ---------------------------------------------------------------------------


def _salvage(
    client: "DataQueryClient",
    file_group_id: str,
    destination: Optional[Path],
    temp_destination: Optional[Path],
    total_bytes: int,
    bytes_downloaded: int,
    start_time: float,
) -> Optional[DownloadResult]:
    """Recover a partially-failed download.

    If all bytes arrived despite the failure, promote the temp file to the
    final destination and return a COMPLETED result. Otherwise unlink the
    temp file and return None.
    """
    try:
        if not (temp_destination and temp_destination.exists()):
            return None
        if total_bytes and bytes_downloaded >= total_bytes:
            if destination is None:
                destination = temp_destination.with_suffix("")
            temp_destination.replace(destination)
            return client._create_download_result(
                file_group_id,
                destination,
                total_bytes,
                bytes_downloaded,
                start_time,
                DownloadStatus.COMPLETED,
            )
        temp_destination.unlink(missing_ok=True)
    except OSError as cleanup_err:
        logger.warning(
            "Salvage cleanup failed",
            temp_path=str(temp_destination),
            error=str(cleanup_err),
        )
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def download_file_multipart(
    client: "DataQueryClient",
    file_group_id: str,
    file_datetime: Optional[str],
    options: DownloadOptions,
    num_parts: int,
    progress_callback: Optional[Callable] = None,
) -> DownloadResult:
    """Download one file via parallel HTTP range requests into a preallocated temp file.

    Each part runs concurrently without external concurrency control. Small
    files (< ``SMALL_FILE_THRESHOLD``) and servers that don't honor ranges fall
    back to ``client._download_file_single_stream``. On unrecoverable failure
    the partial temp file is salvaged when all bytes arrived, otherwise a
    ``FAILED`` result is returned.
    """
    params: dict = {"file-group-id": file_group_id}
    if file_datetime:
        params["file-datetime"] = file_datetime

    start_time = time.time()
    destination: Optional[Path] = None
    temp_destination: Optional[Path] = None
    total_bytes: int = 0
    reporter: Optional[_ProgressReporter] = None
    loop = asyncio.get_running_loop()

    try:
        url = client._build_files_api_url(C.API_GROUP_FILE_DOWNLOAD)

        async with await client._enter_request_cm("GET", url, params=params, headers=C.PROBE_HEADERS) as probe_resp:
            await client._handle_response(probe_resp)
            content_range = probe_resp.headers.get("content-range") or probe_resp.headers.get("Content-Range")
            if not (content_range and "/" in content_range):
                return await client._download_file_single_stream(
                    file_group_id=file_group_id,
                    file_datetime=file_datetime,
                    options=options,
                    progress_callback=progress_callback,
                )
            try:
                total_bytes = int(content_range.split("/")[-1])
            except ValueError:
                total_bytes = int(probe_resp.headers.get("content-length", "0"))

            if total_bytes and total_bytes < C.SMALL_FILE_THRESHOLD:
                return await client._download_file_single_stream(
                    file_group_id=file_group_id,
                    file_datetime=file_datetime,
                    options=options,
                    progress_callback=progress_callback,
                )

            filename = get_filename_from_response(probe_resp, file_group_id, file_datetime)
            destination = client._resolve_destination(options, file_group_id, filename)
            if destination.exists() and not options.overwrite_existing:
                raise FileExistsError(f"File already exists: {destination}")

        temp_destination = destination.with_suffix(destination.suffix + C.TEMP_SUFFIX)
        await loop.run_in_executor(None, _preallocate_file, temp_destination, total_bytes)

        progress = DownloadProgress(
            file_group_id=file_group_id,
            total_bytes=total_bytes,
            start_time=datetime.now(),
        )
        reporter = _ProgressReporter(
            progress=progress,
            total_bytes=total_bytes,
            progress_callback=progress_callback,
            show_progress=options.show_progress,
            file_group_id=file_group_id,
        )

        # One slot per part → all parts run concurrently without external throttling.
        ctx = _RangeContext(
            client=client,
            url=url,
            params=params,
            temp_path=temp_destination,
            chunk_size=options.chunk_size or C.DEFAULT_CHUNK_SIZE,
            semaphore=asyncio.Semaphore(num_parts),
            timeout=aiohttp.ClientTimeout(total=None, sock_read=client.config.timeout),
            throttler=None,
            reporter=reporter,
            max_retries=options.max_retries,
            retry_delay=options.retry_delay,
        )

        ranges = _compute_ranges(total_bytes, num_parts)
        await asyncio.gather(*(_download_range(ctx, s, e) for s, e in ranges))
        reporter.flush()
        temp_destination.replace(destination)

        return client._create_download_result(
            file_group_id,
            destination,
            total_bytes,
            reporter.bytes_downloaded,
            start_time,
            DownloadStatus.COMPLETED,
        )
    except FileExistsError as e:
        return client._create_download_result(
            file_group_id, destination, 0, 0, start_time, DownloadStatus.ALREADY_EXISTS, e
        )
    except Exception as e:
        bytes_downloaded = reporter.bytes_downloaded if reporter else 0
        salvaged = _salvage(
            client, file_group_id, destination, temp_destination, total_bytes, bytes_downloaded, start_time
        )
        if salvaged is not None:
            return salvaged
        return client._create_download_result(
            file_group_id, destination, 0, bytes_downloaded, start_time, DownloadStatus.FAILED, e
        )


async def download_file_parallel(
    client: "DataQueryClient",
    file_group_id: str,
    file_datetime: Optional[str],
    destination_path: Path,
    num_parts: int,
    global_semaphore: asyncio.Semaphore,
    progress_callback: Optional[Callable] = None,
) -> Optional[DownloadResult]:
    """Download one file using parallel range requests under a shared semaphore.

    Each HTTP range competes for ``global_semaphore``, so the caller controls
    total API-wide concurrency regardless of how many files are running in
    parallel. Falls back to a single-stream download for files under 10 MB or
    when the server doesn't advertise a content range.

    Returns a ``DownloadResult`` (``COMPLETED`` or ``ALREADY_EXISTS``) on
    success, or ``None`` on unrecoverable failure (already logged).
    """
    if file_datetime:
        validate_file_datetime(file_datetime)
    if not num_parts or num_parts <= 0:
        num_parts = 1

    download_options = DownloadOptions(
        destination_path=destination_path,
        overwrite_existing=client.config.overwrite_existing,
    )

    # Single part or range downloads disabled → skip the probe/preallocate overhead.
    if num_parts <= 1 or not client.config.enable_range_downloads:
        return await client.download_file_async(
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            options=download_options,
            progress_callback=progress_callback,
        )

    params: dict = {"file-group-id": file_group_id}
    if file_datetime:
        params["file-datetime"] = file_datetime

    start_time = time.time()
    destination: Optional[Path] = None
    temp_destination: Optional[Path] = None
    total_bytes = 0
    reporter: Optional[_ProgressReporter] = None
    loop = asyncio.get_running_loop()

    try:
        url = client._build_files_api_url(C.DOWNLOAD_API_PATH)

        # Step 1: probe size; fall back to single-stream when appropriate.
        probe = await _probe_size(client, url, params, file_group_id, file_datetime, global_semaphore)
        if probe is None:
            return await client.download_file_async(
                file_group_id=file_group_id,
                file_datetime=file_datetime,
                options=download_options,
                progress_callback=progress_callback,
            )
        total_bytes = probe.total_bytes

        # Step 2: resolve destination and check for existing file.
        destination = client._resolve_destination(download_options, file_group_id, probe.filename)
        if destination.exists() and not download_options.overwrite_existing:
            raise FileExistsError(f"File already exists: {destination}")

        # Step 3: preallocate temp file off the event loop.
        temp_destination = destination.with_suffix(destination.suffix + C.TEMP_SUFFIX)
        await loop.run_in_executor(None, _preallocate_file, temp_destination, total_bytes)

        # Step 4: build shared state and ranges.
        progress = DownloadProgress(
            file_group_id=file_group_id,
            total_bytes=total_bytes,
            start_time=datetime.now(),
        )
        reporter = _ProgressReporter(
            progress=progress,
            total_bytes=total_bytes,
            progress_callback=progress_callback,
            show_progress=download_options.show_progress,
            file_group_id=file_group_id,
        )

        throttler: Optional[BandwidthThrottler] = None
        if download_options.max_bandwidth_mbps:
            throttler = BandwidthThrottler(
                max_bytes_per_second=int(download_options.max_bandwidth_mbps * C.MBPS_TO_BYTES_PER_SECOND)
            )

        ctx = _RangeContext(
            client=client,
            url=url,
            params=params,
            temp_path=temp_destination,
            chunk_size=download_options.chunk_size or C.DEFAULT_CHUNK_SIZE,
            semaphore=global_semaphore,
            timeout=aiohttp.ClientTimeout(total=None, sock_read=client.config.timeout),
            throttler=throttler,
            reporter=reporter,
            max_retries=download_options.max_retries,
            retry_delay=download_options.retry_delay,
        )

        # Step 5: launch all ranges concurrently.
        ranges = _compute_ranges(total_bytes, num_parts)
        await asyncio.gather(*(_download_range(ctx, s, e) for s, e in ranges))
        reporter.flush()
        temp_destination.replace(destination)

        return client._create_download_result(
            file_group_id,
            destination,
            total_bytes,
            reporter.bytes_downloaded,
            start_time,
            DownloadStatus.COMPLETED,
        )

    except FileExistsError as e:
        return client._create_download_result(
            file_group_id,
            destination,
            0,
            0,
            start_time,
            DownloadStatus.ALREADY_EXISTS,
            e,
        )
    except Exception as e:
        bytes_downloaded = reporter.bytes_downloaded if reporter else 0
        salvaged = _salvage(
            client, file_group_id, destination, temp_destination, total_bytes, bytes_downloaded, start_time
        )
        if salvaged is not None:
            return salvaged

        logger.error("Flattened parallel download failed", file_group_id=file_group_id, error=str(e))
        return None


# ---------------------------------------------------------------------------
# Bulk orchestration with stagger + retry
# ---------------------------------------------------------------------------


def _file_id(file_info: dict) -> Optional[str]:
    return file_info.get("file-group-id", file_info.get("file_group_id"))


def _file_dt(file_info: dict) -> Optional[str]:
    return file_info.get("file-datetime", file_info.get("file_datetime"))


async def _download_one_with_stagger(
    client: "DataQueryClient",
    file_info: dict,
    destination_dir: Path,
    num_parts: int,
    global_semaphore: asyncio.Semaphore,
    delay_seconds: float,
    progress_callback: Optional[Callable],
) -> Optional[DownloadResult]:
    file_group_id = _file_id(file_info)
    file_datetime = _file_dt(file_info)
    if not file_group_id:
        logger.error("File info missing file-group-id", file_info=file_info)
        return None
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    try:
        result = await download_file_parallel(
            client=client,
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            destination_path=destination_dir,
            num_parts=num_parts,
            global_semaphore=global_semaphore,
            progress_callback=progress_callback,
        )
        logger.info(
            "Downloaded file (parallel ranges)",
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            status=result.status.value if result else "failed",
        )
        return result
    except Exception as e:
        logger.error(
            "Flattened parallel download failed",
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            error=str(e),
        )
        return None


def _classify(
    files: list[dict],
    results: list,
) -> tuple[list[DownloadResult], list[dict]]:
    succeeded: list[DownloadResult] = []
    failed: list[dict] = []
    for file_info, result in zip(files, results):
        if isinstance(result, BaseException):
            failed.append(file_info)
        elif (
            result
            and hasattr(result, "status")
            and hasattr(result, "file_group_id")
            and result.status.value in ("completed", "already_exists")
        ):
            succeeded.append(result)
        else:
            failed.append(file_info)
    return succeeded, failed


async def download_files_with_retry(
    client: "DataQueryClient",
    files: list[dict],
    destination_dir: Path,
    num_parts: int,
    global_semaphore: asyncio.Semaphore,
    intelligent_delay: float,
    base_retry_delay: float,
    max_retries: int,
    progress_callback: Optional[Callable] = None,
) -> tuple[list[DownloadResult], list[dict], int]:
    """Run a staggered, retrying batch of parallel-range downloads.

    Each file dispatches to :func:`download_file_parallel` under a shared
    ``global_semaphore`` so total in-flight HTTP requests stay capped across
    the batch. Files are launched ``intelligent_delay`` seconds apart to
    spread the burst against the API rate limiter. Failures are retried up
    to ``max_retries`` times with exponential backoff (``base_retry_delay``).

    Returns ``(successful, failed, retry_count)``.
    """

    async def _launch(batch: list[dict]) -> list:
        tasks = [
            _download_one_with_stagger(
                client=client,
                file_info=fi,
                destination_dir=destination_dir,
                num_parts=num_parts,
                global_semaphore=global_semaphore,
                delay_seconds=i * intelligent_delay,
                progress_callback=progress_callback,
            )
            for i, fi in enumerate(batch)
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)

    initial_results = await _launch(files)
    successful, failed = _classify(files, initial_results)

    retry_count = 0
    while failed and retry_count < max_retries:
        retry_count += 1
        logger.info(
            f"Retrying failed downloads (attempt {retry_count}/{max_retries})",
            failed_count=len(failed),
        )
        await asyncio.sleep(base_retry_delay * (2 ** (retry_count - 1)))

        retry_results = await _launch(failed)
        more_succeeded, still_failed = _classify(failed, retry_results)
        for r in more_succeeded:
            logger.info(
                "Retry succeeded for file",
                file_group_id=r.file_group_id,
                attempt=retry_count,
            )
        successful.extend(more_succeeded)
        failed = still_failed
        if not failed:
            logger.info("All failed downloads recovered after retries")
            break

    if failed:
        logger.warning(
            f"Some downloads failed after {max_retries} retries",
            failed_count=len(failed),
        )

    return successful, failed, retry_count
