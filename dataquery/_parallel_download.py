"""
Flattened-concurrency parallel downloader.

Downloads one file as ``num_parts`` parallel HTTP range requests, where each
range competes for a single ``global_semaphore`` shared across all files being
downloaded concurrently. This gives true flattened concurrency where the total
in-flight HTTP requests = ``max_concurrent × num_parts`` rather than a
hierarchical file-then-parts model.

Used by ``DataQuery.run_group_download_async`` for bulk date-range downloads.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Callable, Optional

import aiohttp
import structlog

from .client import get_filename_from_response, validate_file_datetime
from .models import (
    BandwidthThrottler,
    DownloadOptions,
    DownloadProgress,
    DownloadResult,
    DownloadStatus,
)
from .utils import format_file_size as _format_file_size

if TYPE_CHECKING:
    from .client import DataQueryClient

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

_SMALL_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB — below this, single-stream wins
_PROBE_HEADERS = {"Range": "bytes=0-0"}
_DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MB
_CALLBACK_BYTE_THRESHOLD = 1024 * 1024  # invoke callback every 1 MB at most
_CALLBACK_TIME_THRESHOLD = 0.5  # ...or every 500 ms, whichever first


def _seek_write(fh: IO[bytes], pos: int, data: bytes) -> None:
    """Sync seek+write; runs in the default thread executor."""
    fh.seek(pos)
    fh.write(data)


def _preallocate_file(path: Path, size: int) -> None:
    """Sync truncate to ``size`` bytes; runs in the default thread executor."""
    with open(path, "wb", buffering=1024 * 1024) as f:
        f.truncate(size)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def download_file_parallel_flattened(
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
    bytes_downloaded = 0
    destination: Optional[Path] = None
    temp_destination: Optional[Path] = None
    total_bytes = 0
    loop = asyncio.get_running_loop()

    try:
        url = client._build_files_api_url("group/file/download")

        # Step 1: probe file size with a 1-byte range request.
        async with global_semaphore:
            async with await client._enter_request_cm("GET", url, params=params, headers=_PROBE_HEADERS) as probe_resp:
                await client._handle_response(probe_resp)

                content_range = probe_resp.headers.get("Content-Range")
                if content_range and "/" in content_range:
                    try:
                        total_bytes = int(content_range.rsplit("/", 1)[1])
                    except ValueError:
                        total_bytes = int(probe_resp.headers.get("Content-Length", "0"))
                else:
                    # Server didn't honor the range — fall back to streaming.
                    return await client.download_file_async(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        options=download_options,
                        progress_callback=progress_callback,
                    )

                # Small files → single stream (per-part overhead isn't worth it).
                if total_bytes and total_bytes < _SMALL_FILE_THRESHOLD:
                    return await client.download_file_async(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        options=download_options,
                        progress_callback=progress_callback,
                    )

                filename = get_filename_from_response(probe_resp, file_group_id, file_datetime)

        # Step 2: resolve destination and check for existing file.
        destination = client._resolve_destination(download_options, file_group_id, filename)

        if destination.exists() and not download_options.overwrite_existing:
            raise FileExistsError(f"File already exists: {destination}")

        # Step 3: preallocate temp file off the event loop.
        temp_destination = destination.with_suffix(destination.suffix + ".part")
        await loop.run_in_executor(None, _preallocate_file, temp_destination, total_bytes)

        # Step 4: compute byte ranges.
        part_size = total_bytes // num_parts
        ranges: list[tuple[int, int]] = []
        cursor = 0
        for i in range(num_parts):
            end = (cursor + part_size - 1) if i < num_parts - 1 else (total_bytes - 1)
            if cursor > end:
                break
            ranges.append((cursor, end))
            cursor = end + 1

        chunk_size = download_options.chunk_size or _DEFAULT_CHUNK_SIZE

        progress = DownloadProgress(
            file_group_id=file_group_id,
            total_bytes=total_bytes,
            start_time=datetime.now(),
        )

        last_callback_bytes = 0
        last_callback_time = time.time()
        max_part_retries = download_options.max_retries
        part_retry_delay = download_options.retry_delay
        range_timeout = aiohttp.ClientTimeout(total=None, sock_read=client.config.timeout)

        throttler: Optional[BandwidthThrottler] = None
        if download_options.max_bandwidth_mbps:
            throttler = BandwidthThrottler(max_bytes_per_second=int(download_options.max_bandwidth_mbps * 125000))

        show_progress = download_options.show_progress
        run_in_executor = loop.run_in_executor
        update_progress = progress.update_progress

        async def download_range(start_byte: int, end_byte: int) -> None:
            nonlocal bytes_downloaded, last_callback_bytes, last_callback_time
            range_headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            part_bytes_written = 0

            for attempt in range(max_part_retries + 1):
                try:
                    if attempt > 0 and part_bytes_written > 0:
                        bytes_downloaded -= part_bytes_written
                        part_bytes_written = 0
                        await asyncio.sleep(part_retry_delay * (2 ** (attempt - 1)))

                    with open(temp_destination, "r+b") as part_fh:
                        async with global_semaphore:
                            async with await client._enter_request_cm(
                                "GET",
                                url,
                                params=params,
                                headers=range_headers,
                                timeout=range_timeout,
                            ) as resp:
                                await client._handle_response(resp)
                                current_pos = start_byte

                                async for chunk in resp.content.iter_chunked(chunk_size):
                                    if throttler:
                                        await throttler.throttle(len(chunk))

                                    await run_in_executor(None, _seek_write, part_fh, current_pos, chunk)

                                    chunk_len = len(chunk)
                                    current_pos += chunk_len
                                    part_bytes_written += chunk_len
                                    bytes_downloaded += chunk_len

                                    # Byte threshold checked first to avoid time.time() syscall on every chunk.
                                    if (
                                        bytes_downloaded - last_callback_bytes >= _CALLBACK_BYTE_THRESHOLD
                                        or bytes_downloaded == total_bytes
                                        or time.time() - last_callback_time >= _CALLBACK_TIME_THRESHOLD
                                    ):
                                        last_callback_bytes = bytes_downloaded
                                        last_callback_time = time.time()
                                        update_progress(bytes_downloaded)
                                        if progress_callback:
                                            progress_callback(progress)
                                        elif show_progress:
                                            logger.debug(
                                                "Download progress (flattened)",
                                                file=file_group_id,
                                                percentage=f"{progress.percentage:.1f}%",
                                                downloaded=_format_file_size(
                                                    bytes_downloaded, precision=2, strict=True
                                                ),
                                            )
                    return
                except Exception:
                    if attempt == max_part_retries:
                        raise

        # Step 5: launch all ranges concurrently.
        await asyncio.gather(*(download_range(s, e) for s, e in ranges))
        update_progress(bytes_downloaded)
        temp_destination.replace(destination)

        return client._create_download_result(
            file_group_id,
            destination,
            total_bytes,
            bytes_downloaded,
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
        # Salvage: if all bytes arrived despite the error, finalize the file.
        try:
            if temp_destination and temp_destination.exists():
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
                else:
                    temp_destination.unlink(missing_ok=True)
        except Exception:
            pass

        logger.error("Flattened parallel download failed", file_group_id=file_group_id, error=str(e))
        return None
