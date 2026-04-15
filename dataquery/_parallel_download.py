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
from typing import TYPE_CHECKING, Callable, Optional

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


async def download_file_parallel_flattened(
    client: "DataQueryClient",
    file_group_id: str,
    file_datetime: Optional[str],
    destination_path: Path,
    num_parts: int,
    global_semaphore: asyncio.Semaphore,
    progress_callback: Optional[Callable] = None,
) -> Optional[DownloadResult]:
    """Download a file using parallel range requests under a shared semaphore.

    Each HTTP range competes for ``global_semaphore``, so the caller controls
    total API-wide concurrency regardless of how many files are running in
    parallel. Falls back to a single-stream download for files under 10MB or
    when the server doesn't advertise a content range.

    Returns a ``DownloadResult`` (``COMPLETED`` or ``ALREADY_EXISTS``) on
    success, or ``None`` on unrecoverable failure (already logged).
    """
    if file_datetime:
        validate_file_datetime(file_datetime)

    if num_parts is None or num_parts <= 0:
        num_parts = 1

    # Fast path: range downloads disabled → single-stream.
    if not client.config.enable_range_downloads:
        return await client.download_file_async(
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            options=DownloadOptions(
                destination_path=destination_path,
                overwrite_existing=client.config.overwrite_existing,
            ),
            progress_callback=progress_callback,
        )

    params: dict = {"file-group-id": file_group_id}
    if file_datetime:
        params["file-datetime"] = file_datetime

    download_options = DownloadOptions(
        destination_path=destination_path,
        overwrite_existing=client.config.overwrite_existing,
    )
    if download_options.destination_path:
        resolved_path = Path(download_options.destination_path)
        destination_dir = resolved_path.parent if resolved_path.suffix else resolved_path
    else:
        destination_dir = Path(client.config.download_dir)

    if download_options.create_directories:
        destination_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    bytes_downloaded = 0
    destination: Optional[Path] = None
    temp_destination: Optional[Path] = None
    total_bytes: int = 0

    try:
        # Step 1: probe file size with a 1-byte range request.
        url = client._build_files_api_url("group/file/download")
        probe_headers = {"Range": "bytes=0-0"}

        async with global_semaphore:
            async with await client._enter_request_cm("GET", url, params=params, headers=probe_headers) as probe_resp:
                await client._handle_response(probe_resp)
                content_range = probe_resp.headers.get("content-range") or probe_resp.headers.get("Content-Range")
                if content_range and "/" in content_range:
                    try:
                        total_bytes = int(content_range.split("/")[-1])
                    except Exception:
                        total_bytes = int(probe_resp.headers.get("content-length", "0"))
                else:
                    # Server didn't advertise a range — fall back to streaming.
                    return await client.download_file_async(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        options=download_options,
                        progress_callback=progress_callback,
                    )

                # Small files → single stream (the per-part overhead isn't worth it).
                ten_mb = 10 * 1024 * 1024
                if total_bytes and total_bytes < ten_mb:
                    return await client.download_file_async(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        options=download_options,
                        progress_callback=progress_callback,
                    )

                # Large files → scale parts up to 20.
                if total_bytes > 500 * 1024 * 1024:  # >500MB
                    num_parts = max(num_parts, min(total_bytes // (100 * 1024 * 1024), 20))

                filename = get_filename_from_response(probe_resp, file_group_id, file_datetime)
                destination = destination_dir / filename

                if destination.exists() and not download_options.overwrite_existing:
                    raise FileExistsError(f"File already exists: {destination}")

        # Step 2: preallocate temp file for random-access writes.
        temp_destination = destination.with_suffix(destination.suffix + ".part")
        with open(temp_destination, "wb", buffering=1024 * 1024) as f:
            f.truncate(total_bytes)

        # Step 3: compute byte ranges for each part.
        part_size = total_bytes // num_parts
        ranges: list[tuple[int, int]] = []
        start = 0
        for i in range(num_parts):
            end = (start + part_size - 1) if i < num_parts - 1 else (total_bytes - 1)
            if start > end:
                break
            ranges.append((start, end))
            start = end + 1

        # Chunk size scales with part size.
        chunk_size = download_options.chunk_size or 1048576
        if part_size > 100 * 1024 * 1024:
            chunk_size = max(chunk_size, 4 * 1024 * 1024)
        elif part_size > 50 * 1024 * 1024:
            chunk_size = max(chunk_size, 2 * 1024 * 1024)

        progress = DownloadProgress(
            file_group_id=file_group_id,
            total_bytes=total_bytes,
            start_time=datetime.now(),
        )

        bytes_lock = asyncio.Lock()
        last_callback_bytes = 0
        last_callback_time = time.time()
        callback_threshold_bytes = 1024 * 1024  # 1MB
        callback_threshold_time = 0.5  # seconds

        max_part_retries = download_options.max_retries
        part_retry_delay = download_options.retry_delay

        # Disable total timeout for large file parts; keep per-socket-read timeout.
        range_timeout = aiohttp.ClientTimeout(total=None, sock_read=client.config.timeout)
        loop = asyncio.get_running_loop()

        throttler: Optional[BandwidthThrottler] = None
        if download_options.max_bandwidth_mbps:
            throttler = BandwidthThrottler(max_bytes_per_second=int(download_options.max_bandwidth_mbps * 125000))

        async def download_range(start_byte: int, end_byte: int) -> None:
            nonlocal bytes_downloaded, last_callback_bytes, last_callback_time
            range_headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            part_bytes_written = 0  # rollback accounting across retries

            for attempt in range(max_part_retries + 1):
                try:
                    if attempt > 0 and part_bytes_written > 0:
                        async with bytes_lock:
                            bytes_downloaded -= part_bytes_written
                            progress.update_progress(bytes_downloaded)
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

                                def _seek_write(fh, pos, data):
                                    fh.seek(pos)
                                    fh.write(data)

                                async for chunk in resp.content.iter_chunked(chunk_size):
                                    if throttler:
                                        await throttler.throttle(len(chunk))

                                    await loop.run_in_executor(
                                        None,
                                        _seek_write,
                                        part_fh,
                                        current_pos,
                                        chunk,
                                    )

                                    chunk_len = len(chunk)
                                    current_pos += chunk_len
                                    part_bytes_written += chunk_len

                                    should_callback = False
                                    async with bytes_lock:
                                        bytes_downloaded += chunk_len
                                        progress.update_progress(bytes_downloaded)

                                        current_time = time.time()
                                        bytes_diff = bytes_downloaded - last_callback_bytes
                                        time_diff = current_time - last_callback_time

                                        should_callback = (
                                            bytes_diff >= callback_threshold_bytes
                                            or time_diff >= callback_threshold_time
                                            or bytes_downloaded == total_bytes
                                        )

                                        if should_callback:
                                            last_callback_bytes = bytes_downloaded
                                            last_callback_time = current_time

                                    if should_callback:
                                        if progress_callback:
                                            progress_callback(progress)
                                        elif download_options.show_progress:
                                            logger.info(
                                                "Download progress (flattened)",
                                                file=file_group_id,
                                                percentage=f"{progress.percentage:.1f}%",
                                                downloaded=_format_file_size(
                                                    bytes_downloaded, precision=2, strict=True
                                                ),
                                            )
                    return  # part complete
                except Exception:
                    if attempt == max_part_retries:
                        raise

        # Step 4: launch all ranges concurrently.
        await asyncio.gather(*(download_range(s, e) for s, e in ranges))

        temp_destination.replace(destination)
        download_time = time.time() - start_time
        return DownloadResult(
            file_group_id=file_group_id,
            group_id="",
            local_path=destination,
            file_size=total_bytes,
            download_time=download_time,
            bytes_downloaded=bytes_downloaded,
            status=DownloadStatus.COMPLETED,
            error_message=None,
        )

    except FileExistsError as e:
        return DownloadResult(
            file_group_id=file_group_id,
            group_id="",
            local_path=destination,
            file_size=0,
            download_time=time.time() - start_time,
            bytes_downloaded=0,
            status=DownloadStatus.ALREADY_EXISTS,
            error_message=f"FileExistsError: {e}",
        )
    except Exception as e:
        # On error, salvage the file if all bytes arrived; otherwise clean up.
        try:
            if temp_destination and temp_destination.exists():
                if total_bytes and bytes_downloaded >= total_bytes:
                    if destination is None:
                        destination = temp_destination.with_suffix("")
                    temp_destination.replace(destination)
                    return DownloadResult(
                        file_group_id=file_group_id,
                        group_id="",
                        local_path=destination,
                        file_size=total_bytes,
                        download_time=time.time() - start_time,
                        bytes_downloaded=bytes_downloaded,
                        status=DownloadStatus.COMPLETED,
                        error_message=None,
                    )
                else:
                    temp_destination.unlink(missing_ok=True)
        except Exception:
            pass

        logger.error(
            "Flattened parallel download failed for file",
            file_group_id=file_group_id,
            error=str(e),
        )
        return None
