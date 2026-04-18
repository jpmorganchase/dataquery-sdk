"""Download machinery: file-existence helpers, progress wrapping, and the
flattened-concurrency parallel range downloader."""

from __future__ import annotations

from .parallel import download_file_parallel_flattened
from .utils import create_progress_wrapper, download_and_track, file_exists_locally

__all__ = [
    "create_progress_wrapper",
    "download_and_track",
    "download_file_parallel_flattened",
    "file_exists_locally",
]
