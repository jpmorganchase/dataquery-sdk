"""Tunables for the single-stream and parallel download machinery."""

from __future__ import annotations

SMALL_FILE_THRESHOLD = 10 * 1024 * 1024

LARGE_FILE_THRESHOLD = 1024 * 1024 * 1024

DEFAULT_CHUNK_SIZE = 1024 * 1024

LARGE_FILE_CHUNK_SIZE = 8 * 1024 * 1024

PREALLOC_BUFFER_SIZE = 1024 * 1024


CALLBACK_BYTE_THRESHOLD = 1024 * 1024
CALLBACK_TIME_THRESHOLD = 0.5


PROBE_HEADERS = {"Range": "bytes=0-0"}


TEMP_SUFFIX = ".part"


MBPS_TO_BYTES_PER_SECOND = 125_000


DEFAULT_WRITTEN_RESEARCH_CHUNK_DAYS: int = 7

NO_FILES_FOUND_ERROR: str = "No available files found for date range"
