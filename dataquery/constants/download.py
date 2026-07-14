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


# Default size, in days, of each chunk when splitting a date range so that
# the available-files endpoint is queried over a smaller window per call.
# Used by ``split_date_range_into_chunks``. The endpoint caps each call at
# one calendar month; smaller windows just keep responses lighter. (The old
# group-level limits, e.g. 14 days for RESEARCH_EQUITY_ALL, were lifted
# server-side.)
DEFAULT_WRITTEN_RESEARCH_CHUNK_DAYS: int = 7

# Error string set on the OperationReport when the available-files endpoint
# returns nothing for a date window. Chunked workflows match on it to tell
# a quiet window apart from a real failure.
NO_FILES_FOUND_ERROR: str = "No available files found for date range"
