"""Tunables for the single-stream and parallel download machinery."""

from __future__ import annotations

# Byte-size thresholds and chunk sizes --------------------------------------

# Files below this size always use single-stream downloads; per-part overhead
# isn't worth it for small files.
SMALL_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB

# Files above this size use the larger chunk size below (bigger buffers reduce
# syscall overhead when streaming many GBs).
LARGE_FILE_THRESHOLD = 1024 * 1024 * 1024  # 1 GB

# Default chunk size for streaming reads when DownloadOptions.chunk_size is
# unset. Also the minimum allowed chunk size in the model validator.
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MB

# Chunk size used for large files (>= LARGE_FILE_THRESHOLD). Also the maximum
# allowed chunk size in the model validator.
LARGE_FILE_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# Buffer size used when preallocating the temp file via truncate().
PREALLOC_BUFFER_SIZE = 1024 * 1024  # 1 MB


# Progress-callback throttling ----------------------------------------------

# Dispatch the progress callback at most every N bytes OR every N seconds,
# whichever comes first. The byte threshold is checked before the time
# threshold to avoid a time.time() syscall on every chunk.
CALLBACK_BYTE_THRESHOLD = 1024 * 1024  # 1 MB
CALLBACK_TIME_THRESHOLD = 0.5  # seconds


# HTTP headers --------------------------------------------------------------

# Header used to probe a file's total size via a cheap 1-byte range request.
PROBE_HEADERS = {"Range": "bytes=0-0"}


# Filesystem conventions ----------------------------------------------------

# Suffix appended to the final destination path while a download is in flight.
TEMP_SUFFIX = ".part"


# Unit conversions ----------------------------------------------------------

# Megabits-per-second → bytes-per-second (1 Mbps = 1_000_000 bits/s = 125_000 bytes/s).
MBPS_TO_BYTES_PER_SECOND = 125_000
