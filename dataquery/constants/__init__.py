"""Internal tunable constants used across the SDK.

Centralizing these values here avoids drift between code paths that must agree
(e.g. the single-stream and parallel downloaders share thresholds) and gives a
single place to adjust tunables. Pydantic model field defaults and validation
bounds are intentionally NOT hoisted here — those are part of the public
schema.

Constants are organized into topical submodules; this ``__init__`` re-exports
them so existing call sites using ``from . import constants as C`` keep
working without change. New code may also import from a specific submodule,
e.g. ``from dataquery.constants.api import API_GROUPS``.
"""

from __future__ import annotations

from .api import (
    API_EXPRESSIONS_TIME_SERIES,
    API_GRID_DATA,
    API_GROUP_ATTRIBUTES,
    API_GROUP_FILE_AVAILABILITY,
    API_GROUP_FILE_DOWNLOAD,
    API_GROUP_FILES,
    API_GROUP_FILES_AVAILABLE,
    API_GROUP_FILTERS,
    API_GROUP_INSTRUMENTS,
    API_GROUP_INSTRUMENTS_SEARCH,
    API_GROUP_TIME_SERIES,
    API_GROUPS,
    API_GROUPS_SEARCH,
    API_HEARTBEAT,
    API_INSTRUMENTS_TIME_SERIES,
    DOWNLOAD_API_PATH,
    SSE_NOTIFICATION_PATH,
)
from .download import (
    CALLBACK_BYTE_THRESHOLD,
    CALLBACK_TIME_THRESHOLD,
    DEFAULT_CHUNK_SIZE,
    LARGE_FILE_CHUNK_SIZE,
    LARGE_FILE_THRESHOLD,
    MBPS_TO_BYTES_PER_SECOND,
    PREALLOC_BUFFER_SIZE,
    PROBE_HEADERS,
    SMALL_FILE_THRESHOLD,
    TEMP_SUFFIX,
)
from .rate_limit import RATE_LIMIT_MIN_WAIT_SECONDS
from .sse import SSE_HEALTHY_CONNECTION_SECONDS

__all__ = [
    # api
    "API_EXPRESSIONS_TIME_SERIES",
    "API_GRID_DATA",
    "API_GROUPS",
    "API_GROUPS_SEARCH",
    "API_GROUP_ATTRIBUTES",
    "API_GROUP_FILES",
    "API_GROUP_FILES_AVAILABLE",
    "API_GROUP_FILE_AVAILABILITY",
    "API_GROUP_FILE_DOWNLOAD",
    "API_GROUP_FILTERS",
    "API_GROUP_INSTRUMENTS",
    "API_GROUP_INSTRUMENTS_SEARCH",
    "API_GROUP_TIME_SERIES",
    "API_HEARTBEAT",
    "API_INSTRUMENTS_TIME_SERIES",
    "DOWNLOAD_API_PATH",
    "SSE_NOTIFICATION_PATH",
    # download
    "CALLBACK_BYTE_THRESHOLD",
    "CALLBACK_TIME_THRESHOLD",
    "DEFAULT_CHUNK_SIZE",
    "LARGE_FILE_CHUNK_SIZE",
    "LARGE_FILE_THRESHOLD",
    "MBPS_TO_BYTES_PER_SECOND",
    "PREALLOC_BUFFER_SIZE",
    "PROBE_HEADERS",
    "SMALL_FILE_THRESHOLD",
    "TEMP_SUFFIX",
    # rate_limit
    "RATE_LIMIT_MIN_WAIT_SECONDS",
    # sse
    "SSE_HEALTHY_CONNECTION_SECONDS",
]
