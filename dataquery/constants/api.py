"""API endpoint paths.

Paths are relative to the main API base URL (``_build_api_url``) or the files
API base URL (``_build_files_api_url``) unless otherwise noted.
"""

from __future__ import annotations

# Main API -------------------------------------------------------------------

API_GROUPS = "groups"
API_GROUPS_SEARCH = "groups/search"
API_GROUP_INSTRUMENTS = "group/instruments"
API_GROUP_INSTRUMENTS_SEARCH = "group/instruments/search"
API_GROUP_FILTERS = "group/filters"
API_GROUP_ATTRIBUTES = "group/attributes"
API_INSTRUMENTS_TIME_SERIES = "instruments/time-series"
API_EXPRESSIONS_TIME_SERIES = "expressions/time-series"
API_GROUP_TIME_SERIES = "group/time-series"
API_GRID_DATA = "grid-data"
API_HEARTBEAT = "services/heartbeat"

# Files API ------------------------------------------------------------------

API_GROUP_FILES = "group/files"
API_GROUP_FILE_AVAILABILITY = "group/file/availability"
API_GROUP_FILE_DOWNLOAD = "group/file/download"
API_GROUP_FILES_AVAILABLE = "group/files/available-files"

# Backwards-compatible alias.
DOWNLOAD_API_PATH = API_GROUP_FILE_DOWNLOAD

# SSE (appended to the files-API base URL, with a leading slash) -------------

SSE_NOTIFICATION_PATH = "/events/notification"
