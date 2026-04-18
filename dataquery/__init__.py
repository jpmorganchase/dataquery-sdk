"""
DATAQUERY SDK - Python SDK for DATAQUERY Data API

A high-performance Python SDK for the DATAQUERY Data API, providing seamless access
to economic data files with advanced features like querying, downloading, availability
checking, rate limiting, retry logic, connection pool monitoring, and comprehensive logging.

Quick Start:
    >>> from dataquery import DataQuery
    >>> async with DataQuery() as dq:
    ...     groups = await dq.list_groups_async()
    ...     print(f"Found {len(groups)} groups")

For more information, visit: https://github.com/dataquery/dataquery-sdk
"""

__version__ = "0.1.5"
__author__ = "DATAQUERY SDK Team"
__email__ = "dataquery_support@jpmorgan.com"
__license__ = "MIT"
__url__ = "https://github.com/jpmorganchase/dataquery-sdk"

# Core
from .client import DataQueryClient
from .config import EnvConfig
from .dataquery import DataQuery
from .exceptions import (
    AuthenticationError,
    AvailabilityError,
    ConfigurationError,
    DataQueryError,
    DateRangeError,
    DownloadError,
    FileNotFoundInGroupError,
    FileTypeError,
    GroupNotFoundError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
    WorkflowError,
)
from .models import (
    AvailabilityInfo,
    AvailableFilesResponse,
    ClientConfig,
    DateRange,
    DownloadOptions,
    DownloadProgress,
    DownloadResult,
    DownloadStatus,
    FileInfo,
    FileList,
    Group,
    GroupList,
)
from .sse.client import SSEClient, SSEEvent
from .sse.subscriber import NotificationDownloadManager

# Type aliases for convenience
__all__ = [
    # Core
    "DataQuery",
    "DataQueryClient",
    # Models
    "ClientConfig",
    "Group",
    "GroupList",
    "FileInfo",
    "FileList",
    "AvailabilityInfo",
    "AvailableFilesResponse",
    "DownloadResult",
    "DownloadStatus",
    "DownloadOptions",
    "DownloadProgress",
    "DateRange",
    # Exceptions
    "DataQueryError",
    "AuthenticationError",
    "ValidationError",
    "NotFoundError",
    "RateLimitError",
    "NetworkError",
    "ConfigurationError",
    "DownloadError",
    "AvailabilityError",
    "GroupNotFoundError",
    "FileNotFoundInGroupError",
    "DateRangeError",
    "FileTypeError",
    "WorkflowError",
    # Configuration
    "EnvConfig",
    # SSE notification-driven download
    "NotificationDownloadManager",
    "SSEClient",
    "SSEEvent",
]

# Version info
__version_info__ = tuple(int(x) for x in __version__.split("."))

# Package metadata
__package_info__ = {
    "name": "dataquery-sdk",
    "version": __version__,
    "author": __author__,
    "email": __email__,
    "license": __license__,
    "url": __url__,
    "description": "Python SDK for DATAQUERY Data API - Query, download, and check availability of economic data files",
    "python_requires": ">=3.10",
}
