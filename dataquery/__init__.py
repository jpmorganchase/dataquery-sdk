"""DATAQUERY SDK - Python SDK for DATAQUERY Data API."""

__version__ = "1.2.3"
__author__ = "DATAQUERY SDK Team"
__email__ = "dataquery_support@jpmorgan.com"
__license__ = "MIT"
__url__ = "https://github.com/jpmorganchase/dataquery-sdk"

from .config import EnvConfig
from .core import DataQueryClient
from .dataquery import DataQuery
from .function_registry import (
    format_function_syntax,
    get_function_categories,
    get_function_param_counts,
    get_function_registry,
    list_functions_by_category,
    lookup_function,
)
from .sse.client import SSEClient, SSEEvent
from .sse.subscriber import NotificationDownloadManager
from .types.exceptions import (
    APIResponseError,
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
    PaginationError,
    RateLimitError,
    ValidationError,
    WorkflowError,
)
from .types.models import (
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
    Link,
    Paginated,
)
from .utils import download_zip_async

__all__ = [
    "DataQuery",
    "DataQueryClient",
    "ClientConfig",
    "Group",
    "GroupList",
    "Paginated",
    "Link",
    "FileInfo",
    "FileList",
    "AvailabilityInfo",
    "AvailableFilesResponse",
    "DownloadResult",
    "DownloadStatus",
    "DownloadOptions",
    "DownloadProgress",
    "DateRange",
    "DataQueryError",
    "APIResponseError",
    "AuthenticationError",
    "ValidationError",
    "NotFoundError",
    "PaginationError",
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
    "EnvConfig",
    "NotificationDownloadManager",
    "SSEClient",
    "SSEEvent",
    "format_function_syntax",
    "get_function_categories",
    "get_function_param_counts",
    "get_function_registry",
    "list_functions_by_category",
    "lookup_function",
    "download_zip_async",
]

__version_info__ = tuple(int(x) for x in __version__.split("."))

__package_info__ = {
    "name": "dataquery-sdk",
    "version": __version__,
    "author": __author__,
    "email": __email__,
    "license": __license__,
    "url": __url__,
    "description": "Python SDK for DATAQUERY Data API - Query, download, and check availability of economic data files",
    "python_requires": ">=3.12",
}
