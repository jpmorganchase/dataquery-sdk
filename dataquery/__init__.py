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

For more information, visit: https://github.com/dataquery/dataquery-sdk-python
"""

__version__ = "1.0.0"
__author__ = "DATAQUERY SDK Team"
__email__ = "support@dataquery.com"
__license__ = "MIT"
__url__ = "https://github.com/dataquery/dataquery-sdk-python"

# Core imports
from .dataquery import DataQuery, setup_logging
from .models import (
    ClientConfig,
    Group,
    FileInfo,
    FileList,
    AvailabilityInfo,
    DownloadResult,
    DownloadStatus,
    DownloadOptions,
    DownloadProgress,
    OAuthToken,
    TokenRequest,
    TokenResponse,
    DateRange,
    GroupList,
    AvailableFilesResponse,
)
from .exceptions import (
    DataQueryError,
    AuthenticationError,
    ValidationError,
    NotFoundError,
    RateLimitError,
    NetworkError,
    ConfigurationError,
    DownloadError,
    AvailabilityError,
    GroupNotFoundError,
    FileNotFoundError,
    DateRangeError,
    FileTypeError,
    WorkflowError,
)

# Client imports
from .client import DataQueryClient

# Configuration imports
from .config import EnvConfig

# Utility imports
from .utils import (
    create_env_template,
    save_config_to_env,
    load_env_file,
    get_env_value,
    set_env_value,
    validate_env_config,
    ensure_directory,
    get_download_paths,
)

# Rate limiting imports
from .rate_limiter import (
    TokenBucketRateLimiter,
    EnhancedTokenBucketRateLimiter,
    RateLimitConfig,
    RateLimitContext,
    QueuePriority,
    create_rate_limiter,
)

# Retry imports
from .retry import (
    RetryManager,
    RetryConfig,
    RetryStrategy,
    CircuitBreaker,
    CircuitState,
)

# Connection pool imports
from .connection_pool import (
    ConnectionPoolMonitor,
    ConnectionPoolConfig,
    ConnectionPoolStats,
)

# Logging imports
from .logging_config import (
    LoggingManager,
    LoggingConfig,
    LogLevel,
    LogFormat,
)

# Authentication imports
from .auth import (
    OAuthManager,
    TokenManager,
)

# Auto-download imports
from .auto_download import AutoDownloadManager

# Common utility helpers
from .utils import (
    format_file_size,
    format_duration,
)

# Type aliases for convenience
__all__ = [
    # Core
    "DataQuery",
    "setup_logging",
    
    # Models
    "ClientConfig",
    "Group",
    "FileInfo",
    "FileList",
    "AvailabilityInfo",
    "DownloadResult",
    "DownloadStatus",
    "DownloadOptions",
    "DownloadProgress",
    "OAuthToken",
    "TokenRequest",
    "TokenResponse",
    "DateRange",
    "GroupList",
    "AvailableFilesResponse",
    
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
    "FileNotFoundError",
    "DateRangeError",
    "FileTypeError",
    "WorkflowError",
    
    # Client
    "DataQueryClient",
    "format_file_size",
    "format_duration",
    "AutoDownloadManager",
    
    # Configuration
    "EnvConfig",
    
    # Utilities
    "create_env_template",
    "save_config_to_env",
    "load_env_file",
    "get_env_value",
    "set_env_value",
    "validate_env_config",
    "ensure_directory",
    "get_download_paths",
    
    # Rate Limiting
    "TokenBucketRateLimiter",
    "EnhancedTokenBucketRateLimiter",
    "RateLimitConfig",
    "RateLimitContext",
    "QueuePriority",
    "create_rate_limiter",
    
    # Retry
    "RetryManager",
    "RetryConfig",
    "RetryStrategy",
    "CircuitBreaker",
    "CircuitState",
    
    # Connection Pool
    "ConnectionPoolMonitor",
    "ConnectionPoolConfig",
    "ConnectionPoolStats",
    
    # Logging
    "LoggingManager",
    "LoggingConfig",
    "LogLevel",
    "LogFormat",
    
    # Authentication
    "OAuthManager",
    "TokenManager",
]

# Version info
__version_info__ = tuple(int(x) for x in __version__.split('.'))

# Package metadata
__package_info__ = {
    "name": "dataquery-sdk",
    "version": __version__,
    "author": __author__,
    "email": __email__,
    "license": __license__,
    "url": __url__,
    "description": "Python SDK for DATAQUERY Data API - Query, download, and check availability of economic data files",
    "python_requires": ">=3.11",
} 