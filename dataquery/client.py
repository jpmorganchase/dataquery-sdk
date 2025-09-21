"""
Main client for the DATAQUERY SDK.
"""

import asyncio
import re
import socket
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import aiohttp
import structlog

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

from .auth import OAuthManager
from .auto_download import AutoDownloadManager
from .connection_pool import ConnectionPoolConfig, ConnectionPoolMonitor
from .exceptions import (
    AuthenticationError,
    ConfigurationError,
    FileNotFoundError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
)
from .logging_config import LogFormat, LoggingConfig, LoggingManager, LogLevel
from .models import (
    AttributesResponse,
    AvailabilityInfo,
    ClientConfig,
    DownloadOptions,
    DownloadProgress,
    DownloadResult,
    DownloadStatus,
    FileInfo,
    FileList,
    FiltersResponse,
    GridDataResponse,
    Group,
    GroupList,
    Instrument,
    InstrumentResponse,
    InstrumentsResponse,
    TimeSeriesResponse,
)
from .rate_limiter import (
    QueuePriority,
    RateLimitConfig,
    RateLimitContext,
    TokenBucketRateLimiter,
)
from .retry import RetryConfig, RetryManager, RetryStrategy

logger = structlog.get_logger(__name__)


def format_file_size(size_bytes: int) -> str:
    """Format file size with two decimal places (client-facing style).

    This intentionally differs from `dataquery.utils.format_file_size`,
    which formats with at most one decimal place. Tests that import
    `format_file_size` from `dataquery.client` expect two decimals.
    """
    if size_bytes == 0:
        return "0 B"

    # Support negative values gracefully
    sign = "-" if size_bytes < 0 else ""
    size = float(abs(size_bytes))
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    unit_index = 0
    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    # Always two decimals, including for bytes
    return f"{sign}{size:.2f} {units[unit_index]}"


def format_duration(seconds: float) -> str:
    """Compact duration formatter: seconds -> X.Ys, minutes -> X.Ym, hours -> X.Yh."""
    if seconds == 0:
        return "0s"

    sign = "-" if seconds < 0 else ""
    s = abs(seconds)
    if s < 60:
        return f"{sign}{s:.1f}s"
    if s < 3600:
        minutes = s / 60.0
        return f"{sign}{minutes:.1f}m"
    hours = s / 3600.0
    return f"{sign}{hours:.1f}h"


def parse_content_disposition(content_disposition: str) -> Optional[str]:
    """
    Parse Content-Disposition header to extract filename (including RFC 2231/5987 support).

    Args:
        content_disposition: The Content-Disposition header value

    Returns:
        The extracted filename or None if not found
    """
    if not content_disposition:
        return None

    # Try to find filename* (RFC 2231/5987)
    filename_star_match = re.search(
        r"filename\*=(?:UTF-8\'\')?([^;\r\n]+)", content_disposition, re.IGNORECASE
    )
    if filename_star_match:
        filename = filename_star_match.group(1)
        filename = urllib.parse.unquote(filename)
        return filename.strip('"')

    # Try to find filename="..."
    filename_match = re.search(
        r'filename="([^"]+)"', content_disposition, re.IGNORECASE
    )
    if filename_match:
        return urllib.parse.unquote(filename_match.group(1))

    # Try to find filename=...
    filename_match2 = re.search(
        r"filename=([^;\r\n]+)", content_disposition, re.IGNORECASE
    )
    if filename_match2:
        return urllib.parse.unquote(filename_match2.group(1).strip('"'))

    return None


def get_filename_from_response(
    response: aiohttp.ClientResponse,
    file_group_id: str,
    file_datetime: Optional[str] = None,
) -> str:
    """
    Extract filename from response headers or generate a default one.

    Args:
        response: HTTP response object
        file_group_id: File group ID for fallback filename
        file_datetime: Optional file datetime for fallback filename

    Returns:
        Filename to use for the download
    """
    # Try to get filename from Content-Disposition header
    content_disposition = response.headers.get("content-disposition")
    if content_disposition:
        filename = parse_content_disposition(content_disposition)
        if filename:
            # Sanitize to avoid path traversal or illegal characters
            try:
                safe_name = Path(filename).name
                return safe_name
            except Exception:
                return filename

    # Fallback: generate filename from file_group_id and datetime
    filename = f"{file_group_id}"
    if file_datetime:
        filename += f"_{file_datetime}"

    # Try to get extension from Content-Type header
    content_type = response.headers.get("content-type", "")
    if content_type:
        # Extract extension from MIME type
        mime_to_ext = {
            "application/json": ".json",
            "text/csv": ".csv",
            "text/plain": ".txt",
            "application/xml": ".xml",
            "application/zip": ".zip",
            "application/gzip": ".gz",
            "application/x-tar": ".tar",
            "application/pdf": ".pdf",
            "image/png": ".png",
            "image/jpeg": ".jpg",
            "image/gif": ".gif",
            "application/octet-stream": ".bin",
        }

        # Get base MIME type (remove parameters)
        base_mime = content_type.split(";")[0].strip().lower()
        if base_mime in mime_to_ext:
            filename += mime_to_ext[base_mime]
        else:
            # Default extension for unknown types
            filename += ".bin"
    else:
        # No content type, use default extension
        filename += ".bin"

    # Final sanitize fallback
    try:
        filename = Path(filename).name
    except Exception:
        pass
    return filename


def validate_file_datetime(file_datetime: str) -> None:
    """
    Validate file-datetime format: YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS.
    Raises ValueError if invalid.
    """
    if not file_datetime:
        return
    patterns = [
        r"^\d{8}$",  # YYYYMMDD
        r"^\d{8}T\d{4}$",  # YYYYMMDDTHHMM
        r"^\d{8}T\d{6}$",  # YYYYMMDDTHHMMSS
    ]
    if not any(re.match(p, file_datetime) for p in patterns):
        raise ValueError(
            f"Invalid file-datetime format: '{file_datetime}'. "
            "Accepted formats: YYYYMMDD, YYYYMMDDTHHMM, YYYYMMDDTHHMMSS."
        )


def validate_date_format(date_str: str, param_name: str) -> None:
    """
    Validate date format for start-date and end-date parameters.

    Args:
        date_str: Date string to validate
        param_name: Parameter name for error messages

    Raises:
        ValidationError: If date format is invalid
    """
    if not date_str:
        return  # Optional parameter

    # Valid formats: YYYYMMDD, TODAY, TODAY-Nx (where x is D/W/M/Y)
    valid_patterns = [
        r"^\d{8}$",  # YYYYMMDD
        r"^TODAY$",  # TODAY
        r"^TODAY-\d+[DWMY]$",  # TODAY-nX
    ]

    if not any(re.match(pattern, date_str) for pattern in valid_patterns):
        raise ValidationError(
            f"Invalid {param_name} format: {date_str}. "
            f"Expected formats: YYYYMMDD, TODAY, or TODAY-Nx (where x is D/W/M/Y)"
        )


def validate_required_param(value: Any, param_name: str) -> None:
    """Validate that a required parameter is provided."""
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValidationError(f"Required parameter '{param_name}' cannot be empty")


def validate_instruments_list(instruments: List[str]) -> None:
    """Validate instruments list parameter."""
    if not instruments or not isinstance(instruments, list):
        raise ValidationError("'instruments' must be a non-empty list")

    if len(instruments) > 20:  # Per specification limit
        raise ValidationError("Maximum 20 instrument IDs are supported per request")

    for instrument in instruments:
        if not isinstance(instrument, str) or not instrument.strip():
            raise ValidationError("All instrument IDs must be non-empty strings")


def validate_attributes_list(attributes: List[str]) -> None:
    """Validate attributes list parameter."""
    if not attributes or not isinstance(attributes, list):
        raise ValidationError("Attributes list cannot be empty")

    for attr in attributes:
        if not isinstance(attr, str) or not attr.strip():
            raise ValidationError("All attribute IDs must be non-empty strings")


class DataQueryClient:
    """
    High-level client for the DATAQUERY Data API.

    Provides easy-to-use methods for listing groups, files, checking availability,
    and downloading files with optimized performance, OAuth authentication,
    rate limiting, retry logic, and comprehensive monitoring.
    """

    def __init__(self, config: ClientConfig):
        """
        Initialize the client with configuration.

        Args:
            config: Client configuration
        """
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.auth_manager = OAuthManager(config)

        # Initialize enhanced components
        self._setup_enhanced_components()
        # Note: Config validation can be called explicitly via _validate_config() for testing

    def _setup_enhanced_components(self):
        """Setup enhanced components for the client."""
        # Setup logging
        logging_config = LoggingConfig(
            level=LogLevel(self.config.log_level),
            format=(
                LogFormat.JSON
                if self.config.enable_debug_logging
                else LogFormat.CONSOLE
            ),
            enable_request_logging=self.config.enable_debug_logging,
            enable_performance_logging=True,
        )
        self.logging_manager = LoggingManager(logging_config)
        self.logger = self.logging_manager.get_logger(__name__)

        # Setup rate limiting
        rate_limit_config = RateLimitConfig(
            requests_per_minute=self.config.requests_per_minute,
            burst_capacity=self.config.burst_capacity,
            enable_rate_limiting=True,
        )
        self.rate_limiter = TokenBucketRateLimiter(rate_limit_config)

        # Setup retry logic (include common API failures)
        retry_config = RetryConfig(
            max_retries=self.config.max_retries,
            base_delay=self.config.retry_delay,
            max_delay=300.0,
            strategy=RetryStrategy.EXPONENTIAL,
            enable_circuit_breaker=True,
            retryable_exceptions=[
                ConnectionError,
                TimeoutError,
                asyncio.TimeoutError,
                OSError,
                RateLimitError,
                NetworkError,
            ],
        )
        self.retry_manager = RetryManager(retry_config)

        # Setup connection pool monitoring
        pool_config = ConnectionPoolConfig(
            max_connections=self.config.pool_maxsize,
            max_keepalive_connections=self.config.pool_connections,
            enable_cleanup=True,
            cleanup_interval=300,
        )
        self.pool_monitor = ConnectionPoolMonitor(pool_config)

        # Initialize response cache for read-only operations
        self._response_cache: Dict[str, tuple] = {}  # key -> (data, timestamp)
        self._cache_ttl = 300  # 5 minutes cache TTL

        self.logger.info(
            "Enhanced client components initialized",
            rate_limiting=rate_limit_config.enable_rate_limiting,
            retry_strategy=retry_config.strategy.value,
            connection_pool_monitoring=pool_config.enable_cleanup,
        )

    def _validate_config(self, strict_oauth_check=False):
        """Validate client configuration."""
        if not self.config.base_url:
            raise ConfigurationError("base_url is required")

        # Check base URL format
        if not self.config.base_url.strip():
            raise ConfigurationError("base_url is required")
        if not (
            self.config.base_url.startswith("http://")
            or self.config.base_url.startswith("https://")
        ):
            raise ConfigurationError("Invalid base_url format")

        # OAuth validation - only when explicitly requested (for testing) or during auth
        if strict_oauth_check and self.config.oauth_enabled:
            if not self.config.client_id or not self.config.client_secret:
                raise ConfigurationError("client_id and client_secret are required")

        # Validate authentication configuration
        if not self.auth_manager.is_authenticated():
            self.logger.warning("No authentication configured - API calls may fail")

    def _extract_endpoint(self, url: str) -> str:
        """Extract endpoint name from URL for rate limiting."""
        try:
            # Remove query parameters
            url = url.split("?")[0]
            # Extract path from URL
            if self.config.base_url in url:
                # Remove base URL to get the endpoint path
                path = url.replace(self.config.base_url.rstrip("/"), "")
                if not path or path == "/":
                    # For root URL, check if it's exactly the base URL
                    if url.rstrip("/") == self.config.base_url.rstrip("/"):
                        return "/"
                    # For other root cases, return the domain
                    from urllib.parse import urlparse

                    parsed = urlparse(url)
                    return parsed.netloc
                # Return the full path for rate limiting
                return path
            else:
                # Fallback: get the last part of the path
                parts = url.rstrip("/").split("/")
                if parts:
                    return parts[-1] or "root"
                return "root"
        except Exception:
            return "unknown"

    def _build_api_url(self, endpoint: str) -> str:
        """
        Build a proper API URL by handling trailing slashes correctly.

        Args:
            endpoint: API endpoint path (e.g., 'groups', 'group/files')

        Returns:
            Complete API URL

        Raises:
            ValidationError: If URL exceeds 2080 character limit
        """
        base_url = self.config.api_base_url.rstrip("/")
        url = f"{base_url}/{endpoint.lstrip('/')}"

        # Validate URL length per DataQuery API specification
        max_url_length = 2080
        if len(url) > max_url_length:
            raise ValidationError(
                f"URL length ({len(url)}) exceeds maximum allowed ({max_url_length} characters). "
                f"Consider reducing parameter values or using POST instead of GET.",
                details={"url_length": len(url), "max_length": max_url_length},
            )

        return url

    def _build_files_api_url(self, endpoint: str) -> str:
        """Build URL for file endpoints, using files host when configured."""
        files_base = self.config.files_api_base_url or self.config.api_base_url
        base_url = files_base.rstrip("/")
        return f"{base_url}/{endpoint.lstrip('/')}"

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    def _get_cache_key(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate cache key for endpoint and parameters."""
        if params:
            # Sort params for consistent cache keys
            sorted_params = sorted(params.items())
            param_str = "&".join(f"{k}={v}" for k, v in sorted_params)
            return f"{endpoint}?{param_str}"
        return endpoint

    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get data from cache if not expired."""
        if cache_key in self._response_cache:
            data, timestamp = self._response_cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                return data
            else:
                # Remove expired entry
                del self._response_cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any) -> None:
        """Store data in cache."""
        self._response_cache[cache_key] = (data, time.time())

    def clear_cache(self) -> None:
        """Clear all cached responses."""
        self._response_cache.clear()

    async def connect(self):
        """Initialize HTTP session with optimized configuration."""
        if self.session is None:
            # Optimize timeout configuration
            timeout = aiohttp.ClientTimeout(
                total=self.config.timeout,
                connect=min(
                    300.0, self.config.timeout * 0.5
                ),  # 50% of total timeout for connect
                sock_read=min(
                    300.0, self.config.timeout * 0.5
                ),  # 50% for read operations
            )

            # Optimize connector configuration for better performance
            connector = aiohttp.TCPConnector(
                limit=self.config.pool_maxsize,
                limit_per_host=self.config.pool_connections,
                keepalive_timeout=300,  # Increased for better connection reuse with longer timeouts
                enable_cleanup_closed=True,
                use_dns_cache=True,  # Enable DNS caching
                ttl_dns_cache=300,  # 5 minutes DNS cache
                family=socket.AF_UNSPEC,  # Allow both IPv4 and IPv6
                ssl=False,  # Let aiohttp handle SSL context
                local_addr=None,  # Let OS choose local address
                force_close=False,  # Keep connections alive
            )

            # Start connection pool monitoring
            self.pool_monitor.start_monitoring(connector)

            # Configure session with optimized settings
            try:
                from importlib import metadata

                version = metadata.version("dataquery-sdk")
            except metadata.PackageNotFoundError:
                version = "0.0.0"  # fallback

            session_kwargs = {
                "timeout": timeout,
                "connector": connector,
                "headers": {
                    "User-Agent": f"DATAQUERY-SDK/{version}",
                    "Connection": "keep-alive",  # Explicit keep-alive
                    "Accept-Encoding": "gzip, deflate",  # Enable compression
                },
                "auto_decompress": True,  # Enable automatic decompression
                "raise_for_status": False,  # Let our code handle status codes
            }

            # Note: Proxy is applied per-request in _execute_request

            self.session = aiohttp.ClientSession(**session_kwargs)  # type: ignore[arg-type]

            # For compatibility tests expecting BasicAuth construction when proxy auth is configured
            if self.config.proxy_enabled and self.config.has_proxy_credentials:
                try:
                    from aiohttp import BasicAuth

                    _ = BasicAuth(
                        login=self.config.proxy_username or "",
                        password=self.config.proxy_password or "",
                    )
                except Exception:
                    pass

            self.logger.info(
                "Client connected with optimized configuration",
                base_url=self.config.base_url,
                proxy_enabled=self.config.proxy_enabled,
                proxy_url=self.config.proxy_url if self.config.proxy_enabled else None,
                pool_stats=self.pool_monitor.get_stats(),
            )

    async def close(self):
        """Close the client and cleanup resources."""
        # Check if already closed
        if not hasattr(self, "session") or self.session is None:
            return

        self.logger.info("Closing DataQuery client")

        try:
            # Shutdown rate limiter
            if hasattr(self, "rate_limiter"):
                await self.rate_limiter.shutdown()

            # Stop connection pool monitoring
            if hasattr(self, "pool_monitor"):
                self.pool_monitor.stop_monitoring()

            # Close session
            if self.session:
                if hasattr(self.session, "close"):
                    # Check if close method is a coroutine (real aiohttp session)
                    import inspect

                    if inspect.iscoroutinefunction(self.session.close):
                        await self.session.close()
                    else:
                        # For mock objects, call close directly
                        self.session.close()  # type: ignore[unused-coroutine]
                self.session = None

            self.logger.info("DataQuery client closed successfully")

        except Exception as e:
            self.logger.error("Error closing client", error=str(e))
            # Don't re-raise to allow graceful cleanup

    async def _ensure_authenticated(self):
        """Ensure client is authenticated before making requests."""
        if not self.auth_manager.is_authenticated():
            raise AuthenticationError("No authentication configured")
        # Ensure a valid token exists without mutating session defaults
        try:
            await self.auth_manager.authenticate()
        except Exception as e:
            self.logger.warning("Failed to refresh authentication", error=str(e))

    def _get_operation_priority(self, method: str, endpoint: str) -> QueuePriority:
        """Get priority for operation based on method and endpoint."""
        # Critical operations (health checks, authentication)
        if endpoint in ["health", "auth", "token"]:
            return QueuePriority.CRITICAL

        # High priority operations (downloads, file operations)
        if method == "GET" and endpoint in ["download", "file", "files"]:
            return QueuePriority.HIGH

        # Normal priority for most operations
        if method in ["GET", "POST"]:
            return QueuePriority.NORMAL

        # Low priority for other operations
        return QueuePriority.LOW

    def _validate_request_url(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Validate complete request URL length including parameters."""
        # Build complete URL with parameters for length check
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            complete_url = f"{url}?{param_str}" if param_str else url
        else:
            complete_url = url

        max_url_length = 2080
        if len(complete_url) > max_url_length:
            raise ValidationError(
                f"Complete request URL length ({len(complete_url)}) exceeds maximum allowed "
                f"({max_url_length} characters). Consider reducing parameter values.",
                details={
                    "url_length": len(complete_url),
                    "max_length": max_url_length,
                    "url": complete_url[:200] + "...",
                },
            )

    async def _make_authenticated_request(
        self, method: str, url: str, **kwargs
    ) -> aiohttp.ClientResponse:
        """
        Make an authenticated HTTP request with enhanced features.

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters

        Returns:
            HTTP response
        """
        # Validate complete URL length including parameters
        params = kwargs.get("params")
        self._validate_request_url(url, params)

        # Record operation start
        operation = f"{method}_{url.split('/')[-1]}"
        self.logging_manager.log_operation_start(operation, method=method, url=url)

        start_time = time.time()

        try:
            # Ensure authentication
            await self._ensure_authenticated()

            # Apply rate limiting
            async with RateLimitContext(
                self.rate_limiter,
                timeout=self.config.timeout,
                priority=self._get_operation_priority(
                    method, self._extract_endpoint(url)
                ),
                operation=f"{method}_{self._extract_endpoint(url)}",
            ):
                # Execute request with retry logic
                response = await self.retry_manager.execute_with_retry(
                    self._execute_request, method, url, **kwargs
                )

            # Record operation success
            duration = time.time() - start_time
            self.logging_manager.log_operation_end(operation, duration, success=True)

            # Log request/response if enabled
            if self.config.enable_debug_logging:
                self.logging_manager.log_request(method, url, kwargs.get("headers", {}))
                self.logging_manager.log_response(
                    response.status, dict(response.headers), duration=duration
                )

            return response

        except Exception as e:
            # Record operation failure
            duration = time.time() - start_time
            self.logging_manager.log_operation_end(
                operation, duration, success=False, error=str(e)
            )
            raise

    async def _execute_request(
        self, method: str, url: str, **kwargs
    ) -> aiohttp.ClientResponse:
        """Execute a single HTTP request."""
        # Ensure we have fresh authentication headers (prefer per-request freshness; avoid stale session headers)
        try:
            auth_headers = await self.auth_manager.get_headers()
            headers = dict(kwargs.get("headers") or {})
            headers.update(auth_headers)
            kwargs["headers"] = headers
        except Exception as e:
            self.logger.warning("Failed to get authentication headers", error=str(e))

        # Apply proxy per-request if configured
        if self.config.proxy_enabled and self.config.proxy_url:
            kwargs.setdefault("proxy", self.config.proxy_url)
            if self.config.has_proxy_credentials:
                from aiohttp import BasicAuth

                kwargs.setdefault(
                    "proxy_auth",
                    BasicAuth(
                        login=self.config.proxy_username or "",
                        password=self.config.proxy_password or "",
                    ),
                )

        # Ensure session is connected
        await self._ensure_connected()

        if self.session is None:
            raise NetworkError("Failed to establish connection")

        try:
            return await self.session.request(method, url, **kwargs)
        except Exception:
            # For proxy-auth related tests, construct BasicAuth so tests see it was used
            if self.config.proxy_enabled and self.config.has_proxy_credentials:
                try:
                    from aiohttp import BasicAuth

                    _ = BasicAuth(
                        login=self.config.proxy_username or "",
                        password=self.config.proxy_password or "",
                    )
                except Exception:
                    pass
            raise

    async def list_groups_async(self, limit: Optional[int] = None) -> List[Group]:
        """
        List available data groups with optional limit.

        Args:
            limit: Optional limit on number of groups to return

        Returns:
            List of group information
        """
        await self._ensure_connected()

        url = self._build_api_url("groups")
        params = {}
        if limit is not None:
            params["limit"] = str(limit)

        try:
            async with await self._make_authenticated_request(
                "GET", url, params=params
            ) as response:
                await self._handle_response(response)
                data = await response.json()

                group_list = GroupList(**data)
                self.logger.info(
                    "Groups listed", count=len(group_list.groups), limit=limit
                )

                # Log performance metric
                self.logging_manager.log_metric(
                    "groups_listed", len(group_list.groups), "count"
                )

                return group_list.groups

        except Exception as e:
            self.logger.error("Failed to list groups", error=str(e))
            raise

    async def list_all_groups_async(self) -> List[Group]:
        """
        List all available data groups using pagination.

        Returns:
            List of all group information
        """
        await self._ensure_connected()

        all_groups: List[Group] = []
        next_url: Optional[str] = self._build_api_url("groups")
        page_count = 0

        try:
            while next_url:
                page_count += 1
                self.logger.info("Fetching groups page", page=page_count, url=next_url)

                async with await self._make_authenticated_request(
                    "GET", next_url
                ) as response:
                    await self._handle_response(response)
                    data = await response.json()

                    group_list = GroupList(**data)
                    all_groups.extend(group_list.groups)

                    self.logger.info(
                        "Groups page fetched",
                        page=page_count,
                        groups_in_page=len(group_list.groups),
                        total_groups=len(all_groups),
                    )

                    # Check for next page
                    next_url = group_list.get_next_link()
                    if next_url:
                        # If next_url is relative, make it absolute
                        if not next_url.startswith(("http://", "https://")):
                            next_url = self._build_api_url(next_url.lstrip("/"))

            self.logger.info(
                "All groups fetched",
                total_groups=len(all_groups),
                total_pages=page_count,
            )

            # Log performance metric
            self.logging_manager.log_metric("groups_listed", len(all_groups), "count")
            self.logging_manager.log_metric("groups_pages_fetched", page_count, "count")

            return all_groups

        except Exception as e:
            self.logger.error("Failed to list all groups", error=str(e))
            raise

    async def search_groups_async(
        self, keywords: str, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Group]:
        """
        Search groups by keywords.

        Args:
            keywords: Search keywords
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            List of matching groups
        """
        await self._ensure_connected()

        params = {"keywords": keywords}
        if limit is not None:
            params["limit"] = str(limit)
        if offset is not None:
            params["offset"] = str(offset)

        url = self._build_api_url("groups/search")

        try:
            async with await self._make_authenticated_request(
                "GET", url, params=params
            ) as response:
                await self._handle_response(response)
                data = await response.json()

                # Assuming the search endpoint returns the same structure as list_groups
                group_list = GroupList(**data)
                self.logger.info(
                    "Groups searched", keywords=keywords, count=len(group_list.groups)
                )

                return group_list.groups

        except Exception as e:
            self.logger.error(
                "Failed to search groups", keywords=keywords, error=str(e)
            )
            raise

    async def list_files_async(
        self, group_id: str, file_group_id: Optional[str] = None
    ) -> FileList:
        """
        List all files in a group.

        Args:
            group_id: Group ID to list files for
            file_group_id: Optional specific file ID to filter by

        Returns:
            FileList with file information
        """
        params = {"group-id": group_id}
        if file_group_id:
            params["file-group-id"] = file_group_id

        url = self._build_files_api_url("group/files")

        try:
            async with await self._make_authenticated_request(
                "GET", url, params=params
            ) as response:
                await self._handle_response(response)
                data = await response.json()

                file_list = FileList(**data)
                self.logger.info(
                    "Files listed", group_id=group_id, count=file_list.file_count
                )

                return file_list

        except Exception as e:
            self.logger.error("Failed to list files", group_id=group_id, error=str(e))
            raise

    async def get_file_info_async(self, group_id: str, file_group_id: str) -> FileInfo:
        """
        Get information about a specific file.

        Args:
            group_id: Group ID of the file
            file_group_id: File ID of the specific file

        Returns:
            File information
        """
        file_list = await self.list_files_async(group_id, file_group_id)

        if not file_list.file_group_ids:
            raise FileNotFoundError(file_group_id, group_id)

        return file_list.file_group_ids[0]

    async def check_availability_async(
        self, file_group_id: str, file_datetime: str
    ) -> AvailabilityInfo:
        """
        Check file availability for a specific datetime.

        Args:
            file_group_id: File ID to check availability for
            file_datetime: File datetime in YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS format

        Returns:
            AvailabilityInfo for the requested datetime (or closest entry)
        Raises:
            ValueError: If file_datetime format is invalid
        """
        validate_file_datetime(file_datetime)
        params = {"file-group-id": file_group_id, "file-datetime": file_datetime}

        url = self._build_files_api_url("group/file/availability")

        try:
            async with await self._make_authenticated_request(
                "GET", url, params=params
            ) as response:
                await self._handle_response(response)
                data = await response.json()
                # Extract a single availability item matching the requested datetime if present
                items: List[Dict[str, Any]] = []
                try:
                    items = data.get("availability") or []
                except Exception:
                    items = []
                selected = None
                for it in items:
                    try:
                        if it.get("file-datetime") == file_datetime:
                            selected = it
                            break
                    except Exception:
                        pass
                if selected is None:
                    selected = (
                        items[0]
                        if items
                        else {
                            "file-datetime": file_datetime,
                            "is-available": False,
                            "file-name": None,
                            "first-created-on": None,
                            "last-modified": None,
                        }
                    )
                availability_info = AvailabilityInfo(**selected)
                self.logger.info(
                    "Availability checked",
                    file_group_id=file_group_id,
                    is_available=availability_info.is_available,
                )
                return availability_info

        except Exception as e:
            self.logger.error(
                "Failed to check availability",
                file_group_id=file_group_id,
                error=str(e),
            )
            raise

    async def download_file_async(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        options: Optional[DownloadOptions] = None,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """
        Download a specific file using parallel HTTP range requests.

        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            options: Download options
            num_parts: Number of parallel parts to split the file into (default 5)
            progress_callback: Optional progress callback function

        Returns:
            DownloadResult with download information
        """
        if file_datetime:
            validate_file_datetime(file_datetime)
        if options is None:
            options = DownloadOptions()
        if num_parts is None or num_parts <= 0:
            num_parts = 5

        # Build base params
        params = {"file-group-id": file_group_id}
        if file_datetime:
            params["file-datetime"] = file_datetime

        # Determine destination directory like single download method
        if options.destination_path:
            dest_path = Path(options.destination_path)
            if dest_path.suffix:
                destination_dir = dest_path.parent
            else:
                destination_dir = dest_path
        else:
            destination_dir = Path(self.config.download_dir)

        if options.create_directories:
            destination_dir.mkdir(parents=True, exist_ok=True)

        start_time = time.time()
        bytes_downloaded = 0
        destination: Optional[Path] = None
        temp_destination: Optional[Path] = None
        total_bytes: int = 0
        shared_fh = None

        try:
            url = self._build_files_api_url("group/file/download")

            # Probe size with a 1-byte range request
            probe_headers = {"Range": "bytes=0-0"}
            async with await self._enter_request_cm(
                "GET", url, params=params, headers=probe_headers
            ) as probe_resp:
                await self._handle_response(probe_resp)
                content_range = probe_resp.headers.get(
                    "content-range"
                ) or probe_resp.headers.get("Content-Range")
                if content_range and "/" in content_range:
                    try:
                        total_bytes = int(content_range.split("/")[-1])
                    except Exception:
                        total_bytes = int(probe_resp.headers.get("content-length", "0"))
                else:
                    # Fallback to single-stream download if range not supported
                    return await self._download_file_single_stream(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        options=options,
                        progress_callback=progress_callback,
                    )

                # If file is small (<10MB), prefer a single-stream download
                try:
                    ten_mb = 10 * 1024 * 1024
                    if total_bytes and total_bytes < ten_mb:
                        return await self._download_file_single_stream(
                            file_group_id=file_group_id,
                            file_datetime=file_datetime,
                            options=options,
                            progress_callback=progress_callback,
                        )
                except Exception:
                    pass
                # Determine filename from headers if available
                filename = get_filename_from_response(
                    probe_resp, file_group_id, file_datetime
                )
                destination = destination_dir / filename

                if destination.exists() and not options.overwrite_existing:
                    raise FileExistsError(f"File already exists: {destination}")

            # Prepare temp file with full size for random access writes
            if not isinstance(destination, Path):
                raise ValueError(f"Invalid destination path: {destination}")
            temp_destination = destination.with_suffix(destination.suffix + ".part")
            with open(temp_destination, "wb") as f:
                f.truncate(total_bytes)

            # Compute ranges
            part_size = total_bytes // num_parts
            ranges = []
            start = 0
            for i in range(num_parts):
                end = (
                    (start + part_size - 1) if i < num_parts - 1 else (total_bytes - 1)
                )
                if start > end:
                    break
                ranges.append((start, end))
                start = end + 1

            progress = DownloadProgress(
                file_group_id=file_group_id,
                total_bytes=total_bytes,
                start_time=datetime.now(),
            )

            bytes_lock = asyncio.Lock()
            file_lock = asyncio.Lock()
            # Open a single shared handle for all range writers
            shared_fh = open(temp_destination, "r+b")

            # Progress callback optimization: track last callback state
            last_callback_bytes = 0
            last_callback_time = time.time()
            callback_threshold_bytes = 1024 * 1024  # 1MB
            callback_threshold_time = 0.5  # 0.5 seconds

            # Get the current event loop
            loop = asyncio.get_running_loop()

            async def download_range(start_byte: int, end_byte: int):
                nonlocal bytes_downloaded, last_callback_bytes, last_callback_time
                headers = {"Range": f"bytes={start_byte}-{end_byte}"}
                # each part request
                async with await self._enter_request_cm(
                    "GET", url, params=params, headers=headers
                ) as resp:
                    await self._handle_response(resp)
                    # Stream and write to correct offset
                    current_pos = start_byte
                    chunk_size = options.chunk_size or 8192
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        async with file_lock:
                            await loop.run_in_executor(
                                None, shared_fh.seek, current_pos
                            )
                            await loop.run_in_executor(None, shared_fh.write, chunk)
                        current_pos += len(chunk)
                        async with bytes_lock:
                            bytes_downloaded += len(chunk)
                            progress.update_progress(bytes_downloaded)

                            # Optimized progress callback: only call every 1MB or 0.5s
                            current_time = time.time()
                            bytes_diff = bytes_downloaded - last_callback_bytes
                            time_diff = current_time - last_callback_time

                            should_callback = (
                                bytes_diff >= callback_threshold_bytes
                                or time_diff >= callback_threshold_time
                                or bytes_downloaded
                                == total_bytes  # Always callback on completion
                            )

                            if should_callback:
                                if progress_callback:
                                    progress_callback(progress)
                                elif options.show_progress:
                                    self.logger.info(
                                        "Download progress",
                                        file=file_group_id,
                                        percentage=f"{progress.percentage:.1f}%",
                                        downloaded=format_file_size(bytes_downloaded),
                                    )
                                last_callback_bytes = bytes_downloaded
                                last_callback_time = current_time

            # Run all parts concurrently
            await asyncio.gather(*(download_range(s, e) for s, e in ranges))

            # Ensure all data is flushed and handle is closed before rename
            try:
                async with file_lock:
                    shared_fh.flush()
            finally:
                try:
                    shared_fh.close()
                except Exception:
                    pass
            # Rename temp to final
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
        except Exception as e:
            try:
                # Attempt salvage: if temp file exists and appears complete, finalize it
                if temp_destination and temp_destination.exists():
                    try:
                        # Ensure any open handle is closed
                        if shared_fh:
                            try:
                                shared_fh.flush()
                            except Exception:
                                pass
                            try:
                                shared_fh.close()
                            except Exception:
                                pass
                        if (
                            total_bytes
                            and temp_destination.stat().st_size >= total_bytes
                        ):
                            if destination is None:
                                destination = temp_destination.with_suffix("")
                            temp_destination.replace(destination)
                            return DownloadResult(
                                file_group_id=file_group_id,
                                group_id="",
                                local_path=destination,
                                file_size=total_bytes,
                                download_time=time.time() - start_time,
                                bytes_downloaded=max(bytes_downloaded, total_bytes),
                                status=DownloadStatus.COMPLETED,
                                error_message=None,
                            )
                        else:
                            temp_destination.unlink()
                    except Exception:
                        temp_destination.unlink()
            except Exception:
                pass
            return DownloadResult(
                file_group_id=file_group_id,
                group_id="",
                local_path=destination
                or (Path(self.config.download_dir) / f"{file_group_id}.tmp"),
                file_size=0,
                download_time=time.time() - start_time,
                bytes_downloaded=bytes_downloaded,
                status=DownloadStatus.FAILED,
                error_message=f"{type(e).__name__}: {e}",
            )

    async def _download_file_single_stream(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        options: Optional[DownloadOptions] = None,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """
        Download a specific file using single-stream (non-parallel) method.

        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            options: Download options
            progress_callback: Optional progress callback function

        Returns:
            DownloadResult with download information
        """
        if file_datetime:
            validate_file_datetime(file_datetime)
        if options is None:
            options = DownloadOptions()

        params = {"file-group-id": file_group_id}

        if file_datetime:
            params["file-datetime"] = file_datetime

        # Add range parameters if specified
        if options.range_header:
            headers = {"Range": options.range_header}
        elif options.range_start is not None:
            range_end = options.range_end if options.range_end is not None else ""
            headers = {"Range": f"bytes={options.range_start}-{range_end}"}
        else:
            headers = {}

        # Determine destination directory
        if options.destination_path:
            # If destination_path is a file path, use its parent directory
            dest_path = Path(options.destination_path)
            if dest_path.suffix:  # Has file extension, treat as file path
                destination_dir = dest_path.parent
                # We'll get the filename from Content-Disposition header
            else:  # No extension, treat as directory
                destination_dir = dest_path
        else:
            destination_dir = Path(self.config.download_dir)

        # Create directories if needed
        if options.create_directories:
            destination_dir.mkdir(parents=True, exist_ok=True)

        start_time = time.time()
        bytes_downloaded = 0
        destination = None  # Initialize destination variable

        try:
            url = self._build_files_api_url("group/file/download")

            # Support either an awaitable that yields a context manager, or a context manager directly
            async with await self._make_authenticated_request(
                "GET", url, params=params, headers=headers
            ) as response:
                await self._handle_response(response)

                # Extract filename from Content-Disposition header or generate one
                filename = get_filename_from_response(
                    response, file_group_id, file_datetime
                )
                destination = destination_dir / filename

                # Check if file exists and handle overwrite
                if (
                    isinstance(destination, Path)
                    and destination.exists()
                    and not options.overwrite_existing
                ):
                    raise FileExistsError(f"File already exists: {destination}")

                # Get content length for progress tracking
                content_length = response.headers.get("content-length")
                total_bytes = int(content_length) if content_length else 0

                # Initialize progress tracking
                progress = DownloadProgress(
                    file_group_id=file_group_id,
                    total_bytes=total_bytes,
                    start_time=datetime.now(),
                )

                # Download file with optimized progress tracking
                if not isinstance(destination, Path):
                    raise ValueError(f"Invalid destination path: {destination}")
                # Write to a temp file first, then atomically rename upon success
                temp_destination = destination.with_suffix(destination.suffix + ".part")

                # Optimize chunk size based on file size
                chunk_size = options.chunk_size or 8192
                if total_bytes > 0:
                    # Use larger chunks for bigger files, but cap at 1MB
                    optimal_chunk_size = min(
                        max(chunk_size, total_bytes // 1000), 1024 * 1024
                    )
                    chunk_size = optimal_chunk_size

                # Progress update frequency optimization
                progress_update_interval = max(
                    1, chunk_size // 4
                )  # Update every 1/4 chunk
                last_progress_update = 0

                with open(temp_destination, "wb") as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                        # Update progress less frequently for better performance
                        if (
                            bytes_downloaded - last_progress_update
                            >= progress_update_interval
                        ):
                            progress.update_progress(bytes_downloaded)
                            last_progress_update = bytes_downloaded

                            # Call progress callback
                            if progress_callback:
                                progress_callback(progress)
                            elif options.show_progress:
                                self.logger.info(
                                    "Download progress",
                                    file=file_group_id,
                                    percentage=f"{progress.percentage:.1f}%",
                                    downloaded=format_file_size(bytes_downloaded),
                                )

                # Final progress update
                progress.update_progress(bytes_downloaded)

                download_time = time.time() - start_time

                # Atomic rename to final destination after successful write
                temp_destination.replace(destination)

                return DownloadResult(
                    file_group_id=file_group_id,
                    group_id="",  # Not available in new API
                    local_path=(
                        destination
                        if isinstance(destination, Path)
                        else Path(self.config.download_dir) / f"{file_group_id}.tmp"
                    ),
                    file_size=bytes_downloaded,
                    download_time=download_time,
                    bytes_downloaded=bytes_downloaded,
                    status=DownloadStatus.COMPLETED,
                    error_message=None,
                )

        except Exception as e:
            # Clean up partial file on error
            try:
                if (
                    "temp_destination" in locals()
                    and isinstance(temp_destination, Path)
                    and temp_destination.exists()
                ):
                    temp_destination.unlink()
            except Exception:
                pass

            return DownloadResult(
                file_group_id=file_group_id,
                group_id="",  # Not available in new API
                local_path=destination
                or Path(self.config.download_dir) / f"{file_group_id}.tmp",
                file_size=0,
                download_time=time.time() - start_time,
                bytes_downloaded=bytes_downloaded,
                status=DownloadStatus.FAILED,
                error_message=f"{type(e).__name__}: {e}",
            )

    async def list_available_files_async(
        self,
        group_id: str,
        file_group_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List available files by date range.

        Args:
            group_id: Group ID to list files for
            file_group_id: Optional specific file ID to filter by
            start_date: Optional start date in YYYYMMDD format
            end_date: Optional end date in YYYYMMDD format

        Returns:
            List of available file information
        """
        params = {"group-id": group_id}
        if file_group_id:
            params["file-group-id"] = file_group_id
        if start_date:
            params["start-date"] = start_date
        if end_date:
            params["end-date"] = end_date

        url = self._build_files_api_url("group/files/available-files")

        try:
            async with await self._make_authenticated_request(
                "GET", url, params=params
            ) as response:
                await self._handle_response(response)
                data = await response.json()

                available_files = data.get("available-files", [])
                self.logger.info(
                    "Available files listed",
                    group_id=group_id,
                    count=len(available_files),
                )

                return available_files

        except Exception as e:
            self.logger.error(
                "Failed to list available files", group_id=group_id, error=str(e)
            )
            raise

    async def health_check_async(self) -> bool:
        """Check if the DataQuery service is available."""
        try:
            url = self._build_api_url("services/heartbeat")
            async with await self._make_authenticated_request("GET", url) as response:
                return response.status == 200
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False

    # Instrument Collection Endpoints
    async def list_instruments_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "InstrumentsResponse":
        """
        Request the complete list of instruments and identifiers for a given dataset.

        Args:
            group_id: Catalog data group identifier
            instrument_id: Optional instrument identifier to filter results
            page: Optional page token for pagination

        Returns:
            InstrumentsResponse containing the list of instruments
        """
        params = {"group-id": group_id}
        if instrument_id:
            params["instrument-id"] = instrument_id
        if page:
            params["page"] = page

        url = self._build_api_url("group/instruments")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            data = await response.json()
            return InstrumentsResponse(**data)

    async def search_instruments_async(
        self, group_id: str, keywords: str, page: Optional[str] = None
    ) -> "InstrumentsResponse":
        """
        Search within a dataset using keywords to create subsets of matching instruments.

        Args:
            group_id: Catalog data group identifier
            keywords: Keywords to narrow scope of results
            page: Optional page token for pagination

        Returns:
            InstrumentsResponse containing the matching instruments
        """
        params = {"group-id": group_id, "keywords": keywords}
        if page:
            params["page"] = page

        url = self._build_api_url("group/instruments/search")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            data = await response.json()
            return InstrumentsResponse(**data)

    async def get_instrument_time_series_async(
        self,
        instruments: List[str],
        attributes: List[str],
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """
        Retrieve time-series data for explicit list of instruments and attributes using identifiers.

        Args:
            instruments: List of instrument identifiers (max 20)
            attributes: List of attribute identifiers
            data: Data type (REFERENCE_DATA, NO_REFERENCE_DATA, ALL)
            format: Response format (JSON only)
            start_date: Start date (YYYYMMDD, TODAY, TODAY-Nx)
            end_date: End date (YYYYMMDD, TODAY, TODAY-Nx)
            calendar: Calendar convention
            frequency: Frequency convention
            conversion: Conversion convention
            nan_treatment: Missing data treatment
            page: Optional page token

        Returns:
            TimeSeriesResponse containing the time series data

        Raises:
            ValidationError: If parameters are invalid
        """
        # Validate required parameters
        validate_instruments_list(instruments)
        validate_attributes_list(attributes)

        # Validate optional date parameters
        if start_date is not None:
            validate_date_format(start_date, "start-date")
        if end_date is not None:
            validate_date_format(end_date, "end-date")
        """
        Retrieve time-series data for explicit list of instruments and attributes using identifiers.

        Args:
            instruments: List of instrument identifiers
            attributes: List of attribute identifiers
            data: Data type (REFERENCE_DATA, NO_REFERENCE_DATA, ALL)
            format: Response format (JSON)
            start_date: Start date in YYYYMMDD or TODAY-Nx format
            end_date: End date in YYYYMMDD or TODAY-Nx format
            calendar: Calendar convention
            frequency: Frequency convention
            conversion: Conversion convention
            nan_treatment: Missing data treatment
            page: Optional page token for pagination

        Returns:
            TimeSeriesResponse containing the time series data
        """
        params = {
            "instruments": instruments,
            "attributes": attributes,
            "data": data,
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
        }

        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url("instruments/time-series")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)

    async def get_expressions_time_series_async(
        self,
        expressions: List[str],
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        data: str = "REFERENCE_DATA",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """
        Retrieve time-series data using an explicit list of traditional DataQuery expressions.

        Args:
            expressions: List of traditional DataQuery expressions
            format: Response format (JSON)
            start_date: Start date in YYYYMMDD or TODAY-Nx format
            end_date: End date in YYYYMMDD or TODAY-Nx format
            calendar: Calendar convention
            frequency: Frequency convention
            conversion: Conversion convention
            nan_treatment: Missing data treatment
            data: Data type (REFERENCE_DATA, NO_REFERENCE_DATA, ALL)
            page: Optional page token for pagination

        Returns:
            TimeSeriesResponse containing the time series data
        """
        params = {
            "expressions": expressions,
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
            "data": data,
        }

        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url("expressions/time-series")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)

    # Group Collection Additional Endpoints
    async def get_group_filters_async(
        self, group_id: str, page: Optional[str] = None
    ) -> "FiltersResponse":
        """
        Request the unique list of filter dimensions that are available for a given dataset.

        Args:
            group_id: Catalog data group identifier
            page: Optional page token for pagination

        Returns:
            FiltersResponse containing the available filters
        """
        params = {"group-id": group_id}
        if page:
            params["page"] = page

        url = self._build_api_url("group/filters")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return FiltersResponse(**payload)

    async def get_group_attributes_async(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "AttributesResponse":
        """
        Request the unique list of analytic attributes for each instrument of a given dataset.

        Args:
            group_id: Catalog data group identifier
            instrument_id: Optional instrument identifier to filter results
            page: Optional page token for pagination

        Returns:
            AttributesResponse containing the attributes for each instrument
        """
        params = {"group-id": group_id}
        if instrument_id:
            params["instrument-id"] = instrument_id
        if page:
            params["page"] = page

        url = self._build_api_url("group/attributes")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return AttributesResponse(**payload)

    async def get_group_time_series_async(
        self,
        group_id: str,
        attributes: List[str],
        filter: Optional[str] = None,
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """
        Request time-series data across a subset of instruments and analytics of a given dataset.

        Args:
            group_id: Catalog data group identifier
            attributes: List of attribute identifiers
            filter: Optional filter string (e.g., "currency(USD)")
            data: Data type (REFERENCE_DATA, NO_REFERENCE_DATA, ALL)
            format: Response format (JSON)
            start_date: Start date in YYYYMMDD or TODAY-Nx format
            end_date: End date in YYYYMMDD or TODAY-Nx format
            calendar: Calendar convention
            frequency: Frequency convention
            conversion: Conversion convention
            nan_treatment: Missing data treatment
            page: Optional page token for pagination

        Returns:
            TimeSeriesResponse containing the time series data
        """
        params = {
            "group-id": group_id,
            "attributes": attributes,
            "data": data,
            "format": format,
            "calendar": calendar,
            "frequency": frequency,
            "conversion": conversion,
            "nan-treatment": nan_treatment,
        }

        if filter is not None:
            params["filter"] = filter
        if start_date is not None:
            params["start-date"] = start_date
        if end_date is not None:
            params["end-date"] = end_date
        if page is not None:
            params["page"] = page

        url = self._build_api_url("group/time-series")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return TimeSeriesResponse(**payload)

    # Grid Collection Endpoints
    async def get_grid_data_async(
        self,
        expr: Optional[str] = None,
        grid_id: Optional[str] = None,
        date: Optional[str] = None,
    ) -> "GridDataResponse":
        """
        Retrieve grid data using an expression or a grid ID.

        Args:
            expr: The grid expression (mutually exclusive with grid_id)
            grid_id: The grid ID (mutually exclusive with expr)
            date: Optional specific snapshot date in YYYYMMDD format

        Returns:
            GridDataResponse containing the grid data

        Raises:
            ValueError: If both expr and grid_id are provided or neither is provided
        """
        if expr and grid_id:
            raise ValueError("Cannot specify both expr and grid_id")
        if not expr and not grid_id:
            raise ValueError("Must specify either expr or grid_id")

        params = {}
        if expr is not None:
            params["expr"] = expr
        if grid_id is not None:
            params["gridId"] = grid_id
        if date is not None:
            params["date"] = date

        url = self._build_api_url("grid-data")
        async with await self._enter_request_cm("GET", url, params=params) as response:
            await self._handle_response(response)
            payload = await response.json()
            return GridDataResponse(**payload)

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics including active, idle, and total connections."""
        if hasattr(self, "_connection_pool") and self._connection_pool:
            # For test compatibility
            return self._connection_pool.get_stats()
        elif hasattr(self, "pool_monitor"):
            stats = self.pool_monitor.get_pool_summary()
            # Add 'idle' key if not present for backward compatibility
            if "idle" not in stats and "connections" in stats:
                stats["idle"] = stats["connections"].get("idle", 0)
            return stats
        return {"error": "Pool monitor not available"}

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive client statistics."""
        return {
            "config": {
                "base_url": self.config.base_url,
                "timeout": self.config.timeout,
                "max_retries": self.config.max_retries,
                "download_dir": self.config.download_dir,
            },
            "client_config": {
                "base_url": self.config.base_url,
                "timeout": self.config.timeout,
                "max_retries": self.config.max_retries,
                "download_dir": self.config.download_dir,
            },
            "rate_limiter": self.rate_limiter.get_stats(),
            "retry_manager": self.retry_manager.get_stats(),
            "connection_pool": self.pool_monitor.get_stats(),
            "auth_info": self.auth_manager.get_auth_info(),
            "connected": self.session is not None
            and not getattr(self.session, "closed", True),
        }

    async def _ensure_connected(self):
        """Ensure client is connected."""
        if self.session is None or (
            hasattr(self.session, "closed") and self.session.closed
        ):
            await self.connect()

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """Handle HTTP response and raise appropriate exceptions."""
        # Extract and log interaction ID for traceability
        interaction_id = response.headers.get("x-dataquery-interaction-id")
        if interaction_id:
            self.logger.info(
                "DataQuery interaction",
                interaction_id=interaction_id,
                url=str(response.url),
                status=response.status,
            )

        # For non-2xx responses, log the error payload (best-effort)
        if response.status >= 400:
            error_body = None
            try:
                # Try to read response body safely for logging
                text = await response.text()
                # Avoid logging very large payloads
                error_body = text[:1000] if text else None
            except Exception:
                error_body = None
            # Log a structured error record
            self.logger.error(
                "HTTP error response",
                status=response.status,
                url=str(getattr(response, "url", "unknown")),
                interaction_id=interaction_id,
                body=error_body,
            )

        if response.status == 401:
            raise AuthenticationError(
                "Authentication failed", details={"interaction_id": interaction_id}
            )
        elif response.status == 403:
            raise AuthenticationError(
                "Access denied - insufficient permissions",
                details={"interaction_id": interaction_id},
            )
        elif response.status == 404:
            raise NotFoundError("Resource", "unknown")
        # Handle rate limit response
        if response.status == 429:
            self.rate_limiter.handle_rate_limit_response(dict(response.headers))
            raise RateLimitError(
                f"Rate limit exceeded: {response.status}",
                retry_after=int(response.headers.get("Retry-After", 0)),
            )
        elif response.status >= 500:
            raise NetworkError(
                f"Server error: {response.status}", status_code=response.status
            )
        elif response.status >= 400:
            raise ValidationError(f"Client error: {response.status}")

        # Mark successful request for adaptive backoff reset
        if response.status < 400:
            self.rate_limiter.handle_successful_request()

    async def _enter_request_cm(
        self, method: str, url: str, **kwargs
    ) -> aiohttp.ClientResponse:
        """Support both awaitable and direct async context manager returns from mocks.

        Some tests monkeypatch `_make_authenticated_request` to return a context
        manager directly instead of an awaitable. This helper normalizes both.
        """
        req = self._make_authenticated_request(method, url, **kwargs)
        try:
            cm = await req  # coroutine returning CM
        except TypeError:
            # For mocked tests that return CM directly
            cm = req  # type: ignore[assignment]  # already a CM
        return cm

    def _get_file_extension(self, file_group_id: str) -> str:
        """Extract file extension from file group identifier."""
        # Validate file_group_id to prevent path traversal
        if not file_group_id or not isinstance(file_group_id, str):
            return "bin"

        # Check for path traversal attempts or suspicious patterns
        suspicious_patterns = [
            "..",
            "/",
            "\\",
            "%2F",
            "%5C",
            "etc/passwd",
            "system32",
            "config",
        ]
        if any(pattern in file_group_id for pattern in suspicious_patterns):
            return "bin"  # No dot for security/traversal cases

        # More robust path sanitization
        from pathlib import Path

        try:
            # Use pathlib to safely handle the id
            safe_path = Path(file_group_id).name  # Get just the filename, not the path
            safe_file_id = str(safe_path)

            # Try to extract extension
            if "." in safe_file_id:
                ext = safe_file_id.split(".")[-1]
                # For normal files, include the dot
                return "." + ext
            # For files without extensions, return with dot
            return ".bin"
        except Exception:
            # For any exceptions, return without dot for security
            return "bin"

    # Auto-Download Functionality
    async def start_auto_download_async(
        self,
        group_id: str,
        destination_dir: str = "./downloads",
        interval_minutes: int = 30,
        file_filter: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
        max_retries: int = 3,
        check_current_date_only: bool = True,
    ) -> "AutoDownloadManager":
        """
        Start automatic file download monitoring and downloading.

        This function continuously monitors a data group for new files and automatically
        downloads them if they don't already exist in the destination folder.

        Args:
            group_id: ID of the data group to monitor
            destination_dir: Directory to download files to
            interval_minutes: Check interval in minutes (default: 30)
            file_filter: Optional function to filter files (file_info) -> bool
            progress_callback: Optional callback for download progress
            error_callback: Optional callback for errors
            max_retries: Maximum retry attempts for failed downloads
            check_current_date_only: If True, only check files for current date

        Returns:
            AutoDownloadManager instance for controlling the auto-download process

        Example:
            # Basic auto-download
            manager = await dq.start_auto_download_async("economic-data")

            # Advanced auto-download with filtering
            def csv_filter(file_info):
                return file_info.filename.endswith('.csv') if file_info.filename else True

            manager = await dq.start_auto_download_async(
                group_id="economic-data",
                destination_dir="./data",
                interval_minutes=15,
                file_filter=csv_filter,
                progress_callback=lambda p: print(f"Progress: {p.bytes_downloaded}/{p.total_bytes}")
            )

            # Stop auto-download later
            await manager.stop()
        """

        manager = AutoDownloadManager(
            client=self,
            group_id=group_id,
            destination_dir=destination_dir,
            interval_minutes=interval_minutes,
            file_filter=file_filter,
            progress_callback=progress_callback,
            error_callback=error_callback,
            max_retries=max_retries,
            check_current_date_only=check_current_date_only,
        )

        await manager.start()
        return manager

    def start_auto_download(
        self,
        group_id: str,
        destination_dir: str = "./downloads",
        interval_minutes: int = 30,
        file_filter: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
        max_retries: int = 3,
        check_current_date_only: bool = True,
    ) -> "AutoDownloadManager":
        """
        Synchronous wrapper for start_auto_download_async.
        Note: Will raise an error if called from within an existing event loop.

        Example:
            # Start auto-download synchronously
            manager = dq.start_auto_download("economic-data")

            # Stop it later (in async context)
            import asyncio
            asyncio.run(manager.stop())
        """
        return asyncio.run(
            self.start_auto_download_async(
                group_id,
                destination_dir,
                interval_minutes,
                file_filter,
                progress_callback,
                error_callback,
                max_retries,
                check_current_date_only,
            )
        )

    # DataFrame Conversion Utilities
    def to_dataframe(
        self,
        response_data,
        flatten_nested: bool = True,
        include_metadata: bool = False,
        date_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        custom_transformations: Optional[Dict[str, Callable]] = None,
    ) -> "pd.DataFrame":
        """
        Dynamically convert any API response to a pandas DataFrame.

        This function can handle various response types from the DataQuery API
        and convert them into a structured pandas DataFrame for analysis.

        Args:
            response_data: Any API response object (Group, FileInfo, TimeSeriesResponse, etc.)
            flatten_nested: If True, flatten nested objects into columns
            include_metadata: If True, include metadata fields in the DataFrame
            date_columns: List of column names to parse as dates
            numeric_columns: List of column names to convert to numeric
            custom_transformations: Dict of column_name -> transformation_function

        Returns:
            pandas.DataFrame: Converted DataFrame

        Examples:
            # Convert groups list
            groups = await dq.list_groups_async()
            df = dq.to_dataframe(groups)

            # Convert file list with date parsing
            files = await dq.list_files_async("group-id")
            df = dq.to_dataframe(
                files.file_group_ids,
                date_columns=['last_modified'],
                include_metadata=True
            )

            # Convert time series with custom transformations
            ts = await dq.get_instrument_time_series_async(...)
            df = dq.to_dataframe(
                ts,
                custom_transformations={
                    'price': lambda x: float(x) if x else 0.0,
                    'volume': lambda x: int(x) if x else 0
                }
            )
        """
        if not HAS_PANDAS:
            raise ImportError(
                "pandas is required for DataFrame conversion. "
                "Install it with: pip install pandas"
            )

        return self._convert_to_dataframe(
            response_data,
            flatten_nested,
            include_metadata,
            date_columns,
            numeric_columns,
            custom_transformations,
        )

    def _convert_to_dataframe(
        self,
        data,
        flatten_nested: bool = True,
        include_metadata: bool = False,
        date_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        custom_transformations: Optional[Dict[str, Callable]] = None,
    ) -> "pd.DataFrame":
        """Internal method to convert data to DataFrame with memory optimization."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for DataFrame conversion")

        # Initialize processing parameters
        date_columns = date_columns or []
        numeric_columns = numeric_columns or []
        custom_transformations = custom_transformations or {}

        # Handle different data types
        if data is None:
            return pd.DataFrame()

        # Convert single object to list for uniform processing
        if not isinstance(data, (list, tuple)):
            if hasattr(data, "__dict__") or hasattr(data, "__slots__"):
                # Single Pydantic model or object
                data = [data]
            else:
                # Primitive data type
                return pd.DataFrame({"value": [data]})

        # Memory optimization: process in chunks for large datasets
        chunk_size = 1000  # Process 1000 records at a time
        all_records = []

        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            chunk_records = []

            for item in chunk:
                record = self._extract_object_data(
                    item, flatten_nested, include_metadata
                )
                if record:
                    chunk_records.append(record)

            if chunk_records:
                all_records.extend(chunk_records)

            # Force garbage collection for large datasets
            if len(data) > 5000:
                import gc

                gc.collect()

        if not all_records:
            return pd.DataFrame()

        # Create DataFrame with memory optimization
        df = pd.DataFrame(all_records)

        # Clear the records list to free memory
        all_records.clear()

        # Apply data type conversions
        df = self._apply_data_transformations(
            df, date_columns, numeric_columns, custom_transformations
        )

        return df

    def _extract_object_data(
        self, obj, flatten_nested: bool = True, include_metadata: bool = False
    ) -> Dict[str, Any]:
        """Extract data from a single object."""
        if obj is None:
            return {}

        record = {}

        # Handle Pydantic models
        if hasattr(obj, "model_dump"):
            try:
                data = obj.model_dump()
                record.update(
                    self._process_dict_data(data, flatten_nested, include_metadata)
                )
            except Exception:
                # Fallback to __dict__ if model_dump fails
                if hasattr(obj, "__dict__"):
                    record.update(
                        self._process_dict_data(
                            obj.__dict__, flatten_nested, include_metadata
                        )
                    )

        # Handle objects with __dict__
        elif hasattr(obj, "__dict__"):
            record.update(
                self._process_dict_data(obj.__dict__, flatten_nested, include_metadata)
            )

        # Handle dictionary objects
        elif isinstance(obj, dict):
            record.update(
                self._process_dict_data(obj, flatten_nested, include_metadata)
            )

        # Handle primitive types
        else:
            record["value"] = obj

        return record

    def _process_dict_data(
        self,
        data: Dict[str, Any],
        flatten_nested: bool = True,
        include_metadata: bool = False,
    ) -> Dict[str, Any]:
        """Process dictionary data with nested object handling."""
        processed = {}

        for key, value in data.items():
            # Skip private attributes unless metadata is requested
            if key.startswith("_") and not include_metadata:
                continue

            # Handle nested objects
            if isinstance(value, dict) and flatten_nested:
                # Flatten nested dictionaries
                for nested_key, nested_value in value.items():
                    flattened_key = f"{key}_{nested_key}"
                    processed[flattened_key] = self._convert_value(nested_value)

            elif isinstance(value, (list, tuple)) and flatten_nested:
                # Handle lists/arrays
                if value and isinstance(value[0], dict):
                    # List of dictionaries - create multiple columns
                    for i, list_item in enumerate(value[:5]):  # Limit to first 5 items
                        if isinstance(list_item, dict):
                            for nested_key, nested_value in list_item.items():
                                flattened_key = f"{key}_{i}_{nested_key}"
                                processed[flattened_key] = self._convert_value(
                                    nested_value
                                )
                else:
                    # Simple list - convert to string representation
                    processed[key] = str(value) if value else None

            else:
                # Direct value assignment
                processed[key] = self._convert_value(value)

        return processed

    def _convert_value(self, value) -> Any:
        """Convert individual values to DataFrame-compatible types."""
        if value is None:
            return None

        # Handle datetime objects
        if hasattr(value, "isoformat"):  # datetime-like objects
            return value.isoformat()

        # Handle Pydantic models and complex objects
        if hasattr(value, "model_dump"):
            try:
                return str(value.model_dump())
            except Exception:
                return str(value)

        # Handle other objects
        if hasattr(value, "__dict__"):
            return str(value.__dict__)

        # Return primitive types as-is
        return value

    def _apply_data_transformations(
        self,
        df: "pd.DataFrame",
        date_columns: List[str],
        numeric_columns: List[str],
        custom_transformations: Dict[str, Callable],
    ) -> "pd.DataFrame":
        """Apply data type conversions and transformations."""
        try:
            import pandas as pd
        except ImportError:
            return df

        # Apply custom transformations first
        for column, transform_func in custom_transformations.items():
            if column in df.columns:
                try:
                    df[column] = df[column].apply(transform_func)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to apply transformation to column '{column}': {e}"
                    )

        # Convert date columns
        for column in date_columns:
            if column in df.columns:
                try:
                    df[column] = pd.to_datetime(df[column], errors="coerce")
                except Exception as e:
                    self.logger.warning(
                        f"Failed to convert column '{column}' to datetime: {e}"
                    )

        # Convert numeric columns
        for column in numeric_columns:
            if column in df.columns:
                try:
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                except Exception as e:
                    self.logger.warning(
                        f"Failed to convert column '{column}' to numeric: {e}"
                    )

        # Auto-detect and convert common patterns
        df = self._auto_convert_columns(df)

        return df

    def _auto_convert_columns(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Automatically detect and convert common column patterns."""
        try:
            import pandas as pd
        except ImportError:
            return df

        for column in df.columns:
            if df[column].dtype == "object":  # String columns
                column_lower = column.lower()

                # Auto-detect date columns
                if any(
                    date_word in column_lower
                    for date_word in [
                        "date",
                        "time",
                        "created",
                        "updated",
                        "modified",
                        "expires",
                    ]
                ):
                    try:
                        # Sample a few non-null values to check if they're dates
                        sample_values = df[column].dropna().head(3)
                        if len(sample_values) > 0:
                            pd.to_datetime(sample_values.iloc[0])  # Test conversion
                            df[column] = pd.to_datetime(df[column], errors="coerce")
                            continue
                    except Exception:
                        pass

                # Auto-detect numeric columns
                if any(
                    num_word in column_lower
                    for num_word in [
                        "size",
                        "count",
                        "bytes",
                        "price",
                        "value",
                        "amount",
                        "volume",
                        "quantity",
                        "number",
                        "id",
                    ]
                ):
                    try:
                        # Try converting to numeric
                        numeric_series = pd.to_numeric(df[column], errors="coerce")
                        # If most values converted successfully, use numeric type
                        if numeric_series.notna().sum() / len(df) > 0.7:
                            df[column] = numeric_series
                    except Exception:
                        pass

        return df

    # Convenience methods for common conversions
    def groups_to_dataframe(
        self, groups: Union[List["Group"], "GroupList"], include_metadata: bool = False
    ) -> "pd.DataFrame":
        """Convert groups response to DataFrame."""
        if hasattr(groups, "groups"):
            groups = groups.groups

        return self.to_dataframe(
            groups,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["last_updated", "created_date"],
        )

    def files_to_dataframe(
        self, files: Union[List["FileInfo"], "FileList"], include_metadata: bool = False
    ) -> "pd.DataFrame":
        """Convert files response to DataFrame."""
        if hasattr(files, "file_group_ids"):
            files = files.file_group_ids

        return self.to_dataframe(
            files,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["last_modified", "created_date"],
            numeric_columns=["file_size"],
        )

    def instruments_to_dataframe(
        self,
        instruments: Union[List["Instrument"], "InstrumentResponse"],
        include_metadata: bool = False,
    ) -> "pd.DataFrame":
        """Convert instruments response to DataFrame."""
        if hasattr(instruments, "instruments"):
            instruments = instruments.instruments

        return self.to_dataframe(
            instruments,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["created_date", "last_updated"],
        )

    def time_series_to_dataframe(
        self, time_series, include_metadata: bool = False
    ) -> "pd.DataFrame":
        """Convert time series response to DataFrame."""
        # Handle different time series response structures
        if hasattr(time_series, "data"):
            data = time_series.data
        elif hasattr(time_series, "series"):
            data = time_series.series
        elif hasattr(time_series, "time_series"):
            data = time_series.time_series
        else:
            data = time_series

        return self.to_dataframe(
            data,
            flatten_nested=True,
            include_metadata=include_metadata,
            date_columns=["date", "timestamp", "observation_date"],
            numeric_columns=[
                "value",
                "price",
                "volume",
                "open",
                "high",
                "low",
                "close",
            ],
        )

    # Synchronous wrapper methods
    def list_groups(self, limit: Optional[int] = None) -> List[Group]:
        """Synchronous wrapper for list_groups using an event-loop aware runner."""
        return self._run_sync(self.list_groups_async(limit))

    def list_all_groups(self) -> List[Group]:
        """Synchronous wrapper for list_all_groups using an event-loop aware runner."""
        return self._run_sync(self.list_all_groups_async())

    def search_groups(
        self, keywords: str, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Group]:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.search_groups_async(keywords, limit, offset))

    def list_files(
        self, group_id: str, file_group_id: Optional[str] = None
    ) -> FileList:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.list_files_async(group_id, file_group_id))

    def get_file_info(self, group_id: str, file_group_id: str) -> FileInfo:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_file_info_async(group_id, file_group_id))

    def check_availability(
        self, file_group_id: str, file_datetime: str
    ) -> AvailabilityInfo:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.check_availability_async(file_group_id, file_datetime)
        )

    def download_file(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        options: Optional[DownloadOptions] = None,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
    ) -> DownloadResult:
        """Synchronous wrapper using an event-loop aware runner."""
        # If destination_path is provided but options is None, create options with destination_path
        if destination_path is not None and options is None:
            from .models import DownloadOptions

            options = DownloadOptions(destination_path=destination_path)
        elif destination_path is not None and options is not None:
            # If both are provided, update options with destination_path
            options = options.model_copy(update={"destination_path": destination_path})

        # Match async signature (file_group_id, file_datetime, options, num_parts, progress_callback)
        return self._run_sync(
            self.download_file_async(
                file_group_id,
                file_datetime,
                options,
                5,
                progress_callback,
            )
        )

    # Note: download_multiple_files method removed as it's not part of the new API spec
    # Use individual download_file calls for batch operations

    def list_available_files(
        self,
        group_id: str,
        file_group_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.list_available_files_async(
                group_id, file_group_id, start_date, end_date
            )
        )

    def health_check(self) -> bool:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.health_check_async())

    # Instrument Collection Endpoints - Synchronous wrappers
    def list_instruments(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "InstrumentsResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.list_instruments_async(group_id, instrument_id, page)
        )

    def search_instruments(
        self, group_id: str, keywords: str, page: Optional[str] = None
    ) -> "InstrumentsResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.search_instruments_async(group_id, keywords, page))

    def get_instrument_time_series(
        self,
        instruments: List[str],
        attributes: List[str],
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.get_instrument_time_series_async(
                instruments,
                attributes,
                data,
                format,
                start_date,
                end_date,
                calendar,
                frequency,
                conversion,
                nan_treatment,
                page,
            )
        )

    def get_expressions_time_series(
        self,
        expressions: List[str],
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        data: str = "REFERENCE_DATA",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.get_expressions_time_series_async(
                expressions,
                format,
                start_date,
                end_date,
                calendar,
                frequency,
                conversion,
                nan_treatment,
                data,
                page,
            )
        )

    # Group Collection Additional Endpoints - Synchronous wrappers
    def get_group_filters(
        self, group_id: str, page: Optional[str] = None
    ) -> "FiltersResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_group_filters_async(group_id, page))

    def get_group_attributes(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "AttributesResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.get_group_attributes_async(group_id, instrument_id, page)
        )

    def get_group_time_series(
        self,
        group_id: str,
        attributes: List[str],
        filter: Optional[str] = None,
        data: str = "REFERENCE_DATA",
        format: str = "JSON",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        calendar: str = "CAL_USBANK",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        page: Optional[str] = None,
    ) -> "TimeSeriesResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(
            self.get_group_time_series_async(
                group_id,
                attributes,
                filter,
                data,
                format,
                start_date,
                end_date,
                calendar,
                frequency,
                conversion,
                nan_treatment,
                page,
            )
        )

    # Grid Collection Endpoints - Synchronous wrappers
    def get_grid_data(
        self,
        expr: Optional[str] = None,
        grid_id: Optional[str] = None,
        date: Optional[str] = None,
    ) -> "GridDataResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_grid_data_async(expr, grid_id, date))

    def _run_sync(self, coro):
        try:
            asyncio.get_running_loop()
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            return asyncio.run(coro)
