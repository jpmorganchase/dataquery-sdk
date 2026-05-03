"""
Main client for the DATAQUERY SDK.
"""

import asyncio
import socket
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from ..sse.subscriber import NotificationDownloadManager

import aiohttp
import structlog

from .. import constants as C
from ..config import LogFormat, LoggingConfig, LoggingManager, LogLevel
from ..transport.auth import OAuthManager
from ..transport.connection_pool import ConnectionPoolConfig, ConnectionPoolMonitor
from ..transport.rate_limiter import (
    QueuePriority,
    RateLimitConfig,
    RateLimitContext,
    TokenBucketRateLimiter,
)
from ..transport.retry import RetryConfig, RetryManager, RetryStrategy
from ..types.exceptions import (
    AuthenticationError,
    ConfigurationError,
    FileNotFoundInGroupError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
)
from ..types.models import (
    AttributesResponse,
    AvailabilityInfo,
    ClientConfig,
    DownloadOptions,
    DownloadProgress,
    DownloadResult,
    DownloadStatus,
    ErrorResponse,
    FileInfo,
    FileList,
    FiltersResponse,
    GridDataResponse,
    Group,
    GroupList,
    InstrumentsResponse,
    TimeSeriesResponse,
)
from ..utils import (
    format_file_size,
    get_filename_from_response,
    validate_attributes_list,
    validate_date_format,
    validate_file_datetime,
    validate_instruments_list,
)
from ._mixins import (
    DataFrameMixin,
    GridMixin,
    InstrumentsMixin,
    MetadataMixin,
    TimeSeriesMixin,
)

__all__ = [
    "DataQueryClient",
    "get_filename_from_response",
    "validate_attributes_list",
    "validate_date_format",
    "validate_file_datetime",
    "validate_instruments_list",
]

logger = structlog.get_logger(__name__)


class DataQueryClient(
    DataFrameMixin,
    InstrumentsMixin,
    MetadataMixin,
    TimeSeriesMixin,
    GridMixin,
):
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
            format=(LogFormat.JSON if self.config.enable_debug_logging else LogFormat.CONSOLE),
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
            circuit_breaker_threshold=self.config.circuit_breaker_threshold,
            retryable_exceptions=[
                ConnectionError,
                TimeoutError,
                asyncio.TimeoutError,
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
        self._response_cache: OrderedDict[str, tuple] = OrderedDict()  # key -> (data, timestamp)
        self._cache_ttl = 300  # 5 minutes cache TTL
        self._cache_max_size = 256  # Maximum cache entries (LRU eviction)

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
        if not (self.config.base_url.startswith("http://") or self.config.base_url.startswith("https://")):
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

    async def __aexit__(self, *_):
        """Async context manager exit."""
        await self.close()

    def _get_cache_key(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
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
                self._response_cache.move_to_end(cache_key)
                return data
            else:
                # Remove expired entry
                del self._response_cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any) -> None:
        """Store data in cache with LRU eviction."""
        if cache_key in self._response_cache:
            self._response_cache.move_to_end(cache_key)
        self._response_cache[cache_key] = (data, time.time())
        while len(self._response_cache) > self._cache_max_size:
            self._response_cache.popitem(last=False)

    def clear_cache(self) -> None:
        """Clear all cached responses."""
        self._response_cache.clear()

    async def connect(self):
        """Initialize HTTP session with optimized configuration."""
        if self.session is None:
            # Optimize timeout configuration
            timeout = aiohttp.ClientTimeout(total=self.config.timeout, connect=300.0, sock_read=self.config.timeout)

            # Optimize connector configuration for better performance
            connector = aiohttp.TCPConnector(
                limit=self.config.pool_maxsize,
                limit_per_host=self.config.pool_connections,
                keepalive_timeout=300,  # Increased for better connection reuse with longer timeouts
                enable_cleanup_closed=True,
                use_dns_cache=True,  # Enable DNS caching
                ttl_dns_cache=300,  # 5 minutes DNS cache
                family=socket.AF_UNSPEC,  # Allow both IPv4 and IPv6
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

    def _validate_request_url(self, url: str, params: Optional[Dict[str, Any]] = None) -> None:
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

    async def _make_authenticated_request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
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
                priority=self._get_operation_priority(method, self._extract_endpoint(url)),
                operation=f"{method}_{self._extract_endpoint(url)}",
            ):
                # Execute request with retry logic
                response = await self.retry_manager.execute_with_retry(self._execute_request, method, url, **kwargs)

            # Record operation success
            duration = time.time() - start_time
            self.logging_manager.log_operation_end(operation, duration, success=True)

            # Log request/response if enabled
            if self.config.enable_debug_logging:
                self.logging_manager.log_request(method, url, kwargs.get("headers", {}))
                self.logging_manager.log_response(response.status, dict(response.headers), duration=duration)

            return response

        except Exception as e:
            # Record operation failure
            duration = time.time() - start_time
            self.logging_manager.log_operation_end(operation, duration, success=False, error=str(e))
            raise

    async def _execute_request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """Execute a single HTTP request."""
        # Ensure we have fresh authentication headers (prefer per-request freshness; avoid stale session headers)
        try:
            auth_headers = await self.auth_manager.get_headers()
            headers = dict(kwargs.get("headers") or {})
            headers.update(auth_headers)
            kwargs["headers"] = headers
        except AuthenticationError:
            raise
        except Exception as e:
            raise AuthenticationError(f"Failed to obtain auth headers: {e}")

        # Apply proxy per-request if configured — shared helper keeps this in
        # sync with auth.py and sse_client.py.
        for key, value in self.config.get_proxy_kwargs().items():
            kwargs.setdefault(key, value)

        # Ensure session is connected
        await self._ensure_connected()

        if self.session is None:
            raise NetworkError("Failed to establish connection")

        try:
            response = await self.session.request(method, url, **kwargs)

            # Check for retryable server errors (5xx) - these should trigger retry
            if response.status >= 500:
                # Drain body so the connection can be reused; ignore decode errors.
                try:
                    await response.text()
                except (UnicodeDecodeError, aiohttp.ClientPayloadError):
                    pass
                raise NetworkError(f"Server error: {response.status}", status_code=response.status)

            return response
        except NetworkError:
            # Re-raise NetworkError to trigger retry
            raise
        except Exception:
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

        url = self._build_api_url(C.API_GROUPS)
        params = {}
        if limit is not None:
            params["limit"] = str(limit)

        try:
            async with await self._make_authenticated_request("GET", url, params=params) as response:
                await self._handle_response(response)
                data = await response.json()

                group_list = GroupList(**data)
                self.logger.info("Groups listed", count=len(group_list.groups), limit=limit)

                # Log performance metric
                self.logging_manager.log_metric("groups_listed", len(group_list.groups), "count")

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
        next_url: Optional[str] = self._build_api_url(C.API_GROUPS)
        page_count = 0
        # Guard against pathological servers that loop the next-link or that
        # paginate without bound. 1000 pages is far above any realistic
        # catalog size; visited set catches the simpler echo case earlier.
        max_pages = 1000
        visited: set = set()

        try:
            while next_url:
                if next_url in visited:
                    self.logger.warning(
                        "Pagination loop detected — server returned a previously seen next link",
                        url=next_url,
                        page=page_count,
                    )
                    break
                visited.add(next_url)
                page_count += 1
                if page_count > max_pages:
                    self.logger.warning(
                        "Pagination cap hit — stopping after max_pages",
                        max_pages=max_pages,
                        total_groups=len(all_groups),
                    )
                    break

                async with await self._make_authenticated_request("GET", next_url) as response:
                    await self._handle_response(response)
                    data = await response.json()

                    group_list = GroupList(**data)
                    all_groups.extend(group_list.groups)

                    # One page-level log instead of two; the redundant
                    # "fetching/fetched" pair doubled log volume on big catalogs.
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

        url = self._build_api_url(C.API_GROUPS_SEARCH)

        try:
            async with await self._make_authenticated_request("GET", url, params=params) as response:
                await self._handle_response(response)
                data = await response.json()

                # Assuming the search endpoint returns the same structure as list_groups
                group_list = GroupList(**data)
                self.logger.info("Groups searched", keywords=keywords, count=len(group_list.groups))

                return group_list.groups

        except Exception as e:
            self.logger.error("Failed to search groups", keywords=keywords, error=str(e))
            raise

    async def list_files_async(self, group_id: str, file_group_id: Optional[str] = None) -> FileList:
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

        url = self._build_files_api_url(C.API_GROUP_FILES)

        try:
            async with await self._make_authenticated_request("GET", url, params=params) as response:
                await self._handle_response(response)
                data = await response.json()

                file_list = FileList(**data)
                self.logger.info("Files listed", group_id=group_id, count=file_list.file_count)

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
            raise FileNotFoundInGroupError(file_group_id, group_id)

        return file_list.file_group_ids[0]

    async def check_availability_async(self, file_group_id: str, file_datetime: str) -> AvailabilityInfo:
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

        url = self._build_files_api_url(C.API_GROUP_FILE_AVAILABILITY)

        try:
            async with await self._make_authenticated_request("GET", url, params=params) as response:
                await self._handle_response(response)
                data = await response.json()
                # Extract a single availability item matching the requested datetime if present
                items: List[Dict[str, Any]] = data.get("availability") or [] if isinstance(data, dict) else []
                selected = None
                for it in items:
                    if isinstance(it, dict) and it.get("file-datetime") == file_datetime:
                        selected = it
                        break
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

    def _prepare_download_params(
        self,
        file_group_id: str,
        file_datetime: Optional[str],
        options: Optional[DownloadOptions],
        num_parts: Optional[int] = None,
    ) -> Tuple[Dict[str, str], DownloadOptions, int]:
        """Prepare and validate download parameters."""
        if file_datetime:
            validate_file_datetime(file_datetime)
        if options is None:
            options = DownloadOptions()

        if not num_parts or num_parts <= 0:
            num_parts = 1

        params = {"file-group-id": file_group_id}
        if file_datetime:
            params["file-datetime"] = file_datetime

        return params, options, num_parts

    def _resolve_destination(
        self,
        options: DownloadOptions,
        file_group_id: str,
        filename: Optional[str] = None,
    ) -> Path:
        """Resolve the final destination path for the file."""
        if options.destination_path:
            dest_path = Path(options.destination_path)
            if dest_path.suffix:
                # It's a full file path
                destination = dest_path
                destination_dir = dest_path.parent
            else:
                # It's a directory
                destination_dir = dest_path
                destination = destination_dir / (filename or f"{file_group_id}.bin")
        else:
            destination_dir = Path(self.config.download_dir)
            destination = destination_dir / (filename or f"{file_group_id}.bin")

        if options.create_directories:
            destination_dir.mkdir(parents=True, exist_ok=True)

        return destination

    def _create_download_result(
        self,
        file_group_id: str,
        destination: Optional[Path],
        total_bytes: int,
        bytes_downloaded: int,
        start_time: float,
        status: DownloadStatus,
        error: Optional[Exception] = None,
    ) -> DownloadResult:
        """Create a DownloadResult object."""
        return DownloadResult(
            file_group_id=file_group_id,
            group_id="",
            local_path=destination or (Path(self.config.download_dir) / f"{file_group_id}.tmp"),
            file_size=total_bytes,
            download_time=time.time() - start_time,
            bytes_downloaded=bytes_downloaded,
            status=status,
            error_message=f"{type(error).__name__}: {error}" if error else None,
        )

    async def download_file_async(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        options: Optional[DownloadOptions] = None,
        num_parts: int = 1,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """
        Download a specific file using single-stream or parallel HTTP range requests.

        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            options: Download options
            num_parts: Number of parallel parts to split the file into (default 1,
                single-stream). Set >1 to enable parallel HTTP range requests.
            progress_callback: Optional progress callback function

        Returns:
            DownloadResult with download information
        """
        _, options, num_parts = self._prepare_download_params(file_group_id, file_datetime, options, num_parts)

        if num_parts <= 1 or not self.config.enable_range_downloads:
            return await self._download_file_single_stream(
                file_group_id=file_group_id,
                file_datetime=file_datetime,
                options=options,
                progress_callback=progress_callback,
            )

        from ..download.parallel import download_file_multipart

        return await download_file_multipart(
            client=self,
            file_group_id=file_group_id,
            file_datetime=file_datetime,
            options=options,
            num_parts=num_parts,
            progress_callback=progress_callback,
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
        params, options, _ = self._prepare_download_params(file_group_id, file_datetime, options)

        # Add range parameters if specified
        if options.range_header:
            headers = {"Range": options.range_header}
        elif options.range_start is not None:
            range_end = options.range_end if options.range_end is not None else ""
            headers = {"Range": f"bytes={options.range_start}-{range_end}"}
        else:
            headers = {}

        start_time = time.time()
        bytes_downloaded = 0
        destination = None  # Initialize destination variable

        try:
            url = self._build_files_api_url(C.API_GROUP_FILE_DOWNLOAD)

            # Support either an awaitable that yields a context manager, or a context manager directly
            async with await self._make_authenticated_request("GET", url, params=params, headers=headers) as response:
                await self._handle_response(response)

                # Extract filename from Content-Disposition header or generate one
                filename = get_filename_from_response(response, file_group_id, file_datetime)
                destination = self._resolve_destination(options, file_group_id, filename)

                # Check if file exists and handle overwrite
                if isinstance(destination, Path) and destination.exists() and not options.overwrite_existing:
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
                temp_destination = destination.with_suffix(destination.suffix + C.TEMP_SUFFIX)

                # Optimize chunk size based on file size
                chunk_size = options.chunk_size or C.DEFAULT_CHUNK_SIZE
                if total_bytes > 0:
                    # Use larger chunks for files >= LARGE_FILE_THRESHOLD (1 GB).
                    max_chunk = (
                        C.LARGE_FILE_CHUNK_SIZE if total_bytes > C.LARGE_FILE_THRESHOLD else C.DEFAULT_CHUNK_SIZE
                    )
                    optimal_chunk_size = min(max(chunk_size, total_bytes // 1000), max_chunk)
                    chunk_size = optimal_chunk_size

                # Progress update frequency optimization
                progress_update_interval = max(1, chunk_size // 4)  # Update every 1/4 chunk
                last_progress_update = 0

                # Dynamic buffer size based on chunk size (minimum 1MB, maximum 8MB)
                buffer_size = min(max(chunk_size, C.DEFAULT_CHUNK_SIZE), C.LARGE_FILE_CHUNK_SIZE)
                with open(temp_destination, "wb", buffering=buffer_size) as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        await asyncio.to_thread(f.write, chunk)
                        bytes_downloaded += len(chunk)

                        # Update progress less frequently for better performance
                        if bytes_downloaded - last_progress_update >= progress_update_interval:
                            progress.update_progress(bytes_downloaded)
                            last_progress_update = bytes_downloaded

                            # Call progress callback
                            if progress_callback:
                                progress_callback(progress)
                            elif options.show_progress:
                                self.logger.debug(
                                    "Download progress",
                                    file=file_group_id,
                                    percentage=f"{progress.percentage:.1f}%",
                                    downloaded=format_file_size(bytes_downloaded),
                                )

                # Final progress update
                progress.update_progress(bytes_downloaded)

                # Atomic rename to final destination after successful write
                temp_destination.replace(destination)

                return self._create_download_result(
                    file_group_id,
                    destination,
                    bytes_downloaded,
                    bytes_downloaded,
                    start_time,
                    DownloadStatus.COMPLETED,
                )

        except FileExistsError as e:
            return self._create_download_result(
                file_group_id,
                destination,
                0,
                0,
                start_time,
                DownloadStatus.ALREADY_EXISTS,
                e,
            )
        except Exception as e:
            # Clean up partial file on error; filesystem errors are logged, not raised.
            try:
                if "temp_destination" in locals() and isinstance(temp_destination, Path):
                    temp_destination.unlink(missing_ok=True)
            except OSError as cleanup_err:
                self.logger.warning(
                    "Partial file cleanup failed after download error",
                    cleanup_error=str(cleanup_err),
                )

            return self._create_download_result(
                file_group_id,
                destination,
                0,
                bytes_downloaded,
                start_time,
                DownloadStatus.FAILED,
                e,
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

        url = self._build_files_api_url(C.API_GROUP_FILES_AVAILABLE)

        try:
            async with await self._make_authenticated_request("GET", url, params=params) as response:
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
            self.logger.error("Failed to list available files", group_id=group_id, error=str(e))
            raise

    async def health_check_async(self) -> bool:
        """Check if the DataQuery service is available."""
        try:
            url = self._build_api_url(C.API_HEARTBEAT)
            async with await self._make_authenticated_request("GET", url) as response:
                return response.status == 200
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False

    # Read-only query methods (instruments, metadata, time series, grid) live
    # in the query mixins — see _query_mixins.py.

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
            "connected": self.session is not None and not getattr(self.session, "closed", True),
        }

    async def _ensure_connected(self):
        """Ensure client is connected."""
        if self.session is None or (hasattr(self.session, "closed") and self.session.closed):
            await self.connect()

    @staticmethod
    def _parse_v2_error(body: Optional[str]) -> Optional[ErrorResponse]:
        """Parse a DataQuery v2 error envelope into an ``ErrorResponse``.

        Accepts the canonical flat shape::

            {"code": <number>, "description": "<text>"}

        and the common wrapped variants used across DataQuery deployments::

            {"info":   {"code": ..., "description": ...}}
            {"error":  {"code": ..., "description": ...}}
            {"errors": [{"code": ..., "description": ...}, ...]}

        Returns ``None`` when the body is empty, not JSON, or doesn't carry
        a recognisable error object.
        """
        if not body:
            return None
        import json as _json

        try:
            data = _json.loads(body)
        except (ValueError, TypeError):
            return None
        if not isinstance(data, dict):
            return None

        candidate: Optional[Dict[str, Any]] = None
        if "code" in data and "description" in data:
            candidate = data
        elif isinstance(data.get("info"), dict):
            candidate = data["info"]
        elif isinstance(data.get("error"), dict):
            candidate = data["error"]
        elif isinstance(data.get("errors"), list) and data["errors"]:
            first = data["errors"][0]
            if isinstance(first, dict):
                candidate = first

        if not candidate:
            return None
        try:
            return ErrorResponse.model_validate(candidate)
        except Exception:  # pragma: no cover - defensive
            return None

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

        # For non-2xx responses, read the body once and try to parse the
        # DataQuery v2 error envelope so the resulting exception carries the
        # server-supplied code/description rather than a bare HTTP status.
        api_error: Optional[ErrorResponse] = None
        error_body: Optional[str] = None
        if response.status >= 400:
            try:
                text = await response.text()
                error_body = text[:1000] if text else None
            except Exception:
                error_body = None
            api_error = self._parse_v2_error(error_body)
            self.logger.error(
                "HTTP error response",
                status=response.status,
                url=str(getattr(response, "url", "unknown")),
                interaction_id=interaction_id,
                body=error_body,
                api_error_code=getattr(api_error, "code", None),
                api_error_description=getattr(api_error, "description", None),
            )

        details: Dict[str, Any] = {"interaction_id": interaction_id, "status_code": response.status}
        if api_error is not None:
            details["code"] = api_error.code
            details["description"] = api_error.description
            extra = api_error.model_dump(exclude={"code", "description", "interaction_id"})
            if extra:
                details["extra"] = extra

        def _msg(default: str) -> str:
            if api_error and api_error.description:
                return f"{default}: [{api_error.code}] {api_error.description}"
            return default

        if response.status == 401:
            raise AuthenticationError(_msg("Authentication failed"), details=details)
        elif response.status == 403:
            raise AuthenticationError(
                _msg("Access denied - insufficient permissions"),
                details=details,
            )
        elif response.status == 404:
            resource_id = str(api_error.code) if api_error and api_error.code is not None else "unknown"
            err = NotFoundError("Resource", resource_id, message=_msg("Resource not found"))
            err.details.update(details)
            raise err
        # Handle rate limit response
        if response.status == 429:
            self.rate_limiter.handle_rate_limit_response(dict(response.headers))
            rate_err = RateLimitError(
                _msg(f"Rate limit exceeded: {response.status}"),
                retry_after=int(response.headers.get("Retry-After", 0)),
            )
            rate_err.details.update(details)
            raise rate_err
        elif response.status >= 500:
            net_err = NetworkError(
                _msg(f"Server error: {response.status}"),
                status_code=response.status,
            )
            net_err.details.update(details)
            raise net_err
        elif response.status >= 400:
            raise ValidationError(_msg(f"Client error: {response.status}"), details=details)

        # Mark successful request for adaptive backoff reset
        if response.status < 400:
            self.rate_limiter.handle_successful_request()

    async def _enter_request_cm(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
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

    # Auto-download is SSE-only — see auto_download_async below.

    async def auto_download_async(
        self,
        group_id: str,
        destination_dir: str = "./downloads",
        file_filter: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
        max_retries: int = 3,
        max_concurrent_downloads: int = 5,
        initial_check: bool = False,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        file_group_id: Optional[Union[str, List[str]]] = None,
        show_progress: bool = True,
        enable_event_replay: bool = True,
        heartbeat_timeout: float = 0.0,
        max_tracked_files: int = 10_000,
        max_tracked_errors: int = 1_000,
    ) -> "NotificationDownloadManager":
        """
        Subscribe to the /notification SSE endpoint and download new files.

        Starts a :class:`NotificationDownloadManager` that maintains a persistent
        SSE connection to the DataQuery notification endpoint. ``group_id`` (and
        optionally ``file_group_id``) are sent as query parameters so the server
        only emits events for the requested subscription.

        Args:
            group_id: Data group to subscribe to (sent as ``group-id``).
            destination_dir: Directory to download files to.
            file_filter: Optional predicate ``(file_info_dict) -> bool`` to
                         select which available files to download.
            progress_callback: Called with :class:`DownloadProgress` updates.
            error_callback: Called with exceptions from the SSE connection or
                            download failures.
            max_retries: Maximum retry attempts per file before giving up.
            max_concurrent_downloads: Concurrency limit for parallel downloads.
            initial_check: If ``True`` (default), perform a file-availability
                           check immediately on start, before any SSE events.
            reconnect_delay: Initial reconnection delay in seconds.
            max_reconnect_delay: Maximum reconnection delay in seconds.
            file_group_id: Optional restriction to one or more file-group-ids.
                           Accepts a single id or a list. Sent to the server as
                           the ``file-group-id`` query parameter (comma-separated
                           when a list).
            show_progress: If ``True`` (default), log download progress at
                           DEBUG level when no ``progress_callback`` is set.
            enable_event_replay: If ``True`` (default), persist the most
                           recently received SSE event id to disk and replay
                           from it on subsequent runs by sending it as the
                           ``last-event-id`` URL parameter. Replay supersedes
                           the bulk initial-check whenever a stored id is
                           found. Set ``False`` to keep the legacy
                           bulk-check-on-every-startup behaviour.
            heartbeat_timeout: Seconds. When > 0, force the SSE stream to
                           reconnect if no bytes (events or comment
                           heartbeats) arrive within this window. ``0`` (the
                           default) disables the watchdog and relies on the
                           server to close the stream cleanly.
            max_tracked_files: Bound on the in-memory dedup / retry maps so
                           the manager can run 24/7 without unbounded memory
                           growth. LRU eviction; default 10,000.
            max_tracked_errors: Bound on ``stats["errors"]`` (ring buffer);
                           default 1,000.

        Returns:
            A running :class:`NotificationDownloadManager` instance.

        Example::

            # Subscribe to every file in the group.
            manager = await client.auto_download_async(group_id="economic-data")

            # Subscribe to specific files only — the server filters.
            manager = await client.auto_download_async(
                group_id="economic-data",
                file_group_id=["JPM_CPI", "JPM_GDP"],
            )
        """
        from ..sse.subscriber import NotificationDownloadManager

        manager = NotificationDownloadManager(
            client=self,
            group_id=group_id,
            destination_dir=destination_dir,
            file_filter=file_filter,
            progress_callback=progress_callback,
            error_callback=error_callback,
            max_retries=max_retries,
            max_concurrent_downloads=max_concurrent_downloads,
            initial_check=initial_check,
            reconnect_delay=reconnect_delay,
            max_reconnect_delay=max_reconnect_delay,
            file_group_id=file_group_id,
            show_progress=show_progress,
            enable_event_replay=enable_event_replay,
            heartbeat_timeout=heartbeat_timeout,
            max_tracked_files=max_tracked_files,
            max_tracked_errors=max_tracked_errors,
        )
        await manager.start()
        return manager

    def auto_download(
        self,
        group_id: str,
        destination_dir: str = "./downloads",
        file_filter: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
        max_retries: int = 3,
        max_concurrent_downloads: int = 5,
        initial_check: bool = False,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        file_group_id: Optional[Union[str, List[str]]] = None,
        show_progress: bool = True,
        enable_event_replay: bool = True,
        heartbeat_timeout: float = 0.0,
        max_tracked_files: int = 10_000,
        max_tracked_errors: int = 1_000,
    ) -> "NotificationDownloadManager":
        """Synchronous wrapper for :meth:`auto_download_async`."""
        return asyncio.run(
            self.auto_download_async(
                group_id,
                destination_dir,
                file_filter,
                progress_callback,
                error_callback,
                max_retries,
                max_concurrent_downloads,
                initial_check,
                reconnect_delay,
                max_reconnect_delay,
                file_group_id=file_group_id,
                show_progress=show_progress,
                enable_event_replay=enable_event_replay,
                heartbeat_timeout=heartbeat_timeout,
                max_tracked_files=max_tracked_files,
                max_tracked_errors=max_tracked_errors,
            )
        )

    # DataFrame conversion methods (to_dataframe, groups_to_dataframe, etc.)
    # live in DataFrameMixin — see _dataframe_mixin.py.

    # Synchronous wrapper methods
    def list_groups(self, limit: Optional[int] = None) -> List[Group]:
        """Synchronous wrapper for list_groups using an event-loop aware runner."""
        return self._run_sync(self.list_groups_async(limit))

    def list_all_groups(self) -> List[Group]:
        """Synchronous wrapper for list_all_groups using an event-loop aware runner."""
        return self._run_sync(self.list_all_groups_async())

    def search_groups(self, keywords: str, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Group]:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.search_groups_async(keywords, limit, offset))

    def list_files(self, group_id: str, file_group_id: Optional[str] = None) -> FileList:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.list_files_async(group_id, file_group_id))

    def get_file_info(self, group_id: str, file_group_id: str) -> FileInfo:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_file_info_async(group_id, file_group_id))

    def check_availability(self, file_group_id: str, file_datetime: str) -> AvailabilityInfo:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.check_availability_async(file_group_id, file_datetime))

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
            from ..types.models import DownloadOptions

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

    def list_available_files(
        self,
        group_id: str,
        file_group_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.list_available_files_async(group_id, file_group_id, start_date, end_date))

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
        return self._run_sync(self.list_instruments_async(group_id, instrument_id, page))

    def search_instruments(self, group_id: str, keywords: str, page: Optional[str] = None) -> "InstrumentsResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.search_instruments_async(group_id, keywords, page))

    def get_instrument_time_series(
        self,
        instruments: List[str],
        attributes: List[str],
        data: str = "ALL",
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
        data: str = "ALL",
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
    def get_group_filters(self, group_id: str, page: Optional[str] = None) -> "FiltersResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_group_filters_async(group_id, page))

    def get_group_attributes(
        self,
        group_id: str,
        instrument_id: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "AttributesResponse":
        """Synchronous wrapper using an event-loop aware runner."""
        return self._run_sync(self.get_group_attributes_async(group_id, instrument_id, page))

    def get_group_time_series(
        self,
        group_id: str,
        attributes: List[str],
        filter: Optional[str] = None,
        data: str = "ALL",
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
