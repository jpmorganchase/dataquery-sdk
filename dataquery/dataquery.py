"""
Main DataQuery class for the DATAQUERY SDK.

This module provides the main DataQuery class that serves as the primary interface
for all API interactions, encapsulating the client and providing high-level operations.
"""

import asyncio
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import structlog
from dotenv import load_dotenv

from .client import DataQueryClient, format_file_size
from .config import EnvConfig
from .exceptions import ConfigurationError
from .models import (
    AttributesResponse,
    AvailabilityInfo,
    ClientConfig,
    DownloadOptions,
    DownloadResult,
    DownloadStatus,
    FileInfo,
    FiltersResponse,
    GridDataResponse,
    Group,
    InstrumentsResponse,
    TimeSeriesResponse,
)

# Load environment variables from .env file
load_dotenv()

logger = structlog.get_logger(__name__)


def setup_logging(log_level: str = "INFO") -> structlog.BoundLogger:
    """Deprecated shim: prefer `LoggingManager`.

    This function intentionally does not configure structlog. It returns a
    namespaced logger tagged to indicate deprecated usage.
    """
    return structlog.get_logger(__name__).bind(
        deprecated_setup_logging=True,
        level=log_level,
    )


"""
Note: format_file_size, format_duration, and ensure_directory are imported from
utils to maintain a single source of truth for these helpers across the SDK.
"""


def get_download_paths() -> Dict[str, Path]:
    """Get download paths from environment variables with defaults."""
    base_download_dir = EnvConfig.get_path("DOWNLOAD_DIR", "./downloads")

    return {
        "base": base_download_dir,
        "workflow": base_download_dir
        / (EnvConfig.get_env_var("WORKFLOW_DIR", "workflow") or "workflow"),
        "groups": base_download_dir
        / (EnvConfig.get_env_var("GROUPS_DIR", "groups") or "groups"),
        "availability": base_download_dir
        / (EnvConfig.get_env_var("AVAILABILITY_DIR", "availability") or "availability"),
        "default": base_download_dir
        / (EnvConfig.get_env_var("DEFAULT_DIR", "files") or "files"),
    }


class ConfigManager:
    """Configuration manager for DATAQUERY SDK."""

    def __init__(self, env_file: Optional[Path] = None):
        """
        Initialize ConfigManager.

        Args:
            env_file: Optional path to .env file. If None, will look for .env in current directory.
        """
        self.env_file = env_file

    def get_client_config(self) -> ClientConfig:
        """Get client configuration from environment variables."""
        try:
            # Pass env_file as keyword argument to match expected signature
            config = EnvConfig.create_client_config(env_file=self.env_file)
            EnvConfig.validate_config(config)
            return config
        except Exception as e:
            logger.warning(
                "Failed to load configuration from environment", error=str(e)
            )
            return self._get_default_config()

    def _get_default_config(self) -> ClientConfig:
        """Get default configuration for examples."""
        return ClientConfig(
            base_url="https://api.dataquery.com",
            # Set oauth_enabled False by default for fallback to satisfy tests
            oauth_enabled=False,
            # All other fields will use their default values from the model
        )


class ProgressTracker:
    """Progress tracking for batch operations."""

    def __init__(self, log_interval: int = 10):
        self.log_interval = log_interval
        self.last_log_time = 0.0

    def create_progress_callback(self) -> Callable:
        """Create a progress callback function."""

        def progress_callback(progress: Any):
            current_time = time.time()
            if current_time - self.last_log_time >= self.log_interval:
                logger.info(
                    "Batch progress",
                    completed=getattr(progress, "completed_files", 0),
                    total=getattr(progress, "total_files", 0),
                    percentage=f"{getattr(progress, 'percentage', 0):.1f}%",
                    current_file=getattr(progress, "current_file", "unknown"),
                )
                self.last_log_time = current_time

        return progress_callback


class DataQuery:
    """
    Main DataQuery class for all API interactions.

    This class serves as the primary interface for the DATAQUERY SDK,
    encapsulating the client and providing high-level operations for
    listing, searching, downloading, and managing data files.

    Supports both async and sync operations with proper event loop management.
    """

    def __init__(
        self,
        config_or_env_file: Optional[Union[ClientConfig, str, Path]] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        **overrides: Any,
    ):
        """
        Initialize DataQuery with configuration.

        Args:
            config_or_env_file: Either a ClientConfig object, a Path, or a str to .env file.
                               If None, will look for .env in current directory.
        """
        # Handle different input types
        if isinstance(config_or_env_file, ClientConfig):
            # Direct ClientConfig provided
            self.client_config = config_or_env_file
        else:
            # env_file provided (Path, str, or None)
            env_file = None
            if isinstance(config_or_env_file, (str, Path)):
                env_file = Path(config_or_env_file)
            config_manager = ConfigManager(env_file)
            self.client_config = config_manager.get_client_config()

        # Apply default-first initialization pattern with optional overrides.
        # Credentials are never defaulted; if provided, enable OAuth and set them.
        if client_id or client_secret:
            if client_id and not isinstance(client_id, str):
                raise ConfigurationError("client_id must be a string")
            if client_secret and not isinstance(client_secret, str):
                raise ConfigurationError("client_secret must be a string")

            self.client_config.oauth_enabled = True
            if client_id:
                self.client_config.client_id = client_id
            if client_secret:
                self.client_config.client_secret = client_secret
            # Auto-derive token URL if missing
            if not self.client_config.oauth_token_url and self.client_config.base_url:
                try:
                    self.client_config.oauth_token_url = (
                        f"{self.client_config.base_url.rstrip('/')}/oauth/token"
                    )
                except Exception:
                    pass

        # Apply any non-credential overrides (e.g., base_url, context_path, files_base_url, etc.)
        for key, value in (overrides or {}).items():
            if key in {"client_id", "client_secret"}:
                continue
            if hasattr(self.client_config, key) and value is not None:
                try:
                    setattr(self.client_config, key, value)
                except Exception:
                    # Best-effort: ignore invalid overrides silently to preserve backward compatibility
                    pass

        # Validate configuration
        try:
            EnvConfig.validate_config(self.client_config)
        except Exception as e:
            logger.error("Configuration validation failed", error=str(e))
            raise ConfigurationError(f"Configuration validation failed: {e}")

        self._client: Optional[DataQueryClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._own_loop: bool = False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect_async()
        return self

    def __enter__(self):
        """Sync context manager entry."""
        self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_async()

    async def connect_async(self):
        """Connect to the API."""
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
            await self._client.connect()

    async def close_async(self):
        """Close the connection and cleanup resources."""
        if self._client:
            await self._client.close()
            self._client = None

    async def cleanup_async(self):
        """Cleanup resources and ensure proper shutdown."""
        await self.close_async()

        # Force garbage collection to clean up any remaining references
        import gc

        gc.collect()

    def _run_sync(self, coro):
        """
        Run an async coroutine in a new event loop.

        Args:
            coro: Coroutine to run

        Returns:
            Result of the coroutine
        """
        try:
            return asyncio.run(coro)
        except RuntimeError as e:
            if "cannot run loop while another loop is running" in str(e):
                raise RuntimeError(
                    "Cannot run sync method when an asyncio event loop is already running. "
                    "Use the async version of the method instead."
                ) from e
            raise

    # Core API Methods

    async def list_groups_async(self, limit: Optional[int] = 100) -> List[Group]:
        """
        List all available data groups with pagination support.

        Args:
            limit: Maximum number of groups to return (default: 100). If None, returns all groups.

        Returns:
            List of group information
        """
        await self.connect_async()

        if limit is None:
            # Fetch all groups using pagination
            assert self._client is not None
            return await self._client.list_all_groups_async()
        else:
            # Fetch limited number of groups
            assert self._client is not None
            return await self._client.list_groups_async(limit=limit)

    async def search_groups_async(
        self, keywords: str, limit: Optional[int] = 100, offset: Optional[int] = None
    ) -> List[Group]:
        """
        Search groups by keywords.

        Args:
            keywords: Search keywords
            limit: Maximum number of results to return (default: 100)
            offset: Number of results to skip

        Returns:
            List of matching groups
        """
        await self.connect_async()
        assert self._client is not None
        return await self._client.search_groups_async(keywords, limit, offset)

    async def list_files_async(
        self, group_id: str, file_group_id: Optional[str] = None
    ) -> List[FileInfo]:
        """
        List all files in a group.

        Args:
            group_id: Group ID to list files for
            file_group_id: Optional specific file ID to filter by

        Returns:
            List of file information
        """
        await self.connect_async()
        assert self._client is not None
        file_list = await self._client.list_files_async(group_id, file_group_id)
        return file_list.file_group_ids

    async def check_availability_async(
        self, file_group_id: str, file_datetime: str
    ) -> AvailabilityInfo:
        """
        Check file availability for a specific datetime.

        Args:
            file_group_id: File ID to check availability for
            file_datetime: File datetime in YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS format

        Returns:
            Availability response with status for the datetime
        """
        await self.connect_async()
        assert self._client is not None
        return await self._client.check_availability_async(file_group_id, file_datetime)

    async def download_file_async(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        options: Optional[DownloadOptions] = None,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """
        Download a specific file using parallel HTTP range requests.

        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            destination_path: Optional download destination directory. The filename will be extracted
                             from the Content-Disposition header in the response. If not provided,
                             uses the default download directory from configuration.
            options: Download options
            num_parts: Number of parallel parts to split the file into (default 5)
            progress_callback: Optional progress callback function

        Returns:
            DownloadResult with download information
        """
        await self.connect_async()

        if destination_path and options is None:
            options = DownloadOptions(
                destination_path=destination_path,
                create_directories=True,
                overwrite_existing=False,
                chunk_size=8192,
                max_retries=3,
                retry_delay=1.0,
                timeout=600.0,
                enable_range_requests=True,
                range_start=None,
                range_end=None,
                range_header=None,
                show_progress=True,
                progress_callback=None,
            )

        assert self._client is not None
        # Pass parameters in correct order: file_group_id, file_datetime, options, num_parts, progress_callback
        # Use default num_parts=5
        return await self._client.download_file_async(
            file_group_id, file_datetime, options, num_parts, progress_callback
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
        await self.connect_async()
        assert self._client is not None
        return await self._client.list_available_files_async(
            group_id, file_group_id, start_date, end_date
        )

    async def health_check_async(self) -> bool:
        """
        Check if the API is healthy.

        Returns:
            True if API is healthy
        """
        await self.connect_async()
        assert self._client is not None
        return await self._client.health_check_async()

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
        await self.connect_async()
        assert self._client is not None
        return await self._client.list_instruments_async(group_id, instrument_id, page)

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
        await self.connect_async()
        assert self._client is not None
        return await self._client.search_instruments_async(group_id, keywords, page)

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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_instrument_time_series_async(
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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_expressions_time_series_async(
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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_group_filters_async(group_id, page)

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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_group_attributes_async(
            group_id, instrument_id, page
        )

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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_group_time_series_async(
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
        await self.connect_async()
        assert self._client is not None
        return await self._client.get_grid_data_async(expr, grid_id, date)

    # Workflow Methods

    async def run_groups_async(self, max_concurrent: int = 5) -> Dict[str, Any]:
        """Run complete operation for listing all groups."""
        logger.info("=== Starting Groups Operation ===")

        try:
            # Step 1: List all groups
            logger.info("Step 1: Listing All Groups")
            groups = await self.list_groups_async()

            if not groups:
                logger.warning("No groups found")
                return {"error": "No groups found"}

            # Step 2: Generate summary report
            logger.info("Step 2: Summary Report")
            report = {
                "total_groups": len(groups),
                "total_files": sum((g.file_groups or 0) for g in groups),
                "groups": [g.model_dump() for g in groups],
                "file_types": [],  # Groups don't have file_types attribute
                "providers": list(set(g.provider for g in groups if g.provider)),
            }

            logger.info("Groups operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error("Groups operation failed", error=str(e))
            raise

    async def run_group_files_async(
        self, group_id: str, max_concurrent: int = 5
    ) -> Dict[str, Any]:
        """Run complete operation for a specific group."""
        logger.info("=== Starting Group Files Operation ===", group_id=group_id)

        try:
            # Step 1: List files in the group
            logger.info("Step 1: Listing Files")
            files = await self.list_files_async(group_id)

            if not files:
                logger.warning("No files found for group", group_id=group_id)
                return {"error": "No files found"}

            # Step 2: Generate summary report
            logger.info("Step 2: Summary Report")
            # Collect file types robustly (handles str or List[str])
            _collected_types = []
            try:
                for _f in files:
                    _ft = getattr(_f, "file_type", None)
                    if isinstance(_ft, list):
                        _collected_types.extend([t for t in _ft if isinstance(t, str)])
                    elif isinstance(_ft, str):
                        _collected_types.append(_ft)
            except Exception:
                pass
            report = {
                "group_id": group_id,
                "total_files": len(files),
                "file_types": list(set(_collected_types)),
                "date_range": None,  # FileInfo doesn't have date_range attribute
                "files": [f.model_dump() for f in files],
            }

            logger.info("Group files operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error(
                "Group files operation failed", group_id=group_id, error=str(e)
            )
            raise

    async def run_availability_async(
        self, file_group_id: str, file_datetime: str
    ) -> Dict[str, Any]:
        """Run operation for checking file availability."""
        logger.info(
            "=== Starting Availability Operation ===",
            file_group_id=file_group_id,
            file_datetime=file_datetime,
        )

        try:
            # Step 1: Check availability
            logger.info("Step 1: Checking Availability")
            availability = await self.check_availability_async(
                file_group_id, file_datetime
            )

            # Step 2: Generate summary report
            logger.info("Step 2: Summary Report")
            report = {
                "file_group_id": file_group_id,
                "file_datetime": file_datetime,
                "is_available": bool(getattr(availability, "is_available", False)),
                "file_name": getattr(availability, "file_name", None),
                "first_created_on": getattr(availability, "first_created_on", None),
                "last_modified": getattr(availability, "last_modified", None),
            }

            logger.info("Availability operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error(
                "Availability operation failed",
                file_group_id=file_group_id,
                error=str(e),
            )
            raise

    async def run_download_async(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        max_concurrent: int = 1,
    ) -> Dict[str, Any]:
        """Run operation for downloading a single file."""
        logger.info(
            "=== Starting Download Operation ===",
            file_group_id=file_group_id,
            file_datetime=file_datetime,
        )

        try:
            # Step 1: Download file
            logger.info("Step 1: Downloading File")
            download_options = (
                DownloadOptions(
                    destination_path=destination_path,
                    create_directories=True,
                    overwrite_existing=False,
                    chunk_size=8192,
                    max_retries=3,
                    retry_delay=1.0,
                    timeout=600.0,
                    enable_range_requests=True,
                    range_start=None,
                    range_end=None,
                    range_header=None,
                    show_progress=True,
                    progress_callback=None,
                )
                if destination_path
                else None
            )
            result = await self.download_file_async(
                file_group_id, file_datetime, destination_path, download_options
            )

            # Step 2: Generate summary report
            logger.info("Step 2: Summary Report")
            report = {
                "file_group_id": file_group_id,
                "file_datetime": file_datetime,
                "download_successful": result.status == DownloadStatus.COMPLETED,
                "local_path": str(result.local_path),
                "file_size": result.file_size,
                "download_time": result.download_time,
                "speed_mbps": result.speed_mbps,
                "error_message": result.error_message,
            }

            logger.info("Download operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error(
                "Download operation failed", file_group_id=file_group_id, error=str(e)
            )
            raise

    async def run_group_download_async(
        self,
        group_id: str,
        start_date: str,
        end_date: str,
        destination_dir: Path = Path("./downloads"),
        max_concurrent: int = 5,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
        delay_between_downloads: float = 1.0,
    ) -> dict:
        """
        Download all files in a group for a date range using parallel HTTP range requests.

        This method implements a flattened concurrency model where the total concurrent
        HTTP requests = max_concurrent × num_parts, providing true parallelism across
        all file parts rather than hierarchical file-then-parts concurrency.

        Args:
            group_id: Group ID to download files from
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            destination_dir: Destination directory for downloads
            max_concurrent: Maximum concurrent files (multiplied by num_parts for total concurrency)
            num_parts: Number of HTTP range parts per file (default 5)
            progress_callback: Optional progress callback for individual parts aggregation
            delay_between_downloads: Delay in seconds between starting each file download (default 1.0)

        Returns:
            Dictionary with download results and statistics
        """
        # Record start time for total operation timing
        operation_start_time = time.time()

        logger.info(
            "=== Starting Group Parallel Download for Date Range Operation ===",
            group_id=group_id,
            start_date=start_date,
            end_date=end_date,
            max_concurrent=max_concurrent,
            num_parts=num_parts,
            total_concurrency=max_concurrent * num_parts,
        )

        await self.connect_async()
        assert self._client is not None

        try:
            # Step 1: Get available files for the date range
            logger.info("Step 1: Getting Available Files for Date Range")
            available_files = await self.list_available_files_async(
                group_id=group_id, start_date=start_date, end_date=end_date
            )

            # Filter to only files explicitly marked available
            try:
                filtered_files = [
                    f
                    for f in (available_files or [])
                    if (f.get("is-available") is True)
                    or (f.get("is_available") is True)
                ]
            except Exception:
                filtered_files = []

            if not filtered_files:
                # Calculate timing even when no files are found
                operation_end_time = time.time()
                total_time_seconds = operation_end_time - operation_start_time
                total_time_minutes = total_time_seconds / 60.0

                logger.warning(
                    "No available files found for date range",
                    group_id=group_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                return {
                    "error": "No available files found for date range",
                    "group_id": group_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "total_files": 0,
                    "successful_downloads": 0,
                    "failed_downloads": 0,
                    "success_rate": 0.0,
                    "total_time_seconds": round(total_time_seconds, 2),
                    "total_time_minutes": round(total_time_minutes, 2),
                    "total_time_formatted": (
                        f"{int(total_time_minutes)}m {int(total_time_seconds % 60)}s"
                        if total_time_minutes >= 1
                        else f"{total_time_seconds:.1f}s"
                    ),
                }

            logger.info("Found available files", count=len(filtered_files))

            # Step 2: Download all available files using flattened concurrency model
            logger.info(
                "Step 2: Downloading Available Files with flattened concurrency model"
            )

            # Create destination directory
            dest_dir = destination_dir / group_id
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Rate-limit-aware flattened concurrency with delay-based protection
            total_concurrent_requests = max_concurrent * num_parts

            # Use full concurrency but with intelligent delay calculation
            rate_limit_capacity = self._calculate_rate_limit_capacity()
            intelligent_delay = self._calculate_intelligent_delay(
                total_concurrent_requests, rate_limit_capacity, delay_between_downloads
            )

            # Use full concurrency - delays will manage rate limiting
            global_semaphore = asyncio.Semaphore(total_concurrent_requests)

            logger.info(
                "Using delay-based rate limit protection with full concurrency",
                requested_concurrency=total_concurrent_requests,
                rate_limit_capacity=rate_limit_capacity,
                base_delay=delay_between_downloads,
                intelligent_delay=intelligent_delay,
                files=len(filtered_files),
            )

            # Create all download tasks with flattened concurrency and staggered delays
            async def download_file_with_flattened_concurrency(
                file_info, delay_seconds: float = 0.0
            ):
                file_group_id = file_info.get(
                    "file-group-id", file_info.get("file_group_id")
                )
                file_datetime = file_info.get(
                    "file-datetime", file_info.get("file_datetime")
                )

                if not file_group_id:
                    logger.error("File info missing file-group-id", file_info=file_info)
                    return None

                # Apply delay before starting download
                if delay_seconds > 0:
                    logger.debug(
                        "Applying delay before download",
                        file_group_id=file_group_id,
                        delay_seconds=delay_seconds,
                    )
                    await asyncio.sleep(delay_seconds)

                dest_path = dest_dir

                try:
                    # Use the client's parallel download method but with our global semaphore
                    # We need to modify the client call to use our flattened concurrency
                    result = await self._download_file_parallel_flattened(
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        dest_path=dest_path,
                        num_parts=num_parts,
                        global_semaphore=global_semaphore,
                        progress_callback=progress_callback,
                    )
                    logger.info(
                        "Downloaded file (flattened concurrency)",
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        status=result.status.value if result else "failed",
                    )
                    return result
                except Exception as e:
                    logger.error(
                        "Flattened parallel download failed",
                        file_group_id=file_group_id,
                        file_datetime=file_datetime,
                        error=str(e),
                    )
                    return None

            # Execute all downloads concurrently with intelligent delay-based rate limiting
            download_tasks = []
            for i, file_info in enumerate(filtered_files):
                # Calculate intelligent delay: combines base delay with rate limit protection
                delay_seconds = i * intelligent_delay
                task = download_file_with_flattened_concurrency(
                    file_info, delay_seconds
                )
                download_tasks.append(task)

            logger.info(
                "Starting downloads with intelligent delay-based rate limiting",
                total_files=len(filtered_files),
                base_delay=delay_between_downloads,
                intelligent_delay=intelligent_delay,
                total_delay_range=f"0-{(len(filtered_files) - 1) * intelligent_delay:.1f}s",
                rate_limit_protection="enabled",
            )

            download_results = await asyncio.gather(
                *download_tasks, return_exceptions=True
            )

            # Process results
            successful = []
            failed = []

            for file_info, result in zip(filtered_files, download_results):
                if isinstance(result, BaseException):
                    failed.append(file_info)
                elif (
                    result
                    and hasattr(result, "status")
                    and hasattr(result, "file_group_id")
                    and result.status.value == "completed"
                ):
                    successful.append(result)
                else:
                    failed.append(file_info)

            # Calculate total operation time
            operation_end_time = time.time()
            total_time_seconds = operation_end_time - operation_start_time
            total_time_minutes = total_time_seconds / 60.0

            # Generate rate limit recommendations
            recommendations = self._get_rate_limit_recommendations(
                total_concurrent_requests
            )

            # Calculate per-file timing statistics
            file_times = []
            total_download_time = 0.0
            for result in successful:
                if result.download_time:
                    file_times.append(
                        {
                            "file_group_id": result.file_group_id,
                            "download_time_seconds": round(result.download_time, 2),
                            "file_size_bytes": result.file_size or 0,
                            "speed_mbps": (
                                round(result.speed_mbps, 2)
                                if result.speed_mbps
                                else 0.0
                            ),
                        }
                    )
                    total_download_time += result.download_time

            # Calculate timing statistics
            avg_file_time = total_download_time / len(successful) if successful else 0.0
            min_file_time = (
                min([ft["download_time_seconds"] for ft in file_times])
                if file_times
                else 0.0
            )
            max_file_time = (
                max([ft["download_time_seconds"] for ft in file_times])
                if file_times
                else 0.0
            )

            report = {
                "group_id": group_id,
                "start_date": start_date,
                "end_date": end_date,
                "total_files": len(filtered_files),
                "successful_downloads": len(successful),
                "failed_downloads": len(failed),
                "success_rate": (
                    (len(successful) / len(filtered_files)) * 100
                    if filtered_files
                    else 0
                ),
                "downloaded_files": [r.file_group_id for r in successful],
                "failed_files": [
                    f.get("file-group-id", f.get("file_group_id", "unknown"))
                    for f in failed
                ],
                "num_parts": num_parts,
                "max_concurrent": max_concurrent,
                "total_concurrent_requests": total_concurrent_requests,
                "concurrency_model": "delay_based_rate_limit_protection",
                "rate_limit_protection": "enabled",
                "base_delay": delay_between_downloads,
                "intelligent_delay": intelligent_delay,
                "delay_range": f"0-{(len(filtered_files) - 1) * intelligent_delay:.1f}s",
                "rate_limit_capacity": rate_limit_capacity,
                "rate_limit_recommendations": recommendations,
                "total_time_seconds": round(total_time_seconds, 2),
                "total_time_minutes": round(total_time_minutes, 2),
                "total_time_formatted": (
                    f"{int(total_time_minutes)}m {int(total_time_seconds % 60)}s"
                    if total_time_minutes >= 1
                    else f"{total_time_seconds:.1f}s"
                ),
                "per_file_timing": {
                    "file_times": file_times,
                    "total_download_time_seconds": round(total_download_time, 2),
                    "average_file_time_seconds": round(avg_file_time, 2),
                    "min_file_time_seconds": round(min_file_time, 2),
                    "max_file_time_seconds": round(max_file_time, 2),
                    "total_download_time_formatted": (
                        f"{int(total_download_time // 60)}m {int(total_download_time % 60)}s"
                        if total_download_time >= 60
                        else f"{total_download_time:.1f}s"
                    ),
                },
            }
            logger.info(
                "Group parallel download for date range operation completed!", **report
            )
            return report
        except Exception as e:
            logger.error(
                "Group parallel download for date range operation failed",
                group_id=group_id,
                start_date=start_date,
                end_date=end_date,
                error=str(e),
            )
            raise

    async def _download_file_parallel_flattened(
        self,
        file_group_id: str,
        file_datetime: Optional[str],
        dest_path: Path,
        num_parts: int,
        global_semaphore: asyncio.Semaphore,
        progress_callback: Optional[Callable] = None,
    ) -> Optional[DownloadResult]:
        """
        Download a file using parallel parts with flattened concurrency control.

        This method implements the core flattened concurrency logic where each
        HTTP range request competes for the global semaphore rather than being
        grouped by file.

        Args:
            file_group_id: File group ID to download
            file_datetime: Optional file datetime
            dest_path: Destination path (directory)
            num_parts: Number of parts to download in parallel
            global_semaphore: Global semaphore controlling total HTTP concurrency
            progress_callback: Optional progress callback

        Returns:
            DownloadResult if successful, None if failed
        """
        import time
        from datetime import datetime

        from .client import get_filename_from_response, validate_file_datetime
        from .models import (
            DownloadOptions,
            DownloadProgress,
            DownloadResult,
            DownloadStatus,
        )

        try:
            assert self._client is not None

            if file_datetime:
                validate_file_datetime(file_datetime)

            if num_parts is None or num_parts <= 0:
                num_parts = 5

            # Build params for API call
            params = {"file-group-id": file_group_id}
            if file_datetime:
                params["file-datetime"] = file_datetime

            # Determine destination directory
            download_options = DownloadOptions(destination_path=dest_path)
            if download_options.destination_path:
                dest_path = Path(download_options.destination_path)
                if dest_path.suffix:
                    destination_dir = dest_path.parent
                else:
                    destination_dir = dest_path
            else:
                destination_dir = Path(self._client.config.download_dir)

            if download_options.create_directories:
                destination_dir.mkdir(parents=True, exist_ok=True)

            start_time = time.time()
            bytes_downloaded = 0
            destination: Optional[Path] = None
            temp_destination: Optional[Path] = None
            total_bytes: int = 0
            shared_fh = None

            # Step 1: Probe file size with a 1-byte range request
            url = self._client._build_files_api_url("group/file/download")
            probe_headers = {"Range": "bytes=0-0"}

            async with global_semaphore:  # Use global semaphore for probe request
                async with await self._client._enter_request_cm(
                    "GET", url, params=params, headers=probe_headers
                ) as probe_resp:
                    await self._client._handle_response(probe_resp)
                    content_range = probe_resp.headers.get(
                        "content-range"
                    ) or probe_resp.headers.get("Content-Range")
                    if content_range and "/" in content_range:
                        try:
                            total_bytes = int(content_range.split("/")[-1])
                        except Exception:
                            total_bytes = int(
                                probe_resp.headers.get("content-length", "0")
                            )
                    else:
                        # Fallback to single-stream download if range not supported
                        return await self._client.download_file_async(
                            file_group_id=file_group_id,
                            file_datetime=file_datetime,
                            options=download_options,
                            progress_callback=progress_callback,
                        )

                    # If file is small (<10MB), prefer a single-stream download
                    ten_mb = 10 * 1024 * 1024
                    if total_bytes and total_bytes < ten_mb:
                        return await self._client.download_file_async(
                            file_group_id=file_group_id,
                            file_datetime=file_datetime,
                            options=download_options,
                            progress_callback=progress_callback,
                        )

                    # Determine filename from headers
                    filename = get_filename_from_response(
                        probe_resp, file_group_id, file_datetime
                    )
                    destination = destination_dir / filename

                    if destination.exists() and not download_options.overwrite_existing:
                        raise FileExistsError(f"File already exists: {destination}")

            # Step 2: Prepare temp file with full size for random access writes
            temp_destination = destination.with_suffix(destination.suffix + ".part")
            with open(temp_destination, "wb", buffering=1024*1024) as f:  # 1MB buffer for network drives
                f.truncate(total_bytes)

            # Step 3: Compute ranges for parallel download
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
            # Open shared file handle with buffering for network drives
            shared_fh = open(temp_destination, "r+b", buffering=1024*1024)  # 1MB buffer

            # Progress callback optimization: track last callback state
            last_callback_bytes = 0
            last_callback_time = time.time()
            callback_threshold_bytes = 1024 * 1024  # 1MB
            callback_threshold_time = 0.5  # 0.5 seconds

            # Get the current event loop for async file I/O
            loop = asyncio.get_running_loop()

            # Step 4: Download each range with global semaphore control
            async def download_range_with_global_semaphore(
                start_byte: int, end_byte: int
            ):
                nonlocal bytes_downloaded, last_callback_bytes, last_callback_time
                headers = {"Range": f"bytes={start_byte}-{end_byte}"}

                assert self._client is not None  # Already checked above
                # Each range request uses the global semaphore
                async with global_semaphore:
                    async with await self._client._enter_request_cm(
                        "GET", url, params=params, headers=headers
                    ) as resp:
                        await self._client._handle_response(resp)
                        # Stream and write to correct offset with batching to reduce lock contention
                        current_pos = start_byte
                        chunk_size = download_options.chunk_size or 65536  # 64KB default for network drives

                        # Write batching: accumulate chunks before writing to reduce lock contention
                        write_buffer = bytearray()
                        buffer_start_pos = current_pos
                        batch_size_threshold = 1024 * 1024  # 1MB batch size

                        async def flush_buffer():
                            """Flush accumulated buffer to file"""
                            nonlocal bytes_downloaded, last_callback_bytes, last_callback_time
                            if not write_buffer:
                                return

                            async with file_lock:
                                await loop.run_in_executor(
                                    None, shared_fh.seek, buffer_start_pos
                                )
                                await loop.run_in_executor(None, shared_fh.write, bytes(write_buffer))

                            buffer_bytes = len(write_buffer)
                            async with bytes_lock:
                                bytes_downloaded += buffer_bytes
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
                                    elif download_options.show_progress:
                                        logger.info(
                                            "Download progress (flattened)",
                                            file=file_group_id,
                                            percentage=f"{progress.percentage:.1f}%",
                                            downloaded=format_file_size(
                                                bytes_downloaded
                                            ),
                                        )
                                    last_callback_bytes = bytes_downloaded
                                    last_callback_time = current_time

                        async for chunk in resp.content.iter_chunked(chunk_size):
                            write_buffer.extend(chunk)
                            current_pos += len(chunk)

                            # Flush buffer when it reaches threshold
                            if len(write_buffer) >= batch_size_threshold:
                                await flush_buffer()
                                write_buffer.clear()
                                buffer_start_pos = current_pos

                        # Flush any remaining data
                        await flush_buffer()

            # Step 5: Execute all range downloads concurrently (each will acquire global semaphore)
            await asyncio.gather(
                *(download_range_with_global_semaphore(s, e) for s, e in ranges)
            )

            # Step 6: Finalize file
            try:
                async with file_lock:
                    shared_fh.flush()
            finally:
                try:
                    shared_fh.close()
                except Exception:
                    pass

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
            # Cleanup on error
            try:
                if shared_fh:
                    shared_fh.close()
                if temp_destination and temp_destination.exists():
                    # Attempt salvage if file appears complete
                    if total_bytes and temp_destination.stat().st_size >= total_bytes:
                        if destination is None:
                            destination = temp_destination.with_suffix("")
                        temp_destination.replace(destination)
                        return DownloadResult(
                            file_group_id=file_group_id,
                            group_id="",
                            local_path=destination,
                            file_size=total_bytes,
                            download_time=time.time() - start_time,
                            bytes_downloaded=bytes_downloaded,
                            status=DownloadStatus.COMPLETED,
                            error_message=None,
                        )
            except Exception:
                pass

            logger.error(
                "Flattened parallel download failed for file",
                file_group_id=file_group_id,
                error=str(e),
            )
            return None

    def _calculate_rate_limit_capacity(self) -> Dict[str, Any]:
        """
        Calculate the rate limit capacity without reducing concurrency.

        Returns:
            Dictionary with rate limit capacity information
        """
        if not self._client:
            return {
                "requests_per_minute": 1000,
                "burst_capacity": 100,
                "safe_interval": 0.1,
            }

        try:
            rate_config = self._client.rate_limiter.config

            if not rate_config.enable_rate_limiting:
                return {
                    "requests_per_minute": 1000,
                    "burst_capacity": 100,
                    "safe_interval": 0.1,
                }

            requests_per_minute = rate_config.requests_per_minute
            burst_capacity = rate_config.burst_capacity

            # Calculate safe interval between requests to stay within rate limits
            # This ensures we don't exceed the per-minute limit
            safe_interval = 60.0 / requests_per_minute  # seconds between requests

            return {
                "requests_per_minute": requests_per_minute,
                "burst_capacity": burst_capacity,
                "safe_interval": safe_interval,
                "queuing_enabled": rate_config.enable_queuing,
            }

        except Exception as e:
            logger.warning(
                "Failed to calculate rate limit capacity, using conservative defaults",
                error=str(e),
            )
            return {
                "requests_per_minute": 100,
                "burst_capacity": 10,
                "safe_interval": 0.6,
            }

    def _calculate_intelligent_delay(
        self,
        total_concurrent_requests: int,
        rate_limit_capacity: Dict[str, Any],
        base_delay: float,
    ) -> float:
        """
        Calculate intelligent delay that ensures rate limit compliance while maintaining concurrency.

        Burst capacity is the absolute number of requests that can be made immediately,
        not per minute or per second. It's a one-time allowance before throttling begins.

        Args:
            total_concurrent_requests: Total number of concurrent requests
            rate_limit_capacity: Rate limit capacity information
            base_delay: Base delay requested by user

        Returns:
            Intelligent delay that ensures rate limit compliance
        """
        try:
            requests_per_minute = rate_limit_capacity["requests_per_minute"]
            burst_capacity = rate_limit_capacity[
                "burst_capacity"
            ]  # Absolute number of immediate requests
            safe_interval = rate_limit_capacity["safe_interval"]

            # Calculate minimum delay needed to stay within rate limits
            # We need to ensure that even with full concurrency, we don't exceed RPM
            min_delay_for_rpm = safe_interval

            # Calculate delay needed for burst capacity
            # Burst capacity is the absolute number of requests that can be made immediately
            # If we have more concurrent requests than burst capacity, we need to spread them over time
            if total_concurrent_requests > burst_capacity:
                # Calculate delay to spread the excess requests over time
                # The excess requests need to be spread out to avoid exceeding burst capacity
                excess_requests = total_concurrent_requests - burst_capacity

                # Calculate how much time we need to spread the excess requests
                # We want to spread them over a reasonable time window based on the excess
                # More excess requests = longer time window to spread them out
                time_window = max(
                    5.0, excess_requests * 0.1
                )  # At least 5s, or 0.1s per excess request
                min_delay_for_burst = (
                    time_window / excess_requests if excess_requests > 0 else 0.0
                )

                logger.debug(
                    "Burst capacity exceeded, calculating spread delay",
                    total_concurrent_requests=total_concurrent_requests,
                    burst_capacity=burst_capacity,
                    excess_requests=excess_requests,
                    time_window=time_window,
                    min_delay_for_burst=min_delay_for_burst,
                )
            else:
                min_delay_for_burst = 0.0

            # Use the maximum of user's base delay and calculated minimum delays
            intelligent_delay = max(base_delay, min_delay_for_rpm, min_delay_for_burst)

            # Log the calculation details for debugging
            logger.debug(
                "Intelligent delay calculation details",
                base_delay=base_delay,
                min_delay_for_rpm=min_delay_for_rpm,
                min_delay_for_burst=min_delay_for_burst,
                final_intelligent_delay=intelligent_delay,
            )

            # Add some safety margin (10%)
            intelligent_delay *= 1.1

            logger.info(
                "Calculated intelligent delay for rate limit protection",
                total_concurrent_requests=total_concurrent_requests,
                requests_per_minute=requests_per_minute,
                burst_capacity=burst_capacity,
                safe_interval=safe_interval,
                base_delay=base_delay,
                intelligent_delay=intelligent_delay,
                burst_capacity_type="absolute_requests",
                rate_limit_protection="enabled",
            )

            return intelligent_delay

        except Exception as e:
            logger.warning(
                "Failed to calculate intelligent delay, using base delay", error=str(e)
            )
            return base_delay

    def _calculate_safe_concurrency_limit(self, requested_concurrency: int) -> int:
        """
        Calculate a safe concurrency limit that respects rate limiting constraints.

        This method ensures that the total concurrent HTTP requests don't overwhelm
        the rate limiter, which could cause request queuing or failures.

        Args:
            requested_concurrency: The desired number of concurrent requests

        Returns:
            Safe concurrency limit that respects rate limiting
        """
        if not self._client:
            return requested_concurrency

        try:
            # Get rate limiter configuration
            rate_config = self._client.rate_limiter.config

            # If rate limiting is disabled, use requested concurrency
            if not rate_config.enable_rate_limiting:
                logger.info(
                    "Rate limiting disabled, using requested concurrency",
                    requested=requested_concurrency,
                )
                return requested_concurrency

            # Calculate safe limits based on rate limiter configuration
            requests_per_minute = rate_config.requests_per_minute
            burst_capacity = rate_config.burst_capacity
            queue_size = rate_config.max_queue_size if rate_config.enable_queuing else 0

            # Conservative calculation:
            # - Don't exceed burst capacity for immediate requests
            # - Consider queue capacity for sustained load
            # - Leave some headroom for other operations

            immediate_capacity = burst_capacity
            sustained_capacity = min(
                requests_per_minute
                // 4,  # Quarter of per-minute limit for sustained load
                (
                    queue_size // 2 if queue_size > 0 else immediate_capacity
                ),  # Half queue size
            )

            # Use the more conservative of immediate or sustained capacity
            safe_limit = max(
                1, min(immediate_capacity, sustained_capacity, requested_concurrency)
            )

            # Add warning if we're significantly reducing concurrency
            if safe_limit < requested_concurrency * 0.5:
                logger.warning(
                    "Significantly reducing concurrency due to rate limits",
                    requested=requested_concurrency,
                    safe_limit=safe_limit,
                    rate_limit_rpm=requests_per_minute,
                    burst_capacity=burst_capacity,
                    recommendation="Consider reducing max_concurrent or num_parts, or increasing rate limits",
                )

            logger.info(
                "Calculated safe concurrency limit",
                requested=requested_concurrency,
                safe_limit=safe_limit,
                rate_limit_rpm=requests_per_minute,
                burst_capacity=burst_capacity,
                queuing_enabled=rate_config.enable_queuing,
            )

            return safe_limit

        except Exception as e:
            logger.warning(
                "Failed to calculate safe concurrency limit, using conservative default",
                error=str(e),
                requested=requested_concurrency,
            )
            # Conservative fallback: use a small fraction of requested
            return max(1, min(10, requested_concurrency // 4))

    def _get_rate_limit_recommendations(
        self, requested_concurrency: int
    ) -> Dict[str, Any]:
        """
        Get recommendations for optimizing rate limit settings for the requested concurrency.

        Args:
            requested_concurrency: Desired concurrent requests

        Returns:
            Dictionary with recommendations
        """
        if not self._client:
            return {}

        try:
            rate_config = self._client.rate_limiter.config

            recommendations: Dict[str, Any] = {
                "current_settings": {
                    "requests_per_minute": rate_config.requests_per_minute,
                    "burst_capacity": rate_config.burst_capacity,
                    "queuing_enabled": rate_config.enable_queuing,
                    "max_queue_size": rate_config.max_queue_size,
                },
                "requested_concurrency": requested_concurrency,
                "recommendations": [],
            }

            # Recommend burst capacity increase if needed
            if requested_concurrency > rate_config.burst_capacity:
                recommendations["recommendations"].append(
                    {
                        "type": "increase_burst_capacity",
                        "current": rate_config.burst_capacity,
                        "recommended": max(
                            requested_concurrency, rate_config.burst_capacity * 2
                        ),
                        "reason": "Burst capacity should accommodate concurrent requests",
                    }
                )

            # Recommend requests per minute increase for sustained load
            sustained_rpm_needed = (
                requested_concurrency * 15
            )  # Assume 15 requests per minute per concurrent slot
            if sustained_rpm_needed > rate_config.requests_per_minute:
                recommendations["recommendations"].append(
                    {
                        "type": "increase_requests_per_minute",
                        "current": rate_config.requests_per_minute,
                        "recommended": sustained_rpm_needed,
                        "reason": "Higher RPM needed for sustained concurrent load",
                    }
                )

            # Recommend enabling queuing if not enabled
            if (
                not rate_config.enable_queuing
                and requested_concurrency > rate_config.burst_capacity
            ):
                recommendations["recommendations"].append(
                    {
                        "type": "enable_queuing",
                        "current": False,
                        "recommended": True,
                        "reason": "Queuing helps manage burst requests beyond capacity",
                    }
                )

            return recommendations

        except Exception as e:
            logger.warning(
                "Failed to generate rate limit recommendations", error=str(e)
            )
            return {}

    # Utility Methods

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics including active, idle, and total connections."""
        if self._client:
            return self._client.get_pool_stats()
        return {"error": "Client not connected"}

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive client statistics."""
        if self._client:
            stats = self._client.get_stats()
            # Add context path information
            stats["api_config"] = {
                "base_url": self.client_config.base_url,
                "context_path": self.client_config.context_path,
                "api_base_url": self.client_config.api_base_url,
            }
            return stats
        return {"status": "not_connected"}

    def create_progress_callback(self, log_interval: int = 10) -> Callable:
        """Create a progress callback function."""
        tracker = ProgressTracker(log_interval)
        return tracker.create_progress_callback()

    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        Get current rate limit configuration and status.

        Returns:
            Dictionary with rate limit information
        """
        if not self._client:
            return {"error": "Client not connected"}

        try:
            rate_config = self._client.rate_limiter.config
            rate_state = self._client.rate_limiter.state

            return {
                "configuration": {
                    "requests_per_minute": rate_config.requests_per_minute,
                    "burst_capacity": rate_config.burst_capacity,
                    "enable_rate_limiting": rate_config.enable_rate_limiting,
                    "enable_queuing": rate_config.enable_queuing,
                    "max_queue_size": rate_config.max_queue_size,
                    "adaptive_rate_limiting": rate_config.adaptive_rate_limiting,
                },
                "current_state": {
                    "available_tokens": rate_state.tokens,
                    "queue_size": len(rate_state.queue),
                    "total_requests": rate_state.total_requests,
                    "successful_requests": rate_state.successful_requests,
                    "failed_requests": rate_state.failed_requests,
                    "rate_limited_requests": rate_state.rate_limited_requests,
                },
            }
        except Exception as e:
            return {"error": f"Failed to get rate limit info: {e}"}

    def optimize_concurrency_for_rate_limits(
        self, max_concurrent: int, num_parts: int
    ) -> Dict[str, Any]:
        """
        Get optimized concurrency settings that respect rate limits.

        Args:
            max_concurrent: Desired maximum concurrent files
            num_parts: Desired number of parts per file

        Returns:
            Dictionary with optimized settings and recommendations
        """
        requested_concurrency = max_concurrent * num_parts
        safe_concurrency = self._calculate_safe_concurrency_limit(requested_concurrency)
        recommendations = self._get_rate_limit_recommendations(requested_concurrency)

        # Calculate optimal max_concurrent and num_parts that fit within safe limits
        if safe_concurrency < requested_concurrency:
            # Try to maintain the ratio but reduce total
            ratio = safe_concurrency / requested_concurrency
            optimal_max_concurrent = max(1, int(max_concurrent * ratio))
            optimal_num_parts = max(1, int(num_parts * ratio))

            # Ensure we don't exceed safe limits
            while optimal_max_concurrent * optimal_num_parts > safe_concurrency:
                if optimal_num_parts > optimal_max_concurrent:
                    optimal_num_parts -= 1
                else:
                    optimal_max_concurrent -= 1
        else:
            optimal_max_concurrent = max_concurrent
            optimal_num_parts = num_parts

        return {
            "requested": {
                "max_concurrent": max_concurrent,
                "num_parts": num_parts,
                "total_concurrency": requested_concurrency,
            },
            "optimized": {
                "max_concurrent": optimal_max_concurrent,
                "num_parts": optimal_num_parts,
                "total_concurrency": optimal_max_concurrent * optimal_num_parts,
            },
            "safe_limit": safe_concurrency,
            "rate_limit_applied": safe_concurrency < requested_concurrency,
            "recommendations": recommendations,
        }

    # Synchronous Wrapper Methods

    def connect(self):
        """Connect to the API."""
        return self._run_sync(self.connect_async())

    def close(self):
        """Close the connection and cleanup resources."""
        if self._client:
            return self._run_sync(self.close_async())

    def list_groups(self, limit: Optional[int] = 100) -> List[Group]:
        """Synchronous wrapper for list_groups."""
        return self._run_sync(self.list_groups_async(limit))

    def search_groups(
        self, keywords: str, limit: Optional[int] = 100, offset: Optional[int] = None
    ) -> List[Group]:
        """Synchronous wrapper for search_groups."""
        return self._run_sync(self.search_groups_async(keywords, limit, offset))

    def list_files(
        self, group_id: str, file_group_id: Optional[str] = None
    ) -> List[FileInfo]:
        """Synchronous wrapper for list_files."""
        return self._run_sync(self.list_files_async(group_id, file_group_id))

    def check_availability(
        self, file_group_id: str, file_datetime: str
    ) -> AvailabilityInfo:
        """Synchronous wrapper for check_availability."""
        return self._run_sync(
            self.check_availability_async(file_group_id, file_datetime)
        )

    def download_file(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        options: Optional[DownloadOptions] = None,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """
        Synchronous wrapper for download_file.
        Note: Will raise an error if called from within an existing event loop.

        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            destination_path: Optional download destination directory. The filename will be extracted
                             from the Content-Disposition header in the response. If not provided,
                             uses the default download directory from configuration.
            options: Download options
            num_parts: Number of parallel parts for download (default: 5)
            progress_callback: Optional progress callback function

        Returns:
            DownloadResult with download information
        """
        return self._run_sync(
            self.download_file_async(
                file_group_id,
                file_datetime,
                destination_path,
                options,
                num_parts,
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
        """Synchronous wrapper for list_available_files."""
        return self._run_sync(
            self.list_available_files_async(
                group_id, file_group_id, start_date, end_date
            )
        )

    def health_check(self) -> bool:
        """Synchronous wrapper for health_check."""
        return self._run_sync(self.health_check_async())

    # Instrument Collection Endpoints - Synchronous wrappers
    def list_instruments(
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
        return self._run_sync(
            self.list_instruments_async(group_id, instrument_id, page)
        )

    def search_instruments(
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
        """
        Request the unique list of filter dimensions that are available for a given dataset.

        Args:
            group_id: Catalog data group identifier
            page: Optional page token for pagination

        Returns:
            FiltersResponse containing the available filters
        """
        return self._run_sync(self.get_group_filters_async(group_id, page))

    def get_group_attributes(
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
        return self._run_sync(self.get_grid_data_async(expr, grid_id, date))

    def run_groups(self, max_concurrent: int = 5) -> Dict[str, Any]:
        """Synchronous wrapper for run_groups_async."""
        return self._run_sync(self.run_groups_async(max_concurrent))

    def run_group_files(self, group_id: str, max_concurrent: int = 5) -> Dict[str, Any]:
        """Synchronous wrapper for run_group_files_async."""
        return self._run_sync(self.run_group_files_async(group_id, max_concurrent))

    def run_availability(
        self, file_group_id: str, file_datetime: str
    ) -> Dict[str, Any]:
        """Synchronous wrapper for run_availability_async."""
        return self._run_sync(self.run_availability_async(file_group_id, file_datetime))

    def run_download(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        max_concurrent: int = 1,
    ) -> Dict[str, Any]:
        """Synchronous wrapper for run_download_async."""
        return self._run_sync(
            self.run_download_async(
                file_group_id, file_datetime, destination_path, max_concurrent
            )
        )

    def run_group_download(
        self,
        group_id: str,
        start_date: str,
        end_date: str,
        destination_dir: Path = Path("./downloads"),
        max_concurrent: int = 5,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
        delay_between_downloads: float = 1.0,
    ) -> dict:
        """Synchronous wrapper for run_group_download_async."""
        return self._run_sync(
            self.run_group_download_async(
                group_id,
                start_date,
                end_date,
                destination_dir,
                max_concurrent,
                num_parts,
                progress_callback,
                delay_between_downloads,
            )
        )

    def cleanup(self):
        """Synchronous cleanup resources and ensure proper shutdown."""
        if self._client:
            self._run_sync(self.close_async())
            self._client = None

        # Force garbage collection to clean up any remaining references
        import gc

        gc.collect()

    # Sync wrapper methods with _sync suffix for testing compatibility

    def connect_sync(self):
        """Synchronous wrapper for connect with _sync suffix."""
        return asyncio.run(self.connect_async())

    def close_sync(self):
        """Synchronous wrapper for close with _sync suffix."""
        if self._client:
            return asyncio.run(self.close_async())

    def list_groups_sync(self, limit: Optional[int] = 100) -> List[Group]:
        """Synchronous wrapper for list_groups with _sync suffix."""
        return asyncio.run(self.list_groups_async(limit))

    def search_groups_sync(
        self, keywords: str, limit: Optional[int] = 100, offset: Optional[int] = None
    ) -> List[Group]:
        """Synchronous wrapper for search_groups with _sync suffix."""
        return asyncio.run(self.search_groups_async(keywords, limit, offset))

    def list_files_sync(
        self, group_id: str, file_group_id: Optional[str] = None
    ) -> List[FileInfo]:
        """Synchronous wrapper for list_files with _sync suffix."""
        return asyncio.run(self.list_files_async(group_id, file_group_id))

    def check_availability_sync(
        self, file_group_id: str, file_datetime: str
    ) -> AvailabilityInfo:
        """Synchronous wrapper for check_availability with _sync suffix."""
        return asyncio.run(self.check_availability_async(file_group_id, file_datetime))

    def download_file_sync(
        self,
        file_group_id: str,
        file_datetime: Optional[str] = None,
        destination_path: Optional[Path] = None,
        options: Optional[DownloadOptions] = None,
        num_parts: int = 5,
        progress_callback: Optional[Callable] = None,
    ) -> DownloadResult:
        """Synchronous wrapper for download_file with _sync suffix."""
        return asyncio.run(
            self.download_file_async(
                file_group_id,
                file_datetime,
                destination_path,
                options,
                num_parts,
                progress_callback,
            )
        )

    def list_available_files_sync(
        self,
        group_id: str,
        file_group_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for list_available_files with _sync suffix."""
        return asyncio.run(
            self.list_available_files_async(
                group_id, file_group_id, start_date, end_date
            )
        )

    def health_check_sync(self) -> bool:
        """Synchronous wrapper for health_check with _sync suffix."""
        return asyncio.run(self.health_check_async())

    def run_group_download_sync(
        self,
        group_id: str,
        start_date: str,
        end_date: str,
        destination_dir: Path = Path("./downloads"),
        max_concurrent: int = 5,
    ) -> dict:
        """Synchronous wrapper for run_group_download with _sync suffix."""
        return asyncio.run(
            self.run_group_download_async(
                group_id, start_date, end_date, destination_dir, max_concurrent
            )
        )

    # Auto-Download wrappers
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
        max_concurrent_downloads: Optional[int] = None,
    ):
        """Proxy to client's start_auto_download_async."""
        await self.connect_async()
        assert self._client is not None
        return await self._client.start_auto_download_async(
            group_id=group_id,
            destination_dir=destination_dir,
            interval_minutes=interval_minutes,
            file_filter=file_filter,
            progress_callback=progress_callback,
            error_callback=error_callback,
            max_retries=max_retries,
            check_current_date_only=check_current_date_only,
            max_concurrent_downloads=max_concurrent_downloads,
        )

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
        max_concurrent_downloads: Optional[int] = None,
    ):
        """Synchronous proxy to client's start_auto_download_async."""
        return self._run_sync(
            self.start_auto_download_async(
                group_id,
                destination_dir,
                interval_minutes,
                file_filter,
                progress_callback,
                error_callback,
                max_retries,
                check_current_date_only,
                max_concurrent_downloads,
            )
        )

    # DataFrame conversion proxies
    def to_dataframe(
        self,
        response_data,
        flatten_nested: bool = True,
        include_metadata: bool = False,
        date_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        custom_transformations: Optional[Dict[str, Callable]] = None,
    ):
        """Proxy to client's to_dataframe utility."""
        if self._client is None:
            # Use a temporary client for utilities if not connected yet
            self._client = DataQueryClient(self.client_config)
        return self._client.to_dataframe(
            response_data,
            flatten_nested=flatten_nested,
            include_metadata=include_metadata,
            date_columns=date_columns,
            numeric_columns=numeric_columns,
            custom_transformations=custom_transformations,
        )

    def groups_to_dataframe(self, groups, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.groups_to_dataframe(
            groups, include_metadata=include_metadata
        )

    def files_to_dataframe(self, files, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.files_to_dataframe(files, include_metadata=include_metadata)

    def instruments_to_dataframe(self, instruments, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.instruments_to_dataframe(
            instruments, include_metadata=include_metadata
        )

    def time_series_to_dataframe(self, time_series, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.time_series_to_dataframe(
            time_series, include_metadata=include_metadata
        )
