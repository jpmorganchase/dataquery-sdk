"""
Main DataQuery class for the DATAQUERY SDK.

This module provides the main DataQuery class that serves as the primary interface
for all API interactions, encapsulating the client and providing high-level operations.
"""

import asyncio
import os
import structlog
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable, Union

from .client import DataQueryClient
from .config import EnvConfig
from .exceptions import (
    ConfigurationError
)
from .models import (
    ClientConfig, Group, FileInfo, DownloadResult, DownloadStatus,
    AvailabilityResponse, DownloadOptions, InstrumentsResponse,
    TimeSeriesResponse, FiltersResponse, AttributesResponse, GridDataResponse
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
    base_download_dir = Path(os.getenv("DATAQUERY_DOWNLOAD_DIR", "./downloads"))

    return {
        "base": base_download_dir,
        "workflow": base_download_dir / os.getenv("DATAQUERY_WORKFLOW_DIR", "workflow"),
        "groups": base_download_dir / os.getenv("DATAQUERY_GROUPS_DIR", "groups"),
        "availability": base_download_dir / os.getenv("DATAQUERY_AVAILABILITY_DIR", "availability"),
        "default": base_download_dir / os.getenv("DATAQUERY_DEFAULT_DIR", "files")
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
            # Pass env_file positionally to match expected signature in tests
            config = EnvConfig.create_client_config(self.env_file)
            EnvConfig.validate_config(config)
            return config
        except Exception as e:
            logger.warning("Failed to load configuration from environment", error=str(e))
            return self._get_default_config()

    def _get_default_config(self) -> ClientConfig:
        """Get default configuration for examples."""
        return ClientConfig(
            base_url="https://api.dataquery.com",
            context_path=None,
            oauth_enabled=False,
            oauth_token_url=None,
            client_id=None,
            client_secret=None,
            aud=None,
            grant_type="client_credentials",
            bearer_token=None,
            token_refresh_threshold=300,
            timeout=30.0,
            max_retries=3,
            retry_delay=1.0,
            pool_connections=10,
            pool_maxsize=20,
            requests_per_minute=100,
            burst_capacity=20,
            proxy_enabled=False,
            proxy_url=None,
            proxy_username=None,
            proxy_password=None,
            proxy_verify_ssl=True,
            log_level="INFO",
            enable_debug_logging=False,
            download_dir="./downloads",
            create_directories=True,
            overwrite_existing=False
        )


class ProgressTracker:
    """Progress tracking for batch operations."""

    def __init__(self, log_interval: int = 10):
        self.log_interval = log_interval
        self.last_log_time = 0

    def create_progress_callback(self) -> Callable:
        """Create a progress callback function."""

        def progress_callback(progress: Any):
            current_time = time.time()
            if current_time - self.last_log_time >= self.log_interval:
                logger.info("Batch progress",
                            completed=getattr(progress, 'completed_files', 0),
                            total=getattr(progress, 'total_files', 0),
                            percentage=f"{getattr(progress, 'percentage', 0):.1f}%",
                            current_file=getattr(progress, 'current_file', 'unknown'))
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
            self.client_config.oauth_enabled = True
            if client_id:
                self.client_config.client_id = client_id
            if client_secret:
                self.client_config.client_secret = client_secret
            # Auto-derive token URL if missing
            if not self.client_config.oauth_token_url and self.client_config.base_url:
                try:
                    self.client_config.oauth_token_url = f"{self.client_config.base_url.rstrip('/')}/oauth/token"
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

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """
        Get the current event loop or create a new one if needed.
        
        Returns:
            Event loop to use for operations
        """
        try:
            # Try to get the current running loop
            loop = asyncio.get_running_loop()
            self._own_loop = False
            return loop
        except RuntimeError:
            # No running loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._own_loop = True
            return loop

    def _run_async(self, coro):
        """
        Run an async coroutine in the appropriate event loop.
        
        Args:
            coro: Coroutine to run
            
        Returns:
            Result of the coroutine
        """
        loop = self._get_or_create_loop()
        if self._own_loop:
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()
                self._own_loop = False
        else:
            # Running inside an existing event loop: offload via a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()

    # Core API Methods

    async def list_groups_async(self, limit: Optional[int] = None) -> List[Group]:
        """
        List all available data groups with pagination support.
        
        Args:
            limit: Optional limit on number of groups to return. If None, returns all groups.
        
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
            self,
            keywords: str,
            limit: Optional[int] = None,
            offset: Optional[int] = None
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
        await self.connect_async()
        assert self._client is not None
        return await self._client.search_groups_async(keywords, limit, offset)

    async def list_files_async(
            self,
            group_id: str,
            file_group_id: Optional[str] = None
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
            self,
            file_group_id: str,
            file_datetime: str
    ) -> AvailabilityResponse:
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
            progress_callback: Optional[Callable] = None
    ) -> DownloadResult:
        """
        Download a specific file.
        
        Args:
            file_group_id: File ID to download
            file_datetime: Optional datetime of the file (YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS)
            destination_path: Optional download destination directory. The filename will be extracted 
                             from the Content-Disposition header in the response. If not provided, 
                             uses the default download directory from configuration.
            options: Download options
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
                timeout=30.0,
                enable_range_requests=True,
                range_start=None,
                range_end=None,
                range_header=None,
                show_progress=True,
                progress_callback=None
            )

        assert self._client is not None
        return await self._client.download_file_async(
            file_group_id, file_datetime, options, progress_callback
        )

    async def list_available_files_async(
            self,
            group_id: str,
            file_group_id: Optional[str] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
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
            page: Optional[str] = None
    ) -> 'InstrumentsResponse':
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
            self,
            group_id: str,
            keywords: str,
            page: Optional[str] = None
    ) -> 'InstrumentsResponse':
        """
        Search within a dataset using keywords to create subsets of matching instruments.
        
        Args:
            group_id: Catalog data group identifier
            keywords: Keywords to narrow aud of results
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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
            instruments, attributes, data, format, start_date, end_date,
            calendar, frequency, conversion, nan_treatment, page
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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
            expressions, format, start_date, end_date, calendar, frequency,
            conversion, nan_treatment, data, page
        )

    # Group Collection Additional Endpoints
    async def get_group_filters_async(
            self,
            group_id: str,
            page: Optional[str] = None
    ) -> 'FiltersResponse':
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
            page: Optional[str] = None
    ) -> 'AttributesResponse':
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
        return await self._client.get_group_attributes_async(group_id, instrument_id, page)

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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
            group_id, attributes, filter, data, format, start_date, end_date,
            calendar, frequency, conversion, nan_treatment, page
        )

    # Grid Collection Endpoints
    async def get_grid_data_async(
            self,
            expr: Optional[str] = None,
            grid_id: Optional[str] = None,
            date: Optional[str] = None
    ) -> 'GridDataResponse':
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

    async def run_groups_async(self, max_concurrent: int = 3) -> Dict[str, Any]:
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
                "total_files": sum(g.associated_file_count or 0 for g in groups),
                "groups": [g.model_dump() for g in groups],
                "file_types": [],  # Groups don't have file_types attribute
                "providers": list(set(g.provider for g in groups if g.provider))
            }

            logger.info("Groups operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error("Groups operation failed", error=str(e))
            raise

    async def run_group_files_async(
            self,
            group_id: str,
            max_concurrent: int = 3
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
            report = {
                "group_id": group_id,
                "total_files": len(files),
                "file_types": list(set(f.file_type for f in files)),
                "date_range": None,  # FileInfo doesn't have date_range attribute
                "files": [f.model_dump() for f in files]
            }

            logger.info("Group files operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error("Group files operation failed", group_id=group_id, error=str(e))
            raise

    async def run_availability_async(
            self,
            file_group_id: str,
            file_datetime: str
    ) -> Dict[str, Any]:
        """Run operation for checking file availability."""
        logger.info("=== Starting Availability Operation ===",
                    file_group_id=file_group_id, file_datetime=file_datetime)

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
                "availability_rate": availability.availability_rate,
                "total_files": len(availability.availability),
                "available_files": len(availability.available_files),
                "unavailable_files": len(availability.unavailable_files),
                "available_dates": [info.file_date for info in availability.available_files],
                "unavailable_dates": [info.file_date for info in availability.unavailable_files]
            }

            logger.info("Availability operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error("Availability operation failed",
                         file_group_id=file_group_id, error=str(e))
            raise

    async def run_download_async(
            self,
            file_group_id: str,
            file_datetime: Optional[str] = None,
            destination_path: Optional[Path] = None,
            max_concurrent: int = 1
    ) -> Dict[str, Any]:
        """Run operation for downloading a single file."""
        logger.info("=== Starting Download Operation ===",
                    file_group_id=file_group_id, file_datetime=file_datetime)

        try:
            # Step 1: Download file
            logger.info("Step 1: Downloading File")
            download_options = DownloadOptions(
                destination_path=destination_path,
                create_directories=True,
                overwrite_existing=False,
                chunk_size=8192,
                max_retries=3,
                retry_delay=1.0,
                timeout=30.0,
                enable_range_requests=True,
                range_start=None,
                range_end=None,
                range_header=None,
                show_progress=True,
                progress_callback=None
            ) if destination_path else None
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
                "error_message": result.error_message
            }

            logger.info("Download operation completed successfully!")
            logger.info("Summary report", **report)

            return report

        except Exception as e:
            logger.error("Download operation failed",
                         file_group_id=file_group_id, error=str(e))
            raise

    async def run_group_download_async(
            self,
            group_id: str,
            start_date: str,
            end_date: str,
            destination_dir: Path = Path("./downloads"),
            max_concurrent: int = 3,
            progress_callback: Optional[Callable] = None
    ) -> dict:
        """
        Download all files in a group for a date range.
        
        Args:
            group_id: Group ID to download files from
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            destination_dir: Destination directory for downloads
            max_concurrent: Maximum concurrent downloads
            progress_callback: Optional progress callback function for individual downloads
            
        Returns:
            Dictionary with download results and statistics
        """
        logger.info("=== Starting Group Download for Date Range Operation ===",
                    group_id=group_id, start_date=start_date, end_date=end_date)

        try:
            # Step 1: Get available files for the date range
            logger.info("Step 1: Getting Available Files for Date Range")
            available_files = await self.list_available_files_async(
                group_id=group_id,
                start_date=start_date,
                end_date=end_date
            )

            if not available_files:
                logger.warning("No available files found for date range",
                               group_id=group_id, start_date=start_date, end_date=end_date)
                return {"error": "No available files found for date range"}

            logger.info("Found available files", count=len(available_files))

            # Step 2: Download all available files
            logger.info("Step 2: Downloading Available Files")
            results = []
            semaphore = asyncio.Semaphore(max_concurrent)
            dest_dir = destination_dir / group_id
            dest_dir.mkdir(parents=True, exist_ok=True)

            async def download_available_file(file_info):
                async with semaphore:
                    file_group_id = file_info.get('file-group-id', file_info.get('file_group_id'))
                    file_datetime = file_info.get('file-datetime', file_info.get('file_datetime'))

                    if not file_group_id:
                        logger.error("File info missing file-group-id", file_info=file_info)
                        return None

                    # Generate filename based on available info
                    if file_datetime:
                        filename = f"{file_group_id}_{file_datetime}"
                    else:
                        filename = file_group_id

                    # Try to get file extension from file info or use default
                    file_extension = file_info.get('extension', '.bin')
                    if not file_extension.startswith('.'):
                        file_extension = f".{file_extension}"

                    dest_path = dest_dir / f"{filename}{file_extension}"

                    try:
                        result = await self.download_file_async(
                            file_group_id,
                            file_datetime=file_datetime,
                            destination_path=dest_path,
                            progress_callback=progress_callback
                        )
                        logger.info("Downloaded file", file_group_id=file_group_id,
                                    file_datetime=file_datetime, status=result.status.value)
                        return result
                    except Exception as e:
                        logger.error("Download failed", file_group_id=file_group_id,
                                     file_datetime=file_datetime, error=str(e))
                        return None

            download_tasks = [download_available_file(f) for f in available_files]
            download_results = await asyncio.gather(*download_tasks)
            successful = [r for r in download_results if r and r.status.value == "completed"]
            failed = [f for f, r in zip(available_files, download_results) if not r or r.status.value != "completed"]

            report = {
                "group_id": group_id,
                "start_date": start_date,
                "end_date": end_date,
                "total_files": len(available_files),
                "successful_downloads": len(successful),
                "failed_downloads": len(failed),
                "success_rate": (len(successful) / len(available_files)) * 100 if available_files else 0,
                "downloaded_files": [r.file_group_id for r in successful],
                "failed_files": [f.get('file-group-id', f.get('file_group_id', 'unknown')) for f in failed],
            }
            logger.info("Group download for date range operation completed!", **report)
            return report
        except Exception as e:
            logger.error("Group download for date range operation failed",
                         group_id=group_id, start_date=start_date, end_date=end_date, error=str(e))
            raise

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
                "api_base_url": self.client_config.api_base_url
            }
            return stats
        return {"status": "not_connected"}

    def create_progress_callback(self, log_interval: int = 10) -> Callable:
        """Create a progress callback function."""
        tracker = ProgressTracker(log_interval)
        return tracker.create_progress_callback()

    # Synchronous Wrapper Methods

    def connect(self):
        """Connect to the API."""
        return self._run_async(self.connect_async())

    def close(self):
        """Close the connection and cleanup resources."""
        if self._client:
            return self._run_async(self.close_async())

    def list_groups(self, limit: Optional[int] = None) -> List[Group]:
        """Synchronous wrapper for list_groups."""
        return self._run_async(self.list_groups_async(limit))

    def search_groups(
            self,
            keywords: str,
            limit: Optional[int] = None,
            offset: Optional[int] = None
    ) -> List[Group]:
        """Synchronous wrapper for search_groups."""
        return self._run_async(self.search_groups_async(keywords, limit, offset))

    def list_files(
            self,
            group_id: str,
            file_group_id: Optional[str] = None
    ) -> List[FileInfo]:
        """Synchronous wrapper for list_files."""
        return self._run_async(self.list_files_async(group_id, file_group_id))

    def check_availability(
            self,
            file_group_id: str,
            file_datetime: str
    ) -> AvailabilityResponse:
        """Synchronous wrapper for check_availability."""
        return self._run_async(self.check_availability_async(file_group_id, file_datetime))

    def download_file(
            self,
            file_group_id: str,
            file_datetime: Optional[str] = None,
            destination_path: Optional[Path] = None,
            options: Optional[DownloadOptions] = None,
            progress_callback: Optional[Callable] = None
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
            progress_callback: Optional progress callback function
            
        Returns:
            DownloadResult with download information
        """
        return self._run_async(self.download_file_async(
            file_group_id, file_datetime, destination_path, options, progress_callback
        ))

    def list_available_files(
            self,
            group_id: str,
            file_group_id: Optional[str] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for list_available_files."""
        return self._run_async(self.list_available_files_async(
            group_id, file_group_id, start_date, end_date
        ))

    def health_check(self) -> bool:
        """Synchronous wrapper for health_check."""
        return self._run_async(self.health_check_async())

    # Instrument Collection Endpoints - Synchronous wrappers
    def list_instruments(
            self,
            group_id: str,
            instrument_id: Optional[str] = None,
            page: Optional[str] = None
    ) -> 'InstrumentsResponse':
        """
        Request the complete list of instruments and identifiers for a given dataset.
        
        Args:
            group_id: Catalog data group identifier
            instrument_id: Optional instrument identifier to filter results
            page: Optional page token for pagination
            
        Returns:
            InstrumentsResponse containing the list of instruments
        """
        return self._run_async(self.list_instruments_async(group_id, instrument_id, page))

    def search_instruments(
            self,
            group_id: str,
            keywords: str,
            page: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search within a dataset using keywords to create subsets of matching instruments.
        
        Args:
            group_id: Catalog data group identifier
            keywords: Keywords to narrow aud of results
            page: Optional page token for pagination
            
        Returns:
            InstrumentsResponse containing the matching instruments
        """
        return self._run_async(self.search_instruments_async(group_id, keywords, page))

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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
        return self._run_async(self.get_instrument_time_series_async(
            instruments, attributes, data, format, start_date, end_date,
            calendar, frequency, conversion, nan_treatment, page
        ))

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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
        return self._run_async(self.get_expressions_time_series_async(
            expressions, format, start_date, end_date, calendar, frequency,
            conversion, nan_treatment, data, page
        ))

    # Group Collection Additional Endpoints - Synchronous wrappers
    def get_group_filters(
            self,
            group_id: str,
            page: Optional[str] = None
    ) -> 'FiltersResponse':
        """
        Request the unique list of filter dimensions that are available for a given dataset.
        
        Args:
            group_id: Catalog data group identifier
            page: Optional page token for pagination
            
        Returns:
            FiltersResponse containing the available filters
        """
        return self._run_async(self.get_group_filters_async(group_id, page))

    def get_group_attributes(
            self,
            group_id: str,
            instrument_id: Optional[str] = None,
            page: Optional[str] = None
    ) -> 'AttributesResponse':
        """
        Request the unique list of analytic attributes for each instrument of a given dataset.
        
        Args:
            group_id: Catalog data group identifier
            instrument_id: Optional instrument identifier to filter results
            page: Optional page token for pagination
            
        Returns:
            AttributesResponse containing the attributes for each instrument
        """
        return self._run_async(self.get_group_attributes_async(group_id, instrument_id, page))

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
            page: Optional[str] = None
    ) -> 'TimeSeriesResponse':
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
        return self._run_async(self.get_group_time_series_async(
            group_id, attributes, filter, data, format, start_date, end_date,
            calendar, frequency, conversion, nan_treatment, page
        ))

    # Grid Collection Endpoints - Synchronous wrappers
    def get_grid_data(
            self,
            expr: Optional[str] = None,
            grid_id: Optional[str] = None,
            date: Optional[str] = None
    ) -> 'GridDataResponse':
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
        return self._run_async(self.get_grid_data_async(expr, grid_id, date))

    def run_groups(self, max_concurrent: int = 3) -> Dict[str, Any]:
        """Synchronous wrapper for run_groups_async."""
        return self._run_async(self.run_groups_async(max_concurrent))

    def run_group_files(
            self,
            group_id: str,
            max_concurrent: int = 3
    ) -> Dict[str, Any]:
        """Synchronous wrapper for run_group_files_async."""
        return self._run_async(self.run_group_files_async(group_id, max_concurrent))

    def run_availability(
            self,
            file_group_id: str,
            file_datetime: str
    ) -> Dict[str, Any]:
        """Synchronous wrapper for run_availability_async."""
        return self._run_async(self.run_availability_async(file_group_id, file_datetime))

    def run_download(
            self,
            file_group_id: str,
            file_datetime: Optional[str] = None,
            destination_path: Optional[Path] = None,
            max_concurrent: int = 1
    ) -> Dict[str, Any]:
        """Synchronous wrapper for run_download_async."""
        return self._run_async(self.run_download_async(
            file_group_id, file_datetime, destination_path, max_concurrent
        ))

    def run_group_download(
            self,
            group_id: str,
            start_date: str,
            end_date: str,
            destination_dir: Path = Path("./downloads"),
            max_concurrent: int = 3,
            progress_callback: Optional[Callable] = None
    ) -> dict:
        """
        Synchronous wrapper for run_group_download_async.
        
        Args:
            group_id: Group ID to download files from
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            destination_dir: Destination directory for downloads
            max_concurrent: Maximum concurrent downloads
            progress_callback: Optional progress callback function for individual downloads
            
        Returns:
            Dictionary with download results and statistics
        """
        return self._run_async(self.run_group_download_async(
            group_id, start_date, end_date, destination_dir, max_concurrent, progress_callback
        ))

    def cleanup(self):
        """Synchronous cleanup resources and ensure proper shutdown."""
        if self._client:
            self._run_async(self.close_async())
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

    def list_groups_sync(self, limit: Optional[int] = None) -> List[Group]:
        """Synchronous wrapper for list_groups with _sync suffix."""
        return asyncio.run(self.list_groups_async(limit))

    def search_groups_sync(
            self,
            keywords: str,
            limit: Optional[int] = None,
            offset: Optional[int] = None
    ) -> List[Group]:
        """Synchronous wrapper for search_groups with _sync suffix."""
        return asyncio.run(self.search_groups_async(keywords, limit, offset))

    def list_files_sync(
            self,
            group_id: str,
            file_group_id: Optional[str] = None
    ) -> List[FileInfo]:
        """Synchronous wrapper for list_files with _sync suffix."""
        return asyncio.run(self.list_files_async(group_id, file_group_id))

    def check_availability_sync(
            self,
            file_group_id: str,
            file_datetime: str
    ) -> AvailabilityResponse:
        """Synchronous wrapper for check_availability with _sync suffix."""
        return asyncio.run(self.check_availability_async(file_group_id, file_datetime))

    def download_file_sync(
            self,
            file_group_id: str,
            file_datetime: Optional[str] = None,
            destination_path: Optional[Path] = None,
            options: Optional[DownloadOptions] = None,
            progress_callback: Optional[Callable] = None
    ) -> DownloadResult:
        """Synchronous wrapper for download_file with _sync suffix."""
        return asyncio.run(
            self.download_file_async(file_group_id, file_datetime, destination_path, options, progress_callback))

    def list_available_files_sync(
            self,
            group_id: str,
            file_group_id: Optional[str] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for list_available_files with _sync suffix."""
        return asyncio.run(self.list_available_files_async(group_id, file_group_id, start_date, end_date))

    def health_check_sync(self) -> bool:
        """Synchronous wrapper for health_check with _sync suffix."""
        return asyncio.run(self.health_check_async())

    def run_group_download_sync(
            self,
            group_id: str,
            start_date: str,
            end_date: str,
            destination_dir: Path = Path("./downloads"),
            max_concurrent: int = 3
    ) -> dict:
        """Synchronous wrapper for run_group_download with _sync suffix."""
        return asyncio.run(
            self.run_group_download_async(group_id, start_date, end_date, destination_dir, max_concurrent))

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
    ):
        """Synchronous proxy to client's start_auto_download_async."""
        return self._run_async(
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
        return self._client.groups_to_dataframe(groups, include_metadata=include_metadata)

    def files_to_dataframe(self, files, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.files_to_dataframe(files, include_metadata=include_metadata)

    def instruments_to_dataframe(self, instruments, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.instruments_to_dataframe(instruments, include_metadata=include_metadata)

    def time_series_to_dataframe(self, time_series, include_metadata: bool = False):
        if self._client is None:
            self._client = DataQueryClient(self.client_config)
        return self._client.time_series_to_dataframe(time_series, include_metadata=include_metadata)

