"""
Data models for the DATAQUERY SDK based on the OpenAPI specification.
"""

from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DownloadStatus(str, Enum):
    """Status of a download operation."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TokenStatus(str, Enum):
    """Status of an OAuth token."""

    VALID = "valid"
    EXPIRED = "expired"
    REFRESHING = "refreshing"
    INVALID = "invalid"


class ClientConfig(BaseModel):
    """Configuration for the DATAQUERY client."""

    # API configuration
    base_url: str = Field(..., description="Base URL of the DATAQUERY API")
    context_path: Optional[str] = Field(
        default="", description="API context path (e.g., '/api/v1')"
    )
    api_version: str = Field(default="2.0.0", description="API version")
    # Optional separate host for file endpoints
    files_base_url: Optional[str] = Field(
        default=None, description="Optional separate base URL for file endpoints"
    )
    files_context_path: Optional[str] = Field(
        default="", description="Optional context path for the files host"
    )

    # OAuth configuration
    oauth_enabled: bool = Field(default=True, description="Enable OAuth authentication")
    oauth_token_url: Optional[str] = Field(
        default=None, description="OAuth token endpoint URL"
    )
    client_id: Optional[str] = Field(default=None, description="OAuth client ID")
    client_secret: Optional[str] = Field(
        default=None, description="OAuth client secret"
    )
    # scope removed
    aud: Optional[str] = Field(default="", description="OAuth audience (aud)")
    grant_type: str = Field(
        default="client_credentials", description="OAuth grant type"
    )

    # Bearer token configuration
    bearer_token: Optional[str] = Field(
        default=None, description="Bearer token for API access"
    )
    token_refresh_threshold: int = Field(
        default=300, description="Seconds before expiry to refresh token"
    )

    # HTTP configuration
    timeout: float = Field(
        default=600.0, description="Default request timeout in seconds"
    )
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(
        default=1.0, description="Delay between retries in seconds"
    )

    # Connection pooling
    pool_connections: int = Field(default=10, description="Number of connection pools")
    pool_maxsize: int = Field(default=20, description="Maximum connections per pool")

    # Rate limiting
    requests_per_minute: int = Field(
        default=100, description="Requests per minute limit"
    )
    burst_capacity: int = Field(
        default=20, description="Burst capacity for rate limiting"
    )

    # Proxy configuration
    proxy_enabled: bool = Field(default=False, description="Enable proxy support")
    proxy_url: Optional[str] = Field(
        default="",
        description="Proxy URL (e.g., http://proxy:8080, socks5://proxy:1080)",
    )
    proxy_username: Optional[str] = Field(
        default="", description="Proxy username for authentication"
    )
    proxy_password: Optional[str] = Field(
        default="", description="Proxy password for authentication"
    )
    proxy_verify_ssl: bool = Field(
        default=True, description="Verify SSL certificates for proxy connections"
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    enable_debug_logging: bool = Field(
        default=False, description="Enable debug logging"
    )

    # Download configuration
    download_dir: str = Field(
        default="./downloads", description="Base download directory"
    )
    create_directories: bool = Field(
        default=True, description="Create parent directories if they don't exist"
    )
    overwrite_existing: bool = Field(
        default=False, description="Overwrite existing files"
    )
    token_storage_dir: Optional[str] = Field(
        default=None,
        description="Optional directory to store OAuth tokens; defaults to '<download_dir>/.tokens'",
    )

    # Batch Download Configuration
    max_concurrent_downloads: int = Field(
        default=5, description="Maximum concurrent downloads"
    )
    batch_size: int = Field(default=10, description="Batch size for operations")
    retry_failed: bool = Field(default=True, description="Retry failed downloads")
    max_retry_attempts: int = Field(
        default=2, description="Maximum retry attempts for failed downloads"
    )
    create_date_folders: bool = Field(
        default=True, description="Create date-based folders"
    )
    preserve_path_structure: bool = Field(
        default=True, description="Preserve original path structure"
    )
    flatten_structure: bool = Field(
        default=False, description="Flatten directory structure"
    )
    show_batch_progress: bool = Field(default=True, description="Show batch progress")
    show_individual_progress: bool = Field(
        default=True, description="Show individual file progress"
    )
    continue_on_error: bool = Field(
        default=True, description="Continue processing on errors"
    )
    log_errors: bool = Field(default=True, description="Log errors")
    save_error_log: bool = Field(default=True, description="Save error log to file")
    use_async_downloads: bool = Field(default=True, description="Use async downloads")
    chunk_size: int = Field(default=8192, description="Download chunk size in bytes")

    # Download Options
    enable_range_requests: bool = Field(
        default=True, description="Enable HTTP range requests"
    )
    show_progress: bool = Field(default=True, description="Show download progress")

    # Workflow Configuration
    workflow_dir: str = Field(
        default="workflow", description="Workflow files subdirectory"
    )
    groups_dir: str = Field(default="groups", description="Groups files subdirectory")
    availability_dir: str = Field(
        default="availability", description="Availability files subdirectory"
    )
    default_dir: str = Field(default="files", description="Default files subdirectory")

    # Security
    mask_secrets: bool = Field(default=True, description="Mask secrets in logs")
    token_storage_enabled: bool = Field(
        default=False, description="Enable token storage"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v):
        # Allow invalid URLs for testing - validation happens in client
        if v and v.startswith(("http://", "https://")):
            return v.rstrip("/")
        return v  # Allow invalid URLs to be created

    @field_validator("context_path")
    @classmethod
    def validate_context_path(cls, v):
        if v is not None:
            # Ensure context path starts with / and doesn't end with /
            if not v.startswith("/"):
                v = "/" + v
            return v.rstrip("/")
        return v

    @field_validator("files_base_url")
    @classmethod
    def validate_files_base_url(cls, v):
        if v:
            if v.startswith(("http://", "https://")):
                return v.rstrip("/")
        return v

    @field_validator("files_context_path")
    @classmethod
    def validate_files_context_path(cls, v):
        if v is not None and v != "":
            if not v.startswith("/"):
                v = "/" + v
            return v.rstrip("/")
        return v

    @field_validator("proxy_url")
    @classmethod
    def validate_proxy_url(cls, v):
        # Allow None or empty string without validation
        if v is None or v == "":
            return v
        if v is not None:
            # Validate proxy URL format
            if not v.startswith(("http://", "https://", "socks4://", "socks5://")):
                raise ValueError(
                    "Proxy URL must start with http://, https://, socks4://, or socks5://"
                )
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @field_validator("oauth_token_url")
    @classmethod
    def validate_oauth_token_url(cls, v, info):
        if info.data.get("oauth_enabled", True) and not v:
            # Auto-generate token URL from base URL
            base_url = info.data.get("base_url", "")
            if base_url:
                return f"{base_url}/oauth/token"
        return v

    @property
    def has_oauth_credentials(self) -> bool:
        """Check if OAuth credentials are configured."""
        return (
            self.oauth_enabled
            and self.client_id is not None
            and self.client_secret is not None
        )

    @property
    def has_bearer_token(self) -> bool:
        """Check if bearer token is configured."""
        return self.bearer_token is not None and self.bearer_token.strip() != ""

    @property
    def has_proxy_credentials(self) -> bool:
        """Check if proxy credentials are configured."""
        return self.proxy_username is not None and self.proxy_password is not None

    @property
    def api_base_url(self) -> str:
        """Get the complete API base URL including context path."""
        if self.context_path:
            return f"{self.base_url}{self.context_path}"
        return self.base_url

    @property
    def files_api_base_url(self) -> Optional[str]:
        """Get the complete files API base URL if configured, including context path."""
        if not self.files_base_url:
            return None
        if self.files_context_path:
            return f"{self.files_base_url}{self.files_context_path}"
        return self.files_base_url


class OAuthToken(BaseModel):
    """OAuth token information."""

    access_token: str = Field(..., description="Access token")
    token_type: str = Field(default="Bearer", description="Token type")
    expires_in: Optional[int] = Field(
        default=None, description="Token expiry time in seconds"
    )
    # scope removed
    refresh_token: Optional[str] = Field(default=None, description="Refresh token")

    # Internal tracking
    issued_at: Optional[datetime] = Field(default=None, description="Token issue time")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    @property
    def expires_at(self) -> Optional[datetime]:
        """Get token expiry time."""
        if self.expires_in and self.issued_at:
            return self.issued_at + timedelta(seconds=self.expires_in)
        return None

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        if not self.expires_at:
            return False
        return datetime.now() >= self.expires_at

    def is_expiring_soon(self, threshold: int = 300) -> bool:
        """Check if token is expiring soon."""
        if not self.expires_at:
            return False
        remaining_time = (self.expires_at - datetime.now()).total_seconds()
        # If threshold is larger than the original token lifetime, not expiring soon
        if self.expires_in and threshold > self.expires_in:
            return False
        return remaining_time < threshold

    def to_authorization_header(self) -> str:
        """Get authorization header value."""
        return f"{self.token_type} {self.access_token}"

    @property
    def status(self) -> TokenStatus:
        """Get the current status, checking if expired."""
        if self.is_expired:
            return TokenStatus.EXPIRED
        return TokenStatus.VALID


class TokenRequest(BaseModel):
    """OAuth token request parameters."""

    grant_type: str = Field("client_credentials", description="OAuth grant type")
    client_id: Optional[str] = Field(None, description="OAuth client ID")
    client_secret: Optional[str] = Field(None, description="OAuth client secret")
    # scope removed
    aud: Optional[str] = Field(None, description="OAuth audience (aud)")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for request."""
        data = {
            "grant_type": self.grant_type,
        }
        if self.client_id:
            data["client_id"] = cast(str, self.client_id)
        if self.client_secret:
            data["client_secret"] = cast(str, self.client_secret)
        # scope removed
        if getattr(self, "aud", None):
            # Send audience as 'aud' per provider requirement
            data["aud"] = cast(str, self.aud)
        return data


class TokenResponse(BaseModel):
    """OAuth token response."""

    access_token: str = Field(..., description="Access token")
    token_type: Optional[str] = Field(default=None, description="Token type")
    expires_in: Optional[int] = Field(
        default=None, description="Token expiry time in seconds"
    )
    # scope removed
    refresh_token: Optional[str] = Field(default=None, description="Refresh token")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    def to_oauth_token(self) -> "OAuthToken":
        """Convert to OAuthToken."""
        return OAuthToken(
            access_token=self.access_token,
            token_type=(self.token_type or "Bearer"),
            expires_in=self.expires_in,
            # scope removed
            refresh_token=self.refresh_token,
            issued_at=datetime.now(),  # Set issued_at when converting
            # status is computed property
        )


class FileMetadata(BaseModel):
    """File metadata information."""

    frequency: Optional[str] = Field(None, description="Data frequency")
    regions: Optional[List[str]] = Field(
        None, description="List of regions covered by the data"
    )
    history_start_date: Optional[str] = Field(
        None, alias="history-start-date", description="History start date"
    )
    median_file_size_mb: Optional[str] = Field(
        None, alias="median-file-size-mb", description="Median file size in MB"
    )
    publication_time: Optional[str] = Field(
        None, alias="publication-time", description="Publication time"
    )
    data_lag: Optional[str] = Field(
        None, alias="data-lag", description="Data lag information"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name


class SchemaColumn(BaseModel):
    """Schema column information."""

    column_id: str = Field(..., alias="columnId", description="Column identifier")
    column_name: str = Field(..., alias="columnName", description="Column name")
    column_description: Optional[str] = Field(
        None, alias="columnDescription", description="Column description"
    )
    data_type: str = Field(..., alias="dataType", description="Data type")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name


class DateRange(BaseModel):
    """Date range information."""

    earliest: str = Field(..., description="Earliest date in YYYYMMDD format")
    latest: str = Field(..., description="Latest date in YYYYMMDD format")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    def get_earliest_date(self) -> Optional[date]:
        """Get earliest date as date object."""
        try:
            return datetime.strptime(self.earliest, "%Y%m%d").date()
        except ValueError:
            return None

    def get_latest_date(self) -> Optional[date]:
        """Get latest date as date object."""
        try:
            return datetime.strptime(self.latest, "%Y%m%d").date()
        except ValueError:
            return None


class Group(BaseModel):
    """Model representing a data group based on the new OpenAPI spec."""

    item: Optional[int] = Field(None, description="Item number")
    group_id: Optional[str] = Field(
        None, alias="group-id", description="Unique group identifier"
    )
    group_name: Optional[str] = Field(
        None, alias="group-name", description="Display name of the group"
    )

    description: Optional[str] = Field(None, description="Group description")
    provider: Optional[str] = Field(None, description="Data provider")
    premium: Optional[bool] = Field(None, description="Whether this is a premium group")
    population: Optional[Dict[str, Any]] = Field(
        None, description="Population information"
    )
    attributes: Optional[List[Dict[str, Any]]] = Field(
        None, description="Group attributes"
    )
    file_groups: Optional[int] = Field(
        None, alias="file-groups", description="Number of file groups"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    # associated_file_count removed; use file_groups instead


class Link(BaseModel):
    """Pagination link model."""

    self: Optional[str] = Field(None, description="Current page URL")
    next: Optional[str] = Field(None, description="Next page URL")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name


class GroupList(BaseModel):
    """Response model for listing groups with pagination support."""

    groups: List[Group] = Field(..., description="List of groups")
    links: Optional[List[Link]] = Field(None, description="Pagination links")

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    def get_next_link(self) -> Optional[str]:
        """Get the next page link URL."""
        if not self.links:
            return None
        for link in self.links:
            if link.next:
                return link.next
        return None

    def has_next_page(self) -> bool:
        """Check if there's a next page available."""
        return self.get_next_link() is not None


class FileInfo(BaseModel):
    """Model representing file information based on the DataQuery spec.

    Only `file_group_id` is supported as the identifier.
    """

    # Primary identifier
    file_group_id: Optional[str] = Field(
        None, alias="file-group-id", description="Unique file group identifier"
    )

    # Additional info
    description: Optional[str] = Field(None, description="File description")
    file_type: Optional[List[str]] = Field(
        None, alias="file-type", description="Type(s) of the file"
    )
    # legacy fields removed
    metadata: Optional[FileMetadata] = Field(None, description="File metadata")
    file_schema: Optional[List[SchemaColumn]] = Field(
        None, description="File schema", alias="schema"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("file_type", mode="before")
    @classmethod
    def normalize_file_type(cls, v):
        """Accept string or list; normalize to list[str]."""
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        if isinstance(v, list):
            return [str(x) for x in v if x is not None]
        # Fallback: coerce to string and wrap
        return [str(v)]

    # inputs must use 'file-group-id'

    def get_file_extension(self) -> str:
        """Get file extension based on file type."""
        if not self.file_type:
            return ".bin"
        types_lower = [t.lower() for t in (self.file_type or [])]
        if "parquet" in types_lower:
            return ".parquet"
        elif "csv" in types_lower:
            return ".csv"
        elif "json" in types_lower:
            return ".json"
        else:
            return ".bin"

    def is_parquet(self) -> bool:
        """Check if file is Parquet format."""
        return bool(self.file_type) and any(
            (t or "").lower() == "parquet" for t in (self.file_type or [])
        )

    def is_csv(self) -> bool:
        """Check if file is CSV format."""
        return bool(self.file_type) and any((t or "").lower() == "csv" for t in (self.file_type or []))

    def is_json(self) -> bool:
        """Check if file is JSON format."""
        return bool(self.file_type) and any((t or "").lower() == "json" for t in (self.file_type or []))


class FileList(BaseModel):
    """Response model for listing files."""

    group_id: str = Field(..., alias="group-id", description="Group identifier")
    file_group_ids: List[FileInfo] = Field(
        ..., alias="file-group-ids", description="List of file information"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    @property
    def file_count(self) -> int:
        """Get total number of files."""
        return len(self.file_group_ids)

    @property
    def file_types(self) -> List[str]:
        """Get list of file types."""
        types: List[str] = []
        for f in self.file_group_ids:
            ft = getattr(f, "file_type", None)
            if ft:
                types.extend([t for t in ft if isinstance(t, str)])
        return list(set(types))

    def get_files_by_type(self, file_type: str) -> List[FileInfo]:
        """Get files by type."""
        target = (file_type or "").lower()
        result: List[FileInfo] = []
        for f in self.file_group_ids:
            ft = getattr(f, "file_type", None)
            if ft and any((t or "").lower() == target for t in ft):
                result.append(f)
        return result

    def get_date_range(self) -> Optional[DateRange]:
        """Get overall date range from metadata if available."""
        # This would need to be implemented based on actual metadata structure
        # For now, return None as the new spec doesn't show date range in file list
        return None


class AvailabilityInfo(BaseModel):
    """Model representing file availability information."""

    group_id: Optional[str] = Field(
        None, alias="group-id", description="Group identifier"
    )
    file_group_id: Optional[str] = Field(
        None, alias="file-group-id", description="File group identifier"
    )
    file_date: str = Field(
        ..., alias="file-datetime", description="File date in YYYYMMDD format"
    )
    is_available: bool = Field(
        ..., alias="is-available", description="Whether the file is available"
    )
    file_name: Optional[str] = Field(
        None, alias="file-name", description="Name of the file"
    )
    first_created_on: Optional[str] = Field(
        None, alias="first-created-on", description="First creation timestamp"
    )
    last_modified: Optional[str] = Field(
        None, alias="last-modified", description="Last modification timestamp"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name

    def get_file_date(self) -> Optional[date]:
        """Get file date as date object."""
        try:
            return datetime.strptime(self.file_date, "%Y%m%d").date()
        except ValueError:
            return None


class AvailabilityResponse(BaseModel):
    """Deprecated: Use AvailabilityInfo directly."""

    group_id: Optional[str] = Field(None, alias="group-id")
    file_group_id: Optional[str] = Field(None, alias="file-group-id")
    date_range: Optional[DateRange] = Field(None, alias="date-range")
    availability: List[AvailabilityInfo] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def available_files(self) -> List[AvailabilityInfo]:
        return [f for f in self.availability if f.is_available]

    @property
    def unavailable_files(self) -> List[AvailabilityInfo]:
        return [f for f in self.availability if not f.is_available]

    @property
    def availability_rate(self) -> float:
        if not self.availability:
            return 0.0
        return (len(self.available_files) / len(self.availability)) * 100


class AvailableFilesResponse(BaseModel):
    """Response model for listing available files by date range."""

    group_id: str = Field(..., alias="group-id", description="Group identifier")
    file_group_id: Optional[str] = Field(
        None, alias="file-group-id", description="File identifier"
    )
    start_date: Optional[str] = Field(
        None, alias="start-date", description="Start date"
    )
    end_date: Optional[str] = Field(None, alias="end-date", description="End date")
    available_files: List[Dict[str, Any]] = Field(
        ..., alias="available-files", description="List of available files"
    )
    summary: Optional[Dict[str, Any]] = Field(
        None, alias="summary", description="Summary of available files"
    )

    model_config = ConfigDict(
        extra="allow", populate_by_name=True
    )  # Allow extra fields from API and populate by both alias and name


class DownloadProgress(BaseModel):
    """Model representing download progress."""

    file_group_id: str = Field(..., description="File identifier")
    bytes_downloaded: int = Field(default=0, description="Number of bytes downloaded")
    total_bytes: int = Field(default=0, description="Total number of bytes to download")
    percentage: float = Field(default=0.0, description="Download percentage (0-100)")
    speed_bps: float = Field(
        default=0.0, description="Download speed in bytes per second"
    )
    eta_seconds: Optional[float] = Field(
        default=None, description="Estimated time to completion in seconds"
    )
    start_time: datetime = Field(
        default_factory=datetime.now, description="Download start time"
    )
    last_update: datetime = Field(
        default_factory=datetime.now, description="Last progress update time"
    )
    status: DownloadStatus = Field(
        default=DownloadStatus.PENDING, description="Download status"
    )

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API

    @property
    def is_complete(self) -> bool:
        """Check if download is complete."""
        return self.percentage >= 100.0

    @property
    def remaining_bytes(self) -> int:
        """Get remaining bytes to download."""
        return max(0, self.total_bytes - self.bytes_downloaded)

    def update_progress(self, bytes_downloaded: int, speed_bps: Optional[float] = None):
        """Update download progress."""
        self.bytes_downloaded = bytes_downloaded
        self.percentage = (
            (bytes_downloaded / self.total_bytes) * 100 if self.total_bytes > 0 else 0
        )

        if speed_bps:
            self.speed_bps = speed_bps

        self.last_update = datetime.now()


class DownloadOptions(BaseModel):
    """Options for file downloads."""

    # File options
    destination_path: Optional[Path] = Field(
        default=None, description="Local path to save the file"
    )
    create_directories: bool = Field(
        default=True, description="Create parent directories if they don't exist"
    )
    overwrite_existing: bool = Field(
        default=False, description="Overwrite existing files"
    )

    # Download options
    chunk_size_setting: int = Field(
        default=8192,
        description="Chunk size for streaming downloads",
        alias="chunk_size",
    )
    max_retries: int = Field(default=3, description="Maximum number of retry attempts")
    retry_delay: float = Field(
        default=1.0, description="Delay between retries in seconds"
    )
    timeout: float = Field(default=600.0, description="Request timeout in seconds")

    # Range requests
    enable_range_requests: bool = Field(
        default=True, description="Enable HTTP range requests for resumable downloads"
    )
    range_start: Optional[int] = Field(
        default=None, description="Start byte position for range download (0-based)"
    )
    range_end: Optional[int] = Field(
        default=None, description="End byte position for range download (inclusive)"
    )
    range_header: Optional[str] = Field(
        default=None, description="Custom range header (e.g., 'bytes=0-1023')"
    )

    # Progress tracking
    show_progress: bool = Field(default=True, description="Show download progress")
    progress_callback: Optional[Any] = Field(
        default=None, description="Custom progress callback function"
    )

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API

    @property
    def overwrite(self) -> bool:
        """Backward compatibility property."""
        # Check if overwrite was passed in extra fields
        extra_overwrite = getattr(self, "__pydantic_extra__", {}).get("overwrite")
        if extra_overwrite is not None:
            return extra_overwrite
        return self.overwrite_existing

    @property
    def verify_checksum(self) -> bool:
        """Backward compatibility property."""
        # Check if verify_checksum was passed in extra fields
        extra_verify_checksum = getattr(self, "__pydantic_extra__", {}).get(
            "verify_checksum"
        )
        if extra_verify_checksum is not None:
            return extra_verify_checksum
        return False  # Default to False for backward compatibility

    @property
    def chunk_size(self) -> int:
        """Public chunk size value always returning a positive integer."""
        extra_chunk_size = getattr(self, "__pydantic_extra__", {}).get("chunk_size")
        value = (
            extra_chunk_size
            if extra_chunk_size is not None
            else self.chunk_size_setting
        )
        # Safety clamp
        try:
            value_int = int(value)
        except Exception:
            value_int = 8192
        if value_int <= 0:
            value_int = 8192
        if value_int > 1024 * 1024:
            value_int = 1024 * 1024
        return value_int

    @field_validator("chunk_size_setting")
    @classmethod
    def validate_chunk_size(cls, v):
        if v <= 0:
            raise ValueError("Chunk size must be positive")
        if v > 1024 * 1024:  # 1MB
            raise ValueError("Chunk size cannot exceed 1MB")
        return v

    @field_validator("range_start", "range_end")
    @classmethod
    def validate_range_values(cls, v):
        if v is not None and v < 0:
            raise ValueError("Range values must be non-negative")
        return v

    @field_validator("range_end")
    @classmethod
    def validate_range_end(cls, v, info):
        if v is not None and "range_start" in info.data:
            range_start = info.data["range_start"]
            if range_start is not None and v <= range_start:
                raise ValueError("Range end must be greater than range start")
        return v


class DownloadResult(BaseModel):
    """Result of a download operation."""

    file_group_id: str = Field(..., description="File identifier")
    group_id: Optional[str] = Field(None, description="Group identifier")
    local_path: Optional[Path] = Field(
        None, description="Local path where file was saved"
    )
    file_size: Optional[int] = Field(None, description="Size of the downloaded file")
    download_time: Optional[float] = Field(
        None, description="Time taken for download in seconds"
    )
    bytes_downloaded: Optional[int] = Field(None, description="Total bytes downloaded")
    status: DownloadStatus = Field(DownloadStatus.FAILED, description="Download status")
    error_message: Optional[str] = Field(
        None, description="Error message if download failed"
    )

    # legacy download result fields removed

    model_config = {"extra": "allow"}  # Allow extra fields from API

    @property
    def speed_mbps(self) -> float:
        """Calculate download speed in MB/s."""
        if not self.download_time or self.download_time <= 0 or not self.file_size:
            return 0.0
        return (self.file_size / (1024 * 1024)) / self.download_time


class Instrument(BaseModel):
    """Model representing an instrument from the DataQuery catalog."""

    item: int = Field(..., description="Item number")
    instrument_id: str = Field(
        ..., alias="instrument-id", description="Unique instrument identifier"
    )
    instrument_name: str = Field(
        ..., alias="instrument-name", description="Instrument short name"
    )
    country: Optional[str] = Field(None, description="ISO-3166-1 alpha-3 country code")
    currency: Optional[str] = Field(None, description="ISO-4217 currency code")
    instrument_cusip: Optional[str] = Field(
        None, alias="instrument-cusip", description="Instrument CUSIP"
    )
    instrument_isin: Optional[str] = Field(
        None, alias="instrument-isin", description="Instrument ISIN"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Attribute(BaseModel):
    """Model representing an attribute measure for an instrument."""

    attribute_id: str = Field(
        ..., alias="attribute-id", description="Unique attribute measure identifier"
    )
    attribute_name: str = Field(
        ..., alias="attribute-name", description="Attribute short name"
    )
    expression: str = Field(
        ..., description="Traditional DataQuery time-series identifier"
    )
    label: str = Field(..., description="Name of a time-series data set")
    last_published: Optional[str] = Field(
        None, alias="last-published", description="Date/Time data was last published"
    )
    message: Optional[str] = Field(None, description="Attribute level user message")
    time_series: Optional[List[List[Union[str, float]]]] = Field(
        None, alias="time-series", description="Time series data"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Filter(BaseModel):
    """Model representing a filter dimension."""

    filter_name: str = Field(
        ..., alias="filter-name", description="Name of a filter dimension"
    )
    description: Optional[str] = Field(
        None, alias="filter-description", description="Description of a filter"
    )
    values: Optional[List[str]] = Field(
        None, description="Valid filter value enumerator"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TimeSeriesDataPoint(BaseModel):
    """Model representing a single time series data point."""

    date: str = Field(..., description="Date in YYYYMMDD format")
    value: float = Field(..., description="Measurable value")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class InstrumentWithAttributes(BaseModel):
    """Model representing an instrument with its attributes."""

    item: int = Field(..., description="Item number")
    instrument_id: str = Field(
        ..., alias="instrument-id", description="Unique instrument identifier"
    )
    instrument_name: str = Field(
        ..., alias="instrument-name", description="Instrument short name"
    )
    instrument_cusip: Optional[str] = Field(
        None, alias="instrument-cusip", description="Instrument CUSIP"
    )
    instrument_isin: Optional[str] = Field(
        None, alias="instrument-isin", description="Instrument ISIN"
    )
    group: Optional[Dict[str, str]] = Field(None, description="Group information")
    attributes: List[Attribute] = Field(..., description="List of attributes")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class InstrumentResponse(BaseModel):
    """Response model for a single instrument."""

    instrument: Instrument = Field(..., description="Single instrument")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class InstrumentsResponse(BaseModel):
    """Response model for listing instruments."""

    links: Optional[List[Link]] = Field(None, description="Pagination links")
    items: int = Field(..., description="Total number of items")
    page_size: int = Field(
        ..., alias="page-size", description="Number of items per page"
    )
    instruments: List[Instrument] = Field(..., description="List of instruments")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class AttributesResponse(BaseModel):
    """Response model for listing attributes."""

    links: Optional[List[Link]] = Field(None, description="Pagination links")
    items: int = Field(..., description="Total number of items")
    page_size: int = Field(
        ..., alias="page-size", description="Number of items per page"
    )
    instruments: List[InstrumentWithAttributes] = Field(
        ..., description="List of instruments with attributes"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class FiltersResponse(BaseModel):
    """Response model for listing filters."""

    links: Optional[List[Link]] = Field(None, description="Pagination links")
    items: int = Field(..., description="Total number of items")
    page_size: int = Field(
        ..., alias="page-size", description="Number of items per page"
    )
    filters: List[Filter] = Field(..., description="List of filters")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TimeSeriesResponse(BaseModel):
    """Response model for time series data."""

    links: Optional[List[Link]] = Field(None, description="Pagination links")
    items: int = Field(..., description="Total number of items")
    page_size: int = Field(
        ..., alias="page-size", description="Number of items per page"
    )
    instruments: List[InstrumentWithAttributes] = Field(
        ..., description="List of instruments with time series data"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class GridDataSeries(BaseModel):
    """Model representing a grid data series."""

    expr: str = Field(..., description="The expression for the grid data")
    error_code: Optional[str] = Field(
        None, alias="errorCode", description="Error code for this specific expression"
    )
    error_message: Optional[str] = Field(
        None,
        alias="errorMessage",
        description="Error message for this specific expression",
    )
    records: Optional[List[Dict[str, Any]]] = Field(
        None, description="The grid data records"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class GridDataResponse(BaseModel):
    """Response model for grid data."""

    error_code: Optional[str] = Field(
        None, alias="errorCode", description="Error code for the entire API request"
    )
    error_message: Optional[str] = Field(
        None,
        alias="errorMessage",
        description="Error message for the entire API request",
    )
    series: List[GridDataSeries] = Field(..., description="List of grid data series")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ServiceStatus(BaseModel):
    """Model representing service status."""

    code: int = Field(..., description="Status code")
    description: str = Field(..., description="Status description")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ErrorResponse(BaseModel):
    """Error response from the API matching DataQuery specification."""

    code: Union[int, str] = Field(..., description="DataQuery error code")
    description: str = Field(
        ..., description="Description of the error that has occurred"
    )
    interaction_id: Optional[str] = Field(
        None,
        alias="x-dataquery-interaction-id",
        description="Interaction ID for traceability",
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class AuthenticationErrorResponse(ErrorResponse):
    """Authentication or Authorization information missing or invalid."""

    pass


class BadRequestResponse(ErrorResponse):
    """The request received was malformed or invalid."""

    pass


class NotFoundResponse(ErrorResponse):
    """The requested resource was not found."""

    pass


class ForbiddenPremiumResponse(ErrorResponse):
    """Premium data access required."""

    pass


class InternalServerErrorResponse(ErrorResponse):
    """Internal Server Error."""

    pass


class Information(BaseModel):
    """A DataQuery information message."""

    code: Union[int, str] = Field(
        ..., description="DataQuery code for information message"
    )
    description: str = Field(..., description="Description of information provided")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class NoContentResponse(Information):
    """Request successfully processed but no content available."""

    pass


class Available(Information):
    """DataQuery Services are functioning as expected."""

    pass


class Unavailable(ErrorResponse):
    """DataQuery Services are currently unavailable."""

    pass


# Time series parameters from OpenAPI specification
class DataType(str, Enum):
    """Data type for time series requests."""

    REFERENCE_DATA = "REFERENCE_DATA"
    NO_REFERENCE_DATA = "NO_REFERENCE_DATA"
    ALL = "ALL"


class Calendar(str, Enum):
    """Calendar convention for time-series."""

    CAL_USBANK = "CAL_USBANK"  # Default
    CAL_ALLDAYS = "CAL_ALLDAYS"
    CAL_WEEKDAYS = "CAL_WEEKDAYS"
    CAL_AUSTRALIA = "CAL_AUSTRALIA"
    CAL_BELGIUM = "CAL_BELGIUM"
    CAL_CANADA = "CAL_CANADA"
    CAL_DENMARK = "CAL_DENMARK"
    CAL_EURO = "CAL_EURO"
    CAL_FINLAND = "CAL_FINLAND"
    CAL_FRANCE = "CAL_FRANCE"
    CAL_GERMANY = "CAL_GERMANY"
    CAL_HONGKONG = "CAL_HONGKONG"
    CAL_IRELAND = "CAL_IRELAND"
    CAL_ITALY = "CAL_ITALY"
    CAL_JAPAN = "CAL_JAPAN"
    CAL_MALAYSIA = "CAL_MALAYSIA"
    CAL_NETHERLANDS = "CAL_NETHERLANDS"
    CAL_NEWZEALAND = "CAL_NEWZEALAND"
    CAL_NYSE = "CAL_NYSE"
    CAL_PORTUGAL = "CAL_PORTUGAL"
    CAL_SAFRICA = "CAL_SAFRICA"
    CAL_SINGAPORE = "CAL_SINGAPORE"
    CAL_SPAIN = "CAL_SPAIN"
    CAL_SWEDEN = "CAL_SWEDEN"
    CAL_SWITZERLAND = "CAL_SWITZERLAND"
    CAL_USEXCH = "CAL_USEXCH"
    CAL_UK = "CAL_UK"
    CALSOF = "CALSOF"
    CALTGT = "CALTGT"
    CALLIF = "CALLIF"
    CAL_EUR_UKFIN = "CAL_EUR_UKFIN"
    CAL_UK_TGT = "CAL_UK_TGT"


class Frequency(str, Enum):
    """Frequency convention for time-series."""

    FREQ_INTRA = "FREQ_INTRA"
    FREQ_DAY = "FREQ_DAY"  # Default
    FREQ_WEEK = "FREQ_WEEK"
    FREQ_MONTH = "FREQ_MONTH"
    FREQ_QUARTER = "FREQ_QUARTER"
    FREQ_ANN = "FREQ_ANN"


class Conversion(str, Enum):
    """Conversion convention for time-series."""

    CONV_LASTBUS_ABS = "CONV_LASTBUS_ABS"  # Default
    CONV_FIRSTBUS_ABS = "CONV_FIRSTBUS_ABS"
    CONV_LASTBUS_REL = "CONV_LASTBUS_REL"
    CONV_FIRSTBUS_REL = "CONV_FIRSTBUS_REL"
    CONV_SUM_ABS_SDT = "CONV_SUM_ABS_SDT"
    CONV_SUM_ABS_EDT = "CONV_SUM_ABS_EDT"
    CONV_SUM_REL_SDT = "CONV_SUM_REL_SDT"
    CONV_SUM_REL_EDT = "CONV_SUM_REL_EDT"
    CONV_AVG_ABS_SDT = "CONV_AVG_ABS_SDT"
    CONV_AVG_ABS_EDT = "CONV_AVG_ABS_EDT"
    CONV_AVG_REL_SDT = "CONV_AVG_REL_SDT"
    CONV_AVG_REL_EDT = "CONV_AVG_REL_EDT"


class NanTreatment(str, Enum):
    """Missing data point treatment for time-series."""

    NA_NOTHING = "NA_NOTHING"  # Default
    NA_LAST = "NA_LAST"
    NA_NEXT = "NA_NEXT"
    NA_INTERP = "NA_INTERP"


class Format(str, Enum):
    """Response format."""

    JSON = "JSON"


class TimeSeriesParameters(BaseModel):
    """Parameters for time series requests."""

    data: DataType = Field(DataType.REFERENCE_DATA, description="Data type to retrieve")
    format: Format = Field(Format.JSON, description="Response format")
    start_date: Optional[str] = Field(
        "TODAY-1D", alias="start-date", description="Start date"
    )
    end_date: Optional[str] = Field("TODAY", alias="end-date", description="End date")
    calendar: Calendar = Field(Calendar.CAL_USBANK, description="Calendar convention")
    frequency: Frequency = Field(Frequency.FREQ_DAY, description="Frequency convention")
    conversion: Conversion = Field(
        Conversion.CONV_LASTBUS_ABS, description="Conversion convention"
    )
    nan_treatment: NanTreatment = Field(
        NanTreatment.NA_NOTHING,
        alias="nan-treatment",
        description="Missing data treatment",
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)
