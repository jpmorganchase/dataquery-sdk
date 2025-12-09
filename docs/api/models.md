# Models API

DataQuery SDK provides several model classes for configuration and data handling.

<div class="grid cards" markdown>

-   :material-cog:{ .lg .middle } **ClientConfig**

    ---

    Configuration class for SDK settings with smart defaults

-   :material-download:{ .lg .middle } **DownloadOptions**

    ---

    Fine-tune download behavior and performance

-   :material-check-circle:{ .lg .middle } **DownloadResult**

    ---

    Comprehensive download operation results

-   :material-progress-clock:{ .lg .middle } **DownloadProgress**

    ---

    Real-time download progress tracking

</div>

## :material-cog: ClientConfig

!!! info "Configuration Class"
    Primary configuration class for DataQuery SDK settings with pre-configured defaults for JPMorgan DataQuery API.

**Key Parameters:**

| Parameter | Type | Default | Environment Variable | Description |
|-----------|------|---------|---------------------|-------------|
| `base_url` | `str` | `"https://api-developer.jpmorgan.com"` | `DATAQUERY_BASE_URL` | Base URL of the DataQuery API |
| `api_version` | `str` | `"2.0.0"` | `DATAQUERY_API_VERSION` | API version |
| `context_path` | `Optional[str]` | `"/research/dataquery-authe/api/v2"` | `DATAQUERY_CONTEXT_PATH` | API context path |
| `oauth_enabled` | `bool` | `True` | `DATAQUERY_OAUTH_ENABLED` | Enable OAuth authentication |
| `client_id` | `str` | — | `DATAQUERY_CLIENT_ID` | OAuth client ID *(required)* |
| `client_secret` | `str` | — | `DATAQUERY_CLIENT_SECRET` | OAuth client secret *(required)* |
| `download_dir` | `Optional[str]` | `"./downloads"` | `DATAQUERY_DOWNLOAD_DIR` | Default download directory |
| `timeout` | `float` | `6000.0` | `DATAQUERY_TIMEOUT` | Request timeout in seconds |
| `max_retries` | `int` | `3` | `DATAQUERY_MAX_RETRIES` | Maximum number of retries |
| `log_level` | `str` | `"INFO"` | `DATAQUERY_LOG_LEVEL` | Logging level |

!!! success "Simplified Configuration"
    With pre-configured defaults, you only need to provide `client_id` and `client_secret`!

## :material-download: DownloadOptions

!!! info "Download Configuration"
    Fine-tune download behavior, performance, and error handling options.

**Key Parameters:**

| Parameter | Type | Default | Environment Variable | Description |
|-----------|------|---------|---------------------|-------------|
| `destination_path` | `Optional[Path]` | `None` | — | Download destination path |
| `create_directories` | `bool` | `True` | `DATAQUERY_CREATE_DIRECTORIES` | Create directories if they don't exist |
| `overwrite_existing` | `bool` | `False` | `DATAQUERY_OVERWRITE_EXISTING` | Overwrite existing files |
| `chunk_size` | `int` | `1048576` | — | Download chunk size in bytes (1MB default) |
| `max_retries` | `int` | `3` | `DATAQUERY_MAX_RETRIES` | Maximum number of retries |
| `show_progress` | `bool` | `True` | — | Show progress during download |
| `enable_range_requests` | `bool` | `True` | — | Enable HTTP range requests for parallel downloads |

!!! tip "Performance Optimization"
    The default 1MB chunk size is optimized for large files. For files >1GB, you can use even larger values (e.g., 2-8MB) for better performance. The SDK automatically optimizes chunk sizes based on file size.

## :material-check-circle: DownloadResult

!!! info "Download Result"
    Comprehensive result object returned after download operations.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `file_group_id` | `str` | File ID that was downloaded |
| `local_path` | `Path` | Local path where file was saved |
| `file_size` | `int` | Size of the downloaded file in bytes |
| `download_time` | `float` | Time taken for download in seconds |
| `status` | `DownloadStatus` | Download status (completed, failed, etc.) |
| `error_message` | `Optional[str]` | Error message if download failed |

## :material-progress-clock: DownloadProgress

!!! info "Progress Tracking"
    Real-time progress information for download operations.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `file_group_id` | `str` | File ID being downloaded |
| `bytes_downloaded` | `int` | Number of bytes downloaded |
| `total_bytes` | `Optional[int]` | Total file size in bytes |
| `percentage` | `float` | Download percentage (0.0 to 100.0) |

## :material-state-machine: DownloadStatus

!!! info "Status Enumeration"
    Enumeration of possible download statuses.

| Status | Description |
|--------|-------------|
| `PENDING` | Download is pending |
| `IN_PROGRESS` | Download is in progress |
| `COMPLETED` | Download completed successfully |
| `FAILED` | Download failed |
| `CANCELLED` | Download was cancelled |

## :material-code-braces: Examples

### Creating Download Options

!!! example "Basic Configuration"
    ```python
    from dataquery.models import DownloadOptions
    from pathlib import Path

    # Basic options with defaults
    options = DownloadOptions(
        destination_path=Path("./downloads"),
        create_directories=True,
        overwrite_existing=False
    )
    ```

!!! example "Advanced Configuration"
    ```python
    # Advanced options for high-performance downloads
    advanced_options = DownloadOptions(
        destination_path=Path("./downloads"),
        create_directories=True,
        overwrite_existing=False,
        chunk_size=4194304,  # 4MB chunks for large files (3GB+)
        max_retries=3,
        enable_range_requests=True,
        show_progress=True
    )
    ```

### Working with Download Results

!!! example "Processing Results"
    ```python
    from dataquery.models import DownloadResult, DownloadStatus

    def process_result(result: DownloadResult):
        print(f"File ID: {result.file_group_id}")
        print(f"Local Path: {result.local_path}")
        print(f"File Size: {result.file_size} bytes")
        print(f"Download Time: {result.download_time:.2f} seconds")

        if result.status == DownloadStatus.COMPLETED:
            print("✅ Download completed successfully!")
        elif result.status == DownloadStatus.FAILED:
            print(f"❌ Download failed: {result.error_message}")
        else:
            print(f"⚠️  Download status: {result.status.value}")
    ```

### Progress Tracking

!!! example "Real-time Progress"
    ```python
    from dataquery.models import DownloadProgress

    def progress_callback(progress: DownloadProgress):
        pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
        print(f"\r{progress.file_group_id}: {pct}", end="", flush=True)

    # Use with download
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads"),
            progress_callback=progress_callback
        )
    ```

### Configuration Examples

!!! example "Minimal Configuration"
    ```python
    from dataquery.models import ClientConfig

    # Only credentials required - everything else uses smart defaults!
    config = ClientConfig(
        client_id="your_client_id",
        client_secret="your_client_secret"
    )
    ```

!!! example "Custom Configuration"
    ```python
    # Override specific defaults as needed
    config = ClientConfig(
        client_id="your_client_id",
        client_secret="your_client_secret",
        base_url="https://custom-api.example.com",  # Override default
        timeout=300.0,  # Override default (6000.0)
        log_level="DEBUG"  # Override default (INFO)
    )
    ```

!!! tip "Available Defaults"
    The SDK provides 40+ pre-configured options including:

    - **API**: `api_version="2.0.0"`, `context_path="/research/dataquery-authe/api/v2"`
    - **Authentication**: `oauth_enabled=True`, `grant_type="client_credentials"`
    - **Connection**: `timeout=6000.0`, `max_retries=3`, `retry_delay=1.0`
    - **Rate Limiting**: `requests_per_minute=300`, `burst_capacity=20`
    - **Downloads**: `download_dir="./downloads"`, `chunk_size=1048576`, `show_progress=True`
    - **Security**: `mask_secrets=True`, `token_storage_enabled=False`

    And many more!
