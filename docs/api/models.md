# Models API

DataQuery SDK provides several model classes for configuration and data handling.

## DataQueryConfig

Configuration class for DataQuery SDK settings.

**Key Parameters:**
- `base_url` (str): Base URL of the DataQuery API (maps to `DATAQUERY_BASE_URL`)
- `api_version` (str): API version (maps to `DATAQUERY_API_VERSION`, default: `2.0.0`)
- `context_path` (str, optional): API context path (maps to `DATAQUERY_CONTEXT_PATH`, default: empty)
- `oauth_enabled` (bool): Enable OAuth authentication (maps to `DATAQUERY_OAUTH_ENABLED`)
- `client_id` (str): OAuth client ID (maps to `DATAQUERY_CLIENT_ID`)
- `client_secret` (str): OAuth client secret (maps to `DATAQUERY_CLIENT_SECRET`)
- `download_dir` (str, optional): Default download directory (maps to `DATAQUERY_DOWNLOAD_DIR`)
- `timeout` (float, optional): Request timeout in seconds (maps to `DATAQUERY_TIMEOUT`)
- `max_retries` (int, optional): Maximum number of retries (maps to `DATAQUERY_MAX_RETRIES`)
- `log_level` (str, optional): Logging level (maps to `DATAQUERY_LOG_LEVEL`)

## DownloadOptions

Configuration class for download operations.

**Key Parameters:**
- `destination_path` (Path, optional): Download destination path
- `create_directories` (bool): Create directories if they don't exist (maps to `DATAQUERY_CREATE_DIRECTORIES`)
- `overwrite_existing` (bool): Overwrite existing files (maps to `DATAQUERY_OVERWRITE_EXISTING`)
- `chunk_size` (int): Download chunk size in bytes
- `max_retries` (int): Maximum number of retries (maps to `DATAQUERY_MAX_RETRIES`)
- `show_progress` (bool): Show progress during download
- `enable_range_requests` (bool): Enable HTTP range requests

## DownloadResult

Result object returned after download operations.

**Attributes:**
- `file_group_id` (str): File ID that was downloaded
- `local_path` (Path): Local path where file was saved
- `file_size` (int): Size of the downloaded file in bytes
- `download_time` (float): Time taken for download in seconds
- `status` (DownloadStatus): Download status (completed, failed, etc.)
- `error_message` (str, optional): Error message if download failed

## DownloadProgress

Progress information for download operations.

**Attributes:**
- `file_group_id` (str): File ID being downloaded
- `bytes_downloaded` (int): Number of bytes downloaded
- `total_bytes` (int, optional): Total file size in bytes
- `percentage` (float): Download percentage (0.0 to 100.0)

## DownloadStatus

Enumeration of possible download statuses.

**Values:**
- `PENDING`: Download is pending
- `IN_PROGRESS`: Download is in progress
- `COMPLETED`: Download completed successfully
- `FAILED`: Download failed
- `CANCELLED`: Download was cancelled

## Examples

### Creating Download Options

```python
from dataquery.models import DownloadOptions
from pathlib import Path

# Basic options
options = DownloadOptions(
    destination_path=Path("./downloads"),
    create_directories=True,
    overwrite_existing=False
)

# Advanced options
advanced_options = DownloadOptions(
    destination_path=Path("./downloads"),
    create_directories=True,
    overwrite_existing=False,
    chunk_size=16384,
    max_retries=3,
    retry_delay=1.0,
    timeout=600.0,
    enable_range_requests=True,
    show_progress=True
)
```

### Working with Download Results

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

### Configuration

```python
from dataquery.models import ClientConfig

# Minimal configuration - only base_url required!
config = ClientConfig(
    base_url="https://api-developer.jpmorgan.com"
    # All other fields use sensible defaults
)

# Override specific defaults as needed
config = ClientConfig(
    base_url="https://api-developer.jpmorgan.com",
    oauth_enabled=True,  # Override default (False)
    client_id="your_client_id",
    client_secret="your_client_secret",
    timeout=300.0,  # Override default (6000.0)
    log_level="DEBUG"  # Override default (INFO)
)

# All available defaults (40+ configuration options):
# API: api_version="2.0.0", context_path=""
# Authentication: oauth_enabled=False, grant_type="client_credentials"
# Connection: timeout=6000.0, max_retries=3, retry_delay=1.0
# Rate Limiting: requests_per_minute=100, burst_capacity=20
# Logging: log_level="INFO", enable_debug_logging=False
# Download: download_dir="./downloads", chunk_size=8192, show_progress=True
# Batch Downloads: max_concurrent_downloads=3, batch_size=10, retry_failed=True
# Workflow: workflow_dir="workflow", groups_dir="groups", default_dir="files"
# Security: mask_secrets=True, token_storage_enabled=False
# And many more...
```
