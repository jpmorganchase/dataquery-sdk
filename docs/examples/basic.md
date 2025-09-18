# Basic Examples

Simple examples to get you started with DataQuery SDK.

## Single File Download

### Basic Download

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def download_file():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

asyncio.run(download_file())
```

### Download with Progress

```python
from dataquery.models import DownloadProgress

def progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\rDownloading: {pct}", end="", flush=True)

async def download_with_progress():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="LARGE_FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads"),
            progress_callback=progress_callback
        )
        print(f"\nDownloaded: {result.local_path}")

asyncio.run(download_with_progress())
```

### Download with Custom Options

```python
from dataquery.models import DownloadOptions

async def download_with_options():
    options = DownloadOptions(
        destination_path=Path("./downloads"),
        create_directories=True,
        overwrite_existing=False,
        chunk_size=16384,
        max_retries=3,
        show_progress=True
    )
    
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            options=options,
            num_parts=5
        )
        print(f"Downloaded: {result.local_path}")

asyncio.run(download_with_options())
```

## Group Downloads

### Basic Group Download

```python
async def download_group():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads")
        )
        
        print(f"Downloaded {report['successful_downloads']} files")
        print(f"Success rate: {report['success_rate']:.1f}%")

asyncio.run(download_group())
```

### Group Download with Concurrency

```python
async def download_group_concurrent():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads"),
            max_concurrent=5,  # Download 5 files at once
            num_parts=3,       # Each file split into 3 parts
            delay_between_downloads=0.5  # 500ms delay between files
        )
        
        print(f"Downloaded {report['successful_downloads']} files")
        print(f"Total time: {report['total_time_formatted']}")

asyncio.run(download_group_concurrent())
```

### Group Download with Progress

```python
def group_progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\r{progress.file_group_id}: {pct}", end="", flush=True)

async def download_group_with_progress():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads"),
            progress_callback=group_progress_callback
        )
        
        print(f"\nDownloaded {report['successful_downloads']} files")

asyncio.run(download_group_with_progress())
```

## Error Handling

### Basic Error Handling

```python
from dataquery.exceptions import DataQueryError, DownloadError

async def download_with_error_handling():
    try:
        async with DataQuery() as dq:
            result = await dq.download_file_async(
                file_group_id="FILE_123",
                file_datetime="20250101",
                destination_path=Path("./downloads")
            )
            
            if result.status.value == "completed":
                print("✅ Download successful!")
            else:
                print(f"❌ Download failed: {result.error_message}")
                
    except DownloadError as e:
        print(f"Download error: {e}")
    except DataQueryError as e:
        print(f"DataQuery error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

asyncio.run(download_with_error_handling())
```

### Retry Logic

```python
import asyncio
from dataquery.exceptions import DownloadError

async def download_with_retry(max_retries=3):
    for attempt in range(max_retries):
        try:
            async with DataQuery() as dq:
                result = await dq.download_file_async(
                    file_group_id="FILE_123",
                    file_datetime="20250101",
                    destination_path=Path("./downloads")
                )
                
                if result.status.value == "completed":
                    print("✅ Download successful!")
                    return result
                else:
                    print(f"❌ Download failed: {result.error_message}")
                    
        except DownloadError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise

asyncio.run(download_with_retry())
```

## Configuration

### Environment Variables

```python
import os
from dataquery import DataQuery

# Set environment variables
os.environ["DATAQUERY_BASE_URL"] = "https://api-developer.jpmorgan.com"
os.environ["DATAQUERY_OAUTH_ENABLED"] = "true"
os.environ["DATAQUERY_CLIENT_ID"] = "your_client_id"
os.environ["DATAQUERY_CLIENT_SECRET"] = "your_client_secret"

async def download_with_env_config():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )

asyncio.run(download_with_env_config())
```

### Programmatic Configuration

```python
from dataquery import DataQuery
from dataquery.models import ClientConfig

async def download_with_config():
    # Minimal configuration - only base_url required!
    config = ClientConfig(
        base_url="https://api-developer.jpmorgan.com"
        # All other settings use sensible defaults
    )
    
    async with DataQuery(config=config) as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )

asyncio.run(download_with_config())
```

### Override Specific Defaults

```python
async def download_with_custom_config():
    # Override only what you need to change
    config = ClientConfig(
        base_url="https://api-developer.jpmorgan.com",
        timeout=300.0,  # Override default timeout
        log_level="DEBUG",  # Override default log level
        oauth_enabled=True,  # Enable OAuth
        client_id="your_client_id",
        client_secret="your_client_secret"
    )
    
    async with DataQuery(config=config) as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )

asyncio.run(download_with_custom_config())
```

## Synchronous Usage

### Synchronous Group Download

```python
from dataquery import DataQuery

def sync_download():
    with DataQuery() as dq:
        report = dq.run_group_download(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads")
        )
        print(f"Downloaded {report['successful_downloads']} files")

sync_download()
```

### Synchronous File Download

```python
def sync_file_download():
    with DataQuery() as dq:
        result = dq.download_file(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

sync_file_download()
```
