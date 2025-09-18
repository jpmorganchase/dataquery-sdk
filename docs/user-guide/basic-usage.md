# Basic Usage

Learn the fundamental concepts and patterns for using DataQuery SDK effectively.

## Core Concepts

### DataQuery

Use the high-level `DataQuery` interface for most operations (downloads, listing groups/files, availability, instruments, time series, grid, etc.).

```python
from dataquery.dataquery import DataQuery

async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async()
        print(len(groups))
```

### Async Context Managers

Always use DataQuery as an async context manager:

```python
# ✅ Correct
async with DataQuery() as dq:
    result = await dq.download_file_async(...)

# ❌ Incorrect - may leak resources
dq = DataQuery()
result = await dq.download_file_async(...)
```

## File Downloads

### Single File Download

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def download_single_file():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        
        print(f"Downloaded: {result.local_path}")
        print(f"Size: {result.file_size} bytes")
        print(f"Time: {result.download_time:.2f} seconds")

asyncio.run(download_single_file())
```

### Download with Options

```python
from dataquery.models import DownloadOptions

async def download_with_options():
    options = DownloadOptions(
        destination_path=Path("./downloads"),
        create_directories=True,
        overwrite_existing=False,
        chunk_size=16384,  # 16KB chunks
        max_retries=3,
        show_progress=True
    )
    
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            options=options,
            num_parts=5  # Use 5 parallel parts
        )

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

### Group Download with Concurrency Control

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

asyncio.run(download_group_concurrent())
```

## Progress Tracking

### Basic Progress Callback

```python
from dataquery.models import DownloadProgress

def progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\r{progress.file_group_id}: {pct}", end="", flush=True)

async def download_with_progress():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="LARGE_FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads"),
            progress_callback=progress_callback
        )

asyncio.run(download_with_progress())
```

### Advanced Progress Tracking

```python
import time
from dataquery.models import DownloadProgress

class ProgressTracker:
    def __init__(self):
        self.start_time = time.time()
        self.file_progress = {}
    
    def callback(self, progress: DownloadProgress):
        file_id = progress.file_group_id
        self.file_progress[file_id] = progress
        
        # Calculate speed
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            speed = progress.bytes_downloaded / elapsed
            speed_mb = speed / (1024 * 1024)
            
            print(f"{file_id}: {progress.percentage:.1f}% "
                  f"({speed_mb:.1f} MB/s)")

async def download_with_advanced_progress():
    tracker = ProgressTracker()
    
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="LARGE_FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads"),
            progress_callback=tracker.callback
        )

asyncio.run(download_with_advanced_progress())
```

## Error Handling

### Basic Error Handling

```python
from dataquery.exceptions import DataQueryError, DownloadError

async def robust_download():
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

asyncio.run(robust_download())
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

## Working with Results

### DownloadResult Object

```python
async def examine_result():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        
        # Access result properties
        print(f"File ID: {result.file_group_id}")
        print(f"Local Path: {result.local_path}")
        print(f"File Size: {result.file_size} bytes")
        print(f"Download Time: {result.download_time:.2f} seconds")
        print(f"Status: {result.status.value}")
        print(f"Error Message: {result.error_message}")
        
        # Check if successful
        if result.status.value == "completed":
            print("✅ Download completed successfully!")
        else:
            print(f"❌ Download failed: {result.error_message}")

asyncio.run(examine_result())
```

### Group Download Report

```python
async def examine_group_report():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads")
        )
        
        # Access report data
        print(f"Group ID: {report['group_id']}")
        print(f"Date Range: {report['start_date']} to {report['end_date']}")
        print(f"Total Files: {report['total_files']}")
        print(f"Successful: {report['successful_downloads']}")
        print(f"Failed: {report['failed_downloads']}")
        print(f"Success Rate: {report['success_rate']:.1f}%")
        print(f"Total Time: {report['total_time_formatted']}")
        
        # Check for failed files
        if report['failed_files']:
            print(f"Failed Files: {report['failed_files']}")

asyncio.run(examine_group_report())
```

## Best Practices

### 1. Always Use Context Managers

```python
# ✅ Good
async with DataQuery() as dq:
    result = await dq.download_file_async(...)

# ❌ Bad
dq = DataQuery()
result = await dq.download_file_async(...)
```

### 2. Handle Errors Appropriately

```python
# ✅ Good
try:
    result = await dq.download_file_async(...)
    if result.status.value != "completed":
        logger.error(f"Download failed: {result.error_message}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")

# ❌ Bad
result = await dq.download_file_async(...)  # No error handling
```

### 3. Use Appropriate Concurrency

```python
# ✅ Good - reasonable concurrency
max_concurrent=5
num_parts=5

# ❌ Bad - too aggressive
max_concurrent=100
num_parts=100
```

### 4. Monitor Progress for Long Downloads

```python
# ✅ Good - show progress for large files
if file_size > 10 * 1024 * 1024:  # 10MB
    result = await dq.download_file_async(
        ...,
        progress_callback=progress_callback
    )
```

### 5. Use Type Hints

```python
# ✅ Good
async def download_file(
    file_id: str,
    date: str,
    destination: Path
) -> DownloadResult:
    # Implementation
    pass
```
