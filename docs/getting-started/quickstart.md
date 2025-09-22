# Quick Start

Get up and running with DataQuery SDK in just a few minutes.

## Basic Setup

### 1. Install the SDK

```bash
pip install dataquery-sdk
```

### 2. Set Environment Variables

```bash
# Required for OAuth authentication
export DATAQUERY_CLIENT_ID="your_client_id_here"
export DATAQUERY_CLIENT_SECRET="your_client_secret_here"

# Optional - Only set these if you want to override the defaults
# export DATAQUERY_BASE_URL="https://api-developer.jpmorgan.com"  # Default
# export DATAQUERY_CONTEXT_PATH="/research/dataquery-authe/api/v2"  # Default
# export DATAQUERY_OAUTH_TOKEN_URL="https://authe.jpmorgan.com/as/token.oauth2"  # Default
```

> **Note**: The SDK comes with pre-configured defaults for JPMorgan's DataQuery API. You only need to set your OAuth credentials to get started!

### 3. Basic Usage

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def main():
    async with DataQuery() as dq:
        # Download a single file
        result = await dq.download_file_async(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

asyncio.run(main())
```

## Your First Download

Let's download a file step by step:

```python
import asyncio
from dataquery import DataQuery
from dataquery.models import DownloadOptions
from pathlib import Path

async def download_example():
    # Create DataQuery instance
    async with DataQuery() as dq:
        # Configure download options
        options = DownloadOptions(
            destination_path=Path("./downloads"),
            create_directories=True,
            overwrite_existing=False,
            show_progress=True
        )
        
        # Download the file
        result = await dq.download_file_async(
            file_group_id="EXAMPLE_FILE_123",
            file_datetime="20250101",
            options=options,
            num_parts=5  # Use 5 parallel parts
        )
        
        # Check result
        if result.status.value == "completed":
            print(f"✅ Successfully downloaded {result.file_size} bytes")
            print(f"📁 Saved to: {result.local_path}")
            print(f"⏱️  Time taken: {result.download_time:.2f} seconds")
        else:
            print(f"❌ Download failed: {result.error_message}")

asyncio.run(download_example())
```

## Group Downloads

Download multiple files by date range:

```python
async def group_download_example():
    async with DataQuery() as dq:
        # Download all files in a group for a date range
        report = await dq.run_group_download_async(
            group_id="YOUR_GROUP_ID",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads"),
            max_concurrent=5,  # Download 5 files at once
            num_parts=5        # Each file split into 5 parts
        )
        
        print(f"📊 Download Summary:")
        print(f"   Total files: {report['total_files']}")
        print(f"   Successful: {report['successful_downloads']}")
        print(f"   Failed: {report['failed_downloads']}")
        print(f"   Success rate: {report['success_rate']:.1f}%")

asyncio.run(group_download_example())
```

## Command Line Interface

Use the CLI for quick downloads:

```bash
# Download a group by date range
python -m dataquery.cli download-group \
    YOUR_GROUP_ID \
    20250101 \
    20250131 \
    ./downloads \
    --max-concurrent 3 \
    --num-parts 5
```

## Progress Tracking

Monitor download progress:

```python
from dataquery.models import DownloadProgress

def progress_callback(progress: DownloadProgress):
    pct = f"{progress.percentage:.1f}%" if progress.total_bytes else "..."
    print(f"\rDownloading {progress.file_group_id}: {pct}", end="", flush=True)

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

## Error Handling

Handle errors gracefully:

```python
async def robust_download():
    async with DataQuery() as dq:
        try:
            result = await dq.download_file_async(
                file_group_id="FILE_123",
                file_datetime="20250101",
                destination_path=Path("./downloads")
            )
            
            if result.status.value == "completed":
                print("✅ Download successful!")
            else:
                print(f"❌ Download failed: {result.error_message}")
                
        except Exception as e:
            print(f"💥 Unexpected error: {e}")

asyncio.run(robust_download())
```

## Next Steps

Now that you have the basics:

1. **Learn more**: Check out the [User Guide](../user-guide/basic-usage.md)
2. **Explore examples**: See [Examples](../examples/basic.md)
3. **API Reference**: Browse the [API documentation](../api/dataquery.md)
4. **Configuration**: Learn about [Configuration Options](configuration.md)

## Need Help?

- 📚 [Full Documentation](../index.md)
- 🐛 [Report Issues](https://github.com/dataquery/dataquery-sdk/issues)
- 💬 [Ask Questions](https://github.com/dataquery/dataquery-sdk/discussions)
