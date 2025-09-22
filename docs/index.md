# DataQuery SDK

A high-performance Python SDK for efficient data querying and file downloads with parallel processing capabilities.

## Features

<div class="grid cards" markdown>

-   :material-download:{ .lg .middle } **Parallel Downloads**

    ---

    HTTP range requests for lightning-fast file downloads with intelligent chunking

-   :material-speedometer:{ .lg .middle } **Intelligent Rate Limiting**

    ---

    Built-in rate limiting and delay management to prevent API throttling

-   :material-folder-multiple:{ .lg .middle } **Batch Operations**

    ---

    Download multiple files by date range with configurable concurrency

-   :material-console:{ .lg .middle } **Command Line Interface**

    ---

    Easy-to-use CLI for automation and scripting workflows

-   :material-test-tube:{ .lg .middle } **Comprehensive Testing**

    ---

    Full test coverage with continuous integration and quality assurance

-   :material-api:{ .lg .middle } **Modern API Design**

    ---

    Clean, async-first API with type hints and comprehensive documentation

</div>

## Quick Start

Get started with DataQuery SDK in minutes:

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

## Installation

```bash
pip install dataquery-sdk
```

## What's New

### Latest Release: v1.0.0

- ✅ Initial release with parallel download capabilities
- ✅ Group download functionality with intelligent rate limiting
- ✅ Command-line interface for batch operations
- ✅ Comprehensive test suite with 95%+ coverage
- ✅ Full type hints and documentation

## Performance

DataQuery SDK is designed for high-performance data operations:

- **Parallel Downloads**: Up to 10x faster than sequential downloads
- **Memory Efficient**: Streaming downloads with configurable chunk sizes
- **Rate Limited**: Intelligent delays prevent API throttling
- **Resumable**: Automatic retry and resume for failed downloads

## Community

- :material-github: [GitHub Repository](https://github.com/dataquery/dataquery-sdk)
- :material-bug: [Report Issues](https://github.com/dataquery/dataquery-sdk/issues)
- :material-lightbulb: [Request Features](https://github.com/dataquery/dataquery-sdk/issues/new?template=feature_request.md)
- :material-book: [Documentation](https://dataquery.github.io/dataquery-sdk/)

## License

This project is licensed under the MIT License.
