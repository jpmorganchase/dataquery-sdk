# DataQuery API

The main `DataQuery` class provides a high-level interface for data querying and file downloads.

## Overview

The `DataQuery` class is the primary interface for interacting with the DataQuery API. It provides convenient methods for file downloads, group operations, and data querying.

## Key Features

<div class="grid cards" markdown>

-   :material-rocket:{ .lg .middle } **High-Level Interface**

    ---

    Easy-to-use methods for common operations

-   :material-lightning-bolt:{ .lg .middle } **Async Support**

    ---

    Full async/await support for concurrent operations

-   :material-folder-download:{ .lg .middle } **Group Downloads**

    ---

    Batch download files by date range

-   :material-chart-line:{ .lg .middle } **Progress Tracking**

    ---

    Built-in progress callbacks

-   :material-shield-check:{ .lg .middle } **Error Handling**

    ---

    Comprehensive error handling and retry logic

</div>

## Async and Sync Usage

All endpoints are available in both forms: async (`method_async(...)`) and sync (`method(...)`). In this page, we show async examples and list the sync equivalent for each method.

```python
# Async
async with DataQuery() as dq:
    groups = await dq.list_groups_async()

# Sync
with DataQuery() as dq:
    groups = dq.list_groups()
```

## Basic Usage

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def main():
    async with DataQuery() as dq:
        # Download a single file
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

asyncio.run(main())
```

## Methods

### :material-database: Core Listing and Search

#### `list_groups_async(limit: Optional[int] = 100) -> List[Group]`

!!! info "Method Description"
    List all available data groups with optional limit.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | `Optional[int]` | `100` | Max number of groups; `None` returns all |

**Returns:** `List[Group]`

!!! note "Sync Equivalent"
    `list_groups(limit: Optional[int] = 100) -> List[Group]`

!!! example "Usage Example"
    ```python
    import asyncio
    from dataquery.dataquery import DataQuery

    async def ex():
        async with DataQuery() as dq:
            groups = await dq.list_groups_async(limit=10)
            print(len(groups))

    asyncio.run(ex())
    ```

#### `search_groups_async(keywords: str, limit: Optional[int] = 100, offset: Optional[int] = None) -> List[Group]`

!!! info "Method Description"
    Search groups by keywords with pagination support.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keywords` | `str` | — | Search terms |
| `limit` | `Optional[int]` | `100` | Max results to return |
| `offset` | `Optional[int]` | `None` | Results to skip for pagination |

**Returns:** `List[Group]`

!!! note "Sync Equivalent"
    `search_groups(keywords: str, limit: Optional[int] = 100, offset: Optional[int] = None) -> List[Group]`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            results = await dq.search_groups_async(keywords="credit", limit=5)
            print([g.name for g in results])

    asyncio.run(ex())
    ```

#### `list_files_async(group_id: str, file_group_id: Optional[str] = None) -> List[FileInfo]`

!!! info "Method Description"
    List files within a group with optional filtering by file_group_id.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `file_group_id` | `Optional[str]` | `None` | Specific file id filter |

**Returns:** `List[FileInfo]`

!!! note "Sync Equivalent"
    `list_files(group_id: str, file_group_id: Optional[str] = None) -> List[FileInfo]`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            files = await dq.list_files_async(group_id="GROUP_123")
            print(len(files))

    asyncio.run(ex())
    ```

### :material-check-circle-outline: Availability and Health

#### `check_availability_async(file_group_id: str, file_datetime: str) -> AvailabilityInfo`

!!! info "Method Description"
    Check if a file is available for a specific datetime.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_group_id` | `str` | — | File group identifier |
| `file_datetime` | `str` | — | YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS |

**Returns:** `AvailabilityInfo`

!!! note "Sync Equivalent"
    `check_availability(file_group_id: str, file_datetime: str) -> AvailabilityInfo`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            av = await dq.check_availability_async("FILE_123", "20250101")
            print(getattr(av, "is_available", False))

    asyncio.run(ex())
    ```

#### `list_available_files_async(...) -> List[dict]`

!!! info "Method Description"
    List available files for a group within a specified date range.

```python
async def list_available_files_async(
    group_id: str,
    file_group_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[dict]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `file_group_id` | `Optional[str]` | `None` | Specific file group ID filter |
| `start_date` | `Optional[str]` | `None` | Start date (YYYYMMDD format) |
| `end_date` | `Optional[str]` | `None` | End date (YYYYMMDD format) |

**Returns:** `List[dict]`

!!! note "Sync Equivalent"
    `list_available_files(group_id: str, ...) -> List[dict]`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            items = await dq.list_available_files_async(
                group_id="GROUP_123", start_date="20250101", end_date="20250131"
            )
            print(len(items))

    asyncio.run(ex())
    ```

#### `health_check_async() -> bool`

!!! info "Method Description"
    Check if the API is healthy and responsive.

**Parameters:** None

**Returns:** `bool` - `True` if API is healthy, `False` otherwise

!!! note "Sync Equivalent"
    `health_check() -> bool`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            ok = await dq.health_check_async()
            print(ok)

    asyncio.run(ex())
    ```

### :material-download: Downloads

#### `download_file_async(...) -> DownloadResult`

!!! info "Method Description"
    Download a single file using parallel HTTP range requests for maximum speed.

```python
async def download_file_async(
    file_group_id: str,
    file_datetime: Optional[str] = None,
    destination_path: Optional[Path] = None,
    options: Optional[DownloadOptions] = None,
    num_parts: int = 5,
    progress_callback: Optional[Callable] = None
) -> DownloadResult
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_group_id` | `str` | — | File identifier to download |
| `file_datetime` | `Optional[str]` | `None` | File datetime (YYYYMMDD format) |
| `destination_path` | `Optional[Path]` | `None` | Download destination directory |
| `options` | `Optional[DownloadOptions]` | `None` | Download configuration options |
| `num_parts` | `int` | `5` | Number of parallel parts for download |
| `progress_callback` | `Optional[Callable]` | `None` | Progress tracking callback |

**Returns:** `DownloadResult`

!!! tip "Performance Tip"
    Higher `num_parts` values can significantly improve download speeds for large files by using more parallel HTTP range requests.

!!! note "Sync Equivalent"
    `download_file(file_group_id: str, ...) -> DownloadResult`

!!! example "Usage Example"
    ```python
    import asyncio
    from pathlib import Path
    from dataquery.dataquery import DataQuery

    async def ex():
        async with DataQuery() as dq:
            res = await dq.download_file_async(
                file_group_id="FILE_123",
                file_datetime="20250101",
                destination_path=Path("./downloads")
            )
            print(res.status, res.local_path)

    asyncio.run(ex())
    ```

#### `run_group_download_async(...) -> dict`

!!! info "Method Description"
    Download all files in a group for a date range with intelligent concurrency management.

```python
async def run_group_download_async(
    group_id: str,
    start_date: str,
    end_date: str,
    destination_dir: Path = Path("./downloads"),
    max_concurrent: int = 5,
    num_parts: int = 5,
    progress_callback: Optional[Callable] = None,
    delay_between_downloads: float = 1.0
) -> dict
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `start_date` | `str` | — | Start date (YYYYMMDD format) |
| `end_date` | `str` | — | End date (YYYYMMDD format) |
| `destination_dir` | `Path` | `Path("./downloads")` | Download destination directory |
| `max_concurrent` | `int` | `5` | Maximum concurrent downloads |
| `num_parts` | `int` | `5` | Number of parallel parts per file |
| `progress_callback` | `Optional[Callable]` | `None` | Progress tracking callback |
| `delay_between_downloads` | `float` | `1.0` | Delay between downloads in seconds |

**Returns:** `dict` - Comprehensive download report

!!! tip "Concurrency Note"
    Total concurrent requests = `max_concurrent × num_parts`. Adjust based on your system capabilities and API rate limits.

!!! note "Sync Equivalent"
    `run_group_download(group_id: str, ...) -> dict`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            report = await dq.run_group_download_async(
                group_id="GROUP_123", start_date="20250101", end_date="20250131"
            )
            print(report.get("successful_downloads"))

    asyncio.run(ex())
    ```

### :material-chart-line: Instruments and Time Series

#### `list_instruments_async(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> InstrumentsResponse`

!!! info "Method Description"
    List instruments and identifiers for a dataset with optional pagination.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `instrument_id` | `Optional[str]` | `None` | Specific instrument ID filter |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `InstrumentsResponse`

!!! note "Sync Equivalent"
    `list_instruments(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> InstrumentsResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            resp = await dq.list_instruments_async(group_id="GROUP_123")
            print(getattr(resp, "count", None))

    asyncio.run(ex())
    ```

#### `search_instruments_async(group_id: str, keywords: str, page: Optional[str] = None) -> InstrumentsResponse`

!!! info "Method Description"
    Search instruments within a dataset using keywords.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `keywords` | `str` | — | Search keywords |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `InstrumentsResponse`

!!! note "Sync Equivalent"
    `search_instruments(group_id: str, keywords: str, page: Optional[str] = None) -> InstrumentsResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            resp = await dq.search_instruments_async(group_id="GROUP_123", keywords="usd")
            print(getattr(resp, "count", None))

    asyncio.run(ex())
    ```

#### `get_instrument_time_series_async(...) -> TimeSeriesResponse`

!!! info "Method Description"
    Retrieve time-series data for explicit instruments and attributes.

```python
async def get_instrument_time_series_async(
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
) -> TimeSeriesResponse
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `instruments` | `List[str]` | — | Instrument identifiers |
| `attributes` | `List[str]` | — | Attribute identifiers |
| `data` | `str` | `"REFERENCE_DATA"` | Data domain |
| `format` | `str` | `"JSON"` | Response format |
| `start_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `end_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `calendar` | `str` | `"CAL_USBANK"` | Calendar convention |
| `frequency` | `str` | `"FREQ_DAY"` | Frequency convention |
| `conversion` | `str` | `"CONV_LASTBUS_ABS"` | Conversion convention |
| `nan_treatment` | `str` | `"NA_NOTHING"` | Missing data handling |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `TimeSeriesResponse`

!!! note "Sync Equivalent"
    `get_instrument_time_series(instruments: List[str], attributes: List[str], ...) -> TimeSeriesResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            ts = await dq.get_instrument_time_series_async(
                instruments=["US912828U816"], attributes=["PX_LAST"],
                start_date="20240101", end_date="20240131"
            )
            print(type(ts))

    asyncio.run(ex())
    ```

#### `get_expressions_time_series_async(...) -> TimeSeriesResponse`

!!! info "Method Description"
    Retrieve time-series data using traditional DataQuery expressions.

```python
async def get_expressions_time_series_async(
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
) -> TimeSeriesResponse
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expressions` | `List[str]` | — | Traditional DataQuery expressions |
| `format` | `str` | `"JSON"` | Response format |
| `start_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `end_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `calendar` | `str` | `"CAL_USBANK"` | Calendar convention |
| `frequency` | `str` | `"FREQ_DAY"` | Frequency convention |
| `conversion` | `str` | `"CONV_LASTBUS_ABS"` | Conversion convention |
| `nan_treatment` | `str` | `"NA_NOTHING"` | Missing data handling |
| `data` | `str` | `"REFERENCE_DATA"` | Data domain |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `TimeSeriesResponse`

!!! note "Sync Equivalent"
    `get_expressions_time_series(expressions: List[str], ...) -> TimeSeriesResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            ts = await dq.get_expressions_time_series_async(
                expressions=["PX_LAST(IBM,USD)"], start_date="20240101", end_date="20240131"
            )
            print(type(ts))

    asyncio.run(ex())
    ```

#### `get_group_filters_async(group_id: str, page: Optional[str] = None) -> FiltersResponse`

!!! info "Method Description"
    Get unique list of filter dimensions for a dataset.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `FiltersResponse`

!!! note "Sync Equivalent"
    `get_group_filters(group_id: str, page: Optional[str] = None) -> FiltersResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            filters = await dq.get_group_filters_async(group_id="GROUP_123")
            print(type(filters))

    asyncio.run(ex())
    ```

#### `get_group_attributes_async(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> AttributesResponse`

!!! info "Method Description"
    Get unique list of analytic attributes per instrument for a dataset.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `instrument_id` | `Optional[str]` | `None` | Specific instrument ID filter |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `AttributesResponse`

!!! note "Sync Equivalent"
    `get_group_attributes(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> AttributesResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            attrs = await dq.get_group_attributes_async(group_id="GROUP_123")
            print(type(attrs))

    asyncio.run(ex())
    ```

#### `get_group_time_series_async(...) -> TimeSeriesResponse`

!!! info "Method Description"
    Retrieve time-series data across a subset of instruments and analytics for a group.

```python
async def get_group_time_series_async(
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
) -> TimeSeriesResponse
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Catalog data group identifier |
| `attributes` | `List[str]` | — | Attribute identifiers |
| `filter` | `Optional[str]` | `None` | Optional filter (e.g. currency(USD)) |
| `data` | `str` | `"REFERENCE_DATA"` | Data domain |
| `format` | `str` | `"JSON"` | Response format |
| `start_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `end_date` | `Optional[str]` | `None` | YYYYMMDD or TODAY-Nx |
| `calendar` | `str` | `"CAL_USBANK"` | Calendar convention |
| `frequency` | `str` | `"FREQ_DAY"` | Frequency convention |
| `conversion` | `str` | `"CONV_LASTBUS_ABS"` | Conversion convention |
| `nan_treatment` | `str` | `"NA_NOTHING"` | Missing data handling |
| `page` | `Optional[str]` | `None` | Pagination token |

**Returns:** `TimeSeriesResponse`

!!! note "Sync Equivalent"
    `get_group_time_series(group_id: str, attributes: List[str], ...) -> TimeSeriesResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            ts = await dq.get_group_time_series_async(
                group_id="GROUP_123", attributes=["PX_LAST"],
                start_date="20240101", end_date="20240131"
            )
            print(type(ts))

    asyncio.run(ex())
    ```

### :material-grid: Grid Data

#### `get_grid_data_async(expr: Optional[str] = None, grid_id: Optional[str] = None, date: Optional[str] = None) -> GridDataResponse`

!!! info "Method Description"
    Retrieve grid data using an expression or a grid ID (mutually exclusive).

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `expr` | `Optional[str]` | `None` | DataQuery expression |
| `grid_id` | `Optional[str]` | `None` | Grid identifier |
| `date` | `Optional[str]` | `None` | Date for grid data |

**Returns:** `GridDataResponse`

!!! warning "Mutually Exclusive"
    Either `expr` or `grid_id` must be provided, but not both.

!!! note "Sync Equivalent"
    `get_grid_data(expr: Optional[str] = None, grid_id: Optional[str] = None, date: Optional[str] = None) -> GridDataResponse`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            grid = await dq.get_grid_data_async(expr="PX_LAST(IBM,USD)")
            print(type(grid))

    asyncio.run(ex())
    ```

### :material-cog-play: Workflow Helpers

#### `run_groups_async(max_concurrent: int = 5) -> dict`

!!! info "Method Description"
    Lists groups and returns a comprehensive summary report.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_concurrent` | `int` | `5` | Maximum concurrent requests |

**Returns:** `dict` - Summary report with group statistics

!!! note "Sync Equivalent"
    `run_groups(max_concurrent: int = 5) -> dict`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            report = await dq.run_groups_async()
            print(report.get("total_groups"))

    asyncio.run(ex())
    ```

#### `run_group_files_async(group_id: str, max_concurrent: int = 5) -> dict`

!!! info "Method Description"
    Lists files for a group and returns a comprehensive summary report.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_id` | `str` | — | Group identifier |
| `max_concurrent` | `int` | `5` | Maximum concurrent requests |

**Returns:** `dict` - Summary report with file statistics

!!! note "Sync Equivalent"
    `run_group_files(group_id: str, max_concurrent: int = 5) -> dict`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            report = await dq.run_group_files_async(group_id="GROUP_123")
            print(report.get("total_files"))

    asyncio.run(ex())
    ```

#### `run_availability_async(file_group_id: str, file_datetime: str) -> dict`

!!! info "Method Description"
    Checks file availability and returns a comprehensive summary report.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_group_id` | `str` | — | File group identifier |
| `file_datetime` | `str` | — | File datetime (YYYYMMDD format) |

**Returns:** `dict` - Summary report with availability status

!!! note "Sync Equivalent"
    `run_availability(file_group_id: str, file_datetime: str) -> dict`

!!! example "Usage Example"
    ```python
    async def ex():
        async with DataQuery() as dq:
            report = await dq.run_availability_async(file_group_id="FILE_123", file_datetime="20250101")
            print(report.get("is_available"))

    asyncio.run(ex())
    ```

#### `run_download_async(...) -> dict`

!!! info "Method Description"
    Downloads a single file and returns a comprehensive summary report.

```python
async def run_download_async(
    file_group_id: str,
    file_datetime: Optional[str] = None,
    destination_path: Optional[Path] = None,
    max_concurrent: int = 1
) -> dict
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_group_id` | `str` | — | File group identifier |
| `file_datetime` | `Optional[str]` | `None` | File datetime (YYYYMMDD format) |
| `destination_path` | `Optional[Path]` | `None` | Download destination path |
| `max_concurrent` | `int` | `1` | Maximum concurrent requests |

**Returns:** `dict` - Summary report with download status

!!! note "Sync Equivalent"
    `run_download(file_group_id: str, ...) -> dict`

!!! example "Usage Example"
    ```python
    from pathlib import Path

    async def ex():
        async with DataQuery() as dq:
            report = await dq.run_download_async(
                file_group_id="FILE_123", file_datetime="20250101", destination_path=Path("./downloads")
            )
            print(report.get("download_successful"))

    asyncio.run(ex())
    ```

## Examples

### Basic File Download

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def main():
    async with DataQuery() as dq:
        result = await dq.download_file_async(
            file_group_id="FILE_123",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

asyncio.run(main())
```

### Group Download

```python
async def group_download():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads"),
            max_concurrent=5,
            num_parts=5
        )
        print(f"Downloaded {report['successful_downloads']} files")

asyncio.run(group_download())
```

### Synchronous Usage

```python
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
