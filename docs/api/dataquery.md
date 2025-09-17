# DataQuery API

The main `DataQuery` class provides a high-level interface for data querying and file downloads.

## Overview

The `DataQuery` class is the primary interface for interacting with the DataQuery API. It provides convenient methods for file downloads, group operations, and data querying.

## Key Features

- **High-Level Interface**: Easy-to-use methods for common operations
- **Async Support**: Full async/await support for concurrent operations
- **Group Downloads**: Batch download files by date range
- **Progress Tracking**: Built-in progress callbacks
- **Error Handling**: Comprehensive error handling and retry logic

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

### Core Listing and Search

#### list_groups_async(limit: Optional[int] = None) -> List[Group]
- **Description**: List all available data groups (with optional limit).
- **Parameters**:
  - `limit` (int, optional): Max number of groups; `None` returns all.
- **Returns**: `List[Group]`
- **Sync equivalent**: `list_groups(limit: Optional[int] = None) -> List[Group]`

Example:
```python
import asyncio
from dataquery.dataquery import DataQuery

async def ex():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async(limit=10)
        print(len(groups))

asyncio.run(ex())
```

#### search_groups_async(keywords: str, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Group]
- **Description**: Search groups by keywords.
- **Parameters**:
  - `keywords` (str): Search terms
  - `limit` (int, optional): Max results
  - `offset` (int, optional): Results to skip
- **Returns**: `List[Group]`
- **Sync equivalent**: `search_groups(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        results = await dq.search_groups_async(keywords="credit", limit=5)
        print([g.name for g in results])

asyncio.run(ex())
```

#### list_files_async(group_id: str, file_group_id: Optional[str] = None) -> List[FileInfo]
- **Description**: List files within a group (optionally filter by file_group_id).
- **Parameters**:
  - `group_id` (str): Group identifier
  - `file_group_id` (str, optional): Specific file id filter
- **Returns**: `List[FileInfo]`
- **Sync equivalent**: `list_files(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        files = await dq.list_files_async(group_id="GROUP_123")
        print(len(files))

asyncio.run(ex())
```

### Availability and Health

#### check_availability_async(file_group_id: str, file_datetime: str) -> AvailabilityInfo
- **Description**: Check if a file is available for a specific datetime.
- **Parameters**:
  - `file_group_id` (str)
  - `file_datetime` (str): YYYYMMDD, YYYYMMDDTHHMM, or YYYYMMDDTHHMMSS
- **Returns**: `AvailabilityInfo`
- **Sync equivalent**: `check_availability(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        av = await dq.check_availability_async("FILE_123", "20250101")
        print(getattr(av, "is_available", False))

asyncio.run(ex())
```

#### list_available_files_async(group_id: str, file_group_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[dict]
- **Description**: List available files for a group in a date range.
- **Returns**: `List[dict]`
- **Sync equivalent**: `list_available_files(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        items = await dq.list_available_files_async(
            group_id="GROUP_123", start_date="20250101", end_date="20250131"
        )
        print(len(items))

asyncio.run(ex())
```

#### health_check_async() -> bool
- **Description**: Check if the API is healthy.
- **Returns**: `bool`
- **Sync equivalent**: `health_check()`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        ok = await dq.health_check_async()
        print(ok)

asyncio.run(ex())
```

### Downloads

#### download_file_async(file_group_id: str, file_datetime: Optional[str] = None, destination_path: Optional[Path] = None, options: Optional[DownloadOptions] = None, num_parts: int = 5, progress_callback: Optional[Callable] = None) -> DownloadResult
- **Description**: Download a single file using parallel HTTP range requests.
- **Parameters**:
  - `file_group_id` (str)
  - `file_datetime` (str, optional)
  - `destination_path` (Path, optional)
  - `options` (DownloadOptions, optional)
  - `num_parts` (int): Default 5
  - `progress_callback` (callable, optional)
- **Returns**: `DownloadResult`
- **Sync equivalent**: `download_file(...)`

Example:
```python
import asyncio
from pathlib import Path
from dataquery.dataquery import DataQuery

async def ex():
    async with DataQuery() as dq:
        res = await dq.download_file_async(
            file_group_id="FILE_123", file_datetime="20250101", destination_path=Path("./downloads")
        )
        print(res.status, res.local_path)

asyncio.run(ex())
```

#### run_group_download_async(group_id: str, start_date: str, end_date: str, destination_dir: Path = Path("./downloads"), max_concurrent: int = 5, num_parts: int = 5, progress_callback: Optional[Callable] = None, delay_between_downloads: float = 1.0) -> dict
- **Description**: Download all files in a group for a date range with flattened concurrency (total concurrent requests = `max_concurrent × num_parts`).
- **Parameters**:
  - `group_id` (str)
  - `start_date` (str): YYYYMMDD
  - `end_date` (str): YYYYMMDD
  - `destination_dir` (Path): Default `./downloads`
  - `max_concurrent` (int): Default 5
  - `num_parts` (int): Default 5
  - `progress_callback` (callable, optional)
  - `delay_between_downloads` (float): Default 1.0s
- **Returns**: `dict` report
- **Sync equivalent**: `run_group_download(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        report = await dq.run_group_download_async(
            group_id="GROUP_123", start_date="20250101", end_date="20250131"
        )
        print(report.get("successful_downloads"))

asyncio.run(ex())
```

### Instruments and Time Series

#### list_instruments_async(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> InstrumentsResponse
- **Description**: List instruments and identifiers for a dataset.
- **Sync equivalent**: `list_instruments(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        resp = await dq.list_instruments_async(group_id="GROUP_123")
        print(getattr(resp, "count", None))

asyncio.run(ex())
```

#### search_instruments_async(group_id: str, keywords: str, page: Optional[str] = None) -> InstrumentsResponse
- **Description**: Search instruments within a dataset.
- **Sync equivalent**: `search_instruments(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        resp = await dq.search_instruments_async(group_id="GROUP_123", keywords="usd")
        print(getattr(resp, "count", None))

asyncio.run(ex())
```

#### get_instrument_time_series_async(instruments: List[str], attributes: List[str], data: str = "REFERENCE_DATA", format: str = "JSON", start_date: Optional[str] = None, end_date: Optional[str] = None, calendar: str = "CAL_USBANK", frequency: str = "FREQ_DAY", conversion: str = "CONV_LASTBUS_ABS", nan_treatment: str = "NA_NOTHING", page: Optional[str] = None) -> TimeSeriesResponse
- **Description**: Time-series for explicit instruments and attributes.
- **Sync equivalent**: `get_instrument_time_series(...)`

Signature:
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

Parameters:

| Parameter     | Type            | Default            | Description                         |
|---------------|-----------------|--------------------|-------------------------------------|
| instruments   | List[str]       | —                  | Instrument identifiers              |
| attributes    | List[str]       | —                  | Attribute identifiers               |
| data          | str             | "REFERENCE_DATA"   | Data domain                         |
| format        | str             | "JSON"             | Response format                     |
| start_date    | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| end_date      | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| calendar      | str             | "CAL_USBANK"       | Calendar convention                 |
| frequency     | str             | "FREQ_DAY"         | Frequency convention                |
| conversion    | str             | "CONV_LASTBUS_ABS" | Conversion convention               |
| nan_treatment | str             | "NA_NOTHING"       | Missing data handling               |
| page          | Optional[str]   | None               | Pagination token                    |

Example:
```python
async def ex():
    async with DataQuery() as dq:
        ts = await dq.get_instrument_time_series_async(
            instruments=["US912828U816"], attributes=["PX_LAST"], start_date="20240101", end_date="20240131"
        )
        print(type(ts))

asyncio.run(ex())
```

#### get_expressions_time_series_async

Signature:
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
- **Parameters:**

| Parameter     | Type            | Default            | Description                         |
|---------------|-----------------|--------------------|-------------------------------------|
| expressions   | List[str]       | —                  | Traditional DataQuery expressions   |
| format        | str             | "JSON"             | Response format                     |
| start_date    | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| end_date      | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| calendar      | str             | "CAL_USBANK"       | Calendar convention                 |
| frequency     | str             | "FREQ_DAY"         | Frequency convention                |
| conversion    | str             | "CONV_LASTBUS_ABS" | Conversion convention               |
| nan_treatment | str             | "NA_NOTHING"       | Missing data handling               |
| data          | str             | "REFERENCE_DATA"   | Data domain                         |
| page          | Optional[str]   | None               | Pagination token                    |
- **Description**: Time-series for traditional DataQuery expressions.
- **Sync equivalent**: `get_expressions_time_series(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        ts = await dq.get_expressions_time_series_async(
            expressions=["PX_LAST(IBM,USD)"], start_date="20240101", end_date="20240131"
        )
        print(type(ts))

asyncio.run(ex())
```

#### get_group_filters_async(group_id: str, page: Optional[str] = None) -> FiltersResponse
- **Description**: Unique list of filter dimensions for a dataset.
- **Sync equivalent**: `get_group_filters(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        filters = await dq.get_group_filters_async(group_id="GROUP_123")
        print(type(filters))

asyncio.run(ex())
```

#### get_group_attributes_async(group_id: str, instrument_id: Optional[str] = None, page: Optional[str] = None) -> AttributesResponse
- **Description**: Unique list of analytic attributes per instrument for a dataset.
- **Sync equivalent**: `get_group_attributes(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        attrs = await dq.get_group_attributes_async(group_id="GROUP_123")
        print(type(attrs))

asyncio.run(ex())
```

#### get_group_time_series_async(group_id: str, attributes: List[str], filter: Optional[str] = None, data: str = "REFERENCE_DATA", format: str = "JSON", start_date: Optional[str] = None, end_date: Optional[str] = None, calendar: str = "CAL_USBANK", frequency: str = "FREQ_DAY", conversion: str = "CONV_LASTBUS_ABS", nan_treatment: str = "NA_NOTHING", page: Optional[str] = None) -> TimeSeriesResponse
- **Description**: Time-series across a subset of instruments and analytics.
- **Sync equivalent**: `get_group_time_series(...)`

Signature:
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

Parameters:

| Parameter     | Type            | Default            | Description                         |
|---------------|-----------------|--------------------|-------------------------------------|
| group_id      | str             | —                  | Catalog data group identifier       |
| attributes    | List[str]       | —                  | Attribute identifiers               |
| filter        | Optional[str]   | None               | Optional filter (e.g. currency(USD))|
| data          | str             | "REFERENCE_DATA"   | Data domain                         |
| format        | str             | "JSON"             | Response format                     |
| start_date    | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| end_date      | Optional[str]   | None               | YYYYMMDD or TODAY-Nx                |
| calendar      | str             | "CAL_USBANK"       | Calendar convention                 |
| frequency     | str             | "FREQ_DAY"         | Frequency convention                |
| conversion    | str             | "CONV_LASTBUS_ABS" | Conversion convention               |
| nan_treatment | str             | "NA_NOTHING"       | Missing data handling               |
| page          | Optional[str]   | None               | Pagination token                    |

Example:
```python
async def ex():
    async with DataQuery() as dq:
        ts = await dq.get_group_time_series_async(
            group_id="GROUP_123", attributes=["PX_LAST"], start_date="20240101", end_date="20240131"
        )
        print(type(ts))

asyncio.run(ex())
```

### Grid Data

#### get_grid_data_async(expr: Optional[str] = None, grid_id: Optional[str] = None, date: Optional[str] = None) -> GridDataResponse
- **Description**: Retrieve grid data using an expression or a grid ID (mutually exclusive).
- **Returns**: `GridDataResponse`
- **Sync equivalent**: `get_grid_data(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        grid = await dq.get_grid_data_async(expr="PX_LAST(IBM,USD)")
        print(type(grid))

asyncio.run(ex())
```

### Workflow Helpers

#### run_groups_async(max_concurrent: int = 5) -> dict
- **Description**: Lists groups and returns a summary report.
- **Sync equivalent**: `run_groups(max_concurrent: int = 5)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        report = await dq.run_groups_async()
        print(report.get("total_groups"))

asyncio.run(ex())
```

#### run_group_files_async(group_id: str, max_concurrent: int = 5) -> dict
- **Description**: Lists files for a group and returns a summary report.
- **Sync equivalent**: `run_group_files(group_id: str, max_concurrent: int = 5)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        report = await dq.run_group_files_async(group_id="GROUP_123")
        print(report.get("total_files"))

asyncio.run(ex())
```

#### run_availability_async(file_group_id: str, file_datetime: str) -> dict
- **Description**: Checks availability and returns a summary report.
- **Sync equivalent**: `run_availability(...)`

Example:
```python
async def ex():
    async with DataQuery() as dq:
        report = await dq.run_availability_async(file_group_id="FILE_123", file_datetime="20250101")
        print(report.get("is_available"))

asyncio.run(ex())
```

#### run_download_async(file_group_id: str, file_datetime: Optional[str] = None, destination_path: Optional[Path] = None, max_concurrent: int = 1) -> dict
- **Description**: Downloads a single file and returns a summary report.
- **Sync equivalent**: `run_download(...)`

Example:
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
