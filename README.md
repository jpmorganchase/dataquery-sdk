# DataQuery SDK

Python SDK for J.P. Morgan DataQuery API - high-performance file downloads and time series data access.

**Key capabilities:**
- Parallel file downloads with progress tracking
- Time series queries (expressions, instruments, groups)
- OAuth 2.0 authentication with auto-refresh
- Connection pooling and rate limiting
- Pandas DataFrame integration

## Table of Contents

- [Quick Start](#quick-start)
    - [Installation](#installation)
    - [Set Credentials](#set-credentials)
    - [Execution Modes](#execution-modes-async-and-sync)
    - [Download Files](#download-jpmaqs-files-simplest)
    - [Query Time Series](#query-time-series-and-convert-to-dataframe)
    - [Discover Datasets](#discover-available-datasets)
- [Common Use Cases](#common-use-cases)
    - [File Downloads](#file-downloads)
    - [Time Series Queries](#time-series-queries)
- [Advanced Usage](#advanced-usage)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Examples & Recipes](#examples--recipes)

---

## Quick Start

### Installation

```bash
pip install dataquery-sdk
```

### Set Credentials

```bash
export DATAQUERY_CLIENT_ID="your_client_id"
export DATAQUERY_CLIENT_SECRET="your_client_secret"
```

### Execution Modes: Async and Sync

**Python scripts** can use either async or sync methods:
- **Sync methods** (simpler): Use `with DataQuery()` and call methods without `await`
- **Async methods** (requires `asyncio.run()`): Use `async with DataQuery()` and `await` method calls

**Jupyter notebooks** must use async methods with `await` (event loop already running)

> Performance is identical - sync methods are lightweight wrappers around async implementations.

---

### Download JPMAQS Files (Simplest)

**Jupyter Notebooks:**
```python
from dataquery import DataQuery

# Download all files for January 2025
async with DataQuery() as dq:
    await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data"
    )
```

**Python Scripts (Sync):**
```python
from dataquery import DataQuery

# Download all files for January 2025
with DataQuery() as dq:
    dq.run_group_download(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data"
    )
```

**Python Scripts (Async):**
```python
import asyncio
from dataquery import DataQuery

async def main():
    async with DataQuery() as dq:
        await dq.run_group_download_async(
            group_id="JPMAQS_GENERIC_RETURNS",
            start_date="20250101",
            end_date="20250131",
            destination_dir="./data"
        )

asyncio.run(main())
```

All JPMAQS Generic Returns files for January are now in `./data/`.

### Query Time Series and Convert to DataFrame

**Jupyter Notebooks:**
```python
from dataquery import DataQuery

# Get time series data and convert to pandas DataFrame
async with DataQuery() as dq:
    result = await dq.get_expressions_time_series_async(
        expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
        start_date="20240101",
        end_date="20240131"
    )

    # Convert to DataFrame for analysis
    df = dq.time_series_to_dataframe(result)
    print(df.head())
```

**Python Scripts:**
```python
import asyncio
from dataquery import DataQuery

async def main():
    async with DataQuery() as dq:
        result = await dq.get_expressions_time_series_async(
            expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
            start_date="20240101",
            end_date="20240131"
        )
        df = dq.time_series_to_dataframe(result)
        return df

df = asyncio.run(main())
print(df.head())
```

### Discover Available Datasets

**Jupyter Notebooks:**
```python
from dataquery import DataQuery

# List all available groups and convert to DataFrame
async with DataQuery() as dq:
    groups = await dq.list_groups_async(limit=100)

    # Convert to DataFrame - generic method works with any response
    df = dq.to_dataframe(groups)
    print(df[['group_id', 'group_name', 'description']].head())
```

**Python Scripts:**
```python
from dataquery import DataQuery

# Sync version
with DataQuery() as dq:
    groups = dq.list_groups(limit=100)

    # Convert to DataFrame - generic method works with any response
    df = dq.to_dataframe(groups)
    print(df[['group_id', 'group_name', 'description']].head())
```

---

## Common Use Cases

### File Downloads

#### Download All Files for Date Range (Recommended)

```python
# Jupyter notebooks - use async/await
async with DataQuery() as dq:
    results = await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data",
        max_concurrent=3  # Download 3 files at once
    )
    print(f"Downloaded {results['successful']} files")
```

```python
# Python scripts - use sync methods
with DataQuery() as dq:
    results = dq.run_group_download(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data"
    )
```

```python
# Python scripts - async requires asyncio.run()
import asyncio
from dataquery import DataQuery

async def download():
    async with DataQuery() as dq:
        return await dq.run_group_download_async(
            group_id="JPMAQS_GENERIC_RETURNS",
            start_date="20250101",
            end_date="20250131",
            destination_dir="./data",
            max_concurrent=3
        )

results = asyncio.run(download())
```

**When to use:** You want all files for a file group within a date range.

#### Download Single File

```python
async with DataQuery() as dq:
    result = await dq.download_file_async(
        file_group_id="JPMAQS_GENERIC_RETURNS",
        file_datetime="20250115",  # Specific date
        destination_path="./downloads"
    )
    print(f"Downloaded: {result.local_path}")
```

**When to use:** You need just one specific file.

---

### Time Series Queries

#### Query by Expression (Fastest for known data)

```python
async with DataQuery() as dq:
    result = await dq.get_expressions_time_series_async(
        expressions=[
            "DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)",
            "DB(MTE,IRISH EUR 2.400 15-May-2030 LON,,IE00BJ38CR43,MIDPRC)"
        ],
        start_date="20240101",
        end_date="20240131"
    )
```

**When to use:** You know the exact expression syntax (from DataQuery Web or documentation).

#### Query by Instrument ID

```python
async with DataQuery() as dq:
    result = await dq.get_instrument_time_series_async(
        instruments=[
            "477f892d3cc8745578887a92d35c2a3e-DQGNMTBNDFIM",
            "67e1bfca56bdee7a0fd5fe4c62a1e0dc-DQGNMTBNDFIM"
        ],
        attributes=["MIDPRC", "REPO_1M"],
        start_date="20240101",
        end_date="20240131"
    )
```

**When to use:** You have instrument IDs from a previous search or list.

#### Query Entire Group with Filter

```python
async with DataQuery() as dq:
    result = await dq.get_group_time_series_async(
        group_id="FI_GO_BO_EA",
        attributes=["MIDPRC", "REPO_1M"],
        filter="country(IRL)",  # Ireland bonds only
        start_date="20240101",
        end_date="20240131"
    )
```

**When to use:** You want all instruments in a dataset, optionally filtered by country/currency.

#### Search for Instruments

```python
async with DataQuery() as dq:
    # Find instruments matching keywords
    results = await dq.search_instruments_async(
        group_id="FI_GO_BO_EA",
        keywords="irish"
    )
    print(f"Found {results.items} instruments")

    # Use the results to query time series
    instrument_ids = [inst.instrument_id for inst in results.instruments]
    data = await dq.get_instrument_time_series_async(
        instruments=instrument_ids[:5],  # First 5 results
        attributes=["MIDPRC"],
        start_date="20240101",
        end_date="20240131"
    )
```

**When to use:** You're discovering data and don't know instrument IDs yet.

---

## Advanced Usage

### High-Performance File Downloads

Use parallel HTTP range requests to download large files faster:

```python
async with DataQuery() as dq:
    result = await dq.download_file_async(
        file_group_id="JPMAQS_GENERIC_RETURNS",
        file_datetime="20250115",
        destination_path="./downloads",
        num_parts=8,  # Split into 8 parallel chunks
        progress_callback=lambda fid, p: print(f"{p.bytes_downloaded:,} bytes")
    )
```

**Performance tuning:**
```python
# Download multiple files concurrently
await dq.run_group_download_async(
    group_id="JPMAQS_GENERIC_RETURNS",
    start_date="20250101",
    end_date="20250131",
    destination_dir="./data",
    max_concurrent=5,  # 5 files at once
    num_parts=4        # Each file in 4 chunks = 20 total parallel requests
)
```

**Optimal settings:**
- `num_parts`: 2-8 (higher = faster on good connections)
- `max_concurrent`: 3-5 (too high may hit rate limits)

**Note:** Jupyter notebooks use `await` directly. Python scripts use sync methods or wrap async code in `asyncio.run()`.

### Grid Data (Premium Datasets)

```python
async with DataQuery() as dq:
    # Query grid data by expression
    grid = await dq.get_grid_data_async(
        expr="DBGRID(FXOVOL,FXO,CW,AUD,USD)",
        date="20240216"
    )
```

### DataFrame Conversion

The SDK provides a universal `to_dataframe()` method that automatically handles any API response type:

```python
async with DataQuery() as dq:
    # Generic to_dataframe() works with all response types
    groups = await dq.list_groups_async(limit=100)
    groups_df = dq.to_dataframe(groups)

    # Time series
    ts_data = await dq.get_expressions_time_series_async(
        expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
        start_date="20240101",
        end_date="20240131"
    )
    ts_df = dq.to_dataframe(ts_data)

    # Instruments
    instruments = await dq.list_instruments_async(group_id="FI_GO_BO_EA")
    instruments_df = dq.to_dataframe(instruments.instruments)

    # Files
    files = await dq.list_available_files_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131"
    )
    files_df = dq.to_dataframe(files)
```

**Advanced options:**
```python
# Include metadata fields
df = dq.to_dataframe(groups, include_metadata=True)

# Parse specific columns as dates
df = dq.to_dataframe(files, date_columns=['last_modified', 'created_date'])

# Convert specific columns to numeric
df = dq.to_dataframe(files, numeric_columns=['file_size'])

# Apply custom transformations
df = dq.to_dataframe(
    data,
    custom_transformations={
        'price': lambda x: float(x) if x else 0.0
    }
)
```

**Type-specific convenience methods** (same functionality as generic method):
- `groups_to_dataframe(groups, include_metadata=False)`
- `time_series_to_dataframe(time_series, include_metadata=False)`
- `instruments_to_dataframe(instruments, include_metadata=False)`
- `files_to_dataframe(files, include_metadata=False)`

---

## API Reference

### File Download Methods

#### `download_file_async()`

Download a single file with optional parallel chunks.

```python
result = await dq.download_file_async(
    file_group_id: str,              # Required: File group identifier
    file_datetime: str,              # Required: File date (YYYY-MM-DD)
    destination_path: Path,          # Required: Where to save
    num_parts: int = 1,              # Optional: Parallel chunks (1-10)
    part_size_mb: int = 100,         # Optional: Chunk size in MB
    progress_callback: Callable = None,  # Optional: Progress updates
    overwrite: bool = False          # Optional: Overwrite existing
) -> DownloadResult
```

**Returns:** `DownloadResult` with status, file size, download time, and local path.

#### `download_files_by_date_range_async()`

Download all files in a date range.

```python
results = await dq.download_files_by_date_range_async(
    file_group_id: str,              # Required: File group identifier
    start_date: str,                 # Required: Start date (YYYY-MM-DD)
    end_date: str,                   # Required: End date (YYYY-MM-DD)
    destination_path: Path,          # Required: Where to save
    num_parts: int = 1,              # Optional: Parallel chunks per file
    max_concurrent: int = 3,         # Optional: Concurrent downloads
    progress_callback: Callable = None  # Optional: Progress updates
) -> List[DownloadResult]
```

**Returns:** List of `DownloadResult` for each file.

#### `list_available_files_async()`

Check what files are available for a group and date range.

```python
files = await dq.list_available_files_async(
    group_id: str,                   # Required: Group identifier
    start_date: str,                 # Required: Start date (YYYY-MM-DD)
    end_date: str                    # Required: End date (YYYY-MM-DD)
) -> List[dict]
```

**Returns:** List of dictionaries with `file_group_id`, `file_datetime`, and `file_size`.

---

### Time Series Query Methods

#### `get_expressions_time_series_async()`

Query using DataQuery expressions (fastest when you know the expression).

**Minimal usage:**
```python
result = await dq.get_expressions_time_series_async(
    expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
    start_date="20240101",
    end_date="20240131"
)
```

**Complete signature:**
```python
result = await dq.get_expressions_time_series_async(
    expressions: List[str],          # Required: List of expressions (max 20)
    format: str = "JSON",            # Optional: Response format
    start_date: str = None,          # Optional: YYYYMMDD or TODAY-1M
    end_date: str = None,            # Optional: YYYYMMDD or TODAY
    calendar: str = "CAL_USBANK",    # Optional: Calendar convention
    frequency: str = "FREQ_DAY",     # Optional: FREQ_DAY, FREQ_WEEK, etc.
    conversion: str = "CONV_LASTBUS_ABS",  # Optional: Conversion method
    nan_treatment: str = "NA_NOTHING",     # Optional: NA_NOTHING, NA_LAST, etc.
    data: str = "REFERENCE_DATA",    # Optional: Data domain
    page: str = None                 # Optional: Pagination token
) -> TimeSeriesResponse
```

**Recommended parameters:**
- `calendar="CAL_WEEKDAYS"` - For international coverage
- `data="ALL"` - To include market data

---

#### `get_instrument_time_series_async()`

Query specific instruments by their IDs.

**Minimal usage:**
```python
result = await dq.get_instrument_time_series_async(
    instruments=["477f892d3cc8745578887a92d35c2a3e-DQGNMTBNDFIM"],
    attributes=["MIDPRC"],
    start_date="20240101",
    end_date="20240131"
)
```

**Complete signature:**
```python
result = await dq.get_instrument_time_series_async(
    instruments: List[str],          # Required: List of instrument IDs (max 20)
    attributes: List[str],           # Required: List of attributes
    data: str = "REFERENCE_DATA",    # Optional: Data domain
    format: str = "JSON",            # Optional: Response format
    start_date: str = None,          # Optional: YYYYMMDD or TODAY-2W
    end_date: str = None,            # Optional: YYYYMMDD or TODAY
    calendar: str = "CAL_USBANK",    # Optional: Calendar convention
    frequency: str = "FREQ_DAY",     # Optional: Frequency
    conversion: str = "CONV_LASTBUS_ABS",  # Optional: Conversion
    nan_treatment: str = "NA_NOTHING",     # Optional: Missing data
    page: str = None                 # Optional: Pagination token
) -> TimeSeriesResponse
```

**When to use:** You have instrument IDs from search or list operations.

---

#### `get_group_time_series_async()`

Query all instruments in a group, optionally with filters.

**Minimal usage:**
```python
result = await dq.get_group_time_series_async(
    group_id="FI_GO_BO_EA",
    attributes=["MIDPRC"],
    start_date="20240101",
    end_date="20240131"
)
```

**With filter:**
```python
result = await dq.get_group_time_series_async(
    group_id="FI_GO_BO_EA",
    attributes=["MIDPRC", "REPO_1M"],
    filter="country(IRL)",           # Ireland bonds only
    start_date="20240101",
    end_date="20240131",
    calendar="CAL_WEEKDAYS"
)
```

**Complete signature:**
```python
result = await dq.get_group_time_series_async(
    group_id: str,                   # Required: Group identifier
    attributes: List[str],           # Required: List of attributes
    filter: str = None,              # Optional: Filter expression
    data: str = "REFERENCE_DATA",    # Optional: Data domain
    format: str = "JSON",            # Optional: Response format
    start_date: str = None,          # Optional: YYYYMMDD or TODAY-1M
    end_date: str = None,            # Optional: YYYYMMDD or TODAY
    calendar: str = "CAL_USBANK",    # Optional: Calendar
    frequency: str = "FREQ_DAY",     # Optional: Frequency
    conversion: str = "CONV_LASTBUS_ABS",  # Optional: Conversion
    nan_treatment: str = "NA_NOTHING",     # Optional: Missing data
    page: str = None                 # Optional: Pagination
) -> TimeSeriesResponse
```

**Available filters:** Check with `get_group_filters_async()` - typically `country(CODE)` or `currency(CODE)`.

---

### Discovery Methods

#### `search_instruments_async()`

Search for instruments within a group by keywords.

```python
results = await dq.search_instruments_async(
    group_id: str,                   # Required: Group to search
    keywords: str,                   # Required: Search terms
    page: str = None                 # Optional: Pagination token
) -> InstrumentsResponse
```

#### `list_instruments_async()`

List all instruments in a group (paginated).

```python
instruments = await dq.list_instruments_async(
    group_id: str,                   # Required: Group identifier
    instrument_id: str = None,       # Optional: Specific instrument
    page: str = None                 # Optional: Pagination token
) -> InstrumentsResponse
```

#### `list_groups_async()`

List available data groups/datasets.

```python
groups = await dq.list_groups_async(
    limit: int = 100                 # Optional: Results per page (100-1000)
) -> List[Group]
```

**Note:** Minimum limit is 100 per API specification.

#### `get_group_attributes_async()`

Get available attributes for a group.

```python
attrs = await dq.get_group_attributes_async(
    group_id: str,                   # Required: Group identifier
    instrument_id: str = None,       # Optional: Specific instrument
    page: str = None                 # Optional: Pagination token
) -> AttributesResponse
```

#### `get_group_filters_async()`

Get available filter dimensions for a group.

```python
filters = await dq.get_group_filters_async(
    group_id: str,                   # Required: Group identifier
    page: str = None                 # Optional: Pagination token
) -> FiltersResponse
```

---

## Configuration

### Environment Variables

```bash
# Required
export DATAQUERY_CLIENT_ID="your_client_id"
export DATAQUERY_CLIENT_SECRET="your_client_secret"

# Optional - API endpoints (defaults shown)
export DATAQUERY_BASE_URL="https://api-developer.jpmorgan.com"
export DATAQUERY_CONTEXT_PATH="/research/dataquery-authe/api/v2"
export DATAQUERY_FILES_BASE_URL="https://api-strm-gw01.jpmchase.com"

# Optional - OAuth
export DATAQUERY_OAUTH_TOKEN_URL="https://authe.jpmorgan.com/as/token.oauth2"
export DATAQUERY_OAUTH_RESOURCE_ID="JPMC:URI:RS-06785-DataQueryExternalApi-PROD"

# Optional - Performance
export DATAQUERY_MAX_RETRIES="3"
export DATAQUERY_TIMEOUT="60"
export DATAQUERY_RATE_LIMIT_RPM="300"  # Requests per minute
```

### Programmatic Configuration

```python
from dataquery import DataQuery, ClientConfig

config = ClientConfig(
    client_id="your_client_id",
    client_secret="your_client_secret",
    max_retries=3,
    timeout=60.0,
    rate_limit_rpm=300
)

async with DataQuery(config=config) as dq:
    # Your code here
    pass
```

### Performance Tuning

**For file downloads:**
```python
# Optimal settings for most use cases
num_parts=4                # 2-8 parallel chunks per file
part_size_mb=50           # 25-100 MB chunks
max_concurrent=3          # 1-5 concurrent file downloads
```

**For time series:**
```python
# Recommended date range
start_date="TODAY-1Y"     # Max 1 year per request
calendar="CAL_WEEKDAYS"   # International coverage
data="ALL"                # Include market data
```

---

## Date and Calendar Reference

### Date Formats

**Absolute dates:**
```python
start_date="20240101"     # YYYYMMDD format
end_date="20241231"
```

**Relative dates:**
```python
start_date="TODAY"        # Today
start_date="TODAY-1D"     # Yesterday
start_date="TODAY-1W"     # 1 week ago
start_date="TODAY-1M"     # 1 month ago
start_date="TODAY-1Y"     # 1 year ago
```

### Calendar Conventions

Choose based on your use case:

| Calendar | Description | Use Case |
|----------|-------------|----------|
| `CAL_WEEKDAYS` | Monday-Friday (recommended for international) | Multi-country data |
| `CAL_USBANK` | US banking days (default) | US-only data |
| `CAL_WEEKDAY_NOHOLIDAY` | All weekdays | Generic business days |
| `CAL_DEFAULT` | Calendar day | Include weekends |

**30+ calendars supported.** See API documentation for complete list.

### Frequency Conventions

| Frequency | Description |
|-----------|-------------|
| `FREQ_DAY` | Daily (default) |
| `FREQ_WEEK` | Weekly |
| `FREQ_MONTH` | Monthly |
| `FREQ_QUARTER` | Quarterly |
| `FREQ_YEAR` | Annual |

---

## Error Handling

### Common Patterns

```python
from dataquery import DataQuery, DataQueryError

async def safe_query():
    try:
        async with DataQuery() as dq:
            result = await dq.get_expressions_time_series_async(
                expressions=["DB(...)"],
                start_date="20240101",
                end_date="20240131"
            )
            return result
    except DataQueryError as e:
        print(f"DataQuery API error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
```

### Check Health

```python
async with DataQuery() as dq:
    if await dq.health_check_async():
        print("API is healthy")
    else:
        print("API health check failed")
```

### Monitor Statistics

```python
async with DataQuery() as dq:
    # Make some requests...
    await dq.list_groups_async(limit=100)

    # Get statistics
    stats = dq.get_stats()
    print(f"Total requests: {stats.get('total_requests', 0)}")
    print(f"Success rate: {stats.get('success_rate', 0):.1f}%")
```

---

## Examples & Recipes

### Download and Process JPMAQS Data

```python
import asyncio
from dataquery import DataQuery
from pathlib import Path

async def download_jpmaqs_month():
    """Download and process a month of JPMAQS data"""
    async with DataQuery() as dq:
        results = await dq.download_files_by_date_range_async(
            file_group_id="JPMAQS",
            start_date="2025-01-01",
            end_date="2025-01-31",
            destination_path="./jpmaqs_data",
            num_parts=4
        )

        for result in results:
            if result.status.value == "completed":
                print(f"Downloaded {result.local_path.name}: {result.file_size:,} bytes")
                # Process the file here
            else:
                print(f"Failed {result.file_id}: {result.error_message}")

asyncio.run(download_jpmaqs_month())
```

### Build Time Series for Portfolio

```python
async def get_portfolio_data(instrument_ids: list):
    """Get time series data for a portfolio of instruments"""
    async with DataQuery() as dq:
        # Get 1 year of daily data
        ts_data = await dq.get_instrument_time_series_async(
            instruments=instrument_ids,
            attributes=["MIDPRC", "REPO_1M"],
            start_date="TODAY-1Y",
            end_date="TODAY",
            calendar="CAL_WEEKDAYS",
            data="ALL"
        )

        # Convert to DataFrame for analysis
        df = dq.time_series_to_dataframe(ts_data)
        return df

# Usage
portfolio_ids = [
    "477f892d3cc8745578887a92d35c2a3e-DQGNMTBNDFIM",
    "67e1bfca56bdee7a0fd5fe4c62a1e0dc-DQGNMTBNDFIM"
]
df = asyncio.run(get_portfolio_data(portfolio_ids))
print(df.head())
```

### Discover and Query New Dataset

```python
async def explore_dataset(group_id: str, keywords: str):
    """Explore a dataset and get sample data"""
    async with DataQuery() as dq:
        # Search for instruments
        search_results = await dq.search_instruments_async(
            group_id=group_id,
            keywords=keywords
        )
        print(f"Found {search_results.items} instruments matching '{keywords}'")

        # Get first 5 instrument IDs
        instrument_ids = [inst.instrument_id for inst in search_results.instruments[:5]]

        # Get available attributes
        attrs = await dq.get_group_attributes_async(group_id=group_id)
        print(f"Available attributes: {len(attrs.instruments)} instruments")

        # Query sample data
        ts_data = await dq.get_instrument_time_series_async(
            instruments=instrument_ids,
            attributes=["MIDPRC"],  # Common attribute
            start_date="TODAY-1W",
            end_date="TODAY",
            calendar="CAL_WEEKDAYS"
        )

        return ts_data

# Usage
data = asyncio.run(explore_dataset("FI_GO_BO_EA", "irish"))
```

---

## Requirements

- **Python 3.10+** (Python 3.11+ recommended for better performance)
- **Dependencies:** aiohttp, pydantic, structlog (auto-installed)
- **Optional:** pandas (for DataFrame conversion)

---

## Support

For issues, questions, or feature requests, contact DataQuery support at DataQuery_Support@jpmorgan.com

---

## License

See LICENSE file for details.
