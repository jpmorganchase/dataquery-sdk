# 🚀 DataQuery SDK

**High-performance Python SDK for efficient data querying and file downloads with parallel processing capabilities.**

A comprehensive SDK that provides **two powerful capabilities**: lightning-fast file delivery and advanced time series data access. Get up and running in minutes with intelligent defaults and enterprise-grade performance.

---

## ✨ Key Features

- 🚀 **Lightning-Fast File Delivery** — 5x faster downloads with parallel HTTP range requests
- 📊 **Advanced Time Series APIs** — Query instruments, expressions, and grid data
- 🔄 **Intelligent Rate Limiting** — Never overwhelm servers with built-in delays
- 🛡️ **Robust Error Handling** — Automatic retries and graceful failure recovery
- 🎯 **Progress Tracking** — Real-time download progress with callbacks
- 🔧 **Smart Defaults** — Works out-of-the-box with pre-configured JPMorgan endpoints
- 🌐 **Dual API** — Full async/await support with convenient sync wrappers
- 📚 **Comprehensive Docs** — Professional documentation with MkDocs

---

## 📋 Requirements

- **Python 3.10+**
- Internet connection for API access

---

## ⚡ Quick Installation

```bash
pip install dataquery-sdk
```

Using uv (recommended):
```bash
uv pip install dataquery-sdk
```

---

## 🎯 Quick Start (2 minutes)

### Step 1: Set Your Credentials

With the SDK's smart defaults, you only need **2 environment variables**:

```bash
export DATAQUERY_CLIENT_ID="your_client_id_here"
export DATAQUERY_CLIENT_SECRET="your_client_secret_here"
```

> 💡 **That's it!** All JPMorgan DataQuery API endpoints are pre-configured as defaults.

### Step 2: Test Your Setup

```python
import asyncio
from dataquery import DataQuery

async def test_connection():
    async with DataQuery() as dq:
        healthy = await dq.health_check_async()
        print("✅ Connection successful!" if healthy else "❌ Connection failed")

asyncio.run(test_connection())
```

---

# 📁 File Delivery

**Download financial data files with maximum speed and reliability using parallel processing.**

## 🚀 Core File Delivery Features

- **Parallel Processing**: Download files 5x faster with HTTP range requests
- **Batch Operations**: Download multiple files by date range
- **Smart Concurrency**: Configurable parallel downloads with rate limiting
- **Progress Tracking**: Real-time progress callbacks
- **Error Recovery**: Automatic retries and resume capabilities

---

## 📊 File Delivery Examples

### 🏦 Single File Download

```python
import asyncio
from pathlib import Path
from dataquery import DataQuery

async def download_single_file():
    async with DataQuery() as dq:
        # Download a single file with parallel processing
        result = await dq.download_file_async(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )

        if result.status.value == "completed":
            print(f"🎉 Success! Downloaded {result.file_size:,} bytes")
            print(f"📁 Saved to: {result.local_path}")
            print(f"⚡ Speed: {result.download_time:.1f}s")
        else:
            print(f"❌ Failed: {result.error_message}")

asyncio.run(download_single_file())
```

### 📈 Batch Download with Progress

```python
import asyncio
from pathlib import Path
from dataquery import DataQuery
from dataquery.models import DownloadProgress

def show_progress(progress: DownloadProgress):
    """Display real-time download progress"""
    if progress.total_bytes:
        pct = progress.percentage
        mb_downloaded = progress.bytes_downloaded / (1024 * 1024)
        mb_total = progress.total_bytes / (1024 * 1024)
        print(f"\r📥 {progress.file_group_id}: {pct:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)",
              end="", flush=True)

async def batch_download():
    async with DataQuery() as dq:
        # Download all files for January 2025
        report = await dq.run_group_download_async(
            group_id="YOUR_GROUP_ID",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./downloads"),
            progress_callback=show_progress
        )

        print(f"""
📊 Download Complete!
   📁 Total files: {report['total_files']}
   ✅ Successful: {report['successful_downloads']}
   ❌ Failed: {report['failed_downloads']}
   📈 Success rate: {report['success_rate']:.1f}%
   ⏱️  Total time: {report['total_time_formatted']}
   🚀 Avg speed: {report.get('avg_speed_mbps', 0):.1f} MB/s
        """)

asyncio.run(batch_download())
```

### 🔍 File Discovery

```python
async def discover_files():
    async with DataQuery() as dq:
        # Find available data groups
        groups = await dq.list_groups_async(limit=10)
        print("📊 Available Data Groups:")
        for i, group in enumerate(groups, 1):
            print(f"   {i}. {group.group_id}")

        # Check what files are available
        if groups:
            files = await dq.list_available_files_async(
                group_id=groups[0].group_id,
                start_date="20250101",
                end_date="20250107"
            )
            print(f"📁 Available files this week: {len(files)}")

        # Check specific file availability
        availability = await dq.check_availability_async(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101"
        )

        if availability and getattr(availability, 'is_available', False):
            print("✅ File is available for download")
        else:
            print("❌ File not available for this date")

asyncio.run(discover_files())
```

### 📋 File Availability APIs

```python
async def check_file_availability():
    async with DataQuery() as dq:
        # Check if a specific file is available for a date
        availability = await dq.check_availability_async(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101"
        )

        print(f"📄 File: {availability.file_name}")
        print(f"✅ Available: {availability.is_available}")
        print(f"📅 Created: {availability.first_created_on}")
        print(f"🔄 Modified: {availability.last_modified}")

async def list_available_files():
    async with DataQuery() as dq:
        # Get all available files for a group in date range
        files = await dq.list_available_files_async(
            group_id="YOUR_GROUP_ID",
            start_date="20250101",
            end_date="20250131"
        )

        print(f"📁 Found {len(files)} available files")
        for file_info in files[:5]:  # Show first 5
            file_id = file_info.get('file_group_id', 'Unknown')
            file_date = file_info.get('file_datetime', 'Unknown')
            print(f"   📄 {file_id} - {file_date}")

        # Filter by specific file ID
        specific_files = await dq.list_available_files_async(
            group_id="YOUR_GROUP_ID",
            file_group_id="SPECIFIC_FILE_ID",
            start_date="20250101",
            end_date="20250131"
        )
        print(f"🎯 Filtered results: {len(specific_files)} files")

asyncio.run(check_file_availability())
asyncio.run(list_available_files())
```

### ⚡ High-Performance Download

```python
async def high_performance_download():
    async with DataQuery() as dq:
        # Maximum performance settings for large downloads
        report = await dq.run_group_download_async(
            group_id="LARGE_DATASET",
            start_date="20250101",
            end_date="20250131",
            destination_dir=Path("./bulk_data"),
            delay_between_downloads=0.5  # Minimal delay
        )

        # Performance statistics
        total_mb = sum(f.get('file_size_bytes', 0) for f in report.get('successful_downloads', [])) / (1024*1024)
        time_minutes = report['total_time_minutes']
        throughput = total_mb / time_minutes if time_minutes > 0 else 0

        print(f"""
🚀 High-Performance Results:
   📊 Downloaded: {total_mb:.1f} MB
   ⏱️  Time: {time_minutes:.1f} minutes
   🔥 Throughput: {throughput:.1f} MB/min
        """)

asyncio.run(high_performance_download())
```

### 💻 Synchronous File Operations

```python
from pathlib import Path
from dataquery import DataQuery

def sync_file_operations():
    # Use 'with' instead of 'async with'
    with DataQuery() as dq:
        # Single file download (sync)
        result = dq.download_file(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101",
            destination_path=Path("./downloads")
        )
        print(f"Downloaded: {result.local_path}")

        # Batch download (sync)
        report = dq.run_group_download(
            group_id="YOUR_GROUP_ID",
            start_date="20250101",
            end_date="20250107",
            destination_dir=Path("./downloads")
        )
        print(f"Downloaded {report['successful_downloads']} files")

# No asyncio.run() needed!
sync_file_operations()
```

---

## 🔧 File Delivery Configuration

### Performance Tuning

| Setting | Conservative | Balanced | Aggressive | Use Case |
|---------|-------------|----------|------------|----------|
| `delay_between_downloads` | 2.0s | 1.0s | 0.2s | Shared/Dedicated network |

The SDK automatically optimizes concurrent downloads and parallel parts based on your connection and file sizes.

### File Delivery API Reference

```python
# Single file download
result = await dq.download_file_async(
    file_group_id: str,           # Required: File identifier
    file_datetime: str = None,    # Optional: Date (YYYYMMDD)
    destination_path: Path = None, # Optional: Where to save
    progress_callback: Callable = None  # Optional: Progress tracking
) -> DownloadResult

# Batch download
report = await dq.run_group_download_async(
    group_id: str,                # Required: Data group ID
    start_date: str,              # Required: Start date (YYYYMMDD)
    end_date: str,                # Required: End date (YYYYMMDD)
    destination_dir: Path = "./downloads",  # Optional: Download folder
    delay_between_downloads: float = 1.0  # Optional: Delay between files
) -> dict

# Discovery and availability methods
groups = await dq.list_groups_async(limit: int = 100) -> List[Group]

# Check if a specific file is available for a date
availability = await dq.check_availability_async(
    file_group_id: str,           # Required: File identifier
    file_datetime: str            # Required: Date in YYYYMMDD format
) -> AvailabilityInfo

# List all available files for a group in date range
files = await dq.list_available_files_async(
    group_id: str,                # Required: Group identifier
    file_group_id: str = None,    # Optional: Filter by specific file ID
    start_date: str = None,       # Optional: Start date (YYYYMMDD)
    end_date: str = None          # Optional: End date (YYYYMMDD)
) -> List[dict]
```

---

# 📊 Time Series Data

**Query real-time and historical market data using advanced time series APIs.**

## 🎯 Time Series Features

- **Instrument Data**: Query specific instruments with multiple attributes
- **Expression Queries**: Use traditional DataQuery expressions
- **Group Time Series**: Bulk queries across instrument groups
- **Grid Data**: Retrieve structured data grids
- **Flexible Filtering**: Advanced filtering and pagination
- **Multiple Formats**: JSON, CSV, and other output formats

---

## 📈 Time Series Examples

### 🏛️ Query Instrument Time Series

```python
async def get_instrument_data():
    async with DataQuery() as dq:
        # Get time series for specific instruments
        ts_response = await dq.get_instrument_time_series_async(
            instruments=["US912828U816", "US912828U824"],  # Treasury bonds
            attributes=["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW"],
            start_date="20240101",
            end_date="20240131",
            frequency="FREQ_DAY",
            format="JSON"
        )

        print(f"Retrieved time series data:")
        print(f"Instruments: {len(ts_response.instruments) if hasattr(ts_response, 'instruments') else 'N/A'}")
        print(f"Data points: {ts_response.items if hasattr(ts_response, 'items') else 'N/A'}")

        return ts_response

asyncio.run(get_instrument_data())
```

### 🔍 Search and Query Instruments

```python
async def search_and_query():
    async with DataQuery() as dq:
        # Search for instruments
        search_results = await dq.search_instruments_async(
            group_id="BONDS_GROUP",
            keywords="treasury 10Y",
            page=None
        )

        print(f"Found {search_results.items} matching instruments")

        # List all instruments in a group
        instruments = await dq.list_instruments_async(
            group_id="BONDS_GROUP",
            page=None
        )

        print(f"Total instruments in group: {instruments.items}")

        # Get available attributes for the group
        attributes = await dq.get_group_attributes_async(
            group_id="BONDS_GROUP"
        )

        print(f"Available attributes: {len(attributes.instruments) if hasattr(attributes, 'instruments') else 'N/A'}")

asyncio.run(search_and_query())
```

### 📊 Expression-Based Queries

```python
async def expression_queries():
    async with DataQuery() as dq:
        # Use traditional DataQuery expressions
        ts_response = await dq.get_expressions_time_series_async(
            expressions=[
                "PX_LAST(IBM,USD)",
                "PX_LAST(AAPL,USD)",
                "PX_LAST(MSFT,USD)"
            ],
            start_date="20240101",
            end_date="20240131",
            frequency="FREQ_DAY",
            format="JSON",
            calendar="CAL_USBANK"
        )

        print(f"Expression query results:")
        print(f"Data points: {ts_response.items if hasattr(ts_response, 'items') else 'N/A'}")

        return ts_response

asyncio.run(expression_queries())
```

### 🏢 Group Time Series Queries

```python
async def group_time_series():
    async with DataQuery() as dq:
        # Query time series across a group with filters
        ts_response = await dq.get_group_time_series_async(
            group_id="EQUITY_PRICES",
            attributes=["PX_LAST", "VOLUME"],
            filter="currency(USD)",  # Filter by USD currency
            start_date="20240101",
            end_date="20240131",
            frequency="FREQ_DAY",
            format="JSON"
        )

        print(f"Group time series results:")
        print(f"Data points: {ts_response.items if hasattr(ts_response, 'items') else 'N/A'}")

        # Get available filters for the group
        filters = await dq.get_group_filters_async(
            group_id="EQUITY_PRICES"
        )

        print(f"Available filters: {len(filters.filters) if hasattr(filters, 'filters') else 'N/A'}")

asyncio.run(group_time_series())
```

### 🗃️ Grid Data Queries

```python
async def grid_data_queries():
    async with DataQuery() as dq:
        # Query using expression
        grid_response = await dq.get_grid_data_async(
            expr="PX_LAST(IBM,USD)",
            date="20240115"
        )

        print(f"Grid data from expression:")
        print(f"Series count: {len(grid_response.series) if hasattr(grid_response, 'series') else 'N/A'}")

        # Query using grid ID
        grid_response2 = await dq.get_grid_data_async(
            grid_id="EQUITY_SNAPSHOT_GRID",
            date="20240115"
        )

        print(f"Grid data from ID:")
        print(f"Series count: {len(grid_response2.series) if hasattr(grid_response2, 'series') else 'N/A'}")

asyncio.run(grid_data_queries())
```

### 💻 Synchronous Time Series

```python
from dataquery import DataQuery

def sync_time_series():
    with DataQuery() as dq:
        # Sync instrument query
        ts_response = dq.get_instrument_time_series(
            instruments=["US912828U816"],
            attributes=["PX_LAST"],
            start_date="20240101",
            end_date="20240131"
        )

        print(f"Sync query complete: {ts_response.items if hasattr(ts_response, 'items') else 'N/A'} data points")

        # Sync expression query
        expr_response = dq.get_expressions_time_series(
            expressions=["PX_LAST(IBM,USD)"],
            start_date="20240101",
            end_date="20240131"
        )

        print(f"Expression query: {expr_response.items if hasattr(expr_response, 'items') else 'N/A'} data points")

sync_time_series()
```

---

## 📊 Time Series API Reference

### Instrument Queries
```python
# Query specific instruments
ts_response = await dq.get_instrument_time_series_async(
    instruments: List[str],       # Required: Instrument identifiers
    attributes: List[str],        # Required: Attribute identifiers
    data: str = "REFERENCE_DATA", # Optional: Data domain
    format: str = "JSON",         # Optional: Response format
    start_date: str = None,       # Optional: YYYYMMDD or TODAY-Nx
    end_date: str = None,         # Optional: YYYYMMDD or TODAY-Nx
    calendar: str = "CAL_USBANK", # Optional: Calendar convention
    frequency: str = "FREQ_DAY",  # Optional: Frequency convention
    conversion: str = "CONV_LASTBUS_ABS", # Optional: Conversion convention
    nan_treatment: str = "NA_NOTHING",    # Optional: Missing data handling
    page: str = None              # Optional: Pagination token
) -> TimeSeriesResponse

# Query using expressions
ts_response = await dq.get_expressions_time_series_async(
    expressions: List[str],       # Required: DataQuery expressions
    format: str = "JSON",         # Optional: Response format
    start_date: str = None,       # Optional: Date range
    end_date: str = None,         # Optional: Date range
    # ... same optional parameters as above
) -> TimeSeriesResponse

# Query group time series
ts_response = await dq.get_group_time_series_async(
    group_id: str,                # Required: Group identifier
    attributes: List[str],        # Required: Attribute identifiers
    filter: str = None,           # Optional: Filter (e.g. "currency(USD)")
    # ... same optional parameters as above
) -> TimeSeriesResponse
```

### Discovery and Metadata
```python
# Search instruments
instruments = await dq.search_instruments_async(
    group_id: str,                # Required: Group identifier
    keywords: str,                # Required: Search keywords
    page: str = None              # Optional: Pagination token
) -> InstrumentsResponse

# List instruments
instruments = await dq.list_instruments_async(
    group_id: str,                # Required: Group identifier
    instrument_id: str = None,    # Optional: Specific instrument filter
    page: str = None              # Optional: Pagination token
) -> InstrumentsResponse

# Get group attributes
attributes = await dq.get_group_attributes_async(
    group_id: str,                # Required: Group identifier
    instrument_id: str = None,    # Optional: Specific instrument filter
    page: str = None              # Optional: Pagination token
) -> AttributesResponse

# Get group filters
filters = await dq.get_group_filters_async(
    group_id: str,                # Required: Group identifier
    page: str = None              # Optional: Pagination token
) -> FiltersResponse
```

### Grid Data
```python
# Get grid data
grid_response = await dq.get_grid_data_async(
    expr: str = None,             # Optional: DataQuery expression
    grid_id: str = None,          # Optional: Grid identifier
    date: str = None              # Optional: Date for grid data
) -> GridDataResponse
# Note: Either expr or grid_id must be provided, but not both
```

---

## 🔧 Configuration

### Environment Variables

The SDK uses smart defaults for JPMorgan DataQuery API. Create a `.env` file:

```bash
# Required - Your credentials
DATAQUERY_CLIENT_ID=your_client_id_here
DATAQUERY_CLIENT_SECRET=your_client_secret_here

# Optional - All defaults are pre-configured for JPMorgan DataQuery API
# DATAQUERY_BASE_URL=https://api-developer.jpmorgan.com  # Default
# DATAQUERY_CONTEXT_PATH=/research/dataquery-authe/api/v2  # Default
# DATAQUERY_OAUTH_TOKEN_URL=https://authe.jpmorgan.com/as/token.oauth2  # Default

# Performance tuning (optional)
DATAQUERY_REQUESTS_PER_MINUTE=300    # Rate limit
DATAQUERY_MAX_CONCURRENT_DOWNLOADS=5 # Parallel files
DATAQUERY_TIMEOUT=6000.0             # Request timeout (seconds)

# Download settings (optional)
DATAQUERY_DOWNLOAD_DIR=./downloads   # Default folder
DATAQUERY_CREATE_DIRECTORIES=true    # Auto-create folders
DATAQUERY_OVERWRITE_EXISTING=false   # Don't overwrite files
```

### Programmatic Configuration

```python
from dataquery import DataQuery
from dataquery.models import ClientConfig

# Custom configuration
config = ClientConfig(
    client_id="your_id",
    client_secret="your_secret",
    base_url="https://custom-api.company.com",  # Override default
    timeout=30.0,                              # Override default
    max_retries=5                              # Override default
)

async def with_custom_config():
    async with DataQuery(config=config) as dq:
        # Use any file delivery or time series method
        result = await dq.download_file_async(...)
        ts_data = await dq.get_instrument_time_series_async(...)
```

---

## 🛠️ Command Line Usage

Use the included examples:

```bash
# File delivery - Download files by date range
python examples/files/download_group_by_date.py \
  YOUR_GROUP_ID \
  20250101 \
  20250131 \
  ./downloads

# Time series - Query instrument data
python examples/time_series/get_instrument_data.py \
  US912828U816 \
  PX_LAST \
  20240101 \
  20240131
```

---

## 🆘 Troubleshooting

### Connection Issues

```python
# Quick diagnostic
import asyncio
from dataquery import DataQuery

async def diagnose():
    try:
        async with DataQuery() as dq:
            healthy = await dq.health_check_async()
            print(f"Health check: {'✅ PASS' if healthy else '❌ FAIL'}")
    except Exception as e:
        print(f"Error: {e}")
        print("Check: 1) Credentials 2) Network 3) API endpoint")

asyncio.run(diagnose())
```

### File Availability Check

```python
async def check_file():
    async with DataQuery() as dq:
        availability = await dq.check_availability_async(
            file_group_id="YOUR_FILE_ID",
            file_datetime="20250101"
        )

        if availability and getattr(availability, 'is_available', False):
            print("✅ File is available")
        else:
            print("❌ File not available for this date")
```

### Time Series Data Validation

```python
async def validate_time_series():
    async with DataQuery() as dq:
        # Check if instruments exist
        instruments = await dq.search_instruments_async(
            group_id="YOUR_GROUP",
            keywords="YOUR_INSTRUMENT"
        )

        if instruments.items > 0:
            print("✅ Instruments found")
        else:
            print("❌ No instruments match your search")
```

### Debug Mode

```python
import os
os.environ["DATAQUERY_LOG_LEVEL"] = "DEBUG"
os.environ["DATAQUERY_ENABLE_DEBUG_LOGGING"] = "true"
```

---

## 📖 Documentation

This repository includes comprehensive documentation:

```bash
# Serve docs locally
uv run mkdocs serve

# Build docs
uv run mkdocs build
```

**Key documentation:**
- 📖 **Getting Started**: `docs/getting-started/quickstart.md`
- ⚙️ **Configuration**: `docs/getting-started/configuration.md`
- 🔗 **API Reference**: `docs/api/dataquery.md` and `docs/api/models.md`
- 💡 **Examples**: `docs/examples/basic.md`

---

## 🛠️ Development

### Setup

```bash
# Clone and install
git clone <repository-url>
cd dataquery-sdk
uv sync --all-extras --dev
```

### Testing

```bash
# Run all tests
uv run pytest -v

# With coverage
uv run pytest -v --cov=dataquery --cov-report=html

# Specific tests
uv run pytest tests/test_client.py -v
```

### Code Quality

```bash
# Linting and formatting
uv run flake8 dataquery/ tests/
uv run black .
uv run isort .

# Type checking
uv run mypy dataquery/
```

---

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Make** your changes with tests and documentation
4. **Run** the full test suite (`uv run pytest`)
5. **Commit** your changes (`git commit -m 'Add amazing feature'`)
6. **Push** to the branch (`git push origin feature/amazing-feature`)
7. **Open** a Pull Request

---


---

## 📋 Changelog

See `CHANGELOG.md` for version history and release notes.

---
