# DataQuery SDK

Professional Python SDK for the DataQuery API - High-performance data access with parallel downloads, time series queries, and seamless OAuth 2.0 authentication.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Features

- **High-Performance Downloads**: Parallel file downloads with automatic retry and progress tracking
- **Time Series Queries**: Query data by expressions, instruments, or groups with flexible filtering
- **OAuth 2.0 Authentication**: Automatic token management and refresh
- **Connection Pooling**: Optimized HTTP connections with configurable rate limiting
- **Pandas Integration**: Direct conversion to DataFrames for analysis
- **Async & Sync APIs**: Use async/await or synchronous methods based on your needs

## Installation

```bash
pip install dataquery-sdk
```

## Quick Start

### 1. Configure Credentials

Set your API credentials as environment variables:

```bash
export DATAQUERY_CLIENT_ID="your_client_id"
export DATAQUERY_CLIENT_SECRET="your_client_secret"
```

Or create a `.env` file in your project directory:

```env
DATAQUERY_CLIENT_ID=your_client_id
DATAQUERY_CLIENT_SECRET=your_client_secret
```

### 2. Download Files

**Synchronous (Python Scripts)**

```python
from dataquery import DataQuery

# Download all files for a date range
with DataQuery() as dq:
    results = dq.run_group_download(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data"
    )
    print(f"Downloaded {results['successful_downloads']} files")
```

**Asynchronous (Jupyter Notebooks)**

```python
from dataquery import DataQuery

# Download all files for a date range
async with DataQuery() as dq:
    results = await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data"
    )
    print(f"Downloaded {results['successful_downloads']} files")
```

### 3. Query Time Series Data

```python
from dataquery import DataQuery

async with DataQuery() as dq:
    # Query by expression
    result = await dq.get_expressions_time_series_async(
        expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
        start_date="20240101",
        end_date="20240131"
    )
    
    # Convert to pandas DataFrame
    df = dq.to_dataframe(result)
    print(df.head())
```

### 4. Discover Available Data

```python
from dataquery import DataQuery

async with DataQuery() as dq:
    # List all available groups
    groups = await dq.list_groups_async(limit=100)
    
    # Convert to DataFrame for easy viewing
    groups_df = dq.to_dataframe(groups)
    print(groups_df[['group_id', 'group_name', 'description']])
```

## Common Use Cases

### Download Single File

```python
from dataquery import DataQuery
from pathlib import Path

async with DataQuery() as dq:
    result = await dq.download_file_async(
        file_group_id="JPMAQS_GENERIC_RETURNS",
        file_datetime="20250115",
        destination_path=Path("./downloads")
    )
    print(f"Downloaded: {result.local_path}")
```

### Query with Filters

```python
async with DataQuery() as dq:
    # Get time series for Ireland bonds only
    result = await dq.get_group_time_series_async(
        group_id="FI_GO_BO_EA",
        attributes=["MIDPRC", "REPO_1M"],
        filter="country(IRL)",
        start_date="20240101",
        end_date="20240131"
    )
    
    df = dq.to_dataframe(result)
```

### Search for Instruments

```python
async with DataQuery() as dq:
    # Search for instruments by keywords
    results = await dq.search_instruments_async(
        group_id="FI_GO_BO_EA",
        keywords="irish"
    )
    
    # Use the results to query time series
    instrument_ids = [inst.instrument_id for inst in results.instruments[:5]]
    data = await dq.get_instrument_time_series_async(
        instruments=instrument_ids,
        attributes=["MIDPRC"],
        start_date="20240101",
        end_date="20240131"
    )
```

## Performance Optimization

### Parallel Downloads

```python
async with DataQuery() as dq:
    # Download multiple files concurrently with parallel chunks
    results = await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data",
        max_concurrent=5,  # Download 5 files simultaneously
        num_parts=4        # Split each file into 4 parallel chunks
    )
```

**Recommended Settings:**
- `max_concurrent`: 3-5 (concurrent file downloads)
- `num_parts`: 2-8 (parallel chunks per file)

### Rate Limiting

Configure rate limits to avoid API throttling:

```python
from dataquery import DataQuery, ClientConfig

config = ClientConfig(
    client_id="your_client_id",
    client_secret="your_client_secret",
    rate_limit_rpm=300,  # Requests per minute
    max_retries=3,
    timeout=60.0
)

async with DataQuery(config=config) as dq:
    # Your code here
    pass
```

## Configuration

### Environment Variables

```bash
# Required
DATAQUERY_CLIENT_ID=your_client_id
DATAQUERY_CLIENT_SECRET=your_client_secret

# Optional - API Endpoints
DATAQUERY_BASE_URL=https://api-developer.jpmorgan.com
DATAQUERY_FILES_BASE_URL=https://api-dataquery.jpmchase.com

# Optional - Performance
DATAQUERY_MAX_RETRIES=3
DATAQUERY_TIMEOUT=60
DATAQUERY_RATE_LIMIT_RPM=300
```

### Programmatic Configuration

```python
from dataquery import DataQuery, ClientConfig

config = ClientConfig(
    client_id="your_client_id",
    client_secret="your_client_secret",
    base_url="https://api-developer.jpmorgan.com",
    max_retries=3,
    timeout=60.0,
    rate_limit_rpm=300
)

async with DataQuery(config=config) as dq:
    # Your code here
    pass
```

## Error Handling

```python
from dataquery import DataQuery
from dataquery.exceptions import (
    DataQueryError,
    AuthenticationError,
    NotFoundError,
    RateLimitError
)

async def safe_query():
    try:
        async with DataQuery() as dq:
            result = await dq.get_expressions_time_series_async(
                expressions=["DB(...)"],
                start_date="20240101",
                end_date="20240131"
            )
            return result
    except AuthenticationError as e:
        print(f"Authentication failed: {e}")
    except NotFoundError as e:
        print(f"Resource not found: {e}")
    except RateLimitError as e:
        print(f"Rate limit exceeded: {e}")
    except DataQueryError as e:
        print(f"API error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
```

## Date Formats

### Absolute Dates

```python
start_date="20240101"  # YYYYMMDD format
end_date="20241231"
```

### Relative Dates

```python
start_date="TODAY"      # Today
start_date="TODAY-1D"   # Yesterday
start_date="TODAY-1W"   # 1 week ago
start_date="TODAY-1M"   # 1 month ago
start_date="TODAY-1Y"   # 1 year ago
```

## Calendar Conventions

| Calendar | Description | Use Case |
|----------|-------------|----------|
| `CAL_WEEKDAYS` | Monday-Friday | International data (recommended) |
| `CAL_USBANK` | US banking days | US-only data (default) |
| `CAL_WEEKDAY_NOHOLIDAY` | All weekdays | Generic business days |
| `CAL_DEFAULT` | Calendar day | Include weekends |

## Examples

The `examples/` directory contains comprehensive examples:

- **File Downloads**: Single file, batch downloads, availability checks
- **Time Series**: Expressions, instruments, groups with filters
- **Discovery**: Search instruments, list groups, get attributes
- **Advanced**: Grid data, auto-download, custom progress tracking

Run an example:

```bash
python examples/files/download_file.py
python examples/expressions/get_expressions_time_series.py
```

## CLI Usage

The SDK includes a command-line interface:

```bash
# Download files
dataquery download --group-id JPMAQS_GENERIC_RETURNS \
                   --start-date 20250101 \
                   --end-date 20250131 \
                   --destination ./data

# List groups
dataquery list-groups --limit 100

# Check file availability
dataquery check-availability --file-group-id JPMAQS_GENERIC_RETURNS \
                             --date 20250115
```

## API Reference

### Core Methods

**File Downloads**
- `download_file_async()` - Download a single file
- `run_group_download_async()` - Download all files in a date range
- `list_available_files_async()` - Check file availability

**Time Series Queries**
- `get_expressions_time_series_async()` - Query by expression
- `get_instrument_time_series_async()` - Query by instrument ID
- `get_group_time_series_async()` - Query entire group with filters

**Discovery**
- `list_groups_async()` - List available data groups
- `search_instruments_async()` - Search for instruments
- `list_instruments_async()` - List all instruments in a group
- `get_group_attributes_async()` - Get available attributes
- `get_group_filters_async()` - Get available filters

**Utilities**
- `to_dataframe()` - Convert any response to pandas DataFrame
- `health_check_async()` - Check API health
- `get_stats()` - Get connection and rate limit statistics

For detailed API documentation, see the [API Reference](docs/api/README.md).

## Requirements

- Python 3.10 or higher
- Dependencies:
  - `aiohttp>=3.8.0` - Async HTTP client
  - `pydantic>=2.0.0` - Data validation
  - `structlog>=23.0.0` - Structured logging
  - `python-dotenv>=1.0.0` - Environment variable management

Optional:
- `pandas>=2.0.0` - For DataFrame conversion

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/dataquery/dataquery-sdk.git
cd dataquery-sdk

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=dataquery --cov-report=html

# Run specific test file
pytest tests/test_client.py -v
```

### Code Quality

```bash
# Format code
black dataquery/ tests/

# Check linting
flake8 dataquery/ tests/ examples/

# Type checking
mypy dataquery/
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- **GitHub Issues**: [Report a bug](https://github.com/dataquery/dataquery-sdk/issues)
- **Documentation**: [Read the docs](https://github.com/dataquery/dataquery-sdk/wiki)
- **Email**: support@dataquery.com

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.
