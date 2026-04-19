# DataQuery SDK

Python SDK for the J.P. Morgan DataQuery API — authenticated file downloads, time-series queries, and real-time notification-driven downloads with OAuth 2.0, rate limiting, and automatic retries built in.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Linting: Ruff](https://img.shields.io/badge/linting-ruff-261230.svg)](https://github.com/astral-sh/ruff)

## Features

- **Parallel file downloads** — when `num_parts > 1`, HTTP range requests split the file into parallel chunks with bounded concurrency; `num_parts=1` (default) uses a simple streaming GET
- **Historical and date-range downloads** — fetch every file in a group (optionally filtered to one or many `file-group-id`s) between two dates
- **Notification-driven downloads (SSE)** — subscribe to the `/notification` stream and auto-download files as soon as they are published
- **Time-series queries** — by expression, by instrument, or by group with attribute / filter projections
- **OAuth 2.0 with token caching and refresh** — or supply a bearer token directly
- **Token-bucket rate limiter** — 300 rpm / 5 tps defaults (configurable up to API limits)
- **Retry + circuit breaker** — exponential backoff, configurable failure threshold
- **Sync and async APIs** — every operation has `_async` and sync variants
- **Optional pandas integration** — `to_dataframe(...)` on any response
- **CLI** — `dataquery groups | files | availability | download | download-group | auth | config`

## Installation

```bash
# Core install
pip install dataquery-sdk

# With pandas DataFrame conversion
pip install "dataquery-sdk[pandas]"

# With dev tooling (ruff, mypy, pytest)
pip install "dataquery-sdk[dev]"
```

Python 3.11+ is required.

## Configure credentials

Set OAuth client credentials via environment variables:

```bash
export DATAQUERY_CLIENT_ID="your_client_id"
export DATAQUERY_CLIENT_SECRET="your_client_secret"
```

Or create a `.env` file in the working directory:

```env
DATAQUERY_CLIENT_ID=your_client_id
DATAQUERY_CLIENT_SECRET=your_client_secret
```

Or pass them directly to the constructor:

```python
from dataquery import DataQuery

dq = DataQuery(client_id="...", client_secret="...")
```

A starter `.env` can be generated with `dataquery config template --output .env`.

## Quick start

### Download files for a date range

```python
from dataquery import DataQuery

# async
async with DataQuery() as dq:
    result = await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data",
    )
    print(f"{result['successful_downloads']}/{result['total_files']} files downloaded")

# sync
with DataQuery() as dq:
    result = dq.run_group_download(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data",
    )
```

### Restrict a date-range download to specific file-group-ids

`file_group_id` accepts a single id or a list. When a list is supplied, availability
queries run in parallel per id and the union of dates is downloaded.

```python
async with DataQuery() as dq:
    result = await dq.run_group_download_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
        destination_dir="./data",
        file_group_id=["FG_ABC", "FG_DEF", "FG_XYZ"],
    )
```

### Download a single file

```python
from pathlib import Path
from dataquery import DataQuery

async with DataQuery() as dq:
    result = await dq.download_file_async(
        file_group_id="JPMAQS_GENERIC_RETURNS",
        file_datetime="20250115",
        destination_path=Path("./downloads"),
        num_parts=5,  # parallel chunks
    )
    print(f"Downloaded: {result.local_path} ({result.file_size} bytes)")
```

### Time-series queries

```python
async with DataQuery() as dq:
    # By expression
    ts = await dq.get_expressions_time_series_async(
        expressions=["DB(MTE,IRISH EUR 1.100 15-May-2029 LON,,IE00BH3SQ895,MIDPRC)"],
        start_date="20240101",
        end_date="20240131",
    )

    # By group with attributes + filter
    ts = await dq.get_group_time_series_async(
        group_id="FI_GO_BO_EA",
        attributes=["MIDPRC", "REPO_1M"],
        filter="country(IRL)",
        start_date="20240101",
        end_date="20240131",
    )

    df = dq.to_dataframe(ts)  # requires pandas extra
```

### Discover available data

```python
async with DataQuery() as dq:
    groups = await dq.list_groups_async(limit=100)
    files = await dq.list_files_async(group_id="JPMAQS_GENERIC_RETURNS")
    available = await dq.list_available_files_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
    )
    instruments = await dq.search_instruments_async(
        group_id="FI_GO_BO_EA", keywords="irish"
    )
```

## Auto-download (coming soon)

Real-time notification-driven downloads via the DataQuery SSE stream are under
active development and will be available in a future release. Once released,
`auto_download_async` will subscribe to the `/notification` endpoint and
download files automatically as they are published — no polling required.

## CLI

The installer registers a `dataquery` script:

```bash
# List / search groups
dataquery groups --limit 100
dataquery groups --search "fixed income" --json

# List files in a group
dataquery files --group-id JPMAQS_GENERIC_RETURNS --json

# Check availability for a single file
dataquery availability --file-group-id JPMAQS_GENERIC_RETURNS --file-datetime 20250115

# Download a single file
dataquery download --file-group-id JPMAQS_GENERIC_RETURNS \
                   --file-datetime 20250115 \
                   --destination ./downloads \
                   --num-parts 5

# Watch a group and download as new files arrive
dataquery download --watch --group-id JPMAQS_GENERIC_RETURNS --destination ./downloads

# Download everything in a date range
dataquery download-group --group-id JPMAQS_GENERIC_RETURNS \
                         --start-date 20250101 --end-date 20250131 \
                         --destination ./data \
                         --max-concurrent 5 --num-parts 4

# Restrict to one or more file-group-ids
dataquery download-group --group-id JPMAQS_GENERIC_RETURNS \
                         --file-group-id FG_ABC FG_DEF \
                         --start-date 20250101 --end-date 20250131

# Config utilities
dataquery config show
dataquery config validate
dataquery config template --output .env

# Verify auth
dataquery auth test
```

Every subcommand accepts `--env-file PATH` (to point at a non-default `.env`) and
most accept `--json` for machine-readable output.

## Configuration

### Environment variables

All environment variables use the `DATAQUERY_` prefix.

**Credentials**

| Variable | Default | Notes |
|---|---|---|
| `DATAQUERY_CLIENT_ID` | _(none)_ | Required for OAuth |
| `DATAQUERY_CLIENT_SECRET` | _(none)_ | Required for OAuth |
| `DATAQUERY_BEARER_TOKEN` | _(none)_ | Alternative to OAuth |
| `DATAQUERY_OAUTH_ENABLED` | `true` | Set `false` to use bearer-token mode |
| `DATAQUERY_OAUTH_TOKEN_URL` | `https://authe.jpmorgan.com/as/token.oauth2` | |
| `DATAQUERY_OAUTH_AUD` | `JPMC:URI:RS-06785-DataQueryExternalApi-PROD` | |

**API endpoints**

| Variable | Default |
|---|---|
| `DATAQUERY_BASE_URL` | `https://api-developer.jpmorgan.com` |
| `DATAQUERY_FILES_BASE_URL` | `https://api-dataquery.jpmchase.com` |
| `DATAQUERY_CONTEXT_PATH` | `/research/dataquery-authe/api/v2` |

**HTTP / retry / rate limit**

| Variable | Default |
|---|---|
| `DATAQUERY_TIMEOUT` | `600.0` |
| `DATAQUERY_MAX_RETRIES` | `3` |
| `DATAQUERY_RETRY_DELAY` | `1.0` |
| `DATAQUERY_CIRCUIT_BREAKER_THRESHOLD` | `5` |
| `DATAQUERY_REQUESTS_PER_MINUTE` | `300` |
| `DATAQUERY_BURST_CAPACITY` | `5` |
| `DATAQUERY_POOL_CONNECTIONS` | `10` |
| `DATAQUERY_POOL_MAXSIZE` | `20` |

**Proxy** (optional)

`DATAQUERY_PROXY_ENABLED`, `DATAQUERY_PROXY_URL`, `DATAQUERY_PROXY_USERNAME`,
`DATAQUERY_PROXY_PASSWORD`, `DATAQUERY_PROXY_VERIFY_SSL`.

### Programmatic configuration

```python
from dataquery import ClientConfig, DataQuery

config = ClientConfig(
    client_id="...",
    client_secret="...",
    base_url="https://api-developer.jpmorgan.com",
    timeout=60.0,
    max_retries=3,
    requests_per_minute=300,
)

async with DataQuery(config) as dq:
    ...

# Or pass overrides as kwargs on top of env/.env resolution:
async with DataQuery(client_id="...", client_secret="...", timeout=60.0) as dq:
    ...
```

## Error handling

All errors inherit from `DataQueryError`:

```python
from dataquery import DataQuery
from dataquery.exceptions import (
    DataQueryError,
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    NetworkError,
    DownloadError,
    ConfigurationError,
)

async with DataQuery() as dq:
    try:
        ts = await dq.get_expressions_time_series_async(
            expressions=["DB(...)"], start_date="20240101", end_date="20240131"
        )
    except AuthenticationError:
        ...  # check credentials
    except RateLimitError:
        ...  # back off — SDK already retried
    except NotFoundError:
        ...  # group / file / instrument not found
    except NetworkError:
        ...  # transient; SDK already retried
    except DataQueryError:
        ...  # any other SDK-level failure
```

## Date formats

```python
start_date="20240101"   # absolute, YYYYMMDD
start_date="TODAY"
start_date="TODAY-1D"   # yesterday
start_date="TODAY-1W"
start_date="TODAY-1M"
start_date="TODAY-1Y"
```

## Performance tuning

`run_group_download_async` uses a flattened concurrency model — total concurrent
HTTP requests = `max_concurrent × num_parts`.

With `num_parts=1` (the default) each file downloads as a single streaming GET.
Set `num_parts > 1` to enable parallel HTTP range requests per file — the SDK
probes the file size first, then downloads byte ranges concurrently. Files under
10 MB always use a single stream regardless of `num_parts`.

```python
await dq.run_group_download_async(
    group_id="JPMAQS_GENERIC_RETURNS",
    start_date="20250101",
    end_date="20250131",
    destination_dir="./data",
    max_concurrent=5,   # parallel files
    num_parts=4,        # parallel chunks per file (1 = single stream)
    max_retries=3,
)
```

Typical settings: `max_concurrent` 3–5, `num_parts` 2–8. The SDK automatically
inserts delays between file starts so the configured `requests_per_minute` is not
exceeded.

## API reference (most-used methods)

| Area | Method | Notes |
|---|---|---|
| Discovery | `list_groups_async(limit)` | |
| | `search_groups_async(keywords, limit, offset)` | |
| | `list_files_async(group_id, file_group_id=None)` | |
| | `list_available_files_async(group_id, file_group_id, start_date, end_date)` | |
| | `list_instruments_async(group_id, instrument_id=None, page=None)` | |
| | `search_instruments_async(group_id, keywords, page=None)` | |
| | `get_group_attributes_async(group_id, ...)` | |
| | `get_group_filters_async(group_id, page=None)` | |
| Downloads | `download_file_async(file_group_id, file_datetime, ...)` | single file |
| | `run_group_download_async(group_id, start_date, end_date, file_group_id=None, ...)` | date range, single or list of ids |
| | `download_historical_async(...)` | chunked historical backfill |
| | `auto_download_async(group_id, ...)` | SSE notifications (the only watch path) |
| Time series | `get_expressions_time_series_async(expressions, start_date, end_date)` | |
| | `get_instrument_time_series_async(instruments, attributes, start_date, end_date)` | |
| | `get_group_time_series_async(group_id, attributes, filter, start_date, end_date)` | |
| Grid data | `get_grid_data_async(...)` | |
| Utilities | `check_availability_async(file_group_id, file_datetime)` | |
| | `health_check_async()` | |
| | `to_dataframe(response)` | requires `pandas` extra |
| | `get_stats()` / `get_pool_stats()` / `get_rate_limit_info()` | diagnostics |

Every async method above has a sync counterpart (without the `_async` suffix, or
with an explicit `_sync` suffix) that runs the coroutine via `asyncio.run`.

## Examples

The `examples/` directory is organised by feature:

- `examples/files/` — single-file and date-range downloads
- `examples/expressions/` — expression time series
- `examples/instruments/` — instrument discovery + time series
- `examples/groups/` and `examples/groups_advanced/` — group discovery and time series
- `examples/grid/` — grid data
- `examples/system/` — SSE notification subscriber, diagnostics

Run any example directly:

```bash
python examples/files/download_file.py
python examples/system/sse_local_server_example.py
```

## Development

```bash
# Clone
git clone https://github.com/jpmorganchase/dataquery-sdk.git
cd dataquery-sdk

# Install with dev + all extras
uv sync --all-extras --dev         # using uv
# or
pip install -e ".[dev,pandas]"

# Run tests
pytest tests/ -v
pytest tests/ --cov=dataquery --cov-report=term-missing

# Lint / format / type-check
ruff check dataquery/ tests/ examples/
ruff format dataquery/ tests/ examples/
mypy dataquery/
```

Pytest markers: `slow`, `integration`, `unit`, `asyncio`.

## Requirements

- Python 3.11+
- `aiohttp>=3.8,<4`, `pydantic>=2,<3`, `structlog>=23`, `python-dotenv>=1`
- Optional: `pandas>=2` (for `to_dataframe`)

## Support

- GitHub Issues: <https://github.com/jpmorganchase/dataquery-sdk/issues>
- Email: dataquery_support@jpmorgan.com

## License

MIT — see [LICENSE](LICENSE).

## Changelog

See [docs/changelog.md](docs/changelog.md).
