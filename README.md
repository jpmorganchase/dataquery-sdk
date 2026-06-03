# DataQuery SDK

Python SDK for the J.P. Morgan DataQuery API. The SDK wraps two distinct
surfaces behind one client:

- **File Delivery API** — list, check availability of, and download files
  (single, date-range, historical backfill, or live via SSE notifications).
- **JSON Data API** — discover groups/instruments and run
  time-series, grid, and attribute queries that return JSON.

OAuth 2.0, token-bucket rate limiting, retries, and a circuit breaker are
built in for both.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Linting: Ruff](https://img.shields.io/badge/linting-ruff-261230.svg)](https://github.com/astral-sh/ruff)

## Contents

- [The two APIs at a glance](#the-two-apis-at-a-glance)
- [Features](#features)
- [New here? Three steps to your first download](#new-here-three-steps-to-your-first-download)
- [Installation](#installation)
- [Configure credentials](#configure-credentials)
- [Quick start — File Delivery API](#quick-start--file-delivery-api)
- [Quick start — JSON Data API](#quick-start--json-data-api)
- [Auto-download (SSE)](#auto-download-sse)
- [CLI](#cli)
- [Configuration](#configuration)
- [Logging](#logging)
- [Error handling](#error-handling)
- [Troubleshooting](#troubleshooting)
- [Date formats](#date-formats)
- [Performance tuning](#performance-tuning)
- [API reference (most-used methods)](#api-reference-most-used-methods)
- [Examples](#examples) · [Development](#development) · [Requirements](#requirements) · [Support](#support)

## The two APIs at a glance

| | **File Delivery API** | **JSON Data API** |
|---|---|---|
| **What you get** | Binary file payloads (CSV, Parquet, etc.) streamed to disk | JSON responses for catalog metadata and time-series data |
| **Typical methods** | `download_file_async`, `run_group_download_async`, `download_historical_async`, `auto_download_async`, `list_files_async`, `list_available_files_async`, `check_availability_async` | `list_groups_async`, `search_groups_async`, `list_instruments_async`, `search_instruments_async`, `get_group_attributes_async`, `get_group_filters_async`, `get_expressions_time_series_async`, `get_instrument_time_series_async`, `get_group_time_series_async`, `get_grid_data_async` |
| **CLI surface** | `dataquery files`, `availability`, `download`, `download-group` | `dataquery groups` |

Both surfaces share the same host and the same OAuth credentials, and run
through one `DataQuery` client — pick the methods that match what you need.

## Features

**File Delivery API**

- **Streaming file downloads** — single streaming GET per file
- **Date-range and historical downloads** — fetch every file in a group (optionally filtered to one or many `file-group-id`s) between two dates, or chunk a long historical backfill into monthly ranges
- **Notification-driven downloads (SSE)** — subscribe to the `/events/notification` stream and auto-download files as soon as they are published

**JSON Data API**

- **Group, file, and instrument discovery** — list and keyword-search the catalog
- **Time-series queries** — by expression, by instrument, or by group with attribute / filter projections
- **Grid data** — pivoted grid queries for tabular responses
- **Optional pandas integration** — `to_dataframe(...)` converts any JSON response

**Cross-cutting**

- **OAuth 2.0 with token caching and refresh** — or supply a bearer token directly
- **Token-bucket rate limiter** — 300 rpm / 5 tps defaults (configurable up to API limits)
- **Retry + circuit breaker** — exponential backoff, configurable failure threshold
- **Sync and async APIs** — every operation has `_async` and sync variants
- **CLI** — `dataquery groups | files | availability | download | download-group | auth | config`

## New here? Three steps to your first download

1. `pip install dataquery-sdk`
2. Put your OAuth credentials in a `.env` file (see [Configure credentials](#configure-credentials))
3. Run a one-liner to confirm everything works:

   ```python
   import asyncio
   from dataquery import DataQuery

   async def main():
       async with DataQuery() as dq:
           groups = await dq.list_groups_async(limit=5)
           for g in groups:
               print(g.group_id, "—", g.group_name)

   asyncio.run(main())
   ```

If that prints groups, auth and networking are working. From there, jump to
[Quick start](#quick-start) (date-range download) or the [CLI](#cli).

## Installation

```bash
# Core install
pip install dataquery-sdk

# With pandas DataFrame conversion
pip install "dataquery-sdk[pandas]"

# With dev tooling (ruff, mypy, pytest)
pip install "dataquery-sdk[dev]"
```

Python 3.12+ is required.

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

## Quick start — File Delivery API

These methods stream binary file payloads to disk.

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
    # OperationReport (Pydantic model) — counts/timing/data/details are dicts on it.
    print(f"{result.counts['successful_downloads']}/{result.counts['total_files']} files downloaded")

# sync — same arguments, drop the _async suffix
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
    )
    print(f"Downloaded: {result.local_path} ({result.file_size} bytes)")
```

### List files / check availability

```python
async with DataQuery() as dq:
    files = await dq.list_files_async(group_id="JPMAQS_GENERIC_RETURNS")
    available = await dq.list_available_files_async(
        group_id="JPMAQS_GENERIC_RETURNS",
        start_date="20250101",
        end_date="20250131",
    )
    info = await dq.check_availability_async(
        file_group_id="JPMAQS_GENERIC_RETURNS",
        file_datetime="20250115",
    )
```

For live notification-driven downloads, see [Auto-download (SSE)](#auto-download-sse).

## Quick start — JSON Data API

These methods return JSON (Pydantic-typed) responses. Use `to_dataframe(...)`
to convert any response to a pandas DataFrame.

### Discover groups and instruments

```python
async with DataQuery() as dq:
    groups = await dq.list_groups_async(limit=100)
    matches = await dq.search_groups_async("fixed income", limit=20)
    instruments = await dq.search_instruments_async(
        group_id="FI_GO_BO_EA", keywords="irish",
    )
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

    # By instrument + attribute
    ts = await dq.get_instrument_time_series_async(
        instruments=["IE00BH3SQ895"],
        attributes=["MIDPRC"],
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

### Group metadata (attributes, filters)

```python
async with DataQuery() as dq:
    attrs = await dq.get_group_attributes_async(group_id="FI_GO_BO_EA")
    filters = await dq.get_group_filters_async(group_id="FI_GO_BO_EA")
```

### Grid data

```python
async with DataQuery() as dq:
    grid = await dq.get_grid_data_async(
        expr="DB(GRID,...)",  # provider-supplied grid expression
        date="20240131",
    )
```

## Auto-download (SSE)

`auto_download_async` subscribes to the DataQuery `/events/notification` SSE
stream and downloads files as soon as the server announces them — no polling.
The call returns immediately with a manager object; the subscription runs in
the background until you call `manager.stop()`.

```python
import asyncio
from dataquery import DataQuery

async def main():
    async with DataQuery() as dq:
        manager = await dq.auto_download_async(
            group_id="JPMAQS_GENERIC_RETURNS",
            destination_dir="./downloads",
            file_group_id=["FG_ABC", "FG_DEF"],  # optional server-side filter
        )
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            await manager.stop()
            print(manager.get_stats())

asyncio.run(main())
```

Key behaviours:

- **Initial backfill** (`initial_check=True`, default) — on startup, checks
  availability for the current day so files published before the subscription
  started are not missed.
- **Cross-process event replay** (`enable_event_replay=True`, default) — the
  last SSE event id is persisted to
  `<destination>/.sse_state/sse_<fingerprint>.json`, so a restart resumes from
  where the previous session stopped rather than replaying from scratch.
- **Reconnects** — exponential backoff between `reconnect_delay` (5s) and
  `max_reconnect_delay` (60s). Set `heartbeat_timeout` (e.g. `90.0`) to force
  a reconnect when no bytes arrive within the window — useful behind stateful
  middleboxes that drop idle sockets.
- **Health stats** — `manager.get_stats()` returns notifications received,
  files downloaded / skipped / failed, the last event id, and a bounded ring
  of recent errors.

The same path is available from the CLI as `dataquery download --watch` (see
below).

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
                   --destination ./downloads

# Watch a group and download as new files arrive (calls auto_download_async under the hood —
# same SSE subscription, same event-replay state files)
dataquery download --watch --group-id JPMAQS_GENERIC_RETURNS --destination ./downloads

# Download everything in a date range
dataquery download-group --group-id JPMAQS_GENERIC_RETURNS \
                         --start-date 20250101 --end-date 20250131 \
                         --destination ./data

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

## Logging

The SDK logs through [structlog](https://www.structlog.org/) and emits
structured events for requests, retries, rate-limit waits, SSE reconnects, and
download progress. Two ways to drive it:

**Standard Python logging** — works without extra setup:

```python
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("dataquery").setLevel(logging.DEBUG)   # SDK-only DEBUG
```

DEBUG includes per-chunk download progress and SSE keepalives — useful while
debugging, noisy in production.

**Structured (JSON) output** — recommended for long-running `auto_download`
services so a log shipper can parse the events:

```python
from pathlib import Path
from dataquery.config.logging import (
    LogFormat, LogLevel, create_logging_config, create_logging_manager,
)

cfg = create_logging_config(
    level=LogLevel.INFO,
    format=LogFormat.JSON,           # or LogFormat.CONSOLE for humans
    enable_file=True,
    log_file=Path("./dataquery.log"),
    enable_request_logging=False,    # set True to log every HTTP request/response
)
create_logging_manager(cfg)          # installs handlers on the root logger
```

`examples/system/enable_request_logging.py` is a runnable version showing
request/response logging for traffic debugging.

**Health snapshots for `auto_download`** — `manager.get_stats()` returns
notifications received, files downloaded / skipped / failed, the last event
id, and a bounded ring of recent errors. Wire it into a `/healthz` endpoint
for daemon-style deployments.

## Error handling

All errors inherit from `DataQueryError`:

```python
from dataquery import DataQuery
from dataquery.types.exceptions import (
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

## Troubleshooting

**`AuthenticationError` / HTTP 401 on the first call**

- Verify both `DATAQUERY_CLIENT_ID` and `DATAQUERY_CLIENT_SECRET` are set:
  `dataquery config show` will print the resolved config (secrets masked).
- Confirm OAuth is reaching the right endpoint:
  `dataquery auth test` performs a token exchange and reports the failure mode.
- If the credentials are correct but the audience is wrong, set
  `DATAQUERY_OAUTH_AUD` to the value provisioned for your client.

**`.env` file isn't picked up**

- The SDK looks for `.env` in the current working directory at instantiation.
  Either `cd` to the directory containing `.env` before running, or pass the
  path explicitly: `DataQuery(env_file=".env.production")`.
- Variables already set in the shell environment win over the `.env` file —
  unset them (`unset DATAQUERY_CLIENT_ID`) if you want the file to take effect.
- The CLI accepts `--env-file PATH` on every subcommand for the same reason.

**Connection / proxy / SSL failures**

- Behind a corporate proxy, set `DATAQUERY_PROXY_ENABLED=true` and
  `DATAQUERY_PROXY_URL=http://proxy.host:port`. Add `DATAQUERY_PROXY_USERNAME`
  / `DATAQUERY_PROXY_PASSWORD` if auth is required.
- For self-signed proxy CAs, set `DATAQUERY_PROXY_VERIFY_SSL=false` (insecure;
  prefer pointing `SSL_CERT_FILE` at the corporate root CA bundle).
- Sporadic `NetworkError` after long idle periods usually means a stateful
  middlebox is dropping the SSE socket — set
  `heartbeat_timeout=90.0` on `auto_download_async` to force a reconnect when
  no bytes arrive within the window.

**Rate-limit pauses**

- Default is 300 rpm / 5 tps. The SDK self-throttles via the token-bucket
  limiter; if you see long sleeps before requests, lower
  `DATAQUERY_REQUESTS_PER_MINUTE` is not the cure — it's likely working as
  designed. Raise it (up to your provisioned limit) to go faster.
- `dq.get_rate_limit_info()` shows the current bucket state.

**SSE auto-download "missed" events after a restart**

- Confirm `enable_event_replay=True` (the default).
- Replay state lives under `<destination>/.sse_state/sse_<fingerprint>.json` —
  if that directory was wiped, the next start has nothing to resume from. Use
  `manager.clear_event_id()` (or `dataquery download --watch --reset-event-id`)
  only when you intentionally want a clean slate.

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

`run_group_download_async` streams each file as a single GET. The SDK
automatically inserts delays between file starts so the configured
`requests_per_minute` is not exceeded.

```python
await dq.run_group_download_async(
    group_id="JPMAQS_GENERIC_RETURNS",
    start_date="20250101",
    end_date="20250131",
    destination_dir="./data",
    max_retries=3,
)
```

Tune throughput via `DATAQUERY_REQUESTS_PER_MINUTE` and `DATAQUERY_BURST_CAPACITY`
(see [Configuration](#environment-variables)) rather than per-call concurrency
flags.

## API reference (most-used methods)

### File Delivery API

| Method | Notes |
|---|---|
| `list_files_async(group_id, file_group_id=None)` | List files in a group |
| `list_available_files_async(group_id, file_group_id, start_date, end_date)` | Files available in a date range |
| `check_availability_async(file_group_id, file_datetime)` | Per-file availability check |
| `download_file_async(file_group_id, file_datetime, ...)` | Single-file streaming download |
| `run_group_download_async(group_id, start_date, end_date, file_group_id=None, ...)` | Date-range download, single or list of ids |
| `download_historical_async(...)` | Chunked historical backfill (monthly ranges) |
| `auto_download_async(group_id, ...)` | SSE notification subscription (the only watch path) |

### JSON Data API

| Method | Notes |
|---|---|
| `list_groups_async(limit)` | List groups |
| `search_groups_async(keywords, limit, offset)` | Keyword search |
| `list_instruments_async(group_id, instrument_id=None, page=None)` | List / lookup instruments |
| `search_instruments_async(group_id, keywords, page=None)` | Instrument keyword search |
| `get_group_attributes_async(group_id, ...)` | Available attributes for a group |
| `get_group_filters_async(group_id, page=None)` | Available filters for a group |
| `get_expressions_time_series_async(expressions, start_date, end_date)` | Time series by expression |
| `get_instrument_time_series_async(instruments, attributes, start_date, end_date)` | Time series by instrument + attribute |
| `get_group_time_series_async(group_id, attributes, filter, start_date, end_date)` | Time series for a group |
| `get_grid_data_async(expr=None, grid_id=None, date=None)` | Grid (pivoted) data |

### Utilities (shared)

| Method | Notes |
|---|---|
| `health_check_async()` | API heartbeat |
| `to_dataframe(response)` | Requires `pandas` extra |
| `get_stats()` / `get_pool_stats()` / `get_rate_limit_info()` | Diagnostics |

Every async method has a sync counterpart with the same name minus the
`_async` suffix — `list_groups_async` ↔ `list_groups`, `download_file_async` ↔
`download_file`, etc. Sync calls run the coroutine internally via
`asyncio.run`, so do not call them from inside an existing event loop.

## Examples

The `examples/` directory is organised by feature:

- `examples/files/` — single-file and date-range downloads
- `examples/expressions/` — expression time series
- `examples/instruments/` — instrument discovery + time series
- `examples/groups/` and `examples/groups_advanced/` — group discovery and time series
- `examples/grid/` — grid data
- `examples/system/` — SSE notification subscriber (single + multi-group), diagnostics

Run any example directly:

```bash
python examples/files/download_file.py
python examples/system/auto_download_example.py            # single group
python examples/system/auto_download_multi_group_example.py  # several groups in parallel
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

- Python 3.12+
- `aiohttp>=3.8,<4`, `pydantic>=2,<3`, `structlog>=23`, `python-dotenv>=1`
- Optional: `pandas>=2` (for `to_dataframe`)

## Support

- GitHub Issues: <https://github.com/jpmorganchase/dataquery-sdk/issues>
- Email: dataquery_support@jpmorgan.com

## License

MIT — see [LICENSE](LICENSE).

## Changelog

See [docs/changelog.md](docs/changelog.md).
