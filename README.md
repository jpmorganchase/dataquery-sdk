### PyDataQuery SDK (Python)

Production-ready client for the DataQuery API with first-class async support, safe sync wrappers, OAuth, rate limiting, retries, connection pooling, and structured logging.

This README is task-oriented: copy/paste the snippets and you’re productive in minutes.

---

## 1) Install

```bash
pip install -e .
# optional (recommended):
pip install pandas  # enables dataframe helpers
```

---

## 2) Configure

Create a `.env` next to your project (or use real env vars). You must set at least `DATAQUERY_BASE_URL` and one auth method (OAuth or Bearer):

```env
# Required
DATAQUERY_BASE_URL=https://your.api.base

# Optional: separate host for file endpoints
# If your file endpoints use a different host, set these; otherwise omit
# DATAQUERY_FILES_BASE_URL=https://files-api.example.com
# DATAQUERY_FILES_CONTEXT_PATH=/research/dataquery-authe/api/v2

# Optional: context path (only if required by your deployment)
# DATAQUERY_CONTEXT_PATH=/research/dataquery-authe/api/v2

# EITHER OAuth (recommended)
DATAQUERY_OAUTH_ENABLED=true
DATAQUERY_OAUTH_TOKEN_URL=https://your.api.base/oauth/token
DATAQUERY_CLIENT_ID=xxx
DATAQUERY_CLIENT_SECRET=xxx

# OR Bearer token
# DATAQUERY_BEARER_TOKEN=xxx

# Optional basics
DATAQUERY_TIMEOUT=30.0
DATAQUERY_MAX_RETRIES=3
DATAQUERY_RETRY_DELAY=1.0
DATAQUERY_REQUESTS_PER_MINUTE=100
DATAQUERY_BURST_CAPACITY=20
DATAQUERY_LOG_LEVEL=INFO
DATAQUERY_DOWNLOAD_DIR=./downloads
```

Tip: generate a full `.env` template anytime:

```python
from dataquery import EnvConfig
EnvConfig.create_env_template()  # writes .env.template
```

---

## 3) Quick start

Async (best for throughput):

```python
import asyncio
from dataquery import DataQuery

async def main():
    async with DataQuery() as dq:
        groups = await dq.list_groups_async(limit=5)
        print(len(groups))

asyncio.run(main())
```

Sync (convenience):

```python
from dataquery import DataQuery

dq = DataQuery()
groups = dq.list_groups(limit=5)
print(len(groups))
dq.cleanup()
```

---

## 4) Common operations

- List groups

```python
groups = await dq.list_groups_async(limit=10)  # List[Group]
```

- Search groups

```python
results = await dq.search_groups_async("economic", limit=5)
```

- List files in a group

```python
files = await dq.list_files_async(group_id)
# files: List[FileInfo]
for f in files[:3]:
  print(f.file_group_id, f.filename, f.size)
```

- Check file availability

```python
availability = await dq.check_availability_async(file_group_id, "20240101")
print(availability.availability_rate)
```

- Download a file

```python
from pathlib import Path
from dataquery import DownloadOptions

dest = Path("./downloads").resolve()
opts = DownloadOptions(destination_path=str(dest), overwrite_existing=True)

result = await dq.download_file_async(
    file_group_id=file_group_id,
    file_datetime="20240101",
    destination_path=dest,
    options=opts,
)
print(result.status, result.local_path)
```

- Instruments and time series

```python
insts = await dq.list_instruments_async(group_id, limit=20)
ts = await dq.get_instrument_time_series_async(
    instruments=[i.instrument_id for i in insts.instruments[:3]],
    attributes=["CLOSE", "VOLUME"],
    start_date="20240101",
    end_date="20240131",
)
print(len(ts.instruments))
```

- Expressions time series

```python
ts = await dq.get_expressions_time_series_async(
    expressions=["GDP_US_REAL"],
    start_date="20240101",
    end_date="20240131",
)
```

- Grid data

```python
grid = await dq.get_grid_data_async(expr="MY_GRID_EXPR", date="20240630")
print(len(grid.series))
```

---

## 5) DataFrames (optional)

Install `pandas` and use the built-in helpers:

```python
import pandas as pd

groups_df = dq.groups_to_dataframe(groups)
files_df = dq.files_to_dataframe(await dq.list_files_async(group_id))
ts_df = dq.time_series_to_dataframe(ts)
```

---

## 6) Auto-download (hands-off)

Continuously monitor a group and fetch new files:

```python
async def run():
    async with DataQuery() as dq:
        manager = await dq.start_auto_download_async(
            group_id=group_id,
            destination_dir="./downloads",
            interval_minutes=30,
        )
        # Run for a while then stop
        await asyncio.sleep(600)
        await manager.stop()
```

---

## 7) CLI (optional)

The package includes a simple CLI for quick checks. Examples:

```bash
python -m dataquery.cli groups --limit 10 --json
python -m dataquery.cli files --group-id <GROUP> --limit 5 --json
python -m dataquery.cli download --file-group-id <FILE> --file-datetime 20240101 --destination ./downloads --json
```

Pass `--env-file .env` to point at a specific env file.

---

## 8) Advanced config (env vars)

- Rate limiting: `DATAQUERY_REQUESTS_PER_MINUTE`, `DATAQUERY_BURST_CAPACITY`
- Retries: `DATAQUERY_MAX_RETRIES`, `DATAQUERY_RETRY_DELAY`
- Proxy: `DATAQUERY_PROXY_ENABLED`, `DATAQUERY_PROXY_URL`, `DATAQUERY_PROXY_USERNAME`, `DATAQUERY_PROXY_PASSWORD`
- Logging: `DATAQUERY_LOG_LEVEL`, `DATAQUERY_ENABLE_DEBUG_LOGGING`
- Downloads: `DATAQUERY_DOWNLOAD_DIR`, `DATAQUERY_OVERWRITE_EXISTING`, `DATAQUERY_CREATE_DIRECTORIES`

See `dataquery/config.py` for all options and defaults.

### Logging with LoggingManager

Prefer configuring logging via the SDK's LoggingManager (the old `dataquery.dataquery.setup_logging` is deprecated and a no-op):

```python
from dataquery.logging_config import (
    create_logging_config,
    create_logging_manager,
    LogLevel,
    LogFormat,
)

# Console-friendly structured logs at DEBUG level
logging_config = create_logging_config(
    level=LogLevel.DEBUG,
    format=LogFormat.CONSOLE,
    enable_console=True,
    enable_file=False,
    enable_request_logging=False,         # set True to log HTTP requests/responses
    enable_performance_logging=True,
)

logging_manager = create_logging_manager(logging_config)
logger = logging_manager.get_logger("app")
logger.info("Logging initialized")
```

---

## 9) Examples

Complete, runnable examples live in `examples/`. Good starting points:
- `examples/groups/list_groups.py`
- `examples/files/list_files.py`
- `examples/files/download_file.py`
- `examples/system/health_check.py`
- `examples/auto_download_example.py`

---

## 10) Migration notes

- Logging: prefer `LoggingManager` (see `dataquery/logging_config.py`). `dataquery.dataquery.setup_logging` is deprecated and acts as a no-op shim for backward compatibility.
- Endpoints consolidated under `/research/dataquery-authe/api/v2/...` (see `specification.yaml`).
- Per-request auth: tokens injected at request-time; session default headers are not mutated.
- Files API: `list_files_async` returns a `FileList` with `file_group_ids: List[FileInfo]`.
- Examples updated to reflect model fields and responses.

---

## 11) Support & troubleshooting

- Problems authenticating? Verify your `.env` values and ensure either OAuth or Bearer token is configured.
- Event loop errors in Jupyter? Prefer async usage inside `asyncio.run(...)`, or use the SDK sync wrappers in plain cells.
- Unexpected 429? Tune rate limits via env vars.
- Need a template? `EnvConfig.create_env_template()`.

If something feels rough, open an issue or PR.

---

## 12) Developer guide (SDK maintainers)

- Local dev setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[all]'
```

- Lint, types, security, tests

```bash
make lint          # flake8 on SDK (dataquery/) only
make type-check    # mypy
make audit         # pip-audit
make test          # pytest
make test-cov      # pytest with coverage
make ci-check      # install-dev + lint + type-check + audit + coverage
```

- Single-env multi-host support
  - Use env defaults and optional split host:

```env
DATAQUERY_BASE_URL=https://core-api.example.com
# Optional split host for file endpoints:
DATAQUERY_FILES_BASE_URL=https://files-api.example.com
DATAQUERY_FILES_CONTEXT_PATH=/research/dataquery-authe/api/v2
# Optional context path for core API:
# DATAQUERY_CONTEXT_PATH=/research/dataquery-authe/api/v2
```

- Per-instance overrides (env as defaults; credentials passed explicitly):

```python
from dataquery import DataQuery

dq = DataQuery(
    client_id="your_client_id",
    client_secret="your_client_secret",
    # Optional overrides; omit to use env
    files_base_url="https://files-api.example.com",
    files_context_path="/research/dataquery-authe/api/v2",
)
```

- Releasing
  - Update `CHANGELOG.md`
  - Bump version in `pyproject.toml`
  - Build and publish

```bash
make build dist
make publish-test  # to TestPyPI (if configured)
make publish       # to PyPI
```

- Notes
  - Supported Python: 3.10+
  - Examples are runnable from the repo, e.g.: `python examples/groups/list_groups.py`