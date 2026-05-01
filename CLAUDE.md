# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install
pip install -e ".[dev]"          # with dev dependencies
uv sync --dev                    # using uv

# Test
pytest tests/ -v                 # run all tests
pytest tests/test_client.py -v   # run a single test file
pytest tests/ -m "not slow"      # skip slow tests
pytest tests/ --cov=dataquery --cov-report=term-missing  # with coverage

# Lint & Format (ruff is the canonical tool per pyproject.toml)
ruff check dataquery/ tests/ examples/
ruff format dataquery/ tests/ examples/

# Type check
mypy dataquery/
```

Pytest markers available: `slow`, `integration`, `unit`, `asyncio`.

## Architecture

This is a Python SDK (`dataquery/`) for a financial data REST API (JP Morgan DataQuery). Requires Python 3.11+.

### Layered structure

```
DataQuery (core/dataquery.py)           ← public API, sync/async context managers
    └─ DataQueryClient (core/client.py) ← all HTTP logic, ~100KB, core of the SDK
         ├─ TokenManager / OAuthManager (auth.py)     ← OAuth 2.0 + Bearer token caching
         ├─ TokenBucketRateLimiter (rate_limiter.py)  ← 1500 RPM / 25 TPS default
         ├─ RetryManager + CircuitBreaker (retry.py)  ← exponential backoff, 3 retries
         ├─ ConnectionPoolMonitor (connection_pool.py) ← aiohttp pool health tracking
         └─ LoggingManager (config/logging.py)        ← structlog, JSON or console output
```

`DataQuery` is the only class users interact with directly. It wraps `DataQueryClient` and bridges sync callers via `asyncio.run()` (`_run_sync()`). Both `async with DataQuery()` and `with DataQuery()` are supported.

### Configuration

All config flows through `EnvConfig` (config/env.py) → `ClientConfig` (models.py). Settings are loaded from environment variables or a `.env` file. The `DataQuery` constructor also accepts explicit `client_id`/`client_secret` overrides.

### Request lifecycle

Every API call: acquire rate limiter token → get/refresh OAuth token → build aiohttp request → send with retry loop → parse JSON into Pydantic model → return or raise custom exception.

### Download mechanics

When `num_parts=1` (the default) a single streaming GET is used. When `num_parts > 1` the SDK issues a 1-byte range probe to learn the file size, then downloads via parallel HTTP `Range` requests into a preallocated temp file. Files under 10 MB always fall back to single-stream regardless of `num_parts`. Progress is reported via callback into `DownloadResult`; per-chunk progress logs at DEBUG level (not INFO).

### SSE notification-driven download

`sse_client.py` (`SSEClient`) maintains a long-lived connection to the API's `/events/notification` stream. `sse_subscriber.py` (`NotificationDownloadManager`) subscribes via that client and triggers per-file downloads when events arrive. The polling-based `AutoDownloadManager` was removed in favor of this push model — `dq.auto_download_async(group_id, ...)` is the only watch API.

### Models

All API responses are Pydantic v2 models in `models.py`: `Group`, `FileInfo`, `FileList`, `DownloadResult`, `TimeSeriesResponse`, `GridDataResponse`, `OAuthToken`, etc.

### Exceptions

Custom hierarchy in `exceptions.py`: `DataQueryError` is the base; subclasses include `AuthenticationError`, `RateLimitError`, `NetworkError`, `DownloadError`, etc. The circuit breaker in `retry.py` uses these to decide what to retry.

### CLI

Entry point `dataquery.cli:main` (registered as `dataquery` script). Subcommands: `groups`, `files`, `availability`, `download`, `download-group`, `config`, `auth`. Supports `--json` output and `--watch` mode for polling downloads.

### Key config defaults

- Rate limit: 300 requests/minute (5 TPS) — configurable up to API limits
- Retry: 3 attempts, exponential backoff
- Line length: 120 (ruff)
- Tests in `tests/`, examples in `examples/` (organized by feature area)
