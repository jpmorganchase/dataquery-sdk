# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Async-first Python SDK (Python 3.12+) for J.P. Morgan's DataQuery API. It wraps **two distinct API surfaces behind one client**, which is the single most important thing to internalize:

- **File Delivery API** — list/check/download binary files (single, date-range, historical, or live via SSE). Base URL: `config.files_api_base_url` (falls back to `api_base_url`).
- **JSON Data API** — discover groups/instruments and run time-series / grid / attribute queries returning JSON. Base URL: `config.api_base_url`.

In `core/client.py` these correspond to two URL builders: `_build_files_api_url()` vs `_build_api_url()`. When adding an endpoint, pick the right one — sending a files call to the JSON base (or vice versa) is a common mistake.

## Commands

```bash
make test              # pytest tests/ -v
make test-cov          # with coverage (term-missing + html)
make lint              # ruff check dataquery/ tests/ examples/
make format            # ruff format + ruff check --fix
make type-check        # mypy dataquery/   (strict: disallow_untyped_defs)
make check-all         # lint + type-check + audit + test
make security-test     # pytest tests/test_security.py -v

# Run a single test / file / pattern
pytest tests/test_client_api_methods.py::test_no_content_body_builds_empty_page
pytest tests/test_client_api_methods.py -q
pytest -k "pagination" -q

# uv variants exist for everything: make uv-test, uv-lint, uv-type-check, etc.
```

Dev install: `pip install -e ".[dev,pandas]"` or `uv sync --all-extras --dev`. `pandas` is an **optional** extra — DataFrame tests skip when it's absent, and `to_dataframe(...)` raises `ImportError` if called without it.

Ruff: line-length 120, double quotes, `E/F/W/I`. Tests are exempt from `E402/E501/F401/F841`.

## Architecture

### Three-layer client stack

1. **`DataQuery`** (`dataquery.py`, ~2400 lines) — the public **facade**. Holds a `DataQueryClient` plus a `SyncRunner`. Every method exists twice: `foo_async(...)` delegates to the client; `foo(...)` is a thin sync wrapper. Also owns `ConfigManager` and the `auto_download`/SSE convenience methods. This is what users import (`from dataquery import DataQuery`).
2. **`DataQueryClient`** (`core/client.py`, ~1800 lines) — HTTP plumbing, OAuth, the request pipeline, downloads, and the groups/files endpoints. Composed from mixins: `class DataQueryClient(DataFrameMixin, InstrumentsMixin, MetadataMixin, TimeSeriesMixin, GridMixin, SearchMixin)`.
3. **Query mixins** (`core/_mixins.py`) — the read-only JSON query surface (instruments, metadata, time-series, grid, search), the pagination engine, and pandas conversion. Mixins depend on exactly three methods the concrete client provides (`_build_api_url`, `_enter_request_cm`, `_handle_response`); `_RequestProto` stubs them for mypy. Keep mixins free of auth/session state — that lives on the client.

### Sync/async duality

Sync methods run their coroutine on a **persistent background event loop** owned by `SyncRunner` (`core/_sync.py`) — not a throwaway `asyncio.run` — so the aiohttp session survives across calls. Consequence: **sync methods raise if called from inside a running event loop**; use the `_async` variant there. When you add an async method to the client, add the `_async` facade wrapper AND the sync wrapper in `dataquery.py` (search for an existing pair like `list_groups_async` / `list_groups` to copy the pattern).

### Request pipeline

`_make_authenticated_request` composes the cross-cutting concerns, in order: **OAuth headers** (`transport/auth.py` — `OAuthManager`/`TokenManager`, token caching + refresh) → **rate limiter** (`transport/rate_limiter.py` — token bucket, 300 rpm / 5 tps default) → **retry + circuit breaker** (`transport/retry.py` — `RetryManager`, exponential backoff) → **connection pool** (`transport/connection_pool.py`). `_enter_request_cm` is a thin normalizer over `_make_authenticated_request` that tolerates both awaitable and direct-context-manager returns (this is what makes test mocking work — see below).

### Pagination (recently reworked — read `core/_mixins.py`)

Paginated JSON responses subclass `Paginated` (`types/models.py`), which carries `links` (`self`/`next`), `items`, `page-size`, and `info`. Two styles:

- **Client-driven** (primary): a single-page method returns a full page object; the caller reads `page.next_link` and passes the page to `get_next_page_async(page)` to fetch the next one (returns `None` at the end). Facade page-returning entry points: `list_groups_page_async`, `search_groups_page_async`, `list_files_page_async`; the instruments/metadata/time-series single-page methods already return full page objects.
- **SDK-driven**: `iter_pages()` walks every page with loop detection + a page cap (`PAGINATION_DEFAULT_MAX_PAGES`), and the `iter_*` / `*_all_*` helpers build on it.

All paginated construction goes through `PaginationMixin._build_page(model_cls, payload)`, which handles DataQuery's non-data envelopes returned inside a 2xx. An envelope is recognized structurally — the payload carries **none** of the model's own fields (by name or wire alias): an `{"errors": [...]}` / `{"error": {...}}` body raises `APIResponseError`; an `{"info": {...}}` (204 no-content) body builds an **empty** page so loops stop cleanly; any other unrecognizable body (`{}`, `{"message": ...}`) also raises `APIResponseError` so malformed 2xx responses fail loudly. A data payload that merely carries an extra `errors` field builds normally. When adding a new paginated endpoint, route construction through `self._build_page(...)`, not `Model(**payload)` directly.

`get_next_page_async` resolves `next` links **surface-aware**: `FileList` pages resolve against the files base (`_page_base_url` → `_build_files_api_url`), everything else against the JSON base. **Link format gotcha (verified against the live API):** the server returns links with a leading slash but WITHOUT the context path (`/group/time-series?...&page=...`), so leading-slash links are treated as base-relative — do NOT resolve them with `urljoin` at the host root (that drops `research/dataquery-authe/api/v2` and 404s with 'no Route matched'). Links that already carry the base's context path join at the host root so the path isn't doubled. Fully absolute links pointing at a different host raise `PaginationError` — credentials are never sent off-host. The facade `list_files_async` returns **all** files by delegating to `client.list_all_files_async` (the files counterpart of `list_all_groups_async`).

Not paginated: grid data, file availability, file-available, and downloads.

### Config resolution

`config/env.py` (`EnvConfig`) maps `DATAQUERY_*` env vars (+ optional `.env`) onto the `ClientConfig` Pydantic model; field names/defaults come from `ClientConfig` itself — except `_CLIENT_ONLY_FIELDS` (`custom_headers`, which also carries `X-User-Agent`), which are per-client and never read from env. Request headers resolve through `ClientConfig.get_custom_headers()` (validates, never echoes values) and `merge_headers` (case-insensitive): custom headers layer over the session defaults but under per-request/SSE headers, and `Authorization` is rejected. `EnvConfig.load_env_file(path)` loads a `.env` (no-op if missing). `DataQuery()` resolves config from a `ClientConfig`, a `.env` path, or kwargs. Shell env vars win over `.env`.

Last-resort fallback: `create_client_config` also loads a **user-level** `~/.dataquery/.env` (`EnvConfig.user_env_file()`; directory overridable with `DATAQUERY_CONFIG_DIR`), written by `dataquery mcp-connect --save-credentials` via `EnvConfig.save_user_env(...)`. It is loaded **last** with `override=False`, so it only fills what is still unset, and with `interpolate=False`, so a secret containing `${...}` is read literally — dotenv expands variables after unquoting, which no quoting style escapes. `save_user_env` merges line-by-line (0600 file in a 0700 dir), so every value must stay on one physical line — that is why `_format_env_value` escapes newlines instead of writing them raw. `tests/conftest.py` redirects this directory session-wide so a developer's real credentials can't leak into tests.

### Response models

`types/models.py` are Pydantic v2 with `ConfigDict(extra="allow", populate_by_name=True)` and kebab-case wire aliases (e.g. `alias="file-group-id"`, `alias="page-size"`). Construct from raw API JSON with `Model(**payload)`; `extra="allow"` means unknown server fields are preserved, so don't assume the declared fields are exhaustive.

### Other entry points

- **CLI** — `dataquery` script → `cli.py:main` (`groups | files | availability | download | download-group | auth | config`). Every subcommand takes `--env-file`, most take `--json`.
- **SSE auto-download** — `sse/` (client, event_store, subscriber) + `download/parallel.py`. `auto_download_async` subscribes to `/events/notification`, persists the last event id under `<destination>/.sse_state/` for cross-restart replay, and returns a manager with `get_stats()`.
- **MCP proxy** (`mcp_proxy.py`), **function registry** (`function_registry.py`, reads `data/*.json`), **CSV export** (`export.py`) — smaller standalone surfaces.

## Testing conventions

- The **network boundary mocked in tests is `_make_authenticated_request`** (monkeypatched to return a dummy response). Because `_enter_request_cm` delegates to it, mocking that one method covers both direct single-page calls and `get_next_page_async` / `iter_pages` follow-up fetches. See `make_client()` / the dummy response objects in `tests/test_client_api_methods.py`.
- `tests/conftest.py` ships a **fallback async runner** so `@pytest.mark.asyncio` tests run even if `pytest-asyncio` isn't installed; it also provides fixtures (`mock_client_factory`, `base_client_config`, `async_response_factory`, `temp_download_dir`).
- pytest runs with `--strict-markers --strict-config`; markers: `slow`, `integration`, `unit`, `asyncio`.

## Conventions when extending

- A new JSON query endpoint usually means: add the method to the right mixin in `core/_mixins.py` (return a `Paginated` subclass via `_build_page` if paged), then add the `_async` + sync facade wrappers in `dataquery.py`, then export any new model/exception from `dataquery/__init__.py`.
- New public exceptions subclass `DataQueryError` (`types/exceptions.py`) and should be added to `__init__.py`'s exports.
- Version lives in `dataquery/__init__.py` (`__version__`); `pyproject.toml` reads it dynamically.
