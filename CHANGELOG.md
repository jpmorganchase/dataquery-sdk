# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.0.7] - 2025-10-20
- Initial release of dataquery-sdk
- Parallel file download functionality with HTTP range requests
- Group download capabilities with intelligent rate limiting
- Comprehensive test suite with coverage reporting
## [0.0.8] - 2025-10-21
- Made label and expression parameters optional in attribute api response 
## [0.0.9] - 2025-12-09
- Performance optimizations
## [0.1.0] - 2025-12-10
- Time series data made optional
## [0.1.1] - 2026-02-18
- Non blocking IO
## [0.1.2] - 2026-02-18
- File already exists status added 
## [0.1.3] - 2026-02-21
- Historical file download added 
## [0.1.4] - 2026-03-23
- Introduced circuit breaker environment variable (`DATAQUERY_CIRCUIT_BREAKER_THRESHOLD`) 
## [0.1.5] - 2026-04-16
- Introduced file-group-id to the group downloads
## [0.2.0] - 2026-05-06
- SSE auto-download with cross-process event replay and multi-group support
- Python 3.11+ only
## [1.0.0] - 2026-06-03
- Stable 1.0 release
- Bumped minimum Python to 3.12; added 3.14 to test matrix
- Security floors: idna>=3.15, urllib3>=2.7.0, pymdown-extensions>=10.21.3
## [1.1.0] - 2026-06-10
- Reliability: 429 responses are now retried instead of surfaced immediately; the server `Retry-After` is honored when timing retry backoff; adaptive rate-limit backoff now engages instead of staying at zero
- Auth: OAuth token-fetch network/timeout failures now raise `NetworkError` (previously `AuthenticationError`); token acquisition is single-flight so concurrent callers don't stampede the token endpoint
- SSE: jittered reconnect backoff to avoid synchronized reconnect storms; stop reconnecting on fatal 403/404 and bound retries on 401; idle `sock_read` timeout distinguished from the heartbeat watchdog; honor the server `retry:` hint; strip a leading UTF-8 BOM; larger read buffer guards against `LineTooLong`; `stop()` is await-safe under concurrent callers
## [1.2.0] - 2026-06-29
- DataQuery functions: new `function-help` command for local lookup of all 158 DQ function syntaxes, parameters, and categories (no API call); backed by a static, frozen `dataquery/data/function.json` dataset
## [1.2.1] - 2026-07-14
- Written research: new `download_zip_async` helper that downloads a group over a date range (split into calendar-month windows to fit the available-files endpoint limit) and safely extracts ZIP archives as each download completes, overlapping unzip with in-flight downloads
- Group downloads: `run_group_download_async` accepts an `on_file_complete` async callback awaited per file on `completed`/`already_exists`
- Extraction is Zip Slip-guarded, skips current-day archives, and surfaces failures via `extraction_errors` (downgrading overall status to `partial`); date windows with no available files no longer mark a multi-window run as `partial`
## [1.2.2] - 2026-07-18
- MCP: new `mcp-connect` CLI command  
## [1.2.3] - 2026-07-23
- Pagination: new client-driven `get_next_page_async(page)` helper — read `next_link` off any paged response and hand the page back to fetch the next one (manual counterpart to the SDK-driven `iter_pages`); next-page links are resolved against the surface the page came from and never sent off-host
- Pagination models: `items`, `page-size`, and `info` fields, plus the `next_link` property and `get_self_link()` accessor, moved onto the shared `Paginated` mixin; `FileList` is now paginated; list fields default to empty instead of being required so partial/empty envelopes parse cleanly
- New `APIResponseError` raised when a 2xx response carries an `errors`/`error` envelope (e.g. `498 Unrecognized Page Token`); an `info` `204` "no content" envelope now yields an empty page so pagination stops cleanly, while any other unrecognized body fails loudly
- Exports: `Paginated`, `Link`, `APIResponseError` are now part of the public API
## [1.2.4] - 2026-08-12
- MCP: `mcp-connect` accepts `--client-id`, `--client-secret` and `--bearer-token` and exports them into the `DATAQUERY_*` environment of the process, so the MCP bridge and the SDK share one credential set instead of each needing its own setup
## [1.2.5] - 2026-08-21
- MCP: `mcp-connect --url` is now optional
## [1.2.6] - 2026-09-24
- Bug fixes
- MCP: capped the `mcp` extra to `fastmcp>=2.14,<4` — fastmcp 4.x repackages onto `httpx2` and drops `FastMCP.as_proxy()`, which broke `mcp-connect`; installs now resolve to the 3.x line
## [1.2.7] - 2026-09-25
- MCP: new `dataquery mcp-install` one-time setup — saves your credentials to `~/.dataquery/.env` (prompting on a terminal, the secret unechoed) and adds a secret-free server running this environment's `mcp-connect` to Claude Desktop (default) or, with `--app`, Claude Code (user scope), ChatGPT desktop / Codex (`~/.codex/config.toml`), Cursor or VS Code, or to any `mcpServers` JSON file (`--config-file`); re-running updates it
## [1.2.8] - 2026-09-26
- MCP: `mcp-connect` retries a request once with a fresh OAuth token when the endpoint answers 401, so a cached token the server no longer accepts (revoked, or issued for other credentials) no longer breaks the connection
- MCP: `mcp-connect` caches its OAuth token under `~/.dataquery/tokens/<credential fingerprint>/` instead of `./downloads/.tokens` relative to wherever the MCP app launched it; an explicit `DATAQUERY_TOKEN_STORAGE_DIR` with `DATAQUERY_TOKEN_STORAGE_ENABLED=true` still wins
## [Unreleased]
- CLI: new `dataquery available-files` command lists the files a group published across a date range (`group/files/available-files`), with optional `--file-group-id`, `--start-date`, `--end-date` and `--json`
- Skill: the `dataquery` skill now covers the full File Delivery API workflow: choosing files versus time-series, date-range availability, single and bulk downloads, and SSE watch
- Skill: new "Local Setup" section walks users through installing uv and `dataquery-sdk`, saving credentials to `~/.dataquery/.env`, and verifying with `dataquery config validate` and `dataquery heartbeat`; preflight gains a credentials check that points back to it
- Fix: `get_expressions_time_series` sends each expression as its own repeated `expressions` query param; comma-joining them made the server read several expressions (which contain commas) as one and reply `Expression syntax error!`
- Fix: time-series responses for computed expressions (e.g. `VOL(30, DB(...))`), which carry null `instrument-id`/`instrument-name`, no longer fail validation
- Fix: `dataquery download` no longer prints `Downloaded to …` and exits 0 when the download failed; it reports the error and exits 1. `download-group` exits 1 when any file fails, and `download --json` no longer crashes serialising the local path
- Fix: CLI time-series commands (`group-timeseries`, `instrument-timeseries`, `expression-timeseries`) follow every page, so summaries and `--output-csv` exports are no longer silently truncated to the first page; with `--page`, and on `instruments`/`filters`/`attributes`, the summary names the next page token
- Fix: per-series server messages (e.g. `Expression syntax error!`) are shown in time-series summaries and in the CSV export error instead of being dropped
- Fix: `check_availability` no longer answers with another date's record when the requested date is missing; a date-only request still matches a timestamped record
- Fix: `check_availability`, `list_available_files` and `get_grid_data` raise `APIResponseError` on an `errors` envelope inside a 2xx response (previously read as "not available", "no files", or a validation error) and treat an `info` envelope as empty; `GridDataResponse.series` defaults to empty so whole-request `errorCode`/`errorMessage` bodies parse, and the CLI shows them
- Fix: `dataquery heartbeat` distinguishes rejected credentials (`authentication failed (HTTP 401/403)`), outages (`DOWN (HTTP 503)`) and network errors (`unreachable`) instead of reporting all of them as DOWN; new `service_status_async()` / `service_status()` return `{"up", "http_status", "error"}`
- Fix: the 2,080-character URL limit is checked against the encoded URL as sent, so requests with long list parameters (up to ~180 characters longer than the old estimate) are caught before reaching the server
- Fix: the `search` summary always counted 0 results because the API returns a bare JSON list
- Fix (auth): the OAuth token cache moved from `./downloads/.tokens/oauth_token.json` (relative to the working directory, shared by every credential set) to `~/.dataquery/tokens/<credential fingerprint>/`, the location `mcp-connect` already used. Each cached token records the credentials it was issued for and is ignored by any other set; token files written by older versions are ignored once and replaced. `DATAQUERY_TOKEN_STORAGE_DIR` is still honoured with `DATAQUERY_TOKEN_STORAGE_ENABLED=true`. This also stops the test suite writing a fake token into the CLI's real cache when run from the repo root
- Fix (auth): requests that get a 401 discard the rejected OAuth token and retry once with a new one, instead of reusing a revoked or foreign cached token until it expires; concurrent 401s share a single replacement token
- Fix (auth): tokens count as expired shortly before their recorded expiry (10% of the lifetime, at most 30s) to cover clock skew and latency; tokens shorter-lived than the refresh threshold refresh at half-life instead of never refreshing early; tokens without `expires_in` are kept in memory only, never cached to disk where they would be reused indefinitely
- Fix (auth): a failed early refresh keeps using the current token while it is still valid instead of failing the request, and a failed refresh no longer requests a new token twice; concurrent processes no longer share one temp file when saving the token
- Fix (CLI): `--attributes` on `group-timeseries`/`instrument-timeseries` is now repeatable and taken verbatim. Attribute IDs usually contain commas (`TR,,LOC`, `01M,FWD_YIELD`), and splitting the value on commas sent the API fragments that matched nothing. **Breaking:** pass several attributes as `--attributes A --attributes B`, not `--attributes A,B`
- Fix: `check_availability` read the live endpoint's single-record response as "not available" for every file; it now reads both the single-record and list shapes. `dataquery availability` text output now says `available` / `not available`
- Fix: `dataquery download-group` crashed with `TypeError` (`str / str`) on every run; `run_group_download_async` now accepts a `str` destination
- Fix: bulk-download reports add `details.failures` (file group, datetime and error for each failed file) and the CLI prints one line per failure
- Fix: if the server rejects a page token part-way through a multi-page time-series pull (seen live as `498`), the CLI keeps the pages already fetched, marks the result `INCOMPLETE` with counts, and exits 1 instead of discarding everything
- Fix: CLI summaries show the server's `info` message (e.g. `[204] no content`) instead of just "0 instrument(s)"
- Fix (SSE watch): the startup catch-up ignored `--file-group-id` and downloaded every file in the group; stopping the watcher with Ctrl+C crashed while printing its stats; subscription confirmations and heartbeats were logged as warnings
- CLI: new `dataquery skill-install` installs the bundled `dataquery` agent skill into Claude Code (`~/.claude/skills`), Codex (`~/.agents/skills`), VS Code / GitHub Copilot (`~/.copilot/skills`) and Cursor (`~/.cursor/skills`), or with `--scope project` into the repository's `.claude/`, `.agents/`, `.github/` or `.cursor/` skills folder. `--app all` covers every app, `--uninstall` removes it, and a folder it did not install is only replaced with `--force`
- Packaging: the skill moved from the repository's `skills/` folder into the package (`dataquery/skills/dataquery`) and ships in the wheel. Its description was shortened to the Agent Skills limit of 1,024 characters and the Claude-only `disable-model-invocation` field removed, so it validates with the spec's `skills-ref` tool and loads in Codex, VS Code and Cursor
