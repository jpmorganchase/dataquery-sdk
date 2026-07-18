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


