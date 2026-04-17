# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.6] - 2026-04-17

### Changed
- Download progress logging demoted from INFO to DEBUG. Per-chunk progress
  lines no longer flood the default log output; enable DEBUG to see them.
- `show_progress` parameter threaded through the full watch/SSE chain:
  `DataQuery.watch_and_download_async` → `DataQueryClient.watch_and_download_async`
  → `NotificationDownloadManager`. Pass `show_progress=False` to suppress all
  progress logging.
- `num_parts=1` (the default) now skips the range-download probe and goes
  straight to a single-stream GET. The parallel range machinery only activates
  when `num_parts > 1`.
- Removed automatic `num_parts` scaling for large files (>500 MB). The caller's
  `num_parts` value is now respected as-is; use the concurrency optimizer or
  pass a higher value explicitly for large files.
- Removed per-part chunk size scaling (1 MB / 2 MB / 4 MB tiers). The default
  1 MB chunk size is used for all downloads regardless of part size.
- Simplified `_parallel_download.py`: delegates destination resolution to
  `client._resolve_destination()` and result construction to
  `client._create_download_result()` instead of duplicating the logic inline.
- Fixed dead condition in `_prepare_download_params` (`num_parts is not None
  and num_parts is None`) and removed a hidden default that silently overrode
  `num_parts` to 5.

## [0.1.5] - 2026-04-16

### Removed (BREAKING)
- `AutoDownloadManager` and the polling-based watch path. The SSE-driven
  `NotificationDownloadManager` is now the only watch implementation.
  - Public symbol `AutoDownloadManager` no longer exported from `dataquery`.
  - Methods removed: `DataQueryClient.start_auto_download_async`,
    `DataQueryClient.start_auto_download`, `DataQuery.start_auto_download_async`,
    `DataQuery.start_auto_download`.
  - Migration: replace `dq.start_auto_download_async(group_id, ...)` with
    `dq.watch_and_download_async(group_id, ...)`. The SSE manager exposes the
    same `start()`, `stop()`, `get_stats()` surface.

### Changed
- CLI `download --watch` now subscribes to the SSE notification stream instead
  of polling.

## [0.1.4] - 2026-04-15

### Changed
- Internal split: `DataFrameMixin` and the read-only query mixins
  (`InstrumentsMixin`, `MetadataMixin`, `TimeSeriesMixin`, `GridMixin`) moved
  out of `client.py` into `_mixins.py`.
- Flattened-concurrency bulk downloader extracted to `_parallel_download.py`
  and optimized: removed an unnecessary `asyncio.Lock` from the per-chunk hot
  path, moved file preallocation off the event loop, and deferred progress
  updates to callback boundaries.
- Token cache file now created with owner-only `0o600` permissions and the
  cache directory with `0o700`.
- `download-group` CLI accepts multiple `--file-group-id` values.
- CLI `download --watch` now blocks until interrupted (was a no-op `sleep(1)`).

### Removed
- Duplicate `_sync`-suffix wrappers on `DataQuery` (use the unsuffixed sync
  variants).

## [0.1.3] - 2026-02-21

### Added
- Historical file download example with monthly chunking
- Simplified group download by date range example
- Replaced flake8, black, isort with Ruff for linting and formatting

## [0.1.2] - 2026-02-18

### Added
- `ALREADY_EXISTS` status in `DownloadStatus` enum
- Files that already exist are now skipped instead of marked as failed
- `AutoDownloadManager` counts skipped files under `files_skipped` stat

## [0.1.1] - 2026-02-18

### Changed
- Non-blocking I/O optimizations for download operations

## [0.1.0] - 2025-12-10

### Changed
- Time series data made optional in API responses

## [0.0.9] - 2025-12-09

### Changed
- Performance optimizations

## [0.0.8] - 2025-10-21

### Fixed
- Made label and expression parameters optional in attribute API response

## [0.0.7] - 2025-10-20

### Added
- Initial release of dataquery-sdk
- Parallel file download functionality with HTTP range requests
- Group download capabilities with intelligent rate limiting
- OAuth 2.0 authentication with automatic token management
- Command-line interface for batch downloads
- Comprehensive test suite with coverage reporting
