# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
