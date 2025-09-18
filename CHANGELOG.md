# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of dataquery-sdk
- Parallel file download functionality with HTTP range requests
- Group download capabilities with intelligent rate limiting
- Command-line interface for batch downloads
- Comprehensive test suite with coverage reporting

### Changed
- Refactored download methods to use parallel functionality by default
- Updated default num_parts from 10 to 5 for better resource management

### Fixed
- Fixed recursion issues in download fallback logic
- Resolved type errors with None values in num_parts parameter
- Fixed test mocking issues after method refactoring

## [0.1.0] - 2025-01-XX

### Added
- Initial release