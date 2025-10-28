# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Nothing yet

## [0.1.0] - 2025-10-27

### Added
- Progress tracking with ETA calculations during data extraction
- Extraction history for performance estimation
- Spinner animations for long-running operations
- Automatic removal of empty columns from extracted data

### Changed
- Improved timestamp handling with clearer `ts` (UTC) and `ts_local` (local time) columns
- Enhanced timezone detection with automatic fallbacks
- Better error messages and error handling throughout
- Improved progress display without visual artifacts

### Removed
- `requirements.txt` file (dependencies now in `pyproject.toml`)

## [0.0.2] - 2025-10-27

### Added
- Detailed daylight saving time (DST) handling in date utilities
- Timezone management methods with UTC offset logging
- Warnings for invalid timezone configurations with automatic fallback to system timezone
- Basic documentation templates for issues and pull requests

### Changed
- Enhanced README.md for improved clarity and streamlined installation instructions
- Improved date parsing to ensure accurate local datetime and UTC conversion across DST transitions

### Fixed
- setuptools-scm version scheme to properly handle 0.0.x versions

### Tests
- Expanded unit tests to cover DST scenarios and timezone conversions
- Added test cases for invalid timezone configurations and fallback behavior

## [0.0.1] - 2025-10-24

### Added
- Initial release of PhasorPoint CLI
- Data extraction from PhasorPoint databases with flexible time ranges
- Support for multiple output formats (Parquet, CSV)
- Automatic power calculations (apparent, active, reactive power)
- Data quality validation and automatic cleanup
- Batch extraction from multiple PMUs
- Chunking and parallel processing for large extractions
- Connection pooling for performance optimization
- Performance diagnostics mode
- Extraction logging with metadata tracking
- Database exploration commands (list-tables, table-info, query)
- Configuration management with .env and config.json support
- Cross-platform support (Windows, Linux, macOS)
- Comprehensive test suite with high coverage
- CI/CD workflows for testing and PyPI publishing
- Complete documentation and usage examples

[Unreleased]: https://github.com/energinet-ti/phasor-point-cli/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/energinet-ti/phasor-point-cli/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/energinet-ti/phasor-point-cli/releases/tag/v0.0.1

