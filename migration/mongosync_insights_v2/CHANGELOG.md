# Changelog

All notable changes to Mongosync Insights are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0] - 2026-02-07

### Added

- Live streaming support for uploaded log files with Server-Sent Events
- Enhanced session management with timeout, connection caching, and validation

### Changed

- Enhanced UI/UX and connection handling
- Reduced pie chart sizes in Live Monitor for better visibility (two charts fit side-by-side without scrolling)

## [0.7.1.6] - 2026-01-22

### Added

- Setup and session management features
- Log streaming functionality for real-time log analysis
- Setup/configuration page with quick setup mode (fetch direction mapping, extract destination, auto-fill connection string)
- Log path auto-detection (parse `--logPath`, `--metricsLoggingFilepath` or use lsof fallback)
- Mongosync proxy at `/api/mongosync/<path>` for backend requests
- Process management: find mongosync PID, kill mongosync with safety checks
- HTTPS support and SSL configuration
- Security headers (HSTS, XSS, CSRF, clickjacking protection)
- Secure cookies and env-based configuration
- Database connection pool
- Configuration management via `app_config.py` and environment variables
- Log file validation and MIME type validation for uploads
- Validation and sanitization of file uploads
- Connection string validation

### Changed

- Replaced config.ini with environment variable configuration
- Default host changed to 127.0.0.1
- Templates updated to get version from app_config
- Renamed folder `static` to `images`
- Removed browser autofill for sensitive fields
- Removed connection string from logs
- Removed extra connection string validations

### Fixed

- Timezone parsing error (#166)
- Duplicated Options and Hidden options (#161)
- Possible configuration injection vulnerability

### Security

- Added missing security headers
- Validation and sanitization of file uploads
- Fix for configuration injection

## [0.6.1] - 2025-08-18

### Changed

- Various improvements and bug fixes (#158)

## [Initial] - 2025-07-11

### Added

- MIGRATION-433: Initial mongosync log analysis tool
- Log file upload and parsing (NDJSON format)
- Support for plain and compressed formats (`.log`, `.json`, `.out`, `.gz`, `.zip`, `.bz2`, `.tar.gz`, `.tgz`, `.tar.bz2`)
- Log metrics: Total/Copied bytes, CEA reads/writes, Collection Copy reads/writes, Events applied, Lag time
- Live monitoring via MongoDB metadata and progress endpoint
- Combined monitoring (metadata + progress endpoint)
- Interactive plots for migration visualization

