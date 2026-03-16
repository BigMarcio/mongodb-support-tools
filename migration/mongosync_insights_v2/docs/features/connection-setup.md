# Connection Setup Modes (Quick Setup and Advanced)

This document consolidates implementation notes, UI behavior, testing guidance, and fix history for the dual-mode setup flow.

## Overview

The setup page supports two modes:

- **Quick Setup (default)**: user enters username/password and the app fetches destination details from mongosync progress API, then builds a connection string server-side.
- **Advanced**: user supplies the full destination MongoDB connection string.

## Quick Setup Flow

1. User enters mongosync host, MongoDB username, password, and optional auth DB.
2. Backend queries the progress endpoint.
3. Backend extracts destination from `directionMapping`.
4. Backend builds URI with safe defaults and encoded credentials.
5. Connection is validated before setup succeeds.

### Connection Construction Behavior

Quick Setup attempts to infer deployment type:

- Atlas destinations -> use `mongodb+srv://`
- Replica set destinations -> include `replicaSet` parameter
- Standard host lists -> use `mongodb://`

Common options include auth DB, TLS, and write reliability options.

## Advanced Mode Flow

1. User enters full destination connection string.
2. Backend validates URI format and connectivity.
3. On success, settings are stored in session and dashboard loads.

Use Advanced mode when custom options are needed (pool tuning, custom TLS behavior, specific read preference, etc.).

## UI Behavior

- Mode toggle switches between Quick and Advanced forms.
- Password-sensitive fields are masked by default and support show/hide.
- Mode-specific validation errors are returned with actionable messages.
- Success responses can include sanitized connection details.

## Error Handling Expectations

### Quick Setup

- Progress endpoint unreachable -> suggest verifying host/network or using Advanced mode
- Missing destination in progress payload -> advise fallback to Advanced mode
- MongoDB auth/connectivity failure -> return direct but non-sensitive error text

### Advanced

- Invalid URI format -> prompt for corrected connection string
- Connectivity/auth failure -> prompt to verify credentials/network

## Testing Checklist

- Quick Setup with standard host
- Quick Setup with replica set destination
- Quick Setup with Atlas-style destination
- Passwords with special characters (URL-encoding behavior)
- Advanced mode with valid full URI
- Mode switching and field-state retention
- Validation for missing required fields
- Endpoint unavailable and no-destination error paths

## Known Limitations

- Quick Setup uses a standard set of URI options
- Custom URI options usually require Advanced mode
- Quick Setup depends on progress endpoint availability

## Notable Fix History

Resolved a Quick Setup response-shape bug where success tuples could be mistaken for error tuples; detection logic now distinguishes response-object errors from string success results.

## Related Docs

- `docs/configuration.md`
- `docs/security.md`
- `README.md`
