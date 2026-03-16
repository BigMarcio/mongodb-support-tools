# Security and HTTPS Guide

This document combines HTTPS deployment guidance and connection-string handling behavior for Mongosync Insights.

## Why HTTPS Matters

Mongosync Insights can handle sensitive credentials and migration metadata. HTTPS protects:

- MongoDB usernames/passwords in transit
- Migration and infrastructure details displayed in dashboards
- Session cookies and browser security headers

Use HTTP only for local development or isolated test environments.

## HTTPS Deployment Options

### 1) Default HTTP (local development)

```bash
python3 mongosync_insights.py
```

### 2) Direct Flask SSL

Use this when running the app directly with certificates.

```bash
export MI_SSL_ENABLED=true
export MI_SSL_CERT=/path/to/fullchain.pem
export MI_SSL_KEY=/path/to/privkey.pem
export MI_PORT=8443
export MI_SECURE_COOKIES=true
python3 mongosync_insights.py
```

### 3) Reverse Proxy (recommended for production)

Run app on localhost HTTP and terminate TLS at Nginx/Apache.

```bash
export MI_HOST=127.0.0.1
export MI_PORT=3030
export MI_SSL_ENABLED=false
export MI_SECURE_COOKIES=true
python3 mongosync_insights.py
```

## Security-related Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MI_SECURE_COOKIES` | `true` | Restricts session cookies to HTTPS |
| `MI_SESSION_TIMEOUT` | `3600` | Session expiration window |
| `MI_SSL_ENABLED` | `false` | Enables Flask SSL mode |
| `MI_SSL_CERT` | system path | TLS certificate file path |
| `MI_SSL_KEY` | system path | TLS private key path |

For the full variable list, see `docs/configuration.md`.

## Connection String Validation and Sanitization

Mongosync Insights validates destination connection strings using PyMongo:

1. Empty-value check
2. URI parse/format validation (`mongodb://` or `mongodb+srv://`)
3. Live connectivity check (`ping`) to confirm connectivity/authentication

Displayed connection details are sanitized to avoid exposing credentials.

### Credential Safety Notes

- Full credentials are never shown in UI display strings
- Sensitive connection details should not be logged
- Prefer environment variables for production connection strings
- URL-encode special characters in passwords (`@`, `:`, `/`, `?`, `#`, etc.)

## Troubleshooting

### Invalid Connection String

- Check URI scheme and host format
- Ensure password special characters are URL-encoded
- Remove stray spaces/quotes

### Connection Failed

- Verify username/password
- Confirm network access and firewall rules
- For Atlas, ensure source IP is allowed

### HTTPS Issues

- Verify cert/key paths are readable
- Confirm app/proxy listens on expected port
- Check browser and server logs for TLS errors

## Related Docs

- `docs/configuration.md` - Full env-var reference
- `docs/features/connection-setup.md` - Quick Setup and Advanced behavior
- `README.md` - End-user install/run guide
