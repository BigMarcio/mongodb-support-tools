# Configuration Management Guide

This document explains the configuration system for Mongosync Insights using environment variables.

## Prerequisites

Python 3.11+ and the `libmagic` system library are required. See `README.md` for complete installation instructions.

## Configuration Overview

Mongosync Insights is configured through environment variables.

Configuration priority:
1. Environment variables (highest priority)
2. Default values (lowest priority)

## Environment Variables Reference

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_HOST` | `127.0.0.1` | Server host address (`0.0.0.0` for all interfaces) |
| `MI_PORT` | `3030` | Server port number |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `MI_LOG_FILE` | `insights.log` | Path to log file |

### MongoDB Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_CONNECTION_STRING` | _(empty)_ | MongoDB connection string (optional, can be provided via UI) |
| `MI_INTERNAL_DB_NAME` | `mongosync_reserved_for_internal_use` | Mongosync internal metadata database |
| `MI_POOL_SIZE` | `10` | MongoDB connection pool size |
| `MI_TIMEOUT_MS` | `5000` | MongoDB connection timeout in milliseconds |

### Live Monitoring Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_REFRESH_TIME` | `10` | Live monitoring refresh interval in seconds |
| `MI_PROGRESS_ENDPOINT_URL` | _(empty)_ | Mongosync progress endpoint URL |

### File Upload Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_MAX_FILE_SIZE` | `10737418240` | Max upload size in bytes (10GB) |

### UI Customization

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_MAX_PARTITIONS_DISPLAY` | `10` | Maximum partitions to display in UI |
| `MI_PLOT_WIDTH` | `1450` | Plot width in pixels |
| `MI_PLOT_HEIGHT` | `1800` | Plot height in pixels |

### Security Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_SECURE_COOKIES` | `true` | Enable secure cookies (requires HTTPS) |
| `MI_SESSION_TIMEOUT` | `3600` | Session timeout in seconds |
| `MI_SSL_ENABLED` | `false` | Enable HTTPS/SSL in Flask |
| `MI_SSL_CERT` | `/etc/letsencrypt/live/your-domain/fullchain.pem` | SSL certificate path |
| `MI_SSL_KEY` | `/etc/letsencrypt/live/your-domain/privkey.pem` | SSL private key path |

For HTTPS deployment details and certificate setup, see `docs/security.md`.

## Usage Examples

### Basic Local Development

```bash
python3 mongosync_insights.py
```

### Custom Port and Host

```bash
export MI_PORT=8080
export MI_HOST=0.0.0.0
python3 mongosync_insights.py
```

### Pre-configured MongoDB Connection

```bash
export MI_CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/"
export MI_REFRESH_TIME=5
python3 mongosync_insights.py
```

### Combined Monitoring (Metadata + Progress Endpoint)

```bash
export MI_CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/"
export MI_PROGRESS_ENDPOINT_URL="localhost:27182/api/v1/progress"
export MI_REFRESH_TIME=5
python3 mongosync_insights.py
```

### Production-style Behind Reverse Proxy

```bash
export MI_HOST=127.0.0.1
export MI_PORT=3030
export LOG_LEVEL=INFO
export MI_SSL_ENABLED=false
export MI_SECURE_COOKIES=true
python3 mongosync_insights.py
```

## Troubleshooting

### Environment Variables Not Taking Effect

```bash
env | grep MI_
```

If running with `sudo`, preserve env vars:

```bash
sudo -E python3 mongosync_insights.py
```

### Connection String Not Working

- Verify format (`mongodb://` or `mongodb+srv://`)
- Check for extra quotes/spaces
- Test in `mongosh` first

## Related Docs

- `README.md` - Installation and getting started
- `docs/security.md` - HTTPS and validation/security behavior
- `docs/features/connection-setup.md` - Quick Setup and Advanced mode details
