# Configuration Management Guide

This document explains the configuration management system for Mongosync Insights using environment variables.

## Prerequisites

**Python 3.11+** is required to run Mongosync Insights. See [README.md](README.md) for complete installation instructions.

For **mongosync** version compatibility, see [README.md — mongosync compatibility](README.md#mongosync-compatibility).

For **migration-verifier** version compatibility, see [README.md — Migration Verifier compatibility](README.md#migration-verifier-compatibility).

## Configuration Overview

Mongosync Insights is configured entirely through **environment variables**. No configuration files are used.

For the **Migration Monitoring** dashboard (progress endpoint, metadata fallback, index building, embedded verifier), see **[MIGRATION_MONITORING.md](MIGRATION_MONITORING.md)**.

### **Configuration Priority**

1. **Environment Variables** (highest priority)
2. **Default Values** (lowest priority)

All configuration can be set using `export` commands before running the application, or through your system's environment configuration.

Invalid numeric environment variables or an unrecognized `LOG_LEVEL` cause immediate startup failure with a descriptive error message.

## Environment Variables Reference

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_HOST` | `127.0.0.1` | Server host address (use `0.0.0.0` for all interfaces) |
| `MI_PORT` | `3030` | Server port number |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `MI_LOG_FILE` | `insights.log` | Path to log file |

### MongoDB Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_CONNECTION_STRING` | _(empty)_ | MongoDB connection string (optional, can be provided via UI) |
| `MI_VERIFIER_CONNECTION_STRING` | _(falls back to `MI_CONNECTION_STRING`)_ | MongoDB connection string for the migration verifier database. When omitted, the value of `MI_CONNECTION_STRING` is used. Set this when the verifier database lives on a different cluster. |
| `MI_MIGRATION_VERIFIER_DB_NAME` | `__mdb_internal_migration_verifier` | Standalone migration-verifier metadata database name. |
| `MI_VERIFIER_GENERATION_LIMIT` | `5` | Maximum generations shown on the Migration Verifier dashboard (1–20). |
| `MI_VERIFIER_FAILED_TASKS_LIMIT` | `20` | Maximum failed document tasks shown on the **Failed Tasks / Document Mismatches** card (1–100). |
| `MI_VERIFIER_SUMMARY_MIN_DURATION_SECS` | `0` | Minimum document-mismatch duration (seconds) passed to migration-verifier `/api/v1/summary` (`minDurationSecs` query param). `0` = no filter. |
| `MI_EMBEDDED_VERIFIER_SRC_DB_NAME` | `__mdb_internal_mongosync_verifier_src` | Embedded verifier source persistence database on the destination cluster. |
| `MI_EMBEDDED_VERIFIER_DST_DB_NAME` | `__mdb_internal_mongosync_verifier_dst` | Embedded verifier destination persistence database on the destination cluster. |
| `MI_POOL_SIZE` | `10` | MongoDB connection pool size |
| `MI_TIMEOUT_MS` | `30000` | MongoDB connection timeout in milliseconds |
| `MI_VERIFIER_METADATA_TIMEOUT_MS` | `120000` | Socket timeout in milliseconds for Migration Verifier metadata DB reads (`verification_tasks` aggregations). Does not affect mongosync live monitoring or general `get_database()` calls. |

### Migration Monitoring Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_REFRESH_TIME` | `10` | Base refresh interval in seconds (sidebar **Settings** control). **Migration Monitoring** dashboard polls at this interval. **Migration Verifier** dashboard uses multiples of this value: **progress** = 3× (default 30s), **summary** = 12× (default 120s), **metadata** = 6× (default 60s). Changing the sidebar refresh updates Migration Monitoring directly and rescales all three verifier intervals proportionally. |
| `MI_MONGOSYNC_PROGRESS_TIMEOUT_SECS` | `MI_REFRESH_TIME` | HTTP timeout in seconds for mongosync `/api/v1/progress` polling |
| `MI_VERIFIER_PROGRESS_TIMEOUT_SECS` | `MI_REFRESH_TIME × 3` | HTTP timeout in seconds for migration-verifier `/api/v1/progress` polling |
| `MI_VERIFIER_SUMMARY_TIMEOUT_SECS` | `MI_REFRESH_TIME × 12` | HTTP timeout in seconds for migration-verifier `/api/v1/summary` polling |
| `MI_INDEX_BUILD_REFRESH_TIME` | `60` | Minimum interval in seconds between destination `list_indexes` scans used for approximate metadata index-building progress (counter reads still run every poll). See [MIGRATION_MONITORING.md](MIGRATION_MONITORING.md). |
| `MI_PROGRESS_ENDPOINT_URL` | _(empty)_ | Mongosync progress endpoint as `host:port` or `host:port/api/v1/progress` (default port **27182**; path `/api/v1/progress` is appended if omitted). Optional — can also be set via UI **host** and **port** fields on the Migration monitoring home page. Leave host empty in the UI to skip the endpoint. |
| `MI_VERIFIER_PROGRESS_ENDPOINT_URL` | _(empty)_ | Migration Verifier progress endpoint as `host:port` or `host:port/api/v1/progress` (default port **27020**; path `/api/v1/progress` is appended if omitted). Optional — can also be set via UI **host** and **port** fields on the Migration Verifier form. Leave host empty in the UI to skip the endpoint. |

### File Upload Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_MAX_FILE_SIZE` | `10737418240` | Max upload file size in bytes (10GB) |

### Log Analysis Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_ERROR_PATTERNS_FILE` | `lib/error_patterns.json` | Path to a custom error patterns JSON file for Log Analyzer error detection (e.g., oplog rollover, timeouts, verifier mismatches). Set via environment variable before startup. Each entry may include an optional `recommendation` string, shown in the Errors tab when a line matches that pattern. |

### Log Viewer & Snapshot Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_LOG_VIEWER_MAX_LINES` | `2000` | Maximum number of recent log lines shown in the Log Viewer tail view |
| `MI_LOG_STORE_DIR` | System temp directory | Directory for SQLite log stores and analysis snapshot files |
| `MI_LOG_STORE_MAX_AGE_HOURS` | `24` | TTL in hours for in-memory log store registry entries (`created_at`) and on-disk SQLite stores / snapshot files (file `mtime`) |

> **Note**: By default, log store databases and snapshot files are saved to the OS temp directory (e.g., `/tmp` on Linux/macOS), which may be cleared on system reboot. Set `MI_LOG_STORE_DIR` to a persistent path (e.g., `/data/mongosync-insights/store`) to retain snapshots across restarts. Maintenance runs when the app is initialized (`create_app`, including packaged and `flask run` imports) and on logout: expired registry entries are removed, then old `mi_logstore_*.db` and snapshot files are deleted by age. Listing or loading a saved snapshot also hides or refreshes TTL for files still within the limit (snapshot/DB `mtime` is touched on load). Lower this value if multi-GB log stores accumulate during a session.

### Security Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MI_SECURE_COOKIES` | mirrors `MI_SSL_ENABLED` (`false` when HTTPS is off) | Enable secure cookies (set `true` when using HTTPS) |
| `MI_SESSION_TIMEOUT` | `3600` | Session timeout in seconds (1 hour default) |
| `MI_SSL_ENABLED` | `false` | Enable HTTPS/SSL in Flask application |
| `MI_SSL_CERT` | `/etc/letsencrypt/live/your-domain/fullchain.pem` | Path to SSL certificate file |
| `MI_SSL_KEY` | `/etc/letsencrypt/live/your-domain/privkey.pem` | Path to SSL private key file |

> **Note**: Sessions are stored **in-memory** on the server. All active sessions are lost when the application restarts. This is by design to avoid persisting sensitive data (such as connection strings) to disk. A session may hold both mongosync and Migration Verifier credentials when you use both forms on `/live/`. Use **Logout** to clear the session cookie and stored credentials.

> **Note**: For detailed HTTPS setup instructions, see [HTTPS_SETUP.md](HTTPS_SETUP.md)
>

### Connection String Validation

> **Note**: For connection string handling information, see [CONNECTION_STRING.md](CONNECTION_STRING.md)

---

## Usage Examples

### Example 1: Basic Local Development

Default settings - no environment variables needed:

```bash
# Run with all defaults
python3 mongosync_insights.py

# Access at: http://127.0.0.1:3030
```

### Example 2: Custom Port and Host

Run on a different port and allow external connections:

```bash
# Set custom port and host
export MI_PORT=8080
export MI_HOST=0.0.0.0

# Run the application
python3 mongosync_insights.py

# Access at: http://your-ip:8080
```

### Example 3: Pre-configured MongoDB Connection

Set the MongoDB connection string for metadata-only monitoring, or as metadata fallback when a progress endpoint is also configured:

```bash
# Optional: metadata-only monitoring, or metadata fallback when a progress endpoint is also set
export MI_CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/"

# Optional: Adjust refresh rate
export MI_REFRESH_TIME=5

# Run the application
python3 mongosync_insights.py
```

### Example 3b: Combined Monitoring (Metadata + Progress Endpoint)

Pre-configure both the progress endpoint and connection string for comprehensive monitoring. In the UI, the equivalent is entering a **host** and **port** (default `27182`) on the Migration monitoring home page; leave host empty for metadata-only mode.

```bash
# Set mongosync progress endpoint (host:port or full path; /api/v1/progress is appended if omitted)
export MI_PROGRESS_ENDPOINT_URL="localhost:27182"

# Optional: metadata fallback — destination cluster connection string
export MI_CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/"

# Optional: throttle destination index-name verification scans (metadata fallback)
export MI_INDEX_BUILD_REFRESH_TIME=60

# Faster refresh for active migrations
export MI_REFRESH_TIME=5

# Run the application
python3 mongosync_insights.py
```

### Example 4: Debug Mode with Logging

Enable detailed logging for troubleshooting:

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
export MI_LOG_FILE=/var/log/mongosync-insights/debug.log

# Run the application
python3 mongosync_insights.py

# Tail the log in another terminal
tail -f /var/log/mongosync-insights/debug.log
```

### Example 4b: Custom Log Analyzer Error Patterns

Point Log Analyzer at a custom error-patterns file (absolute path recommended in production):

```bash
export MI_ERROR_PATTERNS_FILE="/etc/mongosync-insights/custom_error_patterns.json"
python3 mongosync_insights.py
```

Each JSON entry requires `pattern` and `friendly_name`; `recommendation` is optional. See the bundled `lib/error_patterns.json` for the schema.

### Example 5: Production Configuration with HTTPS

Secure production setup with HTTPS:

```bash
# Server configuration
export MI_HOST=127.0.0.1  # Behind reverse proxy
export MI_PORT=3030
export LOG_LEVEL=INFO

# Security settings
export MI_SSL_ENABLED=false  # Nginx handles SSL
export MI_SECURE_COOKIES=true

# MongoDB connection (optional: metadata-only, or metadata fallback when a progress endpoint is also set)
export MI_CONNECTION_STRING="mongodb+srv://user:pass@production-cluster.mongodb.net/"

# Performance settings
export MI_POOL_SIZE=20
export MI_TIMEOUT_MS=10000

# Run the application
python3 mongosync_insights.py
```

See [HTTPS_SETUP.md](HTTPS_SETUP.md) for complete production deployment guide.

### Example 6: Custom Upload Size

Adjust the maximum log file upload size:

```bash
# Allow larger log files (20GB)
export MI_MAX_FILE_SIZE=21474836480

# Run the application
python3 mongosync_insights.py
```

### Example 7: Migration Verifier Monitoring

Pre-configure the migration-verifier progress endpoint and/or connection string:

```bash
# Set migration-verifier progress endpoint (host:port or full path; /api/v1/progress is appended if omitted)
export MI_VERIFIER_PROGRESS_ENDPOINT_URL="localhost:27020"

# Optional: metadata fallback — used when MI_VERIFIER_CONNECTION_STRING is not set
export MI_CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/"

# Optional: metadata fallback on a different cluster (falls back to MI_CONNECTION_STRING when omitted)
export MI_VERIFIER_CONNECTION_STRING="mongodb+srv://user:pass@verifier-cluster.mongodb.net/"

# Optional: override migration-verifier metadata database name (default: __mdb_internal_migration_verifier)
export MI_MIGRATION_VERIFIER_DB_NAME="__mdb_internal_migration_verifier"

# Optional: number of generations shown on the verifier dashboard (default: 5, range: 1–20)
export MI_VERIFIER_GENERATION_LIMIT=10

# Cap failed document tasks shown on the dashboard (default 20)
export MI_VERIFIER_FAILED_TASKS_LIMIT=20

# Optional: ignore short-lived doc mismatches in /summary (0 = no filter)
export MI_VERIFIER_SUMMARY_MIN_DURATION_SECS=0

# HTTP timeouts for endpoint polling (seconds; defaults follow refresh intervals)
export MI_MONGOSYNC_PROGRESS_TIMEOUT_SECS=10
export MI_VERIFIER_PROGRESS_TIMEOUT_SECS=30
export MI_VERIFIER_SUMMARY_TIMEOUT_SECS=120

# Socket timeout for verifier metadata DB reads (milliseconds)
export MI_VERIFIER_METADATA_TIMEOUT_MS=120000

# Run the application
python3 mongosync_insights.py
```

**Note**: When `MI_VERIFIER_CONNECTION_STRING` is not set, it falls back to `MI_CONNECTION_STRING`. Set it explicitly when the migration-verifier writes to a different cluster. The verifier metadata database name is not configurable via the UI; use `MI_MIGRATION_VERIFIER_DB_NAME` to override the default. Provide at least one of the progress endpoint or connection string (via env or UI).

### Example 8: Persistent Snapshots and Custom Log Viewer

Configure snapshot storage location, retention period, and log viewer buffer size:

```bash
# Store snapshots in a persistent directory
export MI_LOG_STORE_DIR=/data/mongosync-insights/store

# Keep snapshots for 48 hours instead of the default 24
export MI_LOG_STORE_MAX_AGE_HOURS=48

# Show up to 5000 recent log lines in the Log Viewer tail view
export MI_LOG_VIEWER_MAX_LINES=5000

# Run the application
python3 mongosync_insights.py
```

---

## Troubleshooting

### Environment Variables Not Taking Effect

**Problem**: Changed environment variables but application uses defaults

**Solution**: 
```bash
# Verify variables are set
env | grep MI_

# Use -E flag with sudo to preserve environment
sudo -E python3 mongosync_insights.py
```

### Connection String Not Working

**Problem**: `MI_CONNECTION_STRING` is set but application still asks for it

**Solution**: 
- Verify the connection string format: `mongodb+srv://user:pass@cluster.mongodb.net/`
- Check for extra quotes or spaces
- Test connection string with `mongosh` first
- Pre-set env vars apply to Migration monitoring when not overridden by a session; clear cookies or restart the app if a prior session is cached

### Progress Endpoint Not Responding

**Problem**: Migration Monitoring shows a progress-endpoint warning

**Solution**:
- Confirm mongosync is running and its API is reachable on the host/port you entered (default port **27182**)
- Verify `MI_PROGRESS_ENDPOINT_URL` uses `host:port` or `host:port/api/v1/progress` (no `http://` required in env)
- Metadata-only monitoring still works if `MI_CONNECTION_STRING` is set; leave the progress **host** empty in the UI to skip the endpoint intentionally

### Verifier Metadata Connection Errors

**Problem**: Migration Verifier dashboard shows a generic metadata connection error

**Solution**:
- The UI shows generic messages such as `Could not connect to verifier database.` — driver details are written to `insights.log` only
- Verify `MI_VERIFIER_CONNECTION_STRING` (or `MI_CONNECTION_STRING` fallback) with `mongosh`
- Check network access, credentials, and that `MI_MIGRATION_VERIFIER_DB_NAME` exists on the cluster

### Log File Permission Denied

**Problem**: Cannot write to log file location

**Solution**:
```bash
# Use writable location
export MI_LOG_FILE=$HOME/mongosync-insights.log

# Or create directory with proper permissions
sudo mkdir -p /var/log/mongosync-insights
sudo chown $USER:$USER /var/log/mongosync-insights
export MI_LOG_FILE=/var/log/mongosync-insights/insights.log
```

---

## Related Documentation

- **[README.md](README.md)** - Getting started and installation guide
- **[LOG_ANALYZER.md](LOG_ANALYZER.md)** - Log Analyzer feature guide
- **[MIGRATION_MONITORING.md](MIGRATION_MONITORING.md)** - Migration Monitoring feature guide
- **[HTTPS_SETUP.md](HTTPS_SETUP.md)** - Enable HTTPS/SSL for secure deployments
- **[CONNECTION_STRING.md](CONNECTION_STRING.md)** - Connection string formats, security, and troubleshooting

### License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

DISCLAIMER
----------
Please note: all tools/ scripts in this repo are released for use "AS IS" **without any warranties of any kind**,
including, but not limited to their installation, use, or performance.  We disclaim any and all warranties, either 
express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness 
for a particular purpose.  We do not warrant that the technology will meet your requirements, that the operation 
thereof will be uninterrupted or error-free, or that any errors will be corrected.

Any use of these scripts and tools is **at your own risk**.  There is no guarantee that they have been through 
thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with 
their use.

You are responsible for reviewing and testing any scripts you run *thoroughly* before use in any non-testing 
environment.

Thanks,  
The MongoDB Support Team