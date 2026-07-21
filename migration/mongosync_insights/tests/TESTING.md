# MongoSync Insights — Automated Test Report

**Scope:** `migration/mongosync_insights/tests/`  
**Framework:** pytest (with some `unittest.TestCase` classes)  
**CI:** [`.github/workflows/mongosync-insights-tests.yml`](../../../.github/workflows/mongosync-insights-tests.yml) — runs `python -m pytest tests/ -q` on Python 3.11  
**Total:** **509 tests** across **30 test files** (+ `conftest.py` shared fixtures)

---

## Summary by Area

| Area | Files | Tests | What is validated |
|------|------:|------:|-------------------|
| App config & bootstrap | 4 | 88 | Env parsing, endpoint URLs, file classification, sessions, app factory |
| Security & paths | 2 | 22 | Connection string sanitization, store ID validation, path traversal |
| Log storage & routes | 7 | 69 | Upload, search, decompress, snapshots, FTS, timestamps |
| Metrics & OTEL parsing | 2 | 39 | MIME detection, phase events, Prometheus/OTEL log parsing |
| Live monitoring | 6 | 103 | Progress/verifier API clients, routes, metadata, formatting, charts |
| Migration metadata UI | 8 | 114 | Verification mode, index building, filters, natural order, phases |
| Migration verifier | 1 | 75 | MongoDB verifier queries, badges, payloads, generation history |
| **Total** | **30** | **509** | |

---

## How to Run

```bash
cd migration/mongosync_insights
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests/ -q          # all 509 tests
python -m pytest tests/ --collect-only -q   # list without running
python -m pytest tests/test_log_time.py -v  # single file
```

---

## Test Files (detail)

### 1. App config & bootstrap (88 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_app_config.py` | 80 | `MI_*` env vars, progress/verifier endpoint URL building, file type classification, archive detection, error patterns, in-memory sessions, DB name resolution, connection validation |
| `test_create_app.py` | 3 | Flask app creation, blueprint registration, hub route |
| `test_session_support.py` | 3 | Session create/update, cookie name |
| `test_connection_cache.py` | 2 | MongoDB client/internal DB cache clearing on validation failure |

### 2. Security & paths (22 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_connection_validator.py` | 5 | Strips credentials from URIs, HTML-escapes hosts, fallback for malformed URIs |
| `test_store_paths.py` | 17 | UUID store IDs, path traversal rejection, poisoned snapshot JSON, registry path constraints |

### 3. Log storage & routes (69 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_log_store.py` | 11 | Insert, FTS search, pagination, timestamp range queries, delete |
| `test_log_store_registry.py` | 4 | Store open/cache hit-miss, expiry, maintenance cleanup |
| `test_log_time.py` | 5 | Log search datetime parsing, start/end bound normalization |
| `test_file_decompressor.py` | 20 | gzip/bzip2/tar/zip decompression, macOS metadata skipping, MIME routing |
| `test_logs_routes.py` | 15 | `/logs` home, upload, search (text + timestamp), snapshot list/load/delete |
| `test_upload_fixtures.py` | 3 | End-to-end upload + search with sample fixtures |
| `test_snapshot_store.py` | 4 | Save/load round-trip, list by mtime, delete, cleanup |

### 4. Metrics & OTEL parsing (39 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_logs_metrics.py` | 21 | MIME sniffing, phase event extraction from logs/API, merge logic, commit/write flags |
| `test_otel_metrics.py` | 18 | Prometheus label/message parsing, metrics log lines, collector histograms, config/titles |

### 5. Live monitoring (103 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_live_monitoring.py` | 21 | `fetch_progress` / `fetch_summary` / verifier progress (timeouts, HTTP errors), state badges, display builders |
| `test_live_routes.py` | 19 | `/live` home, monitoring POST, progress monitor, verifier POST/GET routes |
| `test_live_metadata_status.py` | 24 | Lag time, write-blocking mode, sync phase normalization, progress gating (index/verification), partition byte totals |
| `test_data_sources.py` | 6 | Data source card when endpoint/metadata configured or unavailable |
| `test_utils.py` | 33 | Byte/count/lag/ratio formatting helpers |
| `test_plot_theme.py` | 6 | Light/dark theme tokens, annotation styles, Plotly template registration |

### 6. Migration metadata UI (114 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_verification_mode.py` | 32 | Embedded verifier mode descriptions, info vs progress cards, toolbar badges, persistence fallback display |
| `test_index_building_info.py` | 10 | `buildIndexes` policy descriptions, info card vs live progress precedence |
| `test_index_correction_totals.py` | 28 | Index correction rollups, destination index verification, CEA phase gating, metadata vs progress precedence |
| `test_index_build_destination_cache.py` | 8 | Collection UUID keys, scan throttling, cache hit/miss for extra-built indexes |
| `test_filtered_migration.py` | 12 | Namespace inclusion/exclusion filters, active detection, display cards |
| `test_natural_order.py` | 8 | `copyInNaturalOrder` filter parsing and display |
| `test_phase_start_times.py` | 8 | Phase transition timestamps from metadata, sync card integration |
| `test_verifier_persistence_fallback.py` | 8 | CV phase rollup from MongoDB persistence, fallback when progress endpoint unavailable |

### 7. Migration verifier (75 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_migration_verifier.py` | 75 | Generation naming/lookup, metadata version warnings, failed tasks, namespace stats, completeness rules, state badges (pass/mismatches/in-progress), metadata/progress/summary payloads, lean mode, partial failure resilience, plot rendering |

---

## Shared Test Infrastructure

`conftest.py` provides:

- **`app_client`** — isolated Flask test client with temp log/snapshot directories
- **`sample_progress`** — mock mongosync progress JSON
- Additional fixtures for verifier progress, metadata, etc.

Fixtures under `tests/fixtures/`:

- `sample_mongosync.log`
- `sample_mongosync_metrics.log`

---

## Full Test Inventory (509 tests)

### `test_app_config.py` (80)

- `TestParseEnvInt` (6): default, override, empty, non-integer, below/above min
- `TestValidateConfig` (4): log level, progress endpoint URL validation
- `TestProgressEndpointUrl` (10): build/normalize/validate host:port URLs
- `TestVerifierProgressEndpointUrl` (4)
- `TestVerifierSummaryEndpointUrl` (4)
- `TestVerifierSummaryMinDuration` (2)
- `TestVerifierMetadataTimeout` (2)
- `TestVerifierRefreshIntervals` (1)
- `TestFetchTimeouts` (6)
- `TestClassifyFileType` (14 parametrized)
- `TestIsMultiFileArchive` (5 parametrized)
- `TestLoadErrorPatterns` (4)
- `TestInMemorySessionStore` (3)
- `TestResolveMongosyncDbName` (2)
- `TestMigrationVerifierDbName` (2)
- `TestVerifierGenerationLimit` (2)
- `TestVerifierFailedTasksLimit` (2)
- `TestValidateConnection` (2)
- `TestValidateProgressEndpointUrlRejects` (6 parametrized)

### `test_connection_cache.py` (2)

- `test_clear_connection_cache_clears_internal_db_and_client_cache`
- `test_validate_connection_failure_clears_both_caches`

### `test_connection_validator.py` (5)

- Credential stripping, URI without DB, HTML escaping, malformed URI fallback, empty string fallback

### `test_create_app.py` (3)

- Returns Flask app, blueprints registered, hub route

### `test_data_sources.py` (6)

- None/endpoint-only/metadata-only/both/custom labels/default unavailable status

### `test_file_decompressor.py` (20)

- Archive member skipping (6 parametrized), zip/gzip/bzip2/tar decompression, extension/MIME helpers, file routing

### `test_filtered_migration.py` (12)

- Namespace filter row formatting, active detection, display card inclusion/omission

### `test_index_build_destination_cache.py` (8)

- UUID key handling, scan throttle, cache hit/miss, empty pending groups

### `test_index_building_info.py` (10)

- Build-indexes policy descriptions, info card, live progress precedence

### `test_index_correction_totals.py` (28)

- Index correction rollups, destination verification, phase gating, metadata vs progress display integration

### `test_live_metadata_status.py` (24)

- Lag time, write-blocking mode (4 parametrized), sync phases (5 parametrized), progress gating (8 parametrized), verification mode keys, partition byte totals

### `test_live_monitoring.py` (21)

- Fetch progress/summary/verifier (success + error paths), state badges, display builders, no-config response shape

### `test_live_routes.py` (19)

- Live home, monitoring POST validation, progress monitor, verifier POST/GET endpoints

### `test_log_store.py` (11)

- Insert, FTS, pagination, timestamp gte/lte/range, latest lines, delete

### `test_log_store_registry.py` (4)

- Open hit/miss, duplicate register, expired entry, maintenance cleanup

### `test_log_time.py` (5)

- Parse datetime, Z suffix, reject empty, normalize start/end bounds

### `test_logs_metrics.py` (21)

- MIME detection (7 parametrized), phase events from info/in-memory/API, merge, progress flags

### `test_logs_routes.py` (15)

- Logs home, upload (plain/reject/no file), search (happy path, timestamps, validation), snapshots

### `test_migration_verifier.py` (75)

- Generation naming/lookup, metadata warnings, summary aggregation, mismatches, failed tasks, namespace stats, completeness, history, state badges, metadata/progress/summary payloads, lean mode, resilience, plot rendering

### `test_natural_order.py` (8)

- Filter parsing (dbs/colls, empty, select-all), display inclusion/omission

### `test_otel_metrics.py` (18)

- Label parsing (3 parametrized), Prometheus messages (5), metrics log lines (4), collector (2), config (4)

### `test_phase_start_times.py` (8)

- Phase transition row formatting, sync card phase start times inclusion/omission

### `test_plot_theme.py` (6)

- Light/dark tokens, annotation styles, apply/register Plotly theme

### `test_session_support.py` (3)

- Update existing session, create new, cookie name

### `test_snapshot_store.py` (4)

- Save/load, list by mtime, delete, cleanup old

### `test_store_paths.py` (17)

- Store ID validation (9 parametrized), safe path under base, logstore path, poisoned JSON, registry path constraints

### `test_upload_fixtures.py` (3)

- Upload sample log, upload metrics fixture, search after upload

### `test_utils.py` (33)

- `format_bytes_compact` (9), `format_lag_time_seconds` (7), `format_count` (5), `format_ratio` (2), `format_byte_size` (5), `convert_bytes` (5) — all parametrized

### `test_verification_mode.py` (32)

- Mode descriptions, progress data detection, info cards, display resolution, persistence fallback, side completion, toolbar badges

### `test_verifier_persistence_fallback.py` (8)

- CV phase rollup (4), fetch persistence status (4)

---

## Examples of What Is Tested

### Log timestamp parsing (`test_log_time.py`)

Validates that user-entered log search timestamps are parsed and normalized correctly for filtering logs:

- Parsing `"2026-01-01T10:30:45"` to expected date/time components
- `"2026-01-01T10:30:45.000Z"` keeps the same hour/minute (no unintended shift)
- Empty input raises `InvalidLogTimestamp`
- Start times get `.000` appended (or preserved milliseconds, no `Z`) so bounds match stored timestamps with or without `Z`
- End times get a `Z` suffix on the selected second (inclusive ceiling for lexicographic compare)

### Connection string sanitization (`test_connection_validator.py`)

Validates that MongoDB connection strings are safely shown in the UI without leaking credentials:

- Username/password are removed; hosts and database name remain visible
- Malicious hostnames like `host<script>` are escaped to `&lt;script&gt;`
- Invalid or empty URIs return `"[Connection String Provided]"` instead of crashing or exposing raw input
