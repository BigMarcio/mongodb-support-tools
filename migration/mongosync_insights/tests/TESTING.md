# Mongosync Insights — Automated Test Report

**Scope:** `migration/mongosync_insights/tests/`  
**Framework:** pytest (with some `unittest.TestCase` classes)  
**CI:** [`.github/workflows/mongosync-insights-tests.yml`](../../../.github/workflows/mongosync-insights-tests.yml) — runs `python -m pytest tests/ -q` on Python 3.11  
**Total:** **736 tests** across **33 test files** (+ `conftest.py` shared fixtures)

---

## Summary by Area

| Area | Files | Tests | What is validated |
|------|------:|------:|-------------------|
| App config & bootstrap | 4 | 116 | Env parsing, endpoint URLs, host allowlist, file classification, sessions, app factory |
| Security & paths | 3 | 32 | Connection string sanitization, inline JSON XSS, store ID validation, path traversal |
| Log storage & routes | 9 | 140 | Upload, search, decompress, snapshots, FTS, timestamps, Summary tab payload, CEA busiest collections |
| Metrics & OTEL parsing | 2 | 58 | MIME detection, phase events, Prometheus/OTEL log parsing |
| Live monitoring | 6 | 185 | Progress/verifier API clients, routes, dashboard, metadata, formatting, toolbar badges |
| Migration metadata UI | 8 | 116 | Verification mode, index building, filters, natural order, phases |
| Migration verifier | 1 | 89 | MongoDB verifier queries, badges, payloads, manual summary, mismatch downloads |
| **Total** | **33** | **736** | |

---

## How to Run

```bash
cd migration/mongosync_insights
pip install -r requirements.txt -r requirements-dev.txt
python3 -m pytest tests/ -q          # all 736 tests
python3 -m pytest tests/ --collect-only -q   # list without running (verify current count)
python3 -m pytest tests/test_log_time.py -v  # single file
```

---

## Test Files (detail)

### 1. App config & bootstrap (116 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_app_config.py` | 106 | `MI_*` env vars, progress/verifier endpoint URL building, host allowlist, file type classification, archive detection, error patterns, in-memory sessions, DB name resolution, connection validation |
| `test_create_app.py` | 5 | Flask app creation, blueprint registration, hub route, security headers |
| `test_session_support.py` | 3 | Session create/update, cookie name |
| `test_connection_cache.py` | 2 | MongoDB client/internal DB cache clearing on validation failure |

### 2. Security & paths (32 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_connection_validator.py` | 5 | Strips credentials from URIs, HTML-escapes hosts, fallback for malformed URIs |
| `test_store_paths.py` | 17 | UUID store IDs, path traversal rejection, poisoned snapshot JSON, registry path constraints |
| `test_inline_json_xss.py` | 10 | Inline JSON XSS escaping in templates/JS payloads, uploaded log sinks, stored snapshots |

### 3. Log storage & routes (140 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_log_store.py` | 14 | Insert, FTS search, pagination, timestamp range queries (with/without Z), delete |
| `test_log_store_registry.py` | 4 | Store open/cache hit-miss, expiry, maintenance cleanup |
| `test_log_time.py` | 7 | Log search datetime parsing, start/end bound normalization |
| `test_log_summary.py` | 60 | Latest `/progress` extraction, log-derived metadata, Summary tab payload, index/verification backfill |
| `test_busiest_collections.py` | 11 | CEA busiest collections parsing, ranking, plot payload |
| `test_file_decompressor.py` | 20 | gzip/bzip2/tar/zip decompression, macOS metadata skipping, MIME routing |
| `test_logs_routes.py` | 17 | `/logs` home, upload, search (text + timestamp, no-Z stored logs), snapshot list/load/delete |
| `test_upload_fixtures.py` | 3 | End-to-end upload + search with sample fixtures |
| `test_snapshot_store.py` | 4 | Save/load round-trip, list by mtime, delete, cleanup |

### 4. Metrics & OTEL parsing (58 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_logs_metrics.py` | 40 | MIME sniffing, phase event extraction from logs/API, merge logic, commit/write flags, replication lag |
| `test_otel_metrics.py` | 18 | Prometheus label/message parsing, metrics log lines, collector histograms, config/titles |

### 5. Live monitoring (185 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_live_monitoring.py` | 54 | `fetch_progress` / `fetch_summary` / verifier progress (timeouts, redirects, host allowlist), state badges, display builders, duration hover titles |
| `test_live_routes.py` | 47 | `/live` home, unified monitor POST, routing (migration/dashboard/verifier), dashboard routes, progress monitor |
| `test_live_metadata_status.py` | 26 | Lag time, write-blocking mode, sync phase normalization, progress gating (index/verification), partition byte totals |
| `test_data_sources.py` | 6 | Progress API / Metadata toolbar badges when endpoint/metadata configured or unavailable |
| `test_utils.py` | 46 | Byte/count/lag/ratio formatting helpers, seconds hover titles, replication lag resolution |
| `test_plot_theme.py` | 6 | Light/dark theme tokens, annotation styles, Plotly template registration |

### 6. Migration metadata UI (116 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_verification_mode.py` | 33 | Embedded verifier mode descriptions, info vs progress cards, toolbar badges, persistence fallback display |
| `test_index_building_info.py` | 10 | `buildIndexes` policy descriptions, info card vs live progress precedence |
| `test_index_correction_totals.py` | 28 | Index correction rollups, destination index verification, CEA phase gating, metadata vs progress precedence |
| `test_index_build_destination_cache.py` | 8 | Collection UUID keys, scan throttling, cache hit/miss for extra-built indexes |
| `test_filtered_migration.py` | 13 | Namespace inclusion/exclusion filters, active detection, display cards |
| `test_natural_order.py` | 8 | `copyInNaturalOrder` filter parsing and display |
| `test_phase_start_times.py` | 8 | Phase transition timestamps from metadata, sync card integration |
| `test_verifier_persistence_fallback.py` | 8 | CV phase rollup from MongoDB persistence, fallback when progress endpoint unavailable |

### 7. Migration verifier (89 tests)

| File | Tests | Validates |
|------|------:|-----------|
| `test_migration_verifier.py` | 89 | Generation naming/lookup, metadata version warnings, failed tasks, namespace stats, completeness rules, state badges (pass/mismatches/in-progress), metadata/progress/summary payloads, manual summary run/cooldown, mismatch download streams, lean mode, partial failure resilience, plot rendering |

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

## Full Test Inventory (736 tests)

Per-file totals (run `python3 -m pytest tests/ --collect-only -q` to verify):

| File | Tests |
|------|------:|
| `test_app_config.py` | 106 |
| `test_busiest_collections.py` | 11 |
| `test_connection_cache.py` | 2 |
| `test_connection_validator.py` | 5 |
| `test_create_app.py` | 5 |
| `test_data_sources.py` | 6 |
| `test_file_decompressor.py` | 20 |
| `test_filtered_migration.py` | 13 |
| `test_index_build_destination_cache.py` | 8 |
| `test_index_building_info.py` | 10 |
| `test_index_correction_totals.py` | 28 |
| `test_inline_json_xss.py` | 10 |
| `test_live_metadata_status.py` | 26 |
| `test_live_monitoring.py` | 54 |
| `test_live_routes.py` | 47 |
| `test_log_store.py` | 14 |
| `test_log_store_registry.py` | 4 |
| `test_log_summary.py` | 60 |
| `test_log_time.py` | 7 |
| `test_logs_metrics.py` | 40 |
| `test_logs_routes.py` | 17 |
| `test_migration_verifier.py` | 89 |
| `test_natural_order.py` | 8 |
| `test_otel_metrics.py` | 18 |
| `test_phase_start_times.py` | 8 |
| `test_plot_theme.py` | 6 |
| `test_session_support.py` | 3 |
| `test_snapshot_store.py` | 4 |
| `test_store_paths.py` | 17 |
| `test_upload_fixtures.py` | 3 |
| `test_utils.py` | 46 |
| `test_verification_mode.py` | 33 |
| `test_verifier_persistence_fallback.py` | 8 |
| **Total** | **736** |

### Notable test classes (by file)

**`test_app_config.py`** — `TestParseEnvInt`, `TestProgressEndpointUrl`, `TestVerifierHeavyApiTimeout`, `TestEndpointHostAllowlist`, `TestClassifyFileType` (parametrized), `TestProbeMetadataDatabases`, `TestValidateApiEndpointUrl`

**`test_busiest_collections.py`** — `TestBusiestCollectionsAccumulator`, `TestBuildBusiestCollectionsPlot`

**`test_inline_json_xss.py`** — `TestInlineJsonFilter`, `TestUploadedLogSinks`, `TestStoredSnapshots`

**`test_live_monitoring.py`** — `TestFetchProgress`, `TestEndpointRedirectAndValidation`, `TestFetchSummary`, `TestStreamVerifierMismatches`, `TestBuildHelpers`

**`test_live_routes.py`** — `TestMonitorClassification`, `TestDashboardRoutes`, `TestVerifierRoutes`

**`test_log_summary.py`** — `TestExtractLatestProgress`, `TestMergeIndexBuildingIntoProgress`, `TestBuildLogSummaryPayload`

**`test_migration_verifier.py`** — `TestVerifierRunSummaryResponse`, `TestProgressAllowsSummaryRun`, `TestVerifierProgressEndpointPayload`, `TestBuildVerifierMetadataPayload`

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

### Toolbar badges (`test_data_sources.py`)

Validates Progress API / Metadata toolbar badges on live dashboards:

- Endpoint-only, metadata-only, and both configured
- Custom labels and unavailable status defaults
- No hostnames or connection strings in badge payloads
