# Migration Monitoring

Migration Monitoring is the real-time dashboard for ongoing **mongosync** cluster-to-cluster migrations. Open it from the hub (**Migration monitoring** → **Open migration monitoring**) or go directly to `/live/`.

![Migration monitoring home](images/mongosync_insights_monitoring_home.png)

## Unified setup form (`/live/`)

The home page uses a **sidebar + form** layout: a left guide (**What you'll monitor**) explains the three monitoring areas, and the form panels sit on the right. On narrow screens the guide stacks above the form.

The form has a single **Monitor** button:

1. **Mongosync progress endpoint** (primary)
2. **Destination MongoDB connection string** (optional) — used for mongosync and/or standalone verifier metadata when the corresponding internal databases are found on the destination cluster
3. **Using mongosync Embedded Verifier (recommended)** — checked by default. Unchecking does not disable embedded verifier in mongosync; it only enables monitoring the standalone [migration-verifier](https://github.com/mongodb-labs/migration-verifier) tool
4. When unchecked: **Migration Verifier progress endpoint** fields appear (optional; no second connection string)

At least one of mongosync progress endpoint, connection string, or Migration Verifier progress endpoint is required.

On submit, MI probes the destination cluster once (if a connection string is provided) to detect `__mdb_internal_mongosync` / legacy mongosync metadata and `__mdb_internal_migration_verifier`.

### Routing after setup

| Inputs | Destination | Top navigation |
|--------|-------------|----------------|
| Mongosync progress endpoint only | Full Migration Monitoring (`/live/migration`) | Hidden |
| Mongosync progress endpoint + connection string | Full Migration Monitoring | Hidden |
| Mongosync progress endpoint + standalone verifier endpoint | Dashboard (`/live/dashboard`) | Dashboard, Full Migration, Full Verifier |
| Mongosync progress endpoint + connection string + standalone verifier endpoint | Dashboard | Dashboard, Full Migration, Full Verifier |
| Connection string + standalone verifier endpoint (no mongosync progress endpoint) | If mongosync internal DB exists on cluster: Full Migration Monitoring. Otherwise: Full Verifier Monitoring (`/live/verifier/view`) | Full Migration + Full Verifier only when mongosync DB exists; hidden otherwise |
| Standalone verifier endpoint only | Full Verifier Monitoring | Hidden |
| Connection string only | Full Migration Monitoring (metadata-only) | Hidden |

Uncheck **Using mongosync Embedded Verifier** to enable standalone Migration Verifier endpoint fields. When only the verifier endpoint is provided (no mongosync inputs), the form asks you to confirm before proceeding.

## Monitoring screens

| Screen | Route | Content |
|--------|-------|---------|
| **Dashboard** | `GET /live/dashboard` | Compact combined view: **Migration progress** card on top (progress endpoint only), **standalone verifier progress** card below when embedded verifier is unchecked and a verifier progress endpoint is set. Top navigation links to Full Migration and Full Verifier open in a new tab from each section toolbar |
| **Full Migration Monitoring** | `GET /live/migration` | All cards: sync, index building, embedded verifier, filters, metadata fallbacks |
| **Full Migration Verifier** | `GET /live/verifier/view` | Progress, metadata, mismatch summary, downloads |

The top navigation bar appears only when multiple screens are relevant (see routing table above). When hidden, you stay on a single focused monitoring view. On the Dashboard, full-page links in each section toolbar open in a new tab.

Each monitoring screen shows **Progress API** / **Metadata** toolbar badges when those sources are configured — not hostnames, ports, or connection strings.

## What the full migration dashboard shows

The full Migration Monitoring page polls mongosync and/or the destination metadata database and renders cards for:

- **Migration state** — coordinator state badge (e.g. `RUNNING`, `PAUSED`, `COMMITTED`)
- **Migration progress** — copy phase, bytes/collections/partitions, lag time, phase start times
- **Index building** — indexes rebuilt on the destination after the initial copy
- **Embedded Verifier** — mongosync's built-in verifier (when enabled)
- **Filtered migration** — namespace include/exclude filters and natural-order copy settings (from metadata)

Refresh interval defaults to **10 seconds** (`MI_REFRESH_TIME`). The browser polls Mongosync Insights only; MI's backend fetches mongosync's progress API and/or the destination metadata database. Sidebar **Settings** overrides the interval for this browser only.

![Migration monitoring dashboard](images/mongosync_insights_monitoring.png)

## Configuration inputs

You can provide **one or both** of mongosync progress endpoint and connection string on the unified form (or via environment variables). The endpoint is the primary live source and the connection string is an optional complement.

| Input | Purpose | Env variable |
|-------|---------|--------------|
| **Mongosync progress endpoint** | Poll mongosync's HTTP progress API (`/api/v1/progress`) | `MI_PROGRESS_ENDPOINT_URL` |
| **MongoDB connection string** | Read mongosync internal metadata on the destination cluster (`resumeData`, `globalState`, `indexCorrection`, verifier persistence DBs). Also used for standalone verifier metadata when `__mdb_internal_migration_verifier` is found | `MI_CONNECTION_STRING` |

When environment variables are set, the matching form fields are read-only with an info message. If any verifier env is set (`MI_VERIFIER_PROGRESS_ENDPOINT_URL` or `MI_VERIFIER_CONNECTION_STRING`), the embedded verifier checkbox is unchecked and verifier fields are shown as pre-configured.

The mongosync internal metadata database name is **auto-detected** on the destination cluster: `__mdb_internal_mongosync` (new) when present, otherwise `mongosync_reserved_for_internal_use` (legacy). This is not configurable.

### Progress endpoint (UI)

When not set via `MI_PROGRESS_ENDPOINT_URL`, the form asks for:

- **Host** — hostname or IP (e.g. `localhost`). Leave **empty** to skip the progress endpoint and use metadata-only monitoring.
- **Port** — defaults to **27182** (mongosync's default API port).

The path `/api/v1/progress` is fixed and appended automatically; do not enter it in the UI.

### Progress endpoint (environment)

`MI_PROGRESS_ENDPOINT_URL` accepts either form:

```bash
export MI_PROGRESS_ENDPOINT_URL="localhost:27182"
# or
export MI_PROGRESS_ENDPOINT_URL="localhost:27182/api/v1/progress"
```

A leading `http://` or `https://` prefix is stripped on load.

## Data source priority

When **both** the progress endpoint and connection string are configured:

1. **Progress endpoint** is the primary source for in-memory progress (copy stats, index building tracker, embedded verifier snapshot from mongosync).
2. **Metadata database** fills gaps when the progress endpoint is unavailable, or when the endpoint does not expose index-building or verification data.

If the progress endpoint fails to respond, the dashboard still loads metadata when a connection string is available, and shows a progress-endpoint warning.

## Index building (metadata fallback)

When the progress endpoint is not used or does not report index-building data, MI reads **`indexCorrection`** counters from the internal metadata database.

If counters lag behind reality (indexes already exist on the destination but counters are pending), MI optionally verifies pending indexes by **index name** on the destination cluster. That fallback is **approximate** — the Index building card shows:

> *Progress from metadata (approximate).*

Control how often destination `list_indexes` scans run with:

```bash
export MI_INDEX_BUILD_REFRESH_TIME=60   # seconds; default 60
```

The counter aggregate still runs on every poll; only the destination verification scan is throttled.

Index-building progress is suppressed when `buildIndexes` is `never`, or before the copy phase reaches the appropriate stage (CEA gating).

## Embedded Verifier vs migration-verifier tool

| Feature | Where | Data source |
|---------|-------|-------------|
| **Embedded Verifier** card | Full Migration Monitoring page | Progress endpoint and/or mongosync verifier persistence on the destination (`MI_EMBEDDED_VERIFIER_SRC_DB_NAME` / `MI_EMBEDDED_VERIFIER_DST_DB_NAME`). Requires mongosync **verifier persistence** (`enableVerifierPersistence`). |
| **Standalone Migration Verifier** | Dashboard (progress card) and Full Migration Verifier page | External [migration-verifier](https://github.com/mongodb-labs/migration-verifier) tool HTTP `/api/v1/progress` (`MI_VERIFIER_PROGRESS_ENDPOINT_URL`) and/or metadata (`MI_MIGRATION_VERIFIER_DB_NAME`) via the shared connection string |

When metadata is used for embedded verifier progress, the card notes that progress is **approximate**.

## Migration Verifier monitoring

Uncheck **Using mongosync Embedded Verifier** on the unified form to optionally provide a standalone Migration Verifier progress endpoint. Verifier fields are optional — the form does not error if they are left empty.

Provide a verifier progress endpoint and/or rely on the shared connection string when the verifier metadata database is found:

| Input | Purpose | Env variable |
|-------|---------|--------------|
| **Migration Verifier progress endpoint** | Poll migration-verifier's HTTP progress API (`/api/v1/progress`) for live phase, generation stats, change-stream lag, and task counts | `MI_VERIFIER_PROGRESS_ENDPOINT_URL` |
| **MongoDB connection string** | Read verifier metadata for generation history, namespace progress, and mismatches | `MI_VERIFIER_CONNECTION_STRING` (falls back to `MI_CONNECTION_STRING`) |

At least one must be configured to start a session.

### Progress endpoint (UI)

When not set via `MI_VERIFIER_PROGRESS_ENDPOINT_URL`, the form asks for:

- **Host** — hostname or IP (e.g. `localhost`). Leave **empty** to skip the progress endpoint and use metadata-only monitoring.
- **Port** — defaults to **27020** (migration-verifier's default API port).

The path `/api/v1/progress` is fixed and appended automatically; do not enter it in the UI.

### Progress endpoint (environment)

```bash
export MI_VERIFIER_PROGRESS_ENDPOINT_URL="localhost:27020"
# or
export MI_VERIFIER_PROGRESS_ENDPOINT_URL="localhost:27020/api/v1/progress"
```

Metadata is read from `MI_MIGRATION_VERIFIER_DB_NAME` (default `__mdb_internal_migration_verifier`). For **migration-verifier** version compatibility, see [README.md — Migration Verifier compatibility](README.md#migration-verifier-compatibility) (tested with **0.2.4**).

### How the dashboard updates

The dashboard JavaScript only talks to Mongosync Insights. MI's backend fetches migration-verifier's HTTP APIs and the metadata database on your behalf.

The page refreshes two areas on different schedules. The footer shows the current intervals (`Progress: Xs · Metadata: Ys`). Use the sidebar **Settings** control to change the base refresh rate — both intervals scale together. Settings stores the refresh interval in your **browser session** (`sessionStorage`). It does not change server environment variables; reload defaults to `MI_REFRESH_TIME` unless you change Settings again.

| Area | Default interval | You need | What it updates |
|------|------------------|----------|-----------------|
| **Progress** | 30s (`MI_REFRESH_TIME × 3`) | Progress endpoint | **Verification Progress** card, toolbar status badge, and mismatch-summary eligibility state |
| **Metadata** | 60s (`× 6`) | Connection string | **Generation overview** and related history cards |

**Mismatch summary** (Document / Namespace Mismatches Summary cards) is **not** auto-polled. When Verification Progress reports mismatches (generation &gt; 0 and metadata or document mismatches present), use **Run mismatch summary** above the cards. That call hits migration-verifier `/api/v1/summary` (can take several minutes). Results are cached in your server session until mismatches clear or you log out. After a successful run, the same action is blocked for **600 seconds** (configurable via `MI_VERIFIER_HEAVY_API_TIMEOUT_SECS`); the UI shows last-updated time and cooldown remaining.

If you did not configure a source, that area is not polled (for example, no connection string → no metadata cards).

### With a progress endpoint (recommended)

When migration-verifier’s HTTP API is configured, the dashboard focuses on live data:

1. **Verification Progress** — current phase, generation, document/byte progress, task counts, throughput, and change-stream lag.
2. **Document Mismatches Summary** / **Namespace Mismatches Summary** — live mismatch tallies from the verifier after you run **Run mismatch summary** (manual; see above). When mismatches exist, each card shows a **Download** link that streams the full list as NDJSON (`doc-mismatches.ndjson` or `ns-mismatches.ndjson`) via migration-verifier `/api/v1/docMismatches` and `/api/v1/nsMismatches`.
3. **Generation overview** (if a connection string is also set) — history table for past generations.

The toolbar badge shows overall status:

| Badge | Meaning |
|-------|---------|
| **In Progress** (blue) | Initial verification (generation 0) is still running |
| **Pass** (green) | Recheck finished with no mismatches |
| **Mismatches** (yellow) | Recheck finished but mismatches were found |

When both the endpoint and connection string are set, the live **Verification Progress** card is primary and mismatch summary cards are filled only after **Run mismatch summary**; some metadata-only cards are hidden because the live progress card already covers that information.

![Migration Verifier dashboard — progress endpoint only](images/live_verifier_endpoint_only.png)

### Metadata only (no progress endpoint)

If you only provide a connection string, MI reads the verifier metadata database instead. You get a fuller set of history cards (completeness summary, generation overview, namespace progress, failed tasks, collection metadata mismatches). The toolbar badge is derived from the last completed generation in metadata.

![Migration Verifier dashboard — metadata only](images/live_verifier_metadata_only.png)

### Errors and partial data

- If the **progress endpoint** is unreachable but a connection string is set, metadata cards still update and a warning is shown for the endpoint.
- If a **metadata query** fails, only that card shows a warning; other cards keep updating.
- If migration-verifier’s metadata version does not match what MI expects (currently version **7**), a **Warnings** card appears — upgrade MI or migration-verifier to compatible versions.

### Tuning (optional)

| Variable | What it changes |
|----------|-----------------|
| `MI_REFRESH_TIME` | Base refresh rate (sidebar Settings) |
| `MI_VERIFIER_SUMMARY_MIN_DURATION_SECS` | Ignore very short-lived mismatches in summary counts and document mismatch downloads (`0` = show all) |
| `MI_VERIFIER_GENERATION_LIMIT` | How many generations appear in the overview table (default 5) |

HTTP timeouts and other advanced settings: see **[CONFIGURATION.md](CONFIGURATION.md)**.

![Migration Verifier dashboard — endpoint and metadata combined](images/live_verifier_dashboard.png)

## Related documentation

- **[CONFIGURATION.md](CONFIGURATION.md)** — environment variables (`MI_REFRESH_TIME`, `MI_PROGRESS_ENDPOINT_URL`, `MI_INDEX_BUILD_REFRESH_TIME`, etc.)
- **[LOG_ANALYZER.md](LOG_ANALYZER.md)** — offline log analysis
- **[CONNECTION_STRING.md](CONNECTION_STRING.md)** — connection string formats and security
- **[PACKAGING.md](PACKAGING.md)** — pre-configuring env vars in packaged installs

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