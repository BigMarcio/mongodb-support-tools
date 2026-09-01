# Log Verbosity and Plot Coverage

Mongosync Insights reads the structured JSON log lines that mongosync writes to its log files. The amount of data available to plot depends directly on the **verbosity level** used when mongosync was running.

## Verbosity Hierarchy

Mongosync uses a cumulative verbosity system. Each level includes all levels below it:

| Level | Severity order | How to enable |
|-------|---------------|---------------|
| `error` | lowest verbosity | Always present |
| `warn`  | ↑ | Always present |
| `info`  | ↑ | Default — no flags needed |
| `debug` | ↑ | `--verbosity 1` |
| `trace` | highest verbosity | `--verbosity 2` |

To set verbosity when starting mongosync:

```bash
# default (info and above)
mongosync --cluster0 "..." --cluster1 "..."

# enable debug logs
mongosync --cluster0 "..." --cluster1 "..." --verbosity 1

# enable trace logs
mongosync --cluster0 "..." --cluster1 "..." --verbosity 2
```

## Effect on Plots and Features

The table below lists every chart and panel in Mongosync Insights, the minimum verbosity required, and the exact log pattern that feeds it.

### Global Migration Metrics

| Chart / Feature | Min. verbosity | Source log pattern |
|----------------|---------------|--------------------|
| Mongosync Phases (scatter) | `info` (default) | `"Starting ... phase"` \| `"Commit handler called"`; with `debug` (`--verbosity 1`): also `"Updating the in-memory phase from ... to ..."`; Live Migrate: `sent response` → `progress.atlasLiveMigrateMetrics.PhaseTransitions` — all sources merged, earliest timestamp per phase name wins |
| Mongosync Progress (table) | `info` (default) | Same merged phase sources as scatter (phase rows only), plus `"sent response"` → `body.progress.canCommit` / `canWrite` state transitions. In the Log Analyzer UI, use the chart **Copy as Markdown** badge to export the table. |
| Lag Time (seconds) | `info` (default) | `"Replication progress"` → `lag.overallLagSeconds` when present, else `lagTimeSeconds` (deprecated); optional `lag.crudLagSeconds` / `lag.ddlLagSeconds` for CRUD/DDL breakdown series |
| Est. Source Oplog Time Remaining | `info` (default) | `"Replication progress"` → `estimatedOplogTimeRemaining` |
| Ping Latency — src & dst | `info` (default) | `"Operation duration stats"` → `sourcePingLatencyMs` / `destinationPingLatencyMs` |
| Average Source CRUD Event Rate | `info` (default) | `"Average Source CRUD events rate"` (mongosync before REP-6864, Jun 2025) or `"Estimated average rate of CRUD events on the source."` (Dec 2025+) → `srcCRUDEventsPerSec`. Logged every 30s once CRUD change streams have event activity. |

> **Note:** This line may not appear in all orchestrator modes (e.g. some Live Import runs) or before CEA has processed CRUD events. Do not confuse with `eventApplicationRatePerSecond` on `"Replication progress."` — that is destination apply rate over the last 30s window, not average source CRUD rate since stream start.

### Collection Copy Metrics

| Chart / Feature | Min. verbosity | Source log pattern |
|----------------|---------------|--------------------|
| Partition Init Progress (time series) | `info` (default) | `"Creating a single/initial partition for..."` |
| Partition Init Summary — doc count & sampler | `info` (default) | `"Pre-sampling information"` |
| Partition Init Summary — **partition count & duration** | **`debug` (`--verbosity 1`)** | `"Persisted a new partition after sampling"` |
| Data Copied Over Time | `info` (default) | `"sent response"` → `body.progress.collectionCopy.estimatedCopiedBytes` |
| Estimated Total & Copied (bar) | `info` (default) | `"sent response"` → `body.progress.collectionCopy` |
| Partitions Copied (time series + bar) | `info` (default) | `"Completed writing X / Y partitions to destination cluster"` |

### CEA Metrics

| Chart / Feature | Min. verbosity | Source log pattern |
|----------------|---------------|--------------------|
| Change Events Applied | `info` (default) | `"Replication progress"` → `totalEventsApplied` |
| Events Rate per Second | `info` (default) | `"Replication progress"` → `eventApplicationRatePerSecond` |
| CEA Source Read — avg / max / ops | `info` (default) | `"Operation duration stats"` → `CEASourceRead` |
| CEA Destination Write — avg / max / ops | `info` (default) | `"Operation duration stats"` → `CEADestinationWrite` |
| CEA Busiest Collections (tab) | `info` (default) | `"Recent CRUD change event statistics."` → `recentCRUDStatistics.busiestCollections`; warn lines with `recentCRUDStatistics` for hot-document / `$out` workload |

### Indexes Metrics

| Chart / Feature | Min. verbosity | Source log pattern |
|----------------|---------------|--------------------|
| Index Built Over Time | `info` (default) | `"sent response"` → `body.progress.indexBuilding.indexesBuilt` |
| Total and Index Built (bar) | `info` (default) | `"sent response"` → `body.progress.indexBuilding.totalIndexesToBuild` |

### Verifier Metrics

| Chart / Feature | Min. verbosity | Source log pattern |
|----------------|---------------|--------------------|
| Source Verifier Lag Time | `warn` (automatic) | Field `verifierSrcLagTimeSeconds` present |
| Destination Verifier Lag Time | `warn` (automatic) | Field `verifierDstLagTimeSeconds` present |

> **Note:** Verifier lag lines are emitted by mongosync at `warn` level independently of the `--verbosity` flag. No extra configuration is needed — they appear automatically whenever the live verifier is active.

### Info Tabs (non-chart panels)

| Panel | Min. verbosity | Source log pattern |
|-------|---------------|--------------------|
| Version Info | `info` (default) | `"Version info"` |
| Start Options | `info` (default) | `"Received request"` filtered by `uri=/api/v1/start` |
| Mongosync Options | `info` (default) | `"Mongosync Options"` |
| Hidden Flags | `info` (default) | `"Mongosync HiddenFlags"` |
| Natural Order Collections | `info` (default) | `reason` field → `"Selected for natural order collection reads"` |

> **Note:** Version info is shown on the **Summary** tab (`mongosyncVersion` on the sync card) and in the **Mongosync Logs** tab subtitle — not as a separate Options table.

### Log Analyzer panels

| Panel | Min. verbosity | Source log pattern |
|-------|---------------|--------------------|
| **Summary tab** | `info` (default) | Latest `"sent response"` → `/api/v1/progress` body (state, copy, lag, index building, verifier, direction); supplemented by phase transitions, `/api/v1/start` options, natural-order selections, and partition copy counts from other log events |

> **Note:** The Summary tab is a **non-live snapshot** from the uploaded log — it reuses the Migration Monitoring card layout but does not read a separate log line type. The subtitle shows the timestamp of the latest `/progress` response in the file.

> **Note:** Version Info, Mongosync Options, and Hidden Flags are startup log lines. If you upload a **rotated or partial log file** that was captured mid-migration (i.e., after mongosync already rotated its initial log), these panels will be empty. The data is present in the earlier rotated log files, not the current one. Start Options require the log segment that contains the `/api/v1/start` request, which may be in a different file than the startup lines.

## Summary

| Verbosity | Charts and panels available |
|-----------|----------------------------|
| Default (`info`, no flags) | Everything **except** Partition Init partition count & duration columns; includes Log Analyzer **Summary** and **CEA Busiest Collections** tabs when matching log lines are present |
| `--verbosity 1` (`debug`) | Full coverage, including Partition Init partition count & duration |
| `--verbosity 2` (`trace`) | No additional plots currently — `trace`-level lines are not yet extracted by Mongosync Insights |

## Recommendation

For a complete analysis, capture mongosync logs with at least `--verbosity 1`. This adds one additional log line per partition created (`"Persisted a new partition after sampling"`), which is low volume and has negligible performance impact. All other charts work at the default verbosity level.

## Related documentation

- **[LOG_ANALYZER.md](LOG_ANALYZER.md)** — Log Analyzer workflow and analysis tabs
- **[CONFIGURATION.md](CONFIGURATION.md)** — environment variables

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