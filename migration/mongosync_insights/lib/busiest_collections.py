"""Parse mongosync CEA CRUD statistics for busiest-collections analysis."""
import json
from collections import defaultdict
from datetime import datetime

import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder

from .plot_theme import apply_mi_theme

CRUD_STATS_MESSAGE = "Recent CRUD change event statistics."
LOW_CEA_PARALLELIZATION_PREFIX = (
    "The level of CEA parallelization for this collection is low"
)
BUSY_AGG_OUT_PREFIX = "Most source writes appear to be from $out aggregations"

INTERVAL_SECS = 10
TOP_N_PER_INTERVAL = 20
CHART_TOP_N = 10
DEFAULT_EVENT_TYPES = ("delete", "insert", "replace", "update")


def update_in_cea_phase(cea_phase, canonical_phase):
    """Return updated CEA phase flag from a canonical migration phase name."""
    if canonical_phase == "change event application":
        return True
    if canonical_phase == "commit handler called":
        return False
    return cea_phase


class _NamespaceStats:
    __slots__ = (
        "total_events",
        "total_events_per_type",
        "warning_count",
        "max_spread_disparity",
    )

    def __init__(self):
        self.total_events = 0
        self.total_events_per_type = defaultdict(int)
        self.warning_count = 0
        self.max_spread_disparity = None

    def add_collection(self, namespace, total_events, events_per_type):
        if not namespace:
            return
        self.total_events += int(total_events or 0)
        for op_type, count in (events_per_type or {}).items():
            self.total_events_per_type[op_type] += int(count or 0)

    def add_warning(self, spread_disparity):
        self.warning_count += 1
        if spread_disparity is not None:
            if (
                self.max_spread_disparity is None
                or spread_disparity > self.max_spread_disparity
            ):
                self.max_spread_disparity = spread_disparity


class BusiestCollectionsAccumulator:
    """Accumulate CEA busiest-collection statistics from mongosync log lines."""

    def __init__(self):
        self._namespaces = {}
        self._interval_snapshots = []
        self._warnings = []
        self._intervals_processed = 0
        self._skipped_outside_cea = 0
        self._event_types = set(DEFAULT_EVENT_TYPES)

    def _namespace_stats(self, namespace):
        if namespace not in self._namespaces:
            self._namespaces[namespace] = _NamespaceStats()
        return self._namespaces[namespace]

    def ingest_line(self, json_obj, *, in_cea_phase):
        message = json_obj.get("message") or ""
        stats = json_obj.get("recentCRUDStatistics")
        if not stats:
            return

        if message == CRUD_STATS_MESSAGE:
            if not in_cea_phase:
                self._skipped_outside_cea += 1
                return
            self._ingest_info_line(json_obj, stats)
            return

        if not in_cea_phase:
            self._skipped_outside_cea += 1
            return

        if self._is_warning_message(message) and stats.get("namespace"):
            self._ingest_warning_line(json_obj, stats, message)

    def _is_warning_message(self, message):
        return (
            message.startswith(LOW_CEA_PARALLELIZATION_PREFIX)
            or message.startswith(BUSY_AGG_OUT_PREFIX)
        )

    def _record_event_types(self, events_per_type):
        for op_type in (events_per_type or {}):
            self._event_types.add(op_type)

    def _ingest_info_line(self, json_obj, stats):
        timestamp = json_obj.get("time") or ""
        busiest = stats.get("busiestCollections") or []
        if not busiest:
            return

        interval_counts = {}
        for collection in busiest:
            namespace = collection.get("namespace")
            total_events = int(collection.get("totalEvents") or 0)
            events_per_type = collection.get("totalEventsPerType") or {}
            if not namespace:
                continue
            self._record_event_types(events_per_type)
            ns_stats = self._namespace_stats(namespace)
            ns_stats.add_collection(namespace, total_events, events_per_type)
            interval_counts[namespace] = (
                interval_counts.get(namespace, 0) + total_events
            )

        if interval_counts:
            self._interval_snapshots.append({
                "time": timestamp,
                "counts": interval_counts,
            })
            self._intervals_processed += 1

    def _ingest_warning_line(self, json_obj, stats, message):
        namespace = stats.get("namespace")
        if not namespace:
            return

        total_events = int(stats.get("totalEvents") or 0)
        events_per_type = stats.get("totalEventsPerType") or {}
        spread_disparity = stats.get("spreadDisparity")
        self._record_event_types(events_per_type)

        ns_stats = self._namespace_stats(namespace)
        ns_stats.add_collection(namespace, total_events, events_per_type)
        ns_stats.add_warning(spread_disparity)

        warning_type = "hot_doc"
        if message.startswith(BUSY_AGG_OUT_PREFIX):
            warning_type = "agg_out"

        self._warnings.append({
            "time": json_obj.get("time") or "",
            "level": json_obj.get("level") or "warn",
            "message": message,
            "namespace": namespace,
            "warningType": warning_type,
            "spreadDisparity": spread_disparity,
            "totalEvents": total_events,
            "totalEventsPerType": dict(events_per_type),
        })

        timestamp = json_obj.get("time") or ""
        self._interval_snapshots.append({
            "time": timestamp,
            "counts": {namespace: total_events},
        })

    def finalize(self):
        """Return summary, timeseries, warnings, and meta for template rendering."""
        summary = []
        for namespace, ns_stats in self._namespaces.items():
            summary.append({
                "namespace": namespace,
                "totalEvents": ns_stats.total_events,
                "totalEventsPerType": dict(ns_stats.total_events_per_type),
                "warningCount": ns_stats.warning_count,
                "maxSpreadDisparity": ns_stats.max_spread_disparity,
            })
        summary.sort(key=lambda row: row["totalEvents"], reverse=True)

        timeseries = self._build_timeseries(summary)
        event_types = sorted(self._event_types)

        return {
            "summary": summary,
            "timeseries": timeseries,
            "warnings": list(self._warnings),
            "eventTypes": event_types,
            "meta": {
                "intervalSecs": INTERVAL_SECS,
                "topNPerInterval": TOP_N_PER_INTERVAL,
                "intervalsProcessed": self._intervals_processed,
                "namespacesSeen": len(self._namespaces),
                "skippedOutsideCea": self._skipped_outside_cea,
            },
        }

    def _build_timeseries(self, summary):
        if not self._interval_snapshots:
            return {"times": [], "series": {}}

        top_namespaces = [
            row["namespace"] for row in summary[:CHART_TOP_N]
        ]
        if not top_namespaces:
            return {"times": [], "series": {}}

        times = [snapshot["time"] for snapshot in self._interval_snapshots]
        series = {namespace: [] for namespace in top_namespaces}
        for snapshot in self._interval_snapshots:
            counts = snapshot.get("counts") or {}
            for namespace in top_namespaces:
                series[namespace].append(int(counts.get(namespace, 0)))

        return {"times": times, "series": series}


def build_busiest_collections_plot(timeseries, meta=None):
    """Build Plotly JSON for top-namespace CEA write activity over time."""
    times = timeseries.get("times") or []
    series = timeseries.get("series") or {}
    if not times or not series:
        return ""

    parsed_times = []
    for ts in times:
        try:
            parsed_times.append(
                datetime.strptime(ts[:26], "%Y-%m-%dT%H:%M:%S.%f"),
            )
        except (ValueError, TypeError):
            parsed_times.append(ts)

    fig = go.Figure()
    for namespace, values in series.items():
        fig.add_trace(go.Scatter(
            x=parsed_times,
            y=values,
            mode="lines",
            name=namespace,
            stackgroup="cea_writes",
        ))

    interval_secs = (meta or {}).get("intervalSecs", INTERVAL_SECS)
    apply_mi_theme(
        fig,
        title=(
            f"CEA write activity by namespace "
            f"(top {CHART_TOP_N}, {interval_secs}s intervals)"
        ),
        height=420,
        width=1450,
        legend_tracegroupgap=120,
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Write operations per interval",
    )
    return json.dumps(fig, cls=PlotlyJSONEncoder)
