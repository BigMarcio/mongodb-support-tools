"""Build a Migration Monitoring-style Summary payload from parsed mongosync logs."""

import json
from collections import OrderedDict

from .data_sources import STATUS_ACTIVE, build_data_sources
from .live_metadata_status import (
    format_build_indexes,
    format_namespace_filter_rows,
    format_write_blocking_mode,
    namespace_filter_is_active,
    normalize_verification_mode,
)
from .live_monitoring import _build_display, atlas_live_migrate_collection_totals

_PAGE_TITLE = "Summary"
_EMPTY_ERROR = (
    "No /progress payload or other summary fields were found in this log. "
    "Include log lines that record mongosync HTTP responses to /api/v1/progress, "
    "or the startup /start request and phase-transition messages."
)

_FINISH_PHASE_NEEDLES = (
    "commit handler called",
    "commit completed",
    "committed",
)


def extract_latest_progress(responses):
    """Return (progress dict, log time) from the last sent-response body that contains progress."""
    latest = None
    latest_time = None
    for response in responses or []:
        if not isinstance(response, dict):
            continue
        try:
            parsed_body = json.loads(response.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(parsed_body, dict):
            continue
        progress = parsed_body.get("progress")
        if not isinstance(progress, dict) or not progress:
            continue
        latest = progress
        latest_time = response.get("time")
    return latest, latest_time


def extract_mongosync_version(version_info_lines):
    """Return mongosync version from the first log line with message 'Version info'."""
    for item in version_info_lines or []:
        if not isinstance(item, dict):
            continue
        version = item.get("version")
        if version not in (None, ""):
            return str(version)
    return None


def format_log_timestamp(raw):
    """Convert a mongosync log time string to 'YYYY-MM-DD HH:MM:SS'."""
    if not raw:
        return None
    text = str(raw).strip().replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1]
    if len(text) >= 19 and text[4] == "-" and text[10] == " ":
        return text[:19]
    return text or None


def phase_transition_rows_from_events(phase_events):
    """Convert merged (time, label) phase events to live-monitor phase start rows."""
    rows = []
    for item in phase_events or []:
        if not item or len(item) < 2:
            continue
        raw_time, label = item[0], item[1]
        phase = (label or "").strip()
        if not phase:
            continue
        rows.append(
            {
                "phase": phase.capitalize(),
                "startedAt": format_log_timestamp(raw_time) or "—",
            }
        )
    return rows


def natural_order_rows_from_log(items):
    """Group {database, collection} log rows into live-monitor natural-order rows."""
    if not items:
        return None
    by_db = OrderedDict()
    for item in items:
        if not isinstance(item, dict):
            continue
        db = item.get("database") or "—"
        coll = item.get("collection") or "—"
        by_db.setdefault(db, [])
        if coll not in by_db[db]:
            by_db[db].append(coll)
    if not by_db:
        return None
    return [
        {"database": db, "collections": ", ".join(colls)}
        for db, colls in by_db.items()
    ]


def _first_dict(items):
    if items and isinstance(items[0], dict):
        return items[0]
    if isinstance(items, dict):
        return items
    return {}


def _normalize_namespace_entries(entries):
    if not entries or not isinstance(entries, list):
        return None
    normalized = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        database = item.get("database")
        collections = item.get("collections")
        if isinstance(database, str):
            database = [database]
        normalized.append({"database": database, "collections": collections})
    return normalized or None


def _last_dict(items):
    if not items:
        return {}
    last = items[-1]
    return last if isinstance(last, dict) else {}


def _write_blocking_raw(start_options):
    if not start_options:
        return None
    value = start_options.get("enableUserWriteBlocking")
    if value is True:
        return "sourceAndDestination"
    if value is False:
        return "none"
    if isinstance(value, str) and value:
        return value
    return None


def _disable_write_blocking_from_hidden_flags(hidden_flags):
    """Read disableWriteBlocking from a Mongosync HiddenFlags log object."""
    flags = _first_dict(hidden_flags) if not isinstance(hidden_flags, dict) else hidden_flags
    if not flags:
        return None
    other = flags.get("other")
    if isinstance(other, dict) and "disableWriteBlocking" in other:
        return other.get("disableWriteBlocking")
    if "disableWriteBlocking" in flags:
        return flags.get("disableWriteBlocking")
    return None


def _write_blocking_mode_from_hidden_flags(hidden_flags):
    """Map HiddenFlags disableWriteBlocking to Summary display text."""
    value = _disable_write_blocking_from_hidden_flags(hidden_flags)
    if value is False:
        return "Default"
    if value is True:
        return "Disabled"
    return None


def _build_indexes_raw_from_start_handler(start_handler_parameters):
    """Resolve buildIndexes from Start handler called parameters."""
    params = _last_dict(start_handler_parameters)
    if not params:
        return None
    if "BuildIndexes" not in params:
        return None
    value = params.get("BuildIndexes")
    if value is None:
        return "afterDataCopy"
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _resolve_build_indexes_raw(start_options, hidden_flags, start_handler_parameters):
    from_handler = _build_indexes_raw_from_start_handler(start_handler_parameters)
    if from_handler is not None:
        return from_handler
    return _lookup_option(start_options, hidden_flags, "buildIndexes")


def _detect_random_id_from_start_handler(start_handler_parameters):
    """Resolve DetectRandomId from Start handler called parameters."""
    params = _last_dict(start_handler_parameters)
    if not params or "DetectRandomId" not in params:
        return None
    value = params.get("DetectRandomId")
    if value is False:
        return "False"
    if value is None or value is True:
        return "True"
    return str(value)


def _resolve_detect_random_id(start_options, hidden_flags, start_handler_parameters):
    from_handler = _detect_random_id_from_start_handler(start_handler_parameters)
    if from_handler is not None:
        return from_handler
    raw = _lookup_option(start_options, hidden_flags, "detectRandomId")
    return str(raw) if raw is not None else None


def _reversible_from_start_handler(start_handler_parameters):
    """Resolve Reversible from Start handler called parameters."""
    params = _last_dict(start_handler_parameters)
    if not params or "Reversible" not in params:
        return None
    value = params.get("Reversible")
    if value is True:
        return "True"
    if value is False:
        return "False"
    return str(value) if value is not None else None


def _resolve_reversible(start_options, hidden_flags, start_handler_parameters):
    from_handler = _reversible_from_start_handler(start_handler_parameters)
    if from_handler is not None:
        return from_handler
    raw = _lookup_option(start_options, hidden_flags, "reversible")
    return str(raw) if raw is not None else None


def _resolve_write_blocking_mode(start_options, hidden_flags):
    from_hidden = _write_blocking_mode_from_hidden_flags(hidden_flags)
    if from_hidden is not None:
        return from_hidden
    return format_write_blocking_mode(_write_blocking_raw(start_options))


def _verification_mode_from_start(start_options):
    if not start_options:
        return None
    if start_options.get("enableVerifier") is False:
        return "disabled"
    verification = start_options.get("verification")
    if verification is False:
        return "disabled"
    if isinstance(verification, dict):
        enabled = verification.get("enabled")
        if enabled is None:
            enabled = verification.get("enableVerifier")
        if enabled is False:
            return "disabled"
        raw_mode = verification.get("mode") or verification.get("verificationMode")
        if raw_mode:
            return normalize_verification_mode(raw_mode)
        if enabled is True:
            return "startAtCEA"
    return None


def _lookup_option(start_options, hidden_flags, key):
    if start_options and start_options.get(key) is not None:
        return start_options.get(key)
    if hidden_flags and hidden_flags.get(key) is not None:
        return hidden_flags.get(key)
    return None


def _start_finish_from_phases(rows):
    start = None
    finish = None
    for row in rows or []:
        phase_lower = (row.get("phase") or "").lower()
        if start is None and "initializing collections" in phase_lower:
            start = row.get("startedAt")
        if any(needle in phase_lower for needle in _FINISH_PHASE_NEEDLES):
            finish = row.get("startedAt")
    if start is None and rows:
        start = rows[0].get("startedAt")
    return start, finish


def _committed_time_from_state_transitions(state_transitions):
    """Return formatted commit time from the latest COMMITTED state transition log line."""
    transition = _latest_committed_state_transition(state_transitions)
    if not transition:
        return None
    return format_log_timestamp(transition.get("time"))


def _latest_committed_state_transition(state_transitions):
    """Return the latest log object where newState is COMMITTED."""
    latest = None
    latest_time = None
    for item in state_transitions or []:
        if not isinstance(item, dict):
            continue
        new_state = (item.get("newState") or "").strip().upper()
        if new_state != "COMMITTED":
            continue
        raw_time = (item.get("time") or "")[:26]
        if not raw_time:
            continue
        if latest_time is None or raw_time > latest_time:
            latest_time = raw_time
            latest = item
    return latest


def _phase_after_committed(phase_events, committed_time):
    """Return the latest phase label at or after the COMMITTED state transition."""
    if not phase_events or not committed_time:
        return None
    for raw_time, label in reversed(phase_events):
        if raw_time >= committed_time:
            phase = (label or "").strip()
            if phase:
                return phase
    return None


def merge_state_transitions_into_progress(progress, state_transitions, phase_events=None):
    """Overlay COMMITTED state and final phase when logs continue after the last /progress."""
    if not progress:
        return progress
    committed = _latest_committed_state_transition(state_transitions)
    if not committed:
        return progress
    committed_time = (committed.get("time") or "")[:26]
    out = dict(progress)
    out["state"] = "COMMITTED"
    out["info"] = (
        _phase_after_committed(phase_events, committed_time) or "commit completed"
    )
    return out


def _log_time_sort_key(raw):
    if not raw:
        return ""
    return str(raw).strip().replace("Z", "")[:26]


class SummaryMigrationState:
    """Track latest Summary migration phase/state/index progress from log events."""

    def __init__(self):
        self.progress_snapshot = None
        self.progress_time = None
        self.phase = None
        self.phase_time = None
        self.state = None
        self.state_time = None
        self.index_building = None
        self.index_building_time = None
        self.verification = None
        self.verification_time = None
        self.total_events_applied_peak = None
        self.total_events_applied_last_positive = None

    def note_progress(self, progress, time):
        if not isinstance(progress, dict):
            return
        stamp = _log_time_sort_key(time)
        if not stamp:
            return
        self.progress_snapshot = dict(progress)
        self.progress_time = stamp
        self.note_phase(progress.get("info"), time)
        self.note_state(progress.get("state"), time)
        index_building = _normalize_index_building(progress.get("indexBuilding"))
        if index_building:
            self.note_index_building(index_building, time)
        verification = _normalize_verification(progress.get("verification"))
        if verification:
            self.note_verification(verification, time)
        self.note_events_applied(progress.get("totalEventsApplied"), time)
        atlas_events = _events_applied_from_atlas_progress(progress)
        if atlas_events is not None:
            self.note_events_applied(atlas_events, time)

    def note_phase(self, phase, time):
        if not phase:
            return
        stamp = _log_time_sort_key(time)
        if not stamp:
            return
        if self.phase_time is None or stamp >= self.phase_time:
            self.phase = str(phase).strip()
            self.phase_time = stamp

    def note_state(self, state, time):
        if not state:
            return
        stamp = _log_time_sort_key(time)
        if not stamp:
            return
        if self.state_time is None or stamp >= self.state_time:
            self.state = str(state).strip().upper()
            self.state_time = stamp

    def note_index_building(self, index_building, time):
        normalized = _normalize_index_building(index_building)
        if not normalized:
            return
        stamp = _log_time_sort_key(time)
        if not stamp:
            if self.index_building is None:
                self.index_building = normalized
            return
        if self.index_building_time is None or stamp >= self.index_building_time:
            self.index_building = normalized
            self.index_building_time = stamp

    def note_verification(self, verification, time):
        if not verification:
            return
        stamp = _log_time_sort_key(time)
        if not stamp:
            if self.verification is None:
                self.verification = verification
            return
        if self.verification_time is None or stamp >= self.verification_time:
            self.verification = verification
            self.verification_time = stamp

    def note_events_applied(self, value, time):
        coerced = _coerce_event_count(value)
        if coerced is None:
            return
        if self.total_events_applied_peak is None or coerced > self.total_events_applied_peak:
            self.total_events_applied_peak = coerced
        if coerced > 0:
            self.total_events_applied_last_positive = coerced

    def to_progress(
        self,
        *,
        replication_lines=None,
        state_transitions=None,
        phase_events=None,
        sent_responses=None,
    ):
        out = dict(self.progress_snapshot) if self.progress_snapshot else {}
        if self.phase:
            out["info"] = self.phase
        if self.state:
            out["state"] = self.state
        if self.index_building:
            out["indexBuilding"] = self.index_building
        if self.verification:
            out["verification"] = self.verification

        out = merge_state_transitions_into_progress(
            out or None, state_transitions, phase_events
        )
        out = merge_replication_into_progress(out, replication_lines)
        out = merge_events_applied_into_progress(
            out,
            replication_lines=replication_lines,
            sent_responses=sent_responses,
            tracked_peak=self.total_events_applied_last_positive
            or self.total_events_applied_peak,
        )
        out = backfill_commit_events_applied(
            out,
            replication_lines,
            sent_responses=sent_responses,
            tracked_peak=self.total_events_applied_last_positive
            or self.total_events_applied_peak,
        )
        if out and not out.get("info") and phase_events:
            out["info"] = phase_events[-1][1]
        return out or None


def build_summary_migration_state(
    *,
    progress=None,
    progress_time=None,
    sent_responses=None,
    phase_events=None,
    state_transitions=None,
    index_creation_lines=None,
):
    """Replay log-derived events into a single Summary migration state tracker."""
    state = SummaryMigrationState()

    for response in sent_responses or []:
        if not isinstance(response, dict):
            continue
        try:
            parsed_body = json.loads(response.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        response_progress = (parsed_body or {}).get("progress")
        if isinstance(response_progress, dict):
            state.note_progress(response_progress, response.get("time"))

    if progress and not sent_responses:
        if progress_time:
            state.note_progress(progress, progress_time)
        else:
            state.progress_snapshot = dict(progress)
            if progress.get("info"):
                state.phase = str(progress["info"]).strip()
            if progress.get("state"):
                state.state = str(progress["state"]).strip().upper()
            index_building = _normalize_index_building(progress.get("indexBuilding"))
            if index_building:
                state.index_building = index_building

    for raw_time, label in phase_events or []:
        state.note_phase(label, raw_time)

    for item in state_transitions or []:
        if not isinstance(item, dict):
            continue
        new_state = (item.get("newState") or "").strip().upper()
        if new_state:
            state.note_state(new_state, item.get("time"))

    for item in index_creation_lines or []:
        if not isinstance(item, dict):
            continue
        state.note_index_building(item.get("indexCreationProgress"), item.get("time"))

    return state


def merge_state_transitions_into_progress(progress, state_transitions, phase_events=None):
    """Overlay COMMITTED state and final phase when logs continue after the last /progress."""
    if not progress:
        return progress
    committed = _latest_committed_state_transition(state_transitions)
    if not committed:
        return progress
    committed_time = (committed.get("time") or "")[:26]
    out = dict(progress)
    out["state"] = "COMMITTED"
    out["info"] = (
        _phase_after_committed(phase_events, committed_time) or "commit completed"
    )
    return out


def merge_phase_events_into_progress(progress, phase_events, *, progress_time=None):
    """Overlay newer phase labels from log events when /progress is stale (Summary)."""
    if not progress or not phase_events:
        return progress
    if (progress.get("state") or "").upper() == "COMMITTED":
        return progress
    last_time, last_label = phase_events[-1]
    if not last_label or not last_time:
        return progress
    if progress_time and _log_time_sort_key(last_time) <= _log_time_sort_key(progress_time):
        return progress
    current = (progress.get("info") or "").strip().lower()
    if current == (last_label or "").strip().lower():
        return progress
    out = dict(progress)
    out["info"] = last_label
    return out


def _resolve_finish_time(phase_rows, state_transitions):
    committed = _committed_time_from_state_transitions(state_transitions)
    if committed:
        return committed
    _start, finish = _start_finish_from_phases(phase_rows)
    return finish


def merge_replication_into_progress(progress, replication_lines):
    """Fill lag / oplog window from Replication progress log lines."""
    if not replication_lines:
        return dict(progress) if progress else None
    last = replication_lines[-1] if isinstance(replication_lines[-1], dict) else {}
    out = dict(progress) if progress else {}
    if out.get("lagTimeSeconds") is None and last.get("lagTimeSeconds") is not None:
        out["lagTimeSeconds"] = last["lagTimeSeconds"]
    if not out.get("estimatedOplogTimeRemaining") and last.get("estimatedOplogTimeRemaining"):
        out["estimatedOplogTimeRemaining"] = last["estimatedOplogTimeRemaining"]
    return out or None


def _coerce_event_count(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except (TypeError, ValueError):
            return None
    return None


def _events_applied_from_atlas_progress(progress):
    if not isinstance(progress, dict):
        return None
    alm = progress.get("atlasLiveMigrateMetrics")
    if not isinstance(alm, dict):
        return None
    crud = _coerce_event_count(alm.get("totalCRUDEventsApplied"))
    ddl = _coerce_event_count(alm.get("totalDDLEventsApplied"))
    if crud is None and ddl is None:
        return None
    return (crud or 0) + (ddl or 0)


def _scan_events_applied_from_items(items, *, key="totalEventsApplied"):
    latest_numeric = None
    latest_positive = None
    for item in items or []:
        if not isinstance(item, dict):
            continue
        value = _coerce_event_count(item.get(key))
        if value is None:
            continue
        latest_numeric = value
        if value > 0:
            latest_positive = value
    if latest_positive is not None:
        return latest_positive
    return latest_numeric


def _scan_events_applied_from_sent_responses(sent_responses):
    latest_numeric = None
    latest_positive = None
    for response in sent_responses or []:
        if not isinstance(response, dict):
            continue
        try:
            parsed_body = json.loads(response.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(parsed_body, dict):
            continue
        progress = parsed_body.get("progress")
        if not isinstance(progress, dict):
            continue
        for value in (
            _coerce_event_count(progress.get("totalEventsApplied")),
            _events_applied_from_atlas_progress(progress),
        ):
            if value is None:
                continue
            latest_numeric = value
            if value > 0:
                latest_positive = value
    if latest_positive is not None:
        return latest_positive
    return latest_numeric


def _best_known_events_applied(
    replication_lines=None,
    sent_responses=None,
    *,
    tracked_peak=None,
):
    candidates = [
        _scan_events_applied_from_items(replication_lines),
        _scan_events_applied_from_sent_responses(sent_responses),
        _coerce_event_count(tracked_peak),
    ]
    latest_positive = None
    latest_numeric = None
    for value in candidates:
        coerced = _coerce_event_count(value)
        if coerced is None:
            continue
        latest_numeric = coerced
        if coerced > 0:
            latest_positive = coerced
    if latest_positive is not None:
        return latest_positive
    return latest_numeric


def merge_events_applied_into_progress(
    progress,
    *,
    replication_lines=None,
    sent_responses=None,
    tracked_peak=None,
):
    """Fill totalEventsApplied from replication, /progress history, and tracked peaks."""
    out = dict(progress) if progress else {}
    current = _coerce_event_count(out.get("totalEventsApplied"))
    if current not in (None, 0):
        return out or None
    resolved = _best_known_events_applied(
        replication_lines,
        sent_responses,
        tracked_peak=tracked_peak,
    )
    if resolved is None:
        atlas_events = _events_applied_from_atlas_progress(out)
        if atlas_events is not None:
            resolved = atlas_events
    if resolved is not None:
        out["totalEventsApplied"] = resolved
    return out or None


def _last_known_events_applied(replication_lines):
    """Return best-known totalEventsApplied from replication logs."""
    return _scan_events_applied_from_items(replication_lines)


_LATE_COMMIT_PHASES = frozenset(
    {
        "commit completed",
        "waiting for index constraint enablement at the end of commit",
        "waiting for commit to complete",
    }
)


def _is_late_commit_phase(phase):
    normalized = (phase or "").strip().lower().rstrip(".")
    if normalized in _LATE_COMMIT_PHASES:
        return True
    return "commit" in normalized and "collection copy" not in normalized


def backfill_commit_events_applied(
    progress,
    replication_lines,
    *,
    sent_responses=None,
    tracked_peak=None,
):
    """Keep Events applied from dropping to 0 on late commit phases in Summary."""
    if not progress:
        return progress
    phase = (progress.get("info") or "").strip().lower()
    if not _is_late_commit_phase(phase):
        return progress
    current = _coerce_event_count(progress.get("totalEventsApplied"))
    if current not in (None, 0):
        return progress
    last_known = _best_known_events_applied(
        replication_lines,
        sent_responses,
        tracked_peak=tracked_peak,
    )
    if last_known is None or last_known == current:
        return progress
    out = dict(progress)
    out["totalEventsApplied"] = last_known
    return out


def _normalize_verification(verification):
    """Normalize progress.verification from /progress sent-response payloads."""
    if not verification or not isinstance(verification, dict):
        return None
    source = verification.get("source")
    destination = verification.get("destination")
    if not source and not destination:
        return None
    out = {}
    if isinstance(source, dict):
        out["source"] = source
    if isinstance(destination, dict):
        out["destination"] = destination
    return out or None


def extract_latest_verification_from_sent_responses(responses):
    """Return the last progress.verification block from sent-response bodies."""
    latest = None
    for response in responses or []:
        if not isinstance(response, dict):
            continue
        try:
            parsed_body = json.loads(response.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(parsed_body, dict):
            continue
        progress = parsed_body.get("progress")
        if not isinstance(progress, dict):
            continue
        verification = _normalize_verification(progress.get("verification"))
        if verification:
            latest = verification
    return latest


def merge_verification_into_progress(progress, *, sent_responses=None):
    """Backfill verification when the latest /progress omits it (Summary tab)."""
    out = dict(progress) if progress else {}
    current = _normalize_verification(out.get("verification"))
    if current:
        out["verification"] = current
        return out or None
    backfill = extract_latest_verification_from_sent_responses(sent_responses)
    if backfill:
        out["verification"] = backfill
    return out or None


def _normalize_index_building(index_building):
    """Normalize indexBuilding from /progress or Index creation progress logs."""
    if not index_building or not isinstance(index_building, dict):
        return None
    total = index_building.get("totalIndexesToBuild")
    coll_total = index_building.get("collectionsTotal")
    if coll_total is None:
        coll_total = index_building.get("totalCollections")
    built = index_building.get("indexesBuilt")
    if built is None:
        built = index_building.get("totalIndexesBuilt")
    coll_finished = index_building.get("collectionsFinished")
    if coll_finished is None:
        coll_finished = index_building.get("finishedCollections")
    if not total and not coll_total:
        return None
    return {
        "indexesBuilt": built or 0,
        "totalIndexesToBuild": total or 0,
        "collectionsFinished": coll_finished or 0,
        "collectionsTotal": coll_total or 0,
    }


def extract_latest_index_building_from_sent_responses(responses):
    """Return the last indexBuilding block from sent-response /progress payloads."""
    latest = None
    for response in responses or []:
        if not isinstance(response, dict):
            continue
        try:
            parsed_body = json.loads(response.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(parsed_body, dict):
            continue
        progress = parsed_body.get("progress")
        if not isinstance(progress, dict):
            continue
        normalized = _normalize_index_building(progress.get("indexBuilding"))
        if normalized:
            latest = normalized
    return latest


def extract_latest_index_creation_progress(index_creation_lines):
    """Return index building totals from the last Index creation progress log line."""
    latest = None
    for item in index_creation_lines or []:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_index_building(item.get("indexCreationProgress"))
        if normalized:
            latest = normalized
    return latest


def merge_index_building_into_progress(
    progress,
    *,
    sent_responses=None,
    index_creation_lines=None,
):
    """Backfill indexBuilding when the latest /progress omits it (Summary tab)."""
    out = dict(progress) if progress else {}
    current = _normalize_index_building(out.get("indexBuilding"))
    if current:
        out["indexBuilding"] = current
        return out or None

    backfill = extract_latest_index_building_from_sent_responses(
        sent_responses
    ) or extract_latest_index_creation_progress(index_creation_lines)
    if backfill:
        out["indexBuilding"] = backfill
    return out or None


def build_log_metadata(
    *,
    progress=None,
    phase_events=None,
    natural_order_items=None,
    start_options=None,
    hidden_flags=None,
    start_handler_parameters=None,
    state_transitions=None,
    partitions_copied=None,
    partitions_total=None,
    collections_total=None,
):
    """Build a live-monitor metadata dict from log-derived fields."""
    start_options = _first_dict(start_options)
    hidden_flags = _first_dict(hidden_flags)
    phase_rows = phase_transition_rows_from_events(phase_events)
    start, _phase_finish = _start_finish_from_phases(phase_rows)
    finish = _resolve_finish_time(phase_rows, state_transitions)

    sync_phase = None
    if progress:
        sync_phase = (progress.get("info") or "").strip().lower() or None
    if not sync_phase and phase_events:
        sync_phase = (phase_events[-1][1] or "").strip().lower() or None

    build_indexes_raw = _resolve_build_indexes_raw(
        start_options, hidden_flags, start_handler_parameters
    )
    reversible = _resolve_reversible(start_options, hidden_flags, start_handler_parameters)
    detect_random_id = _resolve_detect_random_id(
        start_options, hidden_flags, start_handler_parameters
    )
    write_blocking_mode = _resolve_write_blocking_mode(start_options, hidden_flags)

    inclusion = _normalize_namespace_entries(start_options.get("includeNamespaces"))
    exclusion = _normalize_namespace_entries(start_options.get("excludeNamespaces"))
    namespace_filter = {
        "inclusionFilter": inclusion,
        "exclusionFilter": exclusion,
    }

    collections_copied = None
    collections_total_out = collections_total if collections_total else None
    atlas_totals = atlas_live_migrate_collection_totals(progress)
    if atlas_totals and atlas_totals.get("collectionsTotal"):
        collections_copied = atlas_totals.get("collectionsCopied")
        collections_total_out = atlas_totals.get("collectionsTotal")

    metadata = {
        "state": (progress.get("state") if progress else None),
        "phase": (sync_phase.capitalize() if sync_phase else None),
        "syncPhase": sync_phase,
        "start": start,
        "finish": finish,
        "reversible": reversible,
        "writeBlockingMode": write_blocking_mode,
        "buildIndexesRaw": build_indexes_raw,
        "buildIndexes": format_build_indexes(build_indexes_raw),
        "verificationModeRaw": _verification_mode_from_start(start_options),
        "detectRandomId": detect_random_id,
        "collectionsCopied": collections_copied,
        "collectionsTotal": collections_total_out,
        "partitionsCopied": partitions_copied,
        "partitionsTotal": partitions_total,
        "phaseTransitions": phase_rows,
        "naturalOrderRows": natural_order_rows_from_log(natural_order_items),
        "namespaceFilterActive": namespace_filter_is_active(namespace_filter),
        "inclusionFilterRows": format_namespace_filter_rows(inclusion, "inclusion"),
        "exclusionFilterRows": format_namespace_filter_rows(exclusion, "exclusion"),
    }
    return metadata


def _metadata_is_useful(metadata):
    if not metadata:
        return False
    return any(
        (
            metadata.get("phaseTransitions"),
            metadata.get("naturalOrderRows"),
            metadata.get("namespaceFilterActive"),
            metadata.get("reversible") is not None,
            metadata.get("buildIndexesRaw"),
            metadata.get("writeBlockingMode"),
            metadata.get("verificationModeRaw"),
            metadata.get("partitionsTotal"),
            metadata.get("collectionsTotal"),
            metadata.get("start"),
        )
    )


def _page_subtitle(progress_time, *, from_progress_api):
    as_of = format_log_timestamp(progress_time)
    if as_of and from_progress_api:
        return f"As of {as_of} UTC · latest /progress in this log (not live)"
    if as_of:
        return f"As of {as_of} UTC · snapshot from this log (not live)"
    return "Snapshot from this log (not live)"


def empty_log_summary_payload(*, error=None):
    return {
        "warnings": [],
        "dataSources": None,
        "progressWarning": None,
        "metadataWarning": None,
        "display": None,
        "error": error or _EMPTY_ERROR,
        "pageTitle": _PAGE_TITLE,
        "pageSubtitle": "Snapshot from this log (not live)",
    }


def build_log_summary_payload(
    *,
    progress=None,
    progress_time=None,
    replication_lines=None,
    phase_events=None,
    natural_order_items=None,
    start_options=None,
    hidden_flags=None,
    start_handler_parameters=None,
    state_transitions=None,
    partitions_copied=None,
    partitions_total=None,
    collections_total=None,
    version_info_lines=None,
    sent_responses=None,
    index_creation_lines=None,
    summary_state=None,
):
    """Build the live-monitor JSON payload for the Log Analyzer Summary tab."""
    from_progress_api = bool(progress or (summary_state and summary_state.progress_snapshot))
    if summary_state is None:
        summary_state = build_summary_migration_state(
            progress=progress,
            progress_time=progress_time,
            sent_responses=sent_responses,
            phase_events=phase_events,
            state_transitions=state_transitions,
            index_creation_lines=index_creation_lines,
        )
    merged_progress = summary_state.to_progress(
        replication_lines=replication_lines,
        state_transitions=state_transitions,
        phase_events=phase_events,
        sent_responses=sent_responses,
    )
    merged_progress = merge_verification_into_progress(
        merged_progress,
        sent_responses=sent_responses,
    )

    metadata = build_log_metadata(
        progress=merged_progress,
        phase_events=phase_events,
        natural_order_items=natural_order_items,
        start_options=start_options,
        hidden_flags=hidden_flags,
        start_handler_parameters=start_handler_parameters,
        state_transitions=state_transitions,
        partitions_copied=partitions_copied,
        partitions_total=partitions_total,
        collections_total=collections_total,
    )
    metadata_useful = _metadata_is_useful(metadata)
    progress_available = bool(merged_progress)

    if not progress_available and not metadata_useful:
        return empty_log_summary_payload()

    as_of = progress_time
    if not as_of and replication_lines:
        last = replication_lines[-1]
        if isinstance(last, dict):
            as_of = last.get("time")

    warnings = []
    if progress_available and isinstance(merged_progress.get("warnings"), list):
        warnings = merged_progress.get("warnings") or []

    display = _build_display(
        merged_progress if progress_available else None,
        metadata if metadata_useful else None,
        progress_available=progress_available,
        summary=True,
        mongosync_version=extract_mongosync_version(version_info_lines),
    )

    return {
        "warnings": warnings,
        "dataSources": build_data_sources(
            progress_configured=from_progress_api,
            metadata_configured=metadata_useful,
            progress_status=STATUS_ACTIVE if from_progress_api else None,
            metadata_status=STATUS_ACTIVE if metadata_useful else None,
            progress_label="Latest /progress",
            metadata_label="Log events",
        ),
        "progressWarning": None,
        "metadataWarning": None,
        "display": display,
        "error": None,
        "pageTitle": _PAGE_TITLE,
        "pageSubtitle": _page_subtitle(as_of, from_progress_api=from_progress_api),
    }
