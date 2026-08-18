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
from .live_monitoring import _build_display

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


def _resolve_finish_time(phase_rows, state_transitions):
    committed = _committed_time_from_state_transitions(state_transitions)
    if committed:
        return committed
    _start, finish = _start_finish_from_phases(phase_rows)
    return finish


def merge_replication_into_progress(progress, replication_lines):
    """Fill lag / events / oplog window from the last Replication progress log line."""
    if not replication_lines:
        return dict(progress) if progress else None
    last = replication_lines[-1] if isinstance(replication_lines[-1], dict) else {}
    out = dict(progress) if progress else {}
    if out.get("lagTimeSeconds") is None and last.get("lagTimeSeconds") is not None:
        out["lagTimeSeconds"] = last["lagTimeSeconds"]
    if out.get("totalEventsApplied") is None and last.get("totalEventsApplied") is not None:
        out["totalEventsApplied"] = last["totalEventsApplied"]
    if not out.get("estimatedOplogTimeRemaining") and last.get("estimatedOplogTimeRemaining"):
        out["estimatedOplogTimeRemaining"] = last["estimatedOplogTimeRemaining"]
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
        "collectionsCopied": None,
        "collectionsTotal": collections_total if collections_total else None,
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
):
    """Build the live-monitor JSON payload for the Log Analyzer Summary tab."""
    from_progress_api = bool(progress)
    merged_progress = merge_replication_into_progress(progress, replication_lines)
    merged_progress = merge_state_transitions_into_progress(
        merged_progress, state_transitions, phase_events
    )
    if merged_progress and not merged_progress.get("info") and phase_events:
        merged_progress["info"] = phase_events[-1][1]

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
