"""Live progress monitor: fetch /api/v1/progress and build a JSON view for the HTML dashboard."""

import json
import logging

import requests

from .app_config import (
    MONGOSYNC_PROGRESS_TIMEOUT_SECS,
    VERIFIER_PROGRESS_TIMEOUT_SECS,
    VERIFIER_SUMMARY_TIMEOUT_SECS,
)
from .data_sources import STATUS_ACTIVE, STATUS_UNAVAILABLE, build_data_sources
from .live_metadata_status import (
    MetadataFetchError,
    describe_build_indexes_policy,
    describe_verification_mode,
    fetch_metadata_status,
    index_build_progress_allowed,
    normalize_verification_mode,
    should_suppress_index_build_progress,
    verification_progress_allowed,
    is_during_or_after_cea,
)
from .utils import (
    format_bytes_compact,
    format_count,
    format_lag_time_seconds,
    format_ratio,
    format_seconds_title,
)

_MIGRATION_START_TIME_TITLE = (
    "This is the time when the phase changes from uninitialized to "
    "Starting initializing collections and indexes after /start is issued"
)

logger = logging.getLogger(__name__)


class ProgressFetchError(Exception):
    """Raised when the progress endpoint cannot be reached or returns invalid data."""

    def __init__(self, message, *, kind="request"):
        super().__init__(message)
        self.kind = kind


def fetch_progress(endpoint_url, *, timeout_secs=None):
    """
    GET mongosync progress JSON from host:port/api/v1/progress.

    Returns:
        tuple[dict, list]: (progress dict, warnings list)

    Raises:
        ProgressFetchError: on timeout, connection, HTTP, or JSON errors.
    """
    if timeout_secs is None:
        timeout_secs = MONGOSYNC_PROGRESS_TIMEOUT_SECS
    url = f"http://{endpoint_url}"
    logger.info("Fetching progress from endpoint: %s", url)
    try:
        response = requests.get(url, timeout=timeout_secs)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.Timeout as e:
        raise ProgressFetchError(
            "Timeout — could not reach the progress endpoint.", kind="timeout"
        ) from e
    except requests.exceptions.ConnectionError as e:
        raise ProgressFetchError(
            "Connection error — could not reach the progress endpoint.", kind="connection"
        ) from e
    except requests.exceptions.HTTPError as e:
        raise ProgressFetchError(
            f"HTTP error from progress endpoint ({e.response.status_code}).", kind="http"
        ) from e
    except requests.exceptions.RequestException as e:
        raise ProgressFetchError(
            f"Request failed: {e}", kind="request"
        ) from e
    except json.JSONDecodeError as e:
        raise ProgressFetchError(
            "Invalid JSON response from progress endpoint.", kind="json"
        ) from e

    progress = data.get("progress") or {}
    warnings = progress.get("warnings") or []
    if not isinstance(warnings, list):
        warnings = []
    return progress, warnings


def fetch_verifier_progress(endpoint_url):
    """GET migration-verifier /api/v1/progress."""
    return fetch_progress(endpoint_url, timeout_secs=VERIFIER_PROGRESS_TIMEOUT_SECS)


def fetch_summary(endpoint_url, *, min_duration_secs=0):
    """
    GET migration-verifier summary JSON from host:port/api/v1/summary.

    Returns:
        dict: summary response (top-level BSON ext JSON)

    Raises:
        ProgressFetchError: on timeout, connection, HTTP, JSON, or API errors.
    """
    url = f"http://{endpoint_url}"
    params = {}
    if min_duration_secs and min_duration_secs > 0:
        params["minDurationSecs"] = min_duration_secs
    logger.info("Fetching summary from endpoint: %s", url)
    try:
        response = requests.get(
            url, params=params or None, timeout=VERIFIER_SUMMARY_TIMEOUT_SECS,
        )
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.Timeout as e:
        raise ProgressFetchError(
            "Timeout — could not reach the summary endpoint.", kind="timeout"
        ) from e
    except requests.exceptions.ConnectionError as e:
        raise ProgressFetchError(
            "Connection error — could not reach the summary endpoint.", kind="connection"
        ) from e
    except requests.exceptions.HTTPError as e:
        raise ProgressFetchError(
            f"HTTP error from summary endpoint ({e.response.status_code}).", kind="http"
        ) from e
    except requests.exceptions.RequestException as e:
        raise ProgressFetchError(
            f"Request failed: {e}", kind="request"
        ) from e
    except json.JSONDecodeError as e:
        raise ProgressFetchError(
            "Invalid JSON response from summary endpoint.", kind="json"
        ) from e

    if not isinstance(data, dict):
        raise ProgressFetchError(
            "Invalid JSON response from summary endpoint.", kind="json"
        )
    if data.get("error"):
        raise ProgressFetchError(str(data["error"]), kind="api")
    return data


def _state_badge_color(state_upper):
    mapping = {
        "RUNNING": "green",
        "IDLE": "gray",
        "INITIALIZING": "blue",
        "PAUSED": "yellow",
        "COMMITTING": "blue",
        "COMMITTED": "blue",
        "ERROR": "red",
        "FAILED": "red",
    }
    return mapping.get(state_upper, "gray")


def _derive_state_badge(progress):
    state = (progress.get("state") or "").upper()
    label = state or "—"
    return {"label": label, "color": _state_badge_color(state)}


def _derive_state_badge_from_state(state):
    state_upper = (state or "").upper()
    label = state_upper or "—"
    return {"label": label, "color": _state_badge_color(state_upper)}


def _display_or_dash(value):
    if value is None or value == "":
        return "—"
    return str(value)


def _build_metadata_metrics(metadata, *, summary=False):
    """Second metrics row from internal database (connection string required)."""
    if not metadata:
        return []
    finish_label = "Migration Committed" if summary else "Finish"
    return [
        {
            "label": "Migration Start time",
            "value": _display_or_dash(metadata.get("start")),
            "title": _MIGRATION_START_TIME_TITLE,
            "small": True,
        },
        {"label": finish_label, "value": _display_or_dash(metadata.get("finish")), "small": True},
        {
            "label": "Reversible",
            "value": _display_or_dash(metadata.get("reversible")),
            "small": True,
        },
        {
            "label": "Write Blocking Mode",
            "value": _display_or_dash(metadata.get("writeBlockingMode")),
            "small": True,
        },
        {
            "label": "Build Indexes",
            "value": _display_or_dash(metadata.get("buildIndexes")),
            "small": True,
        },
        {
            "label": "Detect Random _id",
            "value": _display_or_dash(metadata.get("detectRandomId")),
            "small": True,
        },
    ]


def _duration_metric(label, seconds, *, small=False):
    item = {
        "label": label,
        "value": format_lag_time_seconds(seconds),
        "small": small,
    }
    title = format_seconds_title(seconds)
    if title:
        item["title"] = title
    if label == "Lag time" and seconds is not None and seconds > 60:
        item["highLag"] = True
    return item


def _copied_bytes_title(copied, total):
    if copied is None and total is None:
        return None
    return f"{format_count(copied)} of {format_count(total)} bytes"


def _build_progress_metrics(progress, lag_seconds):
    return [
        _duration_metric("Lag time", lag_seconds, small=False),
        {"label": "Events applied", "value": format_count(progress.get("totalEventsApplied")), "small": False},
        {
            "label": "Oplog window left",
            "value": progress.get("estimatedOplogTimeRemaining") or "—",
            "small": True,
        },
        _duration_metric(
            "Catch-up estimate",
            progress.get("estimatedSecondsToCEACatchup"),
            small=True,
        ),
        {
            "label": "Can commit",
            "value": "TRUE" if progress.get("canCommit") else "FALSE",
            "badge": "green" if progress.get("canCommit") else "gray",
            "small": True,
        },
        {
            "label": "Can write",
            "value": "TRUE" if progress.get("canWrite") else "FALSE",
            "badge": "green" if progress.get("canWrite") else "gray",
            "small": True,
        },
    ]


def _build_db_only_metrics(lag_seconds):
    return [
        _duration_metric("Lag time", lag_seconds, small=False),
        {"label": "Events applied", "value": "—", "small": False},
        {"label": "Oplog window left", "value": "—", "small": True},
        {"label": "Catch-up estimate", "value": "—", "small": True},
        {"label": "Can commit", "value": "—", "small": True},
        {"label": "Can write", "value": "—", "small": True},
    ]


def _byte_copy_prefix(phase_lower):
    if "collection copy" in phase_lower:
        return "Data copied: "
    return "Copied: "


def atlas_live_migrate_collection_totals(progress):
    """Return collectionsCopied/collectionsTotal from progress.atlasLiveMigrateMetrics."""
    if not isinstance(progress, dict):
        return None
    alm = progress.get("atlasLiveMigrateMetrics")
    if not isinstance(alm, dict):
        return None
    total = alm.get("initialNumCollectionsTotal")
    copied = alm.get("initialNumCollectionsCopied")
    if total is None and copied is None:
        return None
    return {
        "collectionsCopied": copied if copied is not None else 0,
        "collectionsTotal": total,
    }


def _build_collections_copied_label(metadata, progress=None):
    copied = None
    total = None
    atlas_totals = atlas_live_migrate_collection_totals(progress)
    if atlas_totals and atlas_totals.get("collectionsTotal"):
        copied = atlas_totals.get("collectionsCopied")
        total = atlas_totals.get("collectionsTotal")
    elif metadata:
        copied = metadata.get("collectionsCopied")
        total = metadata.get("collectionsTotal")
    if not metadata and not atlas_totals:
        return None
    if not total or total <= 0:
        return None
    if copied is None:
        copied = 0
    return f"Collections copied: {format_count(copied)} of {format_count(total)}"


def _build_partitions_copied_label(metadata):
    if not metadata:
        return None
    copied = metadata.get("partitionsCopied")
    total = metadata.get("partitionsTotal")
    if not total or total <= 0:
        return None
    if copied is None:
        copied = 0
    return f"Partitions copied: {format_count(copied)} of {format_count(total)}"


def _bytes_copy_percent(copied, total):
    """Progress bar percent from estimated copied bytes vs total bytes."""
    if not total or total <= 0:
        return None
    if copied is None:
        copied = 0
    return min(100.0, (copied / total) * 100)


def _build_phase_start_times(metadata, *, summary=False):
    if not metadata:
        return None
    rows = metadata.get("phaseTransitions") or []
    if not rows:
        return None
    return {
        "label": "Phase start times",
        "rows": rows,
        "timezoneNote": "UTC",
        "timezoneNoteBelowTitle": summary,
    }


def _apply_summary_mongosync_version(sync_card, *, summary=False, mongosync_version=None):
    if not summary:
        return sync_card
    sync_card["showMongosyncVersion"] = True
    if mongosync_version:
        sync_card["mongosyncVersion"] = mongosync_version
    else:
        sync_card["mongosyncVersionMissingTitle"] = (
            "Missing initial log file for version capture.\n"
            "Upload all rotated mongosync files for full analysis"
        )
    return sync_card


def _build_sync_card(
    progress=None,
    metadata=None,
    *,
    progress_available=False,
    summary=False,
    mongosync_version=None,
):
    if progress_available and progress:
        info = progress.get("info") or ""
        info_lower = info.lower()
        phase = info or "—"
        lag_seconds = progress.get("lagTimeSeconds")
        metrics = _build_progress_metrics(progress, lag_seconds)

        collection_copy = progress.get("collectionCopy") or {}
        copied = collection_copy.get("estimatedCopiedBytes")
        total = collection_copy.get("estimatedTotalBytes")
        state = (progress.get("state") or "").upper()

        copy_percent = _bytes_copy_percent(copied, total)
        copy_indeterminate = copy_percent is None and state in ("RUNNING", "INITIALIZING")
        copy_prefix = _byte_copy_prefix(info_lower)
        copied_label = (
            f"{copy_prefix}{format_bytes_compact(copied)} of {format_bytes_compact(total)}"
        )

        return _apply_summary_mongosync_version(
            {
            "phase": phase,
            "copyPercent": copy_percent,
            "copyIndeterminate": copy_indeterminate,
            "copiedLabel": copied_label,
            "copiedTitle": _copied_bytes_title(copied, total),
            "collectionsCopiedLabel": _build_collections_copied_label(metadata, progress),
            "partitionsCopiedLabel": _build_partitions_copied_label(metadata),
            "showCopyProgress": copy_percent is not None or copy_indeterminate,
            "metrics": metrics,
            "metadataMetrics": _build_metadata_metrics(metadata, summary=summary),
            "phaseStartTimes": _build_phase_start_times(metadata, summary=summary),
            },
            summary=summary,
            mongosync_version=mongosync_version,
        )

    phase = _display_or_dash(metadata.get("phase") if metadata else None)
    phase_lower = (metadata.get("phase") or "").lower() if metadata else ""
    lag_seconds = metadata.get("lagTimeSeconds") if metadata else None
    state = (metadata.get("state") or "").upper() if metadata else ""

    copied = metadata.get("copiedBytes") if metadata else None
    total = metadata.get("totalBytes") if metadata else None

    copy_percent = _bytes_copy_percent(copied, total)
    copy_indeterminate = False
    copied_label = None
    show_copy_progress = False

    if total and total > 0 and copied is not None:
        copy_prefix = _byte_copy_prefix(phase_lower)
        copied_label = (
            f"{copy_prefix}{format_bytes_compact(copied)} of {format_bytes_compact(total)}"
        )
    if copy_percent is not None:
        show_copy_progress = True
    elif state in ("RUNNING", "INITIALIZING"):
        copy_indeterminate = True
        show_copy_progress = True

    return _apply_summary_mongosync_version(
        {
        "phase": phase,
        "copyPercent": copy_percent,
        "copyIndeterminate": copy_indeterminate,
        "copiedLabel": copied_label,
        "copiedTitle": _copied_bytes_title(copied, total) if copied_label else None,
        "collectionsCopiedLabel": _build_collections_copied_label(metadata, progress),
        "partitionsCopiedLabel": _build_partitions_copied_label(metadata),
        "showCopyProgress": show_copy_progress,
        "metrics": _build_db_only_metrics(lag_seconds),
        "metadataMetrics": _build_metadata_metrics(metadata, summary=summary),
        "phaseStartTimes": _build_phase_start_times(metadata, summary=summary),
        },
        summary=summary,
        mongosync_version=mongosync_version,
    )


_INDEX_BUILDING_DESCRIPTION = (
    "Indexes rebuilt on the destination cluster after the data copy."
)

_INDEX_BUILDING_FALLBACK_DESCRIPTION_SUFFIX = (
    " Progress from metadata (approximate)."
)


def _build_index_building_card(
    built, total, coll_finished, coll_total, *, metadata_fallback=False
):
    if not total or not coll_total:
        return None

    percent = None
    if total > 0:
        percent = min(100.0, (built / total) * 100)

    description = _INDEX_BUILDING_DESCRIPTION
    if metadata_fallback:
        description += _INDEX_BUILDING_FALLBACK_DESCRIPTION_SUFFIX

    return {
        "title": "Index building",
        "description": description,
        "built": built,
        "total": total,
        "percent": percent,
        "summary": f"{format_count(built)} of {format_count(total)} indexes built",
        "metrics": [
            {
                "label": "Indexes built",
                "value": f"{format_count(built)} / {format_count(total)}",
                "small": True,
            },
            {
                "label": "Collections finished building indexes",
                "value": f"{format_count(coll_finished)} / {format_count(coll_total)}",
                "small": True,
            },
        ],
    }


def _build_index_building(progress):
    index_building = progress.get("indexBuilding")
    if not index_building or not isinstance(index_building, dict):
        return None

    return _build_index_building_card(
        built=index_building.get("indexesBuilt") or 0,
        total=index_building.get("totalIndexesToBuild") or 0,
        coll_finished=index_building.get("collectionsFinished") or 0,
        coll_total=index_building.get("collectionsTotal") or 0,
    )


def _build_index_building_from_metadata(metadata):
    if not metadata:
        return None
    if metadata.get("buildIndexesRaw") == "never":
        return None
    if not index_build_progress_allowed(
        metadata.get("buildIndexesRaw"),
        metadata.get("syncPhase"),
    ):
        return None
    return _build_index_building_card(
        built=metadata.get("indexIndexesBuilt") or 0,
        total=metadata.get("indexIndexesTotal") or 0,
        coll_finished=metadata.get("indexCollectionsFinished") or 0,
        coll_total=metadata.get("indexCollectionsTotal") or 0,
        metadata_fallback=True,
    )


def _build_index_building_info(metadata):
    if not metadata:
        return None
    message = describe_build_indexes_policy(metadata.get("buildIndexesRaw"))
    if not message:
        return None
    return {
        "title": "Index building",
        "description": message,
        "mode": "info",
    }


def _build_natural_order(metadata):
    if not metadata:
        return None
    rows = metadata.get("naturalOrderRows")
    if not rows:
        return None
    return {
        "title": "Copy in natural order",
        "description": (
            "These collections are copied in natural insertion order instead of parallel "
            "partitions. Mongosync uses this when _ids cannot be range-partitioned "
            "efficiently (for example random UUIDs or string keys), or when the "
            "collection is capped."
        ),
        "label": "Natural order collections",
        "rows": rows,
    }


def _build_filtered_migration(metadata):
    if not metadata or not metadata.get("namespaceFilterActive"):
        return None
    return {
        "title": "Filtered migration",
        "inclusion": {
            "label": "Include",
            "rows": metadata.get("inclusionFilterRows") or [],
        },
        "exclusion": {
            "label": "Exclude",
            "rows": metadata.get("exclusionFilterRows") or [],
        },
    }


def _build_direction_from_metadata(metadata):
    if not metadata:
        return None
    source_addr = metadata.get("directionSource")
    dest_addr = metadata.get("directionDestination")
    if not source_addr and not dest_addr:
        return None
    return {
        "source": {"address": source_addr or "—"},
        "destination": {"address": dest_addr or "—"},
    }


def _build_direction(progress):
    direction = progress.get("directionMapping") or {}
    source_addr = direction.get("Source")
    dest_addr = direction.get("Destination")
    if not source_addr and not dest_addr:
        return None

    source = progress.get("source") or {}
    destination = progress.get("destination") or {}

    def ping_label(side):
        ms = side.get("pingLatencyMs") if isinstance(side, dict) else None
        if ms is None or ms < 0:
            return "unreachable"
        return f"{ms} ms"

    return {
        "source": {"address": source_addr or "—", "ping": ping_label(source)},
        "destination": {"address": dest_addr or "—", "ping": ping_label(destination)},
    }


def _verification_side_kv(side_data):
    if not side_data:
        side_data = {}
    return [
        {"label": "Phase", "value": side_data.get("phase") or "—"},
        {
            "label": "Collections scanned",
            "value": format_ratio(
                side_data.get("scannedCollectionCount"),
                side_data.get("totalCollectionCount"),
            ),
        },
        {
            "label": "Documents hashed",
            "value": format_ratio(
                side_data.get("hashedDocumentCount"),
                side_data.get("estimatedDocumentCount"),
            ),
        },
        _duration_metric("Lag time", side_data.get("lagTimeSeconds")),
    ]


_VERIFICATION_FALLBACK_DESCRIPTION_SUFFIX = (
    " Progress from verifier persistence metadata (approximate)."
)


def _build_verification(progress, *, metadata_fallback=False):
    verification = progress.get("verification")
    if not verification or not isinstance(verification, dict):
        return None

    description = "Progress of mongosync's embedded data verifier."
    if metadata_fallback:
        description += _VERIFICATION_FALLBACK_DESCRIPTION_SUFFIX

    return {
        "title": "Embedded Verifier",
        "description": description,
        "source": _verification_side_kv(verification.get("source")),
        "destination": _verification_side_kv(verification.get("destination")),
    }


def _progress_has_verification_data(progress):
    verification = (progress or {}).get("verification")
    if not verification or not isinstance(verification, dict):
        return False
    return bool(verification.get("source") or verification.get("destination"))


def _build_verification_info(metadata):
    message = describe_verification_mode(metadata.get("verificationModeRaw"))
    if not message:
        return None
    return {
        "title": "Embedded Verifier",
        "description": message,
        "mode": "info",
    }


def _resolve_verification_display(progress, metadata, *, progress_available=False):
    mode = normalize_verification_mode(
        metadata.get("verificationModeRaw") if metadata else None
    )
    if mode == "disabled":
        return _build_verification_info(metadata)
    if progress_available and progress and _progress_has_verification_data(progress):
        return _build_verification(progress)
    if metadata and metadata.get("verificationProgress"):
        if verification_progress_allowed(
            metadata.get("verificationModeRaw"),
            metadata.get("syncPhase"),
        ):
            card = _build_verification(
                {"verification": metadata["verificationProgress"]},
                metadata_fallback=True,
            )
            if card:
                return card
    if mode == "startAtCEA":
        sync_phase = metadata.get("syncPhase") if metadata else None
        if not sync_phase and progress:
            sync_phase = (progress.get("info") or "").strip()
        if not is_during_or_after_cea(sync_phase):
            return _build_verification_info(metadata)
        return None
    if progress_available and progress:
        return _build_verification(progress)
    return None


def _effective_progress_for_badges(progress, metadata, *, progress_available=False):
    """Merge metadata verifier fallback into progress for toolbar badge logic."""
    effective = dict(progress) if progress else {}
    if metadata and metadata.get("verificationProgress"):
        if not _progress_has_verification_data(effective):
            if verification_progress_allowed(
                metadata.get("verificationModeRaw"),
                metadata.get("syncPhase"),
            ):
                effective["verification"] = metadata["verificationProgress"]
    return effective if effective else None


_VERIFICATION_COMPLETE_PHASES = frozenset({"complete", "completed", "done", "finished"})
_VERIFICATION_INACTIVE_PHASES = frozenset({"not started", ""})


def _verification_side_complete(side_data):
    """
    Return True if side verification is complete, False if incomplete,
    or None if the side is not active (ignore for toolbar badge).
    """
    if not side_data or not isinstance(side_data, dict):
        return None

    hashed = side_data.get("hashedDocumentCount") or 0
    estimated = side_data.get("estimatedDocumentCount")
    if estimated and estimated > 0:
        return hashed >= estimated

    phase = (side_data.get("phase") or "").lower().strip()
    if phase in _VERIFICATION_COMPLETE_PHASES:
        return True
    if phase in _VERIFICATION_INACTIVE_PHASES:
        return None
    if phase:
        return False
    return None


def _verification_activity_active(progress, verification_card):
    if not verification_card or verification_card.get("mode") == "info":
        return False
    if not progress:
        return False

    verification = progress.get("verification") or {}
    active_statuses = []
    for side in ("source", "destination"):
        side_data = verification.get(side)
        if not side_data:
            continue
        status = _verification_side_complete(side_data)
        if status is not None:
            active_statuses.append(status)

    if not active_statuses:
        return False
    return not all(active_statuses)


def _index_build_activity_active(index_card):
    if not index_card or index_card.get("mode") == "info":
        return False
    total = index_card.get("total") or 0
    built = index_card.get("built") or 0
    return total > 0 and built < total


def _build_toolbar_badges(progress, verification_card, index_card):
    badges = []
    if _verification_activity_active(progress, verification_card):
        badges.append({"label": "VERIFYING", "color": "blue"})
    if _index_build_activity_active(index_card):
        badges.append({"label": "INDEXING", "color": "blue"})
    return badges


def _build_display(
    progress,
    metadata,
    *,
    progress_available=False,
    summary=False,
    mongosync_version=None,
):
    if progress_available and progress:
        state = (progress.get("state") or "").upper() or "—"
        state_badge = _derive_state_badge(progress)
    else:
        state = (metadata.get("state") or "").upper() if metadata else "—"
        state_badge = _derive_state_badge_from_state(metadata.get("state") if metadata else None)

    sync = _build_sync_card(
        progress=progress,
        metadata=metadata,
        progress_available=progress_available,
        summary=summary,
        mongosync_version=mongosync_version,
    )

    display = {
        "state": state or "—",
        "stateBadge": state_badge,
        "toolbarBadges": [],
        "sync": sync,
        "indexBuilding": None,
        "direction": None,
        "verification": None,
        "naturalOrder": None,
        "filteredMigration": None,
    }

    index_building = None
    if progress_available and progress:
        index_building = _build_index_building(progress)
        if index_building and metadata and not summary and should_suppress_index_build_progress(
            metadata.get("buildIndexesRaw"),
            metadata.get("syncPhase"),
        ):
            index_building = None
        display["direction"] = _build_direction(progress)
    if not index_building and metadata:
        index_building = _build_index_building_from_metadata(metadata)
    if not index_building and metadata:
        index_building = _build_index_building_info(metadata)
    display["indexBuilding"] = index_building

    display["verification"] = _resolve_verification_display(
        progress, metadata, progress_available=progress_available
    )
    effective_progress = _effective_progress_for_badges(
        progress if progress_available else None,
        metadata,
        progress_available=progress_available,
    )
    display["toolbarBadges"] = _build_toolbar_badges(
        effective_progress,
        display["verification"],
        display["indexBuilding"],
    )

    if not display["direction"] and metadata:
        display["direction"] = _build_direction_from_metadata(metadata)

    display["naturalOrder"] = _build_natural_order(metadata)
    display["filteredMigration"] = _build_filtered_migration(metadata)

    return display


def _progress_has_index_building(progress):
    if not progress:
        return False
    index_building = progress.get("indexBuilding")
    return isinstance(index_building, dict) and bool(index_building)


def _resolve_data_sources(endpoint_url, connection_string, *, progress_available, metadata, progress_warning, metadata_warning):
    progress_status = None
    if endpoint_url:
        if progress_available:
            progress_status = STATUS_ACTIVE
        elif progress_warning:
            progress_status = STATUS_UNAVAILABLE

    metadata_status = None
    if connection_string:
        if metadata:
            metadata_status = STATUS_ACTIVE
        elif metadata_warning:
            metadata_status = STATUS_UNAVAILABLE

    return build_data_sources(
        progress_configured=bool(endpoint_url),
        metadata_configured=bool(connection_string),
        progress_status=progress_status,
        metadata_status=metadata_status,
    )


def build_live_monitor_payload(endpoint_url=None, connection_string=None):
    """
    Build the Live Monitoring tab payload from progress endpoint and/or metadata DB.
    """
    base = {
        "warnings": [],
        "dataSources": None,
        "progressWarning": None,
        "metadataWarning": None,
        "display": None,
        "error": None,
    }

    progress = None
    warnings = []
    progress_available = False
    progress_warning = None
    metadata = None
    metadata_warning = None

    if endpoint_url:
        try:
            progress, warnings = fetch_progress(endpoint_url)
            progress_available = True
        except ProgressFetchError as e:
            progress_warning = f"Progress endpoint is not responding: {e}"
            logger.warning("Progress fetch failed: %s", e)

    if connection_string:
        index_progress_needed = not (
            progress_available and _progress_has_index_building(progress)
        )
        verification_progress_needed = not (
            progress_available and _progress_has_verification_data(progress)
        )
        try:
            metadata = fetch_metadata_status(
                connection_string,
                index_progress_needed=index_progress_needed,
                verification_progress_needed=verification_progress_needed,
            )
        except MetadataFetchError as e:
            metadata_warning = str(e)
            logger.warning("Metadata fetch failed: %s", e)

    if not progress_available and not metadata:
        errors = []
        if progress_warning:
            errors.append(progress_warning)
        if metadata_warning:
            errors.append(metadata_warning)
        if not errors:
            errors.append(
                "No progress endpoint URL or metadata connection string is configured."
            )
        base["error"] = " ".join(errors)
        base["progressWarning"] = progress_warning
        base["metadataWarning"] = metadata_warning
        base["dataSources"] = _resolve_data_sources(
            endpoint_url,
            connection_string,
            progress_available=progress_available,
            metadata=metadata,
            progress_warning=progress_warning,
            metadata_warning=metadata_warning,
        )
        return base

    base["warnings"] = warnings if progress_available else []
    base["progressWarning"] = progress_warning
    base["metadataWarning"] = metadata_warning
    base["display"] = _build_display(
        progress, metadata, progress_available=progress_available
    )
    base["dataSources"] = _resolve_data_sources(
        endpoint_url,
        connection_string,
        progress_available=progress_available,
        metadata=metadata,
        progress_warning=progress_warning,
        metadata_warning=metadata_warning,
    )
    return base


def progress_monitor_no_config_response(connection_string=None):
    """Response when neither progress endpoint nor metadata connection is configured."""
    return {
        "warnings": [],
        "dataSources": build_data_sources(
            progress_configured=False,
            metadata_configured=bool(connection_string),
            progress_status=None,
            metadata_status=STATUS_UNAVAILABLE if connection_string else None,
        ),
        "progressWarning": None,
        "metadataWarning": None,
        "display": None,
        "error": (
            "No progress endpoint or metadata connection string is configured. "
            "Return to Migration monitoring home and provide a Mongosync progress endpoint "
            "(host and port; path /api/v1/progress is fixed) and/or a metadata connection string, "
            "or set MI_PROGRESS_ENDPOINT_URL / MI_CONNECTION_STRING."
        ),
    }
