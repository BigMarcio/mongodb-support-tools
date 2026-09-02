"""
Migration Verifier metrics gathering and visualization.
Provides monitoring for MongoDB migration-verifier tool.
https://github.com/mongodb-labs/migration-verifier
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone

from bson import Timestamp

from flask import render_template, request
from pymongo.errors import PyMongoError

from .data_sources import STATUS_ACTIVE, STATUS_UNAVAILABLE, build_data_sources
from .session_support import SESSION_COOKIE_NAME

logger = logging.getLogger(__name__)

_VERIFIER_SUMMARY_CACHE_KEY = "verifier_summary_cache"

_EXCLUDED_TASK_TYPES = ["primary"]
_TASK_STATUSES_PENDING = ["added", "processing"]
_TASK_STATUSES_FAILED = ["failed", "mismatch"]
_VERIFY_DOCUMENTS = "verifyDocuments"
_VERIFY_COLLECTION = "verifyCollection"


def _current_session_id():
    return request.cookies.get(SESSION_COOKIE_NAME)


def get_verifier_summary_cache(session_id=None):
    """Return cached manual summary payload for a session, or None."""
    from .app_config import session_store

    sid = session_id if session_id is not None else _current_session_id()
    if not sid:
        return None
    data = session_store.get_session(sid)
    cache = data.get(_VERIFIER_SUMMARY_CACHE_KEY)
    if not cache or not isinstance(cache, dict):
        return None
    return cache


def set_verifier_summary_cache(verification_summary, session_id=None):
    """Store manual summary results in the server session."""
    from .app_config import session_store

    sid = session_id if session_id is not None else _current_session_id()
    if not sid:
        return
    session_store.update_session(sid, {
        _VERIFIER_SUMMARY_CACHE_KEY: {
            "fetched_at": time.time(),
            "verification_summary": verification_summary,
        },
    })


def clear_verifier_summary_cache(session_id=None):
    """Remove cached manual summary from the server session."""
    from .app_config import session_store

    sid = session_id if session_id is not None else _current_session_id()
    if not sid:
        return
    session_store.update_session(sid, {_VERIFIER_SUMMARY_CACHE_KEY: None})


def progress_allows_summary_run(progress_display):
    """True when gen > 0 and progress reports document or metadata mismatches."""
    if not progress_display or progress_display.get("generation", 0) == 0:
        return False
    tasks = progress_display.get("tasks") or {}
    if (tasks.get("metadataMismatch") or 0) > 0:
        return True
    return bool(progress_display.get("longestDocMismatch"))


def build_summary_cache_meta(cache, *, eligible):
    """Build summaryCache object for progress/run API responses."""
    from .app_config import VERIFIER_SUMMARY_COOLDOWN_SECS

    if not eligible:
        return None

    fetched_at = None
    verification_summary = None
    if cache and isinstance(cache, dict):
        fetched_at = cache.get("fetched_at")
        verification_summary = cache.get("verification_summary")

    age_secs = None
    cooldown_remaining = 0
    if fetched_at:
        age_secs = max(0, int(time.time() - fetched_at))
        cooldown_remaining = max(0, VERIFIER_SUMMARY_COOLDOWN_SECS - age_secs)

    return {
        "eligible": True,
        "lastFetchedAt": fetched_at,
        "lastFetchedAgeSecs": age_secs,
        "cooldownRemainingSecs": cooldown_remaining,
        "verificationSummary": verification_summary,
    }


def _base_task_match(generation):
    """Match filter for verification_tasks, excluding coordinator tasks."""
    return {
        "generation": generation,
        "type": {"$nin": _EXCLUDED_TASK_TYPES},
    }


def _field_from_doc(doc, *keys):
    """Return the first present field from a BSON document (case variants).

    migration-verifier uses the Go mongo-driver v2 default struct encoding, which
    lowercases the entire field name (e.g. MetadataVersion -> metadataversion).
    """
    if not doc:
        return None
    for key in keys:
        if key in doc:
            return doc[key]
    lower_index = {k.lower(): k for k in doc if k != "_id"}
    for key in keys:
        actual = lower_index.get(key.lower())
        if actual is not None:
            return doc[actual]
    return None


def _read_generation_doc(db):
    """Read the single generation metadata document."""
    return db.generation.find_one({})


def get_current_generation(db):
    """Read authoritative generation from the generation collection.

    Falls back to max generation in verification_tasks for older metadata.
    """
    doc = _read_generation_doc(db)
    gen = _field_from_doc(doc, "generation", "Generation")
    if gen is not None:
        return gen

    latest = db.verification_tasks.find_one(
        {},
        {"generation": 1},
        sort=[("generation", -1)],
    )
    if not latest:
        return None
    return latest.get("generation", 0)


def _load_mismatches_for_tasks(db, generation, task_ids):
    """Load mismatch details keyed by task ID (supports legacy ``task`` field)."""
    if not task_ids:
        return {}

    result = {task_id: [] for task_id in task_ids}

    try:
        by_task_id = list(db.mismatches.find(
            {"generation": generation, "taskID": {"$in": task_ids}},
            {"taskID": 1, "detail": 1},
        ))
        for m in by_task_id:
            task_id = m.get("taskID")
            detail = m.get("detail")
            if task_id in result and detail:
                result[task_id].append(detail)

        missing_ids = [tid for tid in task_ids if not result[tid]]
        if missing_ids:
            by_legacy = list(db.mismatches.find(
                {"task": {"$in": missing_ids}},
                {"task": 1, "detail": 1},
            ))
            for m in by_legacy:
                task_id = m.get("task")
                detail = m.get("detail")
                if task_id in result and detail:
                    result[task_id].append(detail)
    except Exception as e:
        logger.warning("Failed to load mismatches for tasks: %s", e)

    return result


def get_verification_summary(db, generation):
    """Get summary of verification tasks for a generation."""
    pipeline = [
        {"$match": _base_task_match(generation)},
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1},
        }},
    ]
    results = list(db.verification_tasks.aggregate(pipeline))
    summary = {
        "completed": 0,
        "failed": 0,
        "mismatch": 0,
        "pending": 0,
        "processing": 0,
    }
    for r in results:
        status = r["_id"]
        if status in summary:
            summary[status] = r["count"]
        elif status == "added":
            summary["pending"] += r["count"]
    return summary


def get_failed_tasks(db, generation, limit=None, document_only=False):
    """Get failed verification tasks with details including mismatch info."""
    if limit is None:
        from .app_config import VERIFIER_FAILED_TASKS_LIMIT
        limit = VERIFIER_FAILED_TASKS_LIMIT

    match = {
        **_base_task_match(generation),
        "status": {"$in": _TASK_STATUSES_FAILED},
    }
    if document_only:
        match["type"] = {"$ne": _VERIFY_COLLECTION}

    pipeline = [
        {"$match": match},
        {"$sort": {"begin_time": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 1,
            "type": 1,
            "status": 1,
            "query_filter": 1,
            "_ids": 1,
            "begin_time": 1,
        }},
    ]
    tasks = list(db.verification_tasks.aggregate(pipeline))

    if tasks:
        task_ids = [t["_id"] for t in tasks]
        mismatch_map = _load_mismatches_for_tasks(db, generation, task_ids)
        for t in tasks:
            t["mismatches"] = mismatch_map.get(t["_id"], [])

    return tasks


def get_namespace_stats(db, generation):
    """Get task-level statistics grouped by namespace for the specified generation."""
    pipeline = [
        {"$match": _base_task_match(generation)},
        {"$group": {
            "_id": "$query_filter.namespace",
            "total": {"$sum": 1},
            "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
            "failed": {"$sum": {"$cond": [{"$in": ["$status", _TASK_STATUSES_FAILED]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$in": ["$status", _TASK_STATUSES_PENDING]}, 1, 0]}},
        }},
        {"$sort": {"failed": -1, "total": -1}},
        {"$limit": 15},
    ]
    return list(db.verification_tasks.aggregate(pipeline, allowDiskUse=True))


def _namespace_stats_add_fields_stage():
    """Shared $addFields stage for namespace statistics aggregations."""
    finished_statuses = ["completed", "failed"]
    return {
        "$addFields": {
            "partitionAdded": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        {"$eq": ["$status", "added"]},
                    ]},
                    1,
                    0,
                ],
            },
            "partitionProcessing": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        {"$eq": ["$status", "processing"]},
                    ]},
                    1,
                    0,
                ],
            },
            "partitionDone": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        {"$in": ["$status", finished_statuses]},
                    ]},
                    1,
                    0,
                ],
            },
            "docsCompared": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        {"$in": ["$status", finished_statuses]},
                    ]},
                    "$found_source_documents_count",
                    0,
                ],
            },
            "bytesCompared": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        {"$in": ["$status", finished_statuses]},
                    ]},
                    "$source_bytes_count",
                    0,
                ],
            },
            "totalDocs": {
                "$cond": {
                    "if": {"$eq": ["$generation", 0]},
                    "then": {"$cond": {
                        "if": {"$eq": ["$type", _VERIFY_COLLECTION]},
                        "then": "$documents_count",
                        "else": 0,
                    }},
                    "else": {"$cond": {
                        "if": {"$eq": ["$type", _VERIFY_DOCUMENTS]},
                        "then": "$documents_count",
                        "else": 0,
                    }},
                },
            },
            "totalBytes": {
                "$cond": [
                    {"$eq": ["$type", _VERIFY_COLLECTION]},
                    "$source_bytes_count",
                    0,
                ],
            },
        },
    }


def _namespace_stats_group_stage(group_id):
    return {
        "$group": {
            "_id": group_id,
            "docsCompared": {"$sum": "$docsCompared"},
            "totalDocs": {"$sum": "$totalDocs"},
            "bytesCompared": {"$sum": "$bytesCompared"},
            "totalBytes": {"$sum": "$totalBytes"},
            "partitionsAdded": {"$sum": "$partitionAdded"},
            "partitionsProcessing": {"$sum": "$partitionProcessing"},
            "partitionsDone": {"$sum": "$partitionDone"},
        },
    }


def _namespace_stats_pipeline(generation=None, generations=None):
    """Port of migration-verifier GetPersistedNamespaceStatistics aggregation."""
    if generations is not None:
        unique_gens = sorted({g for g in generations if g is not None})
        if not unique_gens:
            return []
        match_stage = {
            "type": {"$in": [_VERIFY_DOCUMENTS, _VERIFY_COLLECTION]},
            "generation": {"$in": unique_gens},
        }
        group_id = {
            "generation": "$generation",
            "namespace": "$query_filter.namespace",
        }
        sort_stage = {"$sort": {"_id.generation": 1, "_id.namespace": 1}}
    else:
        match_stage = {
            "type": {"$in": [_VERIFY_DOCUMENTS, _VERIFY_COLLECTION]},
            "generation": generation,
        }
        group_id = "$query_filter.namespace"
        sort_stage = {"$sort": {"_id": 1}}

    return [
        {"$match": match_stage},
        _namespace_stats_add_fields_stage(),
        _namespace_stats_group_stage(group_id),
        sort_stage,
    ]


def _split_batch_namespace_statistics(rows):
    """Group batched namespace stats rows by generation number."""
    by_generation = {}
    for row in rows:
        group_key = row.get("_id") or {}
        generation = group_key.get("generation")
        if generation is None:
            continue
        by_generation.setdefault(generation, []).append({
            "_id": group_key.get("namespace"),
            "docsCompared": row.get("docsCompared") or 0,
            "totalDocs": row.get("totalDocs") or 0,
            "bytesCompared": row.get("bytesCompared") or 0,
            "totalBytes": row.get("totalBytes") or 0,
            "partitionsAdded": row.get("partitionsAdded") or 0,
            "partitionsProcessing": row.get("partitionsProcessing") or 0,
            "partitionsDone": row.get("partitionsDone") or 0,
        })
    return by_generation


def get_persisted_namespace_statistics_batch(db, generations):
    """Namespace statistics for multiple generations in one aggregation."""
    pipeline = _namespace_stats_pipeline(generations=generations)
    if not pipeline:
        return {}
    try:
        rows = list(db.verification_tasks.aggregate(pipeline, allowDiskUse=True))
        return _split_batch_namespace_statistics(rows)
    except Exception as e:
        logger.warning("Failed to get batched namespace statistics: %s", e)
        return {}


def get_persisted_namespace_statistics(db, generation):
    """Document-level namespace progress (aligned with migration-verifier /progress)."""
    try:
        return list(db.verification_tasks.aggregate(
            _namespace_stats_pipeline(generation=generation),
            allowDiskUse=True,
        ))
    except Exception as e:
        logger.warning("Failed to get persisted namespace statistics: %s", e)
        return []


def get_generation_doc_progress(db, generation):
    """Sum namespace document stats into generation-level totals."""
    stats = get_generation_progress_stats(db, generation)
    docs_compared = stats["docsCompared"]
    total_docs = stats["totalDocs"]
    percent = (docs_compared / total_docs * 100) if total_docs > 0 else None
    return {
        "docsCompared": docs_compared,
        "totalDocs": total_docs,
        "docPercentComplete": round(percent, 1) if percent is not None else None,
    }


def _sum_namespace_stats_list(ns_stats):
    docs_compared = 0
    total_docs = 0
    partitions_done = 0
    partitions_pending = 0
    for ns in ns_stats:
        docs_compared += ns.get("docsCompared") or 0
        total_docs += ns.get("totalDocs") or 0
        partitions_done += ns.get("partitionsDone") or 0
        partitions_pending += (ns.get("partitionsAdded") or 0) + (ns.get("partitionsProcessing") or 0)
    return {
        "docsCompared": docs_compared,
        "totalDocs": total_docs,
        "partitionsDone": partitions_done,
        "partitionsTotal": partitions_done + partitions_pending,
    }


def get_generation_progress_stats(db, generation, ns_stats=None):
    """Sum namespace document and partition stats for a generation."""
    if ns_stats is None:
        ns_stats = get_persisted_namespace_statistics(db, generation)
    return _sum_namespace_stats_list(ns_stats)


def get_generation_history(db, limit=4, current_gen=None):
    """Get history of latest generations with their stats."""
    if current_gen is None:
        current_gen = get_current_generation(db)
    if current_gen is None:
        return []

    gen_range = list(range(max(0, current_gen - limit + 1), current_gen + 1))

    pipeline = [
        {"$match": {
            "generation": {"$in": gen_range},
            "type": {"$nin": _EXCLUDED_TASK_TYPES},
        }},
        {"$group": {
            "_id": "$generation",
            "total_tasks": {"$sum": 1},
            "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
            "failed": {"$sum": {"$cond": [{"$in": ["$status", _TASK_STATUSES_FAILED]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$in": ["$status", _TASK_STATUSES_PENDING]}, 1, 0]}},
            "first_task_time": {"$min": "$begin_time"},
        }},
        {"$sort": {"_id": -1}},
        {"$limit": limit},
    ]
    return list(db.verification_tasks.aggregate(pipeline, allowDiskUse=True))


def get_collection_metadata_mismatches(db, generation):
    """Fetch collection/index mismatches for a generation from the mismatches collection."""
    try:
        return list(db.mismatches.find(
            {"generation": generation, "taskType": _VERIFY_COLLECTION},
            {"generation": 1, "detail": 1},
        ))
    except Exception as e:
        logger.warning("Failed to fetch collection metadata mismatches: %s", e)
        return []


def get_generation_name(gen_num):
    """Get human-readable name for a generation."""
    if gen_num is None:
        return "Unknown"
    if gen_num == 0:
        return "Initial Verification"
    return f"Recheck #{gen_num}"


def _format_start_time(start_time):
    if start_time is None or start_time == "N/A":
        return "N/A"
    return str(start_time)[:16]


def _get_source_ns(task):
    qf = task.get("query_filter", {})
    return qf.get("namespace", "N/A") or "N/A"


def _get_dest_ns(task):
    qf = task.get("query_filter", {})
    return qf.get("to", qf.get("namespace", "N/A")) or "N/A"


def _format_single_mismatch_detail(detail, task_type=None):
    """Format one mismatch detail document for display."""
    if not detail or not isinstance(detail, dict):
        return None

    item_id = detail.get("id", "?")
    field_type = detail.get("field", "")
    details_str = detail.get("details", "")
    cluster = detail.get("cluster", "")

    is_collection = task_type == _VERIFY_COLLECTION or (
        task_type is None and not field_type and details_str
    )

    if is_collection or task_type == _VERIFY_COLLECTION:
        if "unique" in str(details_str).lower():
            if "src:" in details_str and "unique\": true" in details_str:
                if "dst:" in details_str and "unique" not in details_str.split("dst:")[1][:50]:
                    return f"Index '{item_id}' ({field_type}): unique constraint missing on {cluster}"
            return f"Index '{item_id}' ({field_type}): Mismatch on {cluster}"
        if "Missing" in details_str:
            return f"Index '{item_id}' ({field_type}): Missing on {cluster}"
        if details_str:
            short = details_str[:60] + ("..." if len(details_str) > 60 else "")
            return f"Index '{item_id}' ({field_type}): {short} ({cluster})"
        return f"Index '{item_id}' ({field_type}): ({cluster})"

    if field_type:
        short = details_str[:40] + ("..." if len(details_str) > 40 else "")
        return f"Doc '{item_id}', field '{field_type}': {short} ({cluster})"
    if details_str:
        short = details_str[:50] + ("..." if len(details_str) > 50 else "")
        return f"Doc '{item_id}': {short} ({cluster})"
    return f"Doc '{item_id}': ({cluster})"


def _get_mismatch_details(task):
    task_type = task.get("type", "")
    mismatches = task.get("mismatches", [])

    if mismatches:
        parts = []
        for detail in mismatches[:3]:
            formatted = _format_single_mismatch_detail(detail, task_type)
            if formatted:
                parts.append(formatted)
        if parts:
            result = "; ".join(parts)
            if len(mismatches) > 3:
                result += f"... +{len(mismatches) - 3} more"
            return result

    if task_type == _VERIFY_COLLECTION:
        return "Metadata mismatch (no details)"

    if task_type == _VERIFY_DOCUMENTS:
        ids = task.get("_ids", [])
        if ids:
            count = len(ids)
            sample = ", ".join([str(i)[:20] for i in ids[:5]])
            if count > 5:
                return f"{count} docs mismatched: {sample}..."
            return f"{count} docs: {sample}"
        return "Document mismatch (no details)"

    if task.get("_ids"):
        return f"{len(task.get('_ids', []))} items"
    return task.get("status", "N/A")


def _serialize_failed_task(task, generation):
    """Build a JSON-safe row for the failed-tasks dashboard card."""
    status = task.get("status", "")
    if status == "failed" and not task.get("mismatches"):
        details = "Task failed (check migration-verifier logs for details)"
    else:
        details = _get_mismatch_details(task)
    return {
        "namespace": _get_source_ns(task),
        "type": task.get("type", ""),
        "status": status,
        "details": details,
        "beginTime": _format_start_time(task.get("begin_time")),
    }


def get_failed_tasks_for_display(db, generation, limit=None):
    """Collect failed/mismatch document tasks for one generation."""
    if limit is None:
        from .app_config import VERIFIER_FAILED_TASKS_LIMIT
        limit = VERIFIER_FAILED_TASKS_LIMIT

    rows = []
    tasks = get_failed_tasks(db, generation, limit=limit, document_only=True)
    for task in tasks:
        rows.append(_serialize_failed_task(task, generation))
    rows.sort(key=lambda r: r.get("beginTime") or "", reverse=True)
    return rows[:limit]


def _derive_state_badge(current_gen, previous_gen_stats, collection_mismatches):
    if current_gen == 0:
        return {"label": "IN PROGRESS", "color": "blue"}
    if not previous_gen_stats:
        return {"label": "NO DATA", "color": "gray"}
    has_mismatches = (
        previous_gen_stats.get("failed", 0) > 0
        or bool(collection_mismatches)
    )
    if has_mismatches:
        return {"label": "MISMATCHES", "color": "yellow"}
    return {"label": "PASS", "color": "green"}


def _verifier_data_sources(
    endpoint_url=None,
    connection_string=None,
    *,
    progress_status=None,
    metadata_status=None,
):
    return build_data_sources(
        progress_configured=bool(endpoint_url),
        metadata_configured=bool(connection_string),
        progress_status=progress_status,
        metadata_status=metadata_status,
        metadata_label="Verifier metadata",
    )


def _safe_int(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _unwrap_option(value):
    """Unwrap migration-verifier option fields (scalar or null)."""
    if value is None:
        return None
    if isinstance(value, dict) and len(value) == 0:
        return None
    return value


def _format_bson_timestamp(ts):
    if not ts:
        return None
    t_val = None
    if isinstance(ts, Timestamp):
        t_val = ts.time
    elif isinstance(ts, dict):
        inner = ts.get("$timestamp") or ts
        t_val = inner.get("t")
    if t_val is None:
        return str(ts)
    dt_str = datetime.fromtimestamp(int(t_val), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return f"{dt_str} UTC"


def _phase_badge_from_progress(phase):
    phase_lower = (phase or "").strip().lower()
    mapping = {
        "idle": ("IDLE", "gray"),
        "check": ("CHECK", "blue"),
        "recheck": ("RECHECK", "yellow"),
    }
    label, color = mapping.get(phase_lower, (phase_lower.upper() or "—", "gray"))
    return {"label": label, "color": color}


def _serialize_generation_stats(stats):
    if not stats:
        return None
    compared = _safe_int(stats.get("docsCompared"))
    total = _safe_int(stats.get("totalDocs"))
    bytes_compared = _safe_int(stats.get("srcBytesCompared"))
    total_bytes = _safe_int(stats.get("totalSrcBytes"))
    result = {
        "docsCompared": compared,
        "totalDocs": total,
        "docsPercent": _percent(compared, total),
        "totalNamespaces": _safe_int(stats.get("totalNamespaces")),
    }
    if total_bytes > 0:
        result["srcBytesCompared"] = bytes_compared
        result["totalSrcBytes"] = total_bytes
        result["bytesPercent"] = _percent(bytes_compared, total_bytes)
    return result


def _serialize_change_stats(stats, label):
    if not stats:
        return None
    event_counts = stats.get("eventCounts") or {}
    lag = _unwrap_option(stats.get("lagSecs"))
    eps = _unwrap_option(stats.get("eventsPerSecond"))
    return {
        "label": label,
        "eventsPerSecond": _safe_float(eps),
        "lagSecs": _safe_int(lag) if lag is not None else None,
        "bufferSaturation": _safe_float(stats.get("bufferSaturation")),
        "eventCounts": {
            "insert": _safe_int(event_counts.get("insert")),
            "update": _safe_int(event_counts.get("update")),
            "replace": _safe_int(event_counts.get("replace")),
            "delete": _safe_int(event_counts.get("delete")),
        },
    }


def _serialize_longest_mismatch(mismatch):
    if not mismatch:
        return None
    return {
        "type": mismatch.get("type"),
        "namespace": mismatch.get("namespace"),
        "id": mismatch.get("_id"),
        "field": _unwrap_option(mismatch.get("field")),
        "detail": _unwrap_option(mismatch.get("detail")),
        "durationSecs": _safe_float(mismatch.get("durationSecs")),
    }


def build_verifier_progress_display(progress):
    """Map migration-verifier /progress fields to dashboard display object."""
    if not progress:
        return None

    phase = (progress.get("phase") or "").strip().lower()
    gen_stats = progress.get("generationStats") or {}
    verification_status = progress.get("verificationStatus") or {}
    gen0_stats = _unwrap_option(progress.get("gen0Stats"))

    compared = _safe_int(gen_stats.get("docsCompared"))
    total_docs = _safe_int(gen_stats.get("totalDocs"))
    bytes_compared = _safe_int(gen_stats.get("srcBytesCompared"))
    total_bytes = _safe_int(gen_stats.get("totalSrcBytes"))

    phase_descriptions = {
        "idle": "Verification is idle (not actively checking or rechecking).",
        "check": "Initial document check (generation 0) is in progress.",
        "recheck": "Recheck generation is in progress.",
    }

    error_val = progress.get("error")
    error_str = None
    if error_val is not None and error_val != "":
        error_str = str(error_val)

    display = {
        "phase": phase,
        "phaseLabel": phase.upper() if phase else "—",
        "phaseBadge": _phase_badge_from_progress(phase),
        "phaseDescription": phase_descriptions.get(phase),
        "generation": _safe_int(progress.get("generation")),
        "documents": {
            "compared": compared,
            "total": total_docs,
            "percent": _percent(compared, total_docs),
        },
        "bytes": {
            "compared": bytes_compared,
            "total": total_bytes,
            "percent": _percent(bytes_compared, total_bytes) if total_bytes > 0 else None,
        },
        "totalNamespaces": _safe_int(gen_stats.get("totalNamespaces")),
        "tasks": {
            "total": _safe_int(verification_status.get("totalTasks")),
            "added": _safe_int(verification_status.get("addedTasks")),
            "processing": _safe_int(verification_status.get("processingTasks")),
            "failed": _safe_int(verification_status.get("failedTasks")),
            "completed": _safe_int(verification_status.get("completedTasks")),
            "metadataMismatch": _safe_int(verification_status.get("metadataMismatchTasks")),
        },
        "docsComparedPerSecond": _safe_float(progress.get("docsComparedPerSecond")),
        "srcBytesComparedPerSecond": _safe_float(progress.get("srcBytesComparedPerSecond")),
        "srcChangeStats": _serialize_change_stats(progress.get("srcChangeStats"), "Source"),
        "dstChangeStats": _serialize_change_stats(progress.get("dstChangeStats"), "Destination"),
        "srcLastRecheckedTS": _format_bson_timestamp(progress.get("srcLastRecheckedTS")),
        "dstLastRecheckedTS": _format_bson_timestamp(progress.get("dstLastRecheckedTS")),
        "recentRecheckSecs": [
            round(float(x), 2)
            for x in (progress.get("recentRecheckSecs") or [])
            if x is not None
        ],
        "totalRechecksDone": _safe_int(progress.get("totalRechecksDone")),
        "gen0Stats": _serialize_generation_stats(gen0_stats),
        "longestDocMismatch": _serialize_longest_mismatch(
            _unwrap_option(progress.get("longestDocMismatch"))
        ),
        "error": error_str,
    }
    display["summaryEligible"] = progress_allows_summary_run(display)
    return display


def _fetch_verifier_progress(endpoint_url):
    from .live_monitoring import fetch_verifier_progress

    progress, _warnings = fetch_verifier_progress(endpoint_url)
    return build_verifier_progress_display(progress)


def _serialize_ns_mismatch_row(row):
    if not row:
        return None
    return {
        "type": row.get("type"),
        "namespace": row.get("namespace"),
        "aspect": row.get("aspect"),
        "component": _unwrap_option(row.get("component")),
        "detail": _unwrap_option(row.get("detail")),
    }


def build_verifier_summary_display(summary, *, min_duration_secs=0, ns_limit=None):
    """Map migration-verifier /summary fields to dashboard display object."""
    if not summary:
        return None

    from .app_config import VERIFIER_FAILED_TASKS_LIMIT

    if ns_limit is None:
        ns_limit = VERIFIER_FAILED_TASKS_LIMIT

    doc_mm = summary.get("docMismatches") or {}
    by_type = doc_mm.get("byType") or {}
    by_namespace = doc_mm.get("byNamespace") or {}

    ns_rows = []
    for row in summary.get("nsMismatches") or []:
        serialized = _serialize_ns_mismatch_row(row)
        if serialized:
            ns_rows.append(serialized)

    eta = _unwrap_option(summary.get("estCheckSecsRemaining"))
    if eta is not None:
        eta = _safe_float(eta)

    notes = summary.get("notes") or []
    if not isinstance(notes, list):
        notes = []

    return {
        "estCheckSecsRemaining": eta,
        "docMismatches": {
            "total": _safe_int(doc_mm.get("total")),
            "byType": {str(k): _safe_int(v) for k, v in by_type.items()},
            "byNamespace": {str(k): _safe_int(v) for k, v in by_namespace.items()},
        },
        "nsMismatches": ns_rows[:ns_limit],
        "nsMismatchesLimit": ns_limit,
        "notes": [str(n) for n in notes if n],
        "minDurationSecs": min_duration_secs,
    }


def _fetch_verifier_summary(endpoint_url):
    from .app_config import (
        VERIFIER_SUMMARY_MIN_DURATION_SECS,
        build_verifier_summary_endpoint_url,
    )
    from .live_monitoring import fetch_summary

    summary_url = build_verifier_summary_endpoint_url(endpoint_url)
    if not summary_url:
        return None
    raw = fetch_summary(
        summary_url,
        min_duration_secs=VERIFIER_SUMMARY_MIN_DURATION_SECS,
    )
    return build_verifier_summary_display(
        raw,
        min_duration_secs=VERIFIER_SUMMARY_MIN_DURATION_SECS,
    )


def _derive_state_badge_from_progress(progress_display):
    """Toolbar badge from /progress when the progress endpoint is available."""
    if not progress_display:
        return {"label": "NO DATA", "color": "gray"}
    if progress_display.get("generation", 0) == 0:
        return {"label": "IN PROGRESS", "color": "blue"}
    tasks = progress_display.get("tasks") or {}
    if (tasks.get("metadataMismatch", 0) or 0) == 0 and not progress_display.get(
        "longestDocMismatch",
    ):
        return {"label": "PASS", "color": "green"}
    return {"label": "MISMATCHES", "color": "yellow"}


_METADATA_SECTION_LABELS = {
    "history": "Generation history",
    "ns_batch": "Namespace statistics",
    "summary": "Verification summary",
    "coll_mm": "Collection metadata mismatches",
    "failed": "Failed tasks",
}

_METADATA_SECTION_DEFAULTS = {
    "history": [],
    "ns_batch": {},
    "summary": {},
    "coll_mm": [],
    "failed": [],
}

_METADATA_SECTION_UNAVAILABLE_KEYS = {
    "history": "generationsUnavailable",
    "ns_batch": "namespacesUnavailable",
    "summary": "verificationCompletenessUnavailable",
    "coll_mm": "collectionMismatchesUnavailable",
    "failed": "failedTasksUnavailable",
}

_ENDPOINT_SKIPPED_SECTIONS = [
    "namespaces",
    "collectionMismatches",
    "verificationCompleteness",
    "failedTasks",
]


def _resolve_metadata_future(future, section_key, timeout_secs):
    """Wait for a metadata future; return (value, warning_or_none)."""
    label = _METADATA_SECTION_LABELS[section_key]
    default = _METADATA_SECTION_DEFAULTS[section_key]
    try:
        return future.result(timeout=timeout_secs), None
    except FuturesTimeoutError:
        msg = f"{label} unavailable: timed out after {timeout_secs}s"
        logger.warning("Verifier metadata section %s timed out", section_key)
        return default, msg
    except PyMongoError as e:
        msg = f"{label} unavailable: {e}"
        logger.warning("Verifier metadata section %s failed: %s", section_key, e)
        return default, msg
    except Exception as e:
        msg = f"{label} unavailable: {e}"
        logger.warning("Verifier metadata section %s failed: %s", section_key, e)
        return default, msg


def _call_metadata_section(fn, section_key):
    """Run a metadata query synchronously; return (value, warning_or_none)."""
    label = _METADATA_SECTION_LABELS[section_key]
    default = _METADATA_SECTION_DEFAULTS[section_key]
    try:
        return fn(), None
    except PyMongoError as e:
        msg = f"{label} unavailable: {e}"
        logger.warning("Verifier metadata section %s failed: %s", section_key, e)
        return default, msg
    except Exception as e:
        msg = f"{label} unavailable: {e}"
        logger.warning("Verifier metadata section %s failed: %s", section_key, e)
        return default, msg


def _fetch_metadata_sections(
    db,
    *,
    gen_limit,
    current_gen,
    gen_range,
    previous_gen,
    include_verification_completeness,
    endpoint_configured,
    timeout_secs,
):
    """Load metadata sections in parallel or sequential (endpoint lean) mode."""
    warnings = []
    unavailable = {}
    attempted = []
    section_values = {}

    if endpoint_configured:
        for section_key, fn in _iter_metadata_section_calls(
            db,
            gen_limit=gen_limit,
            current_gen=current_gen,
            gen_range=gen_range,
            previous_gen=previous_gen,
            include_verification_completeness=include_verification_completeness,
            endpoint_configured=endpoint_configured,
        ):
            attempted.append(section_key)
            value, warning = _call_metadata_section(fn, section_key)
            section_values[section_key] = value
            if warning:
                warnings.append(warning)
                unavailable[_METADATA_SECTION_UNAVAILABLE_KEYS[section_key]] = True
        return section_values, warnings, unavailable, attempted

    max_workers = 1
    if include_verification_completeness:
        max_workers += 1
    if previous_gen is not None:
        max_workers += 2

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for section_key, fn in _iter_metadata_section_calls(
            db,
            gen_limit=gen_limit,
            current_gen=current_gen,
            gen_range=gen_range,
            previous_gen=previous_gen,
            include_verification_completeness=include_verification_completeness,
            endpoint_configured=endpoint_configured,
        ):
            attempted.append(section_key)
            futures[section_key] = executor.submit(fn)

        for section_key, future in futures.items():
            value, warning = _resolve_metadata_future(
                future, section_key, timeout_secs,
            )
            section_values[section_key] = value
            if warning:
                warnings.append(warning)
                unavailable[_METADATA_SECTION_UNAVAILABLE_KEYS[section_key]] = True

    return section_values, warnings, unavailable, attempted


def _iter_metadata_section_calls(
    db,
    *,
    gen_limit,
    current_gen,
    gen_range,
    previous_gen,
    include_verification_completeness,
    endpoint_configured,
):
    """Yield (section_key, callable) pairs for metadata loading."""
    if endpoint_configured:
        yield "history", (
            lambda: get_generation_history(db, gen_limit, current_gen)
        )
        yield "ns_batch", (
            lambda: get_persisted_namespace_statistics_batch(db, gen_range)
        )
        return

    yield "history", lambda: get_generation_history(db, gen_limit, current_gen)
    yield "ns_batch", (
        lambda: get_persisted_namespace_statistics_batch(db, gen_range)
    )
    if include_verification_completeness:
        yield "summary", lambda: get_verification_summary(db, current_gen)
    if previous_gen is not None:
        yield "coll_mm", (
            lambda: get_collection_metadata_mismatches(db, previous_gen)
        )
        yield "failed", (
            lambda: get_failed_tasks_for_display(db, previous_gen)
        )


def _section_succeeded(section_key, unavailable_flags):
    """True when a section was attempted and completed without error."""
    flag = _METADATA_SECTION_UNAVAILABLE_KEYS[section_key]
    return not unavailable_flags.get(flag)


def _build_metadata_display(
    db,
    db_name,
    *,
    include_verification_completeness=True,
    endpoint_configured=False,
):
    """Build metadata-driven display dict from verifier database.

    Returns:
        tuple[dict | None, list[str]]: (display dict or None on total failure, warnings)
    """
    from .app_config import (
        VERIFIER_FAILED_TASKS_LIMIT,
        VERIFIER_GENERATION_LIMIT,
        VERIFIER_METADATA_TIMEOUT_MS,
    )

    warnings = []
    try:
        current_gen = get_current_generation(db)
    except Exception as e:
        logger.warning("Failed to read current generation: %s", e)
        return None, [f"Could not read verifier generation metadata: {e}"]

    if current_gen is None:
        current_gen = 0

    gen_limit = VERIFIER_GENERATION_LIMIT
    previous_gen = current_gen - 1 if current_gen > 0 else None
    gen_range = list(range(max(0, current_gen - gen_limit + 1), current_gen + 1))
    timeout_secs = VERIFIER_METADATA_TIMEOUT_MS / 1000.0

    section_values, section_warnings, unavailable, attempted = _fetch_metadata_sections(
        db,
        gen_limit=gen_limit,
        current_gen=current_gen,
        gen_range=gen_range,
        previous_gen=previous_gen,
        include_verification_completeness=include_verification_completeness,
        endpoint_configured=endpoint_configured,
        timeout_secs=timeout_secs,
    )
    warnings.extend(section_warnings)

    if attempted and not any(
        _section_succeeded(key, unavailable) for key in attempted
    ):
        return None, warnings

    gen_history = section_values.get("history", [])
    ns_by_generation = section_values.get("ns_batch", {})
    task_summary_current = section_values.get("summary", {})
    collection_mismatch_docs = section_values.get("coll_mm", [])
    failed_tasks = section_values.get("failed", [])

    generations_to_show = []
    for g in gen_history:
        gen_num = g["_id"]
        if gen_num is not None:
            generations_to_show.append({
                "num": gen_num,
                "name": get_generation_name(gen_num),
                "total": g["total_tasks"],
                "completed": g["completed"],
                "failed": g["failed"],
                "pending": g.get("pending", 0),
                "startTime": _format_start_time(g.get("first_task_time", "N/A")),
            })

    generations_to_show.sort(
        key=lambda x: x["num"] if x["num"] is not None else -1,
        reverse=True,
    )

    for g in generations_to_show:
        is_current = g["num"] == current_gen
        g["isCurrent"] = is_current
        g["isFinal"] = is_current
        if is_current:
            g["name"] = f"★ {g['name']} (CURRENT)"

    generations_to_show = generations_to_show[:gen_limit]

    ns_doc_stats = ns_by_generation.get(current_gen, [])
    namespaces = []
    if previous_gen is not None and not endpoint_configured:
        ns_doc_stats_previous = ns_by_generation.get(previous_gen, [])
        namespaces = [
            {
                "name": ns["_id"] or "unknown",
                "docsCompared": ns.get("docsCompared") or 0,
                "totalDocs": ns.get("totalDocs") or 0,
                "partitionsDone": ns.get("partitionsDone") or 0,
                "partitionsPending": (
                    (ns.get("partitionsAdded") or 0)
                    + (ns.get("partitionsProcessing") or 0)
                ),
            }
            for ns in ns_doc_stats_previous
        ]
        namespaces.sort(key=_doc_completion_pct)

    verification_completeness = None
    if include_verification_completeness and not unavailable.get(
        "verificationCompletenessUnavailable",
    ):
        verification_completeness = get_verification_completeness(
            ns_doc_stats, task_summary_current, current_gen,
        )

    current_gen_stats = (
        verification_completeness
        if verification_completeness is not None
        else _sum_namespace_stats_list(ns_doc_stats)
    )

    for g in generations_to_show:
        gen_num = g["num"]
        if gen_num == current_gen:
            if verification_completeness is not None:
                g["docsCompared"] = verification_completeness["documents"]["compared"]
                g["totalDocs"] = verification_completeness["documents"]["total"]
                g["partitionsDone"] = verification_completeness["partitions"]["done"]
                g["partitionsTotal"] = verification_completeness["partitions"]["total"]
            else:
                g["docsCompared"] = current_gen_stats["docsCompared"]
                g["totalDocs"] = current_gen_stats["totalDocs"]
                g["partitionsDone"] = current_gen_stats["partitionsDone"]
                g["partitionsTotal"] = current_gen_stats["partitionsTotal"]
        else:
            g.update(_sum_namespace_stats_list(
                ns_by_generation.get(gen_num, []),
            ))

    collection_mismatches = []
    for mm in collection_mismatch_docs:
        detail = mm.get("detail", {})
        namespace = detail.get("nameSpace") or detail.get("namespace") or "N/A"
        formatted = _format_single_mismatch_detail(detail, _VERIFY_COLLECTION)
        collection_mismatches.append({
            "namespace": namespace,
            "details": formatted or "Mismatch detected (check logs for details)",
        })

    previous_gen_row = next(
        (g for g in generations_to_show if g["num"] == previous_gen),
        None,
    )

    result = {
        "metadataAvailable": True,
        "stateBadge": _derive_state_badge(
            current_gen, previous_gen_row, collection_mismatches,
        ),
        "currentGeneration": current_gen,
        "generationLimit": gen_limit,
        "failedTasksLimit": VERIFIER_FAILED_TASKS_LIMIT,
        "generations": generations_to_show,
        "namespaces": namespaces[:25],
        "previousGeneration": previous_gen,
        "failedTasks": failed_tasks,
        "collectionMismatches": collection_mismatches,
    }
    if verification_completeness is not None:
        result["verificationCompleteness"] = verification_completeness
    if warnings:
        result["metadataPartial"] = True
    if unavailable:
        result.update(unavailable)
    if endpoint_configured:
        result["metadataSkippedSections"] = list(_ENDPOINT_SKIPPED_SECTIONS)
    return result, warnings


def build_verifier_progress_payload(
    endpoint_url, connection_string=None, db_name=None,
):
    """Build JSON slice for verifier /progress polling."""
    from .live_monitoring import ProgressFetchError

    result = {
        "error": None,
        "warnings": [],
        "dataSources": None,
        "display": {},
    }
    progress_status = STATUS_UNAVAILABLE
    try:
        progress_display = _fetch_verifier_progress(endpoint_url)
        result["display"]["verificationProgress"] = progress_display
        result["display"]["stateBadge"] = _derive_state_badge_from_progress(
            progress_display,
        )
        if progress_display and progress_allows_summary_run(progress_display):
            cache = get_verifier_summary_cache()
            result["display"]["summaryCache"] = build_summary_cache_meta(
                cache, eligible=True,
            )
        else:
            clear_verifier_summary_cache()
            result["display"]["summaryCache"] = None
        progress_status = STATUS_ACTIVE
    except ProgressFetchError as e:
        logger.warning("Verifier progress endpoint fetch failed: %s", e)
        result["warnings"].append(f"Verifier progress endpoint is not responding: {e}")
        result["display"]["stateBadge"] = {"label": "NO DATA", "color": "gray"}
    result["dataSources"] = _verifier_data_sources(
        endpoint_url,
        connection_string,
        progress_status=progress_status if endpoint_url else None,
        metadata_status=None,
    )
    return result


def build_verifier_run_summary_response(endpoint_url, progress_display=None):
    """Run migration-verifier /summary manually with cooldown and session cache."""
    from .app_config import VERIFIER_SUMMARY_COOLDOWN_SECS
    from .live_monitoring import ProgressFetchError

    result = {
        "error": None,
        "warnings": [],
        "display": {},
    }

    if progress_display is None:
        try:
            progress_display = _fetch_verifier_progress(endpoint_url)
        except ProgressFetchError as e:
            logger.warning("Verifier progress fetch failed before summary: %s", e)
            result["error"] = f"Verifier progress endpoint is not responding: {e}"
            return result, 502

    if not progress_allows_summary_run(progress_display):
        gen = (progress_display or {}).get("generation", 0)
        if gen == 0:
            msg = (
                "Mismatch summary is not available during initial verification "
                "(generation 0)."
            )
        else:
            msg = (
                "Mismatch summary is only available when Verification Progress "
                "reports document or metadata mismatches."
            )
        result["error"] = msg
        return result, 403

    cache = get_verifier_summary_cache()
    if cache and cache.get("fetched_at"):
        age_secs = max(0, int(time.time() - cache["fetched_at"]))
        remaining = max(0, VERIFIER_SUMMARY_COOLDOWN_SECS - age_secs)
        if remaining > 0:
            summary_cache = build_summary_cache_meta(cache, eligible=True)
            result["display"]["summaryCache"] = summary_cache
            if summary_cache.get("verificationSummary"):
                result["display"]["verificationSummary"] = (
                    summary_cache["verificationSummary"]
                )
            result["error"] = (
                f"Mismatch summary was run recently. Try again in "
                f"{remaining} seconds."
            )
            result["cooldownRemainingSecs"] = remaining
            result["lastFetchedAt"] = cache.get("fetched_at")
            result["lastFetchedAgeSecs"] = age_secs
            return result, 429

    try:
        summary_display = _fetch_verifier_summary(endpoint_url)
    except ProgressFetchError as e:
        logger.warning("Verifier summary endpoint fetch failed: %s", e)
        result["error"] = f"Verifier summary endpoint is not responding: {e}"
        if cache:
            summary_cache = build_summary_cache_meta(cache, eligible=True)
            result["display"]["summaryCache"] = summary_cache
            if summary_cache.get("verificationSummary"):
                result["display"]["verificationSummary"] = (
                    summary_cache["verificationSummary"]
                )
        return result, 502

    set_verifier_summary_cache(summary_display)
    result["display"]["verificationSummary"] = summary_display
    result["display"]["summaryCache"] = build_summary_cache_meta(
        get_verifier_summary_cache(), eligible=True,
    )
    return result, 200


def build_verifier_metadata_payload(
    connection_string, endpoint_url=None, db_name=None,
):
    """Build JSON slice for verifier metadata DB polling."""
    from .app_config import (
        MI_MIGRATION_VERIFIER_DB_NAME,
        get_verifier_metadata_database,
    )

    if db_name is None:
        db_name = MI_MIGRATION_VERIFIER_DB_NAME

    result = {
        "error": None,
        "warnings": [],
        "dataSources": None,
        "display": None,
    }

    try:
        db = get_verifier_metadata_database(connection_string, db_name)
        logger.info("Connected to verifier database: %s", db_name)
    except PyMongoError as e:
        logger.error("Failed to connect to verifier database: %s", e)
        result["error"] = "Could not connect to verifier database."
        result["dataSources"] = _verifier_data_sources(
            endpoint_url,
            connection_string,
            progress_status=None,
            metadata_status=STATUS_UNAVAILABLE,
        )
        return result
    except Exception as e:
        logger.error("Unexpected error connecting to verifier database: %s", e)
        result["error"] = "Could not connect to verifier database."
        result["dataSources"] = _verifier_data_sources(
            endpoint_url,
            connection_string,
            progress_status=None,
            metadata_status=STATUS_UNAVAILABLE,
        )
        return result

    try:
        result["warnings"].extend(collect_verifier_metadata_warnings(db))
    except Exception as e:
        logger.error("Error reading verifier metadata warnings: %s", e)
        result["error"] = "Could not load verifier metadata."
        result["dataSources"] = _verifier_data_sources(
            endpoint_url,
            connection_string,
            progress_status=None,
            metadata_status=STATUS_UNAVAILABLE,
        )
        return result

    display, section_warnings = _build_metadata_display(
        db,
        db_name,
        include_verification_completeness=not bool(endpoint_url),
        endpoint_configured=bool(endpoint_url),
    )
    result["warnings"].extend(section_warnings)
    if display is not None:
        display["metadataAvailable"] = True
        result["display"] = display
        metadata_status = STATUS_ACTIVE
    else:
        result["error"] = "Could not load verifier metadata."
        metadata_status = STATUS_UNAVAILABLE
    result["dataSources"] = _verifier_data_sources(
        endpoint_url,
        connection_string,
        progress_status=None,
        metadata_status=metadata_status,
    )
    return result


def _doc_completion_pct(ns):
    total = ns.get("totalDocs") or 0
    if total == 0:
        return 100.0
    return (ns.get("docsCompared") or 0) / total * 100


def _percent(done, total):
    if total == 0:
        return None
    return round(done / total * 100, 1)


def get_verification_completeness(ns_doc_stats, task_summary, generation):
    """Roll up verification completeness for the current generation."""
    docs_compared = 0
    total_docs = 0
    bytes_compared = 0
    total_bytes = 0
    namespaces_complete = 0
    partitions_done = 0
    partitions_pending = 0

    for ns in ns_doc_stats:
        docs_compared += ns.get("docsCompared") or 0
        total_docs += ns.get("totalDocs") or 0
        bytes_compared += ns.get("bytesCompared") or 0
        total_bytes += ns.get("totalBytes") or 0
        p_done = ns.get("partitionsDone") or 0
        p_pending = (ns.get("partitionsAdded") or 0) + (ns.get("partitionsProcessing") or 0)
        partitions_done += p_done
        partitions_pending += p_pending
        if p_done > 0 and p_pending == 0:
            namespaces_complete += 1

    total_namespaces = len(ns_doc_stats)
    partitions_total = partitions_done + partitions_pending

    task_completed = task_summary.get("completed", 0)
    task_failed = task_summary.get("failed", 0) + task_summary.get("mismatch", 0)
    task_pending = task_summary.get("pending", 0) + task_summary.get("processing", 0)
    task_total = task_completed + task_failed + task_pending

    result = {
        "generation": generation,
        "generationName": get_generation_name(generation),
        "isRecheckGeneration": generation > 0,
        "documents": {
            "compared": docs_compared,
            "total": total_docs,
            "percent": _percent(docs_compared, total_docs),
        },
        "namespaces": {
            "complete": namespaces_complete,
            "total": total_namespaces,
            "percent": _percent(namespaces_complete, total_namespaces),
        },
        "partitions": {
            "done": partitions_done,
            "total": partitions_total,
            "percent": _percent(partitions_done, partitions_total),
        },
        "tasks": {
            "completed": task_completed,
            "failed": task_failed,
            "pending": task_pending,
            "percentDone": _percent(task_completed + task_failed, task_total),
        },
    }

    if generation == 0 and total_bytes > 0:
        result["bytes"] = {
            "compared": bytes_compared,
            "total": total_bytes,
            "percent": _percent(bytes_compared, total_bytes),
        }

    return result


def collect_verifier_metadata_warnings(db):
    """Return user-visible warnings when verifier metadata version does not match MI."""
    from .app_config import VERIFIER_METADATA_VERSION

    doc = _read_generation_doc(db)
    if doc is None:
        return []

    version = _field_from_doc(doc, "metadataVersion", "MetadataVersion")
    if version is None:
        message = (
            "Verifier metadata has no metadataVersion field; this layout may predate "
            "migration-verifier metadata versioning. Dashboard fields may be incomplete "
            "or misinterpreted."
        )
        logger.warning(message)
        return [message]

    if version != VERIFIER_METADATA_VERSION:
        message = (
            f"Verifier metadata version mismatch: database has {version}, but "
            f"Mongosync Insights expects {VERIFIER_METADATA_VERSION}. Upgrade "
            "migration-verifier or update VERIFIER_METADATA_VERSION in app_config.py "
            "when using a newer Mongosync Insights release."
        )
        logger.warning(message)
        return [message]

    return []


def plot_verifier_metrics(db_name=None, nav=None):
    """Render the verifier metrics template."""
    from .app_config import (
        MI_MIGRATION_VERIFIER_DB_NAME,
        REFRESH_TIME,
        VERIFIER_HEAVY_API_TIMEOUT_SECS,
        VERIFIER_METADATA_REFRESH_TIME,
        VERIFIER_PROGRESS_REFRESH_TIME,
        VERIFIER_SUMMARY_COOLDOWN_SECS,
    )

    if db_name is None:
        db_name = MI_MIGRATION_VERIFIER_DB_NAME

    progress_refresh = VERIFIER_PROGRESS_REFRESH_TIME
    metadata_refresh = VERIFIER_METADATA_REFRESH_TIME

    return render_template(
        'verifier_metrics.html',
        nav=nav,
        refresh_time=REFRESH_TIME,
        refresh_time_ms=str(int(progress_refresh) * 1000),
        progress_refresh_sec=progress_refresh,
        metadata_refresh_sec=metadata_refresh,
        progress_refresh_ms=str(int(progress_refresh) * 1000),
        metadata_refresh_ms=str(int(metadata_refresh) * 1000),
        summary_cooldown_sec=VERIFIER_SUMMARY_COOLDOWN_SECS,
        heavy_api_timeout_sec=VERIFIER_HEAVY_API_TIMEOUT_SECS,
        db_name=db_name
    )
