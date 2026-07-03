"""
Migration Verifier metrics gathering and visualization.
Provides monitoring for MongoDB migration-verifier tool.
https://github.com/mongodb-labs/migration-verifier
"""
import logging

from flask import render_template
from pymongo.errors import PyMongoError

from .connection_validator import sanitize_for_display

logger = logging.getLogger(__name__)

_EXCLUDED_TASK_TYPES = ["primary"]
_TASK_STATUSES_PENDING = ["added", "processing"]
_TASK_STATUSES_FAILED = ["failed", "mismatch"]
_VERIFY_DOCUMENTS = "verifyDocuments"
_VERIFY_COLLECTION = "verifyCollection"


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


def get_failed_tasks(db, generation, limit=50):
    """Get failed verification tasks with details including mismatch info."""
    pipeline = [
        {"$match": {
            **_base_task_match(generation),
            "status": {"$in": _TASK_STATUSES_FAILED},
        }},
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


def _namespace_stats_pipeline(generation):
    """Port of migration-verifier GetPersistedNamespaceStatistics aggregation."""
    finished_statuses = ["completed", "failed"]
    return [
        {"$match": {
            "type": {"$in": [_VERIFY_DOCUMENTS, _VERIFY_COLLECTION]},
            "generation": generation,
        }},
        {"$addFields": {
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
        }},
        {"$group": {
            "_id": "$query_filter.namespace",
            "docsCompared": {"$sum": "$docsCompared"},
            "totalDocs": {"$sum": "$totalDocs"},
            "bytesCompared": {"$sum": "$bytesCompared"},
            "totalBytes": {"$sum": "$totalBytes"},
            "partitionsAdded": {"$sum": "$partitionAdded"},
            "partitionsProcessing": {"$sum": "$partitionProcessing"},
            "partitionsDone": {"$sum": "$partitionDone"},
        }},
        {"$sort": {"_id": 1}},
    ]


def get_persisted_namespace_statistics(db, generation):
    """Document-level namespace progress (aligned with migration-verifier /progress)."""
    try:
        return list(db.verification_tasks.aggregate(
            _namespace_stats_pipeline(generation),
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


def get_generation_progress_stats(db, generation):
    """Sum namespace document and partition stats for a generation."""
    ns_stats = get_persisted_namespace_statistics(db, generation)
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


def get_generation_history(db, limit=4):
    """Get history of latest generations with their stats."""
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


def get_failed_tasks_for_display(db, generation, limit=50):
    """Collect failed/mismatch tasks for one generation (excludes collection metadata)."""
    rows = []
    tasks = get_failed_tasks(db, generation, limit=limit)
    for task in tasks:
        if task.get("type") == _VERIFY_COLLECTION:
            continue
        rows.append(_serialize_failed_task(task, generation))
    rows.sort(key=lambda r: r.get("beginTime") or "", reverse=True)
    return rows


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


def _build_connectivity(connection_string, db_name):
    return {
        "title": "Connectivity",
        "rows": [
            {"label": "Verifier database", "value": db_name},
            {"label": "Connection string", "value": sanitize_for_display(connection_string)},
        ],
    }


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


def build_verifier_monitor_payload(connection_string, db_name=None):
    """Build JSON payload for the Migration Verifier monitoring dashboard."""
    from .app_config import MI_MIGRATION_VERIFIER_DB_NAME, VERIFIER_GENERATION_LIMIT, get_database

    if db_name is None:
        db_name = MI_MIGRATION_VERIFIER_DB_NAME

    base = {
        "error": None,
        "warnings": [],
        "connectivity": _build_connectivity(connection_string, db_name),
        "display": None,
    }

    try:
        db = get_database(connection_string, db_name)
        logger.info("Connected to verifier database: %s", db_name)
    except PyMongoError as e:
        logger.error("Failed to connect to verifier database: %s", e)
        return {
            "error": str(e),
            "warnings": [],
            "connectivity": base["connectivity"],
            "display": None,
        }
    except Exception as e:
        logger.error("Unexpected error connecting to verifier database: %s", e)
        return {
            "error": f"Connection error: {str(e)}",
            "warnings": [],
            "connectivity": base["connectivity"],
            "display": None,
        }

    try:
        base["warnings"] = collect_verifier_metadata_warnings(db)

        gen_limit = VERIFIER_GENERATION_LIMIT
        current_gen = get_current_generation(db)
        if current_gen is None:
            current_gen = 0

        gen_history = get_generation_history(db, limit=gen_limit)

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
            g["isFinal"] = is_current  # backward compat for older clients
            if is_current:
                g["name"] = f"★ {g['name']} (CURRENT)"

        generations_to_show = generations_to_show[:gen_limit]

        previous_gen = current_gen - 1 if current_gen > 0 else None

        ns_doc_stats = get_persisted_namespace_statistics(db, current_gen)
        namespaces = []
        if previous_gen is not None:
            ns_doc_stats_previous = get_persisted_namespace_statistics(db, previous_gen)
            namespaces = [
                {
                    "name": ns["_id"] or "unknown",
                    "docsCompared": ns.get("docsCompared") or 0,
                    "totalDocs": ns.get("totalDocs") or 0,
                    "partitionsDone": ns.get("partitionsDone") or 0,
                    "partitionsPending": (ns.get("partitionsAdded") or 0) + (ns.get("partitionsProcessing") or 0),
                }
                for ns in ns_doc_stats_previous
            ]
            namespaces.sort(key=_doc_completion_pct)

        task_summary_current = get_verification_summary(db, current_gen)
        verification_completeness = get_verification_completeness(
            ns_doc_stats, task_summary_current, current_gen,
        )

        for g in generations_to_show:
            gen_num = g["num"]
            if gen_num == current_gen:
                g["docsCompared"] = verification_completeness["documents"]["compared"]
                g["totalDocs"] = verification_completeness["documents"]["total"]
                g["partitionsDone"] = verification_completeness["partitions"]["done"]
                g["partitionsTotal"] = verification_completeness["partitions"]["total"]
            else:
                g.update(get_generation_progress_stats(db, gen_num))

        collection_mismatches = []
        if previous_gen is not None:
            collection_mismatch_docs = get_collection_metadata_mismatches(db, previous_gen)
            for mm in collection_mismatch_docs:
                detail = mm.get("detail", {})
                namespace = detail.get("nameSpace") or detail.get("namespace") or "N/A"
                formatted = _format_single_mismatch_detail(detail, _VERIFY_COLLECTION)
                collection_mismatches.append({
                    "namespace": namespace,
                    "details": formatted or "Mismatch detected (check logs for details)",
                })

        if previous_gen is not None:
            failed_tasks = get_failed_tasks_for_display(db, previous_gen)
        else:
            failed_tasks = []

        previous_gen_row = next(
            (g for g in generations_to_show if g["num"] == previous_gen),
            None,
        )

        base["display"] = {
            "stateBadge": _derive_state_badge(
                current_gen, previous_gen_row, collection_mismatches,
            ),
            "currentGeneration": current_gen,
            "generationLimit": gen_limit,
            "verificationCompleteness": verification_completeness,
            "generations": generations_to_show,
            "namespaces": namespaces[:25],
            "previousGeneration": previous_gen,
            "failedTasks": failed_tasks,
            "collectionMismatches": collection_mismatches,
        }
        return base

    except Exception as e:
        logger.error("Error gathering verifier metrics: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return {
            "error": f"Error gathering metrics: {str(e)}",
            "warnings": base.get("warnings", []),
            "connectivity": base["connectivity"],
            "display": None,
        }


def plot_verifier_metrics(db_name=None):
    """Render the verifier metrics template."""
    from .app_config import MI_MIGRATION_VERIFIER_DB_NAME, REFRESH_TIME

    if db_name is None:
        db_name = MI_MIGRATION_VERIFIER_DB_NAME

    refresh_time = REFRESH_TIME
    refresh_time_ms = str(int(refresh_time) * 1000)

    return render_template(
        'verifier_metrics.html',
        refresh_time=refresh_time,
        refresh_time_ms=refresh_time_ms,
        db_name=db_name
    )
