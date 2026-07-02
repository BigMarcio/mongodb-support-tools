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


def get_verification_summary(db, generation):
    """Get summary of verification tasks for a generation."""
    pipeline = [
        {"$match": {"generation": generation}},
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1}
        }}
    ]
    results = list(db.verification_tasks.aggregate(pipeline))
    summary = {
        "completed": 0,
        "failed": 0,
        "mismatch": 0,
        "pending": 0,
        "processing": 0
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
        {"$match": {"generation": generation, "status": {"$in": ["failed", "mismatch"]}}},
        {"$sort": {"begin_time": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 1,
            "type": 1,
            "status": 1,
            "query_filter": 1,
            "_ids": 1,
            "failed_docs": 1,
            "begin_time": 1
        }}
    ]
    tasks = list(db.verification_tasks.aggregate(pipeline))

    if tasks:
        task_ids = [t["_id"] for t in tasks[:20]]
        try:
            mismatches = list(db.mismatches.find(
                {"task": {"$in": task_ids}},
                {"task": 1, "detail": 1}
            ).limit(20))

            mismatch_map = {m["task"]: m for m in mismatches}

            for t in tasks:
                if t["_id"] in mismatch_map:
                    t["mismatch"] = mismatch_map[t["_id"]]
        except Exception:
            pass

    return tasks


def get_namespace_stats(db, generation):
    """Get statistics grouped by namespace for the specified generation."""
    pipeline = [
        {"$match": {"generation": generation}},
        {"$group": {
            "_id": "$query_filter.namespace",
            "total": {"$sum": 1},
            "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
            "failed": {"$sum": {"$cond": [{"$in": ["$status", ["failed", "mismatch"]]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$in": ["$status", ["added", "pending"]]}, 1, 0]}}
        }},
        {"$sort": {"failed": -1, "total": -1}},
        {"$limit": 15}
    ]
    return list(db.verification_tasks.aggregate(pipeline, allowDiskUse=True))


def get_generation_history(db, limit=4):
    """Get history of latest generations with their stats - optimized for performance."""
    latest_gen_doc = db.verification_tasks.find_one(
        {},
        {"generation": 1},
        sort=[("generation", -1)]
    )

    if not latest_gen_doc:
        return []

    latest_gen = latest_gen_doc.get("generation", 0)
    gen_range = list(range(max(0, latest_gen - limit + 1), latest_gen + 1))

    pipeline = [
        {"$match": {"generation": {"$in": gen_range}}},
        {"$group": {
            "_id": "$generation",
            "total_tasks": {"$sum": 1},
            "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
            "failed": {"$sum": {"$cond": [{"$in": ["$status", ["failed", "mismatch"]]}, 1, 0]}},
            "first_task_time": {"$min": "$begin_time"}
        }},
        {"$sort": {"_id": -1}},
        {"$limit": limit}
    ]
    return list(db.verification_tasks.aggregate(pipeline, allowDiskUse=True))


def get_generation_name(gen_num):
    """Get human-readable name for a generation."""
    if gen_num is None:
        return "Unknown"
    elif gen_num == 0:
        return "Initial Verification"
    else:
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


def _get_mismatch_details(task):
    task_type = task.get("type", "")

    if task_type == "verifyCollection":
        mismatch = task.get("mismatch", {})
        if mismatch and isinstance(mismatch, dict):
            detail = mismatch.get("detail", {})
            if detail:
                idx_id = detail.get("id", "?")
                field_type = detail.get("field", "")
                details_str = detail.get("details", "")
                cluster = detail.get("cluster", "")

                if "unique" in details_str.lower():
                    if "src:" in details_str and "unique\": true" in details_str:
                        if "dst:" in details_str and "unique" not in details_str.split("dst:")[1][:50]:
                            return f"Index '{idx_id}' ({field_type}): unique constraint missing on {cluster}"
                    return f"Index '{idx_id}' ({field_type}): Mismatch on {cluster}"
                elif "Missing" in details_str:
                    return f"Index '{idx_id}' ({field_type}): Missing on {cluster}"
                else:
                    return f"Index '{idx_id}' ({field_type}): {details_str[:60]}... ({cluster})"

        failed_docs = task.get("failed_docs", [])
        if failed_docs:
            details = []
            for fd in failed_docs[:3]:
                idx_id = fd.get("id", "?")
                idx_details = fd.get("details", "")
                cluster = fd.get("cluster", "")
                if idx_details:
                    details.append(f"{idx_id}: {idx_details[:30]} ({cluster})")
                else:
                    details.append(f"{idx_id} ({cluster})")
            result = "; ".join(details)
            if len(failed_docs) > 3:
                result += f"... +{len(failed_docs)-3} more"
            return result
        return "Metadata mismatch (no details)"

    elif task_type in ["verify", "verifyDocuments"]:
        mismatch = task.get("mismatch", {})
        if mismatch and isinstance(mismatch, dict):
            detail = mismatch.get("detail", {})
            if detail:
                doc_id = detail.get("id", "?")
                field = detail.get("field", "")
                details_str = detail.get("details", "")
                cluster = detail.get("cluster", "")
                if field:
                    return f"Doc '{doc_id}', field '{field}': {details_str[:40]}... ({cluster})"
                return f"Doc '{doc_id}': {details_str[:50]}... ({cluster})"

        ids = task.get("_ids", [])
        if ids:
            count = len(ids)
            sample = ", ".join([str(i)[:20] for i in ids[:5]])
            if count > 5:
                return f"{count} docs mismatched: {sample}..."
            return f"{count} docs: {sample}"
        return "Document mismatch (no details)"

    mismatch = task.get("mismatch", {})
    if mismatch and isinstance(mismatch, dict):
        detail = mismatch.get("detail", {})
        if detail:
            return f"{detail.get('id', '?')}: {detail.get('details', 'N/A')[:50]}..."
    if task.get("_ids"):
        return f"{len(task.get('_ids', []))} items"
    elif task.get("failed_docs"):
        return f"{len(task.get('failed_docs', []))} issues"
    return task.get("status", "N/A")


def _get_collection_mismatch_detail(cm, mismatch_details):
    task_id = cm.get("_id")
    if task_id in mismatch_details:
        detail = mismatch_details[task_id]
        idx_name = detail.get("id", "?")
        field_type = detail.get("field", "")
        details_str = detail.get("details", "")
        cluster = detail.get("cluster", "")

        if "unique" in details_str.lower():
            if "unique\": true" in details_str and "unique" not in details_str.split("dst:")[1] if "dst:" in details_str else False:
                return f"Index '{idx_name}': unique constraint missing on {cluster}"
            return f"Index '{idx_name}' ({field_type}): property mismatch - {cluster}"
        elif "Missing" in details_str:
            return f"Index '{idx_name}': MISSING on {cluster}"
        else:
            short_details = details_str[:80] + "..." if len(details_str) > 80 else details_str
            return f"Index '{idx_name}': {short_details}"

    failed_docs = cm.get("failed_docs", [])
    if failed_docs:
        details = [f"{fd.get('id', 'N/A')}: {fd.get('details', 'N/A')}"[:40] for fd in failed_docs[:2]]
        return "; ".join(details)
    return "Mismatch detected (check logs for details)"


def _derive_state_badge(final_gen):
    if not final_gen:
        return {"label": "NO DATA", "color": "gray"}
    failed = final_gen.get("failed", 0)
    completed = final_gen.get("completed", 0)
    total = final_gen.get("total", 0)
    if failed > 0:
        return {"label": "FAILED", "color": "red"}
    if completed == total and total > 0:
        return {"label": "PASSED", "color": "green"}
    return {"label": "IN PROGRESS", "color": "blue"}


def _build_connectivity(connection_string, db_name):
    return {
        "title": "Connectivity",
        "rows": [
            {"label": "Verifier database", "value": db_name},
            {"label": "Connection string", "value": sanitize_for_display(connection_string)},
        ],
    }


def build_verifier_monitor_payload(connection_string, db_name=None):
    """Build JSON payload for the Migration Verifier monitoring dashboard."""
    from .app_config import MI_MIGRATION_VERIFIER_DB_NAME, VERIFIER_GENERATION_LIMIT, get_database

    if db_name is None:
        db_name = MI_MIGRATION_VERIFIER_DB_NAME

    base = {
        "error": None,
        "connectivity": _build_connectivity(connection_string, db_name),
        "display": None,
    }

    try:
        db = get_database(connection_string, db_name)
        logger.info("Connected to verifier database: %s", db_name)
    except PyMongoError as e:
        logger.error("Failed to connect to verifier database: %s", e)
        return {"error": str(e), "connectivity": base["connectivity"], "display": None}
    except Exception as e:
        logger.error("Unexpected error connecting to verifier database: %s", e)
        return {"error": f"Connection error: {str(e)}", "connectivity": base["connectivity"], "display": None}

    try:
        gen_limit = VERIFIER_GENERATION_LIMIT
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
                    "startTime": _format_start_time(g.get("first_task_time", "N/A")),
                })

        generations_to_show.sort(
            key=lambda x: x["num"] if x["num"] is not None else -1,
            reverse=True,
        )

        last_gen_num = max([g["num"] for g in generations_to_show]) if generations_to_show else 0

        for g in generations_to_show:
            g["isFinal"] = g["num"] == last_gen_num
            if g["isFinal"]:
                g["name"] = f"★ {g['name']} (FINAL)"

        generations_to_show = generations_to_show[:gen_limit]

        final_gen = next((g for g in generations_to_show if g["isFinal"]), None)

        try:
            all_ns_pipeline = [
                {"$group": {
                    "_id": "$query_filter.namespace",
                    "total": {"$sum": 1},
                    "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
                    "failed": {"$sum": {"$cond": [{"$in": ["$status", ["failed", "mismatch"]]}, 1, 0]}},
                    "pending": {"$sum": {"$cond": [{"$in": ["$status", ["added", "pending", "processing"]]}, 1, 0]}}
                }},
                {"$sort": {"failed": -1, "total": -1}},
                {"$limit": 25}
            ]
            namespace_stats = list(db.verification_tasks.aggregate(all_ns_pipeline, maxTimeMS=60000))
        except Exception as e:
            logger.warning("Failed to get all-time namespace stats: %s", e)
            namespace_stats = get_namespace_stats(db, last_gen_num)

        all_collection_mismatches = list(db.verification_tasks.find({
            "generation": last_gen_num,
            "status": "mismatch",
            "type": "verifyCollection"
        }).limit(100))

        generation_details = []
        for gen in generations_to_show:
            gen_num = gen["num"]
            gen_summary = get_verification_summary(db, gen_num)
            completed = gen_summary.get("completed", 0)
            failed = gen_summary.get("failed", 0) + gen_summary.get("mismatch", 0)
            pending = gen_summary.get("pending", 0) + gen_summary.get("processing", 0)
            total = completed + failed + pending
            percent_complete = (completed / total * 100) if total > 0 else 0

            gen_failed_tasks = get_failed_tasks(db, gen_num, limit=100)
            failed_rows = [
                {
                    "type": t.get("type", "N/A"),
                    "sourceNs": _get_source_ns(t),
                    "destNs": _get_dest_ns(t),
                    "details": _get_mismatch_details(t),
                }
                for t in gen_failed_tasks
            ]

            generation_details.append({
                "num": gen_num,
                "name": gen["name"],
                "isFinal": gen["isFinal"],
                "completed": completed,
                "failed": failed,
                "pending": pending,
                "total": total,
                "percentComplete": round(percent_complete, 1),
                "failedTasks": failed_rows,
            })

        namespaces = [
            {
                "name": ns["_id"] or "unknown",
                "completed": ns["completed"],
                "failed": ns["failed"],
                "pending": ns["pending"],
                "total": ns["total"],
            }
            for ns in (namespace_stats or [])[:25]
        ]

        collection_mismatches = []
        if all_collection_mismatches:
            task_ids = [cm["_id"] for cm in all_collection_mismatches[:50]]
            mismatch_details = {}
            try:
                mismatches = list(db.mismatches.find(
                    {"task": {"$in": task_ids}},
                    {"task": 1, "detail": 1}
                ))
                for m in mismatches:
                    mismatch_details[m["task"]] = m.get("detail", {})
            except Exception as e:
                logger.warning("Failed to fetch mismatch details: %s", e)

            for cm in all_collection_mismatches[:100]:
                qf = cm.get("query_filter", {})
                collection_mismatches.append({
                    "generation": get_generation_name(cm.get("generation")),
                    "namespace": qf.get("namespace", "N/A"),
                    "details": _get_collection_mismatch_detail(cm, mismatch_details),
                })

        base["display"] = {
            "stateBadge": _derive_state_badge(final_gen),
            "generations": generations_to_show,
            "generationDetails": generation_details,
            "namespaces": namespaces,
            "collectionMismatches": collection_mismatches,
            "finalGenerationName": get_generation_name(last_gen_num),
        }
        return base

    except Exception as e:
        logger.error("Error gathering verifier metrics: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return {
            "error": f"Error gathering metrics: {str(e)}",
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
