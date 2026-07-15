import logging
import os

from flask import Blueprint, jsonify, render_template, request

from lib.log_time import InvalidLogTimestamp, normalize_search_end, normalize_search_start
from lib.logs_metrics import upload_file
from lib.log_store_registry import log_store_registry
from lib.snapshot_store import (
    load_snapshot,
    list_snapshots as get_snapshot_list,
    delete_snapshot as remove_snapshot,
    logstore_path,
)
from lib.store_paths import is_valid_store_id

bp = Blueprint("logs", __name__, url_prefix="/logs")

logger = logging.getLogger(__name__)

_SNAPSHOT_NOT_FOUND_KWARGS = {
    "error_title": "Snapshot Not Found",
    "error_message": (
        "The requested analysis snapshot was not found or has expired. "
        "Please upload and parse the log file again."
    ),
}


@bp.route("/")
def logs_home():
    from lib.app_config import (
        MAX_FILE_SIZE,
    )

    max_file_size_gb = MAX_FILE_SIZE / (1024**3)
    return render_template("logs/home.html", max_file_size_gb=max_file_size_gb)


@bp.route("/uploadLogs", methods=["POST"])
def upload_logs():
    return upload_file()


@bp.route("/search_logs")
def search_logs():
    store_id = request.args.get("store_id", "").strip()
    if not store_id:
        return jsonify({"error": "Missing store_id parameter"}), 400
    if not is_valid_store_id(store_id):
        return jsonify({"error": "Invalid store_id parameter"}), 400

    q = request.args.get("q", "").strip()
    level = request.args.get("level", "").strip()
    start_raw = request.args.get("start", "").strip()
    end_raw = request.args.get("end", "").strip()
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (ValueError, TypeError):
        page = 1
    try:
        per_page = min(max(1, int(request.args.get("per_page", 50))), 200)
    except (ValueError, TypeError):
        per_page = 50

    store = log_store_registry.open_store(store_id)
    if store is None:
        return jsonify({"error": "Log store not found or expired"}), 404

    try:
        query = {}
        if level:
            query["level"] = level
        if q:
            query["$text"] = q

        start_bound = None
        end_bound = None
        if start_raw:
            start_bound = normalize_search_start(start_raw)
            query["timestamp_gte"] = start_bound
        if end_raw:
            end_bound = normalize_search_end(end_raw)
            query["timestamp_lte"] = end_bound
        if start_bound and end_bound and start_bound > end_bound:
            return jsonify({"error": "start must be before or equal to end"}), 400

        result = store.find(query, skip=(page - 1) * per_page, limit=per_page)
        result["page"] = page
        result["per_page"] = per_page
        return jsonify(result)
    except InvalidLogTimestamp as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("Log search error: %s", e)
        return jsonify({"error": "Search failed", "detail": str(e)}), 500


@bp.route("/list_snapshots")
def list_snapshots():
    try:
        snapshots = get_snapshot_list()
        return jsonify(snapshots)
    except Exception as e:
        logger.error("Error listing snapshots: %s", e)
        return jsonify([])


@bp.route("/load_snapshot/<snapshot_id>")
def load_snapshot_view(snapshot_id):
    if not is_valid_store_id(snapshot_id):
        return render_template("error.html", **_SNAPSHOT_NOT_FOUND_KWARGS)

    data = load_snapshot(snapshot_id)
    if data is None:
        return render_template("error.html", **_SNAPSHOT_NOT_FOUND_KWARGS)

    store_id = data.get("log_store_id", "")
    if store_id and is_valid_store_id(store_id):
        try:
            db_path = logstore_path(store_id)
        except ValueError:
            db_path = None
        if db_path and os.path.exists(db_path):
            log_store_registry.register(store_id, db_path)

    template_data = data.get("template_data", {})
    return render_template("upload_results.html", **template_data)


@bp.route("/delete_snapshot/<snapshot_id>", methods=["DELETE"])
def delete_snapshot_view(snapshot_id):
    if not is_valid_store_id(snapshot_id):
        return jsonify({"error": "Snapshot not found"}), 404

    deleted, store_id = remove_snapshot(snapshot_id)
    if store_id:
        log_store_registry.remove(store_id)
    if deleted:
        return jsonify({"status": "ok"})
    return jsonify({"error": "Snapshot not found"}), 404
