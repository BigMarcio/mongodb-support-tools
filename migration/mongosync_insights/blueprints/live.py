import logging

from flask import (
    Blueprint,
    Response,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    stream_with_context,
    url_for,
)

from lib.connection_validator import sanitize_for_display
from lib.live_monitoring import (
    ProgressFetchError,
    build_live_monitor_payload,
    progress_monitor_no_config_response,
    stream_verifier_mismatches,
)
from lib.migration_verifier import (
    build_verifier_metadata_payload,
    build_verifier_progress_payload,
    build_verifier_run_summary_response,
    plot_verifier_metrics,
)
from lib.session_support import SESSION_COOKIE_NAME, store_session_data
from lib.app_config import (
    CONNECTION_STRING,
    PROGRESS_ENDPOINT_URL,
    REFRESH_TIME,
    SECURE_COOKIES,
    SESSION_TIMEOUT,
    VERIFIER_CONNECTION_STRING,
    VERIFIER_PROGRESS_ENDPOINT_URL,
    VERIFIER_PROGRESS_REFRESH_TIME,
    VERIFIER_SUMMARY_MIN_DURATION_SECS,
    build_progress_endpoint_url,
    build_verifier_doc_mismatches_endpoint_url,
    build_verifier_ns_mismatches_endpoint_url,
    build_verifier_progress_endpoint_url,
    clear_connection_cache,
    probe_metadata_databases,
    session_store,
    validate_connection,
    validate_progress_endpoint_url,
)
from pymongo.errors import InvalidURI, PyMongoError

bp = Blueprint("live", __name__, url_prefix="/live")

logger = logging.getLogger(__name__)

_VERIFIER_ENV_CONFIGURED = bool(
    VERIFIER_PROGRESS_ENDPOINT_URL or VERIFIER_CONNECTION_STRING
)


def _set_session_cookie(response, session_id):
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        httponly=True,
        secure=SECURE_COOKIES,
        samesite="Strict",
        max_age=SESSION_TIMEOUT,
    )
    return response


def _live_session_context():
    """Resolve live monitoring session state from env and cookie session."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = session_store.get_session(session_id) or {}

    endpoint_url = PROGRESS_ENDPOINT_URL or session_data.get("endpoint_url")
    connection_string = CONNECTION_STRING or session_data.get("connection_string")

    embedded_verifier = session_data.get("embedded_verifier", True)
    if _VERIFIER_ENV_CONFIGURED:
        embedded_verifier = False

    verifier_endpoint_url = (
        VERIFIER_PROGRESS_ENDPOINT_URL or session_data.get("verifier_endpoint_url")
    )

    if VERIFIER_CONNECTION_STRING:
        verifier_connection_string = VERIFIER_CONNECTION_STRING
    elif session_data.get("verifier_metadata_available") and connection_string:
        verifier_connection_string = connection_string
    else:
        verifier_connection_string = session_data.get("verifier_connection_string")

    return {
        "endpoint_url": endpoint_url,
        "connection_string": connection_string,
        "embedded_verifier": embedded_verifier,
        "verifier_endpoint_url": verifier_endpoint_url,
        "verifier_connection_string": verifier_connection_string,
        "mongosync_metadata_available": session_data.get(
            "mongosync_metadata_available", False,
        ),
        "verifier_metadata_available": session_data.get(
            "verifier_metadata_available", False,
        ),
    }


def _has_standalone_verifier(ctx):
    """True when standalone Migration Verifier monitoring is configured."""
    return not ctx["embedded_verifier"] and bool(ctx["verifier_endpoint_url"])


def _resolve_monitor_destination(ctx):
    """
    Resolve the primary live monitoring destination from session context.

    Returns "dashboard", "migration", "verifier", or None when unresolved.
    """
    has_progress = bool(ctx["endpoint_url"])
    has_connection = bool(ctx["connection_string"])
    has_verifier = _has_standalone_verifier(ctx)
    mongosync_metadata = ctx.get("mongosync_metadata_available", False)

    if has_progress and has_verifier:
        return "dashboard"
    if has_progress:
        return "migration"
    if has_connection and has_verifier:
        if mongosync_metadata:
            return "migration"
        return "verifier"
    if has_verifier:
        return "verifier"
    if has_connection:
        return "migration"
    return None


def _live_nav_context(ctx, active_page):
    """Build navigation visibility for live monitoring screens."""
    has_progress = bool(ctx["endpoint_url"])
    has_connection = bool(ctx["connection_string"])
    has_verifier = _has_standalone_verifier(ctx)
    mongosync_metadata = ctx.get("mongosync_metadata_available", False)

    if has_progress and not has_verifier:
        return None
    if not has_progress and has_connection and has_verifier and not mongosync_metadata:
        return None
    if not has_progress and not has_connection and has_verifier:
        return None
    if has_connection and not has_progress and not has_verifier:
        return None

    return {
        "active_page": active_page,
        "show_dashboard": has_progress and has_verifier,
        "show_full_migration": has_progress or (has_connection and mongosync_metadata),
        "show_full_verifier": has_verifier,
    }


def _redirect_for_destination(destination):
    """Redirect to the resolved live monitoring destination."""
    if destination == "dashboard":
        return redirect(url_for("live.dashboard"))
    if destination == "migration":
        return redirect(url_for("live.migration_view"))
    if destination == "verifier":
        return redirect(url_for("live.verifier_view"))
    return redirect(url_for("live.live_home"))


def _enforce_monitor_destination(ctx, route):
    """Redirect when the session cannot access the requested live monitoring route."""
    destination = _resolve_monitor_destination(ctx)
    nav = _live_nav_context(ctx, route)
    if route == "dashboard":
        allowed = destination == "dashboard"
    elif route == "migration":
        allowed = destination == "migration" or bool(
            nav and nav.get("show_full_migration")
        )
    elif route == "verifier":
        allowed = destination == "verifier" or bool(
            nav and nav.get("show_full_verifier")
        )
    else:
        allowed = False
    if not allowed:
        return _redirect_for_destination(destination)
    return None


def _resolve_progress_url(*, env_url, host_field, port_field, build_fn, default_port_error):
    if env_url:
        return env_url, None
    progress_host = (request.form.get(host_field) or "").strip()
    progress_port = (request.form.get(port_field) or "").strip()
    if not progress_host:
        return None, None
    try:
        return build_fn(progress_host, progress_port or None), None
    except ValueError as e:
        logger.error("Invalid progress endpoint port: %s", e)
        return None, render_template(
            "error.html",
            error_title="Invalid Progress Endpoint",
            error_message=str(e),
        )


def _resolve_connection_string(*, env_string, form_field):
    if env_string:
        return env_string
    value = request.form.get(form_field)
    if value:
        value = value.strip()
        return value if value else None
    return None


def _validate_connection_or_error(target_mongo_uri):
    if not target_mongo_uri:
        return None
    try:
        validate_connection(target_mongo_uri)
    except InvalidURI as e:
        clear_connection_cache()
        logger.error("Invalid connection string format: %s", e)
        return render_template(
            "error.html",
            error_title="Invalid Connection String",
            error_message=(
                "The connection string format is invalid. Please check your "
                "MongoDB connection string and try again."
            ),
        )
    except PyMongoError as e:
        clear_connection_cache()
        logger.error("Failed to connect: %s", e)
        return render_template(
            "error.html",
            error_title="Connection Failed",
            error_message=(
                "Could not connect to MongoDB. Please verify your credentials, "
                "network connectivity, and that the cluster is accessible."
            ),
        )
    except Exception as e:
        clear_connection_cache()
        logger.error("Unexpected error during connection validation: %s", e)
        return render_template(
            "error.html",
            error_title="Connection Error",
            error_message="An unexpected error occurred. Please try again.",
        )
    return None


def _probe_connection_flags(connection_string):
    try:
        return probe_metadata_databases(connection_string)
    except Exception as e:
        logger.warning("Metadata database probe failed: %s", e)
        return {
            "mongosync_metadata_available": False,
            "verifier_metadata_available": False,
        }


def _progress_monitor_session_context():
    """Resolve progress endpoint URL and metadata connection string for the monitor tab."""
    ctx = _live_session_context()
    return ctx["endpoint_url"], ctx["connection_string"]


def _verifier_session_context():
    """Resolve verifier connection string and progress endpoint from env or session."""
    ctx = _live_session_context()
    return ctx["verifier_connection_string"], ctx["verifier_endpoint_url"]


def _home_template_context():
    connection_string_configured = bool(CONNECTION_STRING)
    connection_string_display = (
        sanitize_for_display(CONNECTION_STRING) if connection_string_configured else ""
    )
    verifier_env_configured = _VERIFIER_ENV_CONFIGURED
    embedded_verifier_default = not verifier_env_configured

    return {
        "progress_endpoint_configured": bool(PROGRESS_ENDPOINT_URL),
        "progress_endpoint_url": PROGRESS_ENDPOINT_URL,
        "connection_string_configured": connection_string_configured,
        "connection_string_display": connection_string_display,
        "verifier_progress_endpoint_configured": bool(VERIFIER_PROGRESS_ENDPOINT_URL),
        "verifier_progress_endpoint_url": VERIFIER_PROGRESS_ENDPOINT_URL,
        "verifier_env_configured": verifier_env_configured,
        "embedded_verifier_default": embedded_verifier_default,
    }


@bp.route("/")
def live_home():
    return render_template("live/home.html", **_home_template_context())


@bp.route("/monitor", methods=["POST"])
def monitor():
    target_mongo_uri = _resolve_connection_string(
        env_string=CONNECTION_STRING,
        form_field="connectionString",
    )

    progress_url, error_response = _resolve_progress_url(
        env_url=PROGRESS_ENDPOINT_URL,
        host_field="progressHost",
        port_field="progressPort",
        build_fn=build_progress_endpoint_url,
        default_port_error=None,
    )
    if error_response:
        return error_response

    embedded_verifier = request.form.get("embeddedVerifier") == "on"
    if _VERIFIER_ENV_CONFIGURED:
        embedded_verifier = False

    verifier_progress_url = None
    if not embedded_verifier:
        verifier_progress_url, error_response = _resolve_progress_url(
            env_url=VERIFIER_PROGRESS_ENDPOINT_URL,
            host_field="verifierProgressHost",
            port_field="verifierProgressPort",
            build_fn=build_verifier_progress_endpoint_url,
            default_port_error=None,
        )
        if error_response:
            return error_response

    has_mongosync_input = bool(target_mongo_uri or progress_url)
    has_verifier_input = bool(verifier_progress_url)

    if not has_mongosync_input and not has_verifier_input:
        logger.error("No connection string, progress endpoint, or verifier endpoint provided")
        return render_template(
            "error.html",
            error_title="No Input Provided",
            error_message=(
                "Please provide at least one of the following: Mongosync progress "
                "endpoint, MongoDB connection string, or Migration Verifier progress "
                "endpoint."
            ),
        )

    if not has_mongosync_input and has_verifier_input and embedded_verifier:
        logger.error("Verifier endpoint provided without disabling embedded verifier")
        return render_template(
            "error.html",
            error_title="Standalone Verifier Required",
            error_message=(
                "To monitor only the Migration Verifier tool, uncheck "
                "\"Using mongosync Embedded Verifier\" and provide the verifier "
                "progress endpoint."
            ),
        )

    if progress_url and not validate_progress_endpoint_url(progress_url):
        logger.error("Invalid progress endpoint URL format: %s", progress_url)
        return render_template(
            "error.html",
            error_title="Invalid Progress Endpoint",
            error_message=(
                "The progress endpoint format is invalid. Enter a host and port "
                "(default 27182); the path /api/v1/progress is fixed."
            ),
        )

    if verifier_progress_url and not validate_progress_endpoint_url(verifier_progress_url):
        logger.error(
            "Invalid verifier progress endpoint URL format: %s", verifier_progress_url,
        )
        return render_template(
            "error.html",
            error_title="Invalid Progress Endpoint",
            error_message=(
                "The progress endpoint format is invalid. Enter a host and port "
                "(default 27020); the path /api/v1/progress is fixed."
            ),
        )

    probe_flags = {
        "mongosync_metadata_available": False,
        "verifier_metadata_available": False,
    }
    if target_mongo_uri:
        error_response = _validate_connection_or_error(target_mongo_uri)
        if error_response:
            return error_response
        probe_flags = _probe_connection_flags(target_mongo_uri)

    session_data = {
        "connection_string": target_mongo_uri,
        "endpoint_url": progress_url,
        "embedded_verifier": embedded_verifier,
        "verifier_endpoint_url": verifier_progress_url,
        **probe_flags,
    }
    session_id = store_session_data(session_data)

    destination = _resolve_monitor_destination(session_data)
    response = make_response(_redirect_for_destination(destination))
    return _set_session_cookie(response, session_id)


@bp.route("/dashboard")
def dashboard():
    ctx = _live_session_context()
    redirect_response = _enforce_monitor_destination(ctx, "dashboard")
    if redirect_response:
        return redirect_response

    migration_section_visible = bool(ctx["endpoint_url"])
    verifier_section_visible = _has_standalone_verifier(ctx)
    nav = _live_nav_context(ctx, "dashboard")
    progress_refresh_sec = VERIFIER_PROGRESS_REFRESH_TIME

    return render_template(
        "live/dashboard.html",
        nav=nav,
        migration_section_visible=migration_section_visible,
        verifier_section_visible=verifier_section_visible,
        refresh_time=REFRESH_TIME,
        refresh_time_ms=str(int(REFRESH_TIME) * 1000),
        progress_refresh_sec=progress_refresh_sec,
        progress_refresh_ms=str(int(progress_refresh_sec) * 1000),
        show_full_verifier_link=bool(nav and nav["show_full_verifier"]),
    )


@bp.route("/migration")
def migration_view():
    ctx = _live_session_context()
    redirect_response = _enforce_monitor_destination(ctx, "migration")
    if redirect_response:
        return redirect_response

    nav = _live_nav_context(ctx, "migration")
    response = make_response(
        render_template(
            "metrics.html",
            nav=nav,
            refresh_time=REFRESH_TIME,
            refresh_time_ms=str(int(REFRESH_TIME) * 1000),
        )
    )
    return response


@bp.route("/verifier/view")
def verifier_view():
    ctx = _live_session_context()
    redirect_response = _enforce_monitor_destination(ctx, "verifier")
    if redirect_response:
        return redirect_response

    nav = _live_nav_context(ctx, "verifier")
    return plot_verifier_metrics(nav=nav)


@bp.route("/live_monitoring", methods=["POST"])
def live_monitoring():
    """Legacy route — delegates to unified monitor handler."""
    return monitor()


@bp.route("/get_dashboard_migration", methods=["POST"])
def get_dashboard_migration():
    endpoint_url, connection_string = _progress_monitor_session_context()
    if not endpoint_url:
        return jsonify(
            {"error": "No progress endpoint URL is configured for dashboard monitoring."}
        ), 400
    return jsonify(
        build_live_monitor_payload(
            endpoint_url,
            connection_string,
            progress_only=True,
        ),
    )


@bp.route("/get_progress_monitor", methods=["POST"])
def get_progress_monitor():
    endpoint_url, connection_string = _progress_monitor_session_context()

    if not endpoint_url and not connection_string:
        return jsonify(progress_monitor_no_config_response(connection_string=connection_string))

    return jsonify(build_live_monitor_payload(endpoint_url, connection_string))


@bp.route("/verifier", methods=["POST"])
def verifier():
    """Legacy verifier form submit — merge session and redirect to full verifier view."""
    if VERIFIER_PROGRESS_ENDPOINT_URL:
        verifier_progress_url = VERIFIER_PROGRESS_ENDPOINT_URL
    else:
        progress_host = (request.form.get("verifierProgressHost") or "").strip()
        progress_port = (request.form.get("verifierProgressPort") or "").strip()
        try:
            verifier_progress_url = build_verifier_progress_endpoint_url(
                progress_host, progress_port or None,
            ) if progress_host else None
        except ValueError as e:
            logger.error("Invalid verifier progress endpoint port: %s", e)
            return render_template(
                "error.html",
                error_title="Invalid Progress Endpoint",
                error_message=str(e),
            )

    if VERIFIER_CONNECTION_STRING:
        target_mongo_uri = VERIFIER_CONNECTION_STRING
    else:
        target_mongo_uri = request.form.get("verifierConnectionString")
        if target_mongo_uri:
            target_mongo_uri = target_mongo_uri.strip() if target_mongo_uri.strip() else None

    if not target_mongo_uri and not verifier_progress_url:
        logger.error("No connection string or verifier progress endpoint URL provided")
        return render_template(
            "error.html",
            error_title="No Input Provided",
            error_message=(
                "Please provide at least one of the following: Migration Verifier "
                "progress endpoint or MongoDB connection string (or both)."
            ),
        )

    if verifier_progress_url and not validate_progress_endpoint_url(verifier_progress_url):
        logger.error("Invalid verifier progress endpoint URL format: %s", verifier_progress_url)
        return render_template(
            "error.html",
            error_title="Invalid Progress Endpoint",
            error_message=(
                "The progress endpoint format is invalid. Enter a host and port "
                "(default 27020); the path /api/v1/progress is fixed."
            ),
        )

    probe_flags = {
        "mongosync_metadata_available": False,
        "verifier_metadata_available": False,
    }
    if target_mongo_uri:
        error_response = _validate_connection_or_error(target_mongo_uri)
        if error_response:
            return error_response
        probe_flags = _probe_connection_flags(target_mongo_uri)

    session_data = {
        "embedded_verifier": False,
        "verifier_endpoint_url": verifier_progress_url,
        "connection_string": target_mongo_uri,
        **probe_flags,
    }
    if probe_flags["verifier_metadata_available"]:
        session_data["verifier_connection_string"] = target_mongo_uri

    session_id = store_session_data(session_data)
    response = make_response(redirect(url_for("live.verifier_view")))
    return _set_session_cookie(response, session_id)


def _verifier_no_config_response():
    logger.error("No connection string or verifier progress endpoint available")
    return jsonify(
        {
            "error": (
                "No progress endpoint or connection string is configured. "
                "Return to Migration monitoring home and provide a Migration Verifier "
                "progress endpoint or MongoDB connection string."
            )
        }
    ), 400


@bp.route("/get_verifier_progress", methods=["POST"])
def get_verifier_progress():
    connection_string, endpoint_url = _verifier_session_context()
    if not endpoint_url:
        return jsonify({"error": "No verifier progress endpoint is configured."}), 400
    return jsonify(
        build_verifier_progress_payload(endpoint_url, connection_string),
    )


@bp.route("/run_verifier_summary", methods=["POST"])
def run_verifier_summary():
    _connection_string, endpoint_url = _verifier_session_context()
    if not endpoint_url:
        return jsonify({"error": "No verifier progress endpoint is configured."}), 400
    payload, status = build_verifier_run_summary_response(endpoint_url)
    return jsonify(payload), status


@bp.route("/get_verifier_metadata", methods=["POST"])
def get_verifier_metadata():
    connection_string, endpoint_url = _verifier_session_context()
    if not connection_string:
        return jsonify({"error": "No verifier connection string is configured."}), 400
    return jsonify(
        build_verifier_metadata_payload(
            connection_string, endpoint_url=endpoint_url,
        ),
    )


def _stream_verifier_mismatch_download(
    *,
    build_url,
    filename,
    label,
    params=None,
):
    """Proxy migration-verifier mismatch NDJSON stream as a file download."""
    _connection_string, progress_endpoint_url = _verifier_session_context()
    if not progress_endpoint_url:
        return jsonify({"error": "No verifier progress endpoint is configured."}), 400

    target_url = build_url(progress_endpoint_url)
    if not target_url:
        return jsonify({"error": "Invalid verifier progress endpoint URL."}), 400

    try:
        upstream = stream_verifier_mismatches(
            target_url, params=params, label=label,
        )
    except ProgressFetchError as e:
        logger.warning("Verifier %s download failed: %s", label, e)
        return jsonify({"error": str(e)}), 502

    def generate():
        try:
            for chunk in upstream.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-cache",
    }
    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers=headers,
    )


@bp.route("/download_verifier_doc_mismatches", methods=["GET"])
def download_verifier_doc_mismatches():
    params = {}
    if VERIFIER_SUMMARY_MIN_DURATION_SECS > 0:
        params["minDurationSecs"] = VERIFIER_SUMMARY_MIN_DURATION_SECS
    return _stream_verifier_mismatch_download(
        build_url=build_verifier_doc_mismatches_endpoint_url,
        filename="doc-mismatches.ndjson",
        label="document mismatches",
        params=params or None,
    )


@bp.route("/download_verifier_ns_mismatches", methods=["GET"])
def download_verifier_ns_mismatches():
    return _stream_verifier_mismatch_download(
        build_url=build_verifier_ns_mismatches_endpoint_url,
        filename="ns-mismatches.ndjson",
        label="namespace mismatches",
    )
