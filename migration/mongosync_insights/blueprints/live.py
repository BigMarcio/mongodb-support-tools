import logging

from flask import Blueprint, jsonify, make_response, render_template, request

from lib.connection_validator import sanitize_for_display
from lib.live_monitoring import (
    build_live_monitor_payload,
    progress_monitor_no_config_response,
)
from lib.migration_verifier import build_verifier_monitor_payload, plot_verifier_metrics
from lib.session_support import SESSION_COOKIE_NAME, store_session_data
from lib.app_config import (
    CONNECTION_STRING,
    PROGRESS_ENDPOINT_URL,
    REFRESH_TIME,
    SECURE_COOKIES,
    SESSION_TIMEOUT,
    VERIFIER_CONNECTION_STRING,
    VERIFIER_PROGRESS_ENDPOINT_URL,
    build_progress_endpoint_url,
    build_verifier_progress_endpoint_url,
    clear_connection_cache,
    session_store,
    validate_connection,
    validate_progress_endpoint_url,
)
from pymongo.errors import InvalidURI, PyMongoError

bp = Blueprint("live", __name__, url_prefix="/live")

logger = logging.getLogger(__name__)


@bp.route("/")
def live_home():
    if not CONNECTION_STRING:
        connection_string_form = """<label for="connectionString">Destination MongoDB connection string (optional):</label>  
                                    <input type="text" id="connectionString" name="connectionString" size="47" autocomplete="off"
                                        placeholder="mongodb+srv://usr:pwd@cluster0.mongodb.net/"><br><br>"""
    else:
        sanitized_connection = sanitize_for_display(CONNECTION_STRING)
        connection_string_form = f"<p><b>Destination cluster (metadata fallback): </b>{sanitized_connection}</p>"

    progress_endpoint_configured = bool(PROGRESS_ENDPOINT_URL)
    verifier_progress_endpoint_configured = bool(VERIFIER_PROGRESS_ENDPOINT_URL)
    verifier_connection_string_configured = bool(VERIFIER_CONNECTION_STRING)
    verifier_connection_string_display = (
        sanitize_for_display(VERIFIER_CONNECTION_STRING)
        if verifier_connection_string_configured
        else ""
    )

    return render_template(
        "live/home.html",
        connection_string_form=connection_string_form,
        progress_endpoint_configured=progress_endpoint_configured,
        progress_endpoint_url=PROGRESS_ENDPOINT_URL,
        verifier_progress_endpoint_configured=verifier_progress_endpoint_configured,
        verifier_progress_endpoint_url=VERIFIER_PROGRESS_ENDPOINT_URL,
        verifier_connection_string_configured=verifier_connection_string_configured,
        verifier_connection_string_display=verifier_connection_string_display,
    )


@bp.route("/live_monitoring", methods=["POST"])
def live_monitoring():
    if CONNECTION_STRING:
        target_mongo_uri = CONNECTION_STRING
    else:
        target_mongo_uri = request.form.get("connectionString")
        if target_mongo_uri:
            target_mongo_uri = target_mongo_uri.strip() if target_mongo_uri.strip() else None

    if PROGRESS_ENDPOINT_URL:
        progress_url = PROGRESS_ENDPOINT_URL
    else:
        progress_host = (request.form.get("progressHost") or "").strip()
        progress_port = (request.form.get("progressPort") or "").strip()
        try:
            progress_url = build_progress_endpoint_url(
                progress_host, progress_port or None
            )
        except ValueError as e:
            logger.error("Invalid progress endpoint port: %s", e)
            return render_template(
                "error.html",
                error_title="Invalid Progress Endpoint",
                error_message=str(e),
            )

    if not target_mongo_uri and not progress_url:
        logger.error("No connection string or progress endpoint URL provided")
        return render_template(
            "error.html",
            error_title="No Input Provided",
            error_message="Please provide at least one of the following: Mongosync Progress Endpoint or MongoDB connection string (or both).",
        )

    if progress_url and not validate_progress_endpoint_url(progress_url):
        logger.error("Invalid progress endpoint URL format: %s", progress_url)
        return render_template(
            "error.html",
            error_title="Invalid Progress Endpoint",
            error_message="The progress endpoint format is invalid. Enter a host and port (default 27182); the path /api/v1/progress is fixed.",
        )

    if target_mongo_uri:
        try:
            validate_connection(target_mongo_uri)
        except InvalidURI as e:
            clear_connection_cache()
            logger.error("Invalid connection string format: %s", e)
            return render_template(
                "error.html",
                error_title="Invalid Connection String",
                error_message="The connection string format is invalid. Please check your MongoDB connection string and try again.",
            )
        except PyMongoError as e:
            clear_connection_cache()
            logger.error("Failed to connect: %s", e)
            return render_template(
                "error.html",
                error_title="Connection Failed",
                error_message="Could not connect to MongoDB. Please verify your credentials, network connectivity, and that the cluster is accessible.",
            )
        except Exception as e:
            clear_connection_cache()
            logger.error("Unexpected error during connection validation: %s", e)
            return render_template(
                "error.html",
                error_title="Connection Error",
                error_message="An unexpected error occurred. Please try again.",
            )

    session_data = {
        "connection_string": target_mongo_uri,
        "endpoint_url": progress_url,
    }
    session_id = store_session_data(session_data)

    response = make_response(
        render_template(
            "metrics.html",
            refresh_time=REFRESH_TIME,
            refresh_time_ms=str(int(REFRESH_TIME) * 1000),
        )
    )

    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        httponly=True,
        secure=SECURE_COOKIES,
        samesite="Strict",
        max_age=SESSION_TIMEOUT,
    )
    return response


def _progress_monitor_session_context():
    """Resolve progress endpoint URL and metadata connection string for the monitor tab."""
    endpoint_url = PROGRESS_ENDPOINT_URL
    connection_string = CONNECTION_STRING
    session_data = None
    if not endpoint_url or not connection_string:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id)
    if not endpoint_url and session_data:
        endpoint_url = session_data.get("endpoint_url")
    if not connection_string and session_data:
        connection_string = session_data.get("connection_string")
    return endpoint_url, connection_string


@bp.route("/get_progress_monitor", methods=["POST"])
def get_progress_monitor():
    endpoint_url, connection_string = _progress_monitor_session_context()

    if not endpoint_url and not connection_string:
        return jsonify(progress_monitor_no_config_response(connection_string=connection_string))

    return jsonify(build_live_monitor_payload(endpoint_url, connection_string))


@bp.route("/verifier", methods=["POST"])
def verifier():
    if VERIFIER_PROGRESS_ENDPOINT_URL:
        verifier_progress_url = VERIFIER_PROGRESS_ENDPOINT_URL
    else:
        progress_host = (request.form.get("verifierProgressHost") or "").strip()
        progress_port = (request.form.get("verifierProgressPort") or "").strip()
        try:
            verifier_progress_url = build_verifier_progress_endpoint_url(
                progress_host, progress_port or None
            )
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

    if target_mongo_uri:
        try:
            validate_connection(target_mongo_uri)
        except InvalidURI as e:
            logger.error("Invalid connection string format: %s", e)
            clear_connection_cache()
            return render_template(
                "error.html",
                error_title="Invalid Connection String",
                error_message="The connection string format is invalid. Please check your MongoDB connection string and try again.",
            )
        except PyMongoError as e:
            logger.error("Failed to connect: %s", e)
            clear_connection_cache()
            return render_template(
                "error.html",
                error_title="Connection Failed",
                error_message="Could not connect to MongoDB. Please verify your credentials, network connectivity, and that the cluster is accessible.",
            )
        except Exception as e:
            logger.error("Unexpected error during connection validation: %s", e)
            clear_connection_cache()
            return render_template(
                "error.html",
                error_title="Connection Error",
                error_message="An unexpected error occurred. Please try again.",
            )

    session_data = {
        "verifier_connection_string": target_mongo_uri,
        "verifier_endpoint_url": verifier_progress_url,
    }
    session_id = store_session_data(session_data)

    response = make_response(plot_verifier_metrics())

    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        httponly=True,
        secure=SECURE_COOKIES,
        samesite="Strict",
        max_age=SESSION_TIMEOUT,
    )
    return response


@bp.route("/get_verifier_data", methods=["POST"])
def get_verifier_data():
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = session_store.get_session(session_id)

    if VERIFIER_PROGRESS_ENDPOINT_URL:
        endpoint_url = VERIFIER_PROGRESS_ENDPOINT_URL
    else:
        endpoint_url = (session_data or {}).get("verifier_endpoint_url")

    if VERIFIER_CONNECTION_STRING:
        connection_string = VERIFIER_CONNECTION_STRING
    else:
        connection_string = (session_data or {}).get("verifier_connection_string")

    if not connection_string and not endpoint_url:
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

    return jsonify(
        build_verifier_monitor_payload(
            connection_string,
            endpoint_url=endpoint_url,
        )
    )
