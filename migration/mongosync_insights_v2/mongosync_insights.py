import logging
import json
import re
import subprocess
import os
import tempfile
import time
import platform
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from flask import Flask, render_template, request, make_response, Response, stream_with_context, jsonify, redirect, url_for
from mongosync_plot_logs import upload_file, upload_file_json, analyze_streaming_log, get_mongosync_readonly_options, get_mongosync_hidden_options
from mongosync_plot_metadata import plotMetrics, gatherMetrics, gatherPartitionsMetrics, gatherStartupConfigs, gatherEndpointMetrics
from pymongo.errors import InvalidURI, PyMongoError
from pymongo.uri_parser import parse_uri 
from app_config import (
    setup_logging, validate_config, get_app_info, HOST, PORT, MAX_FILE_SIZE, 
    REFRESH_TIME, APP_VERSION, DEVELOPER_CREDITS, validate_connection, clear_connection_cache, 
    SECURE_COOKIES, CONNECTION_STRING, get_mongo_client, INTERNAL_DB_NAME, get_database,
    PROGRESS_ENDPOINT_URL, validate_progress_endpoint_url, session_store, SESSION_TIMEOUT,
    LOG_FILE_PATH, STREAM_UPDATE_INTERVAL, STREAM_MAX_BUFFER_SIZE
)
from connection_validator import sanitize_for_display
from log_streamer import get_monitor

# Cookie name for session ID
SESSION_COOKIE_NAME = 'mi_session_id'
MONGOSYNC_DOWNLOAD_URL = 'https://www.mongodb.com/try/download/mongosync'


def _get_mongosync_install_instructions():
    """Return OS-specific mongosync install guidance for missing PATH errors."""
    os_name = platform.system() or 'Unknown'
    if os_name == 'Darwin':
        instructions = [
            'Install with Homebrew if available: brew install mongosync',
            'If unavailable in Homebrew, download the macOS build from the URL below and place mongosync in your PATH.',
            'Verify installation: mongosync --version'
        ]
    elif os_name == 'Linux':
        instructions = [
            'Download the Linux build from the URL below and extract it.',
            'Move the mongosync binary to a PATH directory (for example: /usr/local/bin).',
            'Verify installation: mongosync --version'
        ]
    elif os_name == 'Windows':
        instructions = [
            'Download the Windows build from the URL below and extract/install it.',
            'Add the mongosync executable directory to PATH in System Environment Variables.',
            'Open a new terminal and verify: mongosync --version'
        ]
    else:
        instructions = [
            'Download mongosync from the URL below for your operating system.',
            'Install it and ensure the mongosync executable is available in PATH.',
            'Verify installation: mongosync --version'
        ]

    return {
        'os_name': os_name,
        'install_instructions': instructions
    }


def validate_logpath_write_permission(logpath):
    """
    Validate that the logpath directory is writable (or can be created).
    Returns (True, None) if valid, or (False, error_message) if not.
    """
    if not logpath or not str(logpath).strip():
        return True, None
    path = os.path.abspath(str(logpath).strip())
    # Resolve to directory: if path ends with .log, use parent
    if path.endswith('.log') or os.path.isfile(path):
        path = os.path.dirname(path)
    if not path or path == os.path.sep:
        return False, "Invalid logpath"
    if os.path.isdir(path):
        if not os.access(path, os.W_OK):
            return False, f"Mongosync logpath directory is not writable: {path}"
        # Try writing a test file to be sure
        try:
            test_file = os.path.join(path, '.mongosync_insights_write_test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except OSError as e:
            return False, f"Mongosync logpath directory is not writable: {e}"
        return True, None
    # Directory does not exist: check parent
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        return False, f"Parent directory does not exist: {parent}"
    if not os.access(parent, os.W_OK):
        return False, f"Parent directory is not writable (cannot create logpath): {parent}"
    return True, None

# Validate configuration on startup
try:
    validate_config()
except (PermissionError, ValueError) as e:
    print(f"Configuration error: {e}")
    exit(1)

# Setup logging
logger = setup_logging()

# Create a Flask app
app = Flask(__name__, static_folder='images', static_url_path='/images')

# Configure Flask for file uploads
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Add security headers to all responses
@app.after_request
def add_security_headers(response):
    """Add security headers to all HTTP responses."""
    # Enforce HTTPS and prevent downgrade attacks
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    
    # Prevent MIME type sniffing
    response.headers['X-Content-Type-Options'] = 'nosniff'
    
    # Prevent clickjacking attacks
    response.headers['X-Frame-Options'] = 'DENY'
    
    # Control referrer information
    response.headers['Referrer-Policy'] = 'no-referrer'
    
    # Content Security Policy - configured to work with Plotly charts
    # Note: Plotly requires 'unsafe-inline' and 'unsafe-eval' for rendering
    # Note: blob: is required for Plotly snapshot/download functionality
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.plot.ly; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob: https:; "
        "font-src 'self' data:; "
        "connect-src 'self' blob:;"
    )
    
    # Additional security headers
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
    
    return response

# Make app version available to all templates
@app.context_processor
def inject_app_version():
    return dict(app_version=APP_VERSION, developer_credits=DEVELOPER_CREDITS)

# Handle file too large error
@app.errorhandler(413)
def too_large(e):
    max_size_mb = MAX_FILE_SIZE / (1024 * 1024)
    return render_template('error.html',
                         error_title="File Too Large",
                         error_message=f"File size exceeds maximum allowed size ({max_size_mb:.1f} MB)."), 413

@app.route('/setup')
def setup_page():
    """Display setup/configuration page"""
    return render_template('setup.html')


@app.route('/initSetup', methods=['POST'])
def init_setup():
    """
    Validate inputs and initialize session.
    Connection string is derived from cluster1 (mongosync config).
    Log file path is derived from logpath (mongosync config).
    """
    logger.info("=== initSetup endpoint called ===")
    logger.info(f"Request method: {request.method}")
    logger.info(f"Request path: {request.path}")
    logger.info(f"Request form keys: {list(request.form.keys())}")
    
    mongosync_host = request.form.get('mongosyncHost', '').strip()
    log_file_path = request.form.get('logFilePath', '').strip()
    connection_string = request.form.get('connectionString', '').strip()
    loadlevel = request.form.get('loadlevel', '1').strip()
    verbosity = request.form.get('verbosity', 'INFO').strip().upper()
    
    logger.info(f"Mongosync host: {mongosync_host}")
    
    if not mongosync_host:
        logger.error("Mongosync hostname not provided")
        return jsonify({"error": "Mongosync hostname is required"}), 400
    
    hostname_pattern = r'^[\w\.\-]+:\d+$'
    if not re.match(hostname_pattern, mongosync_host):
        logger.error(f"Invalid hostname format: {mongosync_host}")
        return jsonify({"error": "Invalid hostname format. Expected format: hostname:port (e.g., localhost:27182)"}), 400
    
    if not connection_string:
        logger.error("Connection string (cluster1) not provided")
        return jsonify({"error": "cluster1 (Destination) connection string is required"}), 400

    cluster0 = request.form.get('cluster0', '').strip()
    if not cluster0:
        logger.error("cluster0 (Source) not provided")
        return jsonify({"error": "cluster0 (Source) connection string is required"}), 400

    if loadlevel not in {'1', '2', '3', '4'}:
        loadlevel = '1'
    if verbosity not in {'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL', 'PANIC'}:
        verbosity = 'INFO'

    def validate_cluster_uri(uri, name):
        """Validate URI format and test connectivity. Raises on error."""
        try:
            parse_uri(uri)
        except InvalidURI as parse_error:
            sanitized = uri.split('@')[1] if '@' in uri else uri
            logger.error(f"URI parsing failed for {name}: {sanitized}")
            raise
        validate_connection(uri)

    # Validate and test cluster1 (connection string)
    try:
        logger.info("Validating cluster1 (destination) connection string...")
        validate_cluster_uri(connection_string, "cluster1")
        logger.info("Cluster1 connection successful")
    except InvalidURI as e:
        clear_connection_cache()
        error_msg = str(e)
        if "Port contains non-digit characters" in error_msg or "must be escaped according to RFC 3986" in error_msg:
            return jsonify({
                "error": "Your cluster1 connection string contains special characters that cause parsing issues. "
                         "Ensure the password is properly URL-encoded, or use credentials without special characters (@, :, /, ?, #, [, ], %, or spaces)."
            }), 400
        return jsonify({"error": f"Invalid cluster1 connection string format: {error_msg}"}), 400
    except PyMongoError as e:
        clear_connection_cache()
        logger.error(f"Failed to connect to cluster1: {e}")
        return jsonify({"error": f"Failed to connect to cluster1 (destination): {str(e)}"}), 400
    except Exception as e:
        clear_connection_cache()
        logger.error(f"Unexpected error validating cluster1: {e}")
        return jsonify({"error": f"Cluster1 connection validation error: {str(e)}"}), 400

    # Validate and test cluster0 (source)
    try:
        logger.info("Validating cluster0 (source) connection string...")
        validate_cluster_uri(cluster0, "cluster0")
        logger.info("Cluster0 connection successful")
    except InvalidURI as e:
        clear_connection_cache()
        return jsonify({"error": f"Invalid cluster0 connection string format: {str(e)}"}), 400
    except PyMongoError as e:
        clear_connection_cache()
        logger.error(f"Failed to connect to cluster0: {e}")
        return jsonify({"error": f"Failed to connect to cluster0 (source): {str(e)}"}), 400
    except Exception as e:
        clear_connection_cache()
        logger.error(f"Unexpected error validating cluster0: {e}")
        return jsonify({"error": f"Cluster0 connection validation error: {str(e)}"}), 400
    
    # Set default stream interval (will be adjustable in dashboard)
    interval_value = 1.0
    
    # Validate log file path if provided (check write permission on directory)
    if log_file_path:
        logpath_dir = os.path.dirname(log_file_path)
        ok, err = validate_logpath_write_permission(logpath_dir)
        if not ok:
            logger.error(f"Logpath write validation failed: {err}")
            return jsonify({"error": err}), 400
        if not os.path.isfile(log_file_path):
            logger.warning(f"Log file path does not exist or is not a file: {log_file_path}")
            # Just warn but don't fail - file might be created later
    
    # Construct progress endpoint URL (without http:// prefix - added by gatherEndpointMetrics)
    progress_endpoint_url = f"{mongosync_host}/api/v1/progress"
    logger.info(f"Constructed progress endpoint: {progress_endpoint_url}")
    
    # Create session with validated data
    session_data = {
        'mongosync_hostname': mongosync_host,
        'endpoint_url': progress_endpoint_url,
        'connection_string': connection_string,
        'cluster0': cluster0,
        'cluster1': connection_string,
        'loadlevel': int(loadlevel),
        'verbosity': verbosity,
        'log_file_path': log_file_path if log_file_path else None,
        'stream_interval': interval_value
    }
    session_id = session_store.create_session(session_data)
    
    response = make_response(jsonify({
        "success": True,
        "message": "Setup completed successfully"
    }))
    
    # Set secure session cookie
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        max_age=SESSION_TIMEOUT,
        secure=SECURE_COOKIES,
        httponly=True,
        samesite='Strict'
    )
    
    logger.info(f"Setup completed successfully for host: {mongosync_host}")
    return response


@app.route('/clearSetup', methods=['POST'])
def clear_setup():
    """Clear session credentials"""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        session_store.delete_session(session_id)
        logger.info("Session cleared")
    
    response = make_response(jsonify({"success": True}))
    response.set_cookie(SESSION_COOKIE_NAME, '', expires=0)
    return response


@app.route('/')
def home_page():
    # Check if user has credentials in session or environment variables
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = session_store.get_session(session_id) if session_id else {}
    
    # If no env vars and no session credentials, redirect to setup
    if not CONNECTION_STRING and not PROGRESS_ENDPOINT_URL:
        if not session_data or not session_data.get('mongosync_hostname'):
            logger.info("No credentials found, redirecting to setup")
            return redirect(url_for('setup_page'))
    
    # Calculate max file size in GB for display
    max_file_size_gb = MAX_FILE_SIZE / (1024 * 1024 * 1024)
    
    # Determine connection string (env var takes precedence)
    connection_string_display = None
    if CONNECTION_STRING:
        connection_string_display = sanitize_for_display(CONNECTION_STRING)
    elif session_data.get('connection_string'):
        connection_string_display = sanitize_for_display(session_data.get('connection_string'))

    # Check if streaming is enabled (env var takes precedence over session)
    log_file_path_value = LOG_FILE_PATH if LOG_FILE_PATH else session_data.get('log_file_path', '')
    has_log_file_path = bool(log_file_path_value)
    log_file_path = log_file_path_value if log_file_path_value else "Not configured"
    
    logger.info(f"=== Home Page Rendering ===")
    logger.info(f"LOG_FILE_PATH (env): {LOG_FILE_PATH}")
    logger.info(f"Session log_file_path: {session_data.get('log_file_path', 'Not set')}")
    logger.info(f"Final log_file_path_value: {log_file_path_value}")
    logger.info(f"has_log_file_path: {has_log_file_path}")
    logger.info(f"log_file_path (display): {log_file_path}")
    
    # Get stream interval (env var takes precedence over session)
    if STREAM_UPDATE_INTERVAL:
        stream_interval = STREAM_UPDATE_INTERVAL
    elif session_data.get('stream_interval'):
        stream_interval = session_data.get('stream_interval')
    else:
        stream_interval = 1.0
    
    return render_template('home.html', 
                           max_file_size_gb=max_file_size_gb,
                           has_log_file_path=has_log_file_path,
                           log_file_path=log_file_path,
                           stream_interval=stream_interval,
                           CONNECTION_STRING=CONNECTION_STRING,
                           connection_string_display=connection_string_display,
                           PROGRESS_ENDPOINT_URL=PROGRESS_ENDPOINT_URL,
                           refresh_time_ms=REFRESH_TIME * 1000)


@app.route('/upload', methods=['POST'])
def uploadLogs():
    return upload_file()


@app.route('/upload_json', methods=['POST'])
def uploadLogsJson():
    """AJAX endpoint for log file upload that returns JSON with plot data."""
    return upload_file_json(session_store, SESSION_COOKIE_NAME)


@app.route('/analyze_streaming_log', methods=['POST'])
def analyzeStreamingLog():
    """AJAX endpoint to analyze the streaming log file and return updated metrics."""
    # Get log file path from env var or session
    log_file_path = LOG_FILE_PATH
    if not log_file_path:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else {}
        log_file_path = session_data.get('log_file_path', '')
    
    return analyze_streaming_log(log_file_path)


@app.route('/get_mongosync_readonly_options', methods=['GET'])
def get_mongosync_readonly_options_endpoint():
    """Return read-only mongosync options from log file for display in Settings."""
    log_file_path = LOG_FILE_PATH
    if not log_file_path:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else {}
        log_file_path = session_data.get('log_file_path', '')
    opts = get_mongosync_readonly_options(log_file_path)
    # Convert values to JSON-serializable (e.g. bool, int, str)
    for k, v in opts.items():
        if v is not None and not isinstance(v, (bool, int, float, str)):
            opts[k] = str(v)
    return jsonify(opts)


@app.route('/get_mongosync_hidden_options', methods=['GET'])
def get_mongosync_hidden_options_endpoint():
    """Return Mongosync Hidden Options from log file for display in Settings."""
    log_file_path = LOG_FILE_PATH
    if not log_file_path:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else {}
        log_file_path = session_data.get('log_file_path', '')
    opts = get_mongosync_hidden_options(log_file_path)
    # Convert values to JSON-serializable
    for category in opts:
        for k, v in opts[category].items():
            if v is not None and not isinstance(v, (bool, int, float, str)):
                opts[category][k] = str(v)
    return jsonify(opts)


@app.route('/get_namespace_filters', methods=['GET'])
def get_namespace_filters():
    """Return namespace filters (inclusionFilter, exclusionFilter) from session."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    filters = {'inclusionFilter': [], 'exclusionFilter': []}
    if session_id:
        session_data = session_store.get_session(session_id)
        if session_data:
            filters = {
                'inclusionFilter': session_data.get('namespace_inclusion_filter', []),
                'exclusionFilter': session_data.get('namespace_exclusion_filter', [])
            }
    return jsonify(filters)


def _validate_namespace_filter_rules(rules):
    """Validate inclusion/exclusion filter rules. Returns (validated_list, error_msg)."""
    if not isinstance(rules, list):
        return [], "Must be an array"
    result = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            return [], f"Rule {i + 1} must be an object"
        db = rule.get('database')
        if not db:
            return [], f"Rule {i + 1}: database is required"
        if isinstance(db, list):
            db_valid = all(isinstance(d, str) and d and '.' not in d for d in db)
        else:
            db_valid = isinstance(db, str) and db and '.' not in db
        if not db_valid:
            return [], f"Rule {i + 1}: database must be non-empty without '.'"
        colls = rule.get('collections')
        if colls is not None:
            if isinstance(colls, list):
                coll_list = [str(c).strip() for c in colls if c]
            else:
                coll_list = [c.strip() for c in str(colls).split(',') if c.strip()]
            if any('.' in c for c in coll_list):
                return [], f"Rule {i + 1}: collection names must not contain '.'"
            rule = dict(rule)
            rule['collections'] = coll_list if coll_list else None
        result.append(rule)
    return result, None


@app.route('/save_namespace_filters', methods=['POST'])
def save_namespace_filters():
    """Save namespace filters to session. Accepts {inclusionFilter, exclusionFilter}."""
    try:
        data = request.get_json() or {}
        inc = data.get('inclusionFilter', [])
        exc = data.get('exclusionFilter', [])

        inc_valid, err = _validate_namespace_filter_rules(inc)
        if err:
            return jsonify({'error': err}), 400
        exc_valid, err = _validate_namespace_filter_rules(exc)
        if err:
            return jsonify({'error': err}), 400

        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return jsonify({'error': 'No session found'}), 400

        session_data = session_store.get_session(session_id)
        if not session_data:
            return jsonify({'error': 'Invalid session'}), 400

        session_data['namespace_inclusion_filter'] = inc_valid
        session_data['namespace_exclusion_filter'] = exc_valid
        session_store.update_session(session_id, session_data)

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"save_namespace_filters error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/namespace_filters')
def namespace_filters_page():
    """Render the namespace filters configuration page (opens in new window)."""
    return render_template('namespace_filters.html')


@app.route('/list_source_namespaces', methods=['POST'])
def list_source_namespaces():
    """
    List databases and collections from the source cluster (cluster0).
    Request body: { "cluster0": "mongodb://..." }.
    Returns: { "databases": [ { "name": "db1", "collections": ["c1","c2"] }, ... ] }.
    """
    try:
        data = request.get_json() or {}
        cluster0 = (data.get('cluster0') or '').strip()
        if not cluster0:
            session_id = request.cookies.get(SESSION_COOKIE_NAME)
            if session_id:
                session_data = session_store.get_session(session_id)
                if session_data:
                    cluster0 = (session_data.get('cluster0') or '').strip()
            if not cluster0:
                return jsonify({'error': 'cluster0 (Source) connection string is required. Configure in Mongosync Settings.'}), 400

        from pymongo import MongoClient
        client = MongoClient(
            cluster0,
            serverSelectionTimeoutMS=15000,
            connectTimeoutMS=10000
        )
        client.admin.command('ping')

        db_list = client.list_database_names()
        result = []
        for db_name in sorted(db_list):
            if db_name in ('admin', 'local', 'config'):
                continue
            try:
                coll_list = client[db_name].list_collection_names()
                result.append({'name': db_name, 'collections': sorted(coll_list)})
            except Exception as e:
                logger.warning(f"Could not list collections for db {db_name}: {e}")
                result.append({'name': db_name, 'collections': []})

        client.close()
        return jsonify({'databases': result})
    except PyMongoError as e:
        logger.error(f"list_source_namespaces error: {e}")
        return jsonify({'error': f'Failed to connect to source cluster: {str(e)}'}), 400
    except Exception as e:
        logger.error(f"list_source_namespaces error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/get_mongosync_config', methods=['GET'])
def get_mongosync_config():
    """Return cluster0 and cluster1 from session (for namespace filters when opened directly)."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    config = {'cluster0': '', 'cluster1': ''}
    if session_id:
        session_data = session_store.get_session(session_id)
        if session_data:
            config['cluster0'] = session_data.get('cluster0', '') or ''
            config['cluster1'] = session_data.get('cluster1', '') or session_data.get('connection_string', '')
    return jsonify(config)


@app.route('/namespace_filters_status', methods=['GET'])
def namespace_filters_status():
    """
    Return whether namespace filters can be edited.
    Rules:
    - If coordinator resumeData exists in cluster1 internal DB, migration is underway -> disable edits.
    - If migration status cannot be determined, fail-safe to disabled.
    """
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = session_store.get_session(session_id) if session_id else None

    cluster1 = ''
    if session_data:
        cluster1 = (
            (session_data.get('cluster1') or '').strip() or
            (session_data.get('connection_string') or '').strip()
        )
    if not cluster1 and CONNECTION_STRING:
        cluster1 = CONNECTION_STRING

    if not cluster1:
        return jsonify({
            'allow_edit': False,
            'reason': 'status_unavailable',
            'details': 'Destination connection string (cluster1) is not configured.'
        }), 200

    try:
        internal_db = get_database(cluster1, INTERNAL_DB_NAME)
        resume_doc = internal_db.resumeData.find_one({'_id': 'coordinator'})
        if resume_doc is not None:
            return jsonify({
                'allow_edit': False,
                'reason': 'migration_underway'
            }), 200

        return jsonify({
            'allow_edit': True,
            'reason': 'not_started'
        }), 200
    except Exception as e:
        logger.warning(f"namespace_filters_status check failed: {e}")
        return jsonify({
            'allow_edit': False,
            'reason': 'status_unavailable',
            'details': str(e)
        }), 200


@app.route('/initMonitor', methods=['POST'])
def initMonitor():
    """Initialize live monitoring and return available tabs based on session credentials."""
    # Get connection string from env var or session
    if CONNECTION_STRING:
        TARGET_MONGO_URI = CONNECTION_STRING
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else {}
        TARGET_MONGO_URI = session_data.get('connection_string')

    # Get progress endpoint URL from env var or session
    if PROGRESS_ENDPOINT_URL:
        progress_url = PROGRESS_ENDPOINT_URL
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else {}
        progress_url = session_data.get('endpoint_url')

    # At this point, credentials should exist from setup or env vars
    if not TARGET_MONGO_URI and not progress_url:
        logger.error("No credentials found in session or environment")
        return jsonify({"error": "No credentials configured. Please return to setup."}), 400

    logger.info(f"Monitor initialized. Connection string: {bool(TARGET_MONGO_URI)}, Endpoint URL: {bool(progress_url)}")
    
    return jsonify({
        "success": True,
        "has_connection_string": bool(TARGET_MONGO_URI),
        "has_endpoint_url": bool(progress_url)
    })


@app.route('/renderMetrics', methods=['POST'])
def renderMetrics():
    # Get connection string from env var or form (no caching)
    if CONNECTION_STRING:
        TARGET_MONGO_URI = CONNECTION_STRING
    else:
        TARGET_MONGO_URI = request.form.get('connectionString')
        if TARGET_MONGO_URI:
            TARGET_MONGO_URI = TARGET_MONGO_URI.strip() if TARGET_MONGO_URI.strip() else None

    # Get progress endpoint URL from env var or form (no caching)
    if PROGRESS_ENDPOINT_URL:
        progress_url = PROGRESS_ENDPOINT_URL
    else:
        progress_url = request.form.get('progressEndpointUrl')
        if progress_url:
            progress_url = progress_url.strip() if progress_url.strip() else None

    # Validate that at least one field is provided
    if not TARGET_MONGO_URI and not progress_url:
        logger.error("No connection string or progress endpoint URL provided")
        return render_template('error.html',
                             error_title="No Input Provided",
                             error_message="Please provide at least one of the following: MongoDB Destination Cluster Connection String or Mongosync Progress Endpoint URL (or both).")

    # Validate progress endpoint URL format if provided
    if progress_url:
        if not validate_progress_endpoint_url(progress_url):
            logger.error(f"Invalid progress endpoint URL format: {progress_url}")
            return render_template('error.html',
                                 error_title="Invalid Progress Endpoint URL",
                                 error_message="The Progress Endpoint URL format is invalid. Expected format: host:port/api/v1/progress (e.g., localhost:27182/api/v1/progress)")

    # Test MongoDB connection if connection string is provided
    if TARGET_MONGO_URI:
        try:
            # Connection test (network, authentication)
            validate_connection(TARGET_MONGO_URI)
                
        except InvalidURI as e:
            clear_connection_cache()
            logger.error(f"Invalid connection string format: {e}")
            return render_template('error.html',
                                error_title="Invalid Connection String",
                                error_message="The connection string format is invalid. Please check your MongoDB destination cluster connection string and try again.")
        except PyMongoError as e:
            clear_connection_cache()
            logger.error(f"Failed to connect: {e}")
            return render_template('error.html',
                                error_title="Connection Failed",
                                error_message="Could not connect to MongoDB. Please verify your credentials, network connectivity, and that the cluster is accessible.")
        except Exception as e:
            clear_connection_cache()
            logger.error(f"Unexpected error during connection validation: {e}")
            return render_template('error.html',
                                error_title="Connection Error",
                                error_message="An unexpected error occurred. Please try again.")

    # Store credentials in server-side in-memory session store
    session_data = {
        'connection_string': TARGET_MONGO_URI,
        'endpoint_url': progress_url
    }
    session_id = session_store.create_session(session_data)

    # Determine which tabs to show (pass only boolean flags to template, not credentials)
    has_connection_string = bool(TARGET_MONGO_URI)
    has_endpoint_url = bool(progress_url)
    
    # Render the metrics page
    response = make_response(plotMetrics(
        has_connection_string=has_connection_string, 
        has_endpoint_url=has_endpoint_url
    ))
    
    # Set session ID in a secure cookie
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        httponly=True,  # Prevent JavaScript access
        secure=SECURE_COOKIES,  # Only send over HTTPS when enabled
        samesite='Strict',  # CSRF protection
        max_age=SESSION_TIMEOUT
    )
    
    return response

@app.route('/get_metrics_data', methods=['POST'])
def getMetrics():
    # Get connection string from env var or in-memory session store
    if CONNECTION_STRING:
        connection_string = CONNECTION_STRING
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        connection_string = session_data.get('connection_string') if session_data else None
    
    if not connection_string:
        logger.info("MongoDB destination cluster connection string not provided - Status tab unavailable")
        return jsonify({
            "error": "MongoDB destination cluster connection string not configured", 
            "message": "The Status tab requires a MongoDB destination cluster connection string. This is optional - you can configure it in Settings if needed.",
            "error_type": "not_configured"
        }), 400
    
    # Get endpoint URL for fetching lag time
    if PROGRESS_ENDPOINT_URL:
        endpoint_url = PROGRESS_ENDPOINT_URL
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        endpoint_url = session_data.get('endpoint_url') if session_data else None
    
    return gatherMetrics(connection_string, endpoint_url)

@app.route('/get_partitions_data', methods=['POST'])
def getPartitionsData():
    # Get connection string from env var or in-memory session store
    if CONNECTION_STRING:
        connection_string = CONNECTION_STRING
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        connection_string = session_data.get('connection_string') if session_data else None
    
    if not connection_string:
        logger.info("MongoDB destination cluster connection string not provided - Partitions tab unavailable")
        return jsonify({
            "error": "MongoDB destination cluster connection string not configured",
            "message": "The Partitions tab requires a MongoDB destination cluster connection string. This is optional - you can configure it in Settings if needed.",
            "error_type": "not_configured"
        }), 400
    
    # Get endpoint URL for pie charts
    if PROGRESS_ENDPOINT_URL:
        endpoint_url = PROGRESS_ENDPOINT_URL
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        endpoint_url = session_data.get('endpoint_url') if session_data else None
    
    return gatherPartitionsMetrics(connection_string, endpoint_url)

@app.route('/get_configs_data', methods=['POST'])
def getConfigsData():
    # Get connection string from env var or in-memory session store
    if CONNECTION_STRING:
        connection_string = CONNECTION_STRING
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        connection_string = session_data.get('connection_string') if session_data else None
    
    if not connection_string:
        logger.info("MongoDB destination cluster connection string not provided - Configs tab unavailable")
        return jsonify({
            "error": "MongoDB destination cluster connection string not configured",
            "message": "The Startup Configs tab requires a MongoDB destination cluster connection string. This is optional - you can configure it in Settings if needed.",
            "error_type": "not_configured"
        }), 400
    
    return gatherStartupConfigs(connection_string)

@app.route('/get_endpoint_data', methods=['POST'])
def getEndpointData():
    # Get endpoint URL from env var or in-memory session store
    if PROGRESS_ENDPOINT_URL:
        endpoint_url = PROGRESS_ENDPOINT_URL
        logger.info(f"Using endpoint URL from env var: {endpoint_url}")
    else:
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = session_store.get_session(session_id) if session_id else None
        endpoint_url = session_data.get('endpoint_url') if session_data else None
        logger.info(f"Using endpoint URL from session: {endpoint_url}")
    
    if not endpoint_url:
        logger.error("No progress endpoint URL available for endpoint data refresh")
        return jsonify({"error": "No progress endpoint URL available. Please refresh the page and re-enter your credentials."}), 400
    
    logger.info(f"Calling gatherEndpointMetrics with URL: {endpoint_url}")
    return gatherEndpointMetrics(endpoint_url)

@app.route('/stream')
def stream_page():
    """Render the streaming log visualization page."""
    if not LOG_FILE_PATH:
        return render_template('error.html',
                             error_title="Streaming Not Configured",
                             error_message="Log file path not configured. Please set MI_LOG_FILE_PATH environment variable.")
    
    return render_template('stream_logs.html')

def _get_mongosync_host_for_proxy():
    """
    Get mongosync host:port for API proxy. Checks session first, then derives from endpoint_url.
    Returns (host, error_response_tuple) - error tuple is (jsonify(...), status) or None if success.
    """
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = session_store.get_session(session_id) if session_id else None
    
    mongosync_host = session_data.get('mongosync_hostname') if session_data else None
    
    if mongosync_host:
        return mongosync_host, None
    
    # Fallback: derive from endpoint_url (format: host:port/api/v1/progress)
    endpoint_url = None
    if PROGRESS_ENDPOINT_URL:
        endpoint_url = PROGRESS_ENDPOINT_URL
    elif session_data:
        endpoint_url = session_data.get('endpoint_url')
    
    if endpoint_url and '/api/v1/progress' in endpoint_url:
        mongosync_host = endpoint_url.split('/api/v1/progress')[0].rstrip('/')
        if mongosync_host and ':' in mongosync_host:
            # Persist to session for future requests (avoids re-deriving on each call)
            if session_id and session_data is not None:
                session_data = dict(session_data)
                session_data['mongosync_hostname'] = mongosync_host
                session_store.update_session(session_id, session_data)
            return mongosync_host, None
    
    logger.error("No mongosync hostname configured (session or endpoint_url)")
    return None, (jsonify({"error": "Mongosync hostname not configured. Use Setup to configure the mongosync host."}), 400)


@app.route('/api/mongosync/<path:endpoint>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def mongosync_proxy(endpoint):
    """
    Proxy endpoint for mongosync API calls.
    Forwards requests to configured mongosync host to avoid CORS issues.
    """
    mongosync_host, err = _get_mongosync_host_for_proxy()
    if err:
        return err
    
    mongosync_base_url = f'http://{mongosync_host}'
    url = f'{mongosync_base_url}/api/v1/{endpoint}'
    
    try:
        # Get request data if present
        data = None
        content_type = 'application/json'
        if request.method in ['POST', 'PUT']:
            if request.is_json:
                data = json.dumps(request.json).encode('utf-8')
                content_type = 'application/json'
            else:
                data = request.data
                content_type = request.content_type or 'application/json'
        
        # Create request
        req = Request(url, data=data, method=request.method)
        
        # Set Content-Type header for POST/PUT requests
        if request.method in ['POST', 'PUT'] and data:
            req.add_header('Content-Type', content_type)
        
        # Copy headers (excluding host and content-length)
        for header, value in request.headers:
            if header.lower() not in ['host', 'content-length', 'content-encoding', 'content-type']:
                req.add_header(header, value)
        
        # Make the request. Some mongosync control endpoints (resume/start)
        # can take longer than 10s before returning.
        with urlopen(req, timeout=30) as response:
            response_data = response.read()
            content_type = response.headers.get('Content-Type', 'application/json')
            
            # Try to parse as JSON, fallback to text
            try:
                json_data = json.loads(response_data.decode('utf-8'))
                return jsonify(json_data), response.status
            except (json.JSONDecodeError, UnicodeDecodeError):
                return Response(response_data, mimetype=content_type, status=response.status)
                
    except HTTPError as e:
        error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
        logger.error(f"Mongosync API error: {e.code} - {error_body}")
        try:
            error_json = json.loads(error_body)
            return jsonify(error_json), e.code
        except json.JSONDecodeError:
            return jsonify({'error': error_body, 'status_code': e.code}), e.code
    except URLError as e:
        logger.error(f"Mongosync API connection error: {e}")
        return jsonify({'error': f'Failed to connect to mongosync API: {str(e)}'}), 503
    except Exception as e:
        logger.error(f"Unexpected error proxying to mongosync API: {e}")
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

def _find_mongosync_pid():
    """Find mongosync PID via ps. Returns (pid, None) or (None, error_message)."""
    try:
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            return None, "Failed to run ps command"
        for line in result.stdout.splitlines():
            if 'grep' in line.lower():
                continue
            line_lower = line.lower()
            if (' mongosync ' in line_lower or
                    line_lower.endswith(' mongosync') or
                    '/mongosync ' in line_lower or
                    line_lower.endswith('/mongosync')):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return int(parts[1]), None
                    except ValueError:
                        continue
        return None, "Mongosync process not found. Ensure mongosync is running on this machine."
    except subprocess.TimeoutExpired:
        return None, "Command timed out"
    except Exception as e:
        return None, str(e)


def _fetch_mongosync_state(mongosync_host):
    """
    Fetch mongosync state from progress API.
    Returns (state, None) e.g. ('PAUSED', None) or (None, error_message).
    """
    try:
        url = f'http://{mongosync_host}/api/v1/progress'
        req = Request(url, method='GET')
        with urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
        progress = data.get('progress', {})
        state = progress.get('state', 'NO DATA')
        return state, None
    except Exception as e:
        return None, str(e)


def _wait_for_process_exit(timeout_sec=30, poll_interval=0.5):
    """
    Poll until mongosync process exits. Returns True if exited, False if timeout.
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        pid, _ = _find_mongosync_pid()
        if pid is None:
            return True
        time.sleep(poll_interval)
    return False


def _collect_host_resources():
    """
    Collect host-level resource metrics with graceful degradation.
    Returns a dict with available fields only.
    """
    host = {
        'hostname': platform.node() or 'unknown'
    }

    # Load averages are POSIX-only
    try:
        load1, load5, load15 = os.getloadavg()
        host['load_avg'] = {
            '1m': round(float(load1), 2),
            '5m': round(float(load5), 2),
            '15m': round(float(load15), 2)
        }
    except Exception:
        pass

    try:
        import psutil  # type: ignore

        host['cpu_percent'] = round(float(psutil.cpu_percent(interval=0.1)), 1)

        vmem = psutil.virtual_memory()
        host['memory'] = {
            'total_bytes': int(vmem.total),
            'used_bytes': int(vmem.used),
            'percent': round(float(vmem.percent), 1)
        }

        disk = psutil.disk_usage('/')
        host['disk'] = {
            'total_bytes': int(disk.total),
            'used_bytes': int(disk.used),
            'percent': round(float(disk.percent), 1)
        }
    except Exception:
        # psutil not available or failed; return partial host data
        pass

    return host


def _collect_mongosync_process_resources(pid):
    """
    Collect mongosync process metrics for the given PID.
    Returns (process_metrics_dict, error_message_or_none).
    """
    if not pid:
        return None, "Process not running"

    try:
        import psutil  # type: ignore

        p = psutil.Process(int(pid))
        with p.oneshot():
            cpu_percent = p.cpu_percent(interval=0.1)
            mem = p.memory_info()
            create_time = p.create_time()
            now = time.time()

        return {
            'pid': int(pid),
            'status': p.status(),
            'cpu_percent': round(float(cpu_percent), 1),
            'rss_bytes': int(mem.rss),
            'vms_bytes': int(mem.vms),
            'uptime_seconds': int(max(0, now - create_time))
        }, None
    except Exception as psutil_error:
        # Fallback to ps output on systems where psutil isn't installed/working
        try:
            result = subprocess.run(
                ['ps', '-p', str(pid), '-o', 'pid=,%cpu=,rss=,vsz=,etime=,stat='],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0 or not result.stdout.strip():
                return None, f"Failed to collect process metrics: {result.stderr or 'no process output'}"

            parts = result.stdout.strip().split()
            # Expected: pid cpu rss vsz etime stat
            if len(parts) < 6:
                return None, "Unexpected process metrics output format"

            return {
                'pid': int(parts[0]),
                'cpu_percent': float(parts[1]),
                # ps rss/vsz are usually in KB
                'rss_bytes': int(float(parts[2]) * 1024),
                'vms_bytes': int(float(parts[3]) * 1024),
                'elapsed': parts[4],
                'status': parts[5]
            }, None
        except Exception as fallback_error:
            return None, f"Failed to collect process metrics: {psutil_error}; fallback failed: {fallback_error}"


@app.route('/get_resources_data', methods=['POST'])
def getResourcesData():
    """
    Return host and mongosync process resource metrics.
    Always returns host metrics where possible; process metrics are optional.
    """
    host = _collect_host_resources()
    pid, _ = _find_mongosync_pid()
    process, process_error = _collect_mongosync_process_resources(pid)

    return jsonify({
        'host': host,
        'process': process or {},
        'process_running': bool(pid and process),
        'process_error': process_error
    }), 200


def _write_config_and_launch(cluster0, cluster1, loadlevel, logpath, verbosity, namespace_filter=None,
                            build_indexes=None, reversible=None, detect_random_id=None,
                            write_blocking_mode=None, embedded_verifier=None):
    """
    Write config.json and launch mongosync. Does not check if already running.
    Returns (log_file_path, None) on success, or (None, (jsonify_response, status_code)) on error.
    namespace_filter: optional dict with inclusionFilter and/or exclusionFilter.
    build_indexes, reversible, detect_random_id, write_blocking_mode, embedded_verifier: optional config options.
    """
    try:
        loadlevel = int(loadlevel) if loadlevel is not None else 1
        if loadlevel < 1 or loadlevel > 4:
            loadlevel = 1
    except (ValueError, TypeError):
        loadlevel = 1

    if not logpath:
        logpath = os.path.join(tempfile.gettempdir(), 'mongosync')
    logpath = os.path.abspath(logpath)

    try:
        os.makedirs(logpath, mode=0o755, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create logpath directory {logpath}: {e}")
        return None, (jsonify({'error': f'Failed to create directory {logpath}: {str(e)}'}), 500)

    config = {
        'cluster0': cluster0,
        'cluster1': cluster1,
        'loadLevel': loadlevel,
        'logPath': logpath,
        'verbosity': (verbosity or 'INFO').strip()
    }
    if namespace_filter and (namespace_filter.get('inclusionFilter') or namespace_filter.get('exclusionFilter')):
        config['namespaceFilter'] = namespace_filter
    if build_indexes is not None and build_indexes in ('afterDataCopy', 'beforeDataCopy', 'never'):
        config['buildIndexes'] = build_indexes
    if reversible is not None:
        config['reversible'] = bool(reversible)
    if detect_random_id is not None:
        config['detectRandomId'] = bool(detect_random_id)
    if write_blocking_mode is not None and write_blocking_mode in ('destinationOnly', 'sourceAndDestination', 'none'):
        config['writeBlockingMode'] = write_blocking_mode
    if embedded_verifier is not None and embedded_verifier:
        config['verificationmode'] = str(embedded_verifier).strip()

    config_path = os.path.join(logpath, 'config.json')
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
    except OSError as e:
        logger.error(f"Failed to write config to {config_path}: {e}")
        return None, (jsonify({'error': f'Failed to write config file: {str(e)}'}), 500)

    config_path_abs = os.path.abspath(config_path)
    try:
        proc = subprocess.Popen(
            ['mongosync', '--acceptDisclaimer', '--config', config_path_abs],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,
            cwd=logpath
        )
        try:
            _, stderr = proc.communicate(timeout=2)
            if proc.returncode is not None and proc.returncode != 0:
                err_msg = (stderr.decode('utf-8', errors='replace') if stderr else '') or f'Exit code {proc.returncode}'
                logger.error(f"Mongosync exited with {proc.returncode}: {err_msg}")
                return None, (jsonify({'error': f'Mongosync failed to start: {err_msg}'}), 500)
        except subprocess.TimeoutExpired:
            pass
        logger.info(f"Launched mongosync with PID {proc.pid}, config at {config_path_abs}, logs in {logpath}")
    except FileNotFoundError:
        guidance = _get_mongosync_install_instructions()
        return None, (jsonify({
            'error': 'mongosync binary not found in PATH.',
            'error_code': 'MONGOSYNC_NOT_FOUND',
            'download_url': MONGOSYNC_DOWNLOAD_URL,
            'os_name': guidance.get('os_name', 'Unknown'),
            'install_instructions': guidance.get('install_instructions', [])
        }), 500)
    except Exception as e:
        logger.error(f"Failed to launch mongosync: {e}")
        return None, (jsonify({'error': f'Failed to launch mongosync: {str(e)}'}), 500)

    log_file_path = os.path.join(logpath, 'mongosync.log')
    return log_file_path, None


def _get_process_cmdline(pid):
    """Get full command line for process. Returns string or None."""
    try:
        if os.path.exists(f'/proc/{pid}/cmdline'):
            # Linux: null-separated args
            with open(f'/proc/{pid}/cmdline', 'r') as f:
                return f.read().replace('\x00', ' ')
        else:
            # macOS: use ps
            result = subprocess.run(
                ['ps', '-ww', '-p', str(pid), '-o', 'args='],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout.strip()
            return None
    except Exception:
        return None


def _detect_log_path_from_cmdline(cmdline):
    """Parse --logPath or --metricsLoggingFilepath from cmdline. Returns path or None."""
    if not cmdline:
        return None
    # Match --logPath /path, --logPath=path, --logPath="/path", --logPath='/path'
    for opt in ['--logPath', '--metricsLoggingFilepath']:
        pattern = rf'{re.escape(opt)}(?:\s*=\s*|\s+)(?:["\']([^"\']+)["\']|(\S+))'
        match = re.search(pattern, cmdline)
        if match:
            path = match.group(1) or match.group(2)
            if path and os.path.isdir(path):
                # Prefer mongosync.log (active log)
                main_log = os.path.join(path, 'mongosync.log')
                if os.path.isfile(main_log):
                    return main_log
                # List .log files and return most recent
                try:
                    logs = [f for f in os.listdir(path) if f.endswith('.log')]
                    if logs:
                        logs.sort(key=lambda f: os.path.getmtime(os.path.join(path, f)), reverse=True)
                        return os.path.join(path, logs[0])
                except OSError:
                    pass
                return main_log  # Return expected path even if not created yet
    return None


def _detect_log_path_from_lsof(pid):
    """Use lsof to find open .log files for the process. Returns path or None."""
    try:
        result = subprocess.run(
            ['lsof', '-p', str(pid)],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            return None
        # Look for lines with .log in the path (last column is often the file path)
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 9 and '.log' in line:
                # File path is typically last column
                path = parts[-1]
                if path.endswith('.log') and os.path.isfile(path):
                    return path
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return None


@app.route('/detect_log_path', methods=['GET'])
def detect_log_path():
    """
    Auto-detect mongosync log file path by inspecting the mongosync process.
    Works only when mongosync runs on the same machine as mongosync_insights.
    """
    pid, err = _find_mongosync_pid()
    if err:
        return jsonify({'detected': False, 'message': err}), 200

    # Method 1: Parse command line for --logPath or --metricsLoggingFilepath
    cmdline = _get_process_cmdline(pid)
    path = _detect_log_path_from_cmdline(cmdline)
    if path:
        logger.info(f"Detected log path via command line: {path}")
        return jsonify({'detected': True, 'path': path, 'method': 'command_line'}), 200

    # Method 2: lsof fallback
    path = _detect_log_path_from_lsof(pid)
    if path:
        logger.info(f"Detected log path via lsof: {path}")
        return jsonify({'detected': True, 'path': path, 'method': 'lsof'}), 200

    return jsonify({
        'detected': False,
        'message': 'Could not determine log path. Mongosync may be logging to stdout. Enter path manually.'
    }), 200


@app.route('/mongosync_running', methods=['GET'])
def mongosync_running():
    """Check if mongosync process is running. Returns {"running": true/false}."""
    pid, _ = _find_mongosync_pid()
    return jsonify({'running': pid is not None}), 200


@app.route('/launch_mongosync', methods=['POST'])
def launch_mongosync():
    """
    Launch mongosync process with config from request body.
    Writes config.json in logpath directory. Mongosync writes mongosync.log in the same directory.
    """
    try:
        data = request.get_json() or {}
        cluster0 = (data.get('cluster0') or '').strip()
        cluster1 = (data.get('cluster1') or '').strip()
        loadlevel = data.get('loadlevel', 1)
        logpath = (data.get('logpath') or '').strip()
        verbosity = (data.get('verbosity') or 'INFO').strip()
        namespace_filter = data.get('namespaceFilter')
        build_indexes = data.get('buildIndexes')
        reversible = data.get('reversible')
        detect_random_id = data.get('detectRandomId')
        write_blocking_mode = data.get('writeBlockingMode')
        embedded_verifier = data.get('embeddedVerifier')

        if not cluster0 or not cluster1:
            return jsonify({'error': 'cluster0 and cluster1 are required'}), 400

        if logpath:
            ok, err = validate_logpath_write_permission(logpath)
            if not ok:
                return jsonify({'error': err}), 400

        pid, _ = _find_mongosync_pid()
        if pid:
            return jsonify({'error': 'Mongosync is already running'}), 400

        log_file_path, err = _write_config_and_launch(
            cluster0, cluster1, loadlevel, logpath, verbosity, namespace_filter,
            build_indexes=build_indexes, reversible=reversible, detect_random_id=detect_random_id,
            write_blocking_mode=write_blocking_mode, embedded_verifier=embedded_verifier
        )
        if err:
            return err

        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if session_id:
            session_data = session_store.get_session(session_id)
            if session_data:
                detected_pid, _ = _find_mongosync_pid()
                session_data['log_file_path'] = log_file_path
                session_data['cluster0'] = cluster0
                session_data['cluster1'] = cluster1
                session_data['mongosync_pid'] = detected_pid
                session_store.update_session(session_id, session_data)
                logger.info(f"Updated session with log_file_path: {log_file_path}")

        return jsonify({
            'success': True,
            'message': f'Mongosync launched successfully. Logs: {os.path.dirname(log_file_path)}',
            'log_file_path': log_file_path
        }), 200

    except Exception as e:
        logger.error(f"launch_mongosync error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/apply_mongosync_settings', methods=['POST'])
def apply_mongosync_settings():
    """
    Apply mongosync settings (verbosity, loadlevel). If mongosync is running,
    gracefully pause, wait for PAUSED state, kill, wait for exit, then relaunch with new config.
    If not running, update config.json for next launch.
    """
    try:
        data = request.get_json() or {}
        cluster0 = (data.get('cluster0') or '').strip()
        cluster1 = (data.get('cluster1') or '').strip()
        loadlevel = data.get('loadlevel', 1)
        logpath = (data.get('logpath') or '').strip()
        verbosity = (data.get('verbosity') or 'INFO').strip()
        namespace_filter = data.get('namespaceFilter')
        build_indexes = data.get('buildIndexes')
        reversible = data.get('reversible')
        detect_random_id = data.get('detectRandomId')
        write_blocking_mode = data.get('writeBlockingMode')
        embedded_verifier = data.get('embeddedVerifier')

        if not cluster0 or not cluster1:
            return jsonify({'error': 'cluster0 and cluster1 are required. Configure in Setup first.'}), 400

        if logpath:
            ok, err = validate_logpath_write_permission(logpath)
            if not ok:
                return jsonify({'error': err}), 400

        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return jsonify({'error': 'No session found'}), 400

        session_data = session_store.get_session(session_id)
        if not session_data:
            return jsonify({'error': 'Invalid session'}), 400

        session_data['cluster0'] = cluster0
        session_data['cluster1'] = cluster1
        session_store.update_session(session_id, session_data)

        mongosync_host = session_data.get('mongosync_hostname')
        pid, _ = _find_mongosync_pid()

        if pid:
            if not mongosync_host:
                return jsonify({
                    'error': 'Mongosync is running but hostname is not configured. '
                             'Use Setup to configure mongosync host before applying live restart settings.'
                }), 400
            # Check if Mongosync Settings actually changed - if not, skip relaunch
            logpath_for_compare = (logpath or '').strip() or os.path.dirname(session_data.get('log_file_path') or '')
            config_path = os.path.join(os.path.abspath(logpath_for_compare), 'config.json') if (logpath_for_compare and logpath_for_compare.strip()) else None
            if config_path and os.path.isfile(config_path):
                try:
                    with open(config_path, 'r') as f:
                        current_config = json.load(f)
                    try:
                        loadlevel_int = int(loadlevel) if loadlevel is not None else 1
                        if loadlevel_int < 1 or loadlevel_int > 4:
                            loadlevel_int = 1
                    except (ValueError, TypeError):
                        loadlevel_int = 1
                    current_logpath = current_config.get('logPath', '')
                    new_logpath_abs = os.path.abspath(logpath_for_compare) if logpath_for_compare else ''
                    paths_match = (not current_logpath and not new_logpath_abs) or (
                        current_logpath and new_logpath_abs and
                        os.path.normpath(current_logpath) == os.path.normpath(new_logpath_abs)
                    )
                    nf_match = json.dumps(current_config.get('namespaceFilter') or {}, sort_keys=True) == json.dumps(namespace_filter or {}, sort_keys=True)
                    build_idx_match = (build_indexes is None) or (current_config.get('buildIndexes') == build_indexes)
                    rev_match = (reversible is None) or (current_config.get('reversible') == bool(reversible))
                    det_match = (detect_random_id is None) or (current_config.get('detectRandomId') == bool(detect_random_id))
                    wbm_match = (write_blocking_mode is None) or (current_config.get('writeBlockingMode') == write_blocking_mode)
                    ev_match = (embedded_verifier is None) or (str(current_config.get('verificationmode', '')).strip() == str(embedded_verifier).strip())

                    # Settings that require restart to apply while mongosync is already running.
                    restart_required_settings_match = (
                        current_config.get('cluster0') == cluster0 and
                        current_config.get('cluster1') == cluster1 and
                        current_config.get('loadLevel') == loadlevel_int and
                        paths_match and
                        current_config.get('verbosity', 'INFO').strip() == (verbosity or 'INFO').strip() and
                        det_match
                    )

                    # Settings that should never trigger restart while running.
                    no_restart_settings_match = (
                        nf_match and
                        build_idx_match and
                        rev_match and
                        wbm_match and
                        ev_match
                    )

                    if restart_required_settings_match:
                        if no_restart_settings_match:
                            return jsonify({
                                'success': True,
                                'relaunched': False,
                                'message': 'No changes in Mongosync Settings. Mongosync was not restarted.'
                            }), 200

                        # Persist START-time settings for future use without restarting mongosync.
                        if namespace_filter and (namespace_filter.get('inclusionFilter') or namespace_filter.get('exclusionFilter')):
                            current_config['namespaceFilter'] = namespace_filter
                        else:
                            current_config.pop('namespaceFilter', None)
                        if build_indexes is not None and build_indexes in ('afterDataCopy', 'beforeDataCopy', 'never'):
                            current_config['buildIndexes'] = build_indexes
                        if reversible is not None:
                            current_config['reversible'] = bool(reversible)
                        if write_blocking_mode is not None and write_blocking_mode in ('destinationOnly', 'sourceAndDestination', 'none'):
                            current_config['writeBlockingMode'] = write_blocking_mode
                        if embedded_verifier is not None and embedded_verifier:
                            current_config['verificationmode'] = str(embedded_verifier).strip()

                        with open(config_path, 'w') as f:
                            json.dump(current_config, f, indent=2)

                        return jsonify({
                            'success': True,
                            'relaunched': False,
                            'message': 'START-endpoint settings were saved. Mongosync was not restarted.'
                        }), 200
                except (OSError, json.JSONDecodeError) as e:
                    logger.warning(f"Could not read/parse config for change detection: {e}")

            # Mongosync is running and config changed: pause (unless IDLE/INITIALIZING) -> kill -> wait exit -> relaunch
            current_state, _ = _fetch_mongosync_state(mongosync_host)
            state_upper = (current_state or '').upper()
            skip_pause = state_upper in ('IDLE', 'INITIALIZING', 'COMMITTED')

            if not skip_pause:
                try:
                    pause_url = f'http://{mongosync_host}/api/v1/pause'
                    pause_req = Request(pause_url, data=b'{}', method='POST')
                    pause_req.add_header('Content-Type', 'application/json')
                    with urlopen(pause_req, timeout=5) as _:
                        logger.info("Pause request sent, polling for PAUSED state")
                except Exception as pause_err:
                    logger.warning(f"Failed to send pause request: {pause_err}")
                    return jsonify({'error': f'Failed to pause mongosync: {pause_err}'}), 500

                # Poll until PAUSED or timeout (60s)
                pause_timeout = 60
                poll_interval = 1
                deadline = time.monotonic() + pause_timeout
                while time.monotonic() < deadline:
                    state, err = _fetch_mongosync_state(mongosync_host)
                    if state and state.upper() == 'PAUSED':
                        logger.info("Mongosync reached PAUSED state")
                        break
                    if err:
                        logger.warning(f"State poll error: {err}")
                    time.sleep(poll_interval)
                else:
                    return jsonify({
                        'error': f'Mongosync did not reach PAUSED state within {pause_timeout}s. Aborting.'
                    }), 504
            else:
                logger.info(f"Mongosync in {state_upper} state, skipping pause step")

            # Kill process
            mongosync_pid = session_data.get('mongosync_pid') or pid
            try:
                result = subprocess.run(
                    ['kill', '-15', str(mongosync_pid)],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode != 0:
                    return jsonify({'error': f'Failed to kill mongosync: {result.stderr or "Unknown error"}'}), 500
            except subprocess.TimeoutExpired:
                return jsonify({'error': 'Kill command timed out'}), 500

            session_data['mongosync_pid'] = None
            session_store.update_session(session_id, session_data)

            # Wait for process exit (30s)
            if not _wait_for_process_exit(timeout_sec=30):
                return jsonify({'error': 'Mongosync process did not exit within 30s. Do not relaunch.'}), 504

            # Relaunch with new config (derive logpath from session if not provided)
            logpath_for_launch = logpath or os.path.dirname(session_data.get('log_file_path') or '')
            log_file_path, err = _write_config_and_launch(
                cluster0, cluster1, loadlevel, logpath_for_launch, verbosity, namespace_filter,
                build_indexes=build_indexes, reversible=reversible, detect_random_id=detect_random_id,
                write_blocking_mode=write_blocking_mode, embedded_verifier=embedded_verifier
            )
            if err:
                return err

            session_data = session_store.get_session(session_id)
            if session_data:
                new_pid, _ = _find_mongosync_pid()
                session_data['log_file_path'] = log_file_path
                session_data['mongosync_pid'] = new_pid
                session_store.update_session(session_id, session_data)

            msg = 'Mongosync settings applied. Mongosync was restarted with new config.'
            if not skip_pause:
                msg = 'Mongosync settings applied. Mongosync was paused, restarted with new config.'
            return jsonify({
                'success': True,
                'relaunched': True,
                'message': msg,
                'log_file_path': log_file_path
            }), 200

        # Mongosync not running: optionally write config and update session log path for next launch/streaming
        logpath_to_use = logpath or (os.path.dirname(session_data.get('log_file_path') or '') or None)
        if logpath_to_use:
            logpath_abs = os.path.abspath(logpath_to_use)
            try:
                os.makedirs(logpath_abs, mode=0o755, exist_ok=True)
                config_path = os.path.join(logpath_abs, 'config.json')
                try:
                    loadlevel_int = int(loadlevel) if loadlevel is not None else 1
                    if loadlevel_int < 1 or loadlevel_int > 4:
                        loadlevel_int = 1
                except (ValueError, TypeError):
                    loadlevel_int = 1
                config = {
                    'cluster0': cluster0,
                    'cluster1': cluster1,
                    'loadLevel': loadlevel_int,
                    'logPath': logpath_abs,
                    'verbosity': (verbosity or 'INFO').strip()
                }
                if namespace_filter and (namespace_filter.get('inclusionFilter') or namespace_filter.get('exclusionFilter')):
                    config['namespaceFilter'] = namespace_filter
                if build_indexes is not None and build_indexes in ('afterDataCopy', 'beforeDataCopy', 'never'):
                    config['buildIndexes'] = build_indexes
                if reversible is not None:
                    config['reversible'] = bool(reversible)
                if detect_random_id is not None:
                    config['detectRandomId'] = bool(detect_random_id)
                if write_blocking_mode is not None and write_blocking_mode in ('destinationOnly', 'sourceAndDestination', 'none'):
                    config['writeBlockingMode'] = write_blocking_mode
                if embedded_verifier is not None and embedded_verifier:
                    config['verificationmode'] = str(embedded_verifier).strip()
                with open(config_path, 'w') as f:
                    json.dump(config, f, indent=2)
                # Update session with log_file_path so streaming can work
                log_file_path = os.path.join(logpath_abs, 'mongosync.log')
                session_data = session_store.get_session(session_id)
                if session_data:
                    session_data['log_file_path'] = log_file_path
                    session_store.update_session(session_id, session_data)
                return jsonify({
                    'success': True,
                    'relaunched': False,
                    'log_file_path': log_file_path,
                    'message': 'Mongosync settings saved. Apply when mongosync is running to restart with new config.'
                }), 200
            except OSError as e:
                logger.warning(f"Could not write config for apply (mongosync not running): {e}")

        return jsonify({
            'success': True,
            'relaunched': False,
            'message': 'Mongosync settings saved. Apply when mongosync is running to restart with new config.'
        }), 200

    except Exception as e:
        logger.error(f"apply_mongosync_settings error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/find_mongosync_pid', methods=['GET'])
def find_mongosync_pid():
    """
    Find the mongosync process ID using ps command and store in session.
    """
    mongosync_pid, err = _find_mongosync_pid()
    if err:
        logger.warning(err)
        status = 404 if 'not found' in err.lower() else 500
        return jsonify({'pid': None, 'message': err, 'error': err}), status

    # Store PID in session
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        session_data = session_store.get_session(session_id)
        if session_data:
            session_data['mongosync_pid'] = mongosync_pid
            session_store.update_session(session_id, session_data)
            logger.info(f"Stored mongosync PID {mongosync_pid} in session")

    return jsonify({'pid': mongosync_pid, 'message': f'Found mongosync process with PID {mongosync_pid}'}), 200


@app.route('/kill_mongosync', methods=['POST'])
def kill_mongosync():
    """
    Pause mongosync then send kill -15 (SIGTERM) to the mongosync process.
    """
    try:
        # Get PID from session
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return jsonify({'error': 'No session found'}), 400
        
        session_data = session_store.get_session(session_id)
        if not session_data:
            return jsonify({'error': 'Invalid session'}), 400
        
        mongosync_pid = session_data.get('mongosync_pid')
        if not mongosync_pid:
            logger.info("No PID in session, attempting to find mongosync process")
            mongosync_pid, _ = _find_mongosync_pid()
            if mongosync_pid:
                session_data['mongosync_pid'] = mongosync_pid
                session_store.update_session(session_id, session_data)
                logger.info(f"Found and stored mongosync PID: {mongosync_pid}")

        if not mongosync_pid:
            return jsonify({'error': 'Mongosync process ID not found. Process may not be running.'}), 404
        
        # First, try to pause mongosync gracefully (except COMMITTED state)
        mongosync_host = session_data.get('mongosync_hostname')
        if mongosync_host:
            try:
                current_state, _ = _fetch_mongosync_state(mongosync_host)
                if (current_state or '').upper() == 'COMMITTED':
                    logger.info("Mongosync is COMMITTED, skipping pause before kill")
                else:
                    pause_url = f'http://{mongosync_host}/api/v1/pause'
                    pause_req = Request(pause_url, data=b'{}', method='POST')
                    pause_req.add_header('Content-Type', 'application/json')

                    with urlopen(pause_req, timeout=5) as response:
                        logger.info(f"Paused mongosync before killing (status: {response.status})")
            except Exception as pause_error:
                # Log but continue with kill even if pause fails
                logger.warning(f"Failed to pause mongosync before kill: {pause_error}")
        
        # Send kill -15 (SIGTERM) to the process
        try:
            result = subprocess.run(
                ['kill', '-15', str(mongosync_pid)],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully sent SIGTERM to mongosync process {mongosync_pid}")
                # Clear PID from session since process is being terminated
                session_data['mongosync_pid'] = None
                session_store.update_session(session_id, session_data)
                
                return jsonify({
                    'message': f'SIGTERM (kill -15) sent to mongosync process {mongosync_pid}',
                    'pid': mongosync_pid,
                    'status': 'success'
                }), 200
            else:
                error_msg = result.stderr if result.stderr else 'Unknown error'
                err_lower = (error_msg or '').lower()
                # Race condition: process may exit between PID discovery and kill signal.
                # Treat "No such process" as successful termination.
                if 'no such process' in err_lower or 'not found' in err_lower:
                    logger.info(f"Process {mongosync_pid} already terminated before SIGTERM; treating as success")
                    session_data['mongosync_pid'] = None
                    session_store.update_session(session_id, session_data)
                    return jsonify({
                        'message': f'Mongosync process {mongosync_pid} was already terminated',
                        'pid': mongosync_pid,
                        'status': 'success'
                    }), 200
                logger.error(f"Failed to kill process {mongosync_pid}: {error_msg}")
                return jsonify({'error': f'Failed to kill process: {error_msg}', 'pid': mongosync_pid}), 500
                
        except subprocess.TimeoutExpired:
            logger.error(f"kill command timed out for PID {mongosync_pid}")
            return jsonify({'error': 'Kill command timed out', 'pid': mongosync_pid}), 500
            
    except Exception as e:
        logger.error(f"Error killing mongosync: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/stream_log')
def stream_log():
    """Server-Sent Events endpoint for streaming log data."""
    logger.info("stream_log endpoint called")
    
    # Get log file path from env var or session
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    logger.info(f"Session ID from cookie: {session_id}")
    session_data = session_store.get_session(session_id) if session_id else {}
    logger.info(f"Session data: {session_data}")
    
    log_file_path = LOG_FILE_PATH if LOG_FILE_PATH else session_data.get('log_file_path', '')
    logger.info(f"LOG_FILE_PATH (env): {LOG_FILE_PATH}")
    logger.info(f"Session log_file_path: {session_data.get('log_file_path', 'Not set')}")
    logger.info(f"Final log_file_path: {log_file_path}")
    
    if not log_file_path:
        logger.warning("No log file path configured for streaming")
        def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Log file path not configured'})}\n\n"
        return Response(stream_with_context(error_stream()), 
                       mimetype='text/event-stream',
                       headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
    
    # Get interval from query param, session, or default
    requested_interval = request.args.get('interval', type=float)
    
    if requested_interval and 1 <= requested_interval <= 15:
        interval = requested_interval
    elif STREAM_UPDATE_INTERVAL:
        interval = STREAM_UPDATE_INTERVAL
    elif session_data.get('stream_interval'):
        interval = session_data.get('stream_interval')
    else:
        interval = 1.0
    
    try:
        # Get or create monitor instance with configured interval
        logger.info(f"Creating/getting monitor for {log_file_path} with interval {interval}")
        monitor = get_monitor(log_file_path, interval, STREAM_MAX_BUFFER_SIZE)
        logger.info(f"Monitor created/retrieved successfully")
        
        def event_stream():
            import time
            
            # Track last sync position for this client (including raw lines)
            client_sync_position = {}
            client_raw_lines_position = 0
            
            # Send initial data (all current metrics and raw lines)
            try:
                all_metrics = monitor.get_all_metrics()
                all_raw_lines = monitor.get_all_raw_lines()
                client_sync_position = monitor.get_sync_position()
                client_raw_lines_position = monitor.get_raw_lines_count()
                
                logger.debug(f"Sending initial data: {sum(len(v) for v in all_metrics.values())} metrics, {len(all_raw_lines)} raw lines")
                yield f"data: {json.dumps({'type': 'data', 'timestamp': datetime.now(timezone.utc).isoformat(), 'metrics': all_metrics, 'raw_lines': all_raw_lines})}\n\n"
            except Exception as e:
                logger.error(f"Error sending initial data: {e}", exc_info=True)
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
            
            # Stream updates
            while True:
                try:
                    # Get new metrics since last sync
                    new_metrics = monitor.get_new_metrics(client_sync_position)
                    new_raw_lines = monitor.get_new_raw_lines(client_raw_lines_position)
                    
                    # Check if there's any new data
                    has_new_data = any(new_metrics.values()) or len(new_raw_lines) > 0
                    
                    if has_new_data:
                        # Update sync position
                        current_position = monitor.get_sync_position()
                        current_raw_lines_count = monitor.get_raw_lines_count()
                        
                        # Log new data received
                        new_counts = {k: len(v) for k, v in new_metrics.items() if v}
                        logger.debug(f"Sending new metrics: {new_counts}, raw lines: {len(new_raw_lines)}")
                        
                        # Send new metrics and raw lines
                        yield f"data: {json.dumps({'type': 'data', 'timestamp': datetime.now(timezone.utc).isoformat(), 'metrics': new_metrics, 'raw_lines': new_raw_lines})}\n\n"
                        
                        # Update last sync position
                        client_sync_position = current_position
                        client_raw_lines_position = current_raw_lines_count
                    else:
                        # Send heartbeat to keep connection alive (less frequently)
                        yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"
                    
                    # Wait before next check
                    time.sleep(STREAM_UPDATE_INTERVAL)
                    
                except GeneratorExit:
                    # Client disconnected
                    logger.info("Client disconnected from stream")
                    break
                except Exception as e:
                    logger.error(f"Error in stream: {e}", exc_info=True)
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                    time.sleep(STREAM_UPDATE_INTERVAL)
        
        return Response(stream_with_context(event_stream()), 
                       mimetype='text/event-stream',
                       headers={
                           'Cache-Control': 'no-cache',
                           'X-Accel-Buffering': 'no',
                           'Connection': 'keep-alive'
                       })
    
    except Exception as e:
        logger.error(f"Error setting up stream: {e}")
        def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        return Response(stream_with_context(error_stream()), 
                       mimetype='text/event-stream',
                       headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

if __name__ == '__main__':
    # Log startup information
    app_info = get_app_info()
    logger.info(f"Starting {app_info['name']} v{app_info['version']}")
    logger.info(f"Log file: {app_info['log_file']}")
    logger.info(f"Server: {app_info['host']}:{app_info['port']}")
    
    # Import SSL config
    from app_config import SSL_ENABLED, SSL_CERT_PATH, SSL_KEY_PATH
    
    # Run the Flask app with or without SSL
    if SSL_ENABLED:
        import ssl
        import os
        
        # Verify certificate files exist
        if not os.path.exists(SSL_CERT_PATH):
            logger.error(f"SSL certificate not found: {SSL_CERT_PATH}")
            logger.error("Please provide a valid SSL certificate or set MI_SSL_ENABLED=false")
            exit(1)
        if not os.path.exists(SSL_KEY_PATH):
            logger.error(f"SSL key not found: {SSL_KEY_PATH}")
            logger.error("Please provide a valid SSL private key or set MI_SSL_ENABLED=false")
            exit(1)
        
        # Create SSL context
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(SSL_CERT_PATH, SSL_KEY_PATH)
        
        logger.info("HTTPS enabled - Starting with SSL/TLS encryption")
        logger.info(f"SSL Certificate: {SSL_CERT_PATH}")
        app.run(host=HOST, port=PORT, ssl_context=context)
    else:
        logger.warning("HTTPS disabled - Starting with HTTP (insecure)")
        logger.warning("For production use, enable HTTPS by setting MI_SSL_ENABLED=true")
        app.run(host=HOST, port=PORT)
