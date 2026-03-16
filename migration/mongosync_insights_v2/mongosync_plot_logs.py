import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
from plotly.subplots import make_subplots
from tqdm import tqdm
from flask import request, render_template, jsonify
import json
from datetime import datetime, timezone
from dateutil import parser
import re
import logging
import os
import magic
from werkzeug.utils import secure_filename
from mongosync_plot_utils import format_byte_size, convert_bytes
from app_config import MAX_FILE_SIZE, ALLOWED_EXTENSIONS, ALLOWED_MIME_TYPES, LOG_FILE_PATH
from file_decompressor import decompress_file, is_compressed_mime_type


# Read-only mongosync options displayed in Settings (from log file, not editable)
MONGOSYNC_READONLY_OPTION_KEYS = [
    'disableMetricsLogging', 'disableTelemetry', 'disableVerification',
    'isMultipleReplicatorConfiguration', 'retryDurationLimit', 'longRetryDurationLimit',
    'retryRandomly', 'pprofPort'
]

# Keys to exclude from log metrics mongosync options table (moved to Settings or redundant)
# Include both display names and log keys for options that are renamed
MONGOSYNC_OPTIONS_EXCLUDE_FROM_LOG_METRICS = [
    'cluster0', 'cluster1', 'logpath', 'logPath', 'port', 'verbosity', 'message',
    'disableVerification', 'disableverification',
    'disableMetricsLogging', 'disableTelemetry', 'isMultipleReplicatorConfiguration',
    'disablemetricslogging', 'disabletelemetry', 'ismultiplereplicatorconfiguration',
    'retryDurationLimit', 'longRetryDurationLimit', 'retryRandomly', 'pprofPort',
    'retryrandomonly', 'pporfport', 'retrydurationlimit', 'longretrydurationlimit',
    'mongosyncID', 'mongosyncid', 'serverID', 'serverid', 'id'
]

# Map display keys to log file keys (mongosync uses different names in logs)
MONGOSYNC_READONLY_LOG_KEY_MAP = {
    'disableMetricsLogging': 'disablemetricslogging',
    'disableTelemetry': 'disabletelemetry',
    'disableVerification': 'disableverification',
    'isMultipleReplicatorConfiguration': 'ismultiplereplicatorconfiguration',
    'retryRandomly': 'retryrandomonly',
    'pprofPort': 'pporfport',
    'retryDurationLimit': 'retrydurationlimit',
    'longRetryDurationLimit': 'longretrydurationlimit',
}


def _safe_float(value):
    """Convert numeric-like values to float, otherwise return None."""
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_operation_stat(item, metric_group, metric_field):
    """
    Extract operation duration stat value from known log shapes.
    Supports top-level keys and nested attr/attributes containers.
    """
    if not item or not isinstance(item, dict):
        return None

    metric_group_aliases = {
        'CollectionCopySourceRead': (
            'CollectionCopySourceRead', 'CollectionCopySourceReads', 'CollectionCopySource',
            'CollectionCopySrcRead', 'CollectionCopySrcReads'
        ),
        'CollectionCopyDestinationWrite': (
            'CollectionCopyDestinationWrite', 'CollectionCopyDestinationWrites', 'CollectionCopyDestination',
            'CollectionCopyDstWrite', 'CollectionCopyDstWrites', 'CollectionCopyDestWrite', 'CollectionCopyDestWrites'
        ),
        'CEASourceRead': (
            'CEASourceRead', 'CEASourceReads', 'CEASource',
            'ChangeEventApplicationSourceRead', 'ChangeEventApplicationSourceReads'
        ),
        'CEADestinationWrite': (
            'CEADestinationWrite', 'CEADestinationWrites', 'CEADestination',
            'ChangeEventApplicationDestinationWrite', 'ChangeEventApplicationDestinationWrites',
            'CEADestWrite', 'CEADestWrites'
        ),
    }
    group_keys = metric_group_aliases.get(metric_group, (metric_group,))

    metric_field_aliases = {
        'averageDurationMs': ('averageDurationMs', 'avgDurationMs', 'avgDuration', 'averageMs', 'avgMs', 'meanDurationMs'),
        'maximumDurationMs': ('maximumDurationMs', 'maxDurationMs', 'maxDuration', 'maximumMs', 'maxMs', 'peakDurationMs'),
        'numOperations': ('numOperations', 'operations', 'operationCount', 'numOps', 'opCount', 'count', 'writeCount'),
    }
    field_keys = metric_field_aliases.get(metric_field, (metric_field,))

    def normalize_key(value):
        return re.sub(r'[^a-z0-9]', '', str(value).lower())

    def group_semantic_match(group_name, candidate_key):
        normalized = normalize_key(candidate_key)
        token_sets = {
            'CollectionCopySourceRead': ('collectioncopy', 'source', 'read'),
            'CollectionCopyDestinationWrite': ('collectioncopy', 'destination', 'write'),
            'CEASourceRead': (('cea', 'changeeventapplication'), 'source', 'read'),
            'CEADestinationWrite': (('cea', 'changeeventapplication'), 'destination', 'write'),
        }
        tokens = token_sets.get(group_name)
        if not tokens:
            return False
        first = tokens[0]
        if isinstance(first, tuple):
            first_ok = any(token in normalized for token in first)
        else:
            first_ok = first in normalized
        return first_ok and all(token in normalized for token in tokens[1:])

    def get_value_case_insensitive(d, keys):
        if not isinstance(d, dict):
            return None
        # Exact keys first for fast path.
        for k in keys:
            if k in d:
                return d.get(k)
        # Fallback to case-insensitive matching.
        lowered = {str(k).lower(): v for k, v in d.items()}
        for k in keys:
            v = lowered.get(str(k).lower())
            if v is not None:
                return v
        return None

    def append_container(containers, candidate):
        if isinstance(candidate, dict):
            containers.append(candidate)
            nested_stats = candidate.get('operationDurationStats')
            if isinstance(nested_stats, dict):
                containers.append(nested_stats)

    containers = [item]
    attr = item.get('attr')
    if isinstance(attr, dict):
        append_container(containers, attr)

    attributes = item.get('attributes')
    if isinstance(attributes, dict):
        append_container(containers, attributes)

    top_level_op_stats = item.get('operationDurationStats')
    if isinstance(top_level_op_stats, dict):
        containers.append(top_level_op_stats)

    # Some mongosync builds nest stats under additional wrappers.
    wrapper_keys = ('payload', 'data', 'metrics', 'stats', 'details', 'detail', 'context', 'progress')
    for base in (item, attr, attributes):
        if not isinstance(base, dict):
            continue
        for wrapper_key in wrapper_keys:
            append_container(containers, base.get(wrapper_key))

    for container in containers:
        if not isinstance(container, dict):
            continue
        for group_key in group_keys:
            group = get_value_case_insensitive(container, (group_key,))
            if isinstance(group, dict):
                value = get_value_case_insensitive(group, field_keys)
                if value is not None:
                    return value
        # Fallback: semantic key match for destination/source groups.
        for candidate_key, candidate_val in container.items():
            if isinstance(candidate_val, dict) and group_semantic_match(metric_group, candidate_key):
                value = get_value_case_insensitive(candidate_val, field_keys)
                if value is not None:
                    return value
    return None


def _extract_operation_stat_series(mongosync_ops_stats, metric_group, metric_field):
    """Keep series index-aligned with operation stat entries (None for missing values)."""
    return [_safe_float(_extract_operation_stat(item, metric_group, metric_field)) for item in mongosync_ops_stats]


def _extract_operation_stat_times(mongosync_ops_stats, fallback_times):
    """Extract operation-stat timestamps aligned with stat entries."""
    op_times = []
    for idx, item in enumerate(mongosync_ops_stats):
        parsed_time = None
        if isinstance(item, dict):
            raw_time = item.get('time')
            if raw_time:
                try:
                    parsed_time = datetime.strptime(str(raw_time)[:26], "%Y-%m-%dT%H:%M:%S.%f")
                except (ValueError, TypeError, IndexError):
                    parsed_time = None
        if parsed_time is None and idx < len(fallback_times):
            parsed_time = fallback_times[idx]
        op_times.append(parsed_time)

    if op_times and all(t is None for t in op_times):
        return list(range(len(op_times)))
    return op_times


def _log_destination_write_shape_debug(logger, mongosync_ops_stats, series_name_to_values, sample_size=25):
    """Debug-only key-shape diagnostics when destination-write series are empty."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    if not mongosync_ops_stats:
        return
    any_destination_data = any(
        any(value is not None for value in values)
        for values in series_name_to_values.values()
    )
    if any_destination_data:
        return

    group_keys = set()
    field_keys = set()
    for item in mongosync_ops_stats[:sample_size]:
        if not isinstance(item, dict):
            continue
        candidate_containers = [item]
        for base_key in ('attr', 'attributes', 'operationDurationStats', 'payload', 'data', 'metrics', 'stats', 'details', 'progress'):
            base_val = item.get(base_key)
            if isinstance(base_val, dict):
                candidate_containers.append(base_val)
                nested = base_val.get('operationDurationStats')
                if isinstance(nested, dict):
                    candidate_containers.append(nested)
        for container in candidate_containers:
            if not isinstance(container, dict):
                continue
            for key, value in container.items():
                if isinstance(value, dict):
                    group_keys.add(str(key))
                    for field_key in value.keys():
                        field_keys.add(str(field_key))

    logger.debug(
        "Destination write metrics missing; sampled operation stat keys. groups=%s fields=%s",
        sorted(group_keys)[:40],
        sorted(field_keys)[:40],
    )


def get_mongosync_readonly_options(log_file_path=None):
    """
    Extract read-only mongosync options from log file for display in Settings.
    Returns dict with keys from MONGOSYNC_READONLY_OPTION_KEYS; values are from log or None.
    """
    logger = logging.getLogger(__name__)
    result = {k: None for k in MONGOSYNC_READONLY_OPTION_KEYS}
    file_path = log_file_path or LOG_FILE_PATH
    if not file_path or not os.path.exists(file_path):
        return result
    pattern = re.compile(r"Mongosync Options", re.IGNORECASE)
    try:
        with open(file_path, 'rb') as f:
            mime = magic.Magic(mime=True)
            sample = f.read(2048)
            file_mime_type = mime.from_buffer(sample)
            f.seek(0)
            if is_compressed_mime_type(file_mime_type):
                it = decompress_file(f, file_mime_type, os.path.basename(file_path))
            else:
                it = f
            for line in it:
                if isinstance(line, bytes):
                    line = line.decode('utf-8', errors='replace')
                line = line.strip()
                if not line or not line.startswith('{'):
                    continue
                try:
                    obj = json.loads(line)
                    if pattern.search(obj.get('message', '')):
                        obj_lower = {k.lower(): v for k, v in obj.items()}
                        for key in MONGOSYNC_READONLY_OPTION_KEYS:
                            # Try mapped log key first (legacy/alternate), then display key lowercased
                            log_keys = [MONGOSYNC_READONLY_LOG_KEY_MAP.get(key), key.lower()]
                            for log_key in log_keys:
                                if log_key and log_key in obj_lower:
                                    result[key] = obj_lower[log_key]
                                    break
                        break
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning(f"Could not read mongosync options from log: {e}")
    return result


def _categorize_hidden_option(key: str) -> str:
    """Categorize hidden option key into collectionCopy, CEA, or Other."""
    k = key.lower()
    if any(x in k for x in ('collection', 'copy', 'batch', 'document')):
        return 'collectionCopy'
    if any(x in k for x in ('cea', 'change', 'event', 'application')):
        return 'CEA'
    return 'Other'


def get_mongosync_hidden_options(log_file_path=None):
    """
    Extract Mongosync Hidden Options from log file for display in Settings.
    Returns dict with keys collectionCopy, CEA, Other; each maps to {key: value}.
    """
    logger = logging.getLogger(__name__)
    result = {'collectionCopy': {}, 'CEA': {}, 'Other': {}}
    file_path = log_file_path or LOG_FILE_PATH
    if not file_path or not os.path.exists(file_path):
        return result
    pattern = re.compile(r"Mongosync HiddenFlags", re.IGNORECASE)
    try:
        with open(file_path, 'rb') as f:
            mime = magic.Magic(mime=True)
            sample = f.read(2048)
            file_mime_type = mime.from_buffer(sample)
            f.seek(0)
            if is_compressed_mime_type(file_mime_type):
                it = decompress_file(f, file_mime_type, os.path.basename(file_path))
            else:
                it = f
            for line in it:
                if isinstance(line, bytes):
                    line = line.decode('utf-8', errors='replace')
                line = line.strip()
                if not line or not line.startswith('{'):
                    continue
                try:
                    obj = json.loads(line)
                    if pattern.search(obj.get('message', '')):
                        for k, v in obj.items():
                            if k in ('time', 'level'):
                                continue
                            category = _categorize_hidden_option(k)
                            result[category][k] = v
                        break
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning(f"Could not read mongosync hidden options from log: {e}")
    return result


def analyze_streaming_log(log_file_path=None):
    """
    Analyze the streaming log file and return plot data.
    This is used for live updates during streaming.
    
    Args:
        log_file_path: Path to the log file. If not provided, uses LOG_FILE_PATH from config.
    """
    logger = logging.getLogger(__name__)
    
    # Use provided path or fall back to config
    file_path = log_file_path if log_file_path else LOG_FILE_PATH
    
    if not file_path or not os.path.exists(file_path):
        logger.error(f"Log file path not configured or file doesn't exist: {file_path}")
        return jsonify({"error": "Streaming log file not configured or not found"}), 400
    
    try:
        # Open and read the log file
        with open(file_path, 'rb') as file:
            # Detect MIME type
            mime = magic.Magic(mime=True)
            file_sample = file.read(2048)
            file_mime_type = mime.from_buffer(file_sample)
            file.seek(0)
            
            logger.info(f"Analyzing streaming log file: {file_path} (MIME: {file_mime_type})")
            
            # Parse the log file
            plot_json, log_lines = parse_log_file_to_json(file, file_mime_type, os.path.basename(file_path))
            
            return jsonify({
                "success": True,
                "plot_json": json.loads(plot_json)
            })
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error analyzing streaming log file: {e}\n{error_trace}")
        return jsonify({"error": f"Error analyzing log file: {str(e)}"}), 400


def upload_file_json(session_store=None, session_cookie_name=None):
    """
    AJAX endpoint for log file upload that returns JSON with plot data.
    Returns JSON response instead of rendering a template.
    
    Args:
        session_store: Session store instance for storing uploaded file path
        session_cookie_name: Cookie name for session ID
    """
    logger = logging.getLogger(__name__)
    
    # Check if a file was uploaded
    if 'file' not in request.files:
        logger.error("No file was uploaded")
        return jsonify({"error": "No file was selected for upload."}), 400

    file = request.files['file']

    if file.filename == '':
        logger.error("Empty file without a filename")
        return jsonify({"error": "Please select a file to upload."}), 400

    if file:
        # Validate filename and extension
        filename = secure_filename(file.filename)
        if not filename:
            logger.error("Invalid filename")
            return jsonify({"error": "Invalid filename. Please use a valid file name."}), 400
        
        # Check file extension
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            logger.error(f"Invalid file extension: {file_ext}")
            return jsonify({"error": f"File type '{file_ext}' is not allowed. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}"}), 400
        
        # Check file size
        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)
        
        if file_size > MAX_FILE_SIZE:
            logger.error(f"File too large: {file_size} bytes")
            max_size_mb = MAX_FILE_SIZE / (1024 * 1024)
            actual_size_mb = file_size / (1024 * 1024)
            return jsonify({"error": f"File size ({actual_size_mb:.1f} MB) exceeds maximum allowed size ({max_size_mb:.1f} MB)."}), 400
        
        # Check MIME type
        try:
            mime = magic.Magic(mime=True)
            file.seek(0)
            file_sample = file.read(2048)
            file_mime_type = mime.from_buffer(file_sample)
            file.seek(0)
            
            logger.info(f"Detected MIME type: {file_mime_type}")
            
            if file_mime_type not in ALLOWED_MIME_TYPES:
                logger.error(f"Invalid MIME type: {file_mime_type}")
                return jsonify({"error": f"File MIME type '{file_mime_type}' is not allowed. Only JSON/text files are accepted."}), 400
        except Exception as e:
            logger.error(f"Error detecting MIME type: {e}")
            return jsonify({"error": f"Unable to validate file type: {str(e)}"}), 400
        
        logger.info(f"File validation passed: {filename} ({file_size} bytes)")
        
        # Save uploaded file to temporary location for streaming
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, f"mongosync_upload_{filename}")
        
        try:
            # Save the file to disk
            file.seek(0)
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file.read())
            
            logger.info(f"Saved uploaded file to: {temp_file_path}")
            
            # Store file path in session for streaming
            if session_store and session_cookie_name:
                session_id = request.cookies.get(session_cookie_name)
                logger.info(f"Upload: session_id from cookie: {session_id}")
                if session_id:
                    session_data = session_store.get_session(session_id)
                    logger.info(f"Upload: session_data retrieved: {session_data}")
                    if session_data:
                        session_data['log_file_path'] = temp_file_path
                        session_store.update_session(session_id, session_data)
                        logger.info(f"Updated session {session_id} with log file path: {temp_file_path}")
                        # Verify the update
                        verify_data = session_store.get_session(session_id)
                        logger.info(f"Verification - session now contains: {verify_data}")
                    else:
                        logger.warning(f"Upload: No session data found for session_id: {session_id}")
                else:
                    logger.warning(f"Upload: No session_id found in cookies")
            else:
                logger.warning(f"Upload: session_store or session_cookie_name not provided")
            
            # Now parse the file for metrics
            file.seek(0)
            plot_json, log_lines = parse_log_file_to_json(file, file_mime_type, filename)
            return jsonify({
                "success": True,
                "plot_json": json.loads(plot_json),
                "log_lines": log_lines[-1000:],  # Return last 1000 lines for display
                "uploaded_file_path": temp_file_path  # Return path for frontend reference
            })
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error processing log file: {e}\n{error_trace}")
            return jsonify({"error": f"Error processing log file: {str(e)}"}), 400

    return jsonify({"error": "No file provided"}), 400


def parse_log_file_to_json(file, file_mime_type, filename):
    """
    Parse log file and return plot JSON and raw log lines.
    This is a refactored version of the upload_file logic that returns JSON.
    """
    logger = logging.getLogger(__name__)
    
    # Pre-compile all regex patterns once
    patterns = {
        'replication_progress': re.compile(r"Replication progress", re.IGNORECASE),
        'version_info': re.compile(r"Version info", re.IGNORECASE),
        'operation_stats': re.compile(r"Operation duration stats", re.IGNORECASE),
        'sent_response': re.compile(r"sent response", re.IGNORECASE),
        'phase_transitions': re.compile(r"Starting initializing collections and indexes phase|Starting initializing partitions phase|Starting collection copy phase|Starting change event application phase|Commit handler called", re.IGNORECASE),
        'mongosync_options': re.compile(r"Mongosync Options", re.IGNORECASE),
        'hidden_flags': re.compile(r"Mongosync HiddenFlags", re.IGNORECASE)
    }
    
    # Initialize result containers
    data = []
    version_info_list = []
    mongosync_ops_stats = []
    mongosync_sent_response = []
    phase_transitions_json = []
    mongosync_opts_list = []
    mongosync_hiddenflags = []
    raw_log_lines = []
    
    line_count = 0
    invalid_json_count = 0
    
    file.seek(0)
    
    # Determine if file is compressed
    if is_compressed_mime_type(file_mime_type):
        logger.info(f"Decompressing {file_mime_type} file before processing")
        file_iterator = decompress_file(file, file_mime_type, filename)
    else:
        file_iterator = file
    
    logger.info("Processing log file...")
    for line in tqdm(file_iterator, desc="Processing log file", disable=True):
        line_count += 1
        if isinstance(line, bytes):
            line = line.decode('utf-8', errors='replace')
        line = line.strip()
        
        if not line:
            continue
        
        # Store raw line for display
        raw_log_lines.append(line)
        
        if not line.startswith('{'):
            continue
            
        try:
            json_obj = json.loads(line)
            message = json_obj.get('message', '')
            
            if patterns['replication_progress'].search(message):
                data.append(json_obj)
            
            if patterns['version_info'].search(message):
                version_info_list.append(json_obj)
            
            if patterns['operation_stats'].search(message):
                mongosync_ops_stats.append(json_obj)
            
            if patterns['sent_response'].search(message):
                mongosync_sent_response.append(json_obj)
            
            if patterns['phase_transitions'].search(message):
                phase_transitions_json.append(json_obj)
            
            if patterns['mongosync_options'].search(message):
                filtered_obj = {k: v for k, v in json_obj.items() if k not in ('time', 'level')}
                mongosync_opts_list.append(filtered_obj)
            
            if patterns['hidden_flags'].search(message):
                filtered_obj = {k: v for k, v in json_obj.items() if k not in ('time', 'level')}
                mongosync_hiddenflags.append(filtered_obj)
                
        except json.JSONDecodeError as e:
            invalid_json_count += 1
            if invalid_json_count == 1:
                logger.error(f"File appears to contain invalid JSON. First error on line {line_count}: {e}")
                raise ValueError(f"The uploaded file does not contain valid JSON format. Error on line {line_count}: {str(e)}")
    
    logger.info(f"Processed {line_count} lines")
    
    # Generate plot using existing logic
    plot_json = generate_plot_json(data, version_info_list, mongosync_ops_stats, 
                                   mongosync_sent_response, phase_transitions_json,
                                   mongosync_opts_list, mongosync_hiddenflags)
    
    return plot_json, raw_log_lines


def generate_plot_json(data, version_info_list, mongosync_ops_stats, mongosync_sent_response,
                       phase_transitions_json, mongosync_opts_list, mongosync_hiddenflags):
    """Generate Plotly JSON from parsed log data."""
    logger = logging.getLogger(__name__)
    
    # Process sent response body
    mongosync_sent_response_body = None 
    for response in mongosync_sent_response:
        try:
            if response is None or not isinstance(response, dict):
                continue
            body = response.get('body')
            if body is None:
                continue
            parsed_body = json.loads(body)
            if isinstance(parsed_body, dict) and 'progress' in parsed_body:
                mongosync_sent_response_body = parsed_body  
        except (json.JSONDecodeError, TypeError, KeyError, AttributeError):  
            pass

    # Create version text
    if version_info_list and len(version_info_list) > 0 and isinstance(version_info_list[0], dict):  
        version = version_info_list[0].get('version', 'Unknown')  
        os_name = version_info_list[0].get('os', 'Unknown')  
        arch = version_info_list[0].get('arch', 'Unknown')  
        version_text = f"MongoSync Version: {version}, OS: {os_name}, Arch: {arch}"   
    else:  
        version_text = "MongoSync Version is not available"

    # Extract serverID and MongosyncID from opts/hiddenflags for title
    server_id_val = ""
    id_val = ""
    if mongosync_opts_list:
        opts_for_title = mongosync_opts_list[0]
        server_id_val = opts_for_title.get('serverID', opts_for_title.get('serverid', ''))
        id_val = opts_for_title.get('id', opts_for_title.get('mongosyncID', opts_for_title.get('MongosyncID', '')))
    if not id_val and mongosync_hiddenflags and len(mongosync_hiddenflags) > 0:
        hf = mongosync_hiddenflags[0]
        if isinstance(hf, dict):
            id_val = hf.get('mongosyncID', hf.get('MongosyncID', hf.get('id', '')))
    if server_id_val is not None:
        server_id_val = str(server_id_val)
    else:
        server_id_val = ""
    if id_val is not None:
        id_val = str(id_val)
    else:
        id_val = ""

    # Get timezone info
    timeZoneInfo = ""
    try:  
        if data:
            dt = parser.isoparse(data[0]['time'])  
            tz_name = dt.strftime('%Z')  
            tz_offset = dt.strftime('%z')  
            if tz_name:  
                timeZoneInfo = tz_name  
            elif tz_offset:  
                tz_sign = tz_offset[0]  
                tz_hour = tz_offset[1:3]  
                tz_min = tz_offset[3:5]  
                timeZoneInfo = f"{tz_sign}{tz_hour}:{tz_min}"
    except Exception:  
        pass

    # Extract data for plotting
    times = []
    for item in data:
        if item and isinstance(item, dict) and 'time' in item and item['time']:
            try:
                times.append(datetime.strptime(item['time'][:26], "%Y-%m-%dT%H:%M:%S.%f"))
            except (ValueError, TypeError, IndexError):
                pass
    
    totalEventsApplied = [item.get('totalEventsApplied') for item in data if item and isinstance(item, dict) and 'totalEventsApplied' in item]
    lagTimeSeconds = [item.get('lagTimeSeconds') for item in data if item and isinstance(item, dict) and 'lagTimeSeconds' in item]
    
    op_stat_times = _extract_operation_stat_times(mongosync_ops_stats, times)
    CollectionCopySourceRead = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'averageDurationMs')
    CollectionCopySourceRead_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'maximumDurationMs')
    CollectionCopySourceRead_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'numOperations')

    CollectionCopyDestinationWrite = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'averageDurationMs')
    CollectionCopyDestinationWrite_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'maximumDurationMs')
    CollectionCopyDestinationWrite_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'numOperations')

    CEASourceRead = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'averageDurationMs')
    CEASourceRead_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'maximumDurationMs')
    CEASourceRead_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'numOperations')

    CEADestinationWrite = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'averageDurationMs')
    CEADestinationWrite_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'maximumDurationMs')
    CEADestinationWrite_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'numOperations')

    _log_destination_write_shape_debug(
        logger,
        mongosync_ops_stats,
        {
            'CollectionCopyDestinationWrite_avg': CollectionCopyDestinationWrite,
            'CollectionCopyDestinationWrite_max': CollectionCopyDestinationWrite_maximum,
            'CollectionCopyDestinationWrite_ops': CollectionCopyDestinationWrite_numOperations,
            'CEADestinationWrite_avg': CEADestinationWrite,
            'CEADestinationWrite_max': CEADestinationWrite_maximum,
            'CEADestinationWrite_ops': CEADestinationWrite_numOperations,
        },
    )
    
    # Estimated bytes
    estimated_total_bytes = 0
    estimated_copied_bytes = 0
    phase_transitions = []
    phase_list = []
    ts_t_list = []
    ts_t_list_formatted = []
    
    if isinstance(mongosync_sent_response_body, dict):
        progress = mongosync_sent_response_body.get('progress')
        if progress and isinstance(progress, dict):
            collection_copy = progress.get('collectionCopy')
            if collection_copy and isinstance(collection_copy, dict):
                estimated_total_bytes = collection_copy.get('estimatedTotalBytes', 0) or 0
                estimated_copied_bytes = collection_copy.get('estimatedCopiedBytes', 0) or 0
            
            atlas_metrics = progress.get('atlasLiveMigrateMetrics')
            if atlas_metrics and isinstance(atlas_metrics, dict):
                phase_transitions = atlas_metrics.get('PhaseTransitions', [])
                if not isinstance(phase_transitions, list):
                    phase_transitions = []
            else:
                phase_transitions = []
        
        if phase_transitions and isinstance(phase_transitions, list) and len(phase_transitions) > 0:
            phase_list = []
            ts_t_list = []
            for item in phase_transitions:
                if item and isinstance(item, dict):
                    phase = item.get('Phase')
                    if phase is not None:
                        phase_list.append(phase)
                    ts = item.get('Ts')
                    if ts and isinstance(ts, dict):
                        t_value = ts.get('T')
                        if t_value is not None:
                            ts_t_list.append(t_value)
            
            ts_t_list_formatted = []
            for t in ts_t_list:
                try:
                    ts_t_list_formatted.append(
                        datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    )
                except (ValueError, TypeError, OSError):
                    pass
    
    # If no phase transitions from response body, try phase_transitions_json
    if not phase_list and phase_transitions_json:
        phase_transitions = phase_transitions_json
        phase_list = [item.get('message') for item in phase_transitions if item and isinstance(item, dict)]
        ts_t_list = []
        for item in phase_transitions:
            if item and isinstance(item, dict) and 'time' in item:
                time_val = item.get('time')
                if time_val:
                    ts_t_list.append(time_val)
        
        ts_t_list_formatted = []
        for t in ts_t_list:
            try:
                ts_t_list_formatted.append(
                    datetime.fromisoformat(t.replace('Z', '+00:00')).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                )
            except (ValueError, TypeError, AttributeError):
                pass

    estimated_total_bytes, estimated_total_bytes_unit = format_byte_size(estimated_total_bytes)
    estimated_copied_bytes = convert_bytes(estimated_copied_bytes, estimated_total_bytes_unit)

    # Create subplot figure: top-level migration charts + Collection Copy/CEA sections
    fig = make_subplots(
        rows=6,
        cols=2,
        subplot_titles=(
            "Mongosync Phases",
            "Estimated Total and Copied " + estimated_total_bytes_unit,
            "Lag Time (seconds)",
            "Change Events Applied",
            "Collection Copy - Avg and Max Read time (ms)",
            "Collection Copy - Avg and Max Write time (ms)",
            "Collection Copy Source Reads",
            "Collection Copy Destination Writes",
            "CEA Source Reads",
            "CEA Destination Write",
            "CEA Source - Avg and Max Read time (ms)",
            "CEA Destination - Avg and Max Write time (ms)"
        ),
        specs=[[{}, {}], [{}, {}], [{}, {}], [{}, {}], [{}, {}], [{}, {}]],
        vertical_spacing=0.055,
        horizontal_spacing=0.06
    )

    # Row 1: Mongosync phases + Estimated bytes
    if phase_list and ts_t_list_formatted and len(phase_list) > 0 and len(ts_t_list_formatted) > 0:
        fig.add_trace(go.Scatter(x=ts_t_list_formatted, y=phase_list, mode='markers+text', marker=dict(color='green')), row=1, col=1)
        fig.update_yaxes(showticklabels=False, row=1, col=1)
    else:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Mongosync Phases', textfont=dict(size=12, color="black")), row=1, col=1)

    if estimated_total_bytes > 0 or estimated_copied_bytes > 0:
        fig.add_trace(go.Bar(name='Estimated ' + estimated_total_bytes_unit + ' to be Copied', x=[estimated_total_bytes_unit], y=[estimated_total_bytes], legendgroup="groupTotalCopied"), row=1, col=2)
        fig.add_trace(go.Bar(name='Estimated Copied ' + estimated_total_bytes_unit, x=[estimated_total_bytes_unit], y=[estimated_copied_bytes], legendgroup="groupTotalCopied"), row=1, col=2)
    else:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Estimated Total and Copied', textfont=dict(size=12, color="black")), row=1, col=2)

    # Row 2: Lag time + events
    fig.add_trace(go.Scatter(x=times, y=lagTimeSeconds, mode='lines', name='Seconds', legendgroup="groupEventsAndLags"), row=2, col=1)
    fig.add_trace(go.Scatter(x=times, y=totalEventsApplied, mode='lines', name='Events', legendgroup="groupEventsAndLags"), row=2, col=2)

    # Row 3: Collection copy avg/max
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead, mode='lines', name='CC Source Avg (ms)', legendgroup="groupCCSourceRead", showlegend=True), row=3, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead_maximum, mode='lines', name='CC Source Max (ms)', legendgroup="groupCCSourceRead", showlegend=True), row=3, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite, mode='lines', name='CC Destination Avg (ms)', legendgroup="groupCCDestinationWrite", showlegend=True), row=3, col=2)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite_maximum, mode='lines', name='CC Destination Max (ms)', legendgroup="groupCCDestinationWrite", showlegend=True), row=3, col=2)

    # Row 4: Collection copy operations
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead_numOperations, mode='lines', name='Reads', legendgroup="groupCCSourceRead", showlegend=False), row=4, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite_numOperations, mode='lines', name='Writes', legendgroup="groupCCDestinationWrite", showlegend=False), row=4, col=2)

    # Row 5: CEA operations
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead_numOperations, mode='lines', name='Reads', legendgroup="groupCEASourceRead", showlegend=False), row=5, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite_numOperations, mode='lines', name='Writes during CEA', legendgroup="groupCEADestinationWrite", showlegend=False), row=5, col=2)

    # Row 6: CEA avg/max
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead, mode='lines', name='CEA Source Avg (ms)', legendgroup="groupCEASourceRead", showlegend=True), row=6, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead_maximum, mode='lines', name='CEA Source Max (ms)', legendgroup="groupCEASourceRead", showlegend=True), row=6, col=1)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite, mode='lines', name='CEA Destination Avg (ms)', legendgroup="groupCEADestinationWrite", showlegend=True), row=6, col=2)
    fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite_maximum, mode='lines', name='CEA Destination Max (ms)', legendgroup="groupCEADestinationWrite", showlegend=True), row=6, col=2)

    # Update axis fonts
    for row in range(1, 7):
        for col in range(1, 3):
            fig.update_xaxes(tickfont=dict(size=8), row=row, col=col)
            fig.update_yaxes(tickfont=dict(size=8), row=row, col=col)
    # Reduce clutter where section labels sit between rows
    fig.update_xaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=2)
    fig.update_xaxes(showticklabels=False, row=4, col=1)
    fig.update_xaxes(showticklabels=False, row=4, col=2)

    # Build title: version + serverID/MongosyncID (field names not bold, values bold) + timezone
    title_parts = [version_text]
    if server_id_val or id_val:
        title_parts_inner = []
        if server_id_val:
            title_parts_inner.append(f"serverID: <b>{server_id_val}</b>")
        if id_val:
            title_parts_inner.append(f"MongosyncID: <b>{id_val}</b>")
        title_parts.append(" | ".join(title_parts_inner))
    if timeZoneInfo:
        title_parts.append(f"Timezone: {timeZoneInfo}")
    title_text = " - ".join(title_parts)

    fig.update_layout(
        height=1360,
        width=900, 
        autosize=True,
        legend_tracegroupgap=120, 
        showlegend=False,
        margin=dict(l=50, r=20, t=115, b=30)
    )
    fig.update_annotations(font=dict(size=11))

    # Highlighted section bars
    fig.add_annotation(
        x=0.5, y=1.045, xref='paper', yref='paper',
        text='<b>Global Migration Metrics</b>',
        showarrow=False,
        font=dict(size=11, color='#1A3C4A'),
        bgcolor='rgba(1, 107, 248, 0.12)',
        bordercolor='#016BF8',
        borderwidth=1,
        borderpad=4
    )
    fig.add_annotation(
        x=0.5, y=0.676, xref='paper', yref='paper',
        text='<b>Collection Copy Metrics</b>',
        showarrow=False,
        font=dict(size=11, color='#1A3C4A'),
        bgcolor='rgba(1, 107, 248, 0.12)',
        bordercolor='#016BF8',
        borderwidth=1,
        borderpad=4
    )
    fig.add_annotation(
        x=0.5, y=0.338, xref='paper', yref='paper',
        text='<b>CEA Metrics</b>',
        showarrow=False,
        font=dict(size=11, color='#1A3C4A'),
        bgcolor='rgba(1, 107, 248, 0.12)',
        bordercolor='#016BF8',
        borderwidth=1,
        borderpad=4
    )

    return json.dumps(fig, cls=PlotlyJSONEncoder)

def upload_file():
    # Use the centralized logging configuration
    logger = logging.getLogger(__name__)
    
    # Check if a file was uploaded
    if 'file' not in request.files:
        logger.error("No file was uploaded")
        return render_template('error.html', 
                             error_title="Upload Error",
                             error_message="No file was selected for upload.")

    file = request.files['file']

    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if file.filename == '':
        logger.error("Empty file without a filename")
        return render_template('error.html',
                             error_title="Upload Error", 
                             error_message="Please select a file to upload.")

    if file:
        # Validate filename and extension
        filename = secure_filename(file.filename)
        if not filename:
            logger.error("Invalid filename")
            return render_template('error.html',
                                 error_title="Upload Error",
                                 error_message="Invalid filename. Please use a valid file name.")
        
        # Check file extension
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            logger.error(f"Invalid file extension: {file_ext}. Allowed: {ALLOWED_EXTENSIONS}")
            return render_template('error.html',
                                 error_title="Invalid File Type",
                                 error_message=f"File type '{file_ext}' is not allowed. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}")
        
        # Check file size (Flask's request.files doesn't have content_length, so we need to read and check)
        file.seek(0, 2)  # Seek to end of file
        file_size = file.tell()  # Get current position (file size)
        file.seek(0)  # Reset to beginning
        
        if file_size > MAX_FILE_SIZE:
            logger.error(f"File too large: {file_size} bytes (max: {MAX_FILE_SIZE} bytes)")
            max_size_mb = MAX_FILE_SIZE / (1024 * 1024)
            actual_size_mb = file_size / (1024 * 1024)
            return render_template('error.html',
                                 error_title="File Too Large",
                                 error_message=f"File size ({actual_size_mb:.1f} MB) exceeds maximum allowed size ({max_size_mb:.1f} MB).")
        
        # Check MIME type using python-magic
        try:
            mime = magic.Magic(mime=True)
            file.seek(0)
            # Read first 2KB for MIME detection (sufficient for most file types)
            file_sample = file.read(2048)
            file_mime_type = mime.from_buffer(file_sample)
            file.seek(0)  # Reset to beginning
            
            logger.info(f"Detected MIME type: {file_mime_type}")
            
            if file_mime_type not in ALLOWED_MIME_TYPES:
                logger.error(f"Invalid MIME type: {file_mime_type}. Allowed: {ALLOWED_MIME_TYPES}")
                return render_template('error.html',
                                     error_title="Invalid File Type",
                                     error_message=f"File MIME type '{file_mime_type}' is not allowed. Only JSON/text files are accepted. Detected type: {file_mime_type}")
        except Exception as e:
            logger.error(f"Error detecting MIME type: {e}")
            return render_template('error.html',
                                 error_title="File Validation Error",
                                 error_message=f"Unable to validate file type: {str(e)}")
        
        logger.info(f"File validation passed: {filename} ({file_size} bytes, {file_ext}, MIME: {file_mime_type})")
        # Optimized single-pass log parsing with streaming approach
        logging.info("Starting optimized log parsing - single pass through file")
        
        # Pre-compile all regex patterns once
        patterns = {
            'replication_progress': re.compile(r"Replication progress", re.IGNORECASE),
            'version_info': re.compile(r"Version info", re.IGNORECASE),
            'operation_stats': re.compile(r"Operation duration stats", re.IGNORECASE),
            'sent_response': re.compile(r"sent response", re.IGNORECASE),
            'phase_transitions': re.compile(r"Starting initializing collections and indexes phase|Starting initializing partitions phase|Starting collection copy phase|Starting change event application phase|Commit handler called", re.IGNORECASE),
            'mongosync_options': re.compile(r"Mongosync Options", re.IGNORECASE),
            'hidden_flags': re.compile(r"Mongosync HiddenFlags", re.IGNORECASE)
        }
        
        # Initialize result containers
        data = []
        version_info_list = []
        mongosync_ops_stats = []
        mongosync_sent_response = []
        phase_transitions_json = []
        mongosync_opts_list = []
        mongosync_hiddenflags = []
        
        # Single pass through the file with streaming
        line_count = 0
        invalid_json_count = 0
        
        # Reset file pointer to beginning
        file.seek(0)
        
        # Determine if file is compressed and get appropriate iterator
        if is_compressed_mime_type(file_mime_type):
            logger.info(f"Decompressing {file_mime_type} file before processing")
            file_iterator = decompress_file(file, file_mime_type, filename)
        else:
            file_iterator = file
        
        logger.info("Processing log file...")
        for line in tqdm(file_iterator, desc="Processing log file", disable=True):
            line_count += 1
            # Handle both bytes and string input (decompressed files return bytes)
            if isinstance(line, bytes):
                line = line.decode('utf-8', errors='replace')
            line = line.strip()
            
            if not line:  # Skip empty lines
                continue
            
            # Skip lines that don't look like JSON objects (handles trailing garbage from decompression)
            if not line.startswith('{'):
                continue
                
            try:
                # Parse JSON only once per line
                json_obj = json.loads(line)
                message = json_obj.get('message', '')
                
                # Apply all filters to the same parsed object
                if patterns['replication_progress'].search(message):
                    data.append(json_obj)
                
                if patterns['version_info'].search(message):
                    version_info_list.append(json_obj)
                
                if patterns['operation_stats'].search(message):
                    mongosync_ops_stats.append(json_obj)
                
                if patterns['sent_response'].search(message):
                    mongosync_sent_response.append(json_obj)
                
                if patterns['phase_transitions'].search(message):
                    phase_transitions_json.append(json_obj)
                
                if patterns['mongosync_options'].search(message):
                    # Filter out time and level fields for options
                    filtered_obj = {k: v for k, v in json_obj.items() if k not in ('time', 'level')}
                    mongosync_opts_list.append(filtered_obj)
                
                if patterns['hidden_flags'].search(message):
                    # Filter out time and level fields for hidden flags
                    filtered_obj = {k: v for k, v in json_obj.items() if k not in ('time', 'level')}
                    mongosync_hiddenflags.append(filtered_obj)
                    
            except json.JSONDecodeError as e:
                invalid_json_count += 1
                if invalid_json_count <= 5:  # Log first 5 errors to avoid spam
                    logging.warning(f"Invalid JSON on line {line_count}: {e}")
                if invalid_json_count == 1:  # If this is the first error, it might be a non-JSON file
                    logging.error(f"File appears to contain invalid JSON. First error on line {line_count}: {e}")
                    return render_template('error.html',
                                         error_title="Invalid File Format",
                                         error_message=f"The uploaded file does not contain valid JSON format. Error on line {line_count}: {str(e)}. Please ensure you're uploading a valid mongosync log file in NDJSON format.")
        
        logging.info(f"Processed {line_count} lines, found {invalid_json_count} invalid JSON lines")
        logging.info(f"Found: {len(data)} replication progress, {len(version_info_list)} version info, "
                    f"{len(mongosync_ops_stats)} operation stats, {len(mongosync_sent_response)} sent responses, "
                    f"{len(phase_transitions_json)} phase transitions, {len(mongosync_opts_list)} options, "
                    f"{len(mongosync_hiddenflags)} hidden flags")  
        
        # The 'body' field is also a JSON string, so parse that as well
        #mongosync_sent_response_body = json.loads(mongosync_sent_response.get('body'))
        mongosync_sent_response_body = None 
        for response in mongosync_sent_response:
            try:  
                parsed_body = json.loads(response['body'])
                # Only use this response if it contains 'progress'
                if 'progress' in parsed_body:
                    mongosync_sent_response_body = parsed_body  
            except (json.JSONDecodeError, TypeError):  
                mongosync_sent_response_body = None  # If parse fails, use None 
                logging.warning(f"No message 'sent response' found in the logs") 

        # Create a string with all the version information
        if version_info_list and isinstance(version_info_list[0], dict):  
            version = version_info_list[0].get('version', 'Unknown')  
            os_name = version_info_list[0].get('os', 'Unknown')  
            arch = version_info_list[0].get('arch', 'Unknown')  
            version_text = f"MongoSync Version: {version}, OS: {os_name}, Arch: {arch}"   
        else:  
            version_text = f"MongoSync Version is not available"  
            logging.error(version_text)  
            

        logging.info(f"Extracting data")

        # Extract serverID and MongosyncID from opts/hiddenflags for title
        server_id_val = ""
        id_val = ""
        if mongosync_opts_list:
            opts_for_title = mongosync_opts_list[0]
            server_id_val = opts_for_title.get('serverID', opts_for_title.get('serverid', ''))
            id_val = opts_for_title.get('id', opts_for_title.get('mongosyncID', opts_for_title.get('MongosyncID', '')))
        if not id_val and mongosync_hiddenflags and len(mongosync_hiddenflags) > 0:
            hf = mongosync_hiddenflags[0]
            if isinstance(hf, dict):
                id_val = hf.get('mongosyncID', hf.get('MongosyncID', hf.get('id', '')))
        server_id_val = str(server_id_val) if server_id_val is not None else ""
        id_val = str(id_val) if id_val is not None else ""

        #Getting the Timezone
        try:  
            dt = parser.isoparse(data[0]['time'])  
            tz_name = dt.strftime('%Z')  
            tz_offset = dt.strftime('%z')  
            if tz_name:  
                timeZoneInfo = tz_name  
            elif tz_offset:  
                # Format offset as +HH:MM  
                tz_sign = tz_offset[0]  
                tz_hour = tz_offset[1:3]  
                tz_min = tz_offset[3:5]  
                timeZoneInfo = f"{tz_sign}{tz_hour}:{tz_min}"  
            else:  
                timeZoneInfo = ""  
        except Exception:  
            timeZoneInfo = ""  
                

        # Extract the data you want to plot
        times = [datetime.strptime(item['time'][:26], "%Y-%m-%dT%H:%M:%S.%f") for item in data if 'time' in item]
        totalEventsApplied = [item['totalEventsApplied'] for item in data if 'totalEventsApplied' in item]
        lagTimeSeconds = [item['lagTimeSeconds'] for item in data if 'lagTimeSeconds' in item]
        op_stat_times = _extract_operation_stat_times(mongosync_ops_stats, times)
        CollectionCopySourceRead = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'averageDurationMs')
        CollectionCopySourceRead_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'maximumDurationMs')
        CollectionCopySourceRead_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopySourceRead', 'numOperations')
        CollectionCopyDestinationWrite = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'averageDurationMs')
        CollectionCopyDestinationWrite_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'maximumDurationMs')
        CollectionCopyDestinationWrite_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CollectionCopyDestinationWrite', 'numOperations')
        CEASourceRead = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'averageDurationMs')
        CEASourceRead_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'maximumDurationMs')
        CEASourceRead_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CEASourceRead', 'numOperations')
        CEADestinationWrite = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'averageDurationMs')
        CEADestinationWrite_maximum = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'maximumDurationMs')
        CEADestinationWrite_numOperations = _extract_operation_stat_series(mongosync_ops_stats, 'CEADestinationWrite', 'numOperations')

        _log_destination_write_shape_debug(
            logger,
            mongosync_ops_stats,
            {
                'CollectionCopyDestinationWrite_avg': CollectionCopyDestinationWrite,
                'CollectionCopyDestinationWrite_max': CollectionCopyDestinationWrite_maximum,
                'CollectionCopyDestinationWrite_ops': CollectionCopyDestinationWrite_numOperations,
                'CEADestinationWrite_avg': CEADestinationWrite,
                'CEADestinationWrite_max': CEADestinationWrite_maximum,
                'CEADestinationWrite_ops': CEADestinationWrite_numOperations,
            },
        )
        
        # Initialize estimated_total_bytes and estimated_copied_bytes with a default value
        estimated_total_bytes = 0
        estimated_copied_bytes = 0
        
        phase_transitions = ""
        # Check that mongosync_sent_response_body is a dict before searching for 'progress'  
        if isinstance(mongosync_sent_response_body, dict):
        #if 'progress' in mongosync_sent_response_body:
            #getting the estimated total and copied
            if 'progress' in mongosync_sent_response_body:
                estimated_total_bytes = mongosync_sent_response_body['progress']['collectionCopy']['estimatedTotalBytes']
                estimated_copied_bytes = mongosync_sent_response_body['progress']['collectionCopy']['estimatedCopiedBytes']

                #Getting the Phase Transisitons
                try:  
                    # Try get Phase Transitions from the sent response body if it is Live Migrate
                    phase_transitions = mongosync_sent_response_body['progress']['atlasLiveMigrateMetrics']['PhaseTransitions']  
                except KeyError as e:  
                    logging.error(f"Key not found: {e}")  
                    phase_transitions = []

            else:
                logging.warning(f"Key 'progress' not found in mongosync_sent_response_body")
            
            # If phase_transitions is not empty, plot the phase transitions as it is Live Migrate
            if phase_transitions:
                phase_list = [item['Phase'] for item in phase_transitions]  
                ts_t_list = [item['Ts']['T'] for item in phase_transitions]  
                ts_t_list_formatted = [ 
                    datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  for t in ts_t_list 
                ]
            # Else get the phase transitions from the phase_transitions_json based on mongosync standalone 
            else:
                if phase_transitions_json:
                    #print (phase_transitions_json)
                    phase_transitions = phase_transitions_json
                    
                    phase_list = [item.get('message') for item in phase_transitions]  
                    ts_t_list = [item['time'] for item in phase_transitions]  
                    # Replace 'Z' with '+00:00' for Python < 3.11 compatibility
                    ts_t_list_formatted = [  
                        datetime.fromisoformat(t.replace('Z', '+00:00')).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  
                        for t in ts_t_list  
                    ]  
        else:
            logging.warning(f"Response body is empty")

        estimated_total_bytes, estimated_total_bytes_unit = format_byte_size(estimated_total_bytes)
        estimated_copied_bytes = convert_bytes(estimated_copied_bytes, estimated_total_bytes_unit)

        logging.info(f"Plotting")

        # Create subplot figure: top-level migration charts + Collection Copy/CEA sections
        fig = make_subplots(
            rows=6,
            cols=2,
            subplot_titles=(
                "Mongosync Phases",
                "Estimated Total and Copied " + estimated_total_bytes_unit,
                "Lag Time (seconds)",
                "Change Events Applied",
                "Collection Copy - Avg and Max Read time (ms)",
                "Collection Copy - Avg and Max Write time (ms)",
                "Collection Copy Source Reads",
                "Collection Copy Destination Writes",
                "CEA Source Reads",
                "CEA Destination Write",
                "CEA Source - Avg and Max Read time (ms)",
                "CEA Destination - Avg and Max Write time (ms)"
            ),
            specs=[[{}, {}], [{}, {}], [{}, {}], [{}, {}], [{}, {}], [{}, {}]],
            vertical_spacing=0.055,
            horizontal_spacing=0.06
        )

        # Row 1: Mongosync phases + Estimated bytes
        if phase_transitions:
            fig.add_trace(go.Scatter(x=ts_t_list_formatted, y=phase_list, mode='markers+text', marker=dict(color='green')), row=1, col=1)
            fig.update_yaxes(showticklabels=False, row=1, col=1)
        else:
            fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Mongosync Phases', textfont=dict(size=12, color="black")), row=1, col=1)

        if estimated_total_bytes > 0 or estimated_copied_bytes > 0:
            fig.add_trace(go.Bar(name='Estimated ' + estimated_total_bytes_unit + ' to be Copied', x=[estimated_total_bytes_unit], y=[estimated_total_bytes], legendgroup="groupTotalCopied"), row=1, col=2)
            fig.add_trace(go.Bar(name='Estimated Copied ' + estimated_total_bytes_unit, x=[estimated_total_bytes_unit], y=[estimated_copied_bytes], legendgroup="groupTotalCopied"), row=1, col=2)
        else:
            fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', name='Estimated Total and Copied', textfont=dict(size=12, color="black")), row=1, col=2)

        # Row 2: Lag time + events
        fig.add_trace(go.Scatter(x=times, y=lagTimeSeconds, mode='lines', name='Seconds', legendgroup="groupEventsAndLags"), row=2, col=1)
        fig.add_trace(go.Scatter(x=times, y=totalEventsApplied, mode='lines', name='Events', legendgroup="groupEventsAndLags"), row=2, col=2)

        # Row 3: Collection copy avg/max
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead, mode='lines', name='CC Source Avg (ms)', legendgroup="groupCCSourceRead", showlegend=True), row=3, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead_maximum, mode='lines', name='CC Source Max (ms)', legendgroup="groupCCSourceRead", showlegend=True), row=3, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite, mode='lines', name='CC Destination Avg (ms)', legendgroup="groupCCDestinationWrite", showlegend=True), row=3, col=2)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite_maximum, mode='lines', name='CC Destination Max (ms)', legendgroup="groupCCDestinationWrite", showlegend=True), row=3, col=2)

        # Row 4: Collection copy operations
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopySourceRead_numOperations, mode='lines', name='Reads', legendgroup="groupCCSourceRead", showlegend=False), row=4, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CollectionCopyDestinationWrite_numOperations, mode='lines', name='Writes', legendgroup="groupCCDestinationWrite", showlegend=False), row=4, col=2)

        # Row 5: CEA operations
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead_numOperations, mode='lines', name='Reads', legendgroup="groupCEASourceRead", showlegend=False), row=5, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite_numOperations, mode='lines', name='Writes during CEA', legendgroup="groupCEADestinationWrite", showlegend=False), row=5, col=2)

        # Row 6: CEA avg/max
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead, mode='lines', name='CEA Source Avg (ms)', legendgroup="groupCEASourceRead", showlegend=True), row=6, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEASourceRead_maximum, mode='lines', name='CEA Source Max (ms)', legendgroup="groupCEASourceRead", showlegend=True), row=6, col=1)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite, mode='lines', name='CEA Destination Avg (ms)', legendgroup="groupCEADestinationWrite", showlegend=True), row=6, col=2)
        fig.add_trace(go.Scatter(x=op_stat_times, y=CEADestinationWrite_maximum, mode='lines', name='CEA Destination Max (ms)', legendgroup="groupCEADestinationWrite", showlegend=True), row=6, col=2)

        # Update axis fonts
        for row in range(1, 7):
            for col in range(1, 3):
                fig.update_xaxes(tickfont=dict(size=8), row=row, col=col)
                fig.update_yaxes(tickfont=dict(size=8), row=row, col=col)
        # Reduce clutter where section labels sit between rows
        fig.update_xaxes(showticklabels=False, row=2, col=1)
        fig.update_xaxes(showticklabels=False, row=2, col=2)
        fig.update_xaxes(showticklabels=False, row=4, col=1)
        fig.update_xaxes(showticklabels=False, row=4, col=2)

        # Build title: version + serverID/MongosyncID (field names not bold, values bold) + timezone
        title_parts = [version_text]
        if server_id_val or id_val:
            title_parts_inner = []
            if server_id_val:
                title_parts_inner.append(f"serverID: <b>{server_id_val}</b>")
            if id_val:
                title_parts_inner.append(f"MongosyncID: <b>{id_val}</b>")
            title_parts.append(" | ".join(title_parts_inner))
        if timeZoneInfo:
            title_parts.append(f"Timezone: {timeZoneInfo}")
        title_text = " - ".join(title_parts)

        # Update layout
        fig.update_layout(height=1360, width=900, autosize=True, legend_tracegroupgap=120, showlegend=False, margin=dict(l=50, r=20, t=115, b=30))


        fig.update_annotations(font=dict(size=11))
        fig.add_annotation(
            x=0.5, y=1.045, xref='paper', yref='paper',
            text='<b>Global Migration Metrics</b>',
            showarrow=False,
            font=dict(size=11, color='#1A3C4A'),
            bgcolor='rgba(1, 107, 248, 0.12)',
            bordercolor='#016BF8',
            borderwidth=1,
            borderpad=4
        )
        fig.add_annotation(
            x=0.5, y=0.676, xref='paper', yref='paper',
            text='<b>Collection Copy Metrics</b>',
            showarrow=False,
            font=dict(size=11, color='#1A3C4A'),
            bgcolor='rgba(1, 107, 248, 0.12)',
            bordercolor='#016BF8',
            borderwidth=1,
            borderpad=4
        )
        fig.add_annotation(
            x=0.5, y=0.338, xref='paper', yref='paper',
            text='<b>CEA Metrics</b>',
            showarrow=False,
            font=dict(size=11, color='#1A3C4A'),
            bgcolor='rgba(1, 107, 248, 0.12)',
            bordercolor='#016BF8',
            borderwidth=1,
            borderpad=4
        )

        # Convert the figure to JSON
        plot_json = json.dumps(fig, cls=PlotlyJSONEncoder)

        logging.info(f"Render the plot in the browse")

        # Render the plot in the browser
        return render_template('upload_results.html', plot_json=plot_json)
