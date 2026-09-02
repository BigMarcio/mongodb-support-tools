"""
Configuration management for Mongosync Insights.
Supports environment variables and configurable paths.
"""
import os
import re
import logging
import uuid
import time
import tempfile
import threading
from pathlib import Path
from functools import lru_cache
from typing import Optional
import certifi
from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError, InvalidURI

VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def parse_env_int(
    env_name: str,
    default: int,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Parse an integer environment variable with optional bounds."""
    raw = os.getenv(env_name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw.strip())
        except ValueError as e:
            raise ValueError(
                f"Invalid {env_name}: {raw!r}. Must be an integer."
            ) from e
    if min_value is not None and value < min_value:
        raise ValueError(
            f"Invalid {env_name}: {value}. Must be >= {min_value}."
        )
    if max_value is not None and value > max_value:
        raise ValueError(
            f"Invalid {env_name}: {value}. Must be <= {max_value}."
        )
    return value


# Environment variable configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('MI_LOG_FILE', 'insights.log')
HOST = os.getenv('MI_HOST', '127.0.0.1')
PORT = parse_env_int('MI_PORT', 3030, min_value=1, max_value=65535)

# Application constants
APP_NAME = "Mongosync Insights"
APP_VERSION = "0.9.2.14"

DEVELOPER_CREDITS = {
    "copyright": "\u00a9 MongoDB Inc.",
    "year": "2025 - 2026",
    "team_name": "Migration Factory TS Team",
}

# File upload settings
MAX_FILE_SIZE = parse_env_int('MI_MAX_FILE_SIZE', 10 * 1024 * 1024 * 1024, min_value=1)
ALLOWED_EXTENSIONS = {'.log', '.json', '.out', '.gz', '.zip', '.bz2', '.tar.gz', '.tgz', '.tar.bz2'}
ALLOWED_MIME_TYPES = [
    'text/plain',
    'application/json',
    'application/x-ndjson',
    'application/gzip', 'application/x-gzip',
    'application/zip', 'application/x-zip-compressed',
    'application/x-bzip2',
    'application/x-tar',  # Tar archives
    'application/octet-stream'  # Generic binary (often used for compressed files)
]

# Log Viewer settings
LOG_VIEWER_MAX_LINES = parse_env_int('MI_LOG_VIEWER_MAX_LINES', 2000, min_value=1)
LOG_STORE_DIR = os.getenv('MI_LOG_STORE_DIR', tempfile.gettempdir())
LOG_STORE_MAX_AGE_HOURS = parse_env_int('MI_LOG_STORE_MAX_AGE_HOURS', 24, min_value=1)

# Compressed file MIME types (subset of ALLOWED_MIME_TYPES)
COMPRESSED_MIME_TYPES = {
    'application/gzip', 'application/x-gzip',
    'application/zip', 'application/x-zip-compressed',
    'application/x-bzip2',
    'application/x-tar',  # Tar archives
    'application/octet-stream'  # Generic binary (often used for compressed files)
}

# File extension to compression type mapping (for octet-stream fallback and tar detection)
EXTENSION_TO_COMPRESSION = {
    '.gz': 'gzip',
    '.zip': 'zip',
    '.bz2': 'bzip2',
    '.tar.gz': 'tar_gzip',
    '.tgz': 'tar_gzip',
    '.tar.bz2': 'tar_bzip2'
}

# Filename substring rules for log/metrics identification (see classify_file_type)
UNRECOGNIZED_FILENAME_ERROR_MESSAGE = (
    "No mongosync log or metrics file was recognized from the filename. "
    "Metrics files must include 'metrics' in the name; "
    "log files must include 'mongosync' or 'liveimport'."
)


def classify_file_type(filename: str):
    """
    Classify a file as mongosync logs, mongosync metrics, or unknown based on filename.

    Metrics: basename contains 'metrics' (case-insensitive).
    Logs: basename contains 'mongosync' or 'liveimport' (case-insensitive).
    Metrics is checked before mongosync/liveimport when both appear in the name.

    Args:
        filename: The filename to classify (can include path, only basename is used)

    Returns:
        'logs' for mongosync log files
        'metrics' for mongosync metrics files
        None for unrecognized files
    """
    import os
    basename = os.path.basename(filename)

    name_without_compression = basename
    for ext in ('.gz', '.bz2', '.zip'):
        if name_without_compression.lower().endswith(ext):
            name_without_compression = name_without_compression[:-len(ext)]

    for name in (name_without_compression, basename):
        lower = name.lower()
        if 'metrics' in lower:
            return 'metrics'
        if 'mongosync' in lower or 'liveimport' in lower:
            return 'logs'

    return None


def is_multi_file_archive(filename: str, mime_type: str) -> bool:
    """True for zip/tar archives where inner members are classified individually."""
    import os
    filename_lower = (filename or '').lower()
    for ext in ('.tar.gz', '.tar.bz2', '.tgz'):
        if filename_lower.endswith(ext):
            return True
    ext = os.path.splitext(filename_lower)[1]
    if ext in ('.zip',):
        return True
    if mime_type in ('application/zip', 'application/x-zip-compressed'):
        return True
    return False


# SSL/TLS settings
SSL_ENABLED = os.getenv('MI_SSL_ENABLED', 'False').lower() == 'true'

# Security settings
SECURE_COOKIES = os.getenv('MI_SECURE_COOKIES', str(SSL_ENABLED)).lower() == 'true'
SSL_CERT_PATH = os.getenv('MI_SSL_CERT', '/etc/letsencrypt/live/your-domain/fullchain.pem')
SSL_KEY_PATH = os.getenv('MI_SSL_KEY', '/etc/letsencrypt/live/your-domain/privkey.pem')

# Live monitoring settings
REFRESH_TIME = parse_env_int('MI_REFRESH_TIME', 10, min_value=1)
INDEX_BUILD_REFRESH_TIME = parse_env_int('MI_INDEX_BUILD_REFRESH_TIME', 60, min_value=1)
VERIFIER_PROGRESS_REFRESH_TIME = REFRESH_TIME * 3 
VERIFIER_METADATA_REFRESH_TIME = REFRESH_TIME * 6
MONGOSYNC_PROGRESS_TIMEOUT_SECS = parse_env_int(
    'MI_MONGOSYNC_PROGRESS_TIMEOUT_SECS', REFRESH_TIME, min_value=1,
)
VERIFIER_PROGRESS_TIMEOUT_SECS = parse_env_int(
    'MI_VERIFIER_PROGRESS_TIMEOUT_SECS', VERIFIER_PROGRESS_REFRESH_TIME, min_value=1,
)
VERIFIER_HEAVY_API_TIMEOUT_SECS = parse_env_int(
    'MI_VERIFIER_HEAVY_API_TIMEOUT_SECS', 600, min_value=1,
)
VERIFIER_SUMMARY_COOLDOWN_SECS = VERIFIER_HEAVY_API_TIMEOUT_SECS
CONNECTION_STRING = os.getenv('MI_CONNECTION_STRING', '')
VERIFIER_CONNECTION_STRING = os.getenv('MI_VERIFIER_CONNECTION_STRING', '') or CONNECTION_STRING

PROGRESS_API_PATH = "/api/v1/progress"
VERIFIER_SUMMARY_API_PATH = "/api/v1/summary"
VERIFIER_DOC_MISMATCHES_API_PATH = "/api/v1/docMismatches"
VERIFIER_NS_MISMATCHES_API_PATH = "/api/v1/nsMismatches"
LIVE_API_PATHS = frozenset({
    PROGRESS_API_PATH,
    VERIFIER_SUMMARY_API_PATH,
    VERIFIER_DOC_MISMATCHES_API_PATH,
    VERIFIER_NS_MISMATCHES_API_PATH,
})
DEFAULT_PROGRESS_PORT = 27182
DEFAULT_VERIFIER_PROGRESS_PORT = 27020
PROGRESS_PORT_MIN = 1
PROGRESS_PORT_MAX = 65535


def _parse_allowed_endpoint_hosts(raw):
    return frozenset(
        host.strip().lower() for host in (raw or "").split(",") if host.strip()
    )


# Empty means any host the operator types is allowed (mongosync usually runs on a
# private host). Set MI_ALLOWED_ENDPOINT_HOSTS to pin monitoring to known hosts.
ALLOWED_ENDPOINT_HOSTS = _parse_allowed_endpoint_hosts(
    os.getenv("MI_ALLOWED_ENDPOINT_HOSTS", "")
)


def _parse_progress_port(port_str: str) -> int:
    """Parse and validate a TCP port for the progress endpoint."""
    try:
        port_num = int(port_str.strip())
    except (ValueError, AttributeError) as e:
        raise ValueError(
            f"Invalid progress endpoint port: {port_str!r}. Must be an integer."
        ) from e
    if port_num < PROGRESS_PORT_MIN or port_num > PROGRESS_PORT_MAX:
        raise ValueError(
            f"Invalid progress endpoint port: {port_num}. "
            f"Must be between {PROGRESS_PORT_MIN} and {PROGRESS_PORT_MAX}."
        )
    return port_num


def _build_progress_endpoint_url(host, port=None, *, default_port):
    """
    Build canonical progress endpoint URL from host and port.

    Returns None when host is empty (progress endpoint not configured).
    Raises ValueError when port is non-numeric or outside 1-65535.
    """
    host = (host or "").strip()
    if not host:
        return None
    port_str = str(port).strip() if port is not None else ""
    port_num = default_port if not port_str else _parse_progress_port(port_str)
    return f"{host}:{port_num}{PROGRESS_API_PATH}"


def build_progress_endpoint_url(host, port=None):
    """Build Mongosync progress endpoint URL (default port 27182)."""
    return _build_progress_endpoint_url(host, port, default_port=DEFAULT_PROGRESS_PORT)


def build_verifier_progress_endpoint_url(host, port=None):
    """Build migration-verifier progress endpoint URL (default port 27020)."""
    return _build_progress_endpoint_url(
        host, port, default_port=DEFAULT_VERIFIER_PROGRESS_PORT
    )


def _build_verifier_api_endpoint_url(progress_endpoint_url, api_path):
    """Derive a migration-verifier API path from a progress endpoint URL."""
    raw = (progress_endpoint_url or "").strip().rstrip("/")
    if not raw:
        return None
    if raw.endswith(PROGRESS_API_PATH):
        return raw[: -len(PROGRESS_API_PATH)] + api_path
    if re.match(r"^[\w\.\-]+:\d+$", raw):
        return f"{raw}{api_path}"
    if raw.endswith(api_path):
        return raw
    return raw


def build_verifier_summary_endpoint_url(progress_endpoint_url):
    """Derive migration-verifier /summary URL from a progress endpoint URL."""
    return _build_verifier_api_endpoint_url(
        progress_endpoint_url, VERIFIER_SUMMARY_API_PATH,
    )


def build_verifier_doc_mismatches_endpoint_url(progress_endpoint_url):
    """Derive migration-verifier /docMismatches URL from a progress endpoint URL."""
    return _build_verifier_api_endpoint_url(
        progress_endpoint_url, VERIFIER_DOC_MISMATCHES_API_PATH,
    )


def build_verifier_ns_mismatches_endpoint_url(progress_endpoint_url):
    """Derive migration-verifier /nsMismatches URL from a progress endpoint URL."""
    return _build_verifier_api_endpoint_url(
        progress_endpoint_url, VERIFIER_NS_MISMATCHES_API_PATH,
    )


def normalize_progress_endpoint_url(raw):
    """Normalize user/env input to host:port/api/v1/progress (no scheme)."""
    s = (raw or "").strip().rstrip("/")
    s = re.sub(r"^https?://", "", s, flags=re.IGNORECASE)
    if s.endswith(PROGRESS_API_PATH):
        return s
    if re.match(r"^[\w\.\-]+:\d+$", s):
        return f"{s}{PROGRESS_API_PATH}"
    return s


_raw_progress_endpoint = os.getenv("MI_PROGRESS_ENDPOINT_URL", "")
PROGRESS_ENDPOINT_URL = (
    normalize_progress_endpoint_url(_raw_progress_endpoint)
    if _raw_progress_endpoint.strip()
    else ""
)

_raw_verifier_progress_endpoint = os.getenv("MI_VERIFIER_PROGRESS_ENDPOINT_URL", "")
VERIFIER_PROGRESS_ENDPOINT_URL = (
    normalize_progress_endpoint_url(_raw_verifier_progress_endpoint)
    if _raw_verifier_progress_endpoint.strip()
    else ""
)

# MongoDB settings
MI_MONGOSYNC_DB_NAME = "mongosync_reserved_for_internal_use"
MI_MONGOSYNC_DB_NAME_NEW = "__mdb_internal_mongosync"
MI_MIGRATION_VERIFIER_DB_NAME = os.getenv(
    "MI_MIGRATION_VERIFIER_DB_NAME", "__mdb_internal_migration_verifier"
)
MI_EMBEDDED_VERIFIER_SRC_DB_NAME = os.getenv(
    "MI_EMBEDDED_VERIFIER_SRC_DB_NAME", "__mdb_internal_mongosync_verifier_src"
)
MI_EMBEDDED_VERIFIER_DST_DB_NAME = os.getenv(
    "MI_EMBEDDED_VERIFIER_DST_DB_NAME", "__mdb_internal_mongosync_verifier_dst"
)
VERIFIER_GENERATION_LIMIT = parse_env_int(
    "MI_VERIFIER_GENERATION_LIMIT", 5, min_value=1, max_value=20
)
VERIFIER_FAILED_TASKS_LIMIT = parse_env_int(
    "MI_VERIFIER_FAILED_TASKS_LIMIT", 20, min_value=1, max_value=100
)
VERIFIER_SUMMARY_MIN_DURATION_SECS = parse_env_int(
    "MI_VERIFIER_SUMMARY_MIN_DURATION_SECS", 0, min_value=0, max_value=86400
)
# Keep in sync with migration-verifier internal/verifier/metadata.go (verifierMetadataVersion).
# Not configurable via environment — change only here when MV bumps the schema version.
VERIFIER_METADATA_VERSION = 7

# Error patterns file (Log Analyzer)
_DEFAULT_ERROR_PATTERNS_FILE = os.path.join(
    os.path.dirname(__file__), 'error_patterns.json',
)


def resolve_error_patterns_file() -> str:
    """Resolve error patterns JSON path from MI_ERROR_PATTERNS_FILE or the bundled default."""
    configured = os.getenv('MI_ERROR_PATTERNS_FILE', '').strip()
    return configured or _DEFAULT_ERROR_PATTERNS_FILE


ERROR_PATTERNS_FILE = resolve_error_patterns_file()


def load_error_patterns():
    """
    Load error patterns from external JSON file.
    
    Returns:
        list: List of dictionaries with 'pattern' and 'friendly_name' keys, and
        optionally 'recommendation' (string shown in the Errors tab for matches).
    """
    import json
    logger = logging.getLogger(__name__)
    patterns_file = resolve_error_patterns_file()
    
    try:
        with open(patterns_file, 'r') as f:
            patterns = json.load(f)
            logger.info(f"Loaded {len(patterns)} error patterns from {patterns_file}")
            return patterns
    except FileNotFoundError:
        logger.warning(f"Error patterns file not found: {patterns_file}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in error patterns file: {e}")
        return []
    except Exception as e:
        logger.error(f"Error loading error patterns: {e}")
        return []

def setup_logging():
    """Configure logging based on environment variables."""
    log_level = getattr(logging, LOG_LEVEL.upper())
    logging.basicConfig(
        filename=LOG_FILE,
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_app_info():
    """Get application information."""
    return {
        'name': APP_NAME,
        'version': APP_VERSION,
        'log_file': LOG_FILE,
        'host': HOST,
        'port': PORT
    }

def validate_config():
    """Validate configuration on startup."""
    # Check if log file directory is writable
    log_file = Path(LOG_FILE)
    log_dir = log_file.parent
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    
    if not os.access(log_dir, os.W_OK):
        raise PermissionError(f"Cannot write to log directory: {log_dir}")

    level_name = LOG_LEVEL.upper()
    if level_name not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Invalid LOG_LEVEL: {LOG_LEVEL!r}. "
            f"Must be one of: {', '.join(sorted(VALID_LOG_LEVELS))}."
        )

    if PROGRESS_ENDPOINT_URL and not validate_progress_endpoint_url(PROGRESS_ENDPOINT_URL):
        raise ValueError(
            f"Invalid MI_PROGRESS_ENDPOINT_URL: {_raw_progress_endpoint!r}. "
            "Use host:port or host:port/api/v1/progress with port between 1 and 65535."
        )

    if (
        VERIFIER_PROGRESS_ENDPOINT_URL
        and not validate_progress_endpoint_url(VERIFIER_PROGRESS_ENDPOINT_URL)
    ):
        raise ValueError(
            f"Invalid MI_VERIFIER_PROGRESS_ENDPOINT_URL: {_raw_verifier_progress_endpoint!r}. "
            "Use host:port or host:port/api/v1/progress with port between 1 and 65535."
        )

    return True


def _match_api_endpoint_url(url, api_path):
    """Match host:port + exact API path, returning the host or None."""
    if not url or api_path not in LIVE_API_PATHS:
        return None
    pattern = r'^([\w\.\-]+):(\d+)' + re.escape(api_path) + r'$'
    match = re.match(pattern, url)
    if not match:
        return None
    try:
        _parse_progress_port(match.group(2))
    except ValueError:
        return None
    return match.group(1)


def validate_api_endpoint_url(url, api_path=PROGRESS_API_PATH):
    """
    Validate a mongosync/verifier API endpoint URL.

    Args:
        url (str): URL to validate in format host:port/<api_path>
        api_path (str): expected API path; must be one of LIVE_API_PATHS

    Returns:
        bool: True if URL matches the expected format and port is 1-65535
    """
    return _match_api_endpoint_url(url, api_path) is not None


def validate_progress_endpoint_url(url):
    """
    Validate Mongosync Progress Endpoint URL format.

    Args:
        url (str): URL to validate in format host:port/api/v1/progress

    Returns:
        bool: True if URL matches the expected format and port is 1-65535
    """
    return validate_api_endpoint_url(url, PROGRESS_API_PATH)


def endpoint_host_allowed(url, api_path=PROGRESS_API_PATH):
    """Check a validated endpoint URL against MI_ALLOWED_ENDPOINT_HOSTS."""
    if not ALLOWED_ENDPOINT_HOSTS:
        return True
    host = _match_api_endpoint_url(url, api_path)
    if host is None:
        return False
    return host.lower() in ALLOWED_ENDPOINT_HOSTS

# Database Connection Management
# Connection pool settings
CONNECTION_POOL_SIZE = parse_env_int('MI_POOL_SIZE', 10, min_value=1)
CONNECTION_TIMEOUT_MS = parse_env_int('MI_TIMEOUT_MS', 30000, min_value=1)
VERIFIER_METADATA_TIMEOUT_MS = parse_env_int(
    'MI_VERIFIER_METADATA_TIMEOUT_MS', 120000, min_value=1000,
)

def _create_mongo_client(connection_string, *, timeout_ms):
    """
    Create a MongoDB client with connection pooling and the given timeout settings.

    Args:
        connection_string (str): MongoDB connection string
        timeout_ms (int): Socket/connect/server-selection timeout in milliseconds

    Returns:
        MongoClient: MongoDB client instance
    """
    logger = logging.getLogger(__name__)

    try:
        from pymongo.uri_parser import parse_uri
        parsed = parse_uri(connection_string)

        uri_tls_options = parsed.get('options', {})
        is_srv = connection_string.strip().lower().startswith('mongodb+srv://')
        tls_explicitly_set = 'tls' in uri_tls_options or 'ssl' in uri_tls_options
        tls_disabled = uri_tls_options.get('tls', uri_tls_options.get('ssl', True)) is False
        use_tls_ca = is_srv or (tls_explicitly_set and not tls_disabled)

        client_kwargs = dict(
            maxPoolSize=CONNECTION_POOL_SIZE,
            minPoolSize=1,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
            socketTimeoutMS=timeout_ms,
            retryWrites=True,
            retryReads=True,
        )
        if use_tls_ca:
            client_kwargs['tlsCAFile'] = certifi.where()

        client = MongoClient(connection_string, **client_kwargs)

        client.admin.command('ping', read_preference=ReadPreference.SECONDARY_PREFERRED)
        logger.info(
            "Successfully connected to MongoDB with pool size %s (timeout_ms=%s)",
            CONNECTION_POOL_SIZE,
            timeout_ms,
        )

        return client

    except InvalidURI as e:
        logger.error(f"Invalid MongoDB connection string: {e}")
        raise
    except PyMongoError as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to MongoDB: {e}")
        raise PyMongoError(f"Connection failed: {e}") from e

@lru_cache(maxsize=4)
def get_mongo_client(connection_string):
    """
    Get a cached MongoDB client with connection pooling.
    
    Args:
        connection_string (str): MongoDB connection string
        
    Returns:
        MongoClient: Cached MongoDB client instance
        
    Raises:
        InvalidURI: If the connection string is invalid
        PyMongoError: If connection fails
    """
    return _create_mongo_client(connection_string, timeout_ms=CONNECTION_TIMEOUT_MS)

@lru_cache(maxsize=4)
def get_verifier_metadata_mongo_client(connection_string):
    """Cached MongoDB client for verifier metadata reads (extended timeout)."""
    return _create_mongo_client(
        connection_string, timeout_ms=VERIFIER_METADATA_TIMEOUT_MS,
    )

def get_database(connection_string, database_name):
    """
    Get a database instance using the cached client.
    
    Args:
        connection_string (str): MongoDB connection string
        database_name (str): Name of the database
        
    Returns:
        Database: MongoDB database instance
    """
    client = get_mongo_client(connection_string)
    return client[database_name]

def get_verifier_metadata_database(connection_string, database_name):
    """
    Get a verifier metadata database with an extended socket timeout.

    Uses MI_VERIFIER_METADATA_TIMEOUT_MS (default 120s) instead of MI_TIMEOUT_MS
    for metadata aggregation reads only.
    """
    client = get_verifier_metadata_mongo_client(connection_string)
    return client[database_name]

_resolved_internal_db_cache = {}
_resolved_internal_db_lock = threading.Lock()

def probe_metadata_databases(connection_string):
    """
    Probe the destination cluster for mongosync and verifier metadata databases.

    Returns:
        dict: mongosync_metadata_available, verifier_metadata_available booleans
    """
    client = get_mongo_client(connection_string)
    db_names = set(client.list_database_names())
    mongosync_available = (
        MI_MONGOSYNC_DB_NAME_NEW in db_names or MI_MONGOSYNC_DB_NAME in db_names
    )
    verifier_available = MI_MIGRATION_VERIFIER_DB_NAME in db_names
    return {
        "mongosync_metadata_available": mongosync_available,
        "verifier_metadata_available": verifier_available,
    }


def resolve_mongosync_db_name(connection_string):
    """
    Auto-detect which mongosync internal database name exists on the cluster.

    Checks for the new name first (__mdb_internal_mongosync), then falls back
    to the legacy name (mongosync_reserved_for_internal_use). Results are cached
    per connection string.

    Args:
        connection_string (str): MongoDB connection string

    Returns:
        str: The resolved internal database name
    """
    with _resolved_internal_db_lock:
        if connection_string in _resolved_internal_db_cache:
            return _resolved_internal_db_cache[connection_string]

    logger = logging.getLogger(__name__)
    try:
        client = get_mongo_client(connection_string)
        db_names = client.list_database_names()
        if MI_MONGOSYNC_DB_NAME_NEW in db_names:
            resolved = MI_MONGOSYNC_DB_NAME_NEW
        else:
            resolved = MI_MONGOSYNC_DB_NAME
        with _resolved_internal_db_lock:
            _resolved_internal_db_cache[connection_string] = resolved
        logger.info(f"Resolved internal DB name: {resolved}")
        return resolved
    except Exception as e:
        logger.warning(f"Could not auto-detect internal DB name, using default: {e}")
        return MI_MONGOSYNC_DB_NAME

def validate_connection(connection_string):
    """
    Validate a MongoDB connection string and test connectivity.
    
    Args:
        connection_string (str): MongoDB connection string to validate
        
    Returns:
        bool: True if connection is valid and accessible
        
    Raises:
        InvalidURI: If the connection string format is invalid
        PyMongoError: If connection test fails
    """
    try:
        # This will use the cached client or create a new one
        client = get_mongo_client(connection_string)
        # Test with a simple command
        result = client.admin.command('ping')
        return result.get('ok', 0) == 1
    except Exception:
        clear_connection_cache()
        raise

def clear_connection_cache():
    """
    Clear the connection cache. Useful when connection strings change.
    """
    logger = logging.getLogger(__name__)
    get_mongo_client.cache_clear()
    get_verifier_metadata_mongo_client.cache_clear()
    with _resolved_internal_db_lock:
        _resolved_internal_db_cache.clear()
    logger.info("MongoDB connection cache cleared")


# =============================================================================
# In-Memory Session Store
# =============================================================================

# Session settings
SESSION_TIMEOUT = parse_env_int('MI_SESSION_TIMEOUT', 3600, min_value=1)  # 1 hour default

class InMemorySessionStore:
    """
    Thread-safe in-memory session store with automatic expiration.
    
    This replaces Flask's built-in session with a simple server-side store.
    Session IDs are stored in cookies, but credentials stay on the server.
    """
    
    def __init__(self, timeout=SESSION_TIMEOUT):
        self._store = {}
        self._lock = threading.Lock()
        self._timeout = timeout
        self._logger = logging.getLogger(__name__)
    
    def create_session(self, data: dict) -> str:
        """
        Create a new session with the given data.
        
        Args:
            data: Dictionary of session data to store
            
        Returns:
            str: Unique session ID
        """
        session_id = str(uuid.uuid4())
        with self._lock:
            self._store[session_id] = {
                'data': data,
                'created_at': time.time(),
                'last_accessed': time.time()
            }
        self._logger.debug(f"Created session: {session_id[:8]}...")
        return session_id
    
    def get_session(self, session_id: str) -> dict:
        """
        Retrieve session data by session ID.
        
        Args:
            session_id: The session ID to look up
            
        Returns:
            dict: Session data, or empty dict if not found/expired
        """
        if not session_id:
            return {}
            
        with self._lock:
            session = self._store.get(session_id)
            if not session:
                return {}
            
            # Check if session has expired
            if time.time() - session['last_accessed'] > self._timeout:
                del self._store[session_id]
                self._logger.debug(f"Session expired: {session_id[:8]}...")
                return {}
            
            # Update last accessed time
            session['last_accessed'] = time.time()
            return session['data'].copy()
    
    def update_session(self, session_id: str, data: dict) -> bool:
        """
        Merge new data into an existing session, preserving unmodified keys.
        
        Args:
            session_id: The session ID to update
            data: New data to merge into the session
            
        Returns:
            bool: True if session was updated, False if not found/expired
        """
        if not session_id:
            return False
            
        with self._lock:
            session = self._store.get(session_id)
            if not session:
                return False
            if time.time() - session['last_accessed'] > self._timeout:
                del self._store[session_id]
                return False
            session['data'].update(data)
            session['last_accessed'] = time.time()
            return True
    
    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session.
        
        Args:
            session_id: The session ID to delete
            
        Returns:
            bool: True if session was deleted, False if not found
        """
        if not session_id:
            return False
            
        with self._lock:
            if session_id in self._store:
                del self._store[session_id]
                self._logger.debug(f"Deleted session: {session_id[:8]}...")
                return True
            return False
    
    def cleanup_expired(self):
        """Remove all expired sessions."""
        current_time = time.time()
        with self._lock:
            expired = [
                sid for sid, session in self._store.items()
                if current_time - session['last_accessed'] > self._timeout
            ]
            for sid in expired:
                del self._store[sid]
            if expired:
                self._logger.debug(f"Cleaned up {len(expired)} expired sessions")


# Global session store instance
session_store = InMemorySessionStore()
