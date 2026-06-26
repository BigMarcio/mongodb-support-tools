"""Tests for environment variable parsing and startup validation."""
import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from lib import app_config
from lib.app_config import (
    InMemorySessionStore,
    build_progress_endpoint_url,
    classify_file_type,
    is_multi_file_archive,
    load_error_patterns,
    normalize_progress_endpoint_url,
    parse_env_int,
    resolve_internal_db_name,
    validate_config,
    validate_connection,
    validate_progress_endpoint_url,
    INTERNAL_DB_NAME,
    INTERNAL_DB_NAME_NEW,
)


class TestParseEnvInt:
    def test_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MI_TEST_INT", None)
            assert parse_env_int("MI_TEST_INT", 42) == 42

    def test_valid_override(self):
        with patch.dict(os.environ, {"MI_TEST_INT": "99"}):
            assert parse_env_int("MI_TEST_INT", 42) == 99

    def test_empty_string_uses_default(self):
        with patch.dict(os.environ, {"MI_TEST_INT": ""}):
            assert parse_env_int("MI_TEST_INT", 42) == 42

    def test_non_integer_raises(self):
        with patch.dict(os.environ, {"MI_TEST_INT": "abc"}):
            with pytest.raises(ValueError, match="MI_TEST_INT"):
                parse_env_int("MI_TEST_INT", 42)

    def test_below_min_raises(self):
        with patch.dict(os.environ, {"MI_TEST_INT": "0"}):
            with pytest.raises(ValueError, match=">= 1"):
                parse_env_int("MI_TEST_INT", 42, min_value=1)

    def test_above_max_raises(self):
        with patch.dict(os.environ, {"MI_TEST_INT": "70000"}):
            with pytest.raises(ValueError, match="<= 65535"):
                parse_env_int("MI_TEST_INT", 42, max_value=65535)


class TestValidateConfig:
    @patch.object(app_config, "LOG_LEVEL", "VERBOSE")
    @patch("lib.app_config.os.access", return_value=True)
    @patch("lib.app_config.Path")
    def test_rejects_unknown_log_level(self, mock_path, mock_access):
        mock_path.return_value.parent.exists.return_value = True
        with pytest.raises(ValueError, match="LOG_LEVEL"):
            validate_config()

    @patch.object(app_config, "LOG_LEVEL", "INFO")
    @patch("lib.app_config.os.access", return_value=True)
    @patch("lib.app_config.Path")
    def test_accepts_valid_log_level(self, mock_path, mock_access):
        mock_path.return_value.parent.exists.return_value = True
        assert validate_config() is True

    @patch.object(app_config, "LOG_LEVEL", "critical")
    @patch("lib.app_config.os.access", return_value=True)
    @patch("lib.app_config.Path")
    def test_accepts_log_level_case_insensitive(self, mock_path, mock_access):
        mock_path.return_value.parent.exists.return_value = True
        assert validate_config() is True


class TestProgressEndpointUrl:
    def test_build_empty_host_returns_none(self):
        assert build_progress_endpoint_url("", "27182") is None
        assert build_progress_endpoint_url("  ", "27182") is None

    def test_build_default_port(self):
        assert (
            build_progress_endpoint_url("myhost", None)
            == "myhost:27182/api/v1/progress"
        )

    def test_build_custom_port(self):
        assert (
            build_progress_endpoint_url("myhost", "9999")
            == "myhost:9999/api/v1/progress"
        )

    def test_normalize_short_form(self):
        assert (
            normalize_progress_endpoint_url("localhost:27182")
            == "localhost:27182/api/v1/progress"
        )

    def test_normalize_full_form_with_scheme(self):
        assert (
            normalize_progress_endpoint_url("http://host:27182/api/v1/progress")
            == "host:27182/api/v1/progress"
        )

    def test_normalize_already_canonical(self):
        url = "host:27182/api/v1/progress"
        assert normalize_progress_endpoint_url(url) == url

    def test_validate_accepts_normalized_output(self):
        url = normalize_progress_endpoint_url("localhost:27182")
        assert validate_progress_endpoint_url(url) is True


class TestClassifyFileType:
    @pytest.mark.parametrize("filename,expected", [
        ("mongosync_metrics.log", "metrics"),
        ("mongosync_metrics_22JUN2026.log", "metrics"),
        ("my_metrics_export.log", "metrics"),
        ("mongosync_metrics_foo.log", "metrics"),
        ("mongosync.log", "logs"),
        ("mongosync.log.1", "logs"),
        ("mongosync-foo.log", "logs"),
        ("liveimport_x.log", "logs"),
        ("mongosync.log.gz", "logs"),
        ("archive/mongosync.log", "logs"),
        ("myfile.log", None),
        ("backup.gz", None),
        ("data.json", None),
    ])
    def test_classify_file_type(self, filename, expected):
        assert classify_file_type(filename) == expected


class TestIsMultiFileArchive:
    @pytest.mark.parametrize(
        "filename,mime,expected",
        [
            ("bundle.zip", "application/octet-stream", True),
            ("logs.tar.gz", "application/gzip", True),
            ("archive.tgz", "application/octet-stream", True),
            ("mongosync.log.gz", "application/gzip", False),
            ("data.zip", "application/zip", True),
        ],
    )
    def test_is_multi_file_archive(self, filename, mime, expected):
        assert is_multi_file_archive(filename, mime) is expected


class TestLoadErrorPatterns:
    def test_loads_default_patterns_file(self):
        patterns = load_error_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) > 0
        assert "pattern" in patterns[0]

    def test_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "ERROR_PATTERNS_FILE", str(tmp_path / "missing.json"))
        assert load_error_patterns() == []

    def test_invalid_json_returns_empty(self, tmp_path, monkeypatch):
        bad = tmp_path / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        monkeypatch.setattr(app_config, "ERROR_PATTERNS_FILE", str(bad))
        assert load_error_patterns() == []


class TestInMemorySessionStore:
    def test_create_and_get_session(self):
        store = InMemorySessionStore(timeout=60)
        sid = store.create_session({"uri": "mongodb://localhost"})
        data = store.get_session(sid)
        assert data == {"uri": "mongodb://localhost"}

    def test_expired_session_returns_empty(self):
        store = InMemorySessionStore(timeout=1)
        sid = store.create_session({"x": 1})
        with patch("lib.app_config.time.time", return_value=time.time() + 2):
            assert store.get_session(sid) == {}

    def test_update_and_delete_session(self):
        store = InMemorySessionStore(timeout=60)
        sid = store.create_session({"a": 1})
        assert store.update_session(sid, {"b": 2}) is True
        assert store.get_session(sid) == {"a": 1, "b": 2}
        assert store.delete_session(sid) is True
        assert store.get_session(sid) == {}


class TestResolveInternalDbName:
    def setup_method(self):
        app_config.clear_connection_cache()

    @patch("lib.app_config.get_mongo_client")
    def test_prefers_new_internal_db(self, mock_get_client):
        mock_get_client.return_value.list_database_names.return_value = [
            INTERNAL_DB_NAME_NEW,
            "app",
        ]
        assert resolve_internal_db_name("mongodb://localhost:27017") == INTERNAL_DB_NAME_NEW

    @patch("lib.app_config.get_mongo_client")
    def test_falls_back_to_legacy(self, mock_get_client):
        mock_get_client.return_value.list_database_names.return_value = [
            INTERNAL_DB_NAME,
        ]
        assert resolve_internal_db_name("mongodb://localhost:27017") == INTERNAL_DB_NAME


class TestValidateConnection:
    def setup_method(self):
        app_config.clear_connection_cache()

    @patch("lib.app_config.get_mongo_client")
    def test_success(self, mock_get_client):
        mock_get_client.return_value.admin.command.return_value = {"ok": 1}
        assert validate_connection("mongodb://localhost:27017") is True

    @patch("lib.app_config.get_mongo_client")
    def test_failure_clears_cache(self, mock_get_client):
        mock_get_client.cache_clear = MagicMock()
        mock_get_client.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError):
            validate_connection("mongodb://localhost:27017")
        mock_get_client.cache_clear.assert_called_once()


class TestValidateProgressEndpointUrlRejects:
    @pytest.mark.parametrize(
        "url",
        [
            "",
            "host/api/v1/progress",
            "host:abc/api/v1/progress",
            "http://host:27182/api/v1/progress",
        ],
    )
    def test_rejects_invalid_urls(self, url):
        assert validate_progress_endpoint_url(url) is False
