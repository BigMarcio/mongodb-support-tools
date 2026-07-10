"""Tests for log store registry and maintenance."""
import time
import uuid
from unittest.mock import patch

import pytest

from lib.log_store import LogStore
from lib.log_store_maintenance import run_log_store_maintenance
from lib.log_store_registry import LogStoreRegistry


class TestLogStoreRegistryExtended:
    def test_open_store_hit_and_miss(self, tmp_path, monkeypatch):
        store_dir = tmp_path / "store"
        store_dir.mkdir()
        monkeypatch.setattr("lib.log_store_registry.LOG_STORE_DIR", str(store_dir))
        registry = LogStoreRegistry(default_ttl=3600)
        store_id = str(uuid.uuid4())
        db_path = store_dir / f"mi_logstore_{store_id}.db"
        LogStore(str(db_path)).close()
        registry.register(store_id, str(db_path))
        assert registry.open_store(store_id) is not None
        assert registry.open_store(str(uuid.uuid4())) is None

    def test_duplicate_register_overwrites(self, tmp_path, monkeypatch):
        store_dir = tmp_path / "store"
        store_dir.mkdir()
        monkeypatch.setattr("lib.log_store_registry.LOG_STORE_DIR", str(store_dir))
        registry = LogStoreRegistry()
        store_id = str(uuid.uuid4())
        db_path = store_dir / f"mi_logstore_{store_id}.db"
        db_path.write_text("x")
        registry.register(store_id, str(db_path))
        registry.register(store_id, str(db_path))
        assert registry.count() == 1

    def test_expired_entry_returns_none(self, tmp_path, monkeypatch):
        store_dir = tmp_path / "store"
        store_dir.mkdir()
        monkeypatch.setattr("lib.log_store_registry.LOG_STORE_DIR", str(store_dir))
        registry = LogStoreRegistry(default_ttl=1)
        store_id = str(uuid.uuid4())
        db_path = store_dir / f"mi_logstore_{store_id}.db"
        db_path.write_text("x")
        registry.register(store_id, str(db_path))
        with patch("lib.log_store_registry.time.time", return_value=time.time() + 10):
            assert registry.open_store(store_id) is None


class TestLogStoreMaintenance:
    def test_run_maintenance_invokes_cleanup(self, monkeypatch):
        monkeypatch.setattr(
            "lib.log_store_maintenance.log_store_registry.cleanup_expired",
            lambda: None,
        )
        monkeypatch.setattr(
            "lib.log_store_maintenance.LogStore.cleanup_old_stores",
            lambda *a, **k: None,
        )
        monkeypatch.setattr(
            "lib.log_store_maintenance.cleanup_old_snapshots",
            lambda *a, **k: None,
        )
        run_log_store_maintenance()
