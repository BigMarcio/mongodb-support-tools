"""Tests for snapshot persistence lifecycle."""
import json
import os
import time
import uuid

import pytest

from lib import snapshot_store
from lib.log_store import LogStore


@pytest.fixture
def store_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(snapshot_store, "LOG_STORE_DIR", str(tmp_path))
    return tmp_path


class TestSnapshotLifecycle:
    def test_save_and_load_round_trip(self, store_dir):
        snapshot_id = str(uuid.uuid4())
        store_id = str(uuid.uuid4())
        template = {"plots": [], "title": "test"}
        path = snapshot_store.save_snapshot(
            snapshot_id,
            "mongosync.log",
            100,
            10,
            store_id,
            template,
        )
        assert os.path.exists(path)
        loaded = snapshot_store.load_snapshot(snapshot_id)
        assert loaded["template_data"] == template
        assert loaded["source_filename"] == "mongosync.log"

    def test_list_snapshots_sorted_by_mtime(self, store_dir):
        ids = [str(uuid.uuid4()) for _ in range(2)]
        for i, sid in enumerate(ids):
            snapshot_store.save_snapshot(sid, f"f{i}.log", 1, 1, str(uuid.uuid4()), {})
            path = snapshot_store._snapshot_path(sid)
            os.utime(path, (time.time() + i, time.time() + i))
        listed = snapshot_store.list_snapshots()
        assert len(listed) >= 2
        assert listed[0]["snapshot_id"] == ids[1]

    def test_delete_snapshot_removes_files(self, store_dir):
        snapshot_id = str(uuid.uuid4())
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        s = LogStore(db_path)
        s.insert_many([{"time": "t", "level": "info", "message": "x"}])
        s.close()
        snapshot_store.save_snapshot(snapshot_id, "f.log", 1, 1, store_id, {"x": 1})
        deleted, returned_store_id = snapshot_store.delete_snapshot(snapshot_id)
        assert deleted is True
        assert returned_store_id == store_id
        assert not os.path.exists(snapshot_store._snapshot_path(snapshot_id))
        assert not os.path.exists(db_path)

    def test_cleanup_old_snapshots(self, store_dir, monkeypatch):
        snapshot_id = str(uuid.uuid4())
        snapshot_store.save_snapshot(snapshot_id, "old.log", 1, 1, str(uuid.uuid4()), {})
        path = snapshot_store._snapshot_path(snapshot_id)
        old = time.time() - (48 * 3600)
        os.utime(path, (old, old))
        snapshot_store.cleanup_old_snapshots(str(store_dir), max_age_hours=24)
        assert not os.path.exists(path)
