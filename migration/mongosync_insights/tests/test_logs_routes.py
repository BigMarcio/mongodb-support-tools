"""Tests for Flask logs blueprint routes."""
import io
import json
import uuid
from unittest.mock import patch

import pytest

from lib import snapshot_store
from lib.log_store import LogStore
from lib.log_store_registry import log_store_registry


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshot_store, "LOG_STORE_DIR", str(tmp_path / "store"))
    (tmp_path / "store").mkdir(exist_ok=True)
    log_store_registry._entries.clear()
    yield
    log_store_registry._entries.clear()


class TestLogsHome:
    def test_logs_home_returns_200(self, app_client):
        r = app_client.get("/logs/")
        assert r.status_code == 200


class TestUploadLogs:
    @patch("lib.logs_metrics.create_metrics_plots", return_value="")
    def test_upload_plain_log(self, _mock_plots, app_client):
        data = {
            "file": (
                io.BytesIO(b'{"time":"2026-01-01T00:00:00Z","level":"info","message":"test"}\n'),
                "mongosync.log",
            )
        }
        r = app_client.post("/logs/uploadLogs", data=data, content_type="multipart/form-data")
        assert r.status_code == 200

    def test_upload_rejects_bad_extension(self, app_client):
        data = {"file": (io.BytesIO(b"data"), "bad.exe")}
        r = app_client.post("/logs/uploadLogs", data=data, content_type="multipart/form-data")
        assert r.status_code == 200
        assert b"Invalid File Type" in r.data

    def test_upload_no_file(self, app_client):
        r = app_client.post("/logs/uploadLogs", data={}, content_type="multipart/form-data")
        assert r.status_code == 200
        assert b"Upload Error" in r.data


class TestSearchLogs:
    def test_search_missing_store_id(self, app_client):
        r = app_client.get("/logs/search_logs")
        assert r.status_code == 400

    def test_search_happy_path(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many(
            [{"time": "2026-01-01T00:00:00Z", "level": "info", "message": "Replication progress"}]
        )
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)
        r = app_client.get(f"/logs/search_logs?store_id={store_id}&q=Replication")
        assert r.status_code == 200
        body = r.get_json()
        assert body["total"] >= 1

    def test_search_by_timestamp_range(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([
            {"time": "2026-01-01T10:00:00.000Z", "level": "info", "message": "early"},
            {"time": "2026-01-01T10:30:00.000Z", "level": "info", "message": "middle"},
            {"time": "2026-01-01T11:00:00.000Z", "level": "info", "message": "late"},
        ])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(
            "/logs/search_logs"
            f"?store_id={store_id}"
            "&start=2026-01-01T10:15:00.000Z"
            "&end=2026-01-01T10:45:00.000Z"
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["total"] == 1
        assert body["results"][0]["message"] == "middle"

    def test_search_end_includes_bare_z_and_high_millisecond_logs(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([
            {"time": "2026-01-01T10:00:00Z", "level": "info", "message": "bare-z"},
            {"time": "2026-01-01T10:30:00.999Z", "level": "info", "message": "high-ms"},
            {"time": "2026-01-01T11:00:00.000Z", "level": "info", "message": "late"},
        ])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(
            "/logs/search_logs"
            f"?store_id={store_id}"
            "&end=2026-01-01T10:30:00"
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["total"] == 2
        messages = {row["message"] for row in body["results"]}
        assert messages == {"bare-z", "high-ms"}

    def test_search_start_includes_no_z_stored_logs(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([
            {"time": "2026-01-01T10:00:00.000", "level": "info", "message": "at-boundary"},
            {"time": "2026-01-01T10:30:00.000", "level": "info", "message": "later"},
        ])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(
            f"/logs/search_logs?store_id={store_id}&start=2026-01-01T10:00:00"
        )
        assert r.status_code == 200
        body = r.get_json()
        assert body["total"] == 2
        messages = {row["message"] for row in body["results"]}
        assert messages == {"at-boundary", "later"}

    def test_search_date_only_without_text(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([
            {"time": "2026-01-01T10:00:00.000Z", "level": "info", "message": "early"},
            {"time": "2026-01-01T11:00:00.000Z", "level": "info", "message": "late"},
        ])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(
            f"/logs/search_logs?store_id={store_id}&start=2026-01-01T10:30:00.000Z"
        )
        assert r.status_code == 200
        assert r.get_json()["total"] == 1

    def test_search_invalid_timestamp(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([{"time": "2026-01-01T00:00:00Z", "level": "info", "message": "x"}])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(f"/logs/search_logs?store_id={store_id}&start=not-a-date")
        assert r.status_code == 400

    def test_search_start_after_end(self, app_client, tmp_path):
        store_id = str(uuid.uuid4())
        db_path = snapshot_store.logstore_path(store_id)
        store = LogStore(db_path)
        store.insert_many([{"time": "2026-01-01T00:00:00Z", "level": "info", "message": "x"}])
        store.build_fts_index()
        store.close()
        log_store_registry.register(store_id, db_path)

        r = app_client.get(
            "/logs/search_logs"
            f"?store_id={store_id}"
            "&start=2026-01-02T00:00:00.000Z"
            "&end=2026-01-01T00:00:00.000Z"
        )
        assert r.status_code == 400

    def test_search_invalid_store_id(self, app_client):
        r = app_client.get("/logs/search_logs?store_id=../x")
        assert r.status_code == 400


class TestSnapshotRoutes:
    def test_list_snapshots_empty(self, app_client):
        r = app_client.get("/logs/list_snapshots")
        assert r.status_code == 200
        assert r.get_json() == []

    def test_load_valid_snapshot(self, app_client, minimal_template_data):
        snapshot_id = str(uuid.uuid4())
        store_id = str(uuid.uuid4())
        data = dict(minimal_template_data)
        data["log_store_id"] = store_id
        snapshot_store.save_snapshot(
            snapshot_id, "mongosync.log", 10, 1, store_id, data
        )
        r = app_client.get(f"/logs/load_snapshot/{snapshot_id}")
        assert r.status_code == 200

    def test_load_snapshot_invalid_id(self, app_client):
        r = app_client.get("/logs/load_snapshot/not-a-uuid")
        assert r.status_code == 200
        assert b"Snapshot Not Found" in r.data

    def test_delete_snapshot_invalid_id(self, app_client):
        r = app_client.delete("/logs/delete_snapshot/not-a-uuid")
        assert r.status_code == 404
