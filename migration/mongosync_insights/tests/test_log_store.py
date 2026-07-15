"""Tests for SQLite log store."""
import json

import pytest

from lib.log_store import LogStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test.db"
    s = LogStore(str(db_path))
    yield s
    s.close()


def _doc(level="info", message="hello world", time="2026-01-01T10:00:00.000Z"):
    return {"time": time, "level": level, "message": message}


class TestLogStoreInsert:
    def test_insert_many_and_total(self, store):
        store.insert_many([_doc(), _doc(message="second line")])
        assert store.total_documents == 2

    def test_insert_line_buffered_until_flush(self, store):
        store.insert_line(json.dumps(_doc()), parsed=_doc())
        assert store.total_documents == 0
        store.flush()
        assert store.total_documents == 1


class TestLogStoreSearch:
    def test_find_by_level(self, store):
        store.insert_many([_doc(level="info"), _doc(level="error", message="boom")])
        store.build_fts_index()
        result = store.find({"level": "error"})
        assert result["total"] == 1
        assert "boom" in result["results"][0]["message"]

    def test_fts_text_search(self, store):
        store.insert_many([_doc(message="Replication progress update")])
        store.build_fts_index()
        result = store.find({"$text": "Replication"})
        assert result["total"] == 1

    def test_pagination(self, store):
        store.insert_many([_doc(message=f"line {i}") for i in range(5)])
        store.build_fts_index()
        page = store.find(skip=2, limit=2)
        assert len(page["results"]) == 2
        assert page["total"] == 5

    def test_find_by_timestamp_gte(self, store):
        store.insert_many([
            _doc(time="2026-01-01T10:00:00.000Z", message="early"),
            _doc(time="2026-01-01T10:30:00.000Z", message="middle"),
            _doc(time="2026-01-01T11:00:00.000Z", message="late"),
        ])
        store.build_fts_index()
        result = store.find({"timestamp_gte": "2026-01-01T10:30:00.000Z"})
        assert result["total"] == 2
        messages = {row["message"] for row in result["results"]}
        assert messages == {"middle", "late"}

    def test_find_by_timestamp_lte(self, store):
        store.insert_many([
            _doc(time="2026-01-01T10:00:00.000Z", message="early"),
            _doc(time="2026-01-01T10:30:00.000Z", message="middle"),
            _doc(time="2026-01-01T11:00:00.000Z", message="late"),
        ])
        store.build_fts_index()
        result = store.find({"timestamp_lte": "2026-01-01T10:30:00.999999Z"})
        assert result["total"] == 2
        messages = {row["message"] for row in result["results"]}
        assert messages == {"early", "middle"}

    def test_find_by_timestamp_range(self, store):
        store.insert_many([
            _doc(time="2026-01-01T10:00:00.000Z", message="early"),
            _doc(time="2026-01-01T10:30:00.000Z", message="middle"),
            _doc(time="2026-01-01T11:00:00.000Z", message="late"),
        ])
        store.build_fts_index()
        result = store.find({
            "timestamp_gte": "2026-01-01T10:15:00.000Z",
            "timestamp_lte": "2026-01-01T10:45:00.999999Z",
        })
        assert result["total"] == 1
        assert result["results"][0]["message"] == "middle"

    def test_find_by_timestamp_range_and_text(self, store):
        store.insert_many([
            _doc(time="2026-01-01T10:00:00.000Z", message="Replication early"),
            _doc(time="2026-01-01T10:30:00.000Z", message="Replication middle"),
            _doc(time="2026-01-01T11:00:00.000Z", message="Replication late"),
        ])
        store.build_fts_index()
        result = store.find({
            "$text": "Replication",
            "timestamp_gte": "2026-01-01T10:20:00.000Z",
            "timestamp_lte": "2026-01-01T10:45:00.999999Z",
        })
        assert result["total"] == 1
        assert result["results"][0]["message"] == "Replication middle"


class TestLogStoreUtilities:
    def test_fetch_latest_raw_lines(self, store):
        store.insert_many([
            _doc(time="2026-01-01T10:00:00.000Z"),
            _doc(time="2026-01-01T11:00:00.000Z"),
        ])
        lines = store.fetch_latest_raw_lines(1)
        assert len(lines) == 1

    def test_delete_removes_db_file(self, tmp_path):
        db_path = tmp_path / "del.db"
        s = LogStore(str(db_path))
        s.insert_many([_doc()])
        s.delete()
        assert not db_path.exists()
