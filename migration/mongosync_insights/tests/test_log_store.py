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
