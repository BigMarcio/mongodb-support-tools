"""Tests for migration verifier metrics."""
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import PyMongoError

from lib.migration_verifier import (
    build_verifier_monitor_payload,
    get_failed_tasks,
    get_generation_history,
    get_generation_name,
    get_namespace_stats,
    get_verification_summary,
    plot_verifier_metrics,
)


class TestGetGenerationName:
    @pytest.mark.parametrize(
        "gen,expected",
        [
            (None, "Unknown"),
            (0, "Initial Verification"),
            (2, "Recheck #2"),
        ],
    )
    def test_get_generation_name(self, gen, expected):
        assert get_generation_name(gen) == expected


class TestGetVerificationSummary:
    def test_aggregates_status_counts(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [
            {"_id": "completed", "count": 5},
            {"_id": "failed", "count": 1},
            {"_id": "added", "count": 2},
        ]
        summary = get_verification_summary(db, generation=1)
        assert summary["completed"] == 5
        assert summary["failed"] == 1
        assert summary["pending"] == 2


class TestGetFailedTasks:
    def test_returns_tasks_from_aggregate(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [{"_id": "t1", "status": "failed"}]
        db.mismatches.find.return_value = []
        tasks = get_failed_tasks(db, generation=1)
        assert len(tasks) == 1


class TestGetNamespaceStats:
    def test_returns_namespace_rows(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [
            {"_id": "db.coll", "total": 10, "failed": 1}
        ]
        stats = get_namespace_stats(db, generation=1)
        assert stats[0]["_id"] == "db.coll"


class TestGetGenerationHistory:
    def test_empty_when_no_tasks(self):
        db = MagicMock()
        db.verification_tasks.find_one.return_value = None
        assert get_generation_history(db) == []

    def test_returns_history_for_latest_generations(self):
        db = MagicMock()
        db.verification_tasks.find_one.return_value = {"generation": 2}
        db.verification_tasks.aggregate.return_value = [
            {"_id": 2, "total_tasks": 5, "completed": 4, "failed": 1}
        ]
        history = get_generation_history(db, limit=2)
        assert history[0]["_id"] == 2


class TestBuildVerifierMonitorPayload:
    @patch("lib.app_config.get_database")
    def test_connection_error_returns_error_dict(self, mock_get_db):
        mock_get_db.side_effect = PyMongoError("fail")
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert "error" in result
        assert result["display"] is None
        assert result["connectivity"] is not None

    @patch("lib.app_config.get_database")
    def test_success_returns_display_payload(self, mock_get_db):
        db = MagicMock()
        db.verification_tasks.find_one.return_value = {"generation": 0}
        db.verification_tasks.aggregate.return_value = []
        find_cursor = MagicMock()
        find_cursor.limit.return_value = []
        db.verification_tasks.find.return_value = find_cursor
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert result.get("error") is None
        assert result["display"] is not None
        assert "stateBadge" in result["display"]
        assert "generations" in result["display"]
        assert "connectivity" in result

    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    @patch("lib.app_config.VERIFIER_GENERATION_LIMIT", 10)
    def test_uses_configured_generation_limit(self, mock_get_db, mock_gen_history):
        db = MagicMock()
        find_cursor = MagicMock()
        find_cursor.limit.return_value = []
        db.verification_tasks.find.return_value = find_cursor
        db.verification_tasks.aggregate.return_value = []
        mock_get_db.return_value = db
        build_verifier_monitor_payload("mongodb://localhost:27017")
        mock_gen_history.assert_called_once()
        assert mock_gen_history.call_args.kwargs["limit"] == 10


class TestPlotVerifierMetrics:
    @patch("lib.migration_verifier.render_template")
    def test_plot_renders_template(self, mock_render):
        mock_render.return_value = "html"
        result = plot_verifier_metrics()
        assert result == "html"
        mock_render.assert_called_once()
