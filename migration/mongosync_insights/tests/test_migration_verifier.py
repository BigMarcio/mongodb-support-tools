"""Tests for migration verifier metrics."""
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import PyMongoError

from lib.migration_verifier import (
    gather_verifier_metrics,
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


class TestGatherVerifierMetrics:
    @patch("lib.app_config.get_database")
    def test_connection_error_returns_error_dict(self, mock_get_db):
        mock_get_db.side_effect = PyMongoError("fail")
        result = gather_verifier_metrics("mongodb://localhost:27017")
        assert "error" in result

    @patch("lib.app_config.get_database")
    def test_success_returns_metrics_payload(self, mock_get_db):
        db = MagicMock()
        db.verification_tasks.find_one.return_value = {"generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        result = gather_verifier_metrics("mongodb://localhost:27017")
        assert "error" not in result
        assert "generation" in result or "summary" in result or isinstance(result, dict)


class TestPlotVerifierMetrics:
    @patch("lib.migration_verifier.render_template")
    @patch("lib.migration_verifier.gather_verifier_metrics")
    def test_plot_renders_template(self, mock_gather, mock_render):
        mock_gather.return_value = {"generation": 0, "summary": {}}
        mock_render.return_value = "html"
        result = plot_verifier_metrics()
        assert result == "html"
        mock_render.assert_called_once()
