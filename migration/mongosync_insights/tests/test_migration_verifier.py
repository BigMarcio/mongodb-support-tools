"""Tests for migration verifier metrics."""
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId
from pymongo.errors import PyMongoError

from lib.migration_verifier import (
    _load_mismatches_for_tasks,
    build_verifier_monitor_payload,
    get_current_generation,
    get_failed_tasks,
    get_generation_doc_progress,
    get_generation_history,
    get_generation_name,
    get_namespace_stats,
    get_persisted_namespace_statistics,
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


class TestGetCurrentGeneration:
    def test_reads_generation_collection(self):
        db = MagicMock()
        db.generation.find_one.return_value = {"Generation": 2}
        assert get_current_generation(db) == 2
        db.generation.find_one.assert_called_once()

    def test_fallback_to_max_task_generation(self):
        db = MagicMock()
        db.generation.find_one.return_value = None
        db.verification_tasks.find_one.return_value = {"generation": 3}
        assert get_current_generation(db) == 3

    def test_returns_none_when_no_data(self):
        db = MagicMock()
        db.generation.find_one.return_value = None
        db.verification_tasks.find_one.return_value = None
        assert get_current_generation(db) is None


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

    def test_excludes_primary_tasks(self):
        db = MagicMock()
        get_verification_summary(db, generation=0)
        pipeline = db.verification_tasks.aggregate.call_args[0][0]
        assert pipeline[0]["$match"]["type"] == {"$nin": ["primary"]}


class TestLoadMismatchesForTasks:
    def test_uses_task_id_field(self):
        task_id = ObjectId()
        db = MagicMock()
        db.mismatches.find.return_value = [
            {"taskID": task_id, "detail": {"id": "idx1", "details": "Missing"}},
        ]
        result = _load_mismatches_for_tasks(db, 0, [task_id])
        assert task_id in result
        assert len(result[task_id]) == 1
        db.mismatches.find.assert_called_with(
            {"generation": 0, "taskID": {"$in": [task_id]}},
            {"taskID": 1, "detail": 1},
        )

    def test_legacy_task_field_fallback(self):
        task_id = ObjectId()
        db = MagicMock()
        db.mismatches.find.side_effect = [
            [],
            [{"task": task_id, "detail": {"id": "legacy"}}],
        ]
        result = _load_mismatches_for_tasks(db, 0, [task_id])
        assert result[task_id][0]["id"] == "legacy"


class TestGetFailedTasks:
    def test_returns_tasks_from_aggregate(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [{"_id": "t1", "status": "failed"}]
        db.mismatches.find.return_value = []
        tasks = get_failed_tasks(db, generation=1)
        assert len(tasks) == 1

    def test_attaches_mismatches_list(self):
        task_id = ObjectId()
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [{"_id": task_id, "status": "failed"}]
        db.mismatches.find.return_value = [
            {"taskID": task_id, "detail": {"id": "doc1"}},
        ]
        tasks = get_failed_tasks(db, generation=1)
        assert tasks[0]["mismatches"][0]["id"] == "doc1"


class TestGetNamespaceStats:
    def test_returns_namespace_rows(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [
            {"_id": "db.coll", "total": 10, "failed": 1}
        ]
        stats = get_namespace_stats(db, generation=1)
        assert stats[0]["_id"] == "db.coll"


class TestGetPersistedNamespaceStatistics:
    def test_runs_namespace_pipeline(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [
            {"_id": "db.coll", "docsCompared": 5, "totalDocs": 10},
        ]
        stats = get_persisted_namespace_statistics(db, generation=0)
        assert stats[0]["docsCompared"] == 5
        pipeline = db.verification_tasks.aggregate.call_args[0][0]
        assert pipeline[0]["$match"]["generation"] == 0


class TestGetGenerationDocProgress:
    def test_sums_namespace_stats(self):
        db = MagicMock()
        with patch(
            "lib.migration_verifier.get_persisted_namespace_statistics",
            return_value=[
                {"docsCompared": 40, "totalDocs": 100},
                {"docsCompared": 10, "totalDocs": 50},
            ],
        ):
            progress = get_generation_doc_progress(db, 0)
        assert progress["docsCompared"] == 50
        assert progress["totalDocs"] == 150
        assert progress["docPercentComplete"] == pytest.approx(33.3, abs=0.1)


class TestGetGenerationHistory:
    def test_empty_when_no_current_generation(self):
        db = MagicMock()
        with patch("lib.migration_verifier.get_current_generation", return_value=None):
            assert get_generation_history(db) == []

    def test_returns_history_for_latest_generations(self):
        db = MagicMock()
        with patch("lib.migration_verifier.get_current_generation", return_value=2):
            db.verification_tasks.aggregate.return_value = [
                {"_id": 2, "total_tasks": 5, "completed": 4, "failed": 1, "pending": 0}
            ]
            history = get_generation_history(db, limit=2)
        assert history[0]["_id"] == 2

    def test_excludes_primary_tasks(self):
        db = MagicMock()
        with patch("lib.migration_verifier.get_current_generation", return_value=0):
            get_generation_history(db, limit=2)
        pipeline = db.verification_tasks.aggregate.call_args[0][0]
        assert pipeline[0]["$match"]["type"] == {"$nin": ["primary"]}


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
        db.generation.find_one.return_value = {"Generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        with patch("lib.migration_verifier.get_generation_history", return_value=[]):
            result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert result.get("error") is None
        assert result["display"] is not None
        assert "stateBadge" in result["display"]
        assert "generations" in result["display"]
        assert "currentGeneration" in result["display"]
        assert "connectivity" in result

    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    @patch("lib.app_config.VERIFIER_GENERATION_LIMIT", 10)
    def test_uses_configured_generation_limit(self, mock_get_db, mock_gen_history):
        db = MagicMock()
        db.generation.find_one.return_value = {"Generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        build_verifier_monitor_payload("mongodb://localhost:27017")
        mock_gen_history.assert_called_once()
        assert mock_gen_history.call_args.kwargs["limit"] == 10

    @patch("lib.migration_verifier.get_generation_doc_progress")
    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_failed_tasks", return_value=[])
    @patch("lib.migration_verifier.get_persisted_namespace_statistics", return_value=[])
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=0)
    @patch("lib.app_config.get_database")
    def test_current_gen_not_max_task_gen(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_stats,
        mock_failed,
        mock_summary,
        mock_history,
        mock_doc_progress,
    ):
        mock_get_db.return_value = MagicMock()
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 10, "completed": 8, "failed": 0, "pending": 2},
            {"_id": 1, "total_tasks": 1, "completed": 0, "failed": 0, "pending": 1},
        ]
        mock_summary.return_value = {
            "completed": 8, "failed": 0, "mismatch": 0, "pending": 2, "processing": 0,
        }
        mock_doc_progress.return_value = {
            "docsCompared": 80, "totalDocs": 100, "docPercentComplete": 80.0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        gens = result["display"]["generations"]
        gen0 = next(g for g in gens if g["num"] == 0)
        gen1 = next(g for g in gens if g["num"] == 1)
        assert gen0["isCurrent"] is True
        assert gen1["isCurrent"] is False
        assert "CURRENT" in gen0["name"]

    @patch("lib.migration_verifier.get_generation_doc_progress")
    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_failed_tasks", return_value=[])
    @patch("lib.migration_verifier.get_persisted_namespace_statistics", return_value=[])
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=0)
    @patch("lib.app_config.get_database")
    def test_dual_progress_fields_in_generation_details(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_stats,
        mock_failed,
        mock_summary,
        mock_history,
        mock_doc_progress,
    ):
        mock_get_db.return_value = MagicMock()
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 4, "completed": 2, "failed": 0, "pending": 2},
        ]
        mock_summary.return_value = {
            "completed": 2, "failed": 0, "mismatch": 0, "pending": 2, "processing": 0,
        }
        mock_doc_progress.return_value = {
            "docsCompared": 50, "totalDocs": 100, "docPercentComplete": 50.0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        detail = result["display"]["generationDetails"][0]
        assert detail["percentComplete"] == 50.0
        assert detail["docPercentComplete"] == 50.0
        assert detail["docsCompared"] == 50
        assert detail["totalDocs"] == 100


class TestPlotVerifierMetrics:
    @patch("lib.migration_verifier.render_template")
    def test_plot_renders_template(self, mock_render):
        mock_render.return_value = "html"
        result = plot_verifier_metrics()
        assert result == "html"
        mock_render.assert_called_once()
