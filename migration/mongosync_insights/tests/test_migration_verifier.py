"""Tests for migration verifier metrics."""
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId
from pymongo.errors import PyMongoError

from lib.app_config import VERIFIER_FAILED_TASKS_LIMIT
from lib.migration_verifier import (
    _derive_state_badge,
    _derive_state_badge_from_summary,
    _fetch_verifier_endpoint_data,
    _load_mismatches_for_tasks,
    _serialize_failed_task,
    build_verifier_monitor_payload,
    build_verifier_progress_display,
    build_verifier_summary_display,
    collect_verifier_metadata_warnings,
    get_current_generation,
    get_failed_tasks,
    get_failed_tasks_for_display,
    get_generation_doc_progress,
    get_generation_history,
    get_generation_name,
    get_generation_progress_stats,
    get_namespace_stats,
    get_persisted_namespace_statistics,
    get_persisted_namespace_statistics_batch,
    get_verification_completeness,
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
        db.generation.find_one.return_value = {"Generation": 2, "metadataVersion": 7}
        assert get_current_generation(db) == 2
        db.generation.find_one.assert_called_once_with({})

    def test_reads_lowercase_generation_field(self):
        db = MagicMock()
        db.generation.find_one.return_value = {"generation": 1, "metadataVersion": 7}
        assert get_current_generation(db) == 1

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


class TestCollectVerifierMetadataWarnings:
    def test_no_warning_when_generation_doc_missing(self):
        db = MagicMock()
        db.generation.find_one.return_value = None
        assert collect_verifier_metadata_warnings(db) == []

    def test_no_warning_when_version_matches(self):
        db = MagicMock()
        db.generation.find_one.return_value = {"metadataVersion": 7, "generation": 1}
        assert collect_verifier_metadata_warnings(db) == []

    def test_warns_on_version_mismatch(self, caplog):
        db = MagicMock()
        db.generation.find_one.return_value = {"metadataVersion": 6, "generation": 1}
        with caplog.at_level("WARNING"):
            warnings = collect_verifier_metadata_warnings(db)
        assert len(warnings) == 1
        assert "mismatch" in warnings[0].lower()
        assert "6" in warnings[0]
        assert "7" in warnings[0]
        assert any("mismatch" in r.message.lower() for r in caplog.records)

    def test_warns_when_metadata_version_missing(self, caplog):
        db = MagicMock()
        db.generation.find_one.return_value = {"generation": 1}
        with caplog.at_level("WARNING"):
            warnings = collect_verifier_metadata_warnings(db)
        assert len(warnings) == 1
        assert "metadataVersion" in warnings[0]
        assert any("metadataVersion" in r.message for r in caplog.records)

    def test_accepts_metadata_version_casing_variant(self):
        db = MagicMock()
        db.generation.find_one.return_value = {"MetadataVersion": 7, "Generation": 0}
        assert collect_verifier_metadata_warnings(db) == []

    def test_accepts_go_driver_lowercase_metadata_version_key(self):
        db = MagicMock()
        db.generation.find_one.return_value = {
            "generation": 1,
            "metadataversion": 7,
            "sourcechangereaderopt": "changeStream",
            "destinationchangereaderopt": "changeStream",
        }
        assert collect_verifier_metadata_warnings(db) == []


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

    def test_document_only_excludes_collection_tasks(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        get_failed_tasks(db, generation=1, document_only=True)
        pipeline = db.verification_tasks.aggregate.call_args[0][0]
        assert pipeline[0]["$match"]["type"] == {"$ne": "verifyCollection"}

    def test_attaches_mismatches_list(self):
        task_id = ObjectId()
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [{"_id": task_id, "status": "failed"}]
        db.mismatches.find.return_value = [
            {"taskID": task_id, "detail": {"id": "doc1"}},
        ]
        tasks = get_failed_tasks(db, generation=1)
        assert tasks[0]["mismatches"][0]["id"] == "doc1"


class TestSerializeFailedTask:
    def test_document_mismatch_with_details(self):
        task = {
            "type": "verifyDocuments",
            "status": "mismatch",
            "query_filter": {"namespace": "db.coll"},
            "begin_time": "2026-07-03T10:00:00",
            "mismatches": [{"id": "doc1", "field": "name", "details": "diff", "cluster": "dst"}],
        }
        row = _serialize_failed_task(task, generation=0)
        assert row["namespace"] == "db.coll"
        assert row["type"] == "verifyDocuments"
        assert row["status"] == "mismatch"
        assert "doc1" in row["details"]
        assert row["beginTime"] == "2026-07-03T10:00"

    def test_operational_failed_without_mismatches(self):
        task = {
            "type": "verifyDocuments",
            "status": "failed",
            "query_filter": {"namespace": "db.coll"},
            "mismatches": [],
        }
        row = _serialize_failed_task(task, generation=1)
        assert row["status"] == "failed"
        assert "migration-verifier logs" in row["details"]


class TestGetFailedTasksForDisplay:
    def test_excludes_verify_collection_tasks(self):
        db = MagicMock()
        with patch(
            "lib.migration_verifier.get_failed_tasks",
            return_value=[
                {
                    "type": "verifyDocuments",
                    "status": "mismatch",
                    "query_filter": {"namespace": "db.coll"},
                    "mismatches": [],
                },
            ],
        ) as mock_get_failed:
            rows = get_failed_tasks_for_display(db, 0)
        mock_get_failed.assert_called_once_with(
            db, 0, limit=VERIFIER_FAILED_TASKS_LIMIT, document_only=True,
        )
        assert len(rows) == 1
        assert rows[0]["type"] == "verifyDocuments"

    def test_respects_limit(self):
        db = MagicMock()
        tasks = [
            {
                "type": "verifyDocuments",
                "status": "failed",
                "query_filter": {"namespace": f"db.c{i}"},
                "begin_time": f"2026-07-0{i}T10:00:00",
                "mismatches": [],
            }
            for i in range(1, 6)
        ]
        with patch("lib.migration_verifier.get_failed_tasks", return_value=tasks):
            rows = get_failed_tasks_for_display(db, 0, limit=3)
        assert len(rows) == 3

    def test_sorts_by_begin_time_descending(self):
        db = MagicMock()
        with patch(
            "lib.migration_verifier.get_failed_tasks",
            return_value=[
                {
                    "type": "verifyDocuments",
                    "status": "failed",
                    "query_filter": {"namespace": "a"},
                    "begin_time": "2026-07-01T10:00:00",
                    "mismatches": [],
                },
                {
                    "type": "verifyDocuments",
                    "status": "failed",
                    "query_filter": {"namespace": "b"},
                    "begin_time": "2026-07-03T10:00:00",
                    "mismatches": [],
                },
            ],
        ):
            rows = get_failed_tasks_for_display(db, 0)
        assert len(rows) == 2
        assert rows[0]["beginTime"] >= rows[1]["beginTime"]


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


class TestGetPersistedNamespaceStatisticsBatch:
    def test_uses_generation_in_match(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = [
            {
                "_id": {"generation": 0, "namespace": "db.coll"},
                "docsCompared": 5,
                "totalDocs": 10,
            },
            {
                "_id": {"generation": 1, "namespace": "db.coll"},
                "docsCompared": 3,
                "totalDocs": 10,
            },
        ]
        result = get_persisted_namespace_statistics_batch(db, [0, 1])
        pipeline = db.verification_tasks.aggregate.call_args[0][0]
        assert pipeline[0]["$match"]["generation"] == {"$in": [0, 1]}
        assert result[0][0]["docsCompared"] == 5
        assert result[1][0]["docsCompared"] == 3

    def test_empty_generations_returns_empty_dict(self):
        assert get_persisted_namespace_statistics_batch(MagicMock(), []) == {}


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


class TestGetGenerationProgressStats:
    def test_sums_docs_and_partitions(self):
        db = MagicMock()
        with patch(
            "lib.migration_verifier.get_persisted_namespace_statistics",
            return_value=[
                {
                    "docsCompared": 40,
                    "totalDocs": 100,
                    "partitionsDone": 3,
                    "partitionsAdded": 1,
                    "partitionsProcessing": 0,
                },
                {
                    "docsCompared": 10,
                    "totalDocs": 50,
                    "partitionsDone": 2,
                    "partitionsAdded": 0,
                    "partitionsProcessing": 1,
                },
            ],
        ):
            stats = get_generation_progress_stats(db, 0)
        assert stats["docsCompared"] == 50
        assert stats["totalDocs"] == 150
        assert stats["partitionsDone"] == 5
        assert stats["partitionsTotal"] == 7

    def test_accepts_prefetched_ns_stats(self):
        db = MagicMock()
        stats = get_generation_progress_stats(db, 0, ns_stats=[
            {
                "docsCompared": 40,
                "totalDocs": 100,
                "partitionsDone": 3,
                "partitionsAdded": 1,
                "partitionsProcessing": 0,
            },
        ])
        assert stats["docsCompared"] == 40
        db.verification_tasks.aggregate.assert_not_called()


class TestGetVerificationCompleteness:
    def test_documents_and_namespaces(self):
        ns_doc_stats = [
            {
                "docsCompared": 40,
                "totalDocs": 100,
                "partitionsDone": 5,
                "partitionsAdded": 0,
                "partitionsProcessing": 0,
                "bytesCompared": 1000,
                "totalBytes": 2000,
            },
            {
                "docsCompared": 10,
                "totalDocs": 50,
                "partitionsDone": 2,
                "partitionsAdded": 1,
                "partitionsProcessing": 0,
                "bytesCompared": 500,
                "totalBytes": 1000,
            },
        ]
        task_summary = {
            "completed": 8,
            "failed": 1,
            "mismatch": 0,
            "pending": 2,
            "processing": 1,
        }
        result = get_verification_completeness(ns_doc_stats, task_summary, generation=0)
        assert result["documents"]["compared"] == 50
        assert result["documents"]["total"] == 150
        assert result["documents"]["percent"] == pytest.approx(33.3, abs=0.1)
        assert result["namespaces"]["complete"] == 1
        assert result["namespaces"]["total"] == 2
        assert result["partitions"]["done"] == 7
        assert result["partitions"]["total"] == 8
        assert result["tasks"]["completed"] == 8
        assert result["tasks"]["failed"] == 1
        assert result["tasks"]["pending"] == 3
        assert result["tasks"]["percentDone"] == pytest.approx(75.0)
        assert result["bytes"]["compared"] == 1500
        assert result["bytes"]["total"] == 3000

    def test_namespace_complete_rule(self):
        ns_doc_stats = [
            {"partitionsDone": 0, "partitionsAdded": 1, "partitionsProcessing": 0},
            {"partitionsDone": 3, "partitionsAdded": 0, "partitionsProcessing": 0},
        ]
        result = get_verification_completeness(ns_doc_stats, {}, generation=0)
        assert result["namespaces"]["complete"] == 1

    def test_bytes_omitted_for_recheck(self):
        ns_doc_stats = [
            {"bytesCompared": 100, "totalBytes": 200, "partitionsDone": 1},
        ]
        result = get_verification_completeness(ns_doc_stats, {}, generation=1)
        assert "bytes" not in result
        assert result["isRecheckGeneration"] is True

    def test_bytes_included_for_gen0(self):
        ns_doc_stats = [
            {"bytesCompared": 100, "totalBytes": 200, "partitionsDone": 1},
        ]
        result = get_verification_completeness(ns_doc_stats, {}, generation=0)
        assert result["bytes"]["percent"] == 50.0


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

    def test_accepts_current_gen_without_lookup(self):
        db = MagicMock()
        db.verification_tasks.aggregate.return_value = []
        with patch("lib.migration_verifier.get_current_generation") as mock_current:
            get_generation_history(db, limit=2, current_gen=3)
        mock_current.assert_not_called()


class TestDeriveStateBadge:
    def test_generation_zero_always_in_progress(self):
        badge = _derive_state_badge(0, {"failed": 5}, [{"namespace": "db.coll"}])
        assert badge == {"label": "IN PROGRESS", "color": "blue"}

    def test_pass_when_previous_gen_clean(self):
        badge = _derive_state_badge(1, {"failed": 0}, [])
        assert badge == {"label": "PASS", "color": "green"}

    def test_mismatches_when_previous_gen_has_failed_tasks(self):
        badge = _derive_state_badge(2, {"failed": 3}, [])
        assert badge == {"label": "MISMATCHES", "color": "yellow"}

    def test_mismatches_when_collection_mismatches_present(self):
        badge = _derive_state_badge(1, {"failed": 0}, [{"namespace": "db.coll"}])
        assert badge == {"label": "MISMATCHES", "color": "yellow"}

    def test_no_data_when_previous_gen_missing(self):
        badge = _derive_state_badge(1, None, [])
        assert badge == {"label": "NO DATA", "color": "gray"}


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
        db.generation.find_one.return_value = {"Generation": 0, "metadataVersion": 7}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        with patch("lib.migration_verifier.get_generation_history", return_value=[]):
            result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert result.get("error") is None
        assert result["warnings"] == []
        assert result["display"] is not None
        assert "stateBadge" in result["display"]
        assert "generations" in result["display"]
        assert "currentGeneration" in result["display"]
        assert "verificationCompleteness" in result["display"]
        assert "generationLimit" in result["display"]
        assert result["display"]["metadataAvailable"] is True
        assert "failedTasks" in result["display"]
        assert isinstance(result["display"]["failedTasks"], list)
        assert result["display"]["failedTasksLimit"] == VERIFIER_FAILED_TASKS_LIMIT
        assert result["display"]["previousGeneration"] is None
        assert result["display"]["stateBadge"] == {"label": "IN PROGRESS", "color": "blue"}
        assert "connectivity" in result

    @patch("lib.migration_verifier.get_failed_tasks_for_display", return_value=[])
    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_persisted_namespace_statistics_batch", return_value={})
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=1)
    @patch("lib.app_config.get_database")
    def test_state_badge_pass_from_previous_generation(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_stats,
        mock_summary,
        mock_history,
        mock_failed_tasks,
    ):
        mock_get_db.return_value = MagicMock()
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 10, "completed": 10, "failed": 0, "pending": 0},
            {"_id": 1, "total_tasks": 2, "completed": 0, "failed": 0, "pending": 2},
        ]
        mock_summary.return_value = {
            "completed": 0, "failed": 0, "mismatch": 0, "pending": 2, "processing": 0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert result["display"]["stateBadge"] == {"label": "PASS", "color": "green"}

    @patch("lib.migration_verifier.get_failed_tasks_for_display", return_value=[])
    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_persisted_namespace_statistics_batch", return_value={})
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=2)
    @patch("lib.app_config.get_database")
    def test_state_badge_mismatches_from_previous_generation(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_stats,
        mock_summary,
        mock_history,
        mock_failed_tasks,
    ):
        mock_get_db.return_value = MagicMock()
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 10, "completed": 10, "failed": 0, "pending": 0},
            {"_id": 1, "total_tasks": 5, "completed": 3, "failed": 2, "pending": 0},
            {"_id": 2, "total_tasks": 1, "completed": 0, "failed": 0, "pending": 1},
        ]
        mock_summary.return_value = {
            "completed": 0, "failed": 0, "mismatch": 0, "pending": 1, "processing": 0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert result["display"]["stateBadge"] == {"label": "MISMATCHES", "color": "yellow"}

    @patch("lib.migration_verifier.get_failed_tasks_for_display", return_value=[])
    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_persisted_namespace_statistics_batch", return_value={})
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=151)
    @patch("lib.app_config.get_database")
    def test_failed_tasks_use_previous_generation(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_stats,
        mock_summary,
        mock_history,
        mock_failed_tasks,
    ):
        mock_get_db.return_value = MagicMock()
        mock_summary.return_value = {
            "completed": 0, "failed": 0, "mismatch": 0, "pending": 0, "processing": 0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        mock_failed_tasks.assert_called_once_with(mock_get_db.return_value, 150)
        mock_coll_mm.assert_called_once_with(mock_get_db.return_value, 150)
        assert result["display"]["previousGeneration"] == 150

    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    @patch("lib.app_config.VERIFIER_GENERATION_LIMIT", 10)
    def test_uses_configured_generation_limit(self, mock_get_db, mock_gen_history):
        db = MagicMock()
        db.generation.find_one.return_value = {"Generation": 0, "metadataVersion": 7}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        mock_gen_history.assert_called_once()
        assert mock_gen_history.call_args[0][1] == 10
        assert result["display"]["generationLimit"] == 10

    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_persisted_namespace_statistics_batch")
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=0)
    @patch("lib.app_config.get_database")
    def test_current_gen_not_max_task_gen(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_batch,
        mock_summary,
        mock_history,
    ):
        mock_get_db.return_value = MagicMock()
        mock_ns_batch.return_value = {}
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 10, "completed": 8, "failed": 0, "pending": 2},
            {"_id": 1, "total_tasks": 1, "completed": 0, "failed": 0, "pending": 1},
        ]
        mock_summary.return_value = {
            "completed": 8, "failed": 0, "mismatch": 0, "pending": 2, "processing": 0,
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        gens = result["display"]["generations"]
        gen0 = next(g for g in gens if g["num"] == 0)
        gen1 = next(g for g in gens if g["num"] == 1)
        assert gen0["isCurrent"] is True
        assert gen1["isCurrent"] is False
        assert "CURRENT" in gen0["name"]

    @patch("lib.migration_verifier.get_generation_history")
    @patch("lib.migration_verifier.get_verification_summary")
    @patch("lib.migration_verifier.get_persisted_namespace_statistics_batch")
    @patch("lib.migration_verifier.get_collection_metadata_mismatches", return_value=[])
    @patch("lib.migration_verifier.get_current_generation", return_value=0)
    @patch("lib.app_config.get_database")
    def test_generations_overview_progress_fields(
        self,
        mock_get_db,
        mock_current_gen,
        mock_coll_mm,
        mock_ns_batch,
        mock_summary,
        mock_history,
    ):
        mock_get_db.return_value = MagicMock()
        mock_history.return_value = [
            {"_id": 0, "total_tasks": 4, "completed": 2, "failed": 0, "pending": 2},
        ]
        mock_summary.return_value = {
            "completed": 2, "failed": 0, "mismatch": 0, "pending": 2, "processing": 0,
        }
        mock_ns_batch.return_value = {
            0: [
                {
                    "_id": "db.coll",
                    "docsCompared": 50,
                    "totalDocs": 100,
                    "partitionsDone": 3,
                    "partitionsAdded": 1,
                    "partitionsProcessing": 0,
                },
            ],
        }
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        gen = result["display"]["generations"][0]
        completeness = result["display"]["verificationCompleteness"]
        assert gen["docsCompared"] == 50
        assert gen["totalDocs"] == 100
        assert gen["partitionsDone"] == 3
        assert gen["partitionsTotal"] == 4
        assert completeness["documents"]["percent"] == 50.0
        assert "generationDetails" not in result["display"]
        assert "initialCheckCompleteness" not in result["display"]

    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    def test_payload_includes_warnings_on_version_mismatch(self, mock_get_db, mock_history):
        db = MagicMock()
        db.generation.find_one.return_value = {"metadataVersion": 6, "generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        result = build_verifier_monitor_payload("mongodb://localhost:27017")
        assert len(result["warnings"]) == 1
        assert "mismatch" in result["warnings"][0].lower()


SAMPLE_MV_PROGRESS = {
    "phase": "recheck",
    "generation": 2,
    "generationStats": {
        "docsCompared": 100,
        "totalDocs": 200,
        "srcBytesCompared": 1024,
        "totalSrcBytes": 2048,
        "totalNamespaces": 5,
    },
    "verificationStatus": {
        "totalTasks": 10,
        "addedTasks": 2,
        "processingTasks": 1,
        "failedTasks": 0,
        "completedTasks": 7,
        "metadataMismatchTasks": 0,
    },
    "srcChangeStats": {
        "eventsPerSecond": 100.5,
        "lagSecs": 0,
        "bufferSaturation": 0.1,
        "eventCounts": {"insert": 1, "update": 2, "replace": 0, "delete": 0},
    },
    "dstChangeStats": {
        "eventsPerSecond": 50.0,
        "lagSecs": 1,
        "bufferSaturation": 0.2,
        "eventCounts": {"insert": 1, "update": 1, "replace": 0, "delete": 0},
    },
    "docsComparedPerSecond": 500.0,
    "srcBytesComparedPerSecond": 10000.0,
    "recentRecheckSecs": [10.5, 20.0],
    "totalRechecksDone": 42,
    "error": None,
}


class TestBuildVerifierProgressDisplay:
    def test_maps_core_fields(self):
        display = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        assert display["phase"] == "recheck"
        assert display["generation"] == 2
        assert display["phaseBadge"]["label"] == "RECHECK"
        assert display["documents"]["compared"] == 100
        assert display["documents"]["total"] == 200
        assert display["documents"]["percent"] == 50.0
        assert display["bytes"]["percent"] == 50.0
        assert display["tasks"]["completed"] == 7
        assert display["srcChangeStats"]["lagSecs"] == 0
        assert display["recentRecheckSecs"] == [10.5, 20.0]
        assert display["totalRechecksDone"] == 42

    def test_empty_progress_returns_none(self):
        assert build_verifier_progress_display(None) is None
        assert build_verifier_progress_display({}) is None


class TestBuildVerifierSummaryDisplay:
    def test_maps_core_fields(self):
        display = build_verifier_summary_display({
            "estCheckSecsRemaining": 120.5,
            "docMismatches": {
                "total": 3,
                "byType": {"content": 2, "missingOnDst": 1},
                "byNamespace": {"db.coll": 3},
            },
            "nsMismatches": [
                {
                    "type": "content",
                    "namespace": "db.coll",
                    "aspect": "index",
                    "detail": "diff",
                },
            ],
            "notes": ["Filtering document mismatches by minimum duration (60 seconds)"],
        }, min_duration_secs=60, ns_limit=5)
        assert display["estCheckSecsRemaining"] == pytest.approx(120.5)
        assert display["docMismatches"]["total"] == 3
        assert display["docMismatches"]["byType"]["content"] == 2
        assert display["nsMismatches"][0]["aspect"] == "index"
        assert display["minDurationSecs"] == 60
        assert len(display["notes"]) == 1

    def test_empty_summary_returns_none(self):
        assert build_verifier_summary_display(None) is None


class TestDeriveStateBadgeFromSummary:
    def test_generation_zero_in_progress(self):
        progress = {"generation": 0}
        summary = {"docMismatches": {"total": 5}, "nsMismatches": [{}]}
        badge = _derive_state_badge_from_summary(summary, progress)
        assert badge == {"label": "IN PROGRESS", "color": "blue"}

    def test_pass_when_no_mismatches(self):
        progress = {"generation": 2}
        summary = {"docMismatches": {"total": 0}, "nsMismatches": []}
        badge = _derive_state_badge_from_summary(summary, progress)
        assert badge == {"label": "PASS", "color": "green"}

    def test_mismatches_when_doc_or_ns_present(self):
        progress = {"generation": 1}
        summary = {"docMismatches": {"total": 1}, "nsMismatches": []}
        assert _derive_state_badge_from_summary(summary, progress)["label"] == "MISMATCHES"


class TestVerifierProgressEndpointPayload:
    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    def test_endpoint_only_payload(self, mock_fetch):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        summary = build_verifier_summary_display({
            "docMismatches": {"total": 0, "byType": {}, "byNamespace": {}},
            "nsMismatches": [],
        })
        mock_fetch.return_value = (progress, summary, [])
        result = build_verifier_monitor_payload(
            connection_string=None,
            endpoint_url="localhost:27020/api/v1/progress",
        )
        assert result["display"]["verificationProgress"]["phase"] == "recheck"
        assert result["display"]["verificationSummary"]["docMismatches"]["total"] == 0
        assert result["display"]["stateBadge"]["label"] == "PASS"
        assert result["display"]["metadataAvailable"] is False
        assert result["connectivity"]["rows"][0]["label"] == "Progress endpoint"

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    def test_endpoint_only_mismatches_badge(self, mock_fetch):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        summary = build_verifier_summary_display({
            "docMismatches": {"total": 2, "byType": {"content": 2}, "byNamespace": {}},
            "nsMismatches": [],
        })
        mock_fetch.return_value = (progress, summary, [])
        result = build_verifier_monitor_payload(
            connection_string=None,
            endpoint_url="localhost:27020/api/v1/progress",
        )
        assert result["display"]["stateBadge"]["label"] == "MISMATCHES"

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    def test_fetch_failure_adds_warning(self, mock_fetch):
        from lib.live_monitoring import ProgressFetchError

        mock_fetch.side_effect = ProgressFetchError("connection error", kind="connection")
        result = build_verifier_monitor_payload(
            connection_string=None,
            endpoint_url="localhost:27020/api/v1/progress",
        )
        assert any("not responding" in w for w in result["warnings"])
        assert result["display"] is None

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    def test_summary_failure_still_shows_progress(self, mock_fetch):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        mock_fetch.return_value = (
            progress,
            None,
            ["Verifier summary endpoint is not responding: timeout"],
        )
        result = build_verifier_monitor_payload(
            connection_string=None,
            endpoint_url="localhost:27020/api/v1/progress",
        )
        assert result["display"]["verificationProgress"]["phase"] == "recheck"
        assert "verificationSummary" not in result["display"]
        assert any("summary" in w.lower() for w in result["warnings"])

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    def test_skips_summary_when_include_summary_false(self, mock_fetch):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        mock_fetch.return_value = (progress, None, [])
        result = build_verifier_monitor_payload(
            connection_string=None,
            endpoint_url="localhost:27020/api/v1/progress",
            include_summary=False,
        )
        mock_fetch.assert_called_once_with(
            "localhost:27020/api/v1/progress", include_summary=False,
        )
        assert result["display"]["verificationProgress"]["phase"] == "recheck"
        assert "verificationSummary" not in result["display"]

    @patch("lib.migration_verifier._fetch_verifier_summary")
    @patch("lib.migration_verifier._fetch_verifier_progress")
    def test_fetch_endpoint_data_skips_summary_call(self, mock_progress, mock_summary):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        mock_progress.return_value = progress
        prog, summary, warnings = _fetch_verifier_endpoint_data(
            "localhost:27020/api/v1/progress", include_summary=False,
        )
        assert prog == progress
        assert summary is None
        assert warnings == []
        mock_progress.assert_called_once()
        mock_summary.assert_not_called()

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    @patch("lib.app_config.get_database")
    def test_skips_metadata_when_endpoint_and_include_metadata_false(
        self, mock_get_db, mock_fetch,
    ):
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        mock_fetch.return_value = (progress, None, [])
        result = build_verifier_monitor_payload(
            "mongodb://localhost:27017",
            endpoint_url="localhost:27020/api/v1/progress",
            include_summary=False,
            include_metadata=False,
        )
        mock_get_db.assert_not_called()
        assert result["display"]["verificationProgress"]["phase"] == "recheck"
        assert result["display"]["metadataAvailable"] is False
        assert "generations" not in result["display"]

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    def test_include_metadata_false_ignored_without_endpoint(
        self, mock_get_db, mock_history, mock_fetch,
    ):
        db = MagicMock()
        db.generation.find_one.return_value = {"metadataVersion": 7, "generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        mock_fetch.return_value = (None, None, [])

        result = build_verifier_monitor_payload(
            "mongodb://localhost:27017",
            include_metadata=False,
        )
        mock_get_db.assert_called_once()
        assert result["display"]["metadataAvailable"] is True
        assert "generations" in result["display"]

    @patch("lib.migration_verifier._fetch_verifier_endpoint_data")
    @patch("lib.migration_verifier.get_generation_history", return_value=[])
    @patch("lib.app_config.get_database")
    def test_both_endpoint_and_metadata(self, mock_get_db, mock_history, mock_fetch):
        db = MagicMock()
        db.generation.find_one.return_value = {"metadataVersion": 7, "generation": 0}
        db.verification_tasks.aggregate.return_value = []
        db.mismatches.find.return_value = []
        mock_get_db.return_value = db
        progress = build_verifier_progress_display(SAMPLE_MV_PROGRESS)
        summary = build_verifier_summary_display({
            "docMismatches": {"total": 1, "byType": {}, "byNamespace": {}},
            "nsMismatches": [],
        })
        mock_fetch.return_value = (progress, summary, [])

        result = build_verifier_monitor_payload(
            "mongodb://localhost:27017",
            endpoint_url="localhost:27020/api/v1/progress",
        )
        assert result["display"]["verificationProgress"]["phase"] == "recheck"
        assert result["display"]["verificationSummary"]["docMismatches"]["total"] == 1
        assert result["display"]["stateBadge"]["label"] == "IN PROGRESS"
        assert result["display"]["metadataAvailable"] is True
        assert "generations" in result["display"]
        assert "verificationCompleteness" not in result["display"]


class TestPlotVerifierMetrics:
    @patch("lib.migration_verifier.render_template")
    def test_plot_renders_template(self, mock_render):
        mock_render.return_value = "html"
        result = plot_verifier_metrics()
        assert result == "html"
        mock_render.assert_called_once()
