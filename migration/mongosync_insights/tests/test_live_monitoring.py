"""Tests for live monitoring fetch and builder helpers."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from lib.app_config import (
    MONGOSYNC_PROGRESS_TIMEOUT_SECS,
    VERIFIER_PROGRESS_TIMEOUT_SECS,
    VERIFIER_SUMMARY_TIMEOUT_SECS,
)
from lib.live_monitoring import (
    ProgressFetchError,
    _build_collections_copied_label,
    _build_db_only_metrics,
    _build_direction,
    _build_index_building_card,
    _build_metadata_metrics,
    _build_progress_metrics,
    _build_sync_card,
    _derive_state_badge,
    _state_badge_color,
    _verification_side_kv,
    build_live_monitor_payload,
    fetch_progress,
    fetch_summary,
    fetch_verifier_progress,
    progress_monitor_no_config_response,
)


class TestFetchProgress:
    @patch("lib.live_monitoring.requests.get")
    def test_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"progress": {"state": "RUNNING", "warnings": ["w1"]}}
        mock_get.return_value = mock_resp
        progress, warnings = fetch_progress("host:27182/api/v1/progress")
        assert progress["state"] == "RUNNING"
        assert warnings == ["w1"]
        mock_get.assert_called_once_with(
            "http://host:27182/api/v1/progress",
            timeout=MONGOSYNC_PROGRESS_TIMEOUT_SECS,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_timeout_raises(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout("timeout")
        with pytest.raises(ProgressFetchError, match="Timeout") as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "timeout"

    @patch("lib.live_monitoring.requests.get")
    def test_connection_error_raises(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("conn")
        with pytest.raises(ProgressFetchError) as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "connection"

    @patch("lib.live_monitoring.requests.get")
    def test_http_error_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=500)
        )
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "http"

    @patch("lib.live_monitoring.requests.get")
    def test_invalid_json_raises(self, mock_get):
        import json as json_mod

        mock_resp = MagicMock()
        mock_resp.json.side_effect = json_mod.JSONDecodeError("bad", "", 0)
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "json"


class TestFetchVerifierProgress:
    @patch("lib.live_monitoring.requests.get")
    def test_uses_verifier_progress_timeout(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"progress": {"generation": 0}}
        mock_get.return_value = mock_resp
        progress, _warnings = fetch_verifier_progress("host:27020/api/v1/progress")
        assert progress["generation"] == 0
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/progress",
            timeout=VERIFIER_PROGRESS_TIMEOUT_SECS,
        )


class TestFetchSummary:
    @patch("lib.live_monitoring.requests.get")
    def test_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "docMismatches": {"total": 1},
            "nsMismatches": [],
        }
        mock_get.return_value = mock_resp
        summary = fetch_summary("host:27020/api/v1/summary")
        assert summary["docMismatches"]["total"] == 1
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/summary",
            params=None,
            timeout=VERIFIER_SUMMARY_TIMEOUT_SECS,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_min_duration_query_param(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"docMismatches": {"total": 0}, "nsMismatches": []}
        mock_get.return_value = mock_resp
        fetch_summary("host:27020/api/v1/summary", min_duration_secs=60)
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/summary",
            params={"minDurationSecs": 60},
            timeout=VERIFIER_SUMMARY_TIMEOUT_SECS,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_api_error_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"error": "verifier busy"}
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError, match="verifier busy") as exc:
            fetch_summary("host:27020/api/v1/summary")
        assert exc.value.kind == "api"

    @patch("lib.live_monitoring.requests.get")
    def test_http_error_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=503)
        )
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            fetch_summary("host:27020/api/v1/summary")
        assert exc.value.kind == "http"


class TestStateBadge:
    @pytest.mark.parametrize(
        "state,color",
        [
            ("RUNNING", "green"),
            ("ERROR", "red"),
            ("UNKNOWN", "gray"),
        ],
    )
    def test_state_badge_color(self, state, color):
        assert _state_badge_color(state) == color

    def test_derive_state_badge(self):
        badge = _derive_state_badge({"state": "running"})
        assert badge["label"] == "RUNNING"
        assert badge["color"] == "green"


class TestBuildHelpers:
    @patch("lib.live_monitoring.fetch_metadata_status")
    @patch("lib.live_monitoring.fetch_progress")
    def test_build_data_sources_both_active(self, mock_progress, mock_metadata):
        mock_progress.return_value = ({"state": "running"}, [])
        mock_metadata.return_value = {"state": "running"}
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            connection_string="mongodb://localhost:27017",
        )
        assert result["dataSources"]["mode"] == "both"
        assert len(result["dataSources"]["badges"]) == 2
        assert result["dataSources"]["badges"][0]["id"] == "progress"
        assert result["dataSources"]["badges"][0]["status"] == "active"
        assert result["dataSources"]["badges"][1]["id"] == "metadata"
        assert result["dataSources"]["badges"][1]["status"] == "active"
        for badge in result["dataSources"]["badges"]:
            assert "host" not in badge["label"].lower()
            assert "localhost" not in str(badge)

    @patch("lib.live_monitoring.fetch_progress")
    def test_build_data_sources_endpoint_unavailable(self, mock_progress):
        mock_progress.side_effect = ProgressFetchError("timeout", kind="timeout")
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            connection_string=None,
        )
        assert result["dataSources"]["mode"] == "endpoint"
        assert result["dataSources"]["badges"][0]["status"] == "unavailable"
        assert result["dataSources"]["badges"][0]["color"] == "yellow"

    def test_build_data_sources_none_when_empty(self):
        from lib.live_monitoring import progress_monitor_no_config_response

        assert progress_monitor_no_config_response()["dataSources"] is None

    def test_build_metadata_metrics_live_labels(self):
        metrics = _build_metadata_metrics({"start": "2026-01-01", "buildIndexes": "After Data Copy"})
        labels = [m["label"] for m in metrics]
        assert "Migration Start time" in labels
        assert "Finish" in labels
        assert "Migration Committed" not in labels
        assert "Build Indexes" in labels
        start_metric = next(m for m in metrics if m["label"] == "Migration Start time")
        assert "initializing collections and indexes" in start_metric["title"]
        assert "/start" in start_metric["title"]

    def test_build_metadata_metrics_summary_labels(self):
        metrics = _build_metadata_metrics(
            {"start": "2026-01-01", "finish": "2026-01-02"},
            summary=True,
        )
        labels = [m["label"] for m in metrics]
        assert "Migration Committed" in labels
        assert "Finish" not in labels

    def test_build_progress_metrics(self):
        metrics = _build_progress_metrics(
            {
                "canCommit": True,
                "canWrite": False,
                "totalEventsApplied": 10,
                "estimatedSecondsToCEACatchup": 90,
            },
            1582,
        )
        by_label = {m["label"]: m for m in metrics}
        assert by_label["Can commit"]["value"] == "TRUE"
        assert by_label["Events applied"]["value"] == "10"
        assert by_label["Lag time"]["value"] == "26m 22s"
        assert by_label["Lag time"]["title"] == "1,582 seconds"
        assert by_label["Lag time"]["highLag"] is True
        assert by_label["Catch-up estimate"]["value"] == "1m 30s"
        assert by_label["Catch-up estimate"]["title"] == "90 seconds"
        assert "title" not in by_label["Events applied"]

    def test_db_only_metrics_lag_title(self):
        metrics = _build_db_only_metrics(90)
        by_label = {m["label"]: m for m in metrics}
        assert by_label["Lag time"]["value"] == "1m 30s"
        assert by_label["Lag time"]["title"] == "90 seconds"
        assert by_label["Lag time"]["highLag"] is True
        assert "title" not in by_label["Catch-up estimate"]

    def test_verification_lag_title(self):
        rows = {r["label"]: r for r in _verification_side_kv({"lagTimeSeconds": 75})}
        assert rows["Lag time"]["value"] == "1m 15s"
        assert rows["Lag time"]["title"] == "75 seconds"
        assert rows["Lag time"]["highLag"] is True

    def test_lag_time_not_highlighted_at_one_minute(self):
        metrics = _build_progress_metrics({}, 60)
        by_label = {m["label"]: m for m in metrics}
        assert "highLag" not in by_label["Lag time"]

    def test_lag_time_highlighted_over_one_minute(self):
        metrics = _build_progress_metrics({}, 61)
        by_label = {m["label"]: m for m in metrics}
        assert by_label["Lag time"]["highLag"] is True

    def test_build_direction_from_progress(self):
        progress = {
            "directionMapping": {"Source": "src:27017", "Destination": "dst:27017"},
            "source": {"pingLatencyMs": 5},
            "destination": {"pingLatencyMs": 10},
        }
        card = _build_direction(progress)
        assert card["source"]["address"] == "src:27017"
        assert card["destination"]["ping"] == "10 ms"


class TestCollectionsCopiedLabel:
    def test_prefers_atlas_live_migrate_metrics_from_progress(self):
        label = _build_collections_copied_label(
            metadata={"collectionsCopied": 1, "collectionsTotal": 4},
            progress={
                "atlasLiveMigrateMetrics": {
                    "initialNumCollectionsCopied": 267,
                    "initialNumCollectionsTotal": 273,
                }
            },
        )
        assert label == "Collections copied: 267 of 273"

    def test_falls_back_to_metadata_without_atlas_metrics(self):
        label = _build_collections_copied_label(
            metadata={"collectionsCopied": 10, "collectionsTotal": 20},
            progress={"info": "collection copy"},
        )
        assert label == "Collections copied: 10 of 20"

    def test_sync_card_uses_atlas_collection_totals(self):
        sync = _build_sync_card(
            progress={
                "info": "collection copy",
                "state": "RUNNING",
                "collectionCopy": {
                    "estimatedCopiedBytes": 50,
                    "estimatedTotalBytes": 100,
                },
                "atlasLiveMigrateMetrics": {
                    "initialNumCollectionsCopied": 267,
                    "initialNumCollectionsTotal": 273,
                },
                "indexBuilding": {
                    "collectionsFinished": 2,
                    "collectionsTotal": 21,
                    "indexesBuilt": 7,
                    "totalIndexesToBuild": 37,
                },
            },
            metadata={"partitionsCopied": 2601, "partitionsTotal": 2601},
            progress_available=True,
        )
        assert sync["collectionsCopiedLabel"] == "Collections copied: 267 of 273"


    def test_sync_card_copy_percent_from_bytes_not_collections(self):
        sync = _build_sync_card(
            progress={
                "info": "collection copy",
                "state": "RUNNING",
                "collectionCopy": {
                    "estimatedCopiedBytes": 90,
                    "estimatedTotalBytes": 100,
                },
                "atlasLiveMigrateMetrics": {
                    "initialNumCollectionsCopied": 267,
                    "initialNumCollectionsTotal": 273,
                },
            },
            metadata={"partitionsCopied": 10, "partitionsTotal": 200},
            progress_available=True,
        )
        assert sync["copyPercent"] == 90.0
        assert sync["collectionsCopiedLabel"] == "Collections copied: 267 of 273"


class TestIndexBuildingCard:
    def test_index_building_card_includes_collections_finished_metric(self):
        card = _build_index_building_card(1, 10, 2, 5)
        labels = [metric["label"] for metric in card["metrics"]]
        assert labels == ["Indexes built", "Collections finished building indexes"]
        assert card["metrics"][1]["value"] == "2 / 5"


class TestProgressMonitorNoConfig:
    def test_response_shape(self):
        resp = progress_monitor_no_config_response()
        assert "error" in resp
        assert resp["display"] is None
