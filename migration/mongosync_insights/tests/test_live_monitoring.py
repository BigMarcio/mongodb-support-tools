"""Tests for live monitoring fetch and builder helpers."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from lib.app_config import (
    MONGOSYNC_PROGRESS_TIMEOUT_SECS,
    VERIFIER_HEAVY_API_TIMEOUT_SECS,
    VERIFIER_PROGRESS_TIMEOUT_SECS,
)
from lib.live_monitoring import (
    ProgressFetchError,
    _build_collections_copied_label,
    _build_db_only_metrics,
    _build_direction,
    _build_index_building_card,
    _build_lag_breakdown,
    _build_metadata_metrics,
    _build_progress_metrics,
    _build_sync_card,
    _derive_state_badge,
    _lag_from_metadata,
    _state_badge_color,
    _verification_side_kv,
    build_live_monitor_payload,
    fetch_progress,
    fetch_summary,
    fetch_verifier_progress,
    progress_monitor_no_config_response,
    stream_verifier_mismatches,
)


def _ok_response(**kwargs):
    """MagicMock response that behaves like a non-redirect 200."""
    resp = MagicMock(**kwargs)
    resp.status_code = 200
    return resp


def _redirect_response(location="http://127.0.0.1:9200/latest/meta-data/"):
    resp = MagicMock()
    resp.status_code = 302
    resp.headers = {"Location": location}
    return resp


class TestFetchProgress:
    @patch("lib.live_monitoring.requests.get")
    def test_success(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.json.return_value = {"progress": {"state": "RUNNING", "warnings": ["w1"]}}
        mock_get.return_value = mock_resp
        progress, warnings = fetch_progress("host:27182/api/v1/progress")
        assert progress["state"] == "RUNNING"
        assert warnings == ["w1"]
        mock_get.assert_called_once_with(
            "http://host:27182/api/v1/progress",
            timeout=MONGOSYNC_PROGRESS_TIMEOUT_SECS,
            allow_redirects=False,
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
        mock_resp = _ok_response()
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

        mock_resp = _ok_response()
        mock_resp.json.side_effect = json_mod.JSONDecodeError("bad", "", 0)
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "json"


class TestEndpointRedirectAndValidation:
    """Endpoint must be reached exactly as validated (no redirects, no free-form URLs)."""

    @patch("lib.live_monitoring.requests.get")
    def test_redirect_is_not_followed(self, mock_get):
        mock_get.return_value = _redirect_response()
        with pytest.raises(ProgressFetchError, match="redirect") as exc:
            fetch_progress("attacker.example.com:80/api/v1/progress")
        assert exc.value.kind == "redirect"
        assert mock_get.call_args.kwargs["allow_redirects"] is False
        assert mock_get.call_count == 1

    @patch("lib.live_monitoring.requests.get")
    def test_summary_redirect_is_not_followed(self, mock_get):
        mock_get.return_value = _redirect_response()
        with pytest.raises(ProgressFetchError) as exc:
            fetch_summary("attacker.example.com:80/api/v1/summary")
        assert exc.value.kind == "redirect"

    @patch("lib.live_monitoring.requests.get")
    def test_mismatch_stream_redirect_is_not_followed(self, mock_get):
        mock_get.return_value = _redirect_response()
        with pytest.raises(ProgressFetchError) as exc:
            stream_verifier_mismatches(
                "attacker.example.com:80/api/v1/docMismatches",
                label="document mismatches",
            )
        assert exc.value.kind == "redirect"

    @pytest.mark.parametrize(
        "endpoint",
        [
            "host:27182/api/v1/progress/../../secret",
            "host:27182/latest/meta-data/",
            "http://host:27182/api/v1/progress",
            "host:99999/api/v1/progress",
            "",
        ],
    )
    @patch("lib.live_monitoring.requests.get")
    def test_invalid_endpoint_never_requested(self, mock_get, endpoint):
        with pytest.raises(ProgressFetchError) as exc:
            fetch_progress(endpoint)
        assert exc.value.kind == "endpoint"
        mock_get.assert_not_called()

    @patch("lib.live_monitoring.endpoint_host_allowed", return_value=False)
    @patch("lib.live_monitoring.requests.get")
    def test_host_allowlist_blocks_endpoint(self, mock_get, _allowed):
        with pytest.raises(ProgressFetchError, match="allowed hosts") as exc:
            fetch_progress("host:27182/api/v1/progress")
        assert exc.value.kind == "endpoint"
        mock_get.assert_not_called()


class TestFetchVerifierProgress:
    @patch("lib.live_monitoring.requests.get")
    def test_uses_verifier_progress_timeout(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.json.return_value = {"progress": {"generation": 0}}
        mock_get.return_value = mock_resp
        progress, _warnings = fetch_verifier_progress("host:27020/api/v1/progress")
        assert progress["generation"] == 0
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/progress",
            timeout=VERIFIER_PROGRESS_TIMEOUT_SECS,
            allow_redirects=False,
        )


class TestFetchSummary:
    @patch("lib.live_monitoring.requests.get")
    def test_success(self, mock_get):
        mock_resp = _ok_response()
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
            timeout=VERIFIER_HEAVY_API_TIMEOUT_SECS,
            allow_redirects=False,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_min_duration_query_param(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.json.return_value = {"docMismatches": {"total": 0}, "nsMismatches": []}
        mock_get.return_value = mock_resp
        fetch_summary("host:27020/api/v1/summary", min_duration_secs=60)
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/summary",
            params={"minDurationSecs": 60},
            timeout=VERIFIER_HEAVY_API_TIMEOUT_SECS,
            allow_redirects=False,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_api_error_raises(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.json.return_value = {"error": "verifier busy"}
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError, match="verifier busy") as exc:
            fetch_summary("host:27020/api/v1/summary")
        assert exc.value.kind == "api"

    @patch("lib.live_monitoring.requests.get")
    def test_http_error_raises(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=503)
        )
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            fetch_summary("host:27020/api/v1/summary")
        assert exc.value.kind == "http"


class TestStreamVerifierMismatches:
    @patch("lib.live_monitoring.requests.get")
    def test_success(self, mock_get):
        mock_resp = _ok_response()
        mock_get.return_value = mock_resp
        response = stream_verifier_mismatches(
            "host:27020/api/v1/docMismatches", label="document mismatches",
        )
        assert response is mock_resp
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/docMismatches",
            params=None,
            stream=True,
            timeout=VERIFIER_HEAVY_API_TIMEOUT_SECS,
            allow_redirects=False,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_min_duration_query_param(self, mock_get):
        mock_resp = _ok_response()
        mock_get.return_value = mock_resp
        stream_verifier_mismatches(
            "host:27020/api/v1/docMismatches",
            params={"minDurationSecs": 60},
            label="document mismatches",
        )
        mock_get.assert_called_once_with(
            "http://host:27020/api/v1/docMismatches",
            params={"minDurationSecs": 60},
            stream=True,
            timeout=VERIFIER_HEAVY_API_TIMEOUT_SECS,
            allow_redirects=False,
        )

    @patch("lib.live_monitoring.requests.get")
    def test_ns_mismatches_endpoint_accepted(self, mock_get):
        mock_resp = _ok_response()
        mock_get.return_value = mock_resp
        response = stream_verifier_mismatches(
            "host:27020/api/v1/nsMismatches", label="namespace mismatches",
        )
        assert response is mock_resp

    @patch("lib.live_monitoring.requests.get")
    def test_http_error_raises(self, mock_get):
        mock_resp = _ok_response()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=503)
        )
        mock_get.return_value = mock_resp
        with pytest.raises(ProgressFetchError) as exc:
            stream_verifier_mismatches(
                "host:27020/api/v1/nsMismatches", label="namespace mismatches",
            )
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

    @patch("lib.live_monitoring.fetch_metadata_status")
    @patch("lib.live_monitoring.fetch_progress")
    def test_progress_only_sync_card_only(self, mock_progress, mock_metadata):
        mock_progress.return_value = (
            {
                "state": "RUNNING",
                "info": "copy",
                "collectionCopy": {"estimatedCopiedBytes": 1, "estimatedTotalBytes": 2},
            },
            [],
        )
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            progress_only=True,
        )
        mock_metadata.assert_not_called()
        assert result["display"]["sync"] is not None
        assert result["display"]["indexBuilding"] is None
        assert result["display"]["verification"] is None
        assert result["display"]["filteredMigration"] is None

    @patch("lib.live_monitoring.fetch_metadata_status")
    @patch("lib.live_monitoring.fetch_progress")
    def test_progress_only_fetches_metadata_for_toolbar_badges(
        self, mock_progress, mock_metadata,
    ):
        mock_progress.return_value = (
            {
                "state": "RUNNING",
                "info": "copy",
                "collectionCopy": {"estimatedCopiedBytes": 1, "estimatedTotalBytes": 2},
            },
            [],
        )
        mock_metadata.return_value = {
            "state": "RUNNING",
            "buildIndexesRaw": "afterDataCopy",
            "syncPhase": "change event application",
            "indexIndexesBuilt": 1,
            "indexIndexesTotal": 5,
            "indexCollectionsFinished": 0,
            "indexCollectionsTotal": 2,
        }
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            connection_string="mongodb://localhost:27017",
            progress_only=True,
        )
        mock_metadata.assert_called_once()
        assert result["display"]["indexBuilding"] is None
        assert result["display"]["toolbarBadges"] == [
            {"label": "INDEXING", "color": "blue"},
        ]

    @patch("lib.live_monitoring.fetch_progress")
    def test_progress_only_toolbar_badges_from_progress_endpoint(self, mock_progress):
        mock_progress.return_value = (
            {
                "state": "RUNNING",
                "verification": {
                    "source": {
                        "phase": "scanning",
                        "hashedDocumentCount": 10,
                        "estimatedDocumentCount": 100,
                    },
                    "destination": {
                        "phase": "scanning",
                        "hashedDocumentCount": 5,
                        "estimatedDocumentCount": 100,
                    },
                },
                "indexBuilding": {
                    "indexesBuilt": 3,
                    "totalIndexesToBuild": 10,
                    "collectionsFinished": 1,
                    "collectionsTotal": 2,
                },
                "collectionCopy": {"estimatedCopiedBytes": 1, "estimatedTotalBytes": 2},
            },
            [],
        )
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            progress_only=True,
        )
        labels = [badge["label"] for badge in result["display"]["toolbarBadges"]]
        assert labels == ["VERIFYING", "INDEXING"]
        assert result["display"]["indexBuilding"] is None
        assert result["display"]["verification"] is None

    @patch("lib.live_monitoring.fetch_progress")
    def test_progress_only_no_metadata_fallback_on_failure(self, mock_progress):
        mock_progress.side_effect = ProgressFetchError("timeout", kind="timeout")
        result = build_live_monitor_payload(
            endpoint_url="host:27182/api/v1/progress",
            connection_string="mongodb://localhost:27017",
            progress_only=True,
        )
        assert result["display"] is None
        assert result["error"] is not None

    def test_progress_only_requires_endpoint(self):
        result = build_live_monitor_payload(progress_only=True)
        assert result["error"] is not None
        assert result["display"] is None

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
        lag = {"overall": 1582, "crud": None, "ddl": None, "has_breakdown": False}
        metrics = _build_progress_metrics(
            {
                "canCommit": True,
                "canWrite": False,
                "totalEventsApplied": 10,
                "estimatedSecondsToCEACatchup": 90,
            },
            lag,
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
        lag = {"overall": 90, "crud": None, "ddl": None, "has_breakdown": False}
        metrics = _build_db_only_metrics(lag)
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
        lag = {"overall": 60, "crud": None, "ddl": None, "has_breakdown": False}
        metrics = _build_progress_metrics({}, lag)
        by_label = {m["label"]: m for m in metrics}
        assert "highLag" not in by_label["Lag time"]

    def test_lag_time_highlighted_over_one_minute(self):
        lag = {"overall": 61, "crud": None, "ddl": None, "has_breakdown": False}
        metrics = _build_progress_metrics({}, lag)
        by_label = {m["label"]: m for m in metrics}
        assert by_label["Lag time"]["highLag"] is True

    def test_build_sync_card_with_lag_breakdown(self):
        progress = {
            "state": "RUNNING",
            "info": "change event application",
            "lag": {
                "overallLagSeconds": 90,
                "crudLagSeconds": 30,
                "ddlLagSeconds": None,
            },
            "collectionCopy": {
                "estimatedCopiedBytes": 100,
                "estimatedTotalBytes": 200,
            },
        }
        card = _build_sync_card(progress=progress, progress_available=True)
        assert card["lagBreakdown"]["show"] is True
        assert card["lagBreakdown"]["crud"] == "30s"
        assert card["lagBreakdown"]["ddl"] is None
        assert card["lagBreakdown"]["ddlUnavailable"] is True

    def test_build_sync_card_legacy_lag_only(self):
        progress = {
            "state": "RUNNING",
            "info": "change event application",
            "lagTimeSeconds": 45,
            "collectionCopy": {},
        }
        card = _build_sync_card(progress=progress, progress_available=True)
        assert card["lagBreakdown"] is None
        lag_metric = next(m for m in card["metrics"] if m["label"] == "Lag time")
        assert lag_metric["value"] == "45s"

    def test_build_sync_card_metadata_lag_breakdown(self):
        metadata = {
            "phase": "Change event application",
            "state": "RUNNING",
            "lagTimeSeconds": 90,
            "crudLagSeconds": 60,
            "ddlLagSeconds": 90,
        }
        card = _build_sync_card(metadata=metadata, progress_available=False)
        assert card["lagBreakdown"]["show"] is True
        assert card["lagBreakdown"]["crud"] == "1m 0s"

    def test_lag_from_metadata(self):
        meta = {"lagTimeSeconds": 10, "crudLagSeconds": 5, "ddlLagSeconds": None}
        lag = _lag_from_metadata(meta)
        assert lag["has_breakdown"] is True
        assert lag["overall"] == 10
        assert lag["crud"] == 5

    def test_build_lag_breakdown_none_without_breakdown(self):
        assert _build_lag_breakdown({"has_breakdown": False}) is None

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
