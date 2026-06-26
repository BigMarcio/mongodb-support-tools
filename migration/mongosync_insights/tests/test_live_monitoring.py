"""Tests for live monitoring fetch and builder helpers."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from lib.live_monitoring import (
    ProgressFetchError,
    _build_connectivity,
    _build_direction,
    _build_metadata_metrics,
    _build_progress_metrics,
    _derive_state_badge,
    _state_badge_color,
    fetch_progress,
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
    def test_build_connectivity(self):
        card = _build_connectivity(
            endpoint_url="host:27182/api/v1/progress",
            connection_string="mongodb://localhost:27017",
        )
        assert card["title"] == "Connectivity"
        assert len(card["rows"]) == 2

    def test_build_connectivity_none_when_empty(self):
        assert _build_connectivity() is None

    def test_build_metadata_metrics(self):
        metrics = _build_metadata_metrics({"start": "2026-01-01", "buildIndexes": "After Data Copy"})
        labels = [m["label"] for m in metrics]
        assert "Start" in labels
        assert "Build Indexes" in labels

    def test_build_progress_metrics(self):
        metrics = _build_progress_metrics({"canCommit": True, "canWrite": False, "totalEventsApplied": 10}, 30)
        by_label = {m["label"]: m for m in metrics}
        assert by_label["Can commit"]["value"] == "TRUE"
        assert by_label["Events applied"]["value"] == "10"

    def test_build_direction_from_progress(self):
        progress = {
            "directionMapping": {"Source": "src:27017", "Destination": "dst:27017"},
            "source": {"pingLatencyMs": 5},
            "destination": {"pingLatencyMs": 10},
        }
        card = _build_direction(progress)
        assert card["source"]["address"] == "src:27017"
        assert card["destination"]["ping"] == "10 ms"


class TestProgressMonitorNoConfig:
    def test_response_shape(self):
        resp = progress_monitor_no_config_response()
        assert "error" in resp
        assert resp["display"] is None
