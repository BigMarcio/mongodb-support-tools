"""Tests for Flask live blueprint routes."""
from unittest.mock import MagicMock, patch

from blueprints.live import (
    _live_nav_context,
    _resolve_monitor_destination,
)


_PROBE_FLAGS = {
    "mongosync_metadata_available": True,
    "verifier_metadata_available": False,
}

_PROBE_FLAGS_NO_MONGOSYNC = {
    "mongosync_metadata_available": False,
    "verifier_metadata_available": False,
}


def _ctx(
    *,
    endpoint_url=None,
    connection_string=None,
    embedded_verifier=True,
    verifier_endpoint_url=None,
    mongosync_metadata_available=False,
    verifier_metadata_available=False,
):
    return {
        "endpoint_url": endpoint_url,
        "connection_string": connection_string,
        "embedded_verifier": embedded_verifier,
        "verifier_endpoint_url": verifier_endpoint_url,
        "verifier_connection_string": None,
        "mongosync_metadata_available": mongosync_metadata_available,
        "verifier_metadata_available": verifier_metadata_available,
    }


class TestMonitorClassification:
    def test_progress_only_destination_is_migration(self):
        assert _resolve_monitor_destination(_ctx(endpoint_url="h:27182/api/v1/progress")) == "migration"

    def test_progress_and_verifier_destination_is_dashboard(self):
        ctx = _ctx(
            endpoint_url="h:27182/api/v1/progress",
            embedded_verifier=False,
            verifier_endpoint_url="h:27020/api/v1/progress",
        )
        assert _resolve_monitor_destination(ctx) == "dashboard"

    def test_connection_and_verifier_with_mongosync_db_is_migration(self):
        ctx = _ctx(
            connection_string="mongodb://localhost:27017",
            embedded_verifier=False,
            verifier_endpoint_url="h:27020/api/v1/progress",
            mongosync_metadata_available=True,
        )
        assert _resolve_monitor_destination(ctx) == "migration"

    def test_connection_and_verifier_without_mongosync_db_is_verifier(self):
        ctx = _ctx(
            connection_string="mongodb://localhost:27017",
            embedded_verifier=False,
            verifier_endpoint_url="h:27020/api/v1/progress",
            mongosync_metadata_available=False,
        )
        assert _resolve_monitor_destination(ctx) == "verifier"

    def test_progress_only_nav_hidden(self):
        assert _live_nav_context(_ctx(endpoint_url="h:27182/api/v1/progress"), "migration") is None

    def test_dashboard_nav_shows_all_links(self):
        ctx = _ctx(
            endpoint_url="h:27182/api/v1/progress",
            embedded_verifier=False,
            verifier_endpoint_url="h:27020/api/v1/progress",
        )
        nav = _live_nav_context(ctx, "dashboard")
        assert nav["show_dashboard"] is True
        assert nav["show_full_migration"] is True
        assert nav["show_full_verifier"] is True

    def test_connection_verifier_with_mongosync_db_nav_without_dashboard(self):
        ctx = _ctx(
            connection_string="mongodb://localhost:27017",
            embedded_verifier=False,
            verifier_endpoint_url="h:27020/api/v1/progress",
            mongosync_metadata_available=True,
        )
        nav = _live_nav_context(ctx, "migration")
        assert nav["show_dashboard"] is False
        assert nav["show_full_migration"] is True
        assert nav["show_full_verifier"] is True


class TestLiveHome:
    def test_live_home_returns_200(self, app_client):
        r = app_client.get("/live/")
        assert r.status_code == 200

    def test_live_home_endpoint_before_connection_string(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert html.index("progressHost") < html.index("connectionString")

    def test_live_home_has_embedded_verifier_checkbox(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert "embeddedVerifier" in html
        assert "Using mongosync Embedded Verifier" in html
        assert "verifierProgressHost" in html

    def test_live_home_has_sidebar_guide(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert "setup-guide-sidebar" in html
        assert "setup-guide-sidebar-title" in html
        assert "monitor" in html.lower()

    def test_live_home_no_tall_info_grid(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert "setup-info-grid" not in html

    def test_setup_preview_removed(self, app_client):
        r = app_client.get("/live/preview")
        assert r.status_code == 404


class TestLiveMonitoring:
    @patch("blueprints.live._probe_connection_flags", return_value=_PROBE_FLAGS)
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_live_monitoring_with_connection_redirects_to_migration(
        self, _session, _validate, _probe, app_client,
    ):
        r = app_client.post(
            "/live/live_monitoring",
            data={"connectionString": "mongodb://localhost:27017"},
        )
        assert r.status_code == 302
        assert "/live/migration" in r.headers["Location"]

    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_monitor_with_progress_redirects_to_migration(self, _session, app_client):
        r = app_client.post(
            "/live/monitor",
            data={
                "progressHost": "localhost",
                "progressPort": "27182",
                "embeddedVerifier": "on",
            },
        )
        assert r.status_code == 302
        assert "/live/migration" in r.headers["Location"]

    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_monitor_with_progress_and_verifier_redirects_to_dashboard(self, _session, app_client):
        r = app_client.post(
            "/live/monitor",
            data={
                "progressHost": "localhost",
                "progressPort": "27182",
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
            },
        )
        assert r.status_code == 302
        assert "/live/dashboard" in r.headers["Location"]

    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_monitor_verifier_only_redirects_to_verifier_view(self, _session, app_client):
        r = app_client.post(
            "/live/monitor",
            data={
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
            },
        )
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    @patch("blueprints.live._probe_connection_flags", return_value=_PROBE_FLAGS)
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_monitor_connection_and_verifier_with_mongosync_db_redirects_to_migration(
        self, _session, _validate, _probe, app_client,
    ):
        r = app_client.post(
            "/live/monitor",
            data={
                "connectionString": "mongodb://localhost:27017",
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
            },
        )
        assert r.status_code == 302
        assert "/live/migration" in r.headers["Location"]

    @patch(
        "blueprints.live._probe_connection_flags",
        return_value=_PROBE_FLAGS_NO_MONGOSYNC,
    )
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_monitor_connection_and_verifier_without_mongosync_db_redirects_to_verifier(
        self, _session, _validate, _probe, app_client,
    ):
        r = app_client.post(
            "/live/monitor",
            data={
                "connectionString": "mongodb://localhost:27017",
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
            },
        )
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    def test_live_monitoring_no_input(self, app_client):
        r = app_client.post("/live/live_monitoring", data={})
        assert r.status_code == 200
        assert b"No Input Provided" in r.data

    def test_live_monitoring_invalid_progress_url(self, app_client):
        r = app_client.post(
            "/live/live_monitoring",
            data={
                "progressHost": "bad host",
                "progressPort": "27182",
            },
        )
        assert r.status_code == 200
        assert b"Invalid Progress Endpoint" in r.data

    def test_live_monitoring_out_of_range_port(self, app_client):
        r = app_client.post(
            "/live/live_monitoring",
            data={"progressHost": "myhost", "progressPort": "70000"},
        )
        assert r.status_code == 200
        assert b"Invalid Progress Endpoint" in r.data
        assert b"65535" in r.data


class TestDashboardRoutes:
    @patch(
        "blueprints.live._live_session_context",
        return_value=_ctx(
            endpoint_url="localhost:27182/api/v1/progress",
            embedded_verifier=False,
            verifier_endpoint_url="localhost:27020/api/v1/progress",
        ),
    )
    def test_dashboard_renders(self, _ctx, app_client):
        r = app_client.get("/live/dashboard")
        assert r.status_code == 200
        assert b"migration-dashboard-root" in r.data
        assert b"verifier-dashboard-root" in r.data

    @patch(
        "blueprints.live._live_session_context",
        return_value=_ctx(endpoint_url="localhost:27182/api/v1/progress"),
    )
    def test_dashboard_redirects_migration_only_session(self, _ctx, app_client):
        r = app_client.get("/live/dashboard")
        assert r.status_code == 302
        assert "/live/migration" in r.headers["Location"]

    @patch(
        "blueprints.live._live_session_context",
        return_value=_ctx(
            embedded_verifier=False,
            verifier_endpoint_url="localhost:27020/api/v1/progress",
        ),
    )
    def test_dashboard_redirects_verifier_only_session(self, _ctx, app_client):
        r = app_client.get("/live/dashboard")
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    @patch(
        "blueprints.live._live_session_context",
        return_value=_ctx(),
    )
    def test_dashboard_redirects_without_valid_session(self, _ctx, app_client):
        r = app_client.get("/live/dashboard")
        assert r.status_code == 302
        assert "/live/" in r.headers["Location"]

    @patch("blueprints.live.plot_verifier_metrics", return_value="html")
    def test_dashboard_session_allows_full_migration_and_verifier_views(
        self, _plot, app_client,
    ):
        ctx = _ctx(
            endpoint_url="localhost:27182/api/v1/progress",
            connection_string="mongodb://localhost:27017",
            embedded_verifier=False,
            verifier_endpoint_url="localhost:27020/api/v1/progress",
            mongosync_metadata_available=True,
        )
        with patch("blueprints.live._live_session_context", return_value=ctx):
            r = app_client.get("/live/migration")
            assert r.status_code == 200
            r2 = app_client.get("/live/verifier/view")
            assert r2.status_code == 200
        _plot.assert_called_once()

    @patch("blueprints.live.build_live_monitor_payload", return_value={"display": {"sync": {}}})
    @patch(
        "blueprints.live._progress_monitor_session_context",
        return_value=("localhost:27182/api/v1/progress", "mongodb://localhost"),
    )
    def test_get_dashboard_migration(self, _ctx, _payload, app_client):
        r = app_client.post("/live/get_dashboard_migration")
        assert r.status_code == 200
        _payload.assert_called_once_with(
            "localhost:27182/api/v1/progress",
            "mongodb://localhost",
            progress_only=True,
        )


class TestProgressMonitor:
    @patch("blueprints.live.build_live_monitor_payload", return_value={"ok": True})
    @patch("blueprints.live._progress_monitor_session_context", return_value=("h:27182/api/v1/progress", "mongodb://localhost"))
    def test_get_progress_monitor(self, _ctx, _payload, app_client):
        r = app_client.post("/live/get_progress_monitor")
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    @patch(
        "blueprints.live._progress_monitor_session_context",
        return_value=(None, None),
    )
    def test_get_progress_monitor_no_config(self, _ctx, app_client):
        r = app_client.post("/live/get_progress_monitor")
        assert r.status_code == 200
        assert "error" in r.get_json()


class TestVerifierRoutes:
    @patch("blueprints.live._probe_connection_flags", return_value=_PROBE_FLAGS)
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post(self, _session, _validate, _probe, app_client):
        r = app_client.post(
            "/live/verifier",
            data={"verifierConnectionString": "mongodb://localhost:27017"},
        )
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post_progress_only(self, _session, app_client):
        r = app_client.post(
            "/live/verifier",
            data={"verifierProgressHost": "localhost", "verifierProgressPort": "27020"},
        )
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    @patch("blueprints.live._probe_connection_flags", return_value=_PROBE_FLAGS)
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post_both_inputs(self, _session, _validate, _probe, app_client):
        r = app_client.post(
            "/live/verifier",
            data={
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
                "verifierConnectionString": "mongodb://localhost:27017",
            },
        )
        assert r.status_code == 302
        assert "/live/verifier/view" in r.headers["Location"]

    def test_verifier_missing_input(self, app_client):
        r = app_client.post("/live/verifier", data={})
        assert r.status_code == 200
        assert b"No Input Provided" in r.data

    def test_verifier_invalid_progress_port(self, app_client):
        r = app_client.post(
            "/live/verifier",
            data={"verifierProgressHost": "localhost", "verifierProgressPort": "70000"},
        )
        assert r.status_code == 200
        assert b"Invalid Progress Endpoint" in r.data

    @patch("blueprints.live.plot_verifier_metrics", return_value="html")
    @patch(
        "blueprints.live._live_session_context",
        return_value=_ctx(
            embedded_verifier=False,
            verifier_endpoint_url="localhost:27020/api/v1/progress",
        ),
    )
    def test_verifier_view_get(self, _ctx, _plot, app_client):
        r = app_client.get("/live/verifier/view")
        assert r.status_code == 200
        _plot.assert_called_once()
        assert _plot.call_args.kwargs["nav"] is None

    @patch("blueprints.live.build_verifier_progress_payload", return_value={"display": {"verificationProgress": {"phase": "check"}}})
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_get_verifier_progress(self, _session, mock_build, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/get_verifier_progress")
        assert r.status_code == 200
        assert r.get_json()["display"]["verificationProgress"]["phase"] == "check"

    @patch("blueprints.live.build_verifier_run_summary_response", return_value=({"display": {}}, 200))
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_run_verifier_summary(self, _session, mock_build, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/run_verifier_summary")
        assert r.status_code == 200
        mock_build.assert_called_once_with("localhost:27020/api/v1/progress")

    @patch("blueprints.live.build_verifier_run_summary_response")
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_run_verifier_summary_forbidden_gen0(self, _session, mock_build, app_client):
        mock_build.return_value = ({"error": "generation 0"}, 403)
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/run_verifier_summary")
        assert r.status_code == 403

    @patch("blueprints.live.build_verifier_run_summary_response")
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_run_verifier_summary_cooldown(self, _session, mock_build, app_client):
        mock_build.return_value = (
            {"error": "cooldown", "cooldownRemainingSecs": 120, "display": {}},
            429,
        )
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/run_verifier_summary")
        assert r.status_code == 429
        assert r.get_json()["cooldownRemainingSecs"] == 120

    @patch("blueprints.live.build_verifier_metadata_payload", return_value={"display": {"generations": []}})
    @patch("blueprints.live.session_store.get_session", return_value={"verifier_connection_string": "mongodb://localhost:27017"})
    def test_get_verifier_metadata(self, _session, mock_build, app_client):
        r = app_client.post("/live/get_verifier_metadata")
        assert r.status_code == 200
        assert "display" in r.get_json()

    @patch("blueprints.live.session_store.get_session", return_value={})
    def test_get_verifier_progress_missing_endpoint(self, _session, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            with patch("blueprints.live.VERIFIER_PROGRESS_ENDPOINT_URL", ""):
                r = app_client.post("/live/get_verifier_progress")
        assert r.status_code == 400

    @patch("blueprints.live.session_store.get_session", return_value={})
    def test_get_verifier_metadata_missing_connection(self, _session, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/get_verifier_metadata")
        assert r.status_code == 400

    @patch("blueprints.live.session_store.get_session", return_value={})
    def test_download_verifier_doc_mismatches_missing_endpoint(self, _session, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            with patch("blueprints.live.VERIFIER_PROGRESS_ENDPOINT_URL", ""):
                r = app_client.get("/live/download_verifier_doc_mismatches")
        assert r.status_code == 400

    @patch("blueprints.live.stream_verifier_mismatches")
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_download_verifier_doc_mismatches_success(self, _session, mock_stream, app_client):
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b'{"type":"content"}\n']
        mock_stream.return_value = mock_resp
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            with patch("blueprints.live.VERIFIER_SUMMARY_MIN_DURATION_SECS", 0):
                r = app_client.get("/live/download_verifier_doc_mismatches")
        assert r.status_code == 200
        assert r.mimetype == "application/x-ndjson"
        assert 'filename="doc-mismatches.ndjson"' in r.headers.get("Content-Disposition", "")
        assert b'{"type":"content"}' in r.data
        mock_resp.close.assert_called_once()

    @patch("blueprints.live.stream_verifier_mismatches")
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_download_verifier_doc_mismatches_passes_min_duration(
        self, _session, mock_stream, app_client,
    ):
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = []
        mock_stream.return_value = mock_resp
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            with patch("blueprints.live.VERIFIER_SUMMARY_MIN_DURATION_SECS", 60):
                app_client.get("/live/download_verifier_doc_mismatches")
        mock_stream.assert_called_once()
        assert mock_stream.call_args.kwargs["params"] == {"minDurationSecs": 60}

    @patch("blueprints.live.stream_verifier_mismatches")
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_download_verifier_ns_mismatches_success(self, _session, mock_stream, app_client):
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b'{"namespace":"db.coll"}\n']
        mock_stream.return_value = mock_resp
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.get("/live/download_verifier_ns_mismatches")
        assert r.status_code == 200
        assert r.mimetype == "application/x-ndjson"
        assert 'filename="ns-mismatches.ndjson"' in r.headers.get("Content-Disposition", "")
        assert b'{"namespace":"db.coll"}' in r.data
