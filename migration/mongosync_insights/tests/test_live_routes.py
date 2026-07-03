"""Tests for Flask live blueprint routes."""
from unittest.mock import patch


class TestLiveHome:
    def test_live_home_returns_200(self, app_client):
        r = app_client.get("/live/")
        assert r.status_code == 200

    def test_live_home_endpoint_before_connection_string(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert html.index("progressHost") < html.index("connectionString")
        assert "main source" in html.lower()

    def test_verifier_progress_before_connection_string(self, app_client):
        r = app_client.get("/live/")
        html = r.data.decode()
        assert html.index("verifierProgressHost") < html.index("verifierConnectionString")


class TestLiveMonitoring:
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_live_monitoring_with_connection(self, _session, _validate, app_client):
        r = app_client.post(
            "/live/live_monitoring",
            data={"connectionString": "mongodb://localhost:27017"},
        )
        assert r.status_code == 200

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
    @patch("blueprints.live.plot_verifier_metrics", return_value="html")
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post(self, _session, _validate, _plot, app_client):
        r = app_client.post(
            "/live/verifier",
            data={"verifierConnectionString": "mongodb://localhost:27017"},
        )
        assert r.status_code == 200

    @patch("blueprints.live.plot_verifier_metrics", return_value="html")
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post_progress_only(self, _session, _plot, app_client):
        r = app_client.post(
            "/live/verifier",
            data={"verifierProgressHost": "localhost", "verifierProgressPort": "27020"},
        )
        assert r.status_code == 200

    @patch("blueprints.live.plot_verifier_metrics", return_value="html")
    @patch("blueprints.live.validate_connection", return_value=True)
    @patch("blueprints.live.store_session_data", return_value="sid")
    def test_verifier_post_both_inputs(self, _session, _validate, _plot, app_client):
        r = app_client.post(
            "/live/verifier",
            data={
                "verifierProgressHost": "localhost",
                "verifierProgressPort": "27020",
                "verifierConnectionString": "mongodb://localhost:27017",
            },
        )
        assert r.status_code == 200

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

    @patch("blueprints.live.build_verifier_monitor_payload", return_value={"display": {"generations": []}})
    @patch("blueprints.live.session_store.get_session", return_value={"verifier_connection_string": "mongodb://localhost:27017"})
    def test_get_verifier_data(self, _session, _gather, app_client):
        r = app_client.post("/live/get_verifier_data")
        assert r.status_code == 200
        assert "display" in r.get_json()

    @patch(
        "blueprints.live.build_verifier_monitor_payload",
        return_value={"display": {"verificationProgress": {"phase": "check"}}},
    )
    @patch(
        "blueprints.live.session_store.get_session",
        return_value={"verifier_endpoint_url": "localhost:27020/api/v1/progress"},
    )
    def test_get_verifier_data_endpoint_only(self, _session, _gather, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            r = app_client.post("/live/get_verifier_data")
            assert r.status_code == 200
            assert r.get_json()["display"]["verificationProgress"]["phase"] == "check"

    @patch("blueprints.live.session_store.get_session", return_value={})
    def test_get_verifier_data_missing_input(self, _session, app_client):
        with patch("blueprints.live.VERIFIER_CONNECTION_STRING", ""):
            with patch("blueprints.live.VERIFIER_PROGRESS_ENDPOINT_URL", ""):
                r = app_client.post("/live/get_verifier_data")
                assert r.status_code == 400
