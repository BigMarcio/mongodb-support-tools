"""Tests for Flask app factory."""
from unittest.mock import patch

from lib.app_config import MAX_FILE_SIZE


class TestCreateApp:
    @patch("lib.app_config.validate_config", return_value=True)
    @patch("lib.app_config.setup_logging")
    @patch("mongosync_insights.run_log_store_maintenance")
    def test_create_app_returns_flask_app(self, _maint, mock_log, _validate):
        mock_log.return_value = __import__("logging").getLogger("test")
        from mongosync_insights import create_app

        app = create_app()
        assert app is not None
        assert app.config["MAX_CONTENT_LENGTH"] == MAX_FILE_SIZE

    @patch("lib.app_config.validate_config", return_value=True)
    @patch("lib.app_config.setup_logging")
    @patch("mongosync_insights.run_log_store_maintenance")
    def test_blueprints_registered(self, _maint, mock_log, _validate):
        mock_log.return_value = __import__("logging").getLogger("test")
        from mongosync_insights import create_app

        app = create_app()
        rules = {rule.rule for rule in app.url_map.iter_rules()}
        assert "/logs/" in rules
        assert "/live/" in rules

    @patch("lib.app_config.validate_config", return_value=True)
    @patch("lib.app_config.setup_logging")
    @patch("mongosync_insights.run_log_store_maintenance")
    def test_hub_route(self, _maint, mock_log, _validate, app_client):
        r = app_client.get("/")
        assert r.status_code == 200


class TestSecurityHeaders:
    def test_img_src_does_not_allow_arbitrary_remote_hosts(self, app_client):
        """Blanket https: in img-src would leave an exfiltration beacon open."""
        csp = app_client.get("/").headers["Content-Security-Policy"]
        img_src = next(
            part.strip()
            for part in csp.split(";")
            if part.strip().startswith("img-src")
        )
        assert img_src == "img-src 'self' data: blob:"

    def test_inline_json_filter_registered(self, app_client):
        assert "inline_json" in app_client.application.jinja_env.filters
