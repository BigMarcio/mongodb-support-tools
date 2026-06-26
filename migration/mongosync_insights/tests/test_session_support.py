"""Tests for session support helpers."""
from unittest.mock import patch

from lib.session_support import SESSION_COOKIE_NAME, store_session_data


class TestStoreSessionData:
    @patch("lib.session_support.session_store")
    def test_updates_existing_session(self, mock_store, app_client):
        mock_store.update_session.return_value = True
        with app_client.application.test_request_context(
            "/live/live_monitoring",
            headers=[("Cookie", f"{SESSION_COOKIE_NAME}=existing-id")],
        ):
            sid = store_session_data({"connection_string": "mongodb://localhost"})
        assert sid == "existing-id"
        mock_store.update_session.assert_called_once()

    @patch("lib.session_support.session_store")
    def test_creates_new_session_when_missing(self, mock_store, app_client):
        mock_store.update_session.return_value = False
        mock_store.create_session.return_value = "new-id"
        with app_client.application.test_request_context("/live/live_monitoring"):
            sid = store_session_data({"connection_string": "mongodb://localhost"})
        assert sid == "new-id"
        mock_store.create_session.assert_called_once()

    def test_session_cookie_name(self):
        assert SESSION_COOKIE_NAME == "mi_session_id"
