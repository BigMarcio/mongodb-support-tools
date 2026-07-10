"""End-to-end upload tests using committed fixtures."""
from pathlib import Path
from unittest.mock import patch

import pytest

from lib import snapshot_store
from lib.log_store_registry import log_store_registry

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshot_store, "LOG_STORE_DIR", str(tmp_path / "store"))
    (tmp_path / "store").mkdir(exist_ok=True)
    log_store_registry._entries.clear()
    yield
    log_store_registry._entries.clear()


class TestUploadFixtures:
    @patch("lib.logs_metrics.create_metrics_plots", return_value="")
    def test_upload_sample_log_fixture(self, _mock_plots, app_client):
        log_path = FIXTURES / "sample_mongosync.log"
        with open(log_path, "rb") as f:
            data = {"file": (f, "mongosync.log")}
            r = app_client.post(
                "/logs/uploadLogs", data=data, content_type="multipart/form-data"
            )
        assert r.status_code == 200
        snapshots = snapshot_store.list_snapshots()
        assert len(snapshots) >= 1
        assert snapshots[0]["line_count"] > 0

    @patch("lib.logs_metrics.create_metrics_plots", return_value="{}")
    def test_upload_metrics_fixture(self, _mock_plots, app_client):
        metrics_path = FIXTURES / "sample_mongosync_metrics.log"
        with open(metrics_path, "rb") as f:
            data = {"file": (f, "mongosync_metrics.log")}
            r = app_client.post(
                "/logs/uploadLogs", data=data, content_type="multipart/form-data"
            )
        assert r.status_code == 200

    @patch("lib.logs_metrics.create_metrics_plots", return_value="")
    def test_search_after_upload(self, _mock_plots, app_client):
        log_path = FIXTURES / "sample_mongosync.log"
        with open(log_path, "rb") as f:
            data = {"file": (f, "mongosync.log")}
            app_client.post(
                "/logs/uploadLogs", data=data, content_type="multipart/form-data"
            )
        snapshots = snapshot_store.list_snapshots()
        store_id = snapshots[0]["log_store_id"]
        r = app_client.get(f"/logs/search_logs?store_id={store_id}&q=Replication")
        assert r.status_code == 200
        assert r.get_json()["total"] >= 1
