"""Shared pytest fixtures for Mongosync Insights tests."""
import logging
from unittest.mock import MagicMock, patch

import pytest

from lib import snapshot_store


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Flask test client with isolated log/snapshot directories."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setenv("MI_LOG_FILE", str(log_dir / "insights.log"))
    monkeypatch.setattr(snapshot_store, "LOG_STORE_DIR", str(tmp_path / "store"))
    (tmp_path / "store").mkdir(exist_ok=True)

    with patch("lib.app_config.validate_config", return_value=True):
        with patch("lib.app_config.setup_logging") as mock_log:
            mock_log.return_value = logging.getLogger("test")
            from mongosync_insights import create_app

            app = create_app()
            app.config["TESTING"] = True
            with app.test_client() as client:
                yield client


@pytest.fixture
def sample_progress():
  return {
      "state": "RUNNING",
      "canCommit": False,
      "canWrite": True,
      "lagTime": 120,
      "source": {"host": "src.example.com", "port": 27017},
      "destination": {"host": "dst.example.com", "port": 27017},
      "verification": {
          "source": {"phase": "verifying", "documentsVerified": 10, "totalDocuments": 100},
          "destination": {"phase": "verifying", "documentsVerified": 5, "totalDocuments": 100},
      },
      "indexBuilding": {"indexesBuilt": 2, "totalIndexes": 10},
  }


@pytest.fixture
def sample_metadata():
    return {
        "globalState": {
            "buildIndexes": "afterDataCopy",
            "verificationMode": "startAtCEA",
            "writeBlocking": "destinationOnly",
        },
        "phaseTransitions": [
            {"Phase": "collection copy", "Ts": {"T": 1700000000}},
        ],
        "lagTimeSeconds": 90,
    }


@pytest.fixture
def minimal_template_data():
    return {
        "plot_json": "{}",
        "metrics_plot_json": "",
        "options_data": [],
        "hidden_options_data": [],
        "start_options_data": [],
        "natural_order_data": [],
        "errors_data": [],
        "partition_init_data": [],
        "has_logs_data": False,
        "has_metrics_data": False,
        "log_viewer_lines": [],
        "log_viewer_max_lines": 1000,
        "log_store_id": "",
    }


@pytest.fixture
def mongo_mock_db():
    """Minimal mock pymongo database with configurable collection data."""

    def _make(collections=None):
        collections = collections or {}
        db = MagicMock()

        def get_collection(name):
            coll = MagicMock()
            data = collections.get(name, [])
            coll.find.return_value = data
            coll.aggregate.return_value = data
            coll.count_documents.return_value = len(data) if isinstance(data, list) else 0
            return coll

        db.__getitem__.side_effect = get_collection
        return db

    return _make
