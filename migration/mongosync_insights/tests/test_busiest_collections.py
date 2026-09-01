"""Tests for CEA busiest collections parsing."""
import json

import pytest

from lib.busiest_collections import (
    CRUD_STATS_MESSAGE,
    BusiestCollectionsAccumulator,
    build_busiest_collections_plot,
    update_in_cea_phase,
)


def _info_line(time, collections, hot_docs=0):
    return {
        "time": time,
        "level": "info",
        "message": CRUD_STATS_MESSAGE,
        "recentCRUDStatistics": {
            "totalHotDocEvents": hot_docs,
            "busiestCollections": collections,
        },
    }


def _warn_line(time, namespace, message, spread=12.5, total=1000):
    return {
        "time": time,
        "level": "warn",
        "message": message,
        "recentCRUDStatistics": {
            "namespace": namespace,
            "totalEvents": total,
            "spreadDisparity": spread,
            "totalEventsPerType": {"update": total},
        },
    }


class TestUpdateInCeaPhase:
    def test_enters_cea(self):
        assert update_in_cea_phase(False, "change event application") is True

    def test_leaves_cea_on_commit(self):
        assert update_in_cea_phase(True, "commit handler called") is False

    def test_unchanged_for_other_phases(self):
        assert update_in_cea_phase(True, "collection copy") is True


class TestBusiestCollectionsAccumulator:
    def test_accumulates_info_line_busiest_collections(self):
        acc = BusiestCollectionsAccumulator()
        acc.ingest_line(_info_line("2026-01-01T12:00:10.000000Z", [{
            "namespace": "db.coll",
            "totalEvents": 100,
            "totalEventsPerType": {"insert": 60, "update": 40},
        }]), in_cea_phase=True)
        acc.ingest_line(_info_line("2026-01-01T12:00:20.000000Z", [{
            "namespace": "db.coll",
            "totalEvents": 50,
            "totalEventsPerType": {"insert": 20, "update": 30},
        }]), in_cea_phase=True)

        result = acc.finalize()
        assert len(result["summary"]) == 1
        row = result["summary"][0]
        assert row["namespace"] == "db.coll"
        assert row["totalEvents"] == 150
        assert row["totalEventsPerType"]["insert"] == 80
        assert row["totalEventsPerType"]["update"] == 70
        assert result["meta"]["intervalsProcessed"] == 2

    def test_ingests_info_lines_without_phase_marker(self):
        acc = BusiestCollectionsAccumulator()
        acc.ingest_line(_info_line("2026-01-01T12:00:10.000000Z", [{
            "namespace": "db.coll",
            "totalEvents": 100,
            "totalEventsPerType": {"insert": 100},
        }]), in_cea_phase=False)

        result = acc.finalize()
        assert len(result["summary"]) == 1
        assert result["summary"][0]["totalEvents"] == 100

    def test_ingests_warning_line_single_collection(self):
        acc = BusiestCollectionsAccumulator()
        warn = _warn_line(
            "2026-01-01T12:01:00.000000Z",
            "db.hot",
            "The level of CEA parallelization for this collection is low. Example",
            spread=15.2,
            total=1200,
        )
        acc.ingest_line(warn, in_cea_phase=True)

        result = acc.finalize()
        assert len(result["warnings"]) == 1
        assert result["warnings"][0]["namespace"] == "db.hot"
        assert result["warnings"][0]["warningType"] == "hot_doc"
        assert result["summary"][0]["totalEvents"] == 1200
        assert result["summary"][0]["warningCount"] == 1
        assert result["summary"][0]["maxSpreadDisparity"] == 15.2

    def test_dynamic_event_types_in_summary(self):
        acc = BusiestCollectionsAccumulator()
        acc.ingest_line(_info_line("2026-01-01T12:00:10.000000Z", [{
            "namespace": "db.a",
            "totalEvents": 10,
            "totalEventsPerType": {"insert": 10},
        }]), in_cea_phase=True)
        acc.ingest_line(_info_line("2026-01-01T12:00:20.000000Z", [{
            "namespace": "db.b",
            "totalEvents": 5,
            "totalEventsPerType": {"replace": 5},
        }]), in_cea_phase=True)

        result = acc.finalize()
        assert "insert" in result["eventTypes"]
        assert "replace" in result["eventTypes"]

    def test_finalize_sorts_by_total_events(self):
        acc = BusiestCollectionsAccumulator()
        acc.ingest_line(_info_line("2026-01-01T12:00:10.000000Z", [
            {"namespace": "db.low", "totalEvents": 10, "totalEventsPerType": {"insert": 10}},
            {"namespace": "db.high", "totalEvents": 99, "totalEventsPerType": {"insert": 99}},
        ]), in_cea_phase=True)

        result = acc.finalize()
        assert [row["namespace"] for row in result["summary"]] == ["db.high", "db.low"]

    def test_empty_finalize(self):
        result = BusiestCollectionsAccumulator().finalize()
        assert result["summary"] == []
        assert result["warnings"] == []
        assert result["timeseries"]["times"] == []
        assert result["meta"]["intervalsProcessed"] == 0


class TestBuildBusiestCollectionsPlot:
    def test_build_plot_returns_json(self):
        timeseries = {
            "times": [
                "2026-01-01T12:00:10.000000Z",
                "2026-01-01T12:00:20.000000Z",
            ],
            "series": {
                "db.coll": [100, 80],
            },
        }
        plot_json = build_busiest_collections_plot(timeseries, {"intervalSecs": 10})
        assert plot_json
        payload = json.loads(plot_json)
        assert payload["data"]
        assert len(payload["data"]) == 1

    def test_build_plot_empty_returns_blank(self):
        assert build_busiest_collections_plot({"times": [], "series": {}}) == ""
