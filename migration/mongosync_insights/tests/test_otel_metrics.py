"""Tests for Prometheus metrics parsing in lib/otel_metrics.py."""
import json
from datetime import datetime

import pytest

from lib.otel_metrics import (
    CONFIG_PATH,
    MetricsCollector,
    generate_title,
    load_metrics_config,
    parse_labels,
    parse_metrics_log_line,
    parse_prometheus_message,
)


class TestParseLabels:
    @pytest.mark.parametrize(
        "labels_str,expected",
        [
            ("", {}),
            ('phase="collection_copy"', {"phase": "collection_copy"}),
            (
                'phase="cea",direction="source"',
                {"phase": "cea", "direction": "source"},
            ),
        ],
    )
    def test_parse_labels(self, labels_str, expected):
        assert parse_labels(labels_str) == expected


class TestParsePrometheusMessage:
    def test_parses_gauge_without_labels(self):
        message = "mongosync_phase 3"
        metrics = parse_prometheus_message(message)
        assert len(metrics) == 1
        assert metrics[0]["name"] == "mongosync_phase"
        assert metrics[0]["value"] == 3.0
        assert metrics[0]["labels"] == {}

    def test_parses_metric_with_labels_and_timestamp(self):
        message = 'mongosync_lag_seconds{direction="destination"} 12.5 1700000000000'
        metrics = parse_prometheus_message(message)
        assert metrics[0]["name"] == "mongosync_lag_seconds"
        assert metrics[0]["labels"] == {"direction": "destination"}
        assert metrics[0]["value"] == 12.5
        assert metrics[0]["timestamp"] == 1700000000000

    def test_skips_help_and_type_comments(self):
        message = "# HELP mongosync_phase Gauge\n# TYPE mongosync_phase gauge\nmongosync_phase 1"
        metrics = parse_prometheus_message(message)
        assert len(metrics) == 1
        assert metrics[0]["name"] == "mongosync_phase"

    def test_handles_escaped_newlines(self):
        message = "mongosync_phase 1\\nmongosync_lag_seconds 2"
        metrics = parse_prometheus_message(message)
        assert len(metrics) == 2

    def test_skips_invalid_values(self):
        message = "bad_metric not_a_number"
        assert parse_prometheus_message(message) == []


class TestParseMetricsLogLine:
    def test_valid_json_line(self):
        line = json.dumps(
            {
                "time": "2026-01-15T10:30:45.123456Z",
                "message": "mongosync_phase 2",
            }
        )
        ts, metrics = parse_metrics_log_line(line)
        assert isinstance(ts, datetime)
        assert ts.year == 2026
        assert len(metrics) == 1

    def test_time_without_microseconds(self):
        line = json.dumps(
            {"time": "2026-01-15T10:30:45Z", "message": "mongosync_phase 1"}
        )
        ts, metrics = parse_metrics_log_line(line)
        assert ts is not None
        assert len(metrics) == 1

    def test_invalid_json(self):
        ts, metrics = parse_metrics_log_line("not json")
        assert ts is None
        assert metrics == []

    def test_missing_message(self):
        line = json.dumps({"time": "2026-01-15T10:30:45.123456Z"})
        ts, metrics = parse_metrics_log_line(line)
        assert ts is not None
        assert metrics == []


class TestMetricsCollector:
    def test_process_line_increments_counts(self):
        collector = MetricsCollector()
        line = json.dumps(
            {
                "time": "2026-01-15T10:30:45.123456Z",
                "message": "mongosync_phase 2",
            }
        )
        collector.process_line(line)
        assert collector.line_count == 1
        assert collector.metrics_count == 1
        times, values = collector.get_gauge_series("mongosync_phase")
        assert len(times) == 1
        assert values[0] == 2.0

    def test_histogram_bucket_ingestion(self):
        collector = MetricsCollector()
        line = json.dumps(
            {
                "time": "2026-01-15T10:30:45.123456Z",
                "message": 'mongosync_request_duration_bucket{le="0.5"} 10',
            }
        )
        collector.process_line(line)
        assert collector.metrics_count == 1
        assert "mongosync_request_duration" in collector.histograms


class TestMetricsConfig:
    def test_load_metrics_config(self):
        config = load_metrics_config(CONFIG_PATH)
        assert isinstance(config, list)
        assert len(config) > 0
        assert "name" in config[0]

    def test_generate_title_from_config_title(self):
        title = generate_title({"name": "mongosync_phase", "title": "Sync Phase", "unit": "enum"})
        assert title == "Sync Phase"

    def test_generate_title_auto_from_name(self):
        title = generate_title({"name": "mongosync_lag_seconds", "unit": "seconds"})
        assert title == "Lag Seconds (sec)"

    def test_generate_title_no_unit_suffix(self):
        title = generate_title({"name": "mongosync_count", "unit": "count"})
        assert title == "Count"
