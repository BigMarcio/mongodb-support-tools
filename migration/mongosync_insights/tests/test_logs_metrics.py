"""Tests for log parsing helpers in lib/logs_metrics.py."""
import io
import json

import pytest

from lib.logs_metrics import (
    INFO_PHASE_TO_CANONICAL,
    _extract_progress_flag_events,
    _merge_phase_events,
    _phase_event_from_in_memory,
    _phase_event_from_info,
    _phase_events_from_api,
    detect_mime_type,
)


class TestDetectMimeType:
    @pytest.mark.parametrize(
        "sample,filename,expected",
        [
            (b"\x1f\x8b\x08", "file.gz", "application/gzip"),
            (b"PK\x03\x04", "file.zip", "application/zip"),
            (b"BZh9", "file.bz2", "application/x-bzip2"),
            (b"x" * 300 + b"ustar" + b"x" * 10, "file.tar", "application/x-tar"),
            (b'{"level":"info"}', "data.json", "application/json"),
            (b"plain log line\n", "mongosync.log", "text/plain"),
            (b"\x00\x01\x02\xff", "unknown.bin", "application/octet-stream"),
        ],
    )
    def test_detect_mime_type(self, sample, filename, expected):
        assert detect_mime_type(sample, filename) == expected


class TestPhaseEventFromInfo:
    @pytest.mark.parametrize(
        "message,canonical",
        [
            (msg, canonical)
            for msg, canonical in INFO_PHASE_TO_CANONICAL.items()
        ],
    )
    def test_maps_info_messages(self, message, canonical):
        obj = {"time": "2026-01-01T12:00:00.123456Z", "message": message}
        event = _phase_event_from_info(obj)
        assert event is not None
        assert event[1] == canonical

    def test_unknown_message_returns_none(self):
        assert _phase_event_from_info({"time": "2026-01-01T12:00:00Z", "message": "other"}) is None


class TestPhaseEventFromInMemory:
    def test_parses_phase_update(self):
        obj = {
            "time": "2026-01-01T12:00:00.123456Z",
            "message": "Updating the in-memory phase from `collection copy` to `change event application`.",
        }
        event = _phase_event_from_in_memory(obj)
        assert event[1] == "change event application"

    def test_excludes_uninitialized(self):
        obj = {
            "time": "2026-01-01T12:00:00.123456Z",
            "message": "Updating the in-memory phase from `foo` to `uninitialized`.",
        }
        assert _phase_event_from_in_memory(obj) is None


class TestPhaseEventsFromApi:
    def test_converts_phase_transitions(self):
        api = [{"Phase": "Collection Copy", "Ts": {"T": 1700000000}}]
        events = _phase_events_from_api(api)
        assert len(events) == 1
        assert events[0][1] == "collection copy"

    def test_skips_missing_timestamp(self):
        api = [{"Phase": "Collection Copy", "Ts": {}}]
        assert _phase_events_from_api(api) == []


class TestMergePhaseEvents:
    def test_earliest_timestamp_wins_per_phase(self):
        info = [
            {
                "time": "2026-01-02T12:00:00.123456Z",
                "message": "Starting collection copy phase",
            }
        ]
        debug = [
            {
                "time": "2026-01-01T12:00:00.123456Z",
                "message": "Updating the in-memory phase from `x` to `collection copy`.",
            }
        ]
        merged = _merge_phase_events(info, debug)
        labels = [label for _, label in merged]
        assert labels == ["collection copy"]
        assert merged[0][0].startswith("2026-01-01")

    def test_api_source_included(self):
        api = [{"Phase": "change event application", "Ts": {"T": 1700000100}}]
        merged = _merge_phase_events([], [], api_transitions=api)
        assert merged[0][1] == "change event application"


class TestExtractProgressFlagEvents:
    def test_tracks_commit_and_write_transitions(self):
        responses = [
            {
                "time": "2026-01-01T12:00:00.123456Z",
                "body": json.dumps({"progress": {"canCommit": False, "canWrite": False}}),
            },
            {
                "time": "2026-01-01T12:01:00.123456Z",
                "body": json.dumps({"progress": {"canCommit": True, "canWrite": True}}),
            },
        ]
        events = _extract_progress_flag_events(responses)
        labels = [label for _, label in events]
        assert "Can Commit is True" in labels
        assert "Can Write is True" in labels

    def test_skips_invalid_json_body(self):
        responses = [{"time": "2026-01-01T12:00:00Z", "body": "not-json"}]
        assert _extract_progress_flag_events(responses) == []
