"""Tests for live metadata status helpers."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from lib.live_metadata_status import (
    build_indexes_starts_after_data_copy,
    compute_lag_time_seconds,
    fetch_partition_byte_totals,
    format_write_blocking_mode,
    index_build_progress_allowed,
    is_during_or_after_cea,
    normalize_sync_phase,
    read_verification_mode_from_global_state,
    should_suppress_index_build_progress,
    verification_progress_allowed,
)


class TestComputeLagTimeSeconds:
    def test_returns_none_without_resume_data(self):
        assert compute_lag_time_seconds(None) is None
        assert compute_lag_time_seconds({}) is None

    def test_uses_max_crud_and_ddl_timestamps(self):
        crud_ts = datetime.now(tz=timezone.utc) - timedelta(seconds=120)
        ddl_ts = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
        resume = {
            "crudChangeStreamResumeInfo": {"lastEventTs": crud_ts},
            "ddlChangeStreamResumeInfo": {"lastEventTs": ddl_ts},
        }
        lag = compute_lag_time_seconds(resume)
        assert 59 <= lag <= 62


class TestFormatWriteBlockingMode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("destinationOnly", "Destination Only"),
            ("sourceAndDestination", "Source and Destination"),
            ("none", "None"),
            ("unknown", None),
        ],
    )
    def test_format_write_blocking_mode(self, raw, expected):
        assert format_write_blocking_mode(raw) == expected


class TestSyncPhaseHelpers:
    @pytest.mark.parametrize(
        "phase,expected",
        [
            (" Collection Copy ", "collection copy"),
            (None, None),
        ],
    )
    def test_normalize_sync_phase(self, phase, expected):
        assert normalize_sync_phase(phase) == expected

    @pytest.mark.parametrize(
        "phase,expected",
        [
            ("collection copy", False),
            ("change event application", True),
            ("commit handler called", True),
        ],
    )
    def test_is_during_or_after_cea(self, phase, expected):
        assert is_during_or_after_cea(phase) is expected


class TestProgressGating:
    def test_build_indexes_starts_after_data_copy(self):
        assert build_indexes_starts_after_data_copy("afterDataCopy") is True
        assert build_indexes_starts_after_data_copy("beforeDataCopy") is False

    def test_should_suppress_index_build_progress(self):
        assert should_suppress_index_build_progress("afterDataCopy", "collection copy") is True
        assert should_suppress_index_build_progress("afterDataCopy", "change event application") is False

    @pytest.mark.parametrize(
        "policy,phase,expected",
        [
            ("never", "change event application", False),
            ("beforeDataCopy", "collection copy", True),
            ("afterDataCopy", "collection copy", False),
            ("afterDataCopy", "change event application", True),
        ],
    )
    def test_index_build_progress_allowed(self, policy, phase, expected):
        assert index_build_progress_allowed(policy, phase) is expected

    @pytest.mark.parametrize(
        "mode,phase,expected",
        [
            ("disabled", "change event application", False),
            ("startAtCEA", "collection copy", False),
            ("startAtCEA", "change event application", True),
        ],
    )
    def test_verification_progress_allowed(self, mode, phase, expected):
        assert verification_progress_allowed(mode, phase) is expected


class TestReadVerificationMode:
    def test_reads_camel_case_key(self):
        assert read_verification_mode_from_global_state({"verificationMode": "startAtCEA"}) == "startAtCEA"

    def test_reads_lowercase_key(self):
        assert read_verification_mode_from_global_state({"verificationmode": "disabled"}) == "disabled"


class TestFetchPartitionByteTotals:
    def test_returns_totals_from_aggregate(self):
        db = MagicMock()
        db.partitions.aggregate.return_value = [
            {"copiedBytes": 100, "totalBytes": 200}
        ]
        assert fetch_partition_byte_totals(db) == (100, 200)

    def test_returns_none_when_empty(self):
        db = MagicMock()
        db.partitions.aggregate.return_value = []
        assert fetch_partition_byte_totals(db) == (None, None)
