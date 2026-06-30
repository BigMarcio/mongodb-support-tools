"""Tests for log timestamp search helpers."""
import pytest

from lib.log_time import (
    InvalidLogTimestamp,
    normalize_search_end,
    normalize_search_start,
    parse_log_search_datetime,
)


class TestParseLogSearchDatetime:
    def test_parses_without_timezone_suffix(self):
        dt = parse_log_search_datetime("2026-01-01T10:30:45")
        assert dt.year == 2026
        assert dt.hour == 10
        assert dt.minute == 30

    def test_strips_z_suffix_without_shifting_time(self):
        dt = parse_log_search_datetime("2026-01-01T10:30:45.000Z")
        assert dt.hour == 10
        assert dt.minute == 30

    def test_rejects_empty(self):
        with pytest.raises(InvalidLogTimestamp):
            parse_log_search_datetime("")


class TestNormalizeSearchBounds:
    def test_normalize_start_without_fraction(self):
        assert normalize_search_start("2026-01-01T10:30:45") == "2026-01-01T10:30:45.000"

    def test_normalize_end_includes_selected_second(self):
        assert normalize_search_end("2026-01-01T10:30:45") == "2026-01-01T10:30:45.999999"
