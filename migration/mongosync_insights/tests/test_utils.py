"""Tests for formatting helpers in lib/utils.py."""
import pytest

from lib.utils import (
    convert_bytes,
    format_byte_size,
    format_bytes_compact,
    format_count,
    format_lag_time_seconds,
    format_ratio,
    format_seconds_title,
)


class TestFormatBytesCompact:
    @pytest.mark.parametrize(
        "size,expected",
        [
            (None, "—"),
            (-1, "—"),
            (0, "0 B"),
            (512, "512 B"),
            (1024, "1.0 KB"),
            (1536, "1.5 KB"),
            (1024 * 1024, "1.0 MB"),
            (6_500_000_000, "6.1 GB"),
            (150 * 1024, "150 KB"),
        ],
    )
    def test_format_bytes_compact(self, size, expected):
        assert format_bytes_compact(size) == expected


class TestFormatLagTimeSeconds:
    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (None, "—"),
            (-5, "—"),
            (0, "0s"),
            (45, "45s"),
            (90, "1m 30s"),
            (3661, "1h 1m"),
            (90000, "1d 1h"),
        ],
    )
    def test_format_lag_time_seconds(self, seconds, expected):
        assert format_lag_time_seconds(seconds) == expected


class TestFormatSecondsTitle:
    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (None, None),
            (-5, None),
            (0, "0 seconds"),
            (1, "1 second"),
            (45, "45 seconds"),
            (90, "90 seconds"),
            (1582, "1,582 seconds"),
        ],
    )
    def test_format_seconds_title(self, seconds, expected):
        assert format_seconds_title(seconds) == expected


class TestFormatCount:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "—"),
            (0, "0"),
            (1234567, "1,234,567"),
            ("42", "42"),
            ("bad", "—"),
        ],
    )
    def test_format_count(self, value, expected):
        assert format_count(value) == expected


class TestFormatRatio:
    def test_both_values(self):
        assert format_ratio(100, 200) == "100 / 200"

    def test_none_numerator(self):
        assert format_ratio(None, 200) == "— / 200"


class TestFormatByteSize:
    @pytest.mark.parametrize(
        "size,expected_value,expected_unit",
        [
            (500, 500, "Bytes"),
            (2048, 2.0, "KiloBytes"),
            (1024 * 1024, 1.0, "MegaBytes"),
            (1024**3, 1.0, "GigaBytes"),
            (1024**4, 1.0, "TeraBytes"),
        ],
    )
    def test_format_byte_size(self, size, expected_value, expected_unit):
        value, unit = format_byte_size(size)
        assert value == expected_value
        assert unit == expected_unit


class TestConvertBytes:
    @pytest.mark.parametrize(
        "size,target,expected",
        [
            (2048, "KiloBytes", 2.0),
            (1024**2, "MegaBytes", 1.0),
            (1024**3, "GigaBytes", 1.0),
            (1024**4, "TeraBytes", 1.0),
            (512, "Bytes", 512),
        ],
    )
    def test_convert_bytes(self, size, target, expected):
        assert convert_bytes(size, target) == expected
