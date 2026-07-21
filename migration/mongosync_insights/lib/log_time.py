"""ISO-8601 helpers for log timestamp search bounds (literal, no timezone conversion).

Start bounds use millisecond-prefixed strings without a ``Z`` suffix so SQLite
string comparison matches stored mongosync log ``time`` values with or without
``Z``. End bounds use a ``Z`` suffix on the selected second (no fraction).
"""
from datetime import datetime


class InvalidLogTimestamp(ValueError):
    """Raised when a search timestamp cannot be parsed."""


def _strip_timezone_suffix(value: str) -> str:
    normalized = value.rstrip("Z")
    t_index = normalized.find("T")
    if t_index == -1:
        return normalized

    time_part = normalized[t_index + 1 :]
    for sep in ("+", "-"):
        idx = time_part.rfind(sep)
        if idx <= 0:
            continue
        maybe_tz = time_part[idx:]
        if len(maybe_tz) >= 6 and maybe_tz[1:3].isdigit() and maybe_tz[3] == ":":
            return normalized[: t_index + 1 + idx]
    return normalized


def parse_log_search_datetime(value: str) -> datetime:
    """Parse a search timestamp using the entered date/time values as-is."""
    raw = (value or "").strip()
    if not raw:
        raise InvalidLogTimestamp("empty timestamp")

    try:
        return datetime.fromisoformat(_strip_timezone_suffix(raw))
    except ValueError as exc:
        raise InvalidLogTimestamp(f"invalid timestamp: {value}") from exc


def normalize_search_start(value: str) -> str:
    """Normalize a search start bound for timestamp_gte comparison."""
    dt = parse_log_search_datetime(value)
    if dt.microsecond:
        ms = dt.microsecond // 1000
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}"
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000"


def normalize_search_end(value: str) -> str:
    """Normalize a search end bound for inclusive timestamp_lte comparison."""
    dt = parse_log_search_datetime(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
