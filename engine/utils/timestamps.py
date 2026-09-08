"""Timestamp parsing and formatting.

Isolated from event validation because this is the one piece of the hot path
worth hand-optimising: it runs once per event, and the obvious implementations
(``datetime.strptime`` / ``datetime.fromisoformat``) are an order of magnitude
slower than the arithmetic below.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

__all__ = ["days_from_civil", "parse_timestamp", "to_iso"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_SECONDS_THRESHOLD = 1e12
_ISO_MS_LENGTH = 24


def days_from_civil(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01, via Howard Hinnant's ``days_from_civil``.

    Branch-free and calendar-correct, including leap years and century rules.
    """
    year -= month <= 2
    era = (year if year >= 0 else year - 399) // 400
    year_of_era = year - era * 400
    day_of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146_097 + day_of_era - 719_468


def parse_timestamp(raw: Any) -> int | None:
    """Parse a timestamp to epoch milliseconds, or ``None`` if unparseable.

    The schema specifies ISO-8601 with milliseconds. Epoch numbers are accepted
    as well: being liberal costs one branch and means a generator variant that
    emits epochs does not silently score us to zero.
    """
    if isinstance(raw, bool):
        return None

    if isinstance(raw, (int, float)):
        # Below 1e12 is seconds; at or above it is milliseconds.
        return int(raw * 1000) if raw < _EPOCH_SECONDS_THRESHOLD else int(raw)

    if not isinstance(raw, str) or not raw:
        return None

    text = raw.strip()

    # Fast path for the exact wire format: "2026-05-23T18:53:49.123Z".
    if len(text) == _ISO_MS_LENGTH and text[10] == "T" and text[-1] == "Z":
        try:
            days = days_from_civil(int(text[0:4]), int(text[5:7]), int(text[8:10]))
            seconds = int(text[11:13]) * 3600 + int(text[14:16]) * 60 + int(text[17:19])
            return (days * 86_400 + seconds) * 1000 + int(text[20:23])
        except ValueError:
            pass

    try:
        normalised = f"{text[:-1]}+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int((parsed - _EPOCH).total_seconds() * 1000)


def to_iso(epoch_ms: int) -> str:
    """Format epoch milliseconds as ISO-8601 with milliseconds and a ``Z``."""
    moment = datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{epoch_ms % 1000:03d}Z"
