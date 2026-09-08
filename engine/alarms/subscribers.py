"""Live alarm feed subscribers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ["Subscriber"]


class Subscriber:
    """A consumer attached to the live alarm feed.

    A slotted class rather than a dict: subscribers are held in a set keyed by
    identity, and ``last_cursor`` is mutated on the publish path where an
    attribute write is cheaper than a dict lookup.

    ``last_cursor`` is the position in the alarm log this consumer has already
    seen. It is the whole resume mechanism: a reconnecting consumer supplies it
    and receives exactly the alarms it missed, with no gap and no repeat.
    """

    __slots__ = ("last_cursor", "send")

    def __init__(self, send: Callable[[dict[str, Any]], None], last_cursor: int = 0) -> None:
        self.send = send
        self.last_cursor = last_cursor
