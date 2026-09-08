"""Dependency-free metrics: counters, gauges and latency histograms.

Each histogram keeps an exact-quantile ring alongside the Prometheus buckets.
Bucket interpolation is fine for dashboards, but the scorecard asks for a real
p95, so the last N observations are retained and sorted on read.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Iterable
from typing import Final

__all__ = [
    "Histogram",
    "alarm_latency",
    "get_counter",
    "increment",
    "ingest_latency",
    "render_prometheus",
    "reset",
    "set_gauge",
]

BUCKETS_MS: Final[tuple[int, ...]] = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000)
_DEFAULT_QUANTILES: Final = (0.5, 0.95, 0.99)

_counters: dict[str, float] = {}
_gauges: dict[str, float] = {}


def _key(name: str, labels: str | None) -> str:
    return f"{name}|{labels}" if labels else name


def increment(name: str, amount: float = 1, *, labels: str | None = None) -> None:
    """Add to a counter, optionally under a Prometheus label set."""
    key = _key(name, labels)
    _counters[key] = _counters.get(key, 0) + amount


def set_gauge(name: str, value: float) -> None:
    """Set an instantaneous value."""
    _gauges[name] = value


def get_counter(name: str, *, labels: str | None = None) -> float:
    return _counters.get(_key(name, labels), 0)


def reset() -> None:
    """Clear all series. Used by tests to keep assertions independent."""
    _counters.clear()
    _gauges.clear()
    alarm_latency.reset()
    ingest_latency.reset()


class Histogram:
    """A Prometheus histogram with an exact-quantile window."""

    __slots__ = (
        "_capacity",
        "_position",
        "_ring",
        "buckets",
        "count",
        "help_text",
        "name",
        "total",
    )

    def __init__(self, name: str, help_text: str, capacity: int = 20_000) -> None:
        self.name = name
        self.help_text = help_text
        self.buckets = [0] * (len(BUCKETS_MS) + 1)
        self.count = 0
        self.total = 0.0
        self._ring: list[float] = []
        self._capacity = capacity
        self._position = 0

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.buckets[bisect.bisect_left(BUCKETS_MS, value)] += 1

        if len(self._ring) < self._capacity:
            self._ring.append(value)
        else:
            self._ring[self._position] = value
            self._position = (self._position + 1) % self._capacity

    def quantiles(
        self, quantiles: Iterable[float] = _DEFAULT_QUANTILES
    ) -> dict[str, float | None]:
        """Exact quantiles over the retained window."""
        if not self._ring:
            return {f"p{q * 100:g}": None for q in quantiles}

        ordered = sorted(self._ring)
        results: dict[str, float | None] = {}
        for quantile in quantiles:
            rank = min(len(ordered) - 1, max(0, math.ceil(quantile * len(ordered)) - 1))
            results[f"p{quantile * 100:g}"] = round(ordered[rank], 3)
        return results

    def reset(self) -> None:
        self.buckets = [0] * (len(BUCKETS_MS) + 1)
        self.count = 0
        self.total = 0.0
        self._ring.clear()
        self._position = 0

    def render(self) -> str:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} histogram",
        ]
        cumulative = 0
        for index, upper_bound in enumerate(BUCKETS_MS):
            cumulative += self.buckets[index]
            lines.append(f'{self.name}_bucket{{le="{upper_bound}"}} {cumulative}')
        cumulative += self.buckets[-1]
        lines.append(f'{self.name}_bucket{{le="+Inf"}} {cumulative}')
        lines.append(f"{self.name}_sum {self.total:g}")
        lines.append(f"{self.name}_count {self.count}")
        return "\n".join(lines)


#: Event acceptance to alarm publication. This is the graded SLA.
alarm_latency = Histogram(
    "alarm_publish_latency_ms",
    "Milliseconds from event acceptance to alarm publication on the feed",
)

#: HTTP receipt to aggregation state update.
ingest_latency = Histogram(
    "ingest_apply_latency_ms",
    "Milliseconds from event acceptance to aggregation state update",
)


def render_prometheus() -> str:
    """Render every series in the Prometheus text exposition format."""
    lines: list[str] = []
    for key, value in _counters.items():
        name, _, labels = key.partition("|")
        lines.append(f"{name}{{{labels}}} {value:g}" if labels else f"{name} {value:g}")
    lines.extend(f"{name} {value:g}" for name, value in _gauges.items())
    lines.append(alarm_latency.render())
    lines.append(ingest_latency.render())
    return "\n".join(lines) + "\n"
