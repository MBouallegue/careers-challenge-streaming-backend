"""Engine configuration.

Every knob is environment-overridable so the durability, dedup and backpressure
behaviour can be tuned for a deployment without editing code or rebuilding.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Final

__all__ = ["EngineConfig", "get_config"]

HOUR_MS: Final = 60 * 60 * 1000
MINUTE_MS: Final = 60 * 1000


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name) or default


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """Immutable engine settings.

    Frozen so no request handler can mutate shared configuration at runtime;
    tests derive variants with :func:`dataclasses.replace`.
    """

    # --- Acceptance rules (docs/event_schema.md) ---------------------------
    future_limit_ms: int = HOUR_MS
    past_limit_ms: int = HOUR_MS

    # --- Aggregation windows ------------------------------------------------
    heartbeat_window_seconds: int = 300
    max_occupancy_window_ms: int = HOUR_MS
    retention_grace_ms: int = 5 * MINUTE_MS

    # --- Fall deduplication -------------------------------------------------
    # These two defaults are measured, not guessed. Capturing the real wire
    # output of event_generator/generate.py in `offline` mode (4,756 events,
    # 116 fall_warn events, ground truth 60 distinct falls) showed that jitter
    # copies of one physical fall carry an IDENTICAL ts and a `seq` gap of
    # exactly 1, while two genuinely distinct falls from one device were
    # observed as little as 1001 ms apart with a seq gap of 3.
    #
    # A 1000 ms window reproduces ground truth exactly (60/60). At 2000 ms we
    # begin merging real falls; at 10000 ms we collapse 116 events into 51
    # alarms and lose 9 real ones. See tests/test_dedup.py, which asserts this
    # against the captured wire fixture.
    dedup_window_ms: int = 1_000
    #: Secondary guard, applied only when both events carry ``seq``. It lets an
    #: operator widen the time window for a noisier fleet without silently
    #: merging distinct falls, because jitter copies are seq-contiguous and
    #: separate falls are not.
    dedup_seq_slack: int = 2

    # --- Backpressure -------------------------------------------------------
    queue_high_water: int = 200_000
    queue_low_water: int = 100_000
    max_ack_delay_ms: int = 4_000
    drain_budget_ms: int = 8
    drain_batch_max: int = 20_000

    # --- Durability ---------------------------------------------------------
    data_dir: str = "data"
    fsync_ms: int = 200
    snapshot_ms: int = 10_000
    wal_segment_bytes: int = 64 * 1024 * 1024

    # --- Live feed ----------------------------------------------------------
    sse_heartbeat_ms: int = 15_000

    # --- Observability ------------------------------------------------------
    #: Interval for the periodic operational summary log line.
    report_ms: int = 30_000

    @classmethod
    def from_env(cls) -> EngineConfig:
        """Build a configuration from the process environment."""
        return cls(
            future_limit_ms=_env_int("FUTURE_LIMIT_MS", HOUR_MS),
            past_limit_ms=_env_int("PAST_LIMIT_MS", HOUR_MS),
            heartbeat_window_seconds=_env_int("HEARTBEAT_WINDOW_SECONDS", 300),
            max_occupancy_window_ms=_env_int("MAX_OCCUPANCY_WINDOW_MS", HOUR_MS),
            retention_grace_ms=_env_int("RETENTION_GRACE_MS", 5 * MINUTE_MS),
            dedup_window_ms=_env_int("DEDUP_WINDOW_MS", 1_000),
            dedup_seq_slack=_env_int("DEDUP_SEQ_SLACK", 2),
            queue_high_water=_env_int("QUEUE_HIGH_WATER", 200_000),
            queue_low_water=_env_int("QUEUE_LOW_WATER", 100_000),
            max_ack_delay_ms=_env_int("MAX_ACK_DELAY_MS", 4_000),
            drain_budget_ms=_env_int("DRAIN_BUDGET_MS", 8),
            drain_batch_max=_env_int("DRAIN_BATCH_MAX", 20_000),
            data_dir=_env_str("DATA_DIR", "data"),
            fsync_ms=_env_int("FSYNC_MS", 200),
            snapshot_ms=_env_int("SNAPSHOT_MS", 10_000),
            wal_segment_bytes=_env_int("WAL_SEGMENT_BYTES", 64 * 1024 * 1024),
            sse_heartbeat_ms=_env_int("SSE_HEARTBEAT_MS", 15_000),
            report_ms=_env_int("REPORT_MS", 30_000),
        )

    def evolve(self, **changes) -> EngineConfig:
        """Return a copy with the given fields replaced."""
        return replace(self, **changes)


_config: EngineConfig | None = None


def get_config() -> EngineConfig:
    """Process-wide configuration, resolved from the environment once."""
    global _config
    if _config is None:
        _config = EngineConfig.from_env()
    return _config
