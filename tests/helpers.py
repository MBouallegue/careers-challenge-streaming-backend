"""Shared test fixtures and builders."""

from __future__ import annotations

import shutil
import tempfile
from typing import Any

from engine.config import EngineConfig
from engine.utils.timestamps import to_iso

#: A fixed clock keeps every assertion deterministic.
#: 2026-05-23T18:53:49.123Z, the timestamp used in the challenge README.
FIXED_NOW_MS = 1_779_562_429_123


def build_config(**overrides: Any) -> EngineConfig:
    """A config with background timers effectively disabled.

    Tests drive flush and snapshot explicitly; leaving the real intervals in
    place would make assertions depend on wall-clock timing.
    """
    return EngineConfig(fsync_ms=3_600_000, snapshot_ms=3_600_000).evolve(**overrides)


def build_temp_config(**overrides: Any) -> EngineConfig:
    """A config rooted in a throwaway data directory."""
    return build_config(data_dir=tempfile.mkdtemp(prefix="teton-test-"), **overrides)


def remove_data_dir(config: EngineConfig) -> None:
    shutil.rmtree(config.data_dir, ignore_errors=True)


def fall_event(
    device_id: str, ts_ms: int, seq: int | None, confidence: float = 0.9, room_id: str = "room_1"
) -> dict[str, Any]:
    return {
        "device_id": device_id,
        "room_id": room_id,
        "type": "fall_warn",
        "ts": to_iso(ts_ms),
        "seq": seq,
        "confidence": confidence,
    }


def presence_event(
    room_id: str, ts_ms: int, in_room: bool, device_id: str = "dev_1"
) -> dict[str, Any]:
    return {
        "device_id": device_id,
        "room_id": room_id,
        "type": "presence",
        "ts": to_iso(ts_ms),
        "in_room": in_room,
    }


def heartbeat_event(device_id: str, ts_ms: int, room_id: str = "room_1") -> dict[str, Any]:
    return {
        "device_id": device_id,
        "room_id": room_id,
        "type": "heartbeat",
        "ts": to_iso(ts_ms),
    }


def normalised(event_type: str, room_id: str, ts_ms: int, payload: Any = None) -> dict[str, Any]:
    """Build an already-normalised engine event, bypassing acceptance rules.

    Useful where the test is about an aggregate rather than about validation,
    for example history older than the one-hour acceptance window.
    """
    return {"d": "dev_1", "r": room_id, "y": event_type, "t": ts_ms, "q": None, "v": payload}
