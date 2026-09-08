"""Periodic state snapshots, so recovery is bounded by snapshot age rather than
by total uptime.

Crash safety comes from the write order: serialise to a temporary file, fsync
it, demote the current snapshot to ``.prev``, then rename the temporary into
place. Rename is atomic, so a crash at any point leaves either a complete
current snapshot or a complete previous one, never a half-written file that
recovery would load as if it were whole.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Final

__all__ = ["SnapshotStore"]

SNAPSHOT_VERSION: Final = 1


class SnapshotStore:
    def __init__(self, config) -> None:
        self.config = config
        self.directory = Path(config.data_dir)
        self.current_path = self.directory / "snapshot.json"
        self.previous_path = self.directory / "snapshot.prev.json"
        self.temp_path = self.directory / "snapshot.tmp"

        self.last_saved_ms = 0
        self.last_duration_ms = 0
        self.saves = 0

    def save(self, state: dict[str, Any]) -> int:
        """Write a snapshot atomically. Returns how long it took, in ms."""
        started = time.time()
        self.directory.mkdir(parents=True, exist_ok=True)

        payload = json.dumps(
            {"version": SNAPSHOT_VERSION, "created_ms": int(started * 1000), **state},
            separators=(",", ":"),
        ).encode("utf-8")

        descriptor = os.open(self.temp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        if self.current_path.exists():
            self.previous_path.unlink(missing_ok=True)
            os.replace(self.current_path, self.previous_path)
        os.replace(self.temp_path, self.current_path)

        self.last_saved_ms = int(started * 1000)
        self.last_duration_ms = int((time.time() - started) * 1000)
        self.saves += 1
        return self.last_duration_ms

    def load(self) -> tuple[dict[str, Any], str] | None:
        """Newest readable snapshot as ``(state, path)``, or ``None``.

        A truncated or corrupt current snapshot falls through to the previous
        generation rather than failing recovery outright.
        """
        for path in (self.current_path, self.previous_path):
            if not path.exists():
                continue
            try:
                snapshot = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                continue
            if isinstance(snapshot, dict) and snapshot.get("version") == SNAPSHOT_VERSION:
                return snapshot, str(path)
        return None
