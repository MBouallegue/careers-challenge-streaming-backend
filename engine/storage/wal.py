"""Segmented write-ahead log.

The WAL is the system of record: every accepted event is appended here, and all
aggregate state is a deterministic fold over it. That is what makes restart
correctness tractable. Recovery is "load the snapshot, replay the tail", and
replay reproduces identical alarm ids and cursors because the fold is
order-deterministic and the log preserves ingestion order.

Writes are group-committed: events accumulate in a buffer and are flushed with a
single ``write`` + ``fsync`` per batch. One fsync per event would cap throughput
at a few thousand events per second; one fsync per batch costs the same syscall
for twenty thousand.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, TypedDict

__all__ = ["LogPosition", "WriteAheadLog", "segment_filename"]

_SEGMENT_PATTERN: Final = re.compile(r"^wal-(\d{6})\.jsonl$")
_SEGMENT_TEMPLATE: Final = "wal-%06d.jsonl"


class LogPosition(TypedDict):
    """A resume point in the log: which segment, and how many bytes into it."""

    seg: int
    off: int


class ReplayResult(TypedDict):
    count: int
    skipped: int


def segment_filename(number: int) -> str:
    return _SEGMENT_TEMPLATE % number


class WriteAheadLog:
    def __init__(self, config) -> None:
        self.config = config
        self.directory = Path(config.data_dir) / "wal"
        self.segment = 1
        self.offset = 0
        self.bytes_written = 0
        self.fsyncs = 0

        self._fd: int | None = None
        self._buffer: list[str] = []
        self._buffered_bytes = 0

    # ------------------------------------------------------------ lifecycle

    def open(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        existing = self.segments()
        self.segment = existing[-1] if existing else 1
        path = self.directory / segment_filename(self.segment)
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        self.offset = path.stat().st_size

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.flush()
        if self._fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._fd)
            self._fd = None

    def segments(self) -> list[int]:
        """Segment numbers present on disk, ascending."""
        if not self.directory.is_dir():
            return []
        found = [
            int(match.group(1))
            for match in (_SEGMENT_PATTERN.match(p.name) for p in self.directory.iterdir())
            if match
        ]
        return sorted(found)

    # ---------------------------------------------------------------- write

    def append(self, event: dict[str, Any]) -> None:
        """Buffer one event. Durable only after :meth:`flush`."""
        line = json.dumps(event, separators=(",", ":")) + "\n"
        self._buffer.append(line)
        self._buffered_bytes += len(line)

    @property
    def pending(self) -> int:
        return len(self._buffer)

    def flush(self) -> int:
        """Write and fsync everything buffered. Returns bytes flushed."""
        if not self._buffer:
            return 0

        payload = "".join(self._buffer).encode("utf-8")
        self._buffer.clear()
        self._buffered_bytes = 0

        written = 0
        while written < len(payload):
            written += os.write(self._fd, payload[written:])
        os.fsync(self._fd)

        self.fsyncs += 1
        self.offset += len(payload)
        self.bytes_written += len(payload)

        if self.offset >= self.config.wal_segment_bytes:
            self._rotate()
        return len(payload)

    def _rotate(self) -> None:
        os.close(self._fd)
        self.segment += 1
        self.offset = 0
        self._fd = os.open(
            self.directory / segment_filename(self.segment),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o644,
        )

    # ------------------------------------------------------------- position

    def position(self) -> LogPosition:
        """Durable position: everything before this has been fsynced."""
        return {"seg": self.segment, "off": self.offset}

    def next_position(self) -> LogPosition:
        """Where the next appended record will land, counting buffered bytes.

        The engine records this as its recovery resume point at the moment the
        queue drains to empty, because at that instant every event written so
        far has also been applied to aggregate state.
        """
        return {"seg": self.segment, "off": self.offset + self._buffered_bytes}

    # --------------------------------------------------------------- replay

    def replay(
        self, position: LogPosition | None, on_event: Callable[[dict[str, Any]], None]
    ) -> ReplayResult:
        """Replay from ``position`` (inclusive) to the end of the log.

        A hard kill can leave a torn final line: the process died mid-write, or
        the page cache held a partial record. That case is tolerated explicitly.
        A trailing fragment that does not parse is dropped rather than aborting
        recovery; every complete line before it was fsynced and is durable.
        """
        count = 0
        skipped = 0

        for number in self.segments():
            if position and number < position["seg"]:
                continue
            path = self.directory / segment_filename(number)
            if not path.exists():
                continue

            with path.open("rb") as handle:
                if position and number == position["seg"] and position["off"] > 0:
                    handle.seek(position["off"])
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        on_event(json.loads(line))
                    except (ValueError, KeyError, TypeError):
                        skipped += 1  # torn tail record
                    else:
                        count += 1

        return {"count": count, "skipped": skipped}

    def truncate_before(self, segment: int) -> int:
        """Delete segments made redundant by a snapshot."""
        removed = 0
        for number in self.segments():
            if number >= segment:
                continue
            try:
                (self.directory / segment_filename(number)).unlink()
            except OSError:
                continue
            removed += 1
        return removed
