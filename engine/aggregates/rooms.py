"""Per-room occupancy.

`presence` events are *state transitions* ("emitted when state changes"), not
samples. Occupancy is therefore the integral of a step function, not a count of
events. The reference stub gets this wrong on purpose: it returns
len(events)/window, which is an event rate, not a time fraction.

We keep a per-room timeline sorted by `ts` and integrate at query time rather
than maintaining a running total. That single decision is what makes late
events correct for free: an event that arrives 20 minutes after the fact is
inserted at its true position, and the next query integrates over the repaired
timeline. A running counter would need compensating deltas and would drift;
recomputation cannot drift.

The cost is O(events in window) per query, but presence is a rare event type
(~5% of traffic, ~2 devices per room), so a 1h window holds a few hundred
points per room.
"""
from __future__ import annotations

from bisect import bisect_right
from typing import Any


class Room:
    """Parallel lists keep bisect cheap and avoid allocating a tuple per event."""

    __slots__ = ("ts", "val")

    def __init__(self) -> None:
        self.ts: list[int] = []
        self.val: list[bool] = []

    def insert(self, ts_ms: int, in_room: bool) -> None:
        # Fast path: in-order arrival, the overwhelming majority.
        if not self.ts or ts_ms > self.ts[-1]:
            self.ts.append(ts_ms)
            self.val.append(in_room)
            return
        idx = bisect_right(self.ts, ts_ms)
        if idx > 0 and self.ts[idx - 1] == ts_ms:
            # Same instant, from a re-delivery or a second device: last wins.
            self.val[idx - 1] = in_room
            return
        self.ts.insert(idx, ts_ms)
        self.val.insert(idx, in_room)

    def current(self) -> bool | None:
        """Latest presence event by ts wins, per the spec."""
        return self.val[-1] if self.val else None

    def occupied_ms(self, now_ms: int, window_ms: int) -> int:
        """Occupied milliseconds within [now_ms - window_ms, now_ms]."""
        if not self.ts or window_ms <= 0:
            return 0

        start = now_ms - window_ms
        idx = bisect_right(self.ts, start) - 1

        # State carried into the window. With no presence event before the
        # window opened we treat the room as unoccupied: it is the only
        # defensible default, and it is documented rather than silent.
        occupied = self.val[idx] if idx >= 0 else False
        cursor = start
        total = 0

        for i in range(idx + 1, len(self.ts)):
            t = self.ts[i]
            if t > now_ms:
                break  # clock-skewed future events are outside this window
            if occupied:
                total += t - cursor
            cursor = t
            occupied = self.val[i]

        if occupied:
            total += now_ms - cursor
        return total

    def prune(self, cutoff_ms: int) -> None:
        """Drop history no query can reach, but keep the carry-in state."""
        idx = bisect_right(self.ts, cutoff_ms) - 1
        # Keep entry `idx` itself: it defines the state at the window start.
        if idx > 0:
            del self.ts[:idx]
            del self.val[:idx]


class RoomStore:
    def __init__(self, config):
        self.config = config
        self.rooms: dict[str, Room] = {}

    def _get(self, room_id: str) -> Room:
        room = self.rooms.get(room_id)
        if room is None:
            room = Room()
            self.rooms[room_id] = room
        return room

    def apply(self, ev: dict) -> None:
        if ev["y"] != "presence":
            return
        self._get(ev["r"]).insert(ev["t"], ev["v"] is True)

    def occupancy(self, room_id: str, window_ms: int, now_ms: int) -> dict[str, Any]:
        room = self.rooms.get(room_id)
        window_sec = round(window_ms / 1000)
        if room is None:
            return {
                "room_id": room_id,
                "known": False,
                "in_room": False,
                "occupied_pct": 0.0,
                "occupied_seconds": 0.0,
                "window_seconds": window_sec,
                "transitions": 0,
            }
        occ_ms = room.occupied_ms(now_ms, window_ms)
        frac = min(1.0, max(0.0, occ_ms / window_ms)) if window_ms else 0.0
        return {
            "room_id": room_id,
            "known": True,
            "in_room": room.current() is True,
            "occupied_pct": round(frac, 6),
            "occupied_seconds": round(occ_ms / 1000.0, 3),
            "window_seconds": window_sec,
            "transitions": len(room.ts),
        }

    def prune(self, now_ms: int) -> None:
        cutoff = now_ms - self.config.max_occupancy_window_ms - self.config.retention_grace_ms
        for room in self.rooms.values():
            room.prune(cutoff)

    def __len__(self) -> int:
        return len(self.rooms)

    def snapshot(self) -> list[dict]:
        # Flat [t, 0|1, t, 0|1, ...] is roughly half the JSON bytes of objects.
        out = []
        for room_id, room in self.rooms.items():
            flat: list[int] = []
            for t, v in zip(room.ts, room.val, strict=True):
                flat.append(t)
                flat.append(1 if v else 0)
            out.append({"i": room_id, "p": flat})
        return out

    def restore(self, records: list[dict] | None) -> None:
        self.rooms.clear()
        for rec in records or []:
            room = Room()
            flat = rec.get("p") or []
            for i in range(0, len(flat), 2):
                room.ts.append(flat[i])
                room.val.append(flat[i + 1] == 1)
            self.rooms[rec["i"]] = room
