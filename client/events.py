"""Event synthesis for the load client.

Mirrors the distribution and the fall-jitter behaviour of
``event_generator/generate.py`` so that load runs exercise the same dedup path
as the graded scenarios, including emitting each physical fall as one to three
copies that share a timestamp and carry consecutive sequence numbers.
"""

from __future__ import annotations

import random
import time
from typing import Any, Final

__all__ = ["EVENT_WEIGHTS", "EventFactory"]

#: Matches the bundled generator: heartbeat ~1Hz, motion next, falls rare.
EVENT_WEIGHTS: Final[tuple[tuple[str, int], ...]] = (
    ("heartbeat", 50),
    ("motion", 15),
    ("presence", 4),
    ("sleep_state", 3),
    ("net_status", 3),
    ("fall_warn", 1),
)

_TYPES: Final = tuple(name for name, _ in EVENT_WEIGHTS)
_CUMULATIVE: Final = tuple(
    sum(weight for _, weight in EVENT_WEIGHTS[: index + 1]) for index in range(len(EVENT_WEIGHTS))
)
_TOTAL_WEIGHT: Final = _CUMULATIVE[-1]


def to_iso(epoch_ms: int) -> str:
    seconds, milliseconds = divmod(epoch_ms, 1000)
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds))
    return f"{stamp}.{milliseconds:03d}Z"


class EventFactory:
    """Builds well-formed events for a simulated fleet."""

    def __init__(self, device_count: int = 5_000, devices_per_room: int = 2, seed: int = 1234):
        self._random = random.Random(seed)
        self._sequences = [0] * device_count
        self._device_count = device_count
        self._devices_per_room = devices_per_room

    def _pick_type(self) -> str:
        roll = self._random.randrange(_TOTAL_WEIGHT)
        for index, threshold in enumerate(_CUMULATIVE):
            if roll < threshold:
                return _TYPES[index]
        return _TYPES[-1]  # pragma: no cover

    def build(self, device_index: int | None = None) -> dict[str, Any]:
        if device_index is None:
            device_index = self._random.randrange(self._device_count)
        self._sequences[device_index] += 1

        epoch_ms = int(time.time() * 1000)
        event_type = self._pick_type()
        event: dict[str, Any] = {
            "device_id": f"dev_{device_index:04d}",
            "room_id": f"room_{device_index // self._devices_per_room:03d}",
            "type": event_type,
            "ts": to_iso(epoch_ms),
            "seq": self._sequences[device_index],
        }

        match event_type:
            case "presence":
                event["in_room"] = self._random.choice([True, False])
            case "motion":
                event["magnitude"] = round(self._random.random(), 2)
            case "sleep_state":
                event["state"] = self._random.choice(["asleep", "awake", "unknown"])
            case "fall_warn":
                event["confidence"] = round(self._random.uniform(0.7, 0.99), 2)
            case "net_status":
                event["rssi"] = self._random.randint(-90, -50)

        return event

    @staticmethod
    def event_id_for(event: dict[str, Any]) -> str:
        """The alarm id the service will assign to this fall.

        Mirrors ``engine.alarms.store``: device plus canonical timestamp. Lets
        the client correlate a received alarm back to the exact moment it sent
        the event, which is what makes an honest end-to-end latency possible.
        """
        from engine.utils.timestamps import parse_timestamp

        return f"fall_{event['device_id']}_{parse_timestamp(event['ts'])}"
