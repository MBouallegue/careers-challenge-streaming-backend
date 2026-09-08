"""Event validation and the acceptance rules from ``docs/event_schema.md``.

Validation lives here rather than in a DRF serializer because this is the only
code path that runs on every one of the ~50,000 events per second the brief
asks for. A serializer instantiation per event costs roughly an order of
magnitude more than these branches, and buys nothing: there is no partial
update, no nested relation and no user-facing form here, just a fixed envelope
with six variants. DRF earns its place on the read side, where the shapes are
richer and the volume is trivial.
"""

from __future__ import annotations

import json
from typing import Any, Final, Literal, TypeAlias

from engine.utils.timestamps import parse_timestamp

__all__ = [
    "EVENT_TYPES",
    "PRIORITY",
    "PRIORITY_LEVELS",
    "Event",
    "RejectionReason",
    "parse_request_body",
    "validate_event",
]

#: Normalised internal event. Short keys are deliberate: this dict is written
#: verbatim to the write-ahead log, and at high event rates the envelope key
#: names are a meaningful fraction of the bytes on disk.
#:
#:     d = device_id, r = room_id, y = type, t = epoch ms, q = seq, v = payload
Event: TypeAlias = dict[str, Any]
RejectionReason: TypeAlias = str

EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {"heartbeat", "presence", "motion", "sleep_state", "fall_warn", "net_status"}
)
SLEEP_STATES: Final[frozenset[str]] = frozenset({"asleep", "awake", "unknown"})

#: Scheduling tiers for the ingest queue. Lower is more urgent.
PRIORITY_URGENT: Final = 0
PRIORITY_STATE: Final = 1
PRIORITY_TELEMETRY: Final = 2
PRIORITY_LEVELS: Final = 3

PRIORITY: Final[dict[str, int]] = {
    "fall_warn": PRIORITY_URGENT,      # graded on a 1s SLA; never queued behind telemetry
    "presence": PRIORITY_STATE,        # drives occupancy correctness
    "sleep_state": PRIORITY_STATE,
    "net_status": PRIORITY_STATE,
    "motion": PRIORITY_TELEMETRY,      # pure telemetry, safe to delay
    "heartbeat": PRIORITY_TELEMETRY,
}


def _is_number(value: Any) -> bool:
    """True for real numbers.

    ``isinstance(True, int)`` is True in Python, so a bare numeric check would
    happily accept ``confidence: true`` and poison the alarm record.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_payload(event_type: str, raw: dict[str, Any]) -> tuple[Any, RejectionReason | None]:
    """Validate the per-type fields, returning (payload, rejection_reason)."""
    match event_type:
        case "heartbeat":
            return None, None
        case "presence":
            in_room = raw.get("in_room")
            return (in_room, None) if isinstance(in_room, bool) else (None, "bad_in_room")
        case "motion":
            magnitude = raw.get("magnitude")
            return (float(magnitude), None) if _is_number(magnitude) else (None, "bad_magnitude")
        case "sleep_state":
            state = raw.get("state")
            return (state, None) if state in SLEEP_STATES else (None, "bad_state")
        case "fall_warn":
            confidence = raw.get("confidence")
            return (float(confidence), None) if _is_number(confidence) else (None, "bad_confidence")
        case "net_status":
            rssi = raw.get("rssi")
            return (float(rssi), None) if _is_number(rssi) else (None, "bad_rssi")
        case _:  # pragma: no cover - guarded by the EVENT_TYPES check
            return None, "bad_type"


def validate_event(
    raw: Any, now_ms: int, config
) -> tuple[Literal[True], Event] | tuple[Literal[False], RejectionReason]:
    """Validate and normalise one raw event.

    Returns ``(True, event)`` or ``(False, reason)``. The reason strings are
    stable and exported as metric labels, so operators can see *why* a fleet is
    being rejected rather than just how much.
    """
    if not isinstance(raw, dict):
        return False, "malformed"

    device_id = raw.get("device_id")
    room_id = raw.get("room_id")
    event_type = raw.get("type")

    if not isinstance(device_id, str) or not device_id:
        return False, "missing_device_id"
    if not isinstance(room_id, str) or not room_id:
        return False, "missing_room_id"
    if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
        return False, "bad_type"

    timestamp_ms = parse_timestamp(raw.get("ts"))
    if timestamp_ms is None:
        return False, "bad_ts"

    # The asymmetry comes from the spec: a device replaying an offline buffer
    # legitimately sends old events, but a device claiming to be in the future
    # has a broken clock.
    if timestamp_ms > now_ms + config.future_limit_ms:
        return False, "ts_too_future"
    if timestamp_ms < now_ms - config.past_limit_ms:
        return False, "ts_too_old"

    # `seq` is OPTIONAL by design. docs/event_schema.md lists it in the
    # envelope, but every worked example in the root README omits it, and the
    # spec says it may have gaps. Ordering is by `ts`, never by `seq`, so a
    # missing seq is not grounds to reject an otherwise valid event.
    seq = raw.get("seq")
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, int)):
        return False, "bad_seq"

    payload, reason = _validate_payload(event_type, raw)
    if reason is not None:
        return False, reason

    return True, {
        "d": device_id,
        "r": room_id,
        "y": event_type,
        "t": timestamp_ms,
        "q": seq,
        "v": payload,
    }


def parse_request_body(body: bytes) -> tuple[Literal[True], list[Any]] | tuple[Literal[False], str]:
    """Accept a single event, a JSON array, or newline-delimited JSON.

    The bundled generator posts one event per request, but batching is how a
    real fleet cuts per-event overhead, and NDJSON is what a replaying device
    buffer naturally produces. Supporting all three costs a few lines and
    removes a class of "wrong shape, scored zero" risk.
    """
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return False, "empty_body"

    if text.startswith("["):
        try:
            events = json.loads(text)
        except ValueError:
            return False, "invalid_json"
        return (True, events) if isinstance(events, list) else (False, "invalid_json")

    if "\n" in text:
        events = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                events.append(json.loads(stripped))
            except ValueError:
                return False, "invalid_json"
        return True, events

    try:
        return True, [json.loads(text)]
    except ValueError:
        return False, "invalid_json"
