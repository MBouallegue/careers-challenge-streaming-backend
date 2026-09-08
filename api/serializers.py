"""DRF serializers for the read surface.

These are **output allowlists, not validators**, and the distinction is the
whole reason they exist.

They perform no validation: there is no ``validate()`` here and ``is_valid()``
is never called. Inbound events are validated in ``engine.events``, which is the
single source of truth for the acceptance rules — ``POST /events`` is the only
path that runs on every event in the fleet, and a serializer instantiation per
event costs roughly an order of magnitude more than that hand-written validator.
Defining the same rules a second time here would be slower *and* would give them
somewhere to drift apart.

What they do buy is that a serializer emits **only its declared fields**. The
engine's internal dicts carry bookkeeping the API must never leak — ``ts_ms``,
``seq_min``/``seq_max``, the ``rx`` receive timestamp. Passing responses through
a declared field set means adding an internal field to an engine structure
cannot silently publish it, which a hand-built dict literal in a view does not
guarantee. It also keeps the response contract in one file rather than scattered
across view bodies.

The cost is one marshalling pass per response. On endpoints queried at human
rates that is a good trade; on ingest it would not be, which is why ingest does
not use them.
"""

from __future__ import annotations

from rest_framework import serializers

__all__ = [
    "AlarmPageSerializer",
    "AlarmSerializer",
    "DeviceHealthSerializer",
    "RoomOccupancySerializer",
]


class DeviceHealthSerializer(serializers.Serializer):
    """Latest heartbeat and rolling availability for one device."""

    device_id = serializers.CharField()
    known = serializers.BooleanField(
        help_text="False when the device has never been seen; the rest is then zeroed."
    )
    room_id = serializers.CharField(required=False, allow_null=True)
    last_heartbeat_ts = serializers.CharField(allow_null=True)
    last_seen_ts = serializers.CharField(required=False, allow_null=True)
    availability_5m = serializers.FloatField(
        help_text="Fraction of the last 300 seconds that contained a heartbeat (0..1)."
    )
    heartbeats_5m = serializers.IntegerField(
        help_text="Distinct seconds covered. Exposed so the ratio is auditable."
    )
    expected_5m = serializers.IntegerField(help_text="Heartbeats expected at ~1Hz.")


class RoomOccupancySerializer(serializers.Serializer):
    """Current presence and time-weighted occupancy over a window."""

    room_id = serializers.CharField()
    known = serializers.BooleanField()
    in_room = serializers.BooleanField(help_text="Latest presence event by ts wins.")
    occupied_pct = serializers.FloatField(
        help_text="Occupied fraction of the window (0..1), integrated over time."
    )
    occupied_seconds = serializers.FloatField()
    window_seconds = serializers.IntegerField()
    transitions = serializers.IntegerField(
        help_text="Presence transitions retained for this room."
    )


class AlarmSerializer(serializers.Serializer):
    """One deduplicated fall. Each physical fall appears exactly once."""

    event_id = serializers.CharField(
        help_text="Deterministic natural key, stable across restart and replay."
    )
    device_id = serializers.CharField()
    room_id = serializers.CharField()
    ts = serializers.CharField(help_text="Canonical (earliest observed) fall timestamp.")
    confidence = serializers.FloatField(allow_null=True)
    cursor = serializers.IntegerField(
        help_text="Position in the alarm feed. Pass back as ?since= to resume."
    )
    duplicates_collapsed = serializers.IntegerField(
        help_text="Jitter copies folded into this alarm."
    )


class AlarmPageSerializer(serializers.Serializer):
    """A page of alarms plus the cursor needed to resume from its end."""

    alarms = AlarmSerializer(many=True)
    count = serializers.IntegerField()
    cursor = serializers.IntegerField(help_text="Highest cursor currently in the feed.")
    since = serializers.CharField(allow_null=True)
