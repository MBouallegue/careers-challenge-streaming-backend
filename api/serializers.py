"""DRF serializers for the read surface.

A note on where serializers are and are not used, because it is a deliberate
asymmetry rather than an oversight.

Response shapes go through serializers: they are the API contract, they are
read by another team's client, and they are queried at human rates. Declaring
them here means the contract is one artefact rather than a set of dict literals
scattered across views.

Inbound events do NOT go through a serializer. ``POST /events`` is the only path
that runs on all ~50,000 events per second the brief asks for, and instantiating
a serializer per event costs roughly an order of magnitude more than the
hand-written validator in ``engine.events``. That validator is also the single
source of truth for the acceptance rules, so routing ingest through a second,
slower definition of the same rules would risk them drifting apart. The event
schema is documented in ``docs/event_schema.md`` and enforced there.
"""

from __future__ import annotations

from rest_framework import serializers

__all__ = [
    "AlarmPageSerializer",
    "AlarmSerializer",
    "DeviceHealthSerializer",
    "IngestResultSerializer",
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


class IngestResultSerializer(serializers.Serializer):
    """Per-request ingest outcome. Batches report counts rather than failing whole."""

    ok = serializers.BooleanField()
    accepted = serializers.IntegerField()
    rejected = serializers.IntegerField()
    reason = serializers.CharField(
        allow_null=True, help_text="First rejection reason in the batch, if any."
    )
