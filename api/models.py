"""The durable relational record of alarms.

This is the one place the ORM earns its keep, and it is a deliberate split:

  * Per-device health and per-room occupancy are hot, high-cardinality,
    continuously-recomputed aggregates over a bounded time window. They live in
    memory, backed by the WAL. Writing them to a database at 50k events/sec
    would be the whole bottleneck and would buy nothing, because every value is
    derived and can be rebuilt from the log.

  * A fall alarm is a clinical record. Someone will want to query it months
    later, join it against a resident, annotate it, export it for an incident
    review. That is a relational database's job, and Django's is to make that
    boring: a model, a migration, a queryset.

`event_id` is the primary key rather than a surrogate, because it is already a
deterministic natural key (device + canonical timestamp). That makes the mirror
idempotent for free: replaying the WAL after a crash re-inserts the same rows,
and a conflict-ignoring bulk insert turns that into a no-op.
"""
from __future__ import annotations

from django.db import models


class Alarm(models.Model):
    event_id = models.CharField(max_length=200, primary_key=True)
    device_id = models.CharField(max_length=100, db_index=True)
    room_id = models.CharField(max_length=100, db_index=True)
    ts = models.DateTimeField(db_index=True, help_text="Canonical (earliest) fall timestamp")
    confidence = models.FloatField(null=True, blank=True)
    cursor = models.BigIntegerField(db_index=True, help_text="Position in the alarm feed")
    duplicates_collapsed = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["cursor"]
        indexes = [models.Index(fields=["room_id", "ts"])]

    def __str__(self) -> str:
        return f"{self.event_id} @ {self.ts.isoformat()} ({self.room_id})"
