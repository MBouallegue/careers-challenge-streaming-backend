"""HTTP views, grouped by concern.

``ingest``    the write path
``telemetry`` per-device health and per-room occupancy
``alarms``    the alarm log and the live SSE feed
``ops``       liveness, readiness, stats, Prometheus
"""

from api.views.alarms import AlarmListView, AlarmStreamView
from api.views.ingest import EventIngestView
from api.views.ops import LivenessView, MetricsView, ReadinessView, StatsView
from api.views.telemetry import DeviceHealthView, RoomOccupancyView

__all__ = [
    "AlarmListView",
    "AlarmStreamView",
    "DeviceHealthView",
    "EventIngestView",
    "LivenessView",
    "MetricsView",
    "ReadinessView",
    "RoomOccupancyView",
    "StatsView",
]
