"""Versioned API routes.

Every route lives under ``/api/v1/`` with a name, so clients can reverse them
and so a v2 can exist alongside without breaking anyone. The unversioned
aliases required by the challenge harness are mounted separately in
``config/urls.py``.
"""

from django.urls import path

from api import views

app_name = "api"

urlpatterns = [
    path("events", views.EventIngestView.as_view(), name="event-ingest"),
    path("devices/<str:device_id>/health", views.DeviceHealthView.as_view(), name="device-health"),
    path("rooms/<str:room_id>/occupancy", views.RoomOccupancyView.as_view(), name="room-occupancy"),
    path("alarms", views.AlarmListView.as_view(), name="alarm-list"),
    path("alarms/stream", views.AlarmStreamView.as_view(), name="alarm-stream"),
    path("ops/health", views.LivenessView.as_view(), name="ops-health"),
    path("ops/ready", views.ReadinessView.as_view(), name="ops-ready"),
    path("ops/stats", views.StatsView.as_view(), name="ops-stats"),
    path("ops/metrics", views.MetricsView.as_view(), name="ops-metrics"),
]
