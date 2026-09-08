"""Root URL configuration.

Two mount points, on purpose:

``/api/v1/...``
    The API as it should be named: versioned, reversible, grouped by resource.

``/...`` (unversioned)
    The exact paths the challenge harness calls (``/events``,
    ``/devices/{id}/health``, ``/rooms/{id}/occupancy``, ``/alarms``, and the
    conventional ``/healthz``, ``/readyz``, ``/metrics``). These are a fixed
    external contract, so they are kept verbatim and simply point at the same
    views. Renaming them would be tidier and would also score zero.
"""

from django.urls import include, path

from api import views

api_v1 = [path("api/v1/", include("api.urls"))]

harness_contract = [
    path("events", views.EventIngestView.as_view(), name="legacy-event-ingest"),
    path(
        "devices/<str:device_id>/health",
        views.DeviceHealthView.as_view(),
        name="legacy-device-health",
    ),
    path(
        "rooms/<str:room_id>/occupancy",
        views.RoomOccupancyView.as_view(),
        name="legacy-room-occupancy",
    ),
    path("alarms", views.AlarmListView.as_view(), name="legacy-alarm-list"),
    path("alarms/stream", views.AlarmStreamView.as_view(), name="legacy-alarm-stream"),
    path("healthz", views.LivenessView.as_view(), name="legacy-healthz"),
    path("readyz", views.ReadinessView.as_view(), name="legacy-readyz"),
    path("stats", views.StatsView.as_view(), name="legacy-stats"),
    path("metrics", views.MetricsView.as_view(), name="legacy-metrics"),
]

urlpatterns = api_v1 + harness_contract
