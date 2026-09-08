"""Admin registration.

Registered but not installed by default: ``django.contrib.admin`` pulls in the
auth, sessions and messages middleware stack, which would run on every ingest
request. Enable it by adding the contrib apps and their middleware in settings
when an operator UI is actually wanted.
"""

from django.contrib import admin

from api.models import Alarm


@admin.register(Alarm)
class AlarmAdmin(admin.ModelAdmin):
    list_display = ("event_id", "room_id", "device_id", "ts", "confidence", "duplicates_collapsed")
    list_filter = ("room_id",)
    search_fields = ("event_id", "device_id", "room_id")
    date_hierarchy = "ts"
    ordering = ("-cursor",)
