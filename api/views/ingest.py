"""Event ingestion view.

This is the Django-routed entry point for ``POST /events``. In production the
same request is normally served by ``config.asgi.FastIngestRoute``, a raw ASGI
route mounted ahead of Django that calls the identical implementation in
``api.ingest``. See that module for the measurements behind the split.

Keeping this view means the route exists under any ASGI server, under
``runserver``, and under the Django test client, and that the behaviour under
test is the behaviour in production.
"""

from __future__ import annotations

from django.http import HttpResponse
from django.views import View

from api.ingest import ingest_body

__all__ = ["EventIngestView"]


class EventIngestView(View):
    """Accept one event, a JSON array of events, or newline-delimited JSON.

    Returns ``202 Accepted``: the event is durable in the write-ahead log and
    queued for aggregation, though the aggregates may not reflect it yet.
    """

    async def post(self, request):
        status, body = await ingest_body(request.body)
        return HttpResponse(body, content_type="application/json", status=status)
