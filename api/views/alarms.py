"""The alarm feed: a replayable log endpoint and a live SSE stream.

Both are addressed by the same monotonic cursor, which is the mechanism that
makes "reconnect and resume without missing alarms generated during the gap"
true rather than aspirational. A cursor is a position in the alarm log, not a
wall-clock time, so it can neither skip nor repeat alarms that share a
timestamp, and it survives a restart because the log is rebuilt deterministically
from the snapshot plus the WAL tail.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Final

from adrf.views import APIView
from django.http import StreamingHttpResponse
from django.views import View
from rest_framework import status
from rest_framework.response import Response

from api.serializers import AlarmPageSerializer
from api.services import engine
from engine.alarms import Subscriber, public_alarm
from engine.config import get_config
from engine.utils.params import parse_since

__all__ = ["AlarmListView", "AlarmStreamView"]

DEFAULT_PAGE_LIMIT: Final = 200_000
MAX_PAGE_LIMIT: Final = 1_000_000
BACKLOG_LIMIT: Final = 200_000


class AlarmListView(APIView):
    """Deduplicated falls, oldest first.

    ``?since=`` accepts a feed cursor (``0`` means "everything"), an epoch in
    milliseconds, or an ISO-8601 timestamp. Small integers are read as cursors
    rather than as epoch 1970, which is what makes ``?since=0`` return the full
    feed instead of nothing.
    """

    async def get(self, request):
        raw_since = request.query_params.get("since")
        kind, value = parse_since(raw_since)
        if kind == "invalid":
            return Response({"error": "bad_since"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            limit = min(int(request.query_params.get("limit", DEFAULT_PAGE_LIMIT)), MAX_PAGE_LIMIT)
        except (TypeError, ValueError):
            limit = DEFAULT_PAGE_LIMIT

        store = engine.alarms
        alarms = (
            store.since_cursor(value, limit)
            if kind == "cursor"
            else store.since_ts(value, limit)
        )

        room_id = request.query_params.get("room_id")
        if room_id:
            alarms = [alarm for alarm in alarms if alarm["room_id"] == room_id]

        page = {
            "alarms": [public_alarm(alarm) for alarm in alarms],
            "count": len(alarms),
            "cursor": store.cursor,
            "since": raw_since,
        }
        return Response(AlarmPageSerializer(page).data)


def _format_sse(alarm: dict[str, Any]) -> str:
    payload = json.dumps(public_alarm(alarm), separators=(",", ":"))
    return f"id: {alarm['cursor']}\nevent: alarm\ndata: {payload}\n\n"


class AlarmStreamView(View):
    """Live Server-Sent Events feed of new alarms.

    A plain Django view rather than a DRF one on purpose: DRF's ``Response`` is
    a rendered, content-negotiated, finite body, which is the opposite of an
    open-ended stream. Streaming is an HTTP concern here, not a serialisation
    one, so it uses the framework primitive that actually models it.

    Resumable two ways: ``?since=<cursor>``, or the standard ``Last-Event-ID``
    header that an ``EventSource`` sends automatically on reconnect. The header
    wins, so a browser reconnect resumes exactly with no client code.
    """

    async def get(self, request):
        header_cursor = request.headers.get("Last-Event-ID")
        raw_since = header_cursor if header_cursor is not None else request.GET.get("since")
        kind, value = parse_since(raw_since)
        if kind == "invalid":
            return Response({"error": "bad_since"}, status=status.HTTP_400_BAD_REQUEST)

        store = engine.alarms
        if kind == "cursor":
            start_cursor = value
        else:
            later = store.since_ts(value, 1)
            start_cursor = later[0]["cursor"] - 1 if later else store.cursor

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        # Subscribe BEFORE snapshotting the backlog, with no await in between.
        # On a single event loop that makes the pair atomic: the subscriber can
        # neither miss an alarm published in the gap nor receive one twice.
        subscriber = Subscriber(send=queue.put_nowait, last_cursor=start_cursor)
        unsubscribe = store.subscribe(subscriber)
        backlog = list(store.since_cursor(start_cursor, BACKLOG_LIMIT))
        if backlog:
            subscriber.last_cursor = backlog[-1]["cursor"]

        keepalive_seconds = get_config().sse_heartbeat_ms / 1000

        async def event_stream():
            try:
                for alarm in backlog:
                    yield _format_sse(alarm)
                yield f": connected cursor={subscriber.last_cursor}\n\n"

                while True:
                    try:
                        alarm = await asyncio.wait_for(queue.get(), timeout=keepalive_seconds)
                    except TimeoutError:
                        # Comment frames keep proxies and load balancers from
                        # reaping an idle stream.
                        yield ": keepalive\n\n"
                        continue
                    yield _format_sse(alarm)
            finally:
                unsubscribe()

        response = StreamingHttpResponse(event_stream(), content_type="text/event-stream")
        response["Cache-Control"] = "no-cache, no-transform"
        response["X-Accel-Buffering"] = "no"
        return response
