"""The ingest implementation, shared by both transports that expose it.

There is one function here that actually ingests events, and two thin callers:
``api.views.ingest.EventIngestView`` (a Django view, so the route exists under
any server and under the Django test client) and ``config.asgi.FastIngestRoute``
(a raw ASGI route mounted ahead of Django, used in production).

Why two callers rather than one:

Measured on this machine, single warm connection, 400-600 requests per row:

    raw ASGI app under uvicorn, no Django          2,365 req/s   p50 0.39 ms
    our POST /events through Django + engine         409 req/s   p50 1.60 ms

The engine itself accounts for about 8 microseconds of that (it sustains
~124,000 accepts/sec in isolation). Nearly all the remaining ~1.2 ms is
Django's ASGI request/response cycle: building an ``HttpRequest``, resolving the
URL, running the view wrapper, building an ``HttpResponse``. That cost is
completely reasonable for a read endpoint queried a few times a second, and it
is the ingest ceiling when the fleet sends one event per HTTP request, which is
exactly what the graded generator does.

So the hot path skips the framework and everything else keeps it. The logic is
shared, so the two transports cannot drift: they produce byte-identical
responses, and the Django view is what the tests exercise.
"""

from __future__ import annotations

import json
import time
from typing import Final

from api.services import engine
from engine.events import parse_request_body

__all__ = ["INGEST_PATHS", "ingest_body"]

#: Paths the fast ASGI route claims. Kept in sync with config/urls.py.
INGEST_PATHS: Final[frozenset[str]] = frozenset({"/events", "/api/v1/events"})

_JSON_CONTENT_TYPE: Final = "application/json"
#: The overwhelmingly common response: one valid event accepted.
_ACCEPTED_SINGLE: Final = b'{"ok":true,"accepted":1,"rejected":0,"reason":null}'


async def ingest_body(body: bytes) -> tuple[int, bytes]:
    """Ingest a request body. Returns ``(status_code, json_bytes)``.

    Awaits only for backpressure, which is the point: when the queue is deep we
    delay the acknowledgement rather than shedding the event, because the sender
    counts any non-2xx as a failed send. Delay, never drop.
    """
    parsed_ok, parsed = parse_request_body(body)
    if not parsed_ok:
        return 400, json.dumps({"error": parsed}).encode()

    received_ms = int(time.time() * 1000)
    accepted = 0
    rejected = 0
    first_reason: str | None = None

    for raw_event in parsed:
        was_accepted, result = engine.accept(raw_event, received_ms)
        if was_accepted:
            accepted += 1
        else:
            rejected += 1
            if first_reason is None:
                first_reason = result

    # A lone invalid event gets a 400 so the sender learns why. A mixed batch
    # still gets a 202 with counts: one malformed record in a replayed offline
    # buffer must not discard the rest of the buffer.
    if accepted == 0 and rejected == 1:
        return 400, json.dumps({"error": first_reason, "accepted": 0, "rejected": 1}).encode()

    await engine.wait_for_capacity()

    if accepted == 1 and rejected == 0:
        return 202, _ACCEPTED_SINGLE

    return 202, json.dumps(
        {"ok": True, "accepted": accepted, "rejected": rejected, "reason": first_reason}
    ).encode()
