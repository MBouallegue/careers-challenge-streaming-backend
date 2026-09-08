"""ASGI entrypoint with an explicit lifespan wrapper.

Django's ``ASGIHandler`` does not implement the ASGI lifespan protocol, so the
framework offers no hook for "run this once when the server starts, and once
when it stops". This service needs one: state must be recovered from the
write-ahead log before the first request is served, and the background drain
task has to be attached to the running event loop, which does not exist yet at
import time.

``AppConfig.ready()`` is the usual place people reach for and is the wrong one
here: it runs during app registry population, before there is a loop, and it
runs in management commands too. Wrapping the ASGI callable keeps the process
lifecycle explicit and confined to the server path.
"""

from __future__ import annotations

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

from django.core.asgi import get_asgi_application

from api.ingest import INGEST_PATHS, ingest_body

django_application = get_asgi_application()


class FastIngestRoute:
    """Serve ``POST /events`` directly from ASGI, ahead of Django.

    Django's request/response cycle costs roughly 1.2 ms per request, which is
    fine everywhere except here: the fleet sends one event per HTTP request, so
    that cost *is* the ingest ceiling. This route reads the body, hands it to
    the same implementation the Django view uses, and writes the response,
    skipping ``HttpRequest`` construction, URL resolution and view dispatch.

    Everything that is not an ingest POST falls through to Django untouched.
    """

    __slots__ = ("application",)

    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] != "http"
            or scope["method"] != "POST"
            or scope["path"] not in INGEST_PATHS
        ):
            await self.application(scope, receive, send)
            return

        chunks: list[bytes] = []
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body")
            if chunk:
                chunks.append(chunk)
            if not message.get("more_body"):
                break

        status, body = await ingest_body(b"".join(chunks))
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})


class LifespanMiddleware:
    """Handle ASGI lifespan messages; pass everything else to Django."""

    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if scope["type"] != "lifespan":
            await self.application(scope, receive, send)
            return

        from api import services

        while True:
            message = await receive()
            match message["type"]:
                case "lifespan.startup":
                    try:
                        await services.startup()
                    except Exception as exc:
                        # Fail loudly: a service that silently starts without
                        # recovered state would report empty aggregates as fact.
                        await send({"type": "lifespan.startup.failed", "message": str(exc)})
                        raise
                    await send({"type": "lifespan.startup.complete"})
                case "lifespan.shutdown":
                    try:
                        await services.shutdown()
                    finally:
                        await send({"type": "lifespan.shutdown.complete"})
                    return


# Order matters: lifespan outermost so startup/shutdown are handled before
# anything else, then the ingest fast path, then Django for every other route.
application = LifespanMiddleware(FastIngestRoute(django_application))
