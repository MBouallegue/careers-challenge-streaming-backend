"""Operational endpoints: liveness, readiness, stats and Prometheus metrics."""

from __future__ import annotations

from adrf.views import APIView
from django.http import HttpResponse
from rest_framework.response import Response

from api.services import engine
from engine import metrics

__all__ = ["LivenessView", "MetricsView", "ReadinessView", "StatsView"]

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


class LivenessView(APIView):
    """The process is up. Deliberately does not touch engine state."""

    async def get(self, request):
        return Response({"ok": True})


class ReadinessView(APIView):
    """The process is up and the ingest queue depth is visible to the caller."""

    async def get(self, request):
        return Response({"ok": True, "queue_depth": engine.queue.size})


class StatsView(APIView):
    """Everything an operator wants during a burst, in one request."""

    async def get(self, request):
        return Response(engine.stats())


class MetricsView(APIView):
    """Prometheus text exposition."""

    async def get(self, request):
        engine.stats()  # refresh gauges before rendering
        return HttpResponse(metrics.render_prometheus(), content_type=PROMETHEUS_CONTENT_TYPE)
